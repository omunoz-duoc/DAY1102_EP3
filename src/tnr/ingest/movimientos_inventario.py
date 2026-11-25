"""Movimientos de inventario continuos: consumidor Python (una línea JSON por recepción).

Bronze append-only con id_recepcion y ts_ingesta -> Silver idempotente por id_evento.
Un movimiento válido cuyo ts_evento cae en un corte ya cerrado marca ese cierre para reproceso."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterator

from sqlalchemy import Connection, text

from tnr.common.batch import new_run_id
from tnr.common.contracts import Rejection, SilverRecord
from tnr.common.db import get_engine
from tnr.common.eventos_web import parse_ts, payload_hash
from tnr.common.stream import BronzeAppender, Recepcion
from tnr.common.stream import iter_recepciones as _iter
from tnr.common.tipos import to_utc_iso, utc_now_iso
from tnr.config import Config

FUENTE = "movimientos_inventario"
VERSION = "movimientos_inventario.v1"
SIGNO = {"venta_confirmada": -1, "reposicion": 1, "ajuste": 0}   # 0 = ambos signos
COLS = ["id_evento", "ts_evento", "id_producto", "id_ubicacion", "tipo", "delta_unidades", "payload_sha256",
        "id_recepcion", "ts_ingesta", "run_id"]


def iter_recepciones(cfg: Config, corte_ingesta: str | None = None) -> Iterator[Recepcion]:
    return _iter(cfg, FUENTE, corte_ingesta)


def normalize_movimiento(payload: Any, *, tz: str, ref: str, ts_ingesta: str,
                         id_recepcion: str) -> SilverRecord | Rejection:
    if not isinstance(payload, dict):
        return Rejection(ref, None, "PAYLOAD_NO_OBJETO")
    id_evento = str(payload.get("id_evento") or "").strip().upper()
    ts, _ = parse_ts(payload.get("ts_evento"), tz)
    sku = str(payload.get("id_producto") or "").strip().upper()
    ubic = str(payload.get("id_ubicacion") or "").strip().upper()
    tipo = str(payload.get("tipo") or "").strip().lower()
    delta = payload.get("delta_unidades")
    errores = []
    if not re.fullmatch(r"MOV-\d{4,}", id_evento):
        errores.append("ID_EVENTO_INVALIDO")
    if ts is None:
        errores.append("TS_EVENTO_INVALIDO")
    if not re.fullmatch(r"P\d{3}", sku):
        errores.append("SKU_INVALIDO")
    if not re.fullmatch(r"(CD|T)\d{2}", ubic):
        errores.append("UBICACION_INVALIDA")
    if tipo not in SIGNO:
        errores.append("TIPO_INVALIDO")
    if not isinstance(delta, int) or isinstance(delta, bool) or delta == 0:
        errores.append("DELTA_NO_ENTERO_O_CERO")
    elif tipo in SIGNO and SIGNO[tipo] and (delta > 0) != (SIGNO[tipo] > 0):
        errores.append("SIGNO_INCOHERENTE_CON_TIPO")
    if errores:
        return Rejection(ref, id_evento or None, errores[0], ",".join(errores))
    return SilverRecord(ref=ref, key=id_evento, data={
        "id_evento": id_evento, "ts_evento": to_utc_iso(ts), "id_producto": sku, "id_ubicacion": ubic, "tipo": tipo,
        "delta_unidades": delta, "payload_sha256": payload_hash(payload), "id_recepcion": id_recepcion,
        "ts_ingesta": ts_ingesta})


class MovimientosConsumer:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.engine = get_engine(cfg.warehouse_path)
        self.run_id = new_run_id(FUENTE)
        self.bronze = BronzeAppender(cfg, FUENTE)
        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO etl_runs (run_id, fuente, ruta, version_esquema, modo, ts_ingesta, estado, bronze_path)"
                " VALUES (:r, :f, 'consumidor Python', :v, 'normal', :t, 'en_proceso', :b)"),
                {"r": self.run_id, "f": FUENTE, "v": VERSION, "t": utc_now_iso(), "b": str(self.bronze.archivo)})

    def _process(self, conn: Connection, rec: Recepcion) -> dict:
        ref = f"{rec.archivo}#{rec.id_recepcion}"
        try:
            payload = json.loads(rec.body)
        except json.JSONDecodeError:
            payload, out = None, Rejection(ref, None, "JSON_INVALIDO")
        else:
            out = normalize_movimiento(payload, tz=self.cfg.tz, ref=ref, ts_ingesta=rec.ts_ingesta,
                                       id_recepcion=rec.id_recepcion)
        estado, detalle = "insertada", None
        if isinstance(out, Rejection):
            estado, detalle = "rechazada", out.motivo
            conn.execute(text("INSERT INTO etl_rechazos (run_id, fuente, ref_bronze, id_registro, motivo, detalle)"
                              " VALUES (:r, :f, :ref, :k, :m, :d)"),
                         {"r": self.run_id, "f": FUENTE, "ref": ref, "k": out.key, "m": out.motivo, "d": out.detalle})
        else:
            prev = conn.execute(text("SELECT payload_sha256 FROM silver_movimientos_inventario WHERE id_evento = :i"),
                                {"i": out.key}).first()
            if prev is None:
                conn.execute(text(f"INSERT INTO silver_movimientos_inventario ({', '.join(COLS)})"
                                  f" VALUES ({', '.join(':' + c for c in COLS)})"), {**out.data, "run_id": self.run_id})
                # cierres ya publicados que este movimiento afecta -> reproceso
                conn.execute(text("UPDATE ctl_cierres_stock SET estado = 'reproceso_pendiente',"
                                  " ts_ultimo_evento_tardio = :t WHERE fecha_corte >= :ts"),
                             {"t": rec.ts_ingesta, "ts": out.data["ts_evento"]})
            elif prev[0] == out.data["payload_sha256"]:
                estado = "duplicada"
            else:
                estado, detalle = "revision", "PAYLOAD_INCOMPATIBLE"
                conn.execute(text("INSERT INTO etl_revision (run_id, fuente, id_registro, motivo, evidencia)"
                                  " VALUES (:r, :f, :k, 'ID_EVENTO_PAYLOAD_INCOMPATIBLE', :e)"),
                             {"r": self.run_id, "f": FUENTE, "k": out.key,
                              "e": json.dumps({"id_recepcion": rec.id_recepcion, "payload": payload})})
        conn.execute(text("INSERT INTO stream_recepciones (id_recepcion, fuente, archivo, ts_ingesta, id_evento, estado,"
                          " detalle, run_id) VALUES (:id, :f, :a, :t, :e, :s, :d, :r)"),
                     {"id": rec.id_recepcion, "f": FUENTE, "a": rec.archivo, "t": rec.ts_ingesta, "e": out.key,
                      "s": estado, "d": detalle, "r": self.run_id})
        return {"id_recepcion": rec.id_recepcion, "ts_ingesta": rec.ts_ingesta, "id_evento": out.key,
                "estado": estado, "detalle": detalle}

    def receive(self, line: bytes | str) -> dict:
        body = line.decode("utf-8", errors="replace") if isinstance(line, bytes) else line
        with self.bronze.lock:
            rec = self.bronze.append(body.strip())
            try:
                with self.engine.begin() as conn:
                    return self._process(conn, rec)
            except Exception as exc:
                return {"id_recepcion": rec.id_recepcion, "ts_ingesta": rec.ts_ingesta,
                        "estado": "pendiente_silver", "detalle": repr(exc)}

    def consume_file(self, path: Path, *, desde: int = 1, hasta: int | None = None) -> list[dict]:
        out = []
        with Path(path).open(encoding="utf-8") as fh:
            for n, line in enumerate(fh, start=1):
                if n < desde or not line.strip():
                    continue
                if hasta is not None and n > hasta:
                    break
                out.append({"linea": n, **self.receive(line)})
        return out

    def recover(self) -> dict:
        with self.engine.connect() as conn:
            confirmadas = {r[0] for r in conn.execute(
                text("SELECT id_recepcion FROM stream_recepciones WHERE fuente = :f"), {"f": FUENTE})}
        estados: dict[str, int] = {}
        with self.bronze.lock:
            for rec in iter_recepciones(self.cfg):
                if rec.id_recepcion not in confirmadas:
                    with self.engine.begin() as conn:
                        e = self._process(conn, rec)["estado"]
                    estados[e] = estados.get(e, 0) + 1
        return {"run_id": self.run_id, "recuperadas": sum(estados.values()), "estados": estados}

    def close(self) -> None:
        with self.engine.begin() as conn:
            n = dict(conn.execute(text("SELECT estado, COUNT(*) FROM stream_recepciones WHERE run_id = :r"
                                       " GROUP BY estado"), {"r": self.run_id}).all())
            conn.execute(text(
                "UPDATE etl_runs SET estado = 'exitoso', ts_fin = :t, n_recibidos = :rec, n_excluidos = :dup,"
                " n_revision = :rev, n_rechazados = :rej, n_publicados = :pub WHERE run_id = :r"),
                {"t": utc_now_iso(), "rec": sum(n.values()), "dup": n.get("duplicada", 0), "rev": n.get("revision", 0),
                 "rej": n.get("rechazada", 0), "pub": n.get("insertada", 0), "r": self.run_id})
