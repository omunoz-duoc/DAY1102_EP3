"""Ruta rápida de eventos web: Bronze (append-only, fsync) -> Silver idempotente por id_evento.

* Cada recepción se escribe en Bronze ANTES de responder, con id_recepcion propio.
* Un único escritor por archivo activo: cada instancia del receptor tiene su archivo y un lock.
* stream_recepciones confirma qué recepciones llegaron a Silver; `recover()` procesa las no confirmadas.
"""
from __future__ import annotations

import json
from typing import Iterator

from sqlalchemy import Connection, text

from tnr.common.batch import new_run_id
from tnr.common.contracts import Rejection
from tnr.common.db import get_engine
from tnr.common.eventos_web import insert_eventos, minuto_utc, normalize_evento
from tnr.common.stream import BronzeAppender, Recepcion
from tnr.common.stream import iter_recepciones as _iter
from tnr.common.tipos import utc_now_iso
from tnr.config import Config

FUENTE = "web_stream"
VERSION = "eventos_web.v1"


def iter_recepciones(cfg: Config, corte_ingesta: str | None = None) -> Iterator[Recepcion]:
    return _iter(cfg, FUENTE, corte_ingesta)


class WebStreamReceiver:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.engine = get_engine(cfg.warehouse_path)
        self.run_id = new_run_id(FUENTE)
        self.bronze = BronzeAppender(cfg, FUENTE)
        self.archivo = self.bronze.archivo
        self._lock = self.bronze.lock
        ts = utc_now_iso()
        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO etl_runs (run_id, fuente, ruta, version_esquema, modo, ts_ingesta, estado, bronze_path)"
                " VALUES (:r, :f, 'POST /eventos', :v, 'normal', :t, 'en_proceso', :b)"),
                {"r": self.run_id, "f": FUENTE, "v": VERSION, "t": ts, "b": str(self.archivo)})

    # ------------------------------------------------------------------ Silver
    def _process(self, conn: Connection, rec: Recepcion) -> dict:
        ref = f"{rec.archivo}#{rec.id_recepcion}"
        try:
            payload = json.loads(rec.body)
        except json.JSONDecodeError:
            payload = None
            out = Rejection(ref, None, "JSON_INVALIDO")
        else:
            out = normalize_evento(payload, origen="stream", tz=self.cfg.tz, ref=ref, ts_ingesta=rec.ts_ingesta,
                                   id_recepcion=rec.id_recepcion)
        estado, detalle = "insertada", None
        if isinstance(out, Rejection):
            estado, detalle = "rechazada", out.motivo
            conn.execute(text("INSERT INTO etl_rechazos (run_id, fuente, ref_bronze, id_registro, motivo, detalle)"
                              " VALUES (:r, :f, :ref, :k, :m, :d)"),
                         {"r": self.run_id, "f": FUENTE, "ref": ref, "k": out.key, "m": out.motivo, "d": out.detalle})
        else:
            prev = conn.execute(text("SELECT payload_sha256 FROM silver_eventos_web WHERE id_evento = :i"),
                                {"i": out.key}).first()
            if prev is None:
                insert_eventos(conn, [out], self.run_id)
                self._marcar_tardio(conn, out.data["ts_evento"], rec.ts_ingesta)
            elif prev[0] == out.data["payload_sha256"]:
                estado = "duplicada"
            else:
                estado, detalle = "revision", "PAYLOAD_INCOMPATIBLE"
                conn.execute(text("INSERT INTO etl_revision (run_id, fuente, id_registro, motivo, evidencia)"
                                  " VALUES (:r, :f, :k, 'ID_EVENTO_PAYLOAD_INCOMPATIBLE', :e)"),
                             {"r": self.run_id, "f": FUENTE, "k": out.key,
                              "e": json.dumps({"id_recepcion": rec.id_recepcion, "payload": payload}, ensure_ascii=False)})
            for a in out.alerts:
                conn.execute(text("INSERT INTO etl_alertas_calidad (run_id, fuente, ref_bronze, id_registro, campo,"
                                  " codigo, valor_original) VALUES (:r, :f, :ref, :k, :c, :co, :v)"),
                             {"r": self.run_id, "f": FUENTE, "ref": ref, "k": out.key, "c": a.campo, "co": a.codigo,
                              "v": None if a.valor_original is None else str(a.valor_original)})
        conn.execute(text("INSERT INTO stream_recepciones (id_recepcion, fuente, archivo, ts_ingesta, id_evento, estado,"
                          " detalle, run_id) VALUES (:id, :f, :a, :t, :e, :s, :d, :r)"),
                     {"id": rec.id_recepcion, "f": FUENTE, "a": rec.archivo, "t": rec.ts_ingesta, "e": out.key,
                      "s": estado, "d": detalle, "r": self.run_id})
        return {"id_recepcion": rec.id_recepcion, "ts_ingesta": rec.ts_ingesta, "id_evento": out.key,
                "estado": estado, "detalle": detalle}

    @staticmethod
    def _marcar_tardio(conn: Connection, ts_evento: str, ts_ingesta: str) -> None:
        """Un evento válido de una ventana ya cerrada la marca para reproceso."""
        conn.execute(text("UPDATE ctl_ventanas_web SET estado = 'reproceso_pendiente', ts_ultimo_evento_tardio = :t"
                          " WHERE minuto_utc = :m"), {"m": minuto_utc(ts_evento), "t": ts_ingesta})

    # ------------------------------------------------------------------ API
    def receive(self, body: bytes | str) -> dict:
        body_text = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body
        with self._lock:                                   # escritor único del archivo activo
            rec = self.bronze.append(body_text)
            try:
                with self.engine.begin() as conn:
                    return self._process(conn, rec)
            except Exception as exc:                       # queda en Bronze sin confirmar -> recover()
                return {"id_recepcion": rec.id_recepcion, "ts_ingesta": rec.ts_ingesta,
                        "estado": "pendiente_silver", "detalle": repr(exc)}

    def recover(self) -> dict:
        """Carga idempotente de recepciones archivadas sin confirmación en Silver."""
        with self.engine.connect() as conn:
            confirmadas = {r[0] for r in conn.execute(text("SELECT id_recepcion FROM stream_recepciones WHERE fuente = :f"), {"f": FUENTE})}
        estados: dict[str, int] = {}
        with self._lock:
            for rec in iter_recepciones(self.cfg):
                if rec.id_recepcion in confirmadas:
                    continue
                with self.engine.begin() as conn:
                    out = self._process(conn, rec)
                estados[out["estado"]] = estados.get(out["estado"], 0) + 1
        return {"run_id": self.run_id, "recuperadas": sum(estados.values()), "estados": estados}

    def close(self) -> None:
        with self.engine.begin() as conn:
            n = dict(conn.execute(text("SELECT estado, COUNT(*) FROM stream_recepciones WHERE run_id = :r GROUP BY estado"),
                                  {"r": self.run_id}).all())
            conn.execute(text(
                "UPDATE etl_runs SET estado = 'exitoso', ts_fin = :t, n_recibidos = :rec, n_excluidos = :dup,"
                " n_revision = :rev, n_rechazados = :rej, n_publicados = :pub WHERE run_id = :r"),
                {"t": utc_now_iso(), "rec": sum(n.values()), "dup": n.get("duplicada", 0), "rev": n.get("revision", 0),
                 "rej": n.get("rechazada", 0), "pub": n.get("insertada", 0), "r": self.run_id})
