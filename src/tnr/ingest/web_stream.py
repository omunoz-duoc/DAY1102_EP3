"""Ruta rápida de eventos web: Bronze (append-only, fsync) -> Silver idempotente por id_evento.

* Cada recepción se escribe en Bronze ANTES de responder, con id_recepcion propio.
* Un único escritor por archivo activo: cada instancia del receptor tiene su archivo y un lock.
* web_recepciones confirma qué recepciones llegaron a Silver; `recover()` procesa las no confirmadas.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from sqlalchemy import Connection, text

from tnr.common.batch import new_run_id
from tnr.common.contracts import Rejection
from tnr.common.db import get_engine
from tnr.common.eventos_web import insert_eventos, minuto_utc, normalize_evento
from tnr.common.tipos import utc_now_iso
from tnr.config import Config

FUENTE = "web_stream"
VERSION = "eventos_web.v1"


@dataclass
class Recepcion:
    id_recepcion: str
    ts_ingesta: str
    body: str
    archivo: str


def bronze_root(cfg: Config) -> Path:
    return cfg.bronze_dir / FUENTE


def iter_recepciones(cfg: Config, corte_ingesta: str | None = None) -> Iterator[Recepcion]:
    """Lee todas las recepciones archivadas, en orden de ingesta."""
    rows = []
    for path in sorted(bronze_root(cfg).glob("*/recepciones_*.jsonl")):
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                r = json.loads(line)
                if corte_ingesta and r["ts_ingesta"] > corte_ingesta:
                    continue
                rows.append(Recepcion(r["id_recepcion"], r["ts_ingesta"], r["body"],
                                      str(path.relative_to(cfg.bronze_dir))))
    rows.sort(key=lambda r: (r.ts_ingesta, r.id_recepcion))
    return iter(rows)


class WebStreamReceiver:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.engine = get_engine(cfg.warehouse_path)
        self.instancia = f"{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.run_id = new_run_id(FUENTE)
        self._lock = threading.Lock()
        self._seq = 0
        ts = utc_now_iso()
        day_dir = bronze_root(cfg) / ts[:10]
        day_dir.mkdir(parents=True, exist_ok=True)
        self.archivo = day_dir / f"recepciones_{self.instancia}.jsonl"
        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO etl_runs (run_id, fuente, ruta, version_esquema, modo, ts_ingesta, estado, bronze_path)"
                " VALUES (:r, :f, 'POST /eventos', :v, 'normal', :t, 'en_proceso', :b)"),
                {"r": self.run_id, "f": FUENTE, "v": VERSION, "t": ts, "b": str(self.archivo)})

    # ------------------------------------------------------------------ Bronze
    def _append_bronze(self, body: str) -> Recepcion:
        self._seq += 1
        rec = Recepcion(id_recepcion=f"R{time.time_ns()}-{self.instancia}-{self._seq:06d}",
                        ts_ingesta=utc_now_iso(), body=body,
                        archivo=str(self.archivo.relative_to(self.cfg.bronze_dir)))
        line = json.dumps({"id_recepcion": rec.id_recepcion, "ts_ingesta": rec.ts_ingesta,
                           "body_sha256": hashlib.sha256(body.encode()).hexdigest(), "body": body},
                          ensure_ascii=False)
        with self.archivo.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        return rec

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
        conn.execute(text("INSERT INTO web_recepciones (id_recepcion, archivo, ts_ingesta, id_evento, estado, detalle,"
                          " run_id) VALUES (:id, :a, :t, :e, :s, :d, :r)"),
                     {"id": rec.id_recepcion, "a": rec.archivo, "t": rec.ts_ingesta, "e": out.key, "s": estado,
                      "d": detalle, "r": self.run_id})
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
            rec = self._append_bronze(body_text)
            try:
                with self.engine.begin() as conn:
                    return self._process(conn, rec)
            except Exception as exc:                       # queda en Bronze sin confirmar -> recover()
                return {"id_recepcion": rec.id_recepcion, "ts_ingesta": rec.ts_ingesta,
                        "estado": "pendiente_silver", "detalle": repr(exc)}

    def recover(self) -> dict:
        """Carga idempotente de recepciones archivadas sin confirmación en Silver."""
        with self.engine.connect() as conn:
            confirmadas = {r[0] for r in conn.execute(text("SELECT id_recepcion FROM web_recepciones"))}
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
            n = dict(conn.execute(text("SELECT estado, COUNT(*) FROM web_recepciones WHERE run_id = :r GROUP BY estado"),
                                  {"r": self.run_id}).all())
            conn.execute(text(
                "UPDATE etl_runs SET estado = 'exitoso', ts_fin = :t, n_recibidos = :rec, n_excluidos = :dup,"
                " n_revision = :rev, n_rechazados = :rej, n_publicados = :pub WHERE run_id = :r"),
                {"t": utc_now_iso(), "rec": sum(n.values()), "dup": n.get("duplicada", 0), "rev": n.get("revision", 0),
                 "rej": n.get("rechazada", 0), "pub": n.get("insertada", 0), "r": self.run_id})
