"""Logs web históricos (arreglo JSON anidado) -> silver_eventos_web con origen='historico'."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from sqlalchemy import Connection, text

from tnr.common.batch import Acquired, dedup_by_key
from tnr.common.contracts import DedupResult, RawRecord, Rejection, SilverRecord
from tnr.common.eventos_web import insert_eventos, normalize_evento
from tnr.common.tipos import utc_now_iso
from tnr.config import Config

ARCHIVO = "logs_web_tnr.json"


class WebHistoricoSource:
    fuente = "web_historico"
    version_esquema = "eventos_web.v1"
    silver_table = "silver_eventos_web"

    def __init__(self, tz: str = "America/Santiago"):
        self.tz = tz
        self.ts_ingesta = utc_now_iso()

    def acquire(self, cfg: Config) -> Acquired:
        self.tz = cfg.tz
        path = cfg.input_dir / ARCHIVO
        return Acquired(files={path.name: path.read_bytes()}, ruta=str(path))

    def extract(self, cfg: Config, bronze_dir: Path) -> Iterable[RawRecord]:
        self.tz = cfg.tz
        eventos = json.loads((bronze_dir / ARCHIVO).read_text(encoding="utf-8"))
        for i, ev in enumerate(eventos):
            key = str(ev.get("id_evento") or "").strip() if isinstance(ev, dict) else ""
            yield RawRecord(ref=f"{ARCHIVO}#[{i}]", data=ev if isinstance(ev, dict) else {"_valor": ev}, key=key)

    def deduplicate(self, raws: list[RawRecord]) -> DedupResult:
        return self._dedup(raws)

    @staticmethod
    def _dedup(raws: list[RawRecord]) -> DedupResult:
        # los valores anidados se comparan serializados
        flat = [RawRecord(r.ref, {k: json.dumps(v, sort_keys=True) for k, v in r.data.items()}, r.key) for r in raws]
        res = dedup_by_key(flat)
        by_ref = {r.ref: r for r in raws}
        res.kept = [by_ref[r.ref] for r in res.kept]
        return res

    def normalize(self, record: RawRecord) -> SilverRecord | Rejection:
        return normalize_evento(record.data, origen="historico", tz=self.tz, ref=record.ref,
                                ts_ingesta=self.ts_ingesta)

    def persist(self, conn: Connection, records: list[SilverRecord], run_id: str) -> int:
        # recarga atómica de la partición histórica; los eventos del stream no se tocan
        conn.execute(text("DELETE FROM silver_eventos_web WHERE origen = 'historico'"))
        insert_eventos(conn, records, run_id)
        return len(records)
