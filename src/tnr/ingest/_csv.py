"""Base para fuentes CSV simples (separador ';', UTF-8) publicadas como snapshot completo."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from sqlalchemy import Connection, text

from tnr.common.batch import Acquired, dedup_by_key
from tnr.common.contracts import DedupResult, RawRecord, SilverRecord
from tnr.config import Config


class CsvSnapshotSource:
    fuente: str
    version_esquema: str
    silver_table: str
    archivo: str
    key_field: str
    silver_cols: list[str]

    def acquire(self, cfg: Config) -> Acquired:
        path = cfg.input_dir / self.archivo
        return Acquired(files={path.name: path.read_bytes()}, ruta=str(path))

    def extract(self, cfg: Config, bronze_dir: Path) -> Iterable[RawRecord]:
        with (bronze_dir / self.archivo).open(encoding="utf-8", newline="") as fh:
            for i, row in enumerate(csv.DictReader(fh, delimiter=";"), start=2):  # fila 1 = encabezado
                yield RawRecord(ref=f"{self.archivo}#fila={i}", data=row,
                                key=(row.get(self.key_field) or "").strip().upper())

    def deduplicate(self, raws: list[RawRecord]) -> DedupResult:
        return dedup_by_key(raws)

    def persist(self, conn: Connection, records: list[SilverRecord], run_id: str) -> int:
        conn.execute(text(f"DELETE FROM {self.silver_table}"))
        cols = self.silver_cols + ["run_id"]
        if records:
            conn.execute(text(f"INSERT INTO {self.silver_table} ({', '.join(cols)})"
                              f" VALUES ({', '.join(':' + c for c in cols)})"),
                         [{**r.data, "run_id": run_id} for r in records])
        return len(records)
