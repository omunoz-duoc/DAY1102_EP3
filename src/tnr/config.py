"""Configuración del proyecto. Todo se puede sobrescribir con variables TNR_*."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

_DEFAULT_ROOT = Path(__file__).resolve().parents[2]


def _path(var: str, default: Path) -> Path:
    value = os.environ.get(var)
    return Path(value).resolve() if value else default


@dataclass(frozen=True)
class Config:
    root: Path
    input_dir: Path
    bronze_dir: Path
    parquet_dir: Path
    warehouse_path: Path
    source_db_path: Path
    sql_dir: Path
    postventa_url: str
    tz: str
    corte: str
    sim_inicio: str
    sim_fin: str

    @classmethod
    def from_env(cls) -> "Config":
        root = _path("TNR_ROOT", _DEFAULT_ROOT)
        data = root / "data"
        return cls(
            root=root,
            input_dir=_path("TNR_INPUT_DIR", data),
            bronze_dir=_path("TNR_BRONZE_DIR", data / "bronze"),
            parquet_dir=_path("TNR_PARQUET_DIR", data / "parquet"),
            warehouse_path=_path("TNR_WAREHOUSE_PATH", root / "warehouse.sqlite"),
            source_db_path=_path("TNR_SOURCE_DB", data / "ventas_origen.sqlite"),
            sql_dir=_path("TNR_SQL_DIR", root / "sql"),
            postventa_url=os.environ.get("TNR_POSTVENTA_URL", "http://127.0.0.1:8766/ordenes"),
            tz=os.environ.get("TNR_TZ", "America/Santiago"),
            corte=os.environ.get("TNR_CORTE", "2025-12-31"),
            sim_inicio=os.environ.get("TNR_SIM_INICIO", "2025-12-26T12:59:00-03:00"),
            sim_fin=os.environ.get("TNR_SIM_FIN", "2025-12-26T13:10:00-03:00"),
        )
