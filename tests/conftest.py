from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
from sqlalchemy import text

from tnr import tasks
from tnr.common.db import get_engine
from tnr.config import Config

ROOT = Path(__file__).resolve().parents[1]


def make_cfg(base: Path) -> Config:
    return dataclasses.replace(
        Config.from_env(),
        root=ROOT, input_dir=ROOT / "data", sql_dir=ROOT / "sql",
        bronze_dir=base / "bronze", parquet_dir=base / "parquet",
        warehouse_path=base / "warehouse.sqlite", source_db_path=base / "ventas_origen.sqlite",
    )


@pytest.fixture(scope="session")
def cfg(tmp_path_factory) -> Config:
    c = make_cfg(tmp_path_factory.mktemp("tnr"))
    tasks.init(c)
    return c


@pytest.fixture(scope="session")
def comercial(cfg):
    """Fase 1: ventas (SQL) + productos + publicación comercial."""
    out = {
        "ventas": tasks.ingest("ventas", input_kind="sql", cfg=cfg),
        "productos": tasks.ingest("productos", cfg=cfg),
    }
    out["publish"] = tasks.publish_comercial("2025-12-31", cfg=cfg)
    return out


@pytest.fixture
def q(cfg):
    engine = get_engine(cfg.warehouse_path)

    def _q(sql: str, **params):
        with engine.connect() as conn:
            return conn.execute(text(sql), params).all()
    return _q
