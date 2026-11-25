from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
from sqlalchemy import text

from tnr import tasks
from tnr.common.db import get_engine
from tnr.config import Config
from tests.postventa_server import FakePostventa

ROOT = Path(__file__).resolve().parents[1]
CORTE = "2025-12-31"


def make_cfg(base: Path, **overrides) -> Config:
    return dataclasses.replace(
        Config.from_env(),
        root=ROOT, input_dir=ROOT / "data", sql_dir=ROOT / "sql",
        bronze_dir=base / "bronze", parquet_dir=base / "parquet",
        warehouse_path=base / "warehouse.sqlite", source_db_path=base / "ventas_origen.sqlite",
        **overrides,
    )


def query(cfg: Config, sql: str, **params):
    with get_engine(cfg.warehouse_path).connect() as conn:
        return conn.execute(text(sql), params).all()


@pytest.fixture(scope="session")
def postventa_server():
    srv = FakePostventa(FakePostventa.pages_from(ROOT / "data"), fallas={"2": 2})  # 2 x 503 en la página 2
    yield srv
    srv.close()


@pytest.fixture(scope="session")
def cfg(tmp_path_factory, postventa_server) -> Config:
    c = make_cfg(tmp_path_factory.mktemp("tnr"), postventa_url=postventa_server.url)
    tasks.init(c)
    return c


@pytest.fixture(scope="session")
def comercial(cfg):
    """Fases 1 y 2: todas las fuentes comerciales + publicación al corte."""
    out = {f: tasks.ingest(f, cfg=cfg) for f in
           ("ventas", "productos", "clientes", "despachos", "notas_credito", "postventa")}
    out["publish"] = tasks.publish_comercial(CORTE, cfg=cfg)
    return out


@pytest.fixture
def q(cfg):
    return lambda sql, **params: query(cfg, sql, **params)
