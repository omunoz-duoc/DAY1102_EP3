"""Tareas invocables (CLI y orquestador). Cada una es idempotente y devuelve un dict serializable a JSON,
apto para XCom de Airflow (PythonOperator) o para `python -m tnr ...` (BashOperator)."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from tnr.common.batch import run_batch
from tnr.common.db import get_engine, run_script
from tnr.config import Config

FUENTES = ("ventas", "productos", "clientes", "despachos", "notas_credito", "postventa", "inventario",
           "movimientos_inventario", "web_historico", "web_stream")


def init(cfg: Config | None = None) -> dict[str, Any]:
    cfg = cfg or Config.from_env()
    for d in (cfg.bronze_dir, cfg.parquet_dir):
        d.mkdir(parents=True, exist_ok=True)
    engine = get_engine(cfg.warehouse_path)
    for sql in sorted(Path(cfg.sql_dir).glob("schema*.sql")):
        run_script(engine, sql.read_text(encoding="utf-8"))
    return {"warehouse": str(cfg.warehouse_path), "bronze": str(cfg.bronze_dir), "parquet": str(cfg.parquet_dir)}


def _source(fuente: str, **opts):
    if fuente == "ventas":
        from tnr.ingest.ventas import VentasSource
        return VentasSource(opts.get("input_kind") or "sql")
    if fuente == "productos":
        from tnr.ingest.productos import ProductosSource
        return ProductosSource()
    if fuente in FUENTES:
        raise NotImplementedError(f"La ingesta '{fuente}' se implementa en una fase posterior")
    raise ValueError(f"Fuente desconocida: {fuente}")


def ingest(fuente: str, *, input_kind: str | None = None, force: bool = False, reproceso: bool = False,
           cfg: Config | None = None) -> dict[str, Any]:
    cfg = cfg or Config.from_env()
    return run_batch(_source(fuente, input_kind=input_kind), cfg, force=force, reproceso=reproceso).to_dict()


def publish_comercial(corte: str | None = None, cfg: Config | None = None) -> dict[str, Any]:
    from tnr.gold.comercial import publish_comercial as _publish
    cfg = cfg or Config.from_env()
    return _publish(cfg, corte or cfg.corte)
