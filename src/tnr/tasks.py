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
    if fuente == "clientes":
        from tnr.ingest.clientes import ClientesSource
        return ClientesSource()
    if fuente == "despachos":
        from tnr.ingest.despachos import DespachosSource
        return DespachosSource()
    if fuente == "notas_credito":
        from tnr.ingest.notas_credito import NotasCreditoSource
        return NotasCreditoSource()
    if fuente == "postventa":
        from tnr.ingest.postventa import PostventaSource
        return PostventaSource()
    if fuente == "web_historico":
        from tnr.ingest.web_historico import WebHistoricoSource
        return WebHistoricoSource()
    if fuente in FUENTES:
        raise NotImplementedError(f"La ingesta '{fuente}' se implementa en una fase posterior")
    raise ValueError(f"Fuente desconocida: {fuente}")


def ingest(fuente: str, *, input_kind: str | None = None, force: bool = False, reproceso: bool = False,
           cfg: Config | None = None) -> dict[str, Any]:
    cfg = cfg or Config.from_env()
    if fuente == "web_stream":      # carga continua: recuperar recepciones de Bronze sin confirmar
        return recover_web(cfg)
    return run_batch(_source(fuente, input_kind=input_kind), cfg, force=force, reproceso=reproceso).to_dict()


def publish_comercial(corte: str | None = None, cfg: Config | None = None) -> dict[str, Any]:
    from tnr.gold.comercial import publish_comercial as _publish
    cfg = cfg or Config.from_env()
    return _publish(cfg, corte or cfg.corte)


def recover_web(cfg: Config | None = None) -> dict[str, Any]:
    from tnr.ingest.web_stream import WebStreamReceiver
    receiver = WebStreamReceiver(cfg or Config.from_env())
    try:
        return receiver.recover()
    finally:
        receiver.close()


def close_web(minuto: str, corte_ingesta: str | None = None, cfg: Config | None = None) -> dict[str, Any]:
    from tnr.gold.web import close_minute
    return close_minute(cfg or Config.from_env(), minuto, corte_ingesta)


def serving_web(desde: str, hasta: str, cfg: Config | None = None) -> dict[str, Any]:
    from tnr.gold.web import leer_serving
    return leer_serving(cfg or Config.from_env(), desde, hasta)
