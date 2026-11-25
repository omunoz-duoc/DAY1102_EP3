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
    if fuente == "inventario":
        from tnr.ingest.inventario import InventarioSource
        return InventarioSource()
    if fuente == "web_historico":
        from tnr.ingest.web_historico import WebHistoricoSource
        return WebHistoricoSource()
    if fuente in FUENTES:
        raise NotImplementedError(f"La ingesta '{fuente}' se implementa en una fase posterior")
    raise ValueError(f"Fuente desconocida: {fuente}")


def ingest(fuente: str, *, input_kind: str | None = None, force: bool = False, reproceso: bool = False,
           file: str | None = None, cfg: Config | None = None) -> dict[str, Any]:
    cfg = cfg or Config.from_env()
    if fuente == "web_stream":      # carga continua: recuperar recepciones de Bronze sin confirmar
        return recover_web(cfg)
    if fuente == "movimientos_inventario":   # recuperar pendientes y consumir el archivo completo
        return consume_movimientos(file or "movimientos_inventario.jsonl", cfg=cfg, recuperar=True)
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


def _resolve_input(path: str, cfg: Config) -> Path:
    p = Path(path)
    return p if p.exists() else cfg.input_dir / path


def consume_movimientos(file: str, *, desde: int = 1, hasta: int | None = None, recuperar: bool = False,
                        cfg: Config | None = None) -> dict[str, Any]:
    from tnr.ingest.movimientos_inventario import MovimientosConsumer
    cfg = cfg or Config.from_env()
    consumer = MovimientosConsumer(cfg)
    try:
        recuperadas = consumer.recover() if recuperar else None
        res = consumer.consume_file(_resolve_input(file, cfg), desde=desde, hasta=hasta)
    finally:
        consumer.close()
    estados: dict[str, int] = {}
    for r in res:
        estados[r["estado"]] = estados.get(r["estado"], 0) + 1
    return {"run_id": consumer.run_id, "recepciones": len(res), "estados": estados, "recuperacion": recuperadas,
            "detalle": res}


def close_stock(corte: str, corte_ingesta: str | None = None, cfg: Config | None = None) -> dict[str, Any]:
    from tnr.gold.stock import close_stock as _close
    return _close(cfg or Config.from_env(), corte, corte_ingesta)


def stock_provisional(corte: str, cfg: Config | None = None) -> dict[str, Any]:
    from tnr.gold.stock import stock_provisional as _prov
    return _prov(cfg or Config.from_env(), corte)
