"""Saldo de stock por SKU x ubicación x corte: provisional (Silver) y cierre (reproceso desde Bronze)."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from sqlalchemy import text

from tnr.common.batch import export_parquet, new_run_id
from tnr.common.contracts import SilverRecord
from tnr.common.db import get_engine
from tnr.common.eventos_web import parse_ts
from tnr.common.tipos import to_utc_iso, utc_now_iso
from tnr.config import Config
from tnr.ingest.movimientos_inventario import iter_recepciones, normalize_movimiento

VERSION_REGLAS = "stock_ubicacion.v1"


def corte_utc(corte: str, tz: str) -> str:
    dt, _ = parse_ts(corte, tz)
    if dt is None:
        raise ValueError(f"Corte inválido: {corte}")
    return to_utc_iso(dt)


def calcular(snapshots: list[dict], movimientos: Iterable[dict], corte: str) -> tuple[list[dict], list[dict]]:
    """Último snapshot válido al corte + movimientos con corte_snapshot < ts_evento <= corte, una vez cada uno."""
    base: dict[tuple, dict] = {}
    for s in snapshots:
        k = (s["id_producto"], s["id_ubicacion"])
        if s["fecha_corte"] <= corte and (k not in base or s["fecha_corte"] > base[k]["fecha_corte"]):
            base[k] = s
    acum = {k: {"delta": 0, "n": 0, "ultima": None} for k in base}
    pendientes = []
    vistos: set[str] = set()
    for m in movimientos:
        if m["id_evento"] in vistos or m["ts_evento"] > corte:
            continue
        vistos.add(m["id_evento"])
        k = (m["id_producto"], m["id_ubicacion"])
        if k not in base:
            pendientes.append(m)            # sin snapshot: pendiente de integración
            continue
        if m["ts_evento"] <= base[k]["fecha_corte"]:
            continue                        # ya incorporado en el snapshot
        a = acum[k]
        a["delta"] += m["delta_unidades"]
        a["n"] += 1
        a["ultima"] = max(filter(None, (a["ultima"], m["ts_ingesta"])))
    filas = []
    for k, s in sorted(base.items()):
        a = acum[k]
        inicial = s["stock_fisico"] - s["stock_reservado"]
        saldo = inicial + a["delta"]
        filas.append({"id_producto": k[0], "id_ubicacion": k[1], "fecha_corte": corte,
                      "corte_snapshot": s["fecha_corte"], "disponible_inicial": inicial,
                      "delta_movimientos": a["delta"], "n_movimientos": a["n"], "saldo_disponible": saldo,
                      "umbral_reposicion": s["umbral_reposicion"],
                      "alerta_reposicion": int(saldo <= s["umbral_reposicion"]),
                      "incidencia_saldo_negativo": int(saldo < 0), "ultima_ingesta_incorporada": a["ultima"]})
    return filas, pendientes


def _snapshots(conn) -> list[dict]:
    return [dict(r) for r in conn.execute(text("SELECT * FROM silver_inventario")).mappings()]


def stock_provisional(cfg: Config, corte: str) -> dict[str, Any]:
    c = corte_utc(corte, cfg.tz)
    with get_engine(cfg.warehouse_path).connect() as conn:
        movs = [dict(r) for r in conn.execute(text(
            "SELECT * FROM silver_movimientos_inventario ORDER BY ts_ingesta, id_recepcion")).mappings()]
        filas, pend = calcular(_snapshots(conn), movs, c)
        cierre = conn.execute(text("SELECT estado, version FROM ctl_cierres_stock WHERE fecha_corte = :c"),
                              {"c": c}).first()
    return {"corte": corte, "corte_utc": c, "estado": "provisional", "cierre_existente": list(cierre) if cierre else None,
            "filas": filas, "pendientes": [m["id_evento"] for m in pend]}


def close_stock(cfg: Config, corte: str, corte_ingesta: str | None = None) -> dict[str, Any]:
    c = corte_utc(corte, cfg.tz)
    ci = corte_utc(corte_ingesta, cfg.tz) if corte_ingesta else utc_now_iso()
    # Reprocesar movimientos archivados en Bronze hasta el corte de ingesta (gana la primera recepción)
    movs, conflictos, leidas = {}, set(), 0
    for rec in iter_recepciones(cfg, ci):
        leidas += 1
        try:
            payload = json.loads(rec.body)
        except json.JSONDecodeError:
            continue
        out = normalize_movimiento(payload, tz=cfg.tz, ref=rec.id_recepcion, ts_ingesta=rec.ts_ingesta,
                                   id_recepcion=rec.id_recepcion)
        if not isinstance(out, SilverRecord):
            continue
        if out.key not in movs:
            movs[out.key] = out.data
        elif movs[out.key]["payload_sha256"] != out.data["payload_sha256"]:
            conflictos.add(out.key)

    engine = get_engine(cfg.warehouse_path)
    run_id = new_run_id("stock_cierre")
    ts = utc_now_iso()
    with engine.begin() as conn:
        filas, pend = calcular(_snapshots(conn), movs.values(), c)
        version = (conn.execute(text("SELECT version FROM ctl_cierres_stock WHERE fecha_corte = :c"),
                                {"c": c}).scalar() or 0) + 1
        conn.execute(text(
            "INSERT INTO etl_runs (run_id, fuente, ruta, version_esquema, modo, ts_ingesta, ts_fin, estado,"
            " n_recibidos, n_publicados, n_revision, detalle) VALUES (:r, 'stock_cierre', :ru, :v, 'reproceso', :t,"
            " :t, 'exitoso', :n, :p, :cf, :d)"),
            {"r": run_id, "ru": f"bronze/movimientos_inventario <= {ci}", "v": VERSION_REGLAS, "t": ts, "n": leidas,
             "p": len(filas), "cf": len(conflictos),
             "d": json.dumps({"fecha_corte": c, "pendientes": [m["id_evento"] for m in pend],
                              "conflictos": sorted(conflictos)})})
        conn.execute(text("DELETE FROM gold_mart_stock_ubicacion WHERE fecha_corte = :c"), {"c": c})
        if filas:
            cols = list(filas[0]) + ["corte_ingesta", "version", "run_id"]
            conn.execute(text(f"INSERT INTO gold_mart_stock_ubicacion ({', '.join(cols)})"
                              f" VALUES ({', '.join(':' + k for k in cols)})"),
                         [{**f, "corte_ingesta": ci, "version": version, "run_id": run_id} for f in filas])
        tardio = conn.execute(text(
            "SELECT MAX(ts_ingesta) FROM silver_movimientos_inventario WHERE ts_evento <= :c AND ts_ingesta > :ci"),
            {"c": c, "ci": ci}).scalar()
        conn.execute(text(
            "INSERT INTO ctl_cierres_stock (fecha_corte, estado, version, corte_ingesta, ts_cierre, run_id, n_pendientes,"
            " ts_ultimo_evento_tardio) VALUES (:c, :e, :v, :ci, :t, :r, :n, :tt) ON CONFLICT (fecha_corte) DO UPDATE SET"
            " estado = excluded.estado, version = excluded.version, corte_ingesta = excluded.corte_ingesta,"
            " ts_cierre = excluded.ts_cierre, run_id = excluded.run_id, n_pendientes = excluded.n_pendientes,"
            " ts_ultimo_evento_tardio = excluded.ts_ultimo_evento_tardio"),
            {"c": c, "e": "reproceso_pendiente" if tardio else "cerrada", "v": version, "ci": ci, "t": ts, "r": run_id,
             "n": len(pend), "tt": tardio})
        path = export_parquet(cfg, conn, "gold_mart_stock_ubicacion", "gold", run_id, "WHERE fecha_corte = :c", {"c": c})
        conn.execute(text("UPDATE etl_runs SET parquet_path = :p WHERE run_id = :r"), {"p": path, "r": run_id})
    zone = ZoneInfo(cfg.tz)
    return {"run_id": run_id, "fecha_corte": c,
            "fecha_corte_local": datetime.fromisoformat(c.replace("Z", "+00:00")).astimezone(zone).isoformat(),
            "version": version, "corte_ingesta": ci, "recepciones_leidas": leidas,
            "estado": "reproceso_pendiente" if tardio else "cerrada", "pendientes": [m["id_evento"] for m in pend],
            "conflictos": sorted(conflictos), "filas": filas, "parquet_path": path}
