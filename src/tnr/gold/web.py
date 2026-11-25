"""Cierre de ventanas de checkout desde Bronze y lectura de Serving (precedencia Gold > Silver)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

from tnr.common.batch import export_parquet, new_run_id
from tnr.common.contracts import SilverRecord
from tnr.common.db import get_engine
from tnr.common.eventos_web import normalize_evento
from tnr.common.tipos import to_utc_iso, utc_now_iso
from tnr.config import Config
from tnr.ingest.web_stream import iter_recepciones

VERSION_REGLAS = "checkout_minuto.v1"


def _minuto_utc(minuto: str, tz: str) -> datetime:
    dt = datetime.fromisoformat(minuto.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(tz))
    dt = dt.astimezone(timezone.utc)
    if dt.second or dt.microsecond:
        raise ValueError("El minuto debe empezar en el segundo 0")
    return dt


def close_minute(cfg: Config, minuto: str, corte_ingesta: str | None = None) -> dict[str, Any]:
    inicio = _minuto_utc(minuto, cfg.tz)
    fin = inicio + timedelta(minutes=1)                       # [inicio, fin)
    key = to_utc_iso(inicio)[:16] + ":00Z"
    corte = corte_ingesta or utc_now_iso()
    if corte_ingesta and not corte_ingesta.endswith("Z"):
        corte = to_utc_iso(datetime.fromisoformat(corte_ingesta))

    # 1. Releer Bronze hasta el corte y aplicar las reglas compartidas
    leidas, validos = 0, {}
    conflictos: list[str] = []
    for rec in iter_recepciones(cfg, corte):
        leidas += 1
        try:
            payload = json.loads(rec.body)
        except json.JSONDecodeError:
            continue
        out = normalize_evento(payload, origen="stream", tz=cfg.tz, ref=rec.id_recepcion,
                               ts_ingesta=rec.ts_ingesta, id_recepcion=rec.id_recepcion)
        if not isinstance(out, SilverRecord):
            continue
        # 2. Resolver id_evento globalmente (antes de filtrar el minuto): gana la primera recepción
        prev = validos.get(out.key)
        if prev is None:
            validos[out.key] = out
        elif prev.data["payload_sha256"] != out.data["payload_sha256"]:
            conflictos.append(out.key)

    # 3. Filtrar el minuto de negocio y agregar por dispositivo
    lo, hi = to_utc_iso(inicio), to_utc_iso(fin)
    agg: dict[str, dict] = {}
    for ev in validos.values():
        d = ev.data
        if not (lo <= d["ts_evento"] < hi) or d["tipo"] != "checkout_request" or d["codigo_http"] is None:
            continue
        a = agg.setdefault(d["dispositivo"], {"intentos": 0, "errores": 0, "sesiones": set(), "lat_suma": 0, "lat_n": 0})
        a["intentos"] += 1
        if 500 <= d["codigo_http"] <= 599:
            a["errores"] += 1
            a["sesiones"].add(d["id_sesion"])
        if d["latencia_ms"] is not None:
            a["lat_suma"] += d["latencia_ms"]
            a["lat_n"] += 1

    engine = get_engine(cfg.warehouse_path)
    run_id = new_run_id("web_cierre")
    ts = utc_now_iso()
    with engine.begin() as conn:
        version = (conn.execute(text("SELECT version FROM ctl_ventanas_web WHERE minuto_utc = :m"),
                                {"m": key}).scalar() or 0) + 1
        conn.execute(text(
            "INSERT INTO etl_runs (run_id, fuente, ruta, version_esquema, modo, ts_ingesta, ts_fin, estado,"
            " n_recibidos, n_publicados, n_revision, detalle) VALUES (:r, 'web_cierre', :ru, :v, 'reproceso', :t, :t,"
            " 'exitoso', :n, :p, :c, :d)"),
            {"r": run_id, "ru": f"bronze/web_stream <= {corte}", "v": VERSION_REGLAS, "t": ts, "n": leidas,
             "p": len(agg), "c": len(set(conflictos)),
             "d": json.dumps({"minuto_utc": key, "minuto_solicitado": minuto, "conflictos": sorted(set(conflictos))})})
        conn.execute(text("DELETE FROM gold_mart_checkout_minuto WHERE minuto_utc = :m"), {"m": key})
        if agg:
            conn.execute(text(
                "INSERT INTO gold_mart_checkout_minuto (minuto_utc, dispositivo, intentos, errores_5xx,"
                " sesiones_afectadas, latencia_suma_ms, latencia_n, version, corte_ingesta, run_id)"
                " VALUES (:m, :d, :i, :e, :s, :ls, :ln, :v, :c, :r)"),
                [{"m": key, "d": dev, "i": a["intentos"], "e": a["errores"], "s": len(a["sesiones"]),
                  "ls": a["lat_suma"] if a["lat_n"] else None, "ln": a["lat_n"], "v": version, "c": corte, "r": run_id}
                 for dev, a in sorted(agg.items())])
        # si ya hay en Silver eventos de este minuto recibidos después del corte, la ventana nace pendiente
        tardios = conn.execute(text(
            "SELECT MAX(ts_ingesta) FROM silver_eventos_web WHERE origen = 'stream' AND ts_evento >= :lo"
            " AND ts_evento < :hi AND ts_ingesta > :c"), {"lo": lo, "hi": hi, "c": corte}).scalar()
        conn.execute(text(
            "INSERT INTO ctl_ventanas_web (minuto_utc, estado, version, corte_ingesta, ts_cierre, run_id,"
            " n_recepciones_leidas, ts_ultimo_evento_tardio) VALUES (:m, :e, :v, :c, :t, :r, :n, :tt)"
            " ON CONFLICT (minuto_utc) DO UPDATE SET estado = excluded.estado, version = excluded.version,"
            " corte_ingesta = excluded.corte_ingesta, ts_cierre = excluded.ts_cierre, run_id = excluded.run_id,"
            " n_recepciones_leidas = excluded.n_recepciones_leidas,"
            " ts_ultimo_evento_tardio = excluded.ts_ultimo_evento_tardio"),
            {"m": key, "e": "reproceso_pendiente" if tardios else "cerrada", "v": version, "c": corte, "t": ts,
             "r": run_id, "n": leidas, "tt": tardios})
        path = export_parquet(cfg, conn, "gold_mart_checkout_minuto", "gold", run_id,
                              "WHERE minuto_utc = :m", {"m": key})
        conn.execute(text("UPDATE etl_runs SET parquet_path = :p WHERE run_id = :r"), {"p": path, "r": run_id})

    intentos = sum(a["intentos"] for a in agg.values())
    errores = sum(a["errores"] for a in agg.values())
    return {"run_id": run_id, "minuto_utc": key, "version": version, "corte_ingesta": corte,
            "recepciones_leidas": leidas, "intentos": intentos, "errores_5xx": errores,
            "tasa_error": round(errores / intentos, 6) if intentos else None,
            "por_dispositivo": {d: {"intentos": a["intentos"], "errores_5xx": a["errores"]} for d, a in agg.items()},
            "estado_ventana": "reproceso_pendiente" if tardios else "cerrada", "parquet_path": path}


def leer_serving(cfg: Config, desde: str, hasta: str) -> dict[str, Any]:
    """Tasa global del intervalo [desde, hasta): suma errores e intentos antes de dividir.
    Sesiones únicas con COUNT(DISTINCT id_sesion) sobre el intervalo completo."""
    lo = to_utc_iso(_minuto_utc(desde, cfg.tz))
    hi = to_utc_iso(_minuto_utc(hasta, cfg.tz))
    with get_engine(cfg.warehouse_path).connect() as conn:
        filas = [dict(r) for r in conn.execute(text(
            "SELECT * FROM serving_checkout_minuto WHERE minuto_utc >= :lo AND minuto_utc < :hi"
            " ORDER BY minuto_utc, dispositivo"), {"lo": lo, "hi": hi}).mappings()]
        sesiones = conn.execute(text(
            "SELECT COUNT(DISTINCT id_sesion) FROM silver_eventos_web WHERE tipo = 'checkout_request'"
            " AND codigo_http BETWEEN 500 AND 599 AND ts_evento >= :lo AND ts_evento < :hi"),
            {"lo": lo, "hi": hi}).scalar_one()
    intentos = sum(f["intentos"] for f in filas)
    errores = sum(f["errores_5xx"] for f in filas)
    lat_n = sum(f["latencia_n"] for f in filas)
    zone = ZoneInfo(cfg.tz)
    for f in filas:   # hora local al servir
        f["minuto_local"] = datetime.fromisoformat(f["minuto_utc"].replace("Z", "+00:00")).astimezone(zone).isoformat()
    return {"desde": desde, "hasta": hasta, "intentos": intentos, "errores_5xx": errores,
            "tasa_error": round(errores / intentos, 6) if intentos else None,
            "latencia_promedio_ms": round(sum(f["latencia_suma_ms"] or 0 for f in filas) / lat_n, 1) if lat_n else None,
            "sesiones_afectadas": sesiones, "filas": filas}
