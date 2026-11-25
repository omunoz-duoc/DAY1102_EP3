"""Consultas de los dashboards (solo lectura). Separadas de la UI para poder probarlas."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text

from tnr.common.db import get_read_engine
from tnr.config import Config


def _df(cfg: Config, sql: str, **params) -> pd.DataFrame:
    with get_read_engine(cfg.warehouse_path).connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


# ------------------------------------------------------------------ comercial (Gold)
def cortes_publicados(cfg: Config) -> list[str]:
    return _df(cfg, "SELECT DISTINCT fecha_corte FROM gold_mart_ventas ORDER BY fecha_corte DESC")["fecha_corte"].tolist()


def kpis_comerciales(cfg: Config, corte: str) -> dict[str, Any]:
    r = _df(cfg, "SELECT COUNT(*) AS ventas, SUM(monto_bruto_clp) AS bruto, SUM(monto_notas_clp) AS notas,"
                 " SUM(monto_neto_clp) AS neto, SUM(customer_id IS NULL) AS sin_cliente,"
                 " SUM(CASE WHEN customer_id IS NULL THEN monto_neto_clp ELSE 0 END) AS neto_sin_cliente"
                 " FROM gold_mart_ventas WHERE fecha_corte = :c", c=corte).iloc[0].to_dict()
    pub = _df(cfg, "SELECT publish_id, version_reglas, ts_publicacion FROM gold_publicaciones"
                   " WHERE fecha_corte = :c AND mart = 'comercial' ORDER BY ts_publicacion DESC LIMIT 1", c=corte)
    r["publicacion"] = pub.iloc[0].to_dict() if len(pub) else None
    return r


def conciliacion(cfg: Config, corte: str) -> pd.DataFrame:
    return _df(cfg, "SELECT escenario, orden, etapa, filas, monto_clp, descripcion FROM gold_mart_conciliacion_ventas"
                    " WHERE fecha_corte = :c ORDER BY escenario, orden", c=corte)


def ventas_canal_modalidad(cfg: Config, corte: str) -> pd.DataFrame:
    return _df(cfg, "SELECT canal, modalidad_entrega, COUNT(*) AS ventas, SUM(monto_neto_clp) AS neto_clp"
                    " FROM gold_mart_ventas WHERE fecha_corte = :c GROUP BY canal, modalidad_entrega"
                    " ORDER BY neto_clp DESC", c=corte)


def clientes(cfg: Config, corte: str, limite: int = 3000) -> list[str]:
    return _df(cfg, "SELECT customer_id FROM gold_mart_cliente_360 WHERE fecha_corte = :c"
                    " ORDER BY monto_neto_clp DESC LIMIT :l", c=corte, l=limite)["customer_id"].tolist()


def perfil_cliente(cfg: Config, corte: str, customer_id: str) -> dict[str, Any] | None:
    df = _df(cfg, "SELECT * FROM gold_mart_cliente_360 WHERE fecha_corte = :c AND customer_id = :id",
             c=corte, id=customer_id)
    return df.iloc[0].to_dict() if len(df) else None


# ------------------------------------------------------------------ operaciones (Serving)
def _utc(ts_local: str, tz: str) -> str:
    dt = datetime.fromisoformat(ts_local)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(tz))
    return dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:00Z")


def checkout(cfg: Config, desde: str, hasta: str) -> dict[str, Any]:
    """Serving por minuto y dispositivo + agregados globales del intervalo (sumar antes de dividir)."""
    lo, hi = _utc(desde, cfg.tz), _utc(hasta, cfg.tz)
    filas = _df(cfg, "SELECT * FROM serving_checkout_minuto WHERE minuto_utc >= :lo AND minuto_utc < :hi"
                     " ORDER BY minuto_utc, dispositivo", lo=lo, hi=hi)
    sesiones = _df(cfg, "SELECT COUNT(DISTINCT id_sesion) AS n FROM silver_eventos_web WHERE tipo = 'checkout_request'"
                        " AND codigo_http BETWEEN 500 AND 599 AND ts_evento >= :lo AND ts_evento < :hi",
                   lo=lo, hi=hi)["n"].iloc[0]
    zone = ZoneInfo(cfg.tz)
    if len(filas):
        filas["minuto_local"] = pd.to_datetime(filas["minuto_utc"], utc=True).dt.tz_convert(zone)
    intentos, errores = int(filas["intentos"].sum()), int(filas["errores_5xx"].sum())
    lat_n = int(filas["latencia_n"].sum())
    return {"filas": filas, "intentos": intentos, "errores_5xx": errores,
            "tasa_error": errores / intentos if intentos else None, "sesiones_afectadas": int(sesiones),
            "latencia_promedio_ms": float(filas["latencia_suma_ms"].fillna(0).sum()) / lat_n if lat_n else None,
            "ultima_actualizacion": filas["ultima_actualizacion"].max() if len(filas) else None}


def stock(cfg: Config, corte: str) -> dict[str, Any]:
    """Cierre publicado si existe y no está pendiente; si no, saldo provisional calculado desde Silver."""
    from tnr.gold.stock import calcular, corte_utc
    c = corte_utc(corte, cfg.tz)
    cierre = _df(cfg, "SELECT * FROM ctl_cierres_stock WHERE fecha_corte = :c", c=c)
    if len(cierre) and cierre["estado"].iloc[0] == "cerrada":
        filas = _df(cfg, "SELECT * FROM gold_mart_stock_ubicacion WHERE fecha_corte = :c"
                         " ORDER BY id_producto, id_ubicacion", c=c)
        return {"estado": "cerrada", "version": int(cierre["version"].iloc[0]), "filas": filas, "pendientes": []}
    snaps = _df(cfg, "SELECT * FROM silver_inventario").to_dict("records")
    movs = _df(cfg, "SELECT * FROM silver_movimientos_inventario ORDER BY ts_ingesta, id_recepcion").to_dict("records")
    filas, pend = calcular(snaps, movs, c)
    estado = "reproceso_pendiente" if len(cierre) else "provisional"
    return {"estado": estado, "version": int(cierre["version"].iloc[0]) if len(cierre) else None,
            "filas": pd.DataFrame(filas), "pendientes": [m["id_evento"] for m in pend]}
