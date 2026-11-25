"""Publicación comercial: gold_mart_ventas (Cliente 360 y conciliación se agregan en la fase 2)."""
from __future__ import annotations

import json
from datetime import date
from typing import Any

from sqlalchemy import Connection, text

from tnr.common.batch import export_parquet, new_run_id
from tnr.common.db import get_engine
from tnr.common.tipos import utc_now_iso
from tnr.config import Config

VERSION_REGLAS = "comercial.v1"
REQUERIDAS = ("silver_ventas", "silver_productos")
OPCIONALES = ("silver_despachos", "silver_notas_credito", "ident_rut_customer")


class DependenciaFaltante(RuntimeError):
    pass


def _run_ids(conn: Connection) -> dict[str, list[str]]:
    out = {}
    for t in REQUERIDAS + OPCIONALES:
        out[t] = [r[0] for r in conn.execute(text(f"SELECT DISTINCT run_id FROM {t} ORDER BY run_id"))]
    return out


def evaluar_notas(conn: Connection, corte: str) -> list[dict[str, Any]]:
    """Evalúa cada nota en orden de fecha e ID contra su venta de origen."""
    rows = conn.execute(text(
        "SELECT n.id_nota, n.id_venta_origen, n.fecha_emision, n.monto_clp,"
        "       v.fecha AS fecha_venta, v.estado_venta, v.cantidad * v.precio_unitario_clp AS bruto"
        " FROM silver_notas_credito n LEFT JOIN silver_ventas v ON v.id_venta = n.id_venta_origen"
        " ORDER BY n.fecha_emision, n.id_nota")).mappings().all()
    aplicado: dict[str, int] = {}
    out = []
    for n in rows:
        if n["fecha_emision"] > corte:
            estado = "pendiente_fuera_de_corte"
        elif n["fecha_venta"] is None:
            estado = "huerfana"
        elif n["fecha_venta"] > corte:
            estado = "pendiente_venta_fuera_de_corte"
        elif n["estado_venta"] != "confirmada":
            estado = "rechazada_venta_no_confirmada"
        elif n["fecha_emision"] < n["fecha_venta"]:
            estado = "rechazada_fecha_anterior_a_venta"
        elif aplicado.get(n["id_venta_origen"], 0) + n["monto_clp"] > n["bruto"]:
            estado = "rechazada_excede_bruto"
        else:
            estado = "aplicada"
            aplicado[n["id_venta_origen"]] = aplicado.get(n["id_venta_origen"], 0) + n["monto_clp"]
        out.append({"id_nota": n["id_nota"], "id_venta_origen": n["id_venta_origen"],
                    "fecha_emision": n["fecha_emision"], "monto_clp": n["monto_clp"], "estado": estado})
    return out


SQL_GOLD_VENTAS = """
INSERT INTO gold_mart_ventas (
    fecha_corte, id_venta, fecha, customer_id, id_producto, categoria, subcategoria, canal, modalidad_entrega,
    id_tienda, id_ubicacion_venta, id_ubicacion_operativa, n_despachos, cantidad, precio_unitario_clp,
    costo_unitario_clp, monto_bruto_clp, n_notas, monto_notas_clp, monto_neto_clp, publish_id)
WITH d AS (
    SELECT id_pedido_omnicanal, id_ubicacion_despacho,
           ROW_NUMBER() OVER (PARTITION BY id_pedido_omnicanal ORDER BY fecha_despacho, id_despacho) AS rn,
           COUNT(*)     OVER (PARTITION BY id_pedido_omnicanal) AS n
    FROM silver_despachos
    WHERE fecha_despacho <= :corte
), despacho_pedido AS (               -- preagregado: una fila por pedido
    SELECT id_pedido_omnicanal, id_ubicacion_despacho AS ubicacion_principal, n AS n_despachos
    FROM d WHERE rn = 1
), notas_venta AS (                   -- preagregado: una fila por venta
    SELECT id_venta_origen AS id_venta, COUNT(*) AS n_notas, SUM(monto_clp) AS monto_notas
    FROM ctl_notas_evaluacion
    WHERE fecha_corte = :corte AND estado = 'aplicada'
    GROUP BY id_venta_origen
)
SELECT :corte, v.id_venta, v.fecha, ic.customer_id, v.id_producto, p.categoria, p.subcategoria, v.canal,
       v.modalidad_entrega, v.id_tienda, v.id_ubicacion_despacho,
       COALESCE(dp.ubicacion_principal, v.id_ubicacion_despacho), COALESCE(dp.n_despachos, 0),
       v.cantidad, v.precio_unitario_clp, v.costo_unitario_clp,
       v.cantidad * v.precio_unitario_clp,
       COALESCE(nv.n_notas, 0), COALESCE(nv.monto_notas, 0),
       v.cantidad * v.precio_unitario_clp - COALESCE(nv.monto_notas, 0),
       :publish_id
FROM silver_ventas v
LEFT JOIN silver_productos p          ON p.id_producto = v.id_producto
LEFT JOIN silver_ventas_identidad vi ON vi.id_venta = v.id_venta
LEFT JOIN ident_rut_customer ic      ON ic.rut_normalizado = vi.rut_normalizado
LEFT JOIN despacho_pedido dp         ON dp.id_pedido_omnicanal = v.id_pedido_omnicanal
LEFT JOIN notas_venta nv             ON nv.id_venta = v.id_venta
WHERE v.estado_venta = 'confirmada' AND v.fecha <= :corte
"""


def publish_comercial(cfg: Config, corte: str) -> dict[str, Any]:
    date.fromisoformat(corte)
    engine = get_engine(cfg.warehouse_path)
    publish_id = new_run_id(f"comercial-{corte}")
    with engine.begin() as conn:
        faltantes = [t for t in REQUERIDAS
                     if conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar_one() == 0]
        if faltantes:
            raise DependenciaFaltante(f"Dependencias sin lote publicado: {faltantes}")
        run_ids = _run_ids(conn)

        # Notas de crédito: estado por corte (control para reintentos)
        notas = evaluar_notas(conn, corte)
        conn.execute(text("DELETE FROM ctl_notas_evaluacion WHERE fecha_corte = :c"), {"c": corte})
        conn.execute(text("DELETE FROM gold_mart_ventas WHERE fecha_corte = :c"), {"c": corte})

        conn.execute(text(
            "INSERT INTO gold_publicaciones (publish_id, mart, fecha_corte, version_reglas, run_ids, ts_publicacion,"
            " n_filas) VALUES (:p, 'gold_mart_ventas', :c, :v, :r, :t, 0)"),
            {"p": publish_id, "c": corte, "v": VERSION_REGLAS, "r": json.dumps(run_ids), "t": utc_now_iso()})
        if notas:
            conn.execute(text(
                "INSERT INTO ctl_notas_evaluacion (fecha_corte, id_nota, id_venta_origen, fecha_emision, monto_clp,"
                " estado, publish_id) VALUES (:c, :id_nota, :id_venta_origen, :fecha_emision, :monto_clp, :estado, :p)"),
                [{**n, "c": corte, "p": publish_id} for n in notas])
        conn.execute(text(SQL_GOLD_VENTAS), {"corte": corte, "publish_id": publish_id})

        # Verificación de granularidad y auditoría de montos
        audit = conn.execute(text(
            "SELECT COUNT(*) AS filas, COUNT(DISTINCT id_venta) AS ventas, SUM(monto_bruto_clp) AS bruto,"
            " SUM(monto_notas_clp) AS notas, SUM(monto_neto_clp) AS neto,"
            " SUM(customer_id IS NULL) AS sin_cliente, SUM(categoria IS NULL) AS sin_catalogo"
            " FROM gold_mart_ventas WHERE fecha_corte = :c"), {"c": corte}).mappings().one()
        esperadas = conn.execute(text(
            "SELECT COUNT(*) FROM silver_ventas WHERE estado_venta = 'confirmada' AND fecha <= :c"),
            {"c": corte}).scalar_one()
        if audit["filas"] != audit["ventas"] or audit["filas"] != esperadas:
            raise RuntimeError(f"Granularidad inválida en gold_mart_ventas: {dict(audit)} vs {esperadas}")
        if audit["bruto"] - audit["notas"] != audit["neto"]:
            raise RuntimeError("Auditoría de montos: bruto - notas != neto")
        estados_notas: dict[str, int] = {}
        for n in notas:
            estados_notas[n["estado"]] = estados_notas.get(n["estado"], 0) + 1
        detalle = {**dict(audit), "notas_por_estado": estados_notas,
                   "dependencias_vacias": [t for t, ids in run_ids.items() if not ids]}
        conn.execute(text("UPDATE gold_publicaciones SET n_filas = :n, detalle = :d WHERE publish_id = :p"),
                     {"n": audit["filas"], "d": json.dumps(detalle), "p": publish_id})

    with engine.begin() as conn:
        path = export_parquet(cfg, conn, "gold_mart_ventas", f"gold/corte={corte}", publish_id,
                              "WHERE fecha_corte = :c", {"c": corte})
        conn.execute(text("UPDATE gold_publicaciones SET parquet_path = :x WHERE publish_id = :p"),
                     {"x": path, "p": publish_id})
    return {"publish_id": publish_id, "fecha_corte": corte, "version_reglas": VERSION_REGLAS,
            "run_ids": run_ids, "parquet_path": path, **detalle}
