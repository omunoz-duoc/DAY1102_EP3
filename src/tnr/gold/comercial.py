"""Publicación comercial: gold_mart_ventas, gold_mart_cliente_360 y gold_mart_conciliacion_ventas."""
from __future__ import annotations

import json
from datetime import date
from typing import Any

from sqlalchemy import Connection, text

from tnr.common.batch import export_parquet, new_run_id
from tnr.gold import conciliacion
from tnr.common.db import get_engine
from tnr.common.tipos import utc_now_iso
from tnr.config import Config

VERSION_REGLAS = "comercial.v2"
REQUERIDAS = ("silver_ventas", "silver_productos")
OPCIONALES = ("silver_clientes", "ident_rut_customer", "silver_despachos", "silver_notas_credito", "silver_postventa")


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


SQL_GOLD_CLIENTE_360 = """
INSERT INTO gold_mart_cliente_360 (
    fecha_corte, customer_id, comuna, canal_alta, fecha_alta, consentimiento_publicidad, estado_identidad,
    frecuencia_compras, fecha_ultima_compra, recencia_dias, monto_bruto_clp, monto_neto_clp, canal_preferido,
    n_ordenes_postventa, n_ordenes_abiertas, n_ordenes_garantia, dias_resolucion_promedio, publish_id)
WITH compras AS (
    SELECT customer_id, COUNT(*) AS frecuencia, MAX(fecha) AS ultima,
           SUM(monto_bruto_clp) AS bruto, SUM(monto_neto_clp) AS neto
    FROM gold_mart_ventas
    WHERE fecha_corte = :corte AND customer_id IS NOT NULL
    GROUP BY customer_id
), canal AS (
    SELECT customer_id, canal,
           ROW_NUMBER() OVER (PARTITION BY customer_id
                              ORDER BY COUNT(*) DESC, SUM(monto_neto_clp) DESC, canal) AS rn
    FROM gold_mart_ventas
    WHERE fecha_corte = :corte AND customer_id IS NOT NULL
    GROUP BY customer_id, canal
), postventa AS (                     -- órdenes por cliente vía venta -> RUT -> customer_id
    SELECT ic.customer_id,
           COUNT(*) AS n,
           SUM(p.estado <> 'cerrada' OR p.fecha_cierre > :corte) AS abiertas,
           SUM(p.en_garantia) AS garantia,
           AVG(CASE WHEN p.estado = 'cerrada' AND p.fecha_cierre IS NOT NULL AND p.fecha_cierre <= :corte
                    THEN julianday(p.fecha_cierre) - julianday(p.fecha_apertura) END) AS dias
    FROM silver_postventa p
    JOIN silver_ventas_identidad vi ON vi.id_venta = p.id_venta
    JOIN ident_rut_customer ic      ON ic.rut_normalizado = vi.rut_normalizado
    WHERE p.fecha_apertura <= :corte
    GROUP BY ic.customer_id
)
SELECT :corte, c.customer_id, c.comuna, c.canal_alta, c.fecha_alta, c.consentimiento_publicidad, c.estado_identidad,
       COALESCE(v.frecuencia, 0), v.ultima,
       CAST(julianday(:corte) - julianday(v.ultima) AS INTEGER),
       COALESCE(v.bruto, 0), COALESCE(v.neto, 0), ca.canal,
       COALESCE(pv.n, 0), COALESCE(pv.abiertas, 0), COALESCE(pv.garantia, 0), pv.dias,
       :publish_id
FROM silver_clientes c
LEFT JOIN compras v    ON v.customer_id = c.customer_id
LEFT JOIN canal ca     ON ca.customer_id = c.customer_id AND ca.rn = 1
LEFT JOIN postventa pv ON pv.customer_id = c.customer_id
"""

SQL_REFERENCIAS_PENDIENTES = """
INSERT INTO ctl_referencias_pendientes (fecha_corte, publish_id, tipo, id_registro, referencia)
SELECT :corte, :publish_id, 'despacho_sin_venta', d.id_despacho, d.id_pedido_omnicanal
FROM silver_despachos d
WHERE NOT EXISTS (SELECT 1 FROM silver_ventas v WHERE v.id_pedido_omnicanal = d.id_pedido_omnicanal)
UNION ALL
SELECT :corte, :publish_id, 'postventa_sin_venta', p.id_orden, p.id_venta
FROM silver_postventa p
WHERE NOT EXISTS (SELECT 1 FROM silver_ventas v WHERE v.id_venta = p.id_venta)
UNION ALL
SELECT :corte, :publish_id, 'nota_' || n.estado, n.id_nota, n.id_venta_origen
FROM ctl_notas_evaluacion n
WHERE n.fecha_corte = :corte AND n.estado <> 'aplicada'
UNION ALL
SELECT :corte, :publish_id, 'venta_sin_catalogo', g.id_venta, g.id_producto
FROM gold_mart_ventas g
WHERE g.fecha_corte = :corte AND g.categoria IS NULL
UNION ALL
SELECT :corte, :publish_id, 'venta_rut_sin_cliente', vi.id_venta, NULL
FROM silver_ventas_identidad vi
WHERE NOT EXISTS (SELECT 1 FROM ident_rut_customer ic WHERE ic.rut_normalizado = vi.rut_normalizado)
"""

MARTS = ("gold_mart_ventas", "gold_mart_cliente_360", "gold_mart_conciliacion_ventas")


def publish_comercial(cfg: Config, corte: str) -> dict[str, Any]:
    date.fromisoformat(corte)
    engine = get_engine(cfg.warehouse_path)
    publish_id = new_run_id(f"comercial-{corte}")
    params = {"corte": corte, "publish_id": publish_id}
    with engine.begin() as conn:
        faltantes = [t for t in REQUERIDAS
                     if conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar_one() == 0]
        if faltantes:
            raise DependenciaFaltante(f"Dependencias sin lote publicado: {faltantes}")
        run_ids = _run_ids(conn)

        # Reemplazo de la publicación del mismo corte
        for t in ("gold_mart_conciliacion_ventas", "gold_mart_cliente_360", "gold_mart_ventas",
                  "ctl_notas_evaluacion", "ctl_referencias_pendientes"):
            conn.execute(text(f"DELETE FROM {t} WHERE fecha_corte = :c"), {"c": corte})
        conn.execute(text(
            "INSERT INTO gold_publicaciones (publish_id, mart, fecha_corte, version_reglas, run_ids, ts_publicacion,"
            " n_filas) VALUES (:p, 'comercial', :c, :v, :r, :t, 0)"),
            {"p": publish_id, "c": corte, "v": VERSION_REGLAS, "r": json.dumps(run_ids), "t": utc_now_iso()})

        # 1. Notas de crédito evaluadas (control para reintentos)
        notas = evaluar_notas(conn, corte)
        if notas:
            conn.execute(text(
                "INSERT INTO ctl_notas_evaluacion (fecha_corte, id_nota, id_venta_origen, fecha_emision, monto_clp,"
                " estado, publish_id) VALUES (:c, :id_nota, :id_venta_origen, :fecha_emision, :monto_clp, :estado, :p)"),
                [{**n, "c": corte, "p": publish_id} for n in notas])

        # 2. Marts
        conn.execute(text(SQL_GOLD_VENTAS), params)
        conn.execute(text(SQL_GOLD_CLIENTE_360), params)
        conn.execute(text(SQL_REFERENCIAS_PENDIENTES), params)
        conciliacion.publicar(cfg, conn, corte, publish_id, run_ids)

        # 3. Verificación de granularidad y auditoría de montos
        audit = dict(conn.execute(text(
            "SELECT COUNT(*) AS filas, COUNT(DISTINCT id_venta) AS ventas, SUM(monto_bruto_clp) AS bruto,"
            " SUM(monto_notas_clp) AS notas, SUM(monto_neto_clp) AS neto,"
            " SUM(customer_id IS NULL) AS sin_cliente, SUM(categoria IS NULL) AS sin_catalogo"
            " FROM gold_mart_ventas WHERE fecha_corte = :c"), {"c": corte}).mappings().one())
        esperadas = conn.execute(text(
            "SELECT COUNT(*) FROM silver_ventas WHERE estado_venta = 'confirmada' AND fecha <= :c"),
            {"c": corte}).scalar_one()
        if audit["filas"] != audit["ventas"] or audit["filas"] != esperadas:
            raise RuntimeError(f"Granularidad inválida en gold_mart_ventas: {audit} vs {esperadas}")
        if audit["bruto"] - audit["notas"] != audit["neto"]:
            raise RuntimeError("Auditoría de montos: bruto - notas != neto")
        notas_aplicadas = conn.execute(text(
            "SELECT COALESCE(SUM(monto_clp), 0) FROM ctl_notas_evaluacion WHERE fecha_corte = :c AND estado = 'aplicada'"),
            {"c": corte}).scalar_one()
        if notas_aplicadas != audit["notas"]:
            raise RuntimeError("Notas aplicadas no cuadran con el mart (posible fan-out en el cruce)")
        c360 = dict(conn.execute(text(
            "SELECT COUNT(*) AS clientes, COUNT(DISTINCT customer_id) AS distintos,"
            " SUM(frecuencia_compras = 0) AS sin_ventas, SUM(monto_neto_clp) AS neto_clientes"
            " FROM gold_mart_cliente_360 WHERE fecha_corte = :c"), {"c": corte}).mappings().one())
        if c360["clientes"] != c360["distintos"]:
            raise RuntimeError("Granularidad inválida en gold_mart_cliente_360")
        if (c360["neto_clientes"] or 0) + conn.execute(text(
                "SELECT COALESCE(SUM(monto_neto_clp), 0) FROM gold_mart_ventas WHERE fecha_corte = :c"
                " AND customer_id IS NULL"), {"c": corte}).scalar_one() != audit["neto"]:
            raise RuntimeError("Cliente 360 + ventas anónimas no cuadran con la venta neta")

        pendientes = dict(conn.execute(text(
            "SELECT tipo, COUNT(*) FROM ctl_referencias_pendientes WHERE fecha_corte = :c GROUP BY tipo"),
            {"c": corte}).all())
        estados_notas: dict[str, int] = {}
        for n in notas:
            estados_notas[n["estado"]] = estados_notas.get(n["estado"], 0) + 1
        detalle = {"ventas": audit, "cliente_360": c360, "notas_por_estado": estados_notas,
                   "referencias_pendientes": pendientes,
                   "dependencias_vacias": [t for t, ids in run_ids.items() if not ids]}
        n_filas = audit["filas"] + c360["clientes"]
        conn.execute(text("UPDATE gold_publicaciones SET n_filas = :n, detalle = :d WHERE publish_id = :p"),
                     {"n": n_filas, "d": json.dumps(detalle), "p": publish_id})

    paths = {}
    with engine.begin() as conn:
        for mart in MARTS:
            paths[mart] = export_parquet(cfg, conn, mart, f"gold/corte={corte}", publish_id,
                                         "WHERE fecha_corte = :c", {"c": corte})
        conn.execute(text("UPDATE gold_publicaciones SET parquet_path = :x WHERE publish_id = :p"),
                     {"x": json.dumps(paths), "p": publish_id})
    return {"publish_id": publish_id, "fecha_corte": corte, "version_reglas": VERSION_REGLAS,
            "run_ids": run_ids, "parquet_paths": paths, **detalle}
