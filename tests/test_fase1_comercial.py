"""Fase 1 · Ventas, productos y gold_mart_ventas (evidencias verificables de la guía EP5)."""
from __future__ import annotations

import csv
import dataclasses
from pathlib import Path

from tnr import tasks
from tests.conftest import make_cfg


def silver_fingerprint(q):
    return q("SELECT id_venta, fecha, cantidad, precio_unitario_clp, costo_unitario_clp, estado_venta"
             " FROM silver_ventas ORDER BY id_venta")


def test_bronze_conserva_37971_recepciones(comercial):
    v = comercial["ventas"]
    assert v["estado"] == "exitoso"
    assert v["recibidos"] == 37_971
    with (Path(v["bronze_path"]) / "ventas_crudas.csv").open(encoding="utf-8") as fh:
        assert sum(1 for _ in csv.DictReader(fh, delimiter=";")) == 37_971


def test_36988_ids_antes_de_reglas_de_calidad(comercial, q):
    v = comercial["ventas"]
    assert v["recibidos"] - v["excluidos"] - v["revision"] == 36_988
    assert v["publicados"] + v["rechazados"] == 36_988
    # cada duplicado excluido es trazable a la fila conservada (menor fila_origen)
    assert q("SELECT COUNT(*) FROM etl_exclusiones WHERE fuente='ventas' AND ref_conservada IS NULL")[0][0] == 0


def test_rechazos_trazables_a_bronze(comercial, q):
    motivos = dict(q("SELECT motivo, COUNT(*) FROM etl_rechazos WHERE run_id = :r GROUP BY motivo",
                     r=comercial["ventas"]["run_id"]))
    assert {"FECHA_INVALIDA", "PRECIO_NO_POSITIVO", "CANTIDAD_ERROR_999"} <= set(motivos)
    assert q("SELECT COUNT(*) FROM etl_rechazos WHERE ref_bronze NOT LIKE 'ventas_crudas.csv#fila_origen=%'")[0][0] == 0


def test_estados_pendiente_y_cancelada_validos_en_silver(comercial, q):
    estados = dict(q("SELECT estado_venta, COUNT(*) FROM silver_ventas GROUP BY 1"))
    assert estados.get("pendiente") == 1 and estados.get("cancelada") == 1


def test_rut_solo_en_relacion_restringida(comercial, q):
    cols = {r[1] for r in q("PRAGMA table_info(silver_ventas)")}
    assert not {"rut_cliente", "nombre_cliente", "rut_normalizado"} & cols
    pend = q("SELECT COUNT(*) FROM silver_ventas v LEFT JOIN silver_ventas_identidad i USING (id_venta)"
             " WHERE v.estado_vinculo_cliente='pendiente' AND i.id_venta IS NOT NULL")[0][0]
    assert pend == 0


def test_reejecucion_idempotente_conserva_silver(comercial, cfg, q):
    antes = silver_fingerprint(q)
    again = tasks.ingest("ventas", input_kind="sql", cfg=cfg)
    assert again["estado"] == "omitido"
    repro = tasks.ingest("ventas", reproceso=True, cfg=cfg)
    assert repro["estado"] == "exitoso"
    assert silver_fingerprint(q) == antes
    # un solo run exitoso por fuente + huella + versión
    assert q("SELECT COUNT(*) FROM etl_runs WHERE fuente='ventas' AND estado='exitoso'")[0][0] == 1


def test_csv_ampliado_es_entrada_equivalente(comercial, tmp_path, q):
    other = make_cfg(tmp_path)
    tasks.init(other)
    res = tasks.ingest("ventas", input_kind="csv", cfg=other)
    assert (res["recibidos"], res["excluidos"], res["rechazados"]) == (
        comercial["ventas"]["recibidos"], comercial["ventas"]["excluidos"], comercial["ventas"]["rechazados"])
    from sqlalchemy import text
    from tnr.common.db import get_engine
    with get_engine(other.warehouse_path).connect() as conn:
        csv_fp = conn.execute(text("SELECT id_venta, fecha, cantidad, precio_unitario_clp, costo_unitario_clp,"
                                   " estado_venta FROM silver_ventas ORDER BY id_venta")).all()
    assert csv_fp == silver_fingerprint(q)


def test_productos_45_sku_y_6_categorias(comercial, q):
    assert comercial["productos"]["publicados"] == 45
    cats = {r[0] for r in q("SELECT DISTINCT categoria FROM silver_productos")}
    assert cats == {"Computación", "Telefonía", "Audio", "Accesorios", "Gaming", "Smart Home"}
    assert q("SELECT precio_lista_clp FROM silver_productos WHERE id_producto='P026'")[0][0] == 129_990
    assert q("SELECT precio_lista_clp FROM silver_productos WHERE id_producto='P038'")[0][0] is None


def test_gold_ventas_grano_y_montos(comercial, q):
    pub = comercial["publish"]
    assert pub["filas"] == pub["ventas"]
    confirmadas = q("SELECT COUNT(*) FROM silver_ventas WHERE estado_venta='confirmada'")[0][0]
    assert pub["filas"] == confirmadas
    assert pub["bruto"] - pub["notas"] == pub["neto"]
    row = q("SELECT COUNT(*), MAX(canal) FROM gold_mart_ventas WHERE id_venta='V025409'")[0]
    assert row == (1, "Online")


def test_republicar_mismo_corte_reemplaza(comercial, cfg, q):
    antes = q("SELECT SUM(monto_neto_clp), COUNT(*) FROM gold_mart_ventas WHERE fecha_corte='2025-12-31'")
    tasks.publish_comercial("2025-12-31", cfg=cfg)
    assert q("SELECT SUM(monto_neto_clp), COUNT(*) FROM gold_mart_ventas WHERE fecha_corte='2025-12-31'") == antes
