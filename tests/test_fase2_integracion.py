"""Fase 2 · CRM, despachos, notas, postventa, Cliente 360 y conciliación."""
from __future__ import annotations

import json

import pytest

from tnr import tasks
from tnr.gold.comercial import DependenciaFaltante
from tests.conftest import CORTE, make_cfg, query
from tests.postventa_server import FakePostventa


def crm(q, id_crm):
    return q("SELECT m.customer_id, m.metodo_vinculo, c.consentimiento_publicidad, c.estado_identidad,"
             " k.email_contacto, k.email_verificado FROM ident_crm_customer m"
             " JOIN silver_clientes c USING (customer_id) JOIN ident_contacto_cliente k USING (customer_id)"
             " WHERE m.id_crm = :i", i=id_crm)[0]


# ---------------------------------------------------------------- CRM e identidad
def test_crm_d0001_consentimiento_mas_restrictivo(comercial, q):
    base, dup = crm(q, "CRM-00001"), crm(q, "CRM-D0001")
    assert base[0] == dup[0]
    assert dup[2] == "no"   # la ficha base dice 'no', la duplicada 'si'
    assert q("SELECT COUNT(*) FROM etl_alertas_calidad WHERE id_registro='CRM-D0001'"
             " AND codigo='CONSENTIMIENTO_CONFLICTO_RESTRICTIVO'")[0][0] == 1


def test_crm_d0002_sin_rut_vincula_por_correo_y_telefono(comercial, q):
    dup = crm(q, "CRM-D0002")
    assert dup[0] == crm(q, "CRM-00002")[0] and dup[1] == "correo_y_telefono"


def test_crm_d0003_contradictorio_queda_en_revision(comercial, q):
    dup = crm(q, "CRM-D0003")
    assert dup[3] == "revision" and dup[1] == "revision"
    assert dup[0] not in {crm(q, "CRM-00003")[0], crm(q, "CRM-00004")[0]}
    ev = json.loads(q("SELECT evidencia FROM etl_revision WHERE id_registro='CRM-D0003'")[0][0])
    assert ev["candidatos_por_correo"] != ev["candidatos_por_telefono"]


def test_crm_d0005_prefiere_correo_verificado(comercial, q):
    dup = crm(q, "CRM-D0005")
    assert dup[0] == crm(q, "CRM-00005")[0]
    assert dup[4] != "registro-sin-verificar@tnr.example.invalid" and dup[5] == 1


def test_mapa_customer_id_estable_al_reprocesar(comercial, cfg, q):
    antes = q("SELECT id_crm, customer_id FROM ident_crm_customer ORDER BY id_crm")
    assert tasks.ingest("clientes", reproceso=True, cfg=cfg)["estado"] == "exitoso"
    assert q("SELECT id_crm, customer_id FROM ident_crm_customer ORDER BY id_crm") == antes


def test_marts_publican_solo_customer_id(comercial, q):
    for mart in ("gold_mart_ventas", "gold_mart_cliente_360"):
        cols = {r[1] for r in q(f"PRAGMA table_info({mart})")}
        assert not {"rut", "rut_normalizado", "email", "email_contacto", "telefono", "nombre_cliente"} & cols


# ---------------------------------------------------------------- integración
def test_v025409_online_con_retiro_en_t07(comercial, q):
    rows = q("SELECT canal, id_ubicacion_operativa, n_despachos FROM gold_mart_ventas"
             " WHERE fecha_corte=:c AND id_venta='V025409'", c=CORTE)
    assert rows == [("Online", "T07", 1)]


def test_notas_v023142_y_huerfana(comercial, q):
    estados = dict(q("SELECT id_nota, estado FROM ctl_notas_evaluacion WHERE fecha_corte=:c"
                     " AND id_nota IN ('NC-0001','NC-0046','NC-0047')", c=CORTE))
    assert estados == {"NC-0001": "aplicada", "NC-0046": "aplicada", "NC-0047": "huerfana"}
    n_notas, notas, bruto, neto = q("SELECT n_notas, monto_notas_clp, monto_bruto_clp, monto_neto_clp"
                                    " FROM gold_mart_ventas WHERE fecha_corte=:c AND id_venta='V023142'", c=CORTE)[0]
    assert n_notas == 2 and neto == bruto - notas
    assert comercial["publish"]["referencias_pendientes"].get("nota_huerfana") == 1
    # el monto de la huérfana queda fuera del neto
    total_notas = q("SELECT SUM(monto_notas_clp) FROM gold_mart_ventas WHERE fecha_corte=:c", c=CORTE)[0][0]
    aplicadas = q("SELECT SUM(monto_clp) FROM ctl_notas_evaluacion WHERE fecha_corte=:c AND estado='aplicada'",
                  c=CORTE)[0][0]
    assert total_notas == aplicadas


def test_ventas_anonimas_se_mantienen_en_el_total(comercial, q):
    pub = comercial["publish"]
    assert pub["ventas"]["sin_cliente"] > 0
    anon = q("SELECT SUM(monto_neto_clp) FROM gold_mart_ventas WHERE fecha_corte=:c AND customer_id IS NULL",
             c=CORTE)[0][0]
    assert pub["cliente_360"]["neto_clientes"] + anon == pub["ventas"]["neto"]


def test_cliente_360_grano_y_clientes_sin_ventas(comercial, q):
    c360 = comercial["publish"]["cliente_360"]
    assert c360["clientes"] == c360["distintos"] == q("SELECT COUNT(*) FROM silver_clientes")[0][0]
    assert q("SELECT COUNT(*) FROM gold_mart_cliente_360 WHERE fecha_corte=:c AND frecuencia_compras=0"
             " AND fecha_ultima_compra IS NULL AND monto_neto_clp=0", c=CORTE)[0][0] == c360["sin_ventas"] >= 1


def test_conciliacion_cascada(comercial, q):
    etapas = {r[0]: (r[1], r[2]) for r in q(
        "SELECT etapa, filas, monto_clp FROM gold_mart_conciliacion_ventas WHERE fecha_corte=:c"
        " AND escenario='ampliado'", c=CORTE)}
    assert etapas["crudo_bronze"][0] == 37_971
    assert etapas["tras_deduplicacion"][0] == 36_988
    assert etapas["confirmadas_al_corte"][1] + etapas["notas_credito_aplicadas"][1] == etapas["venta_neta"][1]
    ref = {r[0]: r[1] for r in q("SELECT etapa, monto_clp FROM gold_mart_conciliacion_ventas"
                                 " WHERE fecha_corte=:c AND escenario='referencia_docente'", c=CORTE)}
    assert ref["ficha_docente"] == 48_500_012_180


# ---------------------------------------------------------------- postventa HTTP
def test_postventa_30_ordenes_con_reintentos(comercial, cfg, postventa_server, q):
    res = comercial["postventa"]
    assert res["estado"] == "exitoso" and res["publicados"] == 30
    assert postventa_server.requests.count("2") == 3   # dos 503 + éxito
    metas = [json.loads(p.read_text()) for p in sorted(
        __import__("pathlib").Path(res["bronze_path"]).glob("*.meta.json"))]
    assert [m["estado_http"] for m in metas] == [200, 200, 200] and metas[1]["intentos"] == 3
    assert q("SELECT COUNT(DISTINCT id_orden) FROM silver_postventa")[0][0] == 30


def test_postventa_incoherente_no_publica(comercial, tmp_path):
    pages = FakePostventa.pages_from(make_cfg(tmp_path).input_dir)
    pages["3"]["ordenes"] = pages["3"]["ordenes"][:-1]      # 29 órdenes con total=30
    srv = FakePostventa(pages)
    try:
        other = make_cfg(tmp_path, postventa_url=srv.url)
        tasks.init(other)
        with pytest.raises(Exception, match="incoherente"):
            tasks.ingest("postventa", cfg=other)
        assert query(other, "SELECT estado FROM etl_runs WHERE fuente='postventa'") == [("fallido",)]
        assert query(other, "SELECT COUNT(*) FROM silver_postventa")[0][0] == 0
    finally:
        srv.close()


def test_postventa_siguiente_fuera_de_origen(comercial, tmp_path):
    pages = FakePostventa.pages_from(make_cfg(tmp_path).input_dir)
    pages["1"]["siguiente"] = "http://otro-host.invalid/ordenes?page=2"
    srv = FakePostventa(pages)
    try:
        other = make_cfg(tmp_path, postventa_url=srv.url)
        tasks.init(other)
        with pytest.raises(Exception, match="origen"):
            tasks.ingest("postventa", cfg=other)
    finally:
        srv.close()


# ---------------------------------------------------------------- dependencias
def test_dependencias_incompletas_y_resolucion_posterior(tmp_path):
    other = make_cfg(tmp_path)
    tasks.init(other)
    with pytest.raises(DependenciaFaltante):
        tasks.publish_comercial(CORTE, cfg=other)
    tasks.ingest("ventas", cfg=other)
    tasks.ingest("productos", cfg=other)
    pub = tasks.publish_comercial(CORTE, cfg=other)
    assert "silver_despachos" in pub["dependencias_vacias"] and pub["cliente_360"]["clientes"] == 0
    assert query(other, "SELECT n_despachos FROM gold_mart_ventas WHERE id_venta='V025409'") == [(0,)]
    tasks.ingest("despachos", cfg=other)                      # llega la dependencia
    tasks.publish_comercial(CORTE, cfg=other)                 # misma fecha de corte: reemplaza
    assert query(other, "SELECT n_despachos, id_ubicacion_operativa FROM gold_mart_ventas"
                        " WHERE id_venta='V025409'") == [(1, "T07")]
    assert query(other, "SELECT COUNT(DISTINCT publish_id) FROM gold_mart_ventas")[0][0] == 1
