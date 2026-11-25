"""Fase 4 · Inventario (snapshot + movimientos), gold_mart_stock_ubicacion y dashboards."""
from __future__ import annotations

import json

import pytest

from tnr import tasks
from tnr.apps import consultas
from tnr.apps.dashboard import create_app
from tnr.apps.productor import replay
from tnr.apps.receptor import create_app as create_receptor
from tests.conftest import CORTE, ROOT, make_cfg, query
from tests.dash_client import call

MOVS = "movimientos_inventario.jsonl"
CORTE_STOCK = "2025-12-26T12:05:00-03:00"


@pytest.fixture
def inv(tmp_path):
    cfg = make_cfg(tmp_path)
    tasks.init(cfg)
    tasks.ingest("productos", cfg=cfg)
    tasks.ingest("inventario", cfg=cfg)
    return cfg


def p001(cfg, corte=CORTE_STOCK):
    return next(f for f in tasks.stock_provisional(corte, cfg=cfg)["filas"] if f["id_producto"] == "P001")


def test_inventario_10_filas_y_p001_inicia_con_6(inv):
    assert query(inv, "SELECT COUNT(*) FROM silver_inventario")[0][0] == 10
    f = p001(inv)
    assert (f["disponible_inicial"], f["saldo_disponible"]) == (6, 6)
    assert query(inv, "SELECT COUNT(*) FROM vw_stock_provisional WHERE sku_sin_catalogo")[0][0] == 0


def test_inventario_rechaza_stock_incoherente(tmp_path):
    cfg = make_cfg(tmp_path)
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "inventario_inicial.csv").write_text(
        "id_producto;id_ubicacion;fecha_corte;stock_fisico;stock_reservado;umbral_reposicion\n"
        "P001;CD01;2025-12-26T11:59:59-03:00;2;3;1\nP002;CD01;2025-12-26T11:59:59-03:00;5;1;-1\n", encoding="utf-8")
    import dataclasses
    cfg = dataclasses.replace(cfg, input_dir=tmp_path / "in")
    tasks.init(cfg)
    res = tasks.ingest("inventario", cfg=cfg)
    assert (res["publicados"], res["rechazados"]) == (0, 2)
    assert sorted(r[0] for r in query(cfg, "SELECT motivo FROM etl_rechazos")) == ["STOCK_INCOHERENTE", "UMBRAL_NEGATIVO"]


def test_p001_6_2_reenvio_2_tardio_cierra_en_5(inv):
    r1 = tasks.consume_movimientos(MOVS, hasta=1, cfg=inv)
    assert r1["estados"] == {"insertada": 1} and p001(inv)["saldo_disponible"] == 2
    assert p001(inv)["alerta_reposicion"] == 1                        # 2 <= umbral 2

    r2 = tasks.consume_movimientos(MOVS, desde=2, hasta=6, cfg=inv)  # línea 6 = reenvío de MOV-0001
    assert r2["detalle"][-1]["estado"] == "duplicada" and p001(inv)["saldo_disponible"] == 2

    c1 = tasks.close_stock(CORTE_STOCK, cfg=inv)
    assert next(f for f in c1["filas"] if f["id_producto"] == "P001")["saldo_disponible"] == 2

    tasks.consume_movimientos(MOVS, desde=7, cfg=inv)               # incluye MOV-0035 tardío
    assert query(inv, "SELECT estado FROM ctl_cierres_stock") == [("reproceso_pendiente",)]
    c2 = tasks.close_stock(CORTE_STOCK, cfg=inv)
    fila = next(f for f in c2["filas"] if f["id_producto"] == "P001")
    assert (fila["saldo_disponible"], c2["version"], c2["estado"]) == (5, 2, "cerrada")
    assert query(inv, "SELECT saldo_disponible, version FROM gold_mart_stock_ubicacion WHERE id_producto='P001'") == [(5, 2)]
    # cada recepción queda en Bronze; Silver tiene un hecho por id_evento
    assert c2["recepciones_leidas"] == 36
    assert query(inv, "SELECT COUNT(*) FROM silver_movimientos_inventario")[0][0] == 35


def test_cierre_trazable(inv):
    tasks.consume_movimientos(MOVS, cfg=inv)
    c = tasks.close_stock(CORTE_STOCK, cfg=inv)
    f = next(f for f in c["filas"] if f["id_producto"] == "P001")
    assert f["corte_snapshot"] == "2025-12-26T14:59:59.000Z" and f["ultima_ingesta_incorporada"]
    run = query(inv, "SELECT fuente, estado FROM etl_runs WHERE run_id = :r", r=c["run_id"])
    assert run == [("stock_cierre", "exitoso")]


def test_movimiento_sin_snapshot_queda_pendiente(inv, tmp_path):
    f = tmp_path / "m.jsonl"
    f.write_text(json.dumps({"id_evento": "MOV-9001", "ts_evento": "2025-12-26T12:02:00-03:00", "id_producto": "P020",
                             "id_ubicacion": "CD01", "tipo": "reposicion", "delta_unidades": 2}) + "\n")
    tasks.consume_movimientos(str(f), cfg=inv)
    assert tasks.stock_provisional(CORTE_STOCK, cfg=inv)["pendientes"] == ["MOV-9001"]
    assert query(inv, "SELECT id_evento FROM vw_movimientos_sin_snapshot") == [("MOV-9001",)]


@pytest.mark.parametrize("mov,motivo", [
    ({"tipo": "venta_confirmada", "delta_unidades": 2}, "SIGNO_INCOHERENTE_CON_TIPO"),
    ({"tipo": "reposicion", "delta_unidades": 0}, "DELTA_NO_ENTERO_O_CERO"),
    ({"tipo": "ajuste", "delta_unidades": 1.5}, "DELTA_NO_ENTERO_O_CERO"),
    ({"tipo": "robo", "delta_unidades": -1}, "TIPO_INVALIDO"),
])
def test_movimientos_invalidos(inv, tmp_path, mov, motivo):
    f = tmp_path / "m.jsonl"
    f.write_text(json.dumps({"id_evento": "MOV-9002", "ts_evento": "2025-12-26T12:02:00-03:00", "id_producto": "P001",
                             "id_ubicacion": "CD01", **mov}) + "\n")
    assert tasks.consume_movimientos(str(f), cfg=inv)["estados"] == {"rechazada": 1}
    assert query(inv, "SELECT motivo FROM etl_rechazos")[0][0] == motivo


# ---------------------------------------------------------------- dashboards
@pytest.fixture
def dash_cfg(comercial, cfg):
    """Warehouse de la sesión (fases 1-2) + web + inventario."""
    receptor = create_receptor(cfg)
    client = receptor.test_client()

    def post(body):
        r = client.post("/eventos", data=body, content_type="application/json")
        return r.status_code, r.get_json()
    if not query(cfg, "SELECT COUNT(*) FROM ctl_ventanas_web")[0][0]:
        replay(ROOT / "data" / "eventos_web_stream.jsonl", post)
        tasks.close_web("2025-12-26T13:00:00-03:00", cfg=cfg)
        tasks.ingest("inventario", cfg=cfg)
        tasks.consume_movimientos(MOVS, cfg=cfg)
    receptor.config["RECEIVER"].close()
    return cfg


def test_consultas_dashboards(dash_cfg):
    k = consultas.kpis_comerciales(dash_cfg, CORTE)
    assert k["ventas"] == 36_641 and k["neto"] == k["bruto"] - k["notas"]
    c = consultas.checkout(dash_cfg, "2025-12-26T13:00", "2025-12-26T13:01")
    assert (c["errores_5xx"], c["intentos"], c["sesiones_afectadas"]) == (5, 21, 5)
    s = consultas.stock(dash_cfg, "2025-12-26T12:05")
    assert s["estado"] == "provisional"
    assert s["filas"].set_index("id_producto").loc["P001", "saldo_disponible"] == 5


def test_lectores_son_solo_lectura(dash_cfg):
    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError
    from tnr.common.db import get_read_engine
    with pytest.raises(OperationalError):
        with get_read_engine(dash_cfg.warehouse_path).begin() as conn:
            conn.execute(text("DELETE FROM gold_mart_ventas"))


def test_callbacks_dash(dash_cfg):
    app = create_app(dash_cfg)
    r = call(app, [("pagina", "children"), ("nav", "children")], [("url", "pathname", "/operaciones")])
    assert "pagina" in r
    r = call(app, [("kpis-comercial", "children"), ("cascada", "figure"), ("referencia", "children"),
                   ("canal-modalidad", "figure"), ("canal-tabla", "children"), ("cliente", "options")],
             [("corte", "value", CORTE)])
    assert len(r["cascada"]["figure"]["data"][0]["x"]) == 6
    r = call(app, [("estado-web", "children"), ("kpis-web", "children"), ("intentos", "figure"), ("tasa", "figure"),
                   ("tabla-web", "children"), ("estado-stock", "children"), ("tabla-stock", "children")],
             [("tick", "n_intervals", 1), ("desde", "value", "2025-12-26T13:00"), ("hasta", "value", "2025-12-26T13:01"),
              ("corte-stock", "value", "2025-12-26T12:05")])
    tasa = r["kpis-web"]["children"][1]["props"]["children"][1]["props"]["children"]
    assert tasa == "23.81%"
