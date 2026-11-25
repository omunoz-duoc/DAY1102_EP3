"""Fase 3 · Eventos web: lote histórico, receptor, provisional, cierre y precedencia de ventanas."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tnr import tasks
from tnr.apps.productor import replay
from tnr.apps.receptor import create_app
from tnr.common.eventos_web import parse_ts
from tests.conftest import ROOT, make_cfg, query

STREAM = ROOT / "data" / "eventos_web_stream.jsonl"
MIN_1300 = "2025-12-26T13:00:00-03:00"
MIN_1301 = "2025-12-26T13:01:00-03:00"
LINEA_REENVIO_SIM001 = 76      # reenvío idéntico de SIM-001-R
LINEA_TARDIO_SIM021 = 78       # éxito tardío de la ventana 13:00, después de un evento de 13:01


@pytest.fixture
def web(tmp_path):
    cfg = make_cfg(tmp_path)
    tasks.init(cfg)
    app = create_app(cfg)
    client = app.test_client()

    def post(body: bytes):
        r = client.post("/eventos", data=body, content_type="application/json")
        return r.status_code, r.get_json()
    yield cfg, post, app.config["RECEIVER"]
    app.config["RECEIVER"].close()


def tasa(cfg):
    s = tasks.serving_web(MIN_1300, MIN_1301, cfg=cfg)
    return s["errores_5xx"], s["intentos"], {f["estado_ventana"] for f in s["filas"]}


# ---------------------------------------------------------------- histórico
def test_historico_5000_eventos(tmp_path):
    cfg = make_cfg(tmp_path)
    tasks.init(cfg)
    res = tasks.ingest("web_historico", cfg=cfg)
    assert res["recibidos"] == res["publicados"] == 5000
    assert query(cfg, "SELECT COUNT(DISTINCT id_evento), MIN(origen), MAX(origen) FROM silver_eventos_web") == [
        (5000, "historico", "historico")]
    assert query(cfg, "SELECT COUNT(*) FROM silver_eventos_web WHERE id_sesion IS NOT NULL"
                      " OR codigo_http IS NOT NULL OR latencia_ms IS NOT NULL")[0][0] == 0
    # 2025-03-23T15:48:04 en Santiago (verano, -03:00) -> 18:48:04Z
    assert query(cfg, "SELECT ts_evento FROM silver_eventos_web WHERE id_evento='E000001'") == [
        ("2025-03-23T18:48:04.000Z",)]


def test_horas_dst_ambigua_e_inexistente():
    # Chile 2025: 6-abr 00:00 -> 5-abr 23:00 (ambigua), 7-sep 00:00 -> 01:00 (inexistente)
    amb, inc = parse_ts("2025-04-05T23:30:00", "America/Santiago")
    assert inc["tipo"] == "ambigua" and amb.isoformat().startswith("2025-04-06T02:30")   # primera ocurrencia, -03
    inex, inc = parse_ts("2025-09-07T00:30:00", "America/Santiago")
    assert inc["tipo"] == "inexistente" and inex.isoformat().startswith("2025-09-07T04:30")  # = 01:30 -03
    assert parse_ts("2025-12-26T13:00:01-03:00", "America/Santiago")[1] is None


# ---------------------------------------------------------------- receptor y provisional
def test_provisional_25_por_ciento_tras_reenvio(web):
    cfg, post, _ = web
    res = replay(STREAM, post, hasta=LINEA_REENVIO_SIM001)
    assert all(r["status"] == 202 for r in res)
    assert res[-1]["estado"] == "duplicada"
    assert tasa(cfg) == (5, 20, {"provisional"})
    # cada recepción queda en Bronze aunque el hecho no se duplique en Silver
    assert query(cfg, "SELECT COUNT(*) FROM web_recepciones")[0][0] == LINEA_REENVIO_SIM001
    assert query(cfg, "SELECT COUNT(*) FROM silver_eventos_web WHERE id_evento='SIM-001-R'")[0][0] == 1
    lineas = sum(1 for p in Path(cfg.bronze_dir).rglob("recepciones_*.jsonl") for _ in p.open())
    assert lineas == LINEA_REENVIO_SIM001


def test_errores_todos_en_mobile(web):
    cfg, post, _ = web
    replay(STREAM, post, hasta=LINEA_REENVIO_SIM001)
    filas = {f["dispositivo"]: (f["errores_5xx"], f["intentos"]) for f in tasks.serving_web(MIN_1300, MIN_1301, cfg=cfg)["filas"]}
    assert filas == {"mobile": (5, 10), "desktop": (0, 10)}


def test_cierre_precedencia_y_tardio(web):
    cfg, post, _ = web
    replay(STREAM, post, hasta=LINEA_TARDIO_SIM021 - 1)
    c1 = tasks.close_web(MIN_1300, cfg=cfg)
    assert (c1["errores_5xx"], c1["intentos"], c1["version"]) == (5, 20, 1)
    assert tasa(cfg) == (5, 20, {"cerrada"})                       # Serving lee Gold

    replay(STREAM, post, desde=LINEA_TARDIO_SIM021)                 # llega SIM-021-R tardío
    assert tasa(cfg) == (5, 20, {"reproceso_pendiente"})            # Gold sigue mandando, marcado
    c2 = tasks.close_web(MIN_1300, cfg=cfg)
    assert (c2["errores_5xx"], c2["intentos"], c2["version"]) == (5, 21, 2)
    assert c2["tasa_error"] == pytest.approx(0.238095, abs=1e-6)    # 23,81 %
    assert tasa(cfg) == (5, 21, {"cerrada"})
    # la ventana 13:01 sigue abierta (provisional desde Silver)
    assert query(cfg, "SELECT COUNT(*) FROM ctl_ventanas_web")[0][0] == 1


def test_cierre_con_corte_de_ingesta_excluye_lo_posterior(web):
    cfg, post, _ = web
    res = replay(STREAM, post, hasta=LINEA_TARDIO_SIM021 - 1)
    corte = res[-1]["ts_ingesta"]
    replay(STREAM, post, desde=LINEA_TARDIO_SIM021)
    c = tasks.close_web(MIN_1300, corte_ingesta=corte, cfg=cfg)
    assert (c["errores_5xx"], c["intentos"], c["estado_ventana"]) == (5, 20, "reproceso_pendiente")


def test_payload_incompatible_va_a_revision_y_invalido_se_rechaza(web):
    cfg, post, _ = web
    linea = json.loads(STREAM.read_text().splitlines()[40])           # SIM-001-R
    post(json.dumps(linea).encode())
    linea["evento"]["codigo_http"] = 200
    st, body = post(json.dumps(linea).encode())
    assert (st, body["estado"]) == (202, "revision")
    bad = {**linea, "id_evento": "SIM-X", "evento": {**linea["evento"], "latencia_ms": -1}}
    assert post(json.dumps(bad).encode())[1]["estado"] == "rechazada"
    sin_sesion = {**linea, "id_evento": "SIM-Y", "id_sesion": None}
    assert post(json.dumps(sin_sesion).encode())[1]["estado"] == "rechazada"
    st, body = post(b"{no es json")
    assert st == 400 and body["id_recepcion"]
    assert query(cfg, "SELECT COUNT(*) FROM web_recepciones WHERE estado='rechazada'")[0][0] == 3


def test_recuperacion_desde_bronze_es_idempotente(web, monkeypatch):
    cfg, post, receiver = web
    replay(STREAM, post, hasta=50)
    # falla Silver: las recepciones quedan solo en Bronze
    monkeypatch.setattr(receiver, "_process", lambda conn, rec: (_ for _ in ()).throw(RuntimeError("silver caído")))
    res = replay(STREAM, post, desde=51, hasta=LINEA_REENVIO_SIM001)
    assert {r["estado"] for r in res} == {"pendiente_silver"}
    monkeypatch.undo()
    out = tasks.recover_web(cfg=cfg)
    assert out["recuperadas"] == LINEA_REENVIO_SIM001 - 50
    assert tasks.recover_web(cfg=cfg)["recuperadas"] == 0             # segunda vez: nada que hacer
    assert tasa(cfg) == (5, 20, {"provisional"})


def test_purchase_15_con_id_venta_valido(web):
    cfg, post, _ = web
    replay(STREAM, post)
    assert query(cfg, "SELECT COUNT(*) FROM silver_eventos_web WHERE tipo='purchase' AND id_venta IS NOT NULL")[0][0] == 15
    # sin ventas cargadas el cruce queda pendiente (se resuelve después)
    assert query(cfg, "SELECT estado_cruce, COUNT(*) FROM vw_purchase_web_venta GROUP BY 1") == [("pendiente", 15)]
