"""Contrato compartido de eventos web: mismas reglas para el lote histórico, la ruta rápida y el cierre."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import Connection, text

from tnr.common.contracts import QualityAlert, Rejection, SilverRecord
from tnr.common.identidad import normalize_rut
from tnr.common.tipos import clean_text, is_blank, to_utc_iso

DISPOSITIVOS = {"mobile", "desktop", "tablet"}
COLS = ["id_evento", "origen", "ts_evento", "tipo", "pagina", "duracion_seg", "codigo_http", "latencia_ms",
        "id_venta", "id_sesion", "dispositivo", "navegador", "tienda_asociada", "campania", "payload_sha256",
        "id_recepcion", "ts_ingesta", "run_id"]


def payload_hash(payload: dict) -> str:
    canon = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()


def parse_ts(value: Any, tz: str) -> tuple[datetime | None, dict | None]:
    """Devuelve (dt_utc, incidencia). Sin offset se aplica la zona declarada:
    hora ambigua -> primera ocurrencia (fold=0); inexistente -> se desplaza al instante válido siguiente (+1h)."""
    if is_blank(value):
        return None, None
    text_ = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text_)
    except ValueError:
        return None, None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc), None
    zone = ZoneInfo(tz)
    d0, d1 = dt.replace(tzinfo=zone, fold=0), dt.replace(tzinfo=zone, fold=1)
    utc = d0.astimezone(timezone.utc)
    incidencia = None
    if utc.astimezone(zone).replace(tzinfo=None) != dt:
        incidencia = {"tipo": "inexistente", "regla": "desplazar_+1h"}
    elif d0.utcoffset() != d1.utcoffset():
        incidencia = {"tipo": "ambigua", "regla": "primera_ocurrencia_fold0"}
    return utc, incidencia


def normalize_evento(payload: Any, *, origen: str, tz: str, ref: str, ts_ingesta: str,
                     id_recepcion: str | None = None) -> SilverRecord | Rejection:
    if not isinstance(payload, dict):
        return Rejection(ref, None, "PAYLOAD_NO_OBJETO")
    id_evento = str(payload.get("id_evento") or "").strip()
    evento = payload.get("evento") if isinstance(payload.get("evento"), dict) else {}
    usuario = payload.get("usuario") if isinstance(payload.get("usuario"), dict) else {}
    contexto = payload.get("contexto") if isinstance(payload.get("contexto"), dict) else {}
    errores: list[str] = []
    alerts: list[QualityAlert] = []

    if not id_evento:
        errores.append("ID_EVENTO_AUSENTE")
    ts, incidencia = parse_ts(payload.get("timestamp"), tz)
    if ts is None:
        errores.append("TIMESTAMP_INVALIDO")
    tipo = (clean_text(evento.get("tipo")) or "").lower()
    if not tipo:
        errores.append("TIPO_AUSENTE")
    id_sesion = clean_text(payload.get("id_sesion"))
    if origen == "stream" and not id_sesion:
        errores.append("ID_SESION_AUSENTE")

    codigo = evento.get("codigo_http")
    if codigo is not None and not (isinstance(codigo, int) and not isinstance(codigo, bool) and 100 <= codigo <= 599):
        errores.append("CODIGO_HTTP_INVALIDO")
    latencia = evento.get("latencia_ms")
    if latencia is not None and not (isinstance(latencia, (int, float)) and not isinstance(latencia, bool) and latencia >= 0):
        errores.append("LATENCIA_INVALIDA")
    if tipo == "checkout_request" and origen == "stream" and codigo is None:
        errores.append("CHECKOUT_SIN_CODIGO_HTTP")
    id_venta = clean_text(evento.get("id_venta"))
    if id_venta is not None and not re.fullmatch(r"V\d{6}", id_venta):
        errores.append("ID_VENTA_FORMATO_INVALIDO")

    if errores:
        return Rejection(ref, id_evento or None, errores[0], ",".join(errores))

    dispositivo = (clean_text(usuario.get("dispositivo")) or "").lower() or None
    if dispositivo not in DISPOSITIVOS:
        alerts.append(QualityAlert("usuario.dispositivo", "DISPOSITIVO_DESCONOCIDO", dispositivo))
        dispositivo = dispositivo or "desconocido"
    if incidencia:
        alerts.append(QualityAlert("timestamp", "TS_" + incidencia["tipo"].upper(), payload.get("timestamp")))
    rut, _ = normalize_rut(usuario.get("rut"))
    duracion = evento.get("duracion_seg")

    data = {
        "id_evento": id_evento, "origen": origen, "ts_evento": to_utc_iso(ts), "tipo": tipo,
        "pagina": clean_text(evento.get("pagina")),
        "duracion_seg": float(duracion) if isinstance(duracion, (int, float)) and not isinstance(duracion, bool) else None,
        "codigo_http": codigo, "latencia_ms": int(latencia) if latencia is not None else None,
        "id_venta": id_venta, "id_sesion": id_sesion, "dispositivo": dispositivo,
        "navegador": clean_text(usuario.get("navegador")), "tienda_asociada": clean_text(contexto.get("tienda_asociada")),
        "campania": clean_text(contexto.get("campania")), "payload_sha256": payload_hash(payload),
        "id_recepcion": id_recepcion, "ts_ingesta": ts_ingesta,
    }
    rec = SilverRecord(ref=ref, key=id_evento, data=data, alerts=alerts,
                       restricted={"rut_normalizado": rut} if rut else None)
    if incidencia:
        rec.data["_incidencia"] = {**incidencia, "valor_local": str(payload.get("timestamp"))}
    return rec


def minuto_utc(ts_evento_utc: str) -> str:
    return ts_evento_utc[:16] + ":00Z"


def insert_eventos(conn: Connection, records: list[SilverRecord], run_id: str) -> None:
    """Escritor único de silver_eventos_web."""
    if not records:
        return
    conn.execute(text(f"INSERT INTO silver_eventos_web ({', '.join(COLS)}) VALUES ({', '.join(':' + c for c in COLS)})"),
                 [{**{k: v for k, v in r.data.items() if not k.startswith('_')}, "run_id": run_id} for r in records])
    ident = [{"id": r.key, "rut": r.restricted["rut_normalizado"]} for r in records if r.restricted]
    if ident:
        conn.execute(text("INSERT INTO silver_eventos_web_identidad (id_evento, rut_normalizado) VALUES (:id, :rut)"),
                     ident)
    incid = [{"id": r.key, "run": run_id, **r.data["_incidencia"], "ts": r.data["ts_evento"]}
             for r in records if r.data.get("_incidencia")]
    if incid:
        conn.execute(text("INSERT OR REPLACE INTO etl_incidencias_tz (id_evento, run_id, valor_local, tipo, regla,"
                          " ts_resuelto) VALUES (:id, :run, :valor_local, :tipo, :regla, :ts)"), incid)
