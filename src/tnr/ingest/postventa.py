"""Postventa: cliente HTTP paginado contra la API de órdenes (servidor externo, URL en TNR_POSTVENTA_URL).

Contrato esperado de cada página: {"pagina": int, "siguiente": str|null, "total": int, "ordenes": [...]}.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlsplit

import requests
from sqlalchemy import Connection, text

from tnr.common.batch import Acquired, dedup_by_key
from tnr.common.contracts import DedupResult, QualityAlert, RawRecord, Rejection, SilverRecord
from tnr.common.tipos import parse_fecha_negocio, utc_now_iso
from tnr.config import Config

TIMEOUT = (3.05, 10)            # conexión, lectura (s)
MAX_INTENTOS = 4
BACKOFF_BASE = 0.5              # s, exponencial acotado
BACKOFF_MAX = 4.0
MAX_PAGINAS = 1000
TRANSITORIOS = {429, 500, 502, 503, 504}
ESTADOS = {"recibida", "en_reparacion", "cerrada"}
SILVER_COLS = ["id_orden", "id_venta", "estado", "fecha_apertura", "fecha_cierre", "en_garantia", "pagina"]


class ExtraccionIncoherente(RuntimeError):
    pass


def _get(session: requests.Session, url: str) -> tuple[requests.Response, int]:
    for intento in range(1, MAX_INTENTOS + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT, headers={"Accept": "application/json"})
            if resp.status_code not in TRANSITORIOS:
                resp.raise_for_status()
                return resp, intento
            error: Exception = requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
        except (requests.ConnectionError, requests.Timeout) as exc:
            error = exc
        if intento == MAX_INTENTOS:
            raise error
        time.sleep(min(BACKOFF_BASE * 2 ** (intento - 1), BACKOFF_MAX))
    raise AssertionError("inalcanzable")


def _primera_pagina(base_url: str) -> str:
    sep = "&" if "?" in base_url else "?"
    return base_url if "page=" in base_url else f"{base_url}{sep}page=1"


class PostventaSource:
    fuente = "postventa"
    version_esquema = "postventa.v1"
    silver_table = "silver_postventa"

    def acquire(self, cfg: Config) -> Acquired:
        url = _primera_pagina(cfg.postventa_url)
        origen = urlsplit(url)[:2]
        files: dict[str, bytes] = {}
        cuerpos: list[str] = []
        visitadas: set[str] = set()
        with requests.Session() as session:
            while url:
                if url in visitadas or len(visitadas) >= MAX_PAGINAS:
                    raise ExtraccionIncoherente(f"Paginación cíclica o excesiva en {url}")
                visitadas.add(url)
                resp, intentos = _get(session, url)
                n = len(visitadas)
                cuerpo = f"pagina_{n:03d}.json"
                files[cuerpo] = resp.content
                files[f"pagina_{n:03d}.meta.json"] = json.dumps({
                    "url": url, "pagina_solicitada": n, "estado_http": resp.status_code, "intentos": intentos,
                    "extraido_en": utc_now_iso(), "content_type": resp.headers.get("Content-Type"),
                }, indent=2).encode()
                cuerpos.append(cuerpo)
                siguiente = resp.json().get("siguiente")
                if siguiente:
                    nxt = urljoin(url, siguiente)
                    if urlsplit(nxt)[:2] != origen:
                        raise ExtraccionIncoherente(f"'siguiente' sale del origen: {nxt}")
                    url = nxt
                else:
                    url = None
        return Acquired(files=files, ruta=cfg.postventa_url, fingerprint=cuerpos)

    def extract(self, cfg: Config, bronze_dir: Path) -> Iterable[RawRecord]:
        paginas = sorted(p for p in bronze_dir.glob("pagina_*.json") if not p.name.endswith(".meta.json"))
        bodies = [json.loads(p.read_text(encoding="utf-8")) for p in paginas]
        totales = {b.get("total") for b in bodies}
        n_ordenes = sum(len(b.get("ordenes") or []) for b in bodies)
        numeros = [b.get("pagina") for b in bodies]
        if len(totales) != 1 or n_ordenes != next(iter(totales)) or numeros != list(range(1, len(bodies) + 1)) \
                or bodies[-1].get("siguiente") is not None:
            raise ExtraccionIncoherente(
                f"Lote incoherente: total={totales}, órdenes={n_ordenes}, páginas={numeros}")
        for path, body in zip(paginas, bodies):
            for i, orden in enumerate(body["ordenes"]):
                yield RawRecord(ref=f"{path.name}#ordenes[{i}]", data={**orden, "pagina": body["pagina"]},
                                key=str(orden.get("id_orden") or "").strip().upper())

    def deduplicate(self, raws: list[RawRecord]) -> DedupResult:
        return dedup_by_key(raws, ignore={"pagina"})

    def normalize(self, record: RawRecord) -> SilverRecord | Rejection:
        d = record.data
        alerts: list[QualityAlert] = []
        errores = []
        id_orden = str(d.get("id_orden") or "").strip().upper()
        id_venta = str(d.get("id_venta") or "").strip().upper()
        estado = str(d.get("estado") or "").strip().lower()
        apertura = parse_fecha_negocio(d.get("fecha_apertura"))
        cierre = parse_fecha_negocio(d.get("fecha_cierre"))
        garantia = d.get("en_garantia")
        if isinstance(garantia, str) and garantia.strip().lower() in {"true", "false"}:
            alerts.append(QualityAlert("en_garantia", "BOOLEANO_COMO_TEXTO", garantia))
            garantia = garantia.strip().lower() == "true"

        if not re.fullmatch(r"ST-\d{4}", id_orden):
            errores.append("ID_ORDEN_INVALIDO")
        if not re.fullmatch(r"V\d{6}", id_venta):
            errores.append("ID_VENTA_INVALIDO")
        if estado not in ESTADOS:
            errores.append("ESTADO_INVALIDO")
        if not isinstance(garantia, bool):
            errores.append("EN_GARANTIA_NO_BOOLEANO")
        if apertura is None:
            errores.append("FECHA_APERTURA_INVALIDA")
        if d.get("fecha_cierre") is not None and cierre is None:
            errores.append("FECHA_CIERRE_INVALIDA")
        if estado == "cerrada" and cierre is None:
            errores.append("CERRADA_SIN_FECHA_CIERRE")
        if estado in ESTADOS - {"cerrada"} and cierre is not None:
            errores.append("ABIERTA_CON_FECHA_CIERRE")
        if apertura and cierre and cierre < apertura:
            errores.append("CIERRE_ANTERIOR_A_APERTURA")
        if errores:
            return Rejection(record.ref, id_orden or None, errores[0], ",".join(errores))
        return SilverRecord(ref=record.ref, key=id_orden, alerts=alerts, data={
            "id_orden": id_orden, "id_venta": id_venta, "estado": estado, "fecha_apertura": apertura.isoformat(),
            "fecha_cierre": cierre.isoformat() if cierre else None, "en_garantia": int(garantia),
            "pagina": d["pagina"]})

    def persist(self, conn: Connection, records: list[SilverRecord], run_id: str) -> int:
        conn.execute(text("DELETE FROM silver_postventa"))
        cols = SILVER_COLS + ["run_id"]
        if records:
            conn.execute(text(f"INSERT INTO silver_postventa ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)})"),
                         [{**r.data, "run_id": run_id} for r in records])
        return len(records)
