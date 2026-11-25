"""Catálogo de productos desde XLSX (hoja `productos`)."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import Connection, text

from tnr.common.batch import Acquired, dedup_by_key
from tnr.common.contracts import (DedupResult, QualityAlert, RawRecord, Rejection, SilverRecord)
from tnr.common.tipos import clean_text, is_blank, parse_clp, strip_accents
from tnr.config import Config

ARCHIVO = "productos_tnr_ampliados.xlsx"
HOJA = "productos"
CATEGORIAS = {  # mapa canónico: clave sin tildes en minúsculas -> etiqueta publicada
    "computacion": "Computación",
    "telefonia": "Telefonía",
    "audio": "Audio",
    "accesorios": "Accesorios",
    "gaming": "Gaming",
    "smart home": "Smart Home",
}
SILVER_COLS = ["id_producto", "nombre_producto", "categoria", "subcategoria", "marca", "precio_lista_clp",
               "costo_referencia_clp"]


class ProductosSource:
    fuente = "productos"
    version_esquema = "productos.v1"
    silver_table = "silver_productos"

    def acquire(self, cfg: Config) -> Acquired:
        path = cfg.input_dir / ARCHIVO
        return Acquired(files={path.name: path.read_bytes()}, ruta=f"{path}#{HOJA}")

    def extract(self, cfg: Config, bronze_dir: Path) -> Iterable[RawRecord]:
        df = pd.read_excel(bronze_dir / ARCHIVO, sheet_name=HOJA, dtype=str, engine="openpyxl")
        for i, row in enumerate(df.to_dict(orient="records"), start=2):  # fila 1 = encabezado
            data = {k: (None if is_blank(v) else str(v)) for k, v in row.items()}
            yield RawRecord(ref=f"{ARCHIVO}#{HOJA}!fila={i}", data=data,
                            key=(data.get("id_producto") or "").strip().upper())

    def deduplicate(self, raws: list[RawRecord]) -> DedupResult:
        return dedup_by_key(raws)

    def normalize(self, record: RawRecord) -> SilverRecord | Rejection:
        d = record.data
        sku = (d.get("id_producto") or "").strip().upper()
        if not re.fullmatch(r"P\d{3}", sku):
            return Rejection(record.ref, sku or None, "SKU_INVALIDO")
        alerts: list[QualityAlert] = []

        cat_raw = clean_text(d.get("categoria"))
        categoria = CATEGORIAS.get(strip_accents(cat_raw).lower()) if cat_raw else None
        if categoria is None:
            alerts.append(QualityAlert("categoria", "CATEGORIA_NO_MAPEADA", cat_raw))
        elif categoria != cat_raw:
            alerts.append(QualityAlert("categoria", "CATEGORIA_NORMALIZADA", cat_raw))

        subcategoria = clean_text(d.get("subcategoria"))
        if subcategoria is None:
            alerts.append(QualityAlert("subcategoria", "SUBCATEGORIA_AUSENTE", None))

        marca = clean_text(d.get("marca"))
        if marca is None:
            alerts.append(QualityAlert("marca", "MARCA_AUSENTE", None))

        precio = parse_clp(d.get("precio_lista"))
        if precio is None or precio <= 0:
            alerts.append(QualityAlert("precio_lista", "PRECIO_LISTA_NO_INTERPRETABLE", d.get("precio_lista")))
            precio = None
        elif str(d.get("precio_lista")).strip() != str(precio):
            alerts.append(QualityAlert("precio_lista", "PRECIO_LISTA_CON_FORMATO", d.get("precio_lista")))

        costo = parse_clp(d.get("costo_unitario_referencia_clp"))
        if costo is None:
            alerts.append(QualityAlert("costo_unitario_referencia_clp", "COSTO_REFERENCIA_AUSENTE",
                                       d.get("costo_unitario_referencia_clp")))
        elif costo < 0:
            alerts.append(QualityAlert("costo_unitario_referencia_clp", "COSTO_REFERENCIA_NEGATIVO", costo))
            costo = None

        data = {"id_producto": sku, "nombre_producto": clean_text(d.get("nombre_producto")), "categoria": categoria,
                "subcategoria": subcategoria, "marca": marca, "precio_lista_clp": precio,
                "costo_referencia_clp": costo}
        return SilverRecord(ref=record.ref, key=sku, data=data, alerts=alerts)

    def persist(self, conn: Connection, records: list[SilverRecord], run_id: str) -> int:
        conn.execute(text("DELETE FROM silver_productos"))
        cols = SILVER_COLS + ["run_id"]
        conn.execute(text(f"INSERT INTO silver_productos ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)})"),
                     [{**r.data, "run_id": run_id} for r in records])
        return len(records)
