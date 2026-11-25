"""Inventario inicial (snapshot por lote). Cada snapshot representa los movimientos ya incorporados hasta su corte."""
from __future__ import annotations

import re

from sqlalchemy import Connection, text

from tnr.common.contracts import RawRecord, Rejection, SilverRecord
from tnr.common.eventos_web import parse_ts
from tnr.common.tipos import parse_int, to_utc_iso
from tnr.ingest._csv import CsvSnapshotSource

SILVER_COLS = ["id_producto", "id_ubicacion", "fecha_corte", "stock_fisico", "stock_reservado", "umbral_reposicion"]


class InventarioSource(CsvSnapshotSource):
    fuente = "inventario"
    version_esquema = "inventario.v1"
    silver_table = "silver_inventario"
    archivo = "inventario_inicial.csv"
    key_field = "id_producto"
    silver_cols = SILVER_COLS

    def __init__(self, tz: str = "America/Santiago"):
        self.tz = tz

    def extract(self, cfg, bronze_dir):
        self.tz = cfg.tz
        for r in super().extract(cfg, bronze_dir):
            d = r.data
            r.key = "|".join((d.get(k) or "").strip().upper() for k in ("id_producto", "id_ubicacion", "fecha_corte"))
            yield r

    def normalize(self, record: RawRecord) -> SilverRecord | Rejection:
        d = record.data
        sku = (d.get("id_producto") or "").strip().upper()
        ubic = (d.get("id_ubicacion") or "").strip().upper()
        corte, _ = parse_ts(d.get("fecha_corte"), self.tz)
        fisico, reservado, umbral = (parse_int(d.get(k)) for k in ("stock_fisico", "stock_reservado", "umbral_reposicion"))
        errores = []
        if not re.fullmatch(r"P\d{3}", sku):
            errores.append("SKU_INVALIDO")
        if not re.fullmatch(r"(CD|T)\d{2}", ubic):
            errores.append("UBICACION_INVALIDA")
        if corte is None:
            errores.append("FECHA_CORTE_INVALIDA")
        if None in (fisico, reservado, umbral):
            errores.append("CANTIDAD_NO_ENTERA")
        elif not fisico >= reservado >= 0:
            errores.append("STOCK_INCOHERENTE")
        elif umbral < 0:
            errores.append("UMBRAL_NEGATIVO")
        if errores:
            return Rejection(record.ref, record.key, errores[0], ",".join(errores))
        return SilverRecord(ref=record.ref, key=record.key, data={
            "id_producto": sku, "id_ubicacion": ubic, "fecha_corte": to_utc_iso(corte), "stock_fisico": fisico,
            "stock_reservado": reservado, "umbral_reposicion": umbral})

    def persist(self, conn: Connection, records: list[SilverRecord], run_id: str) -> int:
        if not records:
            return 0
        cols = SILVER_COLS + ["run_id"]
        conn.execute(text(
            f"INSERT INTO silver_inventario ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)})"
            " ON CONFLICT (id_producto, id_ubicacion, fecha_corte) DO UPDATE SET stock_fisico = excluded.stock_fisico,"
            " stock_reservado = excluded.stock_reservado, umbral_reposicion = excluded.umbral_reposicion,"
            " run_id = excluded.run_id"), [{**r.data, "run_id": run_id} for r in records])
        return len(records)
