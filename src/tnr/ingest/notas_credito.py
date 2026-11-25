"""Notas de crédito: grano una nota; la validez contra la venta se evalúa en la publicación."""
from __future__ import annotations

import re

from tnr.common.contracts import RawRecord, Rejection, SilverRecord
from tnr.common.tipos import parse_fecha_negocio, parse_int
from tnr.ingest._csv import CsvSnapshotSource


class NotasCreditoSource(CsvSnapshotSource):
    fuente = "notas_credito"
    version_esquema = "notas_credito.v1"
    silver_table = "silver_notas_credito"
    archivo = "notas_credito.csv"
    key_field = "id_nota"
    silver_cols = ["id_nota", "id_venta_origen", "fecha_emision", "monto_clp"]

    def normalize(self, record: RawRecord) -> SilverRecord | Rejection:
        d = record.data
        id_nota = (d.get("id_nota") or "").strip().upper()
        venta = (d.get("id_venta_origen") or "").strip().upper()
        raw_fecha = (d.get("fecha_emision") or "").strip()
        fecha = parse_fecha_negocio(raw_fecha)
        monto = parse_int(d.get("monto_clp"))
        errores = []
        if not re.fullmatch(r"NC-\d{4}", id_nota):
            errores.append("ID_NOTA_INVALIDO")
        if not re.fullmatch(r"V\d{6}", venta):
            errores.append("ID_VENTA_ORIGEN_INVALIDO")
        if fecha is None or fecha.isoformat() != raw_fecha:
            errores.append("FECHA_EMISION_NO_ISO")
        if monto is None or monto <= 0:
            errores.append("MONTO_NO_POSITIVO")
        if errores:
            return Rejection(record.ref, id_nota or None, errores[0], ",".join(errores))
        return SilverRecord(ref=record.ref, key=id_nota, data={
            "id_nota": id_nota, "id_venta_origen": venta, "fecha_emision": fecha.isoformat(), "monto_clp": monto})
