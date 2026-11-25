"""Despachos: un evento por despacho, cruzado por id_pedido_omnicanal en la publicación."""
from __future__ import annotations

import re

from tnr.common.contracts import RawRecord, Rejection, SilverRecord
from tnr.common.tipos import parse_fecha_negocio
from tnr.ingest._csv import CsvSnapshotSource


class DespachosSource(CsvSnapshotSource):
    fuente = "despachos"
    version_esquema = "despachos.v1"
    silver_table = "silver_despachos"
    archivo = "despachos.csv"
    key_field = "id_despacho"
    silver_cols = ["id_despacho", "id_pedido_omnicanal", "id_ubicacion_despacho", "fecha_despacho"]

    def normalize(self, record: RawRecord) -> SilverRecord | Rejection:
        d = record.data
        id_despacho = (d.get("id_despacho") or "").strip().upper()
        pedido = (d.get("id_pedido_omnicanal") or "").strip().upper()
        ubicacion = (d.get("id_ubicacion_despacho") or "").strip().upper()
        raw_fecha = (d.get("fecha_despacho") or "").strip()
        fecha = parse_fecha_negocio(raw_fecha)
        errores = []
        if not re.fullmatch(r"DSP-\d{4}", id_despacho):
            errores.append("ID_DESPACHO_INVALIDO")
        if not re.fullmatch(r"PED-V\d{6}", pedido):
            errores.append("ID_PEDIDO_INVALIDO")
        if not re.fullmatch(r"(CD|T)\d{2}", ubicacion):
            errores.append("UBICACION_INVALIDA")
        if fecha is None or fecha.isoformat() != raw_fecha:
            errores.append("FECHA_DESPACHO_INVALIDA")
        if errores:
            return Rejection(record.ref, id_despacho or None, errores[0], ",".join(errores))
        return SilverRecord(ref=record.ref, key=id_despacho, data={
            "id_despacho": id_despacho, "id_pedido_omnicanal": pedido,
            "id_ubicacion_despacho": ubicacion, "fecha_despacho": fecha.isoformat()})
