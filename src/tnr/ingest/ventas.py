"""Ventas transaccionales: base fuente SQLite (ventas_db.sql) o CSV ampliado equivalente."""
from __future__ import annotations

import csv
import io
import re
import sqlite3
from pathlib import Path
from typing import Iterable

from sqlalchemy import Connection, text

from tnr.common.batch import Acquired, dedup_by_key
from tnr.common.contracts import (DedupResult, QualityAlert, RawRecord, Rejection, SilverRecord)
from tnr.common.identidad import normalize_rut
from tnr.common.tipos import clean_text, parse_clp, parse_fecha_negocio, parse_int
from tnr.config import Config

CAMPOS = ["fila_origen", "id_venta", "fecha", "rut_cliente", "nombre_cliente", "id_producto", "id_tienda",
          "ciudad_tienda", "cantidad", "precio_unitario", "canal", "observaciones", "id_pedido_omnicanal",
          "modalidad_entrega", "id_ubicacion_despacho", "estado_venta", "costo_unitario_aplicado_clp"]
ESTADOS = {"confirmada", "pendiente", "cancelada"}
CANALES = {"tienda": "Tienda", "online": "Online"}
CANTIDAD_ERROR = 999          # regla de la simulación
ANIO = 2025
SILVER_COLS = ["id_venta", "fecha", "cantidad", "precio_unitario_clp", "costo_unitario_clp", "canal",
               "estado_venta", "id_producto", "id_tienda", "ciudad_tienda", "id_pedido_omnicanal",
               "modalidad_entrega", "id_ubicacion_despacho", "observaciones", "estado_vinculo_cliente",
               "fila_origen"]


class VentasSource:
    fuente = "ventas"
    version_esquema = "ventas.v1"
    silver_table = "silver_ventas"

    def __init__(self, input_kind: str = "sql"):
        if input_kind not in {"sql", "csv"}:
            raise ValueError("input debe ser 'sql' o 'csv'")
        self.input_kind = input_kind

    # ---------- Bronze ----------
    def _ensure_source_db(self, cfg: Config) -> Path:
        db = cfg.source_db_path
        if not db.exists():
            db.parent.mkdir(parents=True, exist_ok=True)
            con = sqlite3.connect(db)
            try:
                con.executescript((cfg.input_dir / "ventas_db.sql").read_text(encoding="utf-8"))
                con.commit()
            finally:
                con.close()
        return db

    def acquire(self, cfg: Config) -> Acquired:
        if self.input_kind == "csv":
            path = cfg.input_dir / "ventas_2025_tnr_ampliadas.csv"
            return Acquired(files={path.name: path.read_bytes()}, ruta=str(path))
        db = self._ensure_source_db(cfg)
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            rows = con.execute(f"SELECT {', '.join(CAMPOS)} FROM ventas_crudas ORDER BY fila_origen").fetchall()
        finally:
            con.close()
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=";", lineterminator="\n")
        w.writerow(CAMPOS)
        w.writerows(["" if v is None else v for v in row] for row in rows)
        return Acquired(files={"ventas_crudas.csv": buf.getvalue().encode("utf-8")},
                        ruta=f"sqlite:{db}#ventas_crudas ORDER BY fila_origen")

    def extract(self, cfg: Config, bronze_dir: Path) -> Iterable[RawRecord]:
        name = "ventas_crudas.csv" if self.input_kind == "sql" else "ventas_2025_tnr_ampliadas.csv"
        path = bronze_dir / name
        if not path.exists():  # reproceso de un lote archivado con la otra entrada
            path = next(p for p in bronze_dir.glob("*.csv"))
        with path.open(encoding="utf-8", newline="") as fh:
            for i, row in enumerate(csv.DictReader(fh, delimiter=";"), start=1):
                row.setdefault("fila_origen", str(i))
                row["fila_origen"] = int(row["fila_origen"])
                yield RawRecord(ref=f"{path.name}#fila_origen={row['fila_origen']}", data=row,
                                key=(row.get("id_venta") or "").strip())

    # ---------- Staging ----------
    def deduplicate(self, raws: list[RawRecord]) -> DedupResult:
        return dedup_by_key(raws, ignore={"observaciones"}, order_field="fila_origen")

    def normalize(self, record: RawRecord) -> SilverRecord | Rejection:
        d = record.data
        errores: list[str] = []
        alerts: list[QualityAlert] = []

        id_venta = (d.get("id_venta") or "").strip()
        if not re.fullmatch(r"V\d{6}", id_venta):
            errores.append("ID_VENTA_INVALIDO")

        fecha = parse_fecha_negocio(d.get("fecha"))
        if fecha is None:
            errores.append("FECHA_INVALIDA")
        elif fecha.year != ANIO:
            errores.append("FECHA_FUERA_DE_PERIODO")

        cantidad = parse_int(d.get("cantidad"))
        if cantidad is None:
            errores.append("CANTIDAD_NO_INTERPRETABLE")
        elif cantidad == CANTIDAD_ERROR:
            errores.append("CANTIDAD_ERROR_999")
        elif cantidad <= 0:
            errores.append("CANTIDAD_NO_POSITIVA")

        precio = parse_clp(d.get("precio_unitario"))
        if precio is None:
            errores.append("PRECIO_NO_INTERPRETABLE")
        elif precio <= 0:
            errores.append("PRECIO_NO_POSITIVO")

        estado = (d.get("estado_venta") or "").strip().lower()
        if estado not in ESTADOS:
            errores.append("ESTADO_INVALIDO")

        canal = CANALES.get((d.get("canal") or "").strip().lower())
        if canal is None:
            errores.append("CANAL_INVALIDO")

        id_producto = (d.get("id_producto") or "").strip().upper()
        if not re.fullmatch(r"P\d{3}", id_producto):
            errores.append("ID_PRODUCTO_INVALIDO")

        if errores:
            return Rejection(ref=record.ref, key=id_venta or None, motivo=errores[0], detalle=",".join(errores))

        costo = parse_int(d.get("costo_unitario_aplicado_clp"))
        if costo is None:
            alerts.append(QualityAlert("costo_unitario_aplicado_clp", "COSTO_AUSENTE", d.get("costo_unitario_aplicado_clp")))
        elif costo < 0:
            alerts.append(QualityAlert("costo_unitario_aplicado_clp", "COSTO_NEGATIVO", costo))
            costo = None

        raw_fecha = (d.get("fecha") or "").strip()
        if raw_fecha != fecha.isoformat():
            alerts.append(QualityAlert("fecha", "FECHA_FORMATO_ALTERNATIVO", raw_fecha))

        rut, rut_error = normalize_rut(d.get("rut_cliente"))
        if rut_error and rut_error != "RUT_AUSENTE":
            alerts.append(QualityAlert("rut_cliente", rut_error, None))  # el valor original queda solo en Bronze

        data = {
            "id_venta": id_venta,
            "fecha": fecha.isoformat(),
            "cantidad": cantidad,
            "precio_unitario_clp": precio,
            "costo_unitario_clp": costo,
            "canal": canal,
            "estado_venta": estado,
            "id_producto": id_producto,
            "id_tienda": clean_text(d.get("id_tienda")),
            "ciudad_tienda": clean_text(d.get("ciudad_tienda")),
            "id_pedido_omnicanal": clean_text(d.get("id_pedido_omnicanal")),
            "modalidad_entrega": clean_text(d.get("modalidad_entrega")),
            "id_ubicacion_despacho": clean_text(d.get("id_ubicacion_despacho")),
            "observaciones": clean_text(d.get("observaciones")),
            "estado_vinculo_cliente": "rut_disponible" if rut else "pendiente",
            "fila_origen": d["fila_origen"],
        }
        return SilverRecord(ref=record.ref, key=id_venta, data=data, alerts=alerts,
                            restricted={"rut_normalizado": rut} if rut else None)

    # ---------- Silver (snapshot completo: recarga atómica) ----------
    def persist(self, conn: Connection, records: list[SilverRecord], run_id: str) -> int:
        conn.execute(text("DELETE FROM silver_ventas_identidad"))
        conn.execute(text("DELETE FROM silver_ventas"))
        cols = SILVER_COLS + ["run_id"]
        conn.execute(text(f"INSERT INTO silver_ventas ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)})"),
                     [{**r.data, "run_id": run_id} for r in records])
        ident = [{"id_venta": r.key, "rut": r.restricted["rut_normalizado"], "run_id": run_id}
                 for r in records if r.restricted]
        if ident:
            conn.execute(text("INSERT INTO silver_ventas_identidad (id_venta, rut_normalizado, run_id)"
                              " VALUES (:id_venta, :rut, :run_id)"), ident)
        return len(records)
