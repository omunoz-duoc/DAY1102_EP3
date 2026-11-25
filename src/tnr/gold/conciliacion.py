"""Cascada de conciliación de ventas: crudo -> deduplicado -> validado -> confirmado al corte -> neto.

El escenario `referencia_docente` se calcula aparte desde el XLSX docente y no se mezcla con el ampliado.
"""
from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Connection, text

from tnr.common.tipos import parse_clp, parse_int
from tnr.config import Config

FICHA_DOCENTE_CLP = 48_500_012_180     # "Verificación canon" de 5_DOCENTE_Anomalias
FICHA_DOCENTE_TRANSACCIONES = 36_988   # 0_Ficha_Tecnica


def _monto_crudo(row: dict) -> int:
    q, p = parse_int(row.get("cantidad")), parse_clp(row.get("precio_unitario"))
    return q * p if q is not None and p is not None else 0


def _bronze_rows(run: dict) -> list[dict]:
    path = next(Path(run["bronze_path"]).glob("*.csv"))
    with path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh, delimiter=";"))
    for i, r in enumerate(rows, start=1):
        r.setdefault("fila_origen", str(i))
    return rows


@lru_cache(maxsize=2)
def _referencia_docente(path: str) -> dict[str, int]:
    """Aplica al XLSX docente las mismas reglas del pipeline (deduplicación + validación de campos)."""
    from tnr.common.contracts import RawRecord, SilverRecord
    from tnr.ingest.ventas import VentasSource

    df = pd.read_excel(path, sheet_name="1_Ventas_Crudo", dtype=str)
    src = VentasSource("csv")
    raws = []
    for i, row in enumerate(df.to_dict(orient="records"), start=1):
        data = {k: ("" if v is None or (isinstance(v, float) and v != v) else str(v)) for k, v in row.items()}
        data.update(fila_origen=i, estado_venta="confirmada")   # la base docente no tiene estados
        raws.append(RawRecord(ref=f"ref#{i}", data=data, key=data["id_venta"].strip()))
    kept = src.deduplicate(raws).kept
    ok = [r for r in map(src.normalize, kept) if isinstance(r, SilverRecord)]
    return {"filas_crudas": len(raws), "filas_dedup": len(kept), "filas": len(ok),
            "monto": sum(r.data["cantidad"] * r.data["precio_unitario_clp"] for r in ok)}


def publicar(cfg: Config, conn: Connection, corte: str, publish_id: str, run_ids: dict[str, list[str]]) -> None:
    ventas_run_id = run_ids["silver_ventas"][0]
    run = dict(conn.execute(text("SELECT * FROM etl_runs WHERE run_id = :r"), {"r": ventas_run_id}).mappings().one())
    crudas = _bronze_rows(run)
    excl = {r[0] for r in conn.execute(text("SELECT ref_bronze FROM etl_exclusiones WHERE run_id = :r"),
                                       {"r": ventas_run_id})}
    revision = {r[0] for r in conn.execute(text("SELECT id_registro FROM etl_revision WHERE run_id = :r"),
                                           {"r": ventas_run_id})}
    dedup = [r for r in crudas
             if f"{next(Path(run['bronze_path']).glob('*.csv')).name}#fila_origen={r['fila_origen']}" not in excl
             and r["id_venta"] not in revision]

    def q(sql: str) -> Any:
        return conn.execute(text(sql), {"c": corte}).one()

    validadas = q("SELECT COUNT(*), COALESCE(SUM(cantidad * precio_unitario_clp), 0) FROM silver_ventas")
    confirmadas = q("SELECT COUNT(*), COALESCE(SUM(cantidad * precio_unitario_clp), 0) FROM silver_ventas"
                    " WHERE estado_venta = 'confirmada' AND fecha <= :c")
    notas = q("SELECT COUNT(*), COALESCE(SUM(monto_clp), 0) FROM ctl_notas_evaluacion"
              " WHERE fecha_corte = :c AND estado = 'aplicada'")
    neto = q("SELECT COUNT(*), COALESCE(SUM(monto_neto_clp), 0) FROM gold_mart_ventas WHERE fecha_corte = :c")

    etapas = [
        ("ampliado", 1, "crudo_bronze", ventas_run_id, len(crudas), sum(map(_monto_crudo, crudas)),
         "Filas recibidas; monto = cantidad x precio interpretable, incluye anomalías"),
        ("ampliado", 2, "tras_deduplicacion", ventas_run_id, len(dedup), sum(map(_monto_crudo, dedup)),
         f"Se excluyen {len(excl)} duplicados de id_venta"),
        ("ampliado", 3, "tras_validacion", ventas_run_id, validadas[0], validadas[1],
         f"Se rechazan {len(dedup) - validadas[0]} filas por calidad (ver etl_rechazos)"),
        ("ampliado", 4, "confirmadas_al_corte", None, confirmadas[0], confirmadas[1],
         "Excluye pendientes, canceladas y ventas posteriores al corte"),
        ("ampliado", 5, "notas_credito_aplicadas", None, notas[0], -notas[1],
         "Abonos válidos emitidos hasta el corte"),
        ("ampliado", 6, "venta_neta", None, neto[0], neto[1], "Total de gold_mart_ventas"),
    ]
    ref_path = cfg.input_dir / "referencia" / "dataset_tnr_original.xlsx"
    if ref_path.exists():
        ref = _referencia_docente(str(ref_path))
        etapas += [
            ("referencia_docente", 1, "ficha_docente", None, FICHA_DOCENTE_TRANSACCIONES, FICHA_DOCENTE_CLP,
             "Cifra de la simulación original (0_Ficha_Tecnica / 5_DOCENTE_Anomalias)"),
            ("referencia_docente", 2, "reproduccion_reglas_pipeline", None, ref["filas"], ref["monto"],
             f"XLSX docente con las reglas del pipeline: {ref['filas_crudas']} crudas, {ref['filas_dedup']} únicas"),
            ("referencia_docente", 3, "diferencia_vs_ficha", None, ref["filas"] - FICHA_DOCENTE_TRANSACCIONES,
             ref["monto"] - FICHA_DOCENTE_CLP, "Brecha entre la reproducción y la cifra declarada"),
        ]
    conn.execute(text(
        "INSERT INTO gold_mart_conciliacion_ventas (fecha_corte, publish_id, escenario, orden, etapa, run_id, filas,"
        " monto_clp, descripcion) VALUES (:c, :p, :e, :o, :et, :r, :f, :m, :d)"),
        [{"c": corte, "p": publish_id, "e": e, "o": o, "et": et, "r": r, "f": f, "m": int(m), "d": d}
         for e, o, et, r, f, m, d in etapas])
