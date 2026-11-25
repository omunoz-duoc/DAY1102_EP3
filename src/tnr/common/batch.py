"""Ejecución común de lotes: Bronze -> staging -> Silver con control e idempotencia."""
from __future__ import annotations

import hashlib
import json
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol

import pandas as pd
from sqlalchemy import Connection, text

from tnr.common.contracts import (DedupResult, LoadResult, RawRecord, Rejection, SilverRecord)
from tnr.common.db import get_engine
from tnr.common.tipos import utc_now_iso
from tnr.config import Config


@dataclass
class Acquired:
    files: dict[str, bytes]   # nombre -> contenido original
    ruta: str                 # origen legible (archivo, base, URL)
    fingerprint: list[str] | None = None   # archivos que definen la huella (default: todos)


class BatchSource(Protocol):
    fuente: str
    version_esquema: str
    silver_table: str

    def acquire(self, cfg: Config) -> Acquired: ...
    def extract(self, cfg: Config, bronze_dir: Path) -> Iterable[RawRecord]: ...
    def deduplicate(self, raws: list[RawRecord]) -> DedupResult: ...
    def normalize(self, record: RawRecord) -> SilverRecord | Rejection: ...
    def persist(self, conn: Connection, records: list[SilverRecord], run_id: str) -> int: ...


def new_run_id(prefix: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{prefix}-{ts}-{uuid.uuid4().hex[:6]}"


def sha256_files(files: dict[str, bytes]) -> str:
    h = hashlib.sha256()
    for name in sorted(files):
        h.update(name.encode())
        h.update(b"\0")
        h.update(files[name])
    return h.hexdigest()


def _last_success(conn: Connection, fuente: str, sha: str | None = None, version: str | None = None):
    sql = "SELECT * FROM etl_runs WHERE fuente = :f AND estado = 'exitoso'"
    params: dict[str, Any] = {"f": fuente}
    if sha:
        sql += " AND sha256 = :s"
        params["s"] = sha
    if version:
        sql += " AND version_esquema = :v"
        params["v"] = version
    return conn.execute(text(sql + " ORDER BY ts_ingesta DESC LIMIT 1"), params).mappings().first()


def archive_bronze(cfg: Config, fuente: str, run_id: str, acq: Acquired, sha: str, ts: str) -> Path:
    target = cfg.bronze_dir / fuente / ts[:10] / run_id
    target.mkdir(parents=True, exist_ok=False)
    for name, content in acq.files.items():
        (target / name).write_bytes(content)
    manifest = {"run_id": run_id, "fuente": fuente, "ruta": acq.ruta, "sha256": sha, "ts_ingesta": ts,
                "archivos": {n: hashlib.sha256(c).hexdigest() for n, c in acq.files.items()}}
    (target / "_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    return target


def export_parquet(cfg: Config, conn: Connection, table: str, layer: str, version_id: str,
                   where: str = "", params: dict | None = None) -> str:
    df = pd.read_sql_query(text(f"SELECT * FROM {table} {where}"), conn, params=params or {})
    path = cfg.parquet_dir / layer / table / f"{version_id}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow", index=False)
    return str(path)


def _insert_many(conn: Connection, sql: str, rows: list[dict]) -> None:
    if rows:
        conn.execute(text(sql), rows)


def run_batch(source: BatchSource, cfg: Config, *, force: bool = False, reproceso: bool = False) -> LoadResult:
    engine = get_engine(cfg.warehouse_path)
    fuente, version = source.fuente, source.version_esquema
    run_id = new_run_id(fuente)
    ts = utc_now_iso()
    modo = "reproceso" if reproceso else ("forzado" if force else "normal")

    # 1. Adquirir y archivar (o reutilizar Bronze en un reproceso)
    with engine.connect() as conn:
        if reproceso:
            prev = _last_success(conn, fuente)
            if prev is None:
                raise RuntimeError(f"No hay un lote exitoso de '{fuente}' para reprocesar desde Bronze")
            sha, ruta, bronze = prev["sha256"], prev["ruta"], Path(prev["bronze_path"])
        else:
            acq = source.acquire(cfg)
            sha = sha256_files({n: acq.files[n] for n in (acq.fingerprint or acq.files)})
            ruta = acq.ruta
            prev = _last_success(conn, fuente, sha, version)
            if prev is not None and not force:
                conn.execute(text(
                    "INSERT INTO etl_runs (run_id, fuente, ruta, sha256, version_esquema, modo, ts_ingesta, ts_fin,"
                    " estado, detalle) VALUES (:r, :f, :ru, :s, :v, :m, :t, :t, 'omitido', :d)"),
                    {"r": run_id, "f": fuente, "ru": ruta, "s": sha, "v": version, "m": modo, "t": ts,
                     "d": json.dumps({"lote_vigente": prev["run_id"]})})
                conn.commit()
                return LoadResult(run_id=run_id, fuente=fuente, estado="omitido", sha256=sha,
                                  bronze_path=prev["bronze_path"], detalle={"lote_vigente": prev["run_id"]})
            bronze = archive_bronze(cfg, fuente, run_id, acq, sha, ts)

        conn.execute(text(
            "INSERT INTO etl_runs (run_id, fuente, ruta, sha256, version_esquema, modo, ts_ingesta, estado, bronze_path)"
            " VALUES (:r, :f, :ru, :s, :v, :m, :t, 'en_proceso', :b)"),
            {"r": run_id, "f": fuente, "ru": ruta, "s": sha, "v": version, "m": modo, "t": ts, "b": str(bronze)})
        conn.commit()

    result = LoadResult(run_id=run_id, fuente=fuente, estado="en_proceso", sha256=sha, bronze_path=str(bronze))
    try:
        # 2. Staging: extraer, deduplicar, normalizar
        raws = list(source.extract(cfg, bronze))
        dedup = source.deduplicate(raws)
        silver: list[SilverRecord] = []
        rejections: list[Rejection] = []
        for raw in dedup.kept:
            out = source.normalize(raw)
            (silver if isinstance(out, SilverRecord) else rejections).append(out)

        result.recibidos = len(raws)
        result.excluidos = len(dedup.exclusions)
        result.revision = len(dedup.reviews)
        result.rechazados = len(rejections)

        # 3. Publicar Silver + control + estado del lote en una sola transacción
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE etl_runs SET estado = 'reemplazado' WHERE fuente = :f AND sha256 = :s"
                " AND version_esquema = :v AND estado = 'exitoso'"), {"f": fuente, "s": sha, "v": version})
            result.publicados = source.persist(conn, silver, run_id)
            result.alertas = sum(len(s.alerts) for s in silver)   # persist puede agregar alertas
            _insert_many(conn,
                "INSERT INTO etl_rechazos (run_id, fuente, ref_bronze, id_registro, motivo, detalle)"
                " VALUES (:run_id, :fuente, :ref, :key, :motivo, :detalle)",
                [{"run_id": run_id, "fuente": fuente, "ref": r.ref, "key": r.key, "motivo": r.motivo,
                  "detalle": r.detalle} for r in rejections])
            _insert_many(conn,
                "INSERT INTO etl_exclusiones (run_id, fuente, ref_bronze, id_registro, motivo, ref_conservada)"
                " VALUES (:run_id, :fuente, :ref, :key, :motivo, :ref_conservada)",
                [{"run_id": run_id, "fuente": fuente, "ref": e.ref, "key": e.key, "motivo": e.motivo,
                  "ref_conservada": e.ref_conservada} for e in dedup.exclusions])
            _insert_many(conn,
                "INSERT INTO etl_revision (run_id, fuente, id_registro, motivo, evidencia)"
                " VALUES (:run_id, :fuente, :key, :motivo, :evidencia)",
                [{"run_id": run_id, "fuente": fuente, "key": r.key, "motivo": r.motivo,
                  "evidencia": json.dumps(r.evidencia, ensure_ascii=False, default=str)} for r in dedup.reviews])
            # persist puede registrar revisiones propias (p. ej. resolución de identidad)
            result.revision = conn.execute(text("SELECT COUNT(*) FROM etl_revision WHERE run_id = :r"),
                                           {"r": run_id}).scalar_one()
            _insert_many(conn,
                "INSERT INTO etl_alertas_calidad (run_id, fuente, ref_bronze, id_registro, campo, codigo, valor_original)"
                " VALUES (:run_id, :fuente, :ref, :key, :campo, :codigo, :valor)",
                [{"run_id": run_id, "fuente": fuente, "ref": s.ref, "key": s.key, "campo": a.campo,
                  "codigo": a.codigo, "valor": None if a.valor_original is None else str(a.valor_original)}
                 for s in silver for a in s.alerts])
            conn.execute(text(
                "UPDATE etl_runs SET estado = 'exitoso', ts_fin = :t, n_recibidos = :rec, n_excluidos = :exc,"
                " n_revision = :rev, n_rechazados = :rej, n_publicados = :pub, n_alertas = :ale WHERE run_id = :r"),
                {"t": utc_now_iso(), "rec": result.recibidos, "exc": result.excluidos, "rev": result.revision,
                 "rej": result.rechazados, "pub": result.publicados, "ale": result.alertas, "r": run_id})
        result.estado = "exitoso"

        # 4. Snapshot Parquet de la tabla publicada
        with engine.begin() as conn:
            result.parquet_path = export_parquet(cfg, conn, source.silver_table, "silver", run_id)
            conn.execute(text("UPDATE etl_runs SET parquet_path = :p WHERE run_id = :r"),
                         {"p": result.parquet_path, "r": run_id})
    except Exception:
        with engine.begin() as conn:
            conn.execute(text("UPDATE etl_runs SET estado = 'fallido', ts_fin = :t, detalle = :d WHERE run_id = :r"),
                         {"t": utc_now_iso(), "d": traceback.format_exc()[-4000:], "r": run_id})
        raise
    return result


def dedup_by_key(raws: list[RawRecord], *, ignore: set[str] = frozenset(), order_field: str | None = None,
                 motivo_igual: str = "DUPLICADO_EXACTO") -> DedupResult:
    """Agrupa por record.key. Si las diferencias se limitan a `ignore`, conserva el primero
    (por order_field o por orden de llegada) y cuenta los demás. Si difieren campos de negocio, el grupo
    completo va a revisión."""
    from tnr.common.contracts import Exclusion, Review

    groups: dict[str, list[RawRecord]] = {}
    for r in raws:
        groups.setdefault(r.key or r.ref, []).append(r)
    result = DedupResult(kept=[])
    for key, group in groups.items():
        if len(group) == 1:
            result.kept.append(group[0])
            continue
        if order_field:
            group = sorted(group, key=lambda r: r.data[order_field])
        business = {tuple(sorted((k, v) for k, v in r.data.items() if k not in ignore and k != order_field))
                    for r in group}
        if len(business) > 1:
            result.reviews.append(Review(key=key, motivo="CONFLICTO_CAMPOS_NEGOCIO",
                                         evidencia=[{"ref": r.ref, **r.data} for r in group]))
            continue
        first = group[0]
        result.kept.append(first)
        base = {k: v for k, v in first.data.items() if k != order_field}
        for dup in group[1:]:
            same = {k: v for k, v in dup.data.items() if k != order_field} == base
            motivo = motivo_igual if same else "DUPLICADO_DIFIERE_" + "_".join(sorted(ignore)).upper()
            result.exclusions.append(Exclusion(ref=dup.ref, key=key, motivo=motivo, ref_conservada=first.ref))
    return result
