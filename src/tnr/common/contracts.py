"""Contratos compartidos por todos los pipelines.

    extract(config) -> Iterable[RawRecord]
    normalize(record) -> SilverRecord | Rejection
    persist(records, run_id) -> LoadResult
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class RawRecord:
    ref: str                     # referencia a Bronze (archivo#ubicación)
    data: dict[str, Any]
    key: str | None = None       # llave de negocio, si se conoce


@dataclass
class QualityAlert:
    campo: str
    codigo: str
    valor_original: Any = None


@dataclass
class SilverRecord:
    ref: str
    key: str
    data: dict[str, Any]
    alerts: list[QualityAlert] = field(default_factory=list)
    restricted: dict[str, Any] | None = None   # datos sensibles a relación restringida


@dataclass
class Rejection:
    ref: str
    key: str | None
    motivo: str
    detalle: str = ""


@dataclass
class Exclusion:
    ref: str
    key: str
    motivo: str
    ref_conservada: str


@dataclass
class Review:
    key: str
    motivo: str
    evidencia: Any


@dataclass
class DedupResult:
    kept: list[RawRecord]
    exclusions: list[Exclusion] = field(default_factory=list)
    reviews: list[Review] = field(default_factory=list)


@dataclass
class LoadResult:
    run_id: str
    fuente: str
    estado: str
    recibidos: int = 0
    excluidos: int = 0
    revision: int = 0
    rechazados: int = 0
    publicados: int = 0
    alertas: int = 0
    sha256: str | None = None
    bronze_path: str | None = None
    parquet_path: str | None = None
    detalle: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
