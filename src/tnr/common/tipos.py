"""Funciones pequeñas de tipado reutilizables."""
from __future__ import annotations

import math
import re
import unicodedata
from datetime import date, datetime, timezone
from typing import Any

_NULOS = {"", "nan", "none", "null", "n/a", "na", "-"}


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return str(value).strip().lower() in _NULOS


def clean_text(value: Any) -> str | None:
    if is_blank(value):
        return None
    return re.sub(r"\s+", " ", str(value)).strip()


def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def parse_int(value: Any) -> int | None:
    if is_blank(value):
        return None
    try:
        f = float(str(value).strip())
    except ValueError:
        return None
    return int(f) if f.is_integer() else None


def parse_clp(value: Any) -> int | None:
    """'$1.234.567', '-99.999', '$ 129.990', '649990' -> int. None si no es interpretable."""
    if is_blank(value):
        return None
    if isinstance(value, (int, float)):
        return int(value) if float(value).is_integer() else None
    text = str(value).strip().replace("$", "").replace(" ", "")
    if not re.fullmatch(r"-?\d{1,3}(\.\d{3})*|-?\d+", text):
        return None
    return int(text.replace(".", ""))


_FORMATOS_FECHA = ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y")


def parse_fecha_negocio(value: Any) -> date | None:
    """Fecha de negocio a date. Acepta formatos alternativos si representan una fecha válida."""
    if is_blank(value):
        return None
    text = str(value).strip()[:10]
    for fmt in _FORMATOS_FECHA:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def utc_now_iso() -> str:
    return to_utc_iso(datetime.now(timezone.utc))


def to_utc_iso(dt: datetime) -> str:
    """Formato uniforme UTC: 2025-12-26T16:00:00.000Z"""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
