"""Normalización de identidad: RUT chileno (módulo 11), correo y teléfono."""
from __future__ import annotations

import re
from typing import Any

from tnr.common.tipos import is_blank


def dv_rut(cuerpo: int) -> str:
    total, factor = 0, 2
    for digito in reversed(str(cuerpo)):
        total += int(digito) * factor
        factor = 2 if factor == 7 else factor + 1
    resto = 11 - total % 11
    return {11: "0", 10: "K"}.get(resto, str(resto))


def normalize_rut(value: Any) -> tuple[str | None, str | None]:
    """Devuelve (rut_normalizado 'NNNNNNNN-D', codigo_error)."""
    if is_blank(value):
        return None, "RUT_AUSENTE"
    text = re.sub(r"[.\s]", "", str(value)).upper()
    m = re.fullmatch(r"(\d{6,9})-?([\dK])", text)
    if not m:
        return None, "RUT_FORMATO_INVALIDO"
    cuerpo, dv = int(m.group(1)), m.group(2)
    if dv_rut(cuerpo) != dv:
        return None, "RUT_DV_INVALIDO"
    return f"{cuerpo}-{dv}", None


def normalize_email(value: Any) -> str | None:
    if is_blank(value):
        return None
    text = str(value).strip().lower()
    return text if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", text) else None


def normalize_phone(value: Any) -> str | None:
    if is_blank(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) == 9 and digits.startswith("9"):
        digits = "56" + digits
    return f"+{digits}" if re.fullmatch(r"569\d{8}", digits) else None
