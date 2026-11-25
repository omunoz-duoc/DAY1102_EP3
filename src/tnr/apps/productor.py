"""Productor mínimo: reenvía un JSONL línea a línea con POST /eventos en el orden del archivo.

El productor real vive en otro proyecto; este sirve para la defensa y las pruebas."""
from __future__ import annotations

import time
from pathlib import Path
from typing import Callable

import requests

Post = Callable[[bytes], tuple[int, dict]]


def http_post(url: str, timeout: float = 5.0) -> Post:
    session = requests.Session()

    def _post(body: bytes) -> tuple[int, dict]:
        r = session.post(url, data=body, headers={"Content-Type": "application/json"}, timeout=timeout)
        return r.status_code, r.json()
    return _post


def replay(path: Path, post: Post, *, desde: int = 1, hasta: int | None = None, delay: float = 0.0,
           on_response: Callable[[int, int, dict], None] | None = None) -> list[dict]:
    out = []
    with Path(path).open("rb") as fh:
        for n, line in enumerate(fh, start=1):
            if n < desde or not line.strip():
                continue
            if hasta is not None and n > hasta:
                break
            status, body = post(line.strip())
            out.append({"linea": n, "status": status, **body})
            if on_response:
                on_response(n, status, body)
            if delay:
                time.sleep(delay)
    return out
