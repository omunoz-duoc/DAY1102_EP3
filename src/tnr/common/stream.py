"""Bronze para cargas continuas: archivo JSONL append-only por instancia (escritor único) con fsync."""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from tnr.common.tipos import utc_now_iso
from tnr.config import Config


@dataclass
class Recepcion:
    id_recepcion: str
    ts_ingesta: str
    body: str
    archivo: str


class BronzeAppender:
    def __init__(self, cfg: Config, fuente: str):
        self.cfg = cfg
        self.instancia = f"{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.lock = threading.Lock()
        self._seq = 0
        day_dir = cfg.bronze_dir / fuente / utc_now_iso()[:10]
        day_dir.mkdir(parents=True, exist_ok=True)
        self.archivo = day_dir / f"recepciones_{self.instancia}.jsonl"

    def append(self, body: str) -> Recepcion:
        """Llamar con `lock` tomado: un único escritor por archivo activo."""
        self._seq += 1
        rec = Recepcion(id_recepcion=f"R{time.time_ns()}-{self.instancia}-{self._seq:06d}",
                        ts_ingesta=utc_now_iso(), body=body,
                        archivo=str(self.archivo.relative_to(self.cfg.bronze_dir)))
        line = json.dumps({"id_recepcion": rec.id_recepcion, "ts_ingesta": rec.ts_ingesta,
                           "body_sha256": hashlib.sha256(body.encode()).hexdigest(), "body": body},
                          ensure_ascii=False)
        with self.archivo.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        return rec


def iter_recepciones(cfg: Config, fuente: str, corte_ingesta: str | None = None) -> Iterator[Recepcion]:
    """Todas las recepciones archivadas de una fuente, en orden de ingesta (opcionalmente hasta un corte)."""
    rows = []
    for path in sorted((cfg.bronze_dir / fuente).glob("*/recepciones_*.jsonl")):
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                r = json.loads(line)
                if corte_ingesta and r["ts_ingesta"] > corte_ingesta:
                    continue
                rows.append(Recepcion(r["id_recepcion"], r["ts_ingesta"], r["body"],
                                      str(path.relative_to(cfg.bronze_dir))))
    rows.sort(key=lambda r: (r.ts_ingesta, r.id_recepcion))
    return iter(rows)
