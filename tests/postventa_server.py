"""Servidor HTTP de prueba para postventa (el servidor real vive en otro proyecto).

Sirve páginas JSON en memoria y permite inyectar fallas transitorias por página."""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


class FakePostventa:
    def __init__(self, pages: dict[str, dict], fallas: dict[str, int] | None = None):
        self.pages = pages
        self.fallas = dict(fallas or {})   # page -> nº de respuestas 503 antes de responder bien
        self.requests: list[str] = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                url = urlsplit(self.path)
                page = parse_qs(url.query).get("page", ["1"])[0]
                owner.requests.append(page)
                if url.path != "/ordenes" or page not in owner.pages:
                    self.send_error(404)
                    return
                if owner.fallas.get(page, 0) > 0:
                    owner.fallas[page] -= 1
                    self.send_error(503)
                    return
                body = json.dumps(owner.pages[page], ensure_ascii=False).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/ordenes"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    @staticmethod
    def pages_from(data_dir: Path) -> dict[str, dict]:
        return {str(i): json.loads((data_dir / f"postventa_pagina_{i}.json").read_text(encoding="utf-8"))
                for i in (1, 2, 3)}
