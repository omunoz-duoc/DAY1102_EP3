"""Receptor HTTP de eventos web (Flask).

Contrato:
    POST /eventos  cuerpo = un objeto JSON (una línea del JSONL)
      202 {"id_recepcion", "ts_ingesta", "estado"}   tras escribir en Bronze
      400 {"id_recepcion", "ts_ingesta", "error"}     cuerpo no es JSON válido (igual queda en Bronze)
    GET /salud
"""
from __future__ import annotations

import atexit
import json
import signal
import sys

from flask import Flask, jsonify, request

from tnr.config import Config
from tnr.ingest.web_stream import WebStreamReceiver


def create_app(cfg: Config | None = None) -> Flask:
    cfg = cfg or Config.from_env()
    receiver = WebStreamReceiver(cfg)
    atexit.register(receiver.close)
    app = Flask("tnr-receptor")
    app.config["RECEIVER"] = receiver

    @app.post("/eventos")
    def eventos():
        body = request.get_data(cache=False)
        out = receiver.receive(body)
        try:
            json.loads(body)
        except (ValueError, UnicodeDecodeError):
            return jsonify({"id_recepcion": out["id_recepcion"], "ts_ingesta": out["ts_ingesta"],
                            "error": "cuerpo no es JSON válido"}), 400
        return jsonify({k: out[k] for k in ("id_recepcion", "ts_ingesta", "estado")}), 202

    @app.get("/salud")
    def salud():
        return jsonify({"estado": "ok", "run_id": receiver.run_id, "archivo_bronze": str(receiver.archivo)})

    return app


def serve(cfg: Config | None = None, host: str = "127.0.0.1", port: int = 8765) -> None:
    # SIGTERM -> salida normal, para que atexit cierre el run del receptor con sus recuentos
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    create_app(cfg).run(host=host, port=port, threaded=True, use_reloader=False)
