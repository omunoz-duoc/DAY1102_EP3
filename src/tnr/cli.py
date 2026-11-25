"""CLI: python -m tnr <comando>."""
from __future__ import annotations

import argparse
import json
import os
import sys

from tnr import tasks


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=False, default=str))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tnr", description="TechNova Retail 360 · pipelines de datos")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="Crear directorios y esquema SQL")

    ing = sub.add_parser("ingest", help="Ingerir una fuente a Bronze y Silver")
    ing.add_argument("fuente", choices=tasks.FUENTES)
    ing.add_argument("--input", dest="input_kind", choices=["sql", "csv"], help="Entrada de ventas (default sql)")
    ing.add_argument("--force", action="store_true", help="Republicar aunque la huella ya tenga un lote exitoso")
    ing.add_argument("--reproceso", action="store_true", help="Reconstruir desde el último Bronze exitoso")
    ing.add_argument("--file", help="Archivo JSONL para movimientos_inventario")

    pub = sub.add_parser("publish", help="Publicar marts Gold")
    pub.add_argument("dominio", choices=["comercial"])
    pub.add_argument("--corte", help="Fecha de corte YYYY-MM-DD (default TNR_CORTE)")
    srv = sub.add_parser("serve", help="Levantar el receptor HTTP")
    srv.add_argument("app", choices=["web", "dashboard"])
    srv.add_argument("--host", default="127.0.0.1")
    srv.add_argument("--port", type=int, help="default 8765 (web) / 8050 (dashboard)")

    rep = sub.add_parser("replay", help="Reenviar un JSONL al receptor (productor mínimo)")
    rep.add_argument("app", choices=["web", "movimientos"])
    rep.add_argument("--file", required=True, help="Ruta al JSONL (relativa a INPUT_DIR si no existe)")
    rep.add_argument("--url", default=os.environ.get("TNR_WEB_URL", "http://127.0.0.1:8765/eventos"))
    rep.add_argument("--desde", type=int, default=1, help="Primera línea (1-based)")
    rep.add_argument("--hasta", type=int, help="Última línea incluida")
    rep.add_argument("--delay", type=float, default=0.0, help="Segundos entre envíos")

    clo = sub.add_parser("close", help="Cerrar una ventana desde Bronze")
    clo.add_argument("app", choices=["web", "inventario"])
    clo.add_argument("--minuto", help="web: inicio del minuto, ej. 2025-12-26T13:00:00-03:00")
    clo.add_argument("--corte", help="inventario: corte consultado, ej. 2025-12-26T12:05:00-03:00")
    clo.add_argument("--corte-ingesta", help="Corte de ingesta (default: ahora)")

    ser = sub.add_parser("serving", help="Consultar Serving para un intervalo")
    ser.add_argument("app", choices=["web", "inventario"])
    ser.add_argument("--desde", help="web")
    ser.add_argument("--hasta", help="web")
    ser.add_argument("--corte", help="inventario")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.cmd == "init":
            _print(tasks.init())
        elif args.cmd == "ingest":
            _print(tasks.ingest(args.fuente, input_kind=args.input_kind, force=args.force, reproceso=args.reproceso,
                                file=args.file))
        elif args.cmd == "publish":
            _print(tasks.publish_comercial(args.corte))
        elif args.cmd == "serve" and args.app == "web":
            from tnr.apps.receptor import serve
            serve(host=args.host, port=args.port or 8765)
        elif args.cmd == "serve":
            from tnr.apps.dashboard import serve
            serve(host=args.host, port=args.port or 8050)
        elif args.cmd == "replay" and args.app == "movimientos":
            res = tasks.consume_movimientos(args.file, desde=args.desde, hasta=args.hasta)
            for r in res.pop("detalle"):
                print(f"{r['linea']:>4} {r['estado']:<10} {r['id_evento']} {r['id_recepcion']}")
            _print(res)
        elif args.cmd == "replay":
            from pathlib import Path
            from tnr.apps.productor import http_post, replay
            from tnr.config import Config
            path = Path(args.file)
            if not path.exists():
                path = Config.from_env().input_dir / args.file
            replay(path, http_post(args.url), desde=args.desde, hasta=args.hasta, delay=args.delay,
                   on_response=lambda n, st, b: print(f"{n:>4} {st} {b.get('estado', b.get('error'))}"
                                                      f" {b.get('id_recepcion')}", flush=True))
        elif args.cmd == "close" and args.app == "inventario":
            _print(tasks.close_stock(args.corte, args.corte_ingesta))
        elif args.cmd == "close":
            _print(tasks.close_web(args.minuto, args.corte_ingesta))
        elif args.cmd == "serving" and args.app == "inventario":
            _print(tasks.stock_provisional(args.corte))
        elif args.cmd == "serving":
            _print(tasks.serving_web(args.desde, args.hasta))
    except NotImplementedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0
