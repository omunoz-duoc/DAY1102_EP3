"""CLI: python -m tnr <comando>."""
from __future__ import annotations

import argparse
import json
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

    pub = sub.add_parser("publish", help="Publicar marts Gold")
    pub.add_argument("dominio", choices=["comercial"])
    pub.add_argument("--corte", help="Fecha de corte YYYY-MM-DD (default TNR_CORTE)")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.cmd == "init":
            _print(tasks.init())
        elif args.cmd == "ingest":
            _print(tasks.ingest(args.fuente, input_kind=args.input_kind, force=args.force, reproceso=args.reproceso))
        elif args.cmd == "publish":
            _print(tasks.publish_comercial(args.corte))
    except NotImplementedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0
