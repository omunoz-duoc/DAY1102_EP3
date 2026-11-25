"""Conexión SQLite compartida: WAL, busy timeout y foreign keys por conexión."""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine, event

_ENGINES: dict[str, Engine] = {}
BUSY_TIMEOUT_MS = 30_000


def _set_pragmas(dbapi_conn, _record) -> None:
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")
    cur.execute("PRAGMA foreign_keys=ON")
    cur.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    cur.close()


def get_engine(path: Path) -> Engine:
    key = str(Path(path).resolve())
    if key not in _ENGINES:
        Path(key).parent.mkdir(parents=True, exist_ok=True)
        engine = create_engine(f"sqlite:///{key}", connect_args={"timeout": BUSY_TIMEOUT_MS / 1000})
        event.listen(engine, "connect", _set_pragmas)
        _ENGINES[key] = engine
    return _ENGINES[key]


def run_script(engine: Engine, sql_text: str) -> None:
    raw = engine.raw_connection()
    try:
        raw.driver_connection.executescript(sql_text)
        raw.commit()
    finally:
        raw.close()
