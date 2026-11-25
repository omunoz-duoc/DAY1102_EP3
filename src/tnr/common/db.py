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


_READ_ENGINES: dict[str, Engine] = {}


def get_read_engine(path: Path) -> Engine:
    """Conexiones de solo lectura para la UI: aísla la visualización de las escrituras."""
    key = str(Path(path).resolve())
    if key not in _READ_ENGINES:
        engine = create_engine(f"sqlite:///file:{key}?mode=ro&uri=true",
                               connect_args={"timeout": BUSY_TIMEOUT_MS / 1000, "uri": True})

        def _ro_pragmas(dbapi_conn, _record):
            cur = dbapi_conn.cursor()
            cur.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
            cur.execute("PRAGMA query_only=ON")
            cur.close()
        event.listen(engine, "connect", _ro_pragmas)
        _READ_ENGINES[key] = engine
    return _READ_ENGINES[key]
