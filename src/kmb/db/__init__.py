"""Database access: engine/session factories and schema initialisation."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker

from kmb.db.models import SCHEMA_VERSION, Base, RiskLimit, SchemaMeta, utcnow

__all__ = ["Base", "init_db", "make_engine", "session_factory", "session_scope", "utcnow", "database_url"]


def database_url() -> str:
    url = os.environ.get("KMB_DATABASE_URL")
    if url:
        return url
    pw_file = os.environ.get("KMB_DB_PASSWORD_FILE")
    if pw_file and os.path.exists(pw_file):
        with open(pw_file, encoding="utf-8") as fh:
            pw = fh.read().strip()
        host = os.environ.get("KMB_DB_HOST", "192.168.1.129")
        port = os.environ.get("KMB_DB_PORT", "5432")
        user = os.environ.get("KMB_DB_USER", "kmb")
        name = os.environ.get("KMB_DB_NAME", "kmb")
        return f"postgresql+psycopg://{user}:{pw}@{host}:{port}/{name}"
    raise RuntimeError("set KMB_DATABASE_URL or KMB_DB_PASSWORD_FILE")


def make_engine(url: str | None = None, **kw) -> Engine:
    url = url or database_url()
    if url.startswith("sqlite"):
        engine = create_engine(url, connect_args={"check_same_thread": False}, **kw)

        @event.listens_for(engine, "connect")
        def _fk(dbapi_conn, _rec):  # pragma: no cover - trivial
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

        return engine
    kw.setdefault("pool_pre_ping", True)
    kw.setdefault("pool_size", 5)
    kw.setdefault("max_overflow", 5)
    return create_engine(url, **kw)


def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False)


@contextmanager
def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    s = factory()
    try:
        yield s
        s.commit()
    except BaseException:
        s.rollback()
        raise
    finally:
        s.close()


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        meta = s.get(SchemaMeta, "schema_version")
        if meta is None:
            s.add(SchemaMeta(key="schema_version", value=str(SCHEMA_VERSION)))
        elif int(meta.value) > SCHEMA_VERSION:
            raise RuntimeError(f"database schema {meta.value} is newer than this code ({SCHEMA_VERSION})")
        if s.scalar(select(RiskLimit).where(RiskLimit.user_id.is_(None))) is None:
            s.add(RiskLimit(user_id=None))
        s.commit()
