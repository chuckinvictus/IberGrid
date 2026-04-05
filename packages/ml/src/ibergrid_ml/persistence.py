from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from ibergrid_ml.config import ForecastSettings


class Base(DeclarativeBase):
    pass


@lru_cache(maxsize=8)
def get_engine(database_url: str | None = None) -> object:
    resolved_database_url = database_url or ForecastSettings().database_url
    return create_engine(resolved_database_url, future=True)

@lru_cache(maxsize=8)
def get_session_factory(database_url: str | None = None) -> sessionmaker[Session]:
    return sessionmaker(
        bind=get_engine(database_url),
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


@contextmanager
def session_scope(database_url: str | None = None) -> Iterator[Session]:
    session = get_session_factory(database_url)()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
