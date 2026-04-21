"""Read-only Postgres access to the shared trading-system DB."""
from contextlib import contextmanager

from dagster import ConfigurableResource
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

# Engines are expensive and thread-safe; one per URL, cached at module scope.
# ConfigurableResource is a Pydantic model so it can't hold the engine itself.
_engines: dict[str, Engine] = {}


def _get_engine(url: str) -> Engine:
    if url not in _engines:
        _engines[url] = create_engine(url, pool_pre_ping=True)
    return _engines[url]


class PostgresResource(ConfigurableResource):
    url: str

    @contextmanager
    def session(self) -> Session:
        factory = sessionmaker(bind=_get_engine(self.url), expire_on_commit=False)
        s = factory()
        try:
            yield s
        finally:
            s.close()
