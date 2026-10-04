from alembic import context
from sqlalchemy import create_engine

from krakow_di.config import get_settings


def _url() -> str:
    # Testy/nástroje môžu odovzdať URL cez `-x url=...` alebo atribút konfigurácie.
    x = context.get_x_argument(as_dictionary=True).get("url")
    url = x or context.config.attributes.get("url") or get_settings().database_url
    return url.replace("postgresql://", "postgresql+psycopg://", 1)


def run_migrations_online() -> None:
    engine = create_engine(_url())
    with engine.connect() as conn:
        context.configure(connection=conn, target_metadata=None, version_table_schema=None)
        with context.begin_transaction():
            context.run_migrations()
        conn.commit()


run_migrations_online()
