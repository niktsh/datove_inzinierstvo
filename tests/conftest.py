import uuid

import psycopg
import pytest
from alembic import command
from alembic.config import Config

from krakow_di.config import Settings


def alembic_config(url: str) -> Config:
    cfg = Config("alembic.ini")
    cfg.attributes["url"] = url
    return cfg


@pytest.fixture(scope="session")
def test_db_url():
    """Jednorazová databáza na compose Postgrese; pracovnej DB sa nikdy nedotkne."""
    s = Settings(_env_file=".env")
    admin_url = f"postgresql://{s.postgres_user}:{s.postgres_password}@{s.postgres_host}:{s.postgres_port}/postgres"
    name = f"krakow_di_test_{uuid.uuid4().hex[:8]}"
    try:
        admin = psycopg.connect(admin_url, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as e:
        pytest.skip(f"Postgres z docker compose nie je dostupný: {e}")
    admin.execute(f'CREATE DATABASE "{name}"')
    url = admin_url.rsplit("/", 1)[0] + f"/{name}"
    yield url
    admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    admin.close()


@pytest.fixture
def migrated_db(test_db_url):
    cfg = alembic_config(test_db_url)
    command.upgrade(cfg, "head")
    yield test_db_url
    command.downgrade(cfg, "base")


@pytest.fixture
def conn(migrated_db):
    with psycopg.connect(migrated_db, row_factory=psycopg.rows.dict_row) as c:
        yield c
        c.rollback()
