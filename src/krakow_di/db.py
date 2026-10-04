import psycopg
from psycopg.rows import dict_row

from krakow_di.config import get_settings


def connect(url: str | None = None) -> psycopg.Connection:
    """Otvorí synchrónne spojenie psycopg (riadky ako dict). Commit/close rieši volajúci."""
    return psycopg.connect(url or get_settings().database_url, row_factory=dict_row)
