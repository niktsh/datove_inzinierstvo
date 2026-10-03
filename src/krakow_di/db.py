import psycopg
from psycopg.rows import dict_row

from krakow_di.config import get_settings


def connect(url: str | None = None) -> psycopg.Connection:
    """Open a sync psycopg connection (dict rows). Caller owns commit/close."""
    return psycopg.connect(url or get_settings().database_url, row_factory=dict_row)
