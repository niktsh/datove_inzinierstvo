import psycopg
from psycopg.types.json import Jsonb


def log_fetch(
    conn: psycopg.Connection,
    source: str,
    url: str,
    request_params: dict | None,
    status_code: int | None,
    payload: dict | list | None,
    parse_status: str = "pending",
) -> int:
    """Store a raw response as received. Never put secrets (tokens) into request_params."""
    row = conn.execute(
        """
        INSERT INTO raw.fetch_log (source, url, request_params, status_code, payload, parse_status)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """,
        (
            source, url, Jsonb(request_params) if request_params is not None else None,
            status_code, Jsonb(payload) if payload is not None else None, parse_status,
        ),
    ).fetchone()
    return row["id"]


def set_parse_status(conn: psycopg.Connection, fetch_id: int, parse_status: str) -> None:
    conn.execute(
        "UPDATE raw.fetch_log SET parse_status = %s WHERE id = %s", (parse_status, fetch_id)
    )
