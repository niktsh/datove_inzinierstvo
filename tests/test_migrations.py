import psycopg
from alembic import command

from tests.conftest import alembic_config

TABLES = {
    "raw": {"fetch_log"},
    "core": {"flight_offer", "flight_offer_history", "ticket_sale", "theater_performance",
             "theater_snapshot", "event_log", "fx_rate", "theater_sale"},
    "lake": {"message"},
}


def _tables(url):
    with psycopg.connect(url) as c:
        rows = c.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema IN ('raw','core','lake')"
        ).fetchall()
    out: dict[str, set[str]] = {}
    for schema, name in rows:
        out.setdefault(schema, set()).add(name)
    return out


def test_upgrade_downgrade_upgrade(test_db_url):
    cfg = alembic_config(test_db_url)
    command.upgrade(cfg, "head")
    assert _tables(test_db_url) == TABLES
    command.downgrade(cfg, "base")
    assert _tables(test_db_url) == {}
    command.upgrade(cfg, "head")
    assert _tables(test_db_url) == TABLES
    command.downgrade(cfg, "base")


def test_downgrade_one_step_removes_only_theater_additions(test_db_url):
    cfg = alembic_config(test_db_url)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "0001")
    core = _tables(test_db_url)["core"]
    assert "fx_rate" not in core and "theater_sale" not in core and "flight_offer" in core
    command.downgrade(cfg, "base")


def test_lake_unique_source_ref(conn):
    ins = ("INSERT INTO lake.message (team, channel, source_ref, payload_raw) "
           "VALUES ('team_01', 'kafka', %s::jsonb, 'x')")
    conn.execute(ins, ('{"topic": "t", "partition": 0, "offset": 1}',))
    try:
        conn.execute(ins, ('{"topic": "t", "partition": 0, "offset": 1}',))
        raise AssertionError("duplicate source_ref accepted")
    except psycopg.errors.UniqueViolation:
        conn.rollback()
    # NULL source_ref sa môže opakovať
    conn.execute(ins, (None,))
    conn.execute(ins, (None,))
