"""Zápis do `lake.message`: správy sa ukladajú tak, ako prišli; duplicity sa ignorujú."""

import psycopg
from psycopg.types.json import Jsonb

from krakow_di.lake.message import LakeMessage

INSERT = """
INSERT INTO lake.message (team, channel, received_at, source_ref, content_type, payload_raw,
                          payload_json)
VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (team, source_ref) WHERE source_ref IS NOT NULL DO NOTHING
"""


async def write_message(conn: psycopg.AsyncConnection, msg: LakeMessage) -> bool:
    """Uloží správu a commitne. Vráti False, ak už bola uložená (rovnaký team + source_ref)."""
    cur = await conn.execute(INSERT, (
        msg.team, msg.channel, msg.received_at,
        Jsonb(msg.source_ref) if msg.source_ref is not None else None,
        msg.content_type, msg.payload_raw, msg.payload_json(),
    ))
    await conn.commit()
    return cur.rowcount == 1
