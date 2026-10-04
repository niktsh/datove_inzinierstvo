"""Tvorba a validácia správ (envelope podľa docs/UDALOSTI.md, schémy v schemas/events/)."""

import json
import uuid
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "schemas" / "events"
EVENT_VERSION = 1


class InvalidEvent(ValueError):
    """Udalosť nezodpovedá JSON Schema svojho typu."""


def iso_utc(dt: datetime) -> str:
    """UTC čas ako `2026-10-14T09:31:05Z` (formát z kontraktu)."""
    if dt.tzinfo is None:
        raise ValueError("čas musí mať časové pásmo")
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_local(dt: datetime) -> str:
    """Čas s pôvodným posunom, napr. `2026-11-20T06:15:00+01:00` (odlety, začiatky predstavení)."""
    if dt.tzinfo is None:
        raise ValueError("čas musí mať časové pásmo")
    return dt.isoformat(timespec="seconds")


def make_event(
    event_type: str,
    source: str,
    data: dict,
    occurred_at: datetime,
    producer: str,
    event_id: uuid.UUID | None = None,
) -> dict:
    event = {
        "event_id": str(event_id or uuid.uuid4()),
        "event_type": event_type,
        "event_version": EVENT_VERSION,
        "occurred_at": iso_utc(occurred_at),
        "producer": producer,
        "source": source,
        "data": data,
    }
    validate(event)
    return event


@lru_cache
def _validator(event_type: str) -> Draft202012Validator:
    path = SCHEMA_DIR / f"{event_type}.json"
    if not path.is_file():
        raise InvalidEvent(f"chýba schéma pre typ udalosti {event_type}")
    schema = json.loads(path.read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())


def validate(event: dict) -> None:
    """Vyhodí InvalidEvent s čitateľným popisom prvej chyby."""
    event_type = event.get("event_type")
    if not isinstance(event_type, str):
        raise InvalidEvent("chýba event_type")
    errors = sorted(_validator(event_type).iter_errors(event), key=lambda e: list(e.path))
    if errors:
        e = errors[0]
        where = "/".join(str(p) for p in e.path) or "(koreň)"
        raise InvalidEvent(f"{event_type}: {where}: {e.message}")
