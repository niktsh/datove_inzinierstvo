"""Správa uložená do data lake: surové bajty tak, ako prišli (bez normalizácie)."""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class LakeMessage:
    team: str
    channel: str                  # kafka | sse | ws | rest | ...
    payload_raw: bytes
    source_ref: dict | None = None  # presný pôvod; pri None sa duplicity nedajú odstrániť
    content_type: str | None = None
    received_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def payload_json(self) -> str | None:
        """Text JSON na uloženie do jsonb, alebo None, ak správa nie je platný JSON."""
        try:
            text = self.payload_raw.decode("utf-8")
            json.loads(text)
        except (UnicodeDecodeError, ValueError):
            return None
        # PostgreSQL jsonb neuloží znak NUL (\u0000): vtedy ostane len payload_raw
        return None if "\\u0000" in text else text
