"""Filter typov udalostí: zoznam vzorov oddelených čiarkou, napr. `flight.ticket.sold,theater.*`."""

import re
from fnmatch import fnmatchcase

_PATTERN = re.compile(r"^[a-z_.*]{1,64}$")
MAX_PATTERNS = 20


class InvalidFilter(ValueError):
    pass


def parse_types(raw: str | None) -> list[str]:
    if not raw:
        return []
    patterns = [p.strip() for p in raw.split(",") if p.strip()]
    if len(patterns) > MAX_PATTERNS or not all(_PATTERN.match(p) for p in patterns):
        raise InvalidFilter(
            "types: povolené sú písmená a-z, '_', '.', '*' (max. 20 vzorov oddelených čiarkou)"
        )
    return patterns


def matches(event_type: str, patterns: list[str]) -> bool:
    return not patterns or any(fnmatchcase(event_type, p) for p in patterns)


def to_like(patterns: list[str]) -> list[str]:
    """Vzory pre SQL `LIKE ANY` (`*` -> `%`, `_` a `\\` sa escapujú)."""
    def convert(p: str) -> str:
        p = p.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        return p.replace("*", "%")

    return [convert(p) for p in patterns]
