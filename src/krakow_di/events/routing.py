"""Kontrakt udalostí: typ udalosti -> topik a kľúč správy (pozri docs/UDALOSTI.md)."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TopicSpec:
    suffix: str      # topik = <prefix>.<suffix>
    partitions: int
    key_field: str   # pole v `data`, ktoré je kľúčom správy (id entity)
    event_types: tuple[str, ...]


TOPIC_SPECS: tuple[TopicSpec, ...] = (
    TopicSpec(
        "flights.offers", 3, "offer_id",
        ("flight.offer.found", "flight.offer.observed", "flight.offer.price_changed",
         "flight.offer.sold_out", "flight.offer.expired"),
    ),
    TopicSpec("flights.sales", 3, "offer_id", ("flight.ticket.sold",)),
    TopicSpec(
        "theater.performances", 1, "performance_id",
        ("theater.performance.found", "theater.performance.updated"),
    ),
    TopicSpec("theater.availability", 1, "performance_id", ("theater.availability.snapshot",)),
    TopicSpec("theater.sales", 1, "performance_id", ("theater.tickets.sold",)),
)

_BY_TYPE = {et: spec for spec in TOPIC_SPECS for et in spec.event_types}

ALL_EVENT_TYPES = tuple(_BY_TYPE)


def spec_for(event_type: str) -> TopicSpec:
    try:
        return _BY_TYPE[event_type]
    except KeyError:
        raise ValueError(f"neznámy typ udalosti: {event_type}") from None


def topic_for(event_type: str, prefix: str) -> str:
    return f"{prefix}.{spec_for(event_type).suffix}"


def key_for(event: dict) -> str:
    """Kľúč správy = id entity z `data` (všetky udalosti jednej entity idú do jednej partície)."""
    return str(event["data"][spec_for(event["event_type"]).key_field])


def topic_names(prefix: str) -> dict[str, TopicSpec]:
    return {f"{prefix}.{s.suffix}": s for s in TOPIC_SPECS}
