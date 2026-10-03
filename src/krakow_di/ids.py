import hashlib
from datetime import UTC, datetime


def make_offer_id(
    source: str,
    origin_iata: str,
    destination_iata: str,
    departure_at: datetime,
    airline_iata: str,
) -> str:
    """Deterministic offer id: sha256(source|origin|dest|departure UTC to the minute|airline)[:16].

    flight_number and fare_key are deliberately excluded (see ARCHITEKTURA.md).
    """
    if departure_at.tzinfo is None:
        raise ValueError("departure_at must be timezone-aware")
    dep = departure_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%MZ")
    key = "|".join(
        [source.strip().lower(), origin_iata.strip().upper(), destination_iata.strip().upper(),
         dep, airline_iata.strip().upper()]
    )
    return hashlib.sha256(key.encode()).hexdigest()[:16]
