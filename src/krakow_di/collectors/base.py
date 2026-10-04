from dataclasses import dataclass, field

from krakow_di.repo.flight_offers import OfferObservation, UpsertResult


@dataclass(frozen=True)
class OfferChange:
    """Čo zber urobil s jednou ponukou; publisher (fáza 5) z toho vytvorí udalosť."""

    observation: OfferObservation
    result: UpsertResult


@dataclass
class RunSummary:
    requests: int = 0
    errors: int = 0
    empty: int = 0
    aborted: bool = False
    changes: list[OfferChange] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for c in self.changes:
            out[c.result.change] = out.get(c.result.change, 0) + 1
        return out

    def offers_by_origin(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for c in self.changes:
            out[c.observation.origin_iata] = out.get(c.observation.origin_iata, 0) + 1
        return dict(sorted(out.items()))
