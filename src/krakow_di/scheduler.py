"""Plánovač: periodicky spúšťa zberače, snímky divadla a generátor a publikuje ich udalosti.

Každá úloha beží v slučke osobitne (jedna úloha sa nikdy neprekrýva sama so sebou) a chyba
jednej úlohy nezastaví ostatné. Po každom behu sa zapíše heartbeat (kontrola zdravia kontajnera).
"""

import argparse
import asyncio
import logging
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from krakow_di.collectors.ryanair import RyanairCollector, load_config
from krakow_di.collectors.slowacki import SlowackiCollector
from krakow_di.collectors.travelpayouts import TravelpayoutsCollector
from krakow_di.config import Settings, get_settings
from krakow_di.db import connect
from krakow_di.events.builders import offer_events, theater_events
from krakow_di.events.publisher import publish_events
from krakow_di.generator.engine import Generator
from krakow_di.generator.model import ModelParams, TheaterParams
from krakow_di.generator.theater import TheaterGenerator
from krakow_di.routes import load_routes

log = logging.getLogger("krakow_di.scheduler")
MAX_GENERATOR_CATCH_UP = timedelta(hours=6)  # po dlhom výpadku sa neženie obrovský nápor predajov


@dataclass
class Job:
    name: str
    interval_seconds: float
    func: Callable[[], str]
    start_delay: float = 0.0


@dataclass
class JobStats:
    runs: int = 0
    failures: int = 0
    last_ok: datetime | None = None
    last_error: str | None = None
    last_summary: str | None = None


# ---------------------------------------------------------------- úlohy
def _publish(conn, events: list[dict], settings: Settings, publish: bool) -> str:
    if not publish or not events:
        return f"udalostí {len(events)}"
    stored, sent = publish_events(conn, events, settings)
    return f"udalostí uložených {stored}, odoslaných {sent}"


def travelpayouts_job(settings: Settings, publish: bool = True) -> str:
    collector = TravelpayoutsCollector(settings, load_routes())
    with connect(settings.database_url) as conn:
        s = collector.run(conn)
        events = offer_events(conn, s.changes, settings.producer_id)
        return (f"dopyty {s.requests}, chyby {s.errors}, zmeny {s.counts()}; "
                + _publish(conn, events, settings, publish))


def ryanair_job(settings: Settings, publish: bool = True) -> str:
    collector = RyanairCollector(load_config())
    with connect(settings.database_url) as conn:
        s = collector.run(conn)
        events = offer_events(conn, s.changes, settings.producer_id)
        return (f"dopyty {s.requests}, chyby {s.errors}, prerušené {s.aborted}, "
                f"zmeny {s.counts()}; " + _publish(conn, events, settings, publish))


def theater_job(settings: Settings, snapshots: bool, publish: bool = True) -> str:
    collector = SlowackiCollector()
    with connect(settings.database_url) as conn:
        run = collector.run(conn, discover=not snapshots, snapshot=snapshots)
        events = theater_events(run, settings.producer_id, datetime.now(UTC))
        return (f"dopyty {run.requests}, chyby {run.errors}, nové {len(run.found)}, "
                f"snímky {len(run.snapshots)}, predaje {len(run.sales)}; "
                + _publish(conn, events, settings, publish))


@dataclass
class GeneratorJob:
    settings: Settings
    publish: bool = True
    generator: Generator = field(init=False)
    last: datetime | None = None

    def __post_init__(self) -> None:
        s = self.settings
        self.generator = Generator(
            ModelParams(s.generator_base_rate, s.generator_popularity_sigma),
            s.generator_seed, s.producer_id)

    def __call__(self) -> str:
        now = datetime.now(UTC)
        start = self.last or now - timedelta(seconds=self.settings.generator_tick_seconds)
        start = max(start, now - MAX_GENERATOR_CATCH_UP)
        speed = self.settings.generator_time_speedup
        seconds = max((now - start).total_seconds(), 1.0) * speed
        with connect(self.settings.database_url) as conn:
            res = self.generator.run_interval(conn, start, seconds)
            self.last = now
            return self._summary(res) + _publish(conn, res.events, self.settings, self.publish)

    def _summary(self, res) -> str:
        return (f"nové ponuky {res.initialized}, predaje {res.sales} ({res.seats_sold} miest), "
                f"zmeny ceny {res.price_changes}, vypredané {res.sold_out}, "
                f"vypršané {res.expired}; ")


@dataclass
class TheaterGeneratorJob(GeneratorJob):
    """Simulovaný predaj vstupeniek do divadla (rovnaký rytmus ako generátor leteniek)."""

    def __post_init__(self) -> None:
        s = self.settings
        self.generator = TheaterGenerator(
            TheaterParams(s.generator_theater_base_rate, s.generator_popularity_sigma),
            s.generator_seed, s.producer_id)

    def _summary(self, res) -> str:
        return f"predaje {res.sales} ({res.seats_sold} miest); "


def build_jobs(settings: Settings, publish: bool = True) -> dict[str, Job]:
    h = 3600.0
    return {
        "generator": Job("generator", settings.generator_tick_seconds,
                         GeneratorJob(settings, publish), 5),
        "theater_generator": Job("theater_generator", settings.generator_tick_seconds,
                                 TheaterGeneratorJob(settings, publish), 7),
        "theater_snapshots": Job(
            "theater_snapshots", settings.scheduler_theater_snapshots_hours * h,
            lambda: theater_job(settings, True, publish), 60),
        "theater_programme": Job(
            "theater_programme", settings.scheduler_theater_programme_hours * h,
            lambda: theater_job(settings, False, publish), 20),
        "travelpayouts": Job(
            "travelpayouts", settings.scheduler_travelpayouts_hours * h,
            lambda: travelpayouts_job(settings, publish), 40),
        "ryanair": Job(
            "ryanair", settings.scheduler_ryanair_hours * h,
            lambda: ryanair_job(settings, publish), 80),
    }


# ---------------------------------------------------------------- slučka
def write_heartbeat(path: str | Path | None) -> None:
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(datetime.now(UTC).isoformat(), encoding="utf-8")


async def _sleep(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=max(seconds, 0))
    except TimeoutError:
        pass


async def run_job_loop(
    job: Job, stop: asyncio.Event, stats: JobStats, heartbeat: str | None = None,
) -> None:
    await _sleep(stop, job.start_delay)
    while not stop.is_set():
        started = time.monotonic()
        stats.runs += 1
        try:
            stats.last_summary = await asyncio.to_thread(job.func)
            stats.last_ok = datetime.now(UTC)
            stats.last_error = None
            log.info("%s: %s (%.1f s)", job.name, stats.last_summary, time.monotonic() - started)
        except Exception as e:  # noqa: BLE001 (chyba jednej úlohy nesmie zastaviť plánovač)
            stats.failures += 1
            stats.last_error = f"{type(e).__name__}: {e}"
            log.exception("%s zlyhala", job.name)
        write_heartbeat(heartbeat)
        await _sleep(stop, job.interval_seconds - (time.monotonic() - started))


async def run_scheduler(
    jobs: list[Job], stop: asyncio.Event, heartbeat: str | None = None
) -> dict[str, JobStats]:
    stats = {j.name: JobStats() for j in jobs}
    await asyncio.gather(*(run_job_loop(j, stop, stats[j.name], heartbeat) for j in jobs))
    return stats


async def amain(args: argparse.Namespace) -> None:
    settings = get_settings()
    all_jobs = build_jobs(settings, publish=not args.no_publish)
    names = args.jobs.split(",") if args.jobs else list(all_jobs)
    unknown = set(names) - set(all_jobs)
    if unknown:
        raise SystemExit(f"neznáme úlohy: {sorted(unknown)}; dostupné: {sorted(all_jobs)}")
    if args.once:
        job = all_jobs[args.once]
        print(job.func())
        return
    jobs = [all_jobs[n] for n in names]
    log.info("plánovač: %s", ", ".join(f"{j.name} každých {j.interval_seconds / 60:.0f} min"
                                       for j in jobs))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    if args.duration:
        loop.call_later(args.duration, stop.set)
    stats = await run_scheduler(jobs, stop, settings.scheduler_heartbeat_file)
    for name, s in stats.items():
        print(f"{name}: behov {s.runs}, zlyhaní {s.failures}, posledný výsledok: {s.last_summary}")


def main() -> None:
    p = argparse.ArgumentParser(description="Plánovač zberačov, generátora a publikovania")
    p.add_argument("--jobs", help="podmnožina úloh oddelená čiarkou (predvolene všetky)")
    p.add_argument("--once", metavar="ÚLOHA", help="spustí jednu úlohu raz a skončí")
    p.add_argument("--no-publish", action="store_true", help="neposielať udalosti do Kafky")
    p.add_argument("--duration", type=float, help="skončiť po N sekundách (na skúšanie)")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(amain(p.parse_args()))


if __name__ == "__main__":
    main()
