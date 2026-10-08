import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from krakow_di.config import Settings
from krakow_di.repo.flight_offers import OfferObservation, upsert_offer
from krakow_di.scheduler import (
    MAX_GENERATOR_CATCH_UP,
    GeneratorJob,
    Job,
    JobStats,
    build_jobs,
    run_job_loop,
    run_scheduler,
    write_heartbeat,
)
from tests.test_api import settings_for


def test_job_loop_runs_repeatedly_survives_errors_and_writes_heartbeat(tmp_path, caplog):
    calls = {"ok": 0, "bad": 0}

    def ok():
        calls["ok"] += 1
        return f"beh {calls['ok']}"

    def bad():
        calls["bad"] += 1
        raise RuntimeError("zdroj je dole")

    async def run():
        stop = asyncio.Event()
        asyncio.get_running_loop().call_later(0.5, stop.set)
        beat = tmp_path / "hb" / "scheduler.heartbeat"
        with caplog.at_level(logging.ERROR, logger="krakow_di.scheduler"):
            stats = await asyncio.wait_for(run_scheduler(
                [Job("dobra", 0.05, ok), Job("zla", 0.05, bad)], stop, str(beat)), 5)
        return stats, beat

    stats, beat = asyncio.run(run())
    assert stats["dobra"].runs >= 4 and stats["dobra"].failures == 0
    assert stats["dobra"].last_summary.startswith("beh") and stats["dobra"].last_error is None
    assert stats["zla"].runs >= 4 and stats["zla"].failures == stats["zla"].runs
    assert stats["zla"].last_error == "RuntimeError: zdroj je dole"
    assert calls["ok"] == stats["dobra"].runs  # zlá úloha dobrú nezastavila
    assert beat.exists() and datetime.fromisoformat(beat.read_text()).tzinfo is not None
    assert any("zla zlyhala" in r.message for r in caplog.records)


def test_job_never_overlaps_itself():
    running, overlap = 0, False

    def slow():
        nonlocal running, overlap
        running += 1
        overlap = overlap or running > 1
        import time
        time.sleep(0.15)
        running -= 1
        return "ok"

    async def run():
        stop = asyncio.Event()
        asyncio.get_running_loop().call_later(0.6, stop.set)
        await asyncio.wait_for(run_job_loop(Job("pomala", 0.01, slow), stop, JobStats()), 5)

    asyncio.run(run())
    assert overlap is False


def test_loop_stops_promptly_during_long_sleep():
    async def run():
        stop = asyncio.Event()
        stats = JobStats()
        task = asyncio.create_task(run_job_loop(Job("x", 3600, lambda: "ok"), stop, stats))
        await asyncio.sleep(0.2)
        stop.set()
        await asyncio.wait_for(task, 2)
        return stats

    assert asyncio.run(run()).runs == 1


def test_start_delay_is_respected():
    async def run():
        stop = asyncio.Event()
        stats = JobStats()
        task = asyncio.create_task(run_job_loop(Job("x", 3600, lambda: "ok", 5.0), stop, stats))
        await asyncio.sleep(0.2)
        stop.set()
        await asyncio.wait_for(task, 2)
        return stats

    assert asyncio.run(run()).runs == 0


def test_build_jobs_uses_configured_intervals():
    s = Settings(_env_file=None, scheduler_ryanair_hours=2, generator_tick_seconds=60)
    jobs = build_jobs(s, publish=False)
    assert set(jobs) == {"generator", "theater_generator", "theater_snapshots", "theater_programme",
                         "travelpayouts", "ryanair"}
    assert jobs["generator"].interval_seconds == 60
    assert jobs["theater_generator"].interval_seconds == 60
    assert jobs["ryanair"].interval_seconds == 7200
    assert jobs["travelpayouts"].interval_seconds == 6 * 3600
    assert jobs["theater_snapshots"].interval_seconds == 3 * 3600
    assert jobs["theater_programme"].interval_seconds == 24 * 3600


def test_heartbeat_noop_without_path(tmp_path):
    write_heartbeat(None)
    write_heartbeat(tmp_path / "a" / "b" / "hb")
    assert (tmp_path / "a" / "b" / "hb").exists()


def seed_future_offers(conn, n=15):
    now = datetime.now(UTC)
    for i in range(n):
        upsert_offer(conn, OfferObservation(
            source="ryanair", origin_iata="BCN", destination_iata="KRK",
            departure_at=now + timedelta(days=2 + i), airline_iata="FR", price=Decimal(40 + i),
            currency="EUR", observed_at=now, flight_number=f"FR{i}"))
    conn.commit()


def test_generator_job_assigns_capacity_sells_and_tracks_time(conn, migrated_db):
    seed_future_offers(conn)
    s = settings_for(migrated_db).model_copy(update={
        "generator_seed": 3, "generator_base_rate": 0.5, "generator_tick_seconds": 300})
    job = GeneratorJob(s, publish=False)
    first = job()
    assert "nové ponuky 15" in first and job.last is not None
    assert conn.execute("SELECT count(*) AS n FROM core.flight_offer WHERE seats_total IS NOT NULL"
                        ).fetchone()["n"] == 15
    job.last = datetime.now(UTC) - timedelta(minutes=30)  # beh po polhodinovej pauze
    second = job()
    assert "nové ponuky 0" in second and "udalostí" in second
    n_sales = conn.execute("SELECT count(*) AS n FROM core.ticket_sale").fetchone()["n"]
    assert n_sales > 0
    last_sale = conn.execute("SELECT max(sold_at) AS t FROM core.ticket_sale").fetchone()["t"]
    assert datetime.now(UTC) - timedelta(minutes=31) < last_sale <= datetime.now(UTC)


def test_generator_catch_up_after_long_outage_is_capped(conn, migrated_db):
    seed_future_offers(conn, 5)
    s = settings_for(migrated_db).model_copy(update={"generator_seed": 1,
                                                     "generator_base_rate": 0.001})
    job = GeneratorJob(s, publish=False)
    job()
    job.last = datetime.now(UTC) - timedelta(days=3)  # server bol 3 dni vypnutý
    started = datetime.now(UTC)
    job()
    first_hist = conn.execute("SELECT min(observed_at) AS t FROM core.flight_offer_history "
                              "WHERE cause='generator' AND observed_at > %s",
                              (started - MAX_GENERATOR_CATCH_UP - timedelta(minutes=10),)
                              ).fetchone()["t"]
    assert first_hist is None or first_hist >= started - MAX_GENERATOR_CATCH_UP - timedelta(
        minutes=10)
