"""Spustenie generátora: `uv run python -m krakow_di.generator [--ticks N] [--speedup X] ...`."""

import argparse
import logging
import time
from datetime import UTC, datetime

from krakow_di.config import get_settings
from krakow_di.db import connect
from krakow_di.generator.engine import Generator, TickResult
from krakow_di.generator.model import ModelParams

log = logging.getLogger("krakow_di.generator")


def main() -> None:
    s = get_settings()
    p = argparse.ArgumentParser(description="Generátor predaja leteniek (simulácia)")
    p.add_argument("--ticks", type=int, help="počet tikov (bez toho beží donekonečna)")
    p.add_argument("--tick-seconds", type=int, default=s.generator_tick_seconds,
                   help="reálna dĺžka tiku v sekundách")
    p.add_argument("--speedup", type=float, default=s.generator_time_speedup,
                   help="zrýchlenie času: 1 reálny tik simuluje tick-seconds × speedup sekúnd")
    p.add_argument("--seed", type=int, default=s.generator_seed)
    p.add_argument("--base-rate", type=float, default=s.generator_base_rate)
    p.add_argument("--sigma", type=float, default=s.generator_popularity_sigma)
    p.add_argument("--fast", action="store_true", help="nečakať medzi tikmi (rýchla simulácia)")
    p.add_argument("--publish", action="store_true", help="odoslať udalosti do Kafky")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    gen = Generator(ModelParams(args.base_rate, args.sigma), args.seed, s.producer_id)
    log.info("seed=%s, tik=%ss, zrýchlenie=%sx", gen.seed, args.tick_seconds, args.speedup)

    sim_now = datetime.now(UTC)
    total = TickResult()
    done = 0
    with connect() as conn:
        while args.ticks is None or done < args.ticks:
            started = time.monotonic()
            sim_seconds = args.tick_seconds * args.speedup
            res = gen.run_interval(conn, sim_now, sim_seconds)
            sim_now = sim_now.fromtimestamp(sim_now.timestamp() + sim_seconds, UTC)
            done += 1
            if args.publish and res.events:
                from krakow_di.events.publisher import publish_events

                stored, sent = publish_events(conn, res.events)
                log.info("udalosti: uložené %d, odoslané %d", stored, sent)
            log.info(
                "tik %d (sim %s): nové ponuky %d, predaje %d (%d miest), zmeny ceny %d, "
                "vypredané %d, vypršané %d",
                done, sim_now.strftime("%Y-%m-%d %H:%M"), res.initialized, res.sales,
                res.seats_sold, res.price_changes, res.sold_out, res.expired,
            )
            total.merge(TickResult(
                events=[], initialized=res.initialized, sales=res.sales,
                seats_sold=res.seats_sold, price_changes=res.price_changes,
                sold_out=res.sold_out, expired=res.expired,
            ))
            if not args.fast and (args.ticks is None or done < args.ticks):
                time.sleep(max(0.0, args.tick_seconds - (time.monotonic() - started)))
    print(
        f"spolu: tikov {done}, nové ponuky {total.initialized}, predaje {total.sales}, "
        f"predaných miest {total.seats_sold}, zmeny ceny {total.price_changes}, "
        f"vypredané {total.sold_out}, vypršané {total.expired}"
    )


if __name__ == "__main__":
    main()
