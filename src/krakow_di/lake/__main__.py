"""Spustenie data lake: `uv run python -m krakow_di.lake [--config ...] [--duration S]`."""

import argparse
import asyncio
import logging
import signal

from krakow_di.config import get_settings
from krakow_di.lake.config import DEFAULT_PATH, load_adapters
from krakow_di.lake.runner import run_all

log = logging.getLogger("krakow_di.lake")


async def amain(args: argparse.Namespace) -> None:
    adapters = load_adapters(args.config)
    if not adapters:
        log.warning("v %s nie je žiadny zapnutý zdroj", args.config)
        return
    log.info("zdroje: %s", ", ".join(a.name for a in adapters))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    if args.duration:
        loop.call_later(args.duration, stop.set)
    stats = await run_all(adapters, get_settings().database_url, stop)
    for name, s in stats.items():
        print(f"{name}: uložené {s.written}, duplicity {s.duplicates}, reštarty {s.restarts}"
              + (f", posledná chyba: {s.last_error}" if s.last_error else ""))


def main() -> None:
    p = argparse.ArgumentParser(description="Data lake: adaptéry cudzích tímov a našich udalostí")
    p.add_argument("--config", default=str(DEFAULT_PATH))
    p.add_argument("--duration", type=float, help="skončiť po N sekundách (na skúšanie)")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(amain(p.parse_args()))


if __name__ == "__main__":
    main()
