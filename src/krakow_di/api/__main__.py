"""Spustenie API: `uv run python -m krakow_di.api [--host 0.0.0.0] [--port 8000]`."""

import argparse
import logging

import uvicorn

from krakow_di.api.app import create_app


def main() -> None:
    p = argparse.ArgumentParser(description="API Kraków DI (REST + SSE + WebSocket)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    uvicorn.run(create_app(), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
