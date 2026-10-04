#!/usr/bin/env python3
"""Vytvorí topiky Kafky podľa docs/ARCHITEKTURA.md (partície, retention.ms=-1). Idempotentné.

    uv run python tools/create_topics.py
"""

import asyncio

from krakow_di.config import get_settings
from krakow_di.events.topics import ensure_topics


def main() -> None:
    s = get_settings()
    result = asyncio.run(ensure_topics(s.kafka_bootstrap_servers, s.kafka_topic_prefix))
    for topic, state in result.items():
        print(f"{topic}: {state}")


if __name__ == "__main__":
    main()
