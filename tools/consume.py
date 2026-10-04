#!/usr/bin/env python3
"""Testovací consumer: číta topiky, overuje správy podľa JSON Schema a poradie podľa kľúča.

    uv run python tools/consume.py --from-beginning
    uv run python tools/consume.py --topics krakow.flights.sales --max 20 --print

Skončí po `--idle` sekundách bez nových správ (alebo po `--max` správach) a vypíše súhrn:
počty podľa typu udalosti, neplatné správy a porušenia poradia (occurred_at v rámci kľúča
nesmie klesať). Návratový kód je 1, ak sa našla neplatná správa alebo porušené poradie.
"""

import argparse
import asyncio
import json
import sys
from collections import Counter

from aiokafka import AIOKafkaConsumer

from krakow_di.config import get_settings
from krakow_di.events.envelope import InvalidEvent, validate
from krakow_di.events.routing import key_for, topic_names


async def run(args: argparse.Namespace) -> int:
    s = get_settings()
    topics = args.topics or list(topic_names(s.kafka_topic_prefix))
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=args.bootstrap or s.kafka_bootstrap_servers,
        group_id=args.group,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=bool(args.group),
    )
    await consumer.start()
    types: Counter[str] = Counter()
    invalid: list[str] = []
    disorder: list[str] = []
    last_seen: dict[tuple[str, str], str] = {}
    total = 0
    try:
        while args.max is None or total < args.max:
            batch = await consumer.getmany(timeout_ms=int(args.idle * 1000), max_records=500)
            if not batch:
                break
            for messages in batch.values():
                for m in messages:
                    total += 1
                    try:
                        event = json.loads(m.value)
                        validate(event)
                        header = dict(m.headers).get("event_type", b"").decode()
                        if header != event["event_type"]:
                            raise InvalidEvent("hlavička event_type sa nezhoduje s telom")
                        if m.key.decode() != key_for(event):
                            raise InvalidEvent("kľúč správy nie je id entity")
                    except (ValueError, KeyError, InvalidEvent) as e:
                        invalid.append(f"{m.topic}[{m.partition}]@{m.offset}: {e}")
                        continue
                    types[event["event_type"]] += 1
                    ident = (m.topic, m.key.decode())
                    prev = last_seen.get(ident)
                    if prev is not None and event["occurred_at"] < prev:
                        disorder.append(f"{m.topic}[{m.partition}]@{m.offset} kľúč {ident[1]}")
                    last_seen[ident] = event["occurred_at"]
                    if args.print:
                        print(f"{m.topic}[{m.partition}]@{m.offset} {event['event_type']} "
                              f"{m.key.decode()} {event['occurred_at']}")
    finally:
        await consumer.stop()
    print(f"\nPrečítaných správ: {total}, platných: {sum(types.values())}")
    for et, n in sorted(types.items()):
        print(f"  {et}: {n}")
    print(f"Neplatných správ: {len(invalid)}, porušení poradia: {len(disorder)}")
    for line in (invalid + disorder)[:10]:
        print("  !", line)
    return 1 if invalid or disorder else 0


def main() -> None:
    p = argparse.ArgumentParser(description="Testovací consumer udalostí Kraków DI")
    p.add_argument("--topics", nargs="+", help="predvolene všetky topiky projektu")
    p.add_argument("--from-beginning", action="store_true", help="čítať od offsetu 0")
    p.add_argument("--group", help="consumer group (bez nej sa offsety neukladajú)")
    p.add_argument("--bootstrap", help="prepíše KAFKA_BOOTSTRAP_SERVERS")
    p.add_argument("--max", type=int, help="skončiť po N správach")
    p.add_argument("--idle", type=float, default=5.0, help="skončiť po N s bez správ")
    p.add_argument("--print", action="store_true", help="vypísať každú správu")
    sys.exit(asyncio.run(run(p.parse_args())))


if __name__ == "__main__":
    main()
