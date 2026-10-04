#!/usr/bin/env python3
"""Kontrola externého prístupu k našej Kafke (SASL_SSL), spúšťať ZVONKU servera (z notebooku).

    uv run python tools/check_external.py --bootstrap kafka.example.org:9094 \\
        --user teams --password '<HESLO>' --cafile deploy/certs/ca.crt

Overuje: (1) čítanie od offsetu 0 funguje, (2) zápis je zakázaný, (3) cudzia consumer
group (bez prefixu team-) je zakázaná, (4) nesprávne heslo sa odmietne.
Návratový kód 0 = všetko v poriadku.
"""

import argparse
import asyncio
import json
import ssl
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

TOPIC = "krakow.flights.offers"


def common(args, password=None) -> dict:
    ctx = ssl.create_default_context(cafile=args.cafile)
    return dict(
        bootstrap_servers=args.bootstrap, security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512", sasl_plain_username=args.user,
        sasl_plain_password=args.password if password is None else password, ssl_context=ctx,
        request_timeout_ms=15000,
    )


async def can_read(args) -> str:
    consumer = AIOKafkaConsumer(
        TOPIC, group_id=args.group, auto_offset_reset="earliest", **common(args))
    await consumer.start()
    try:
        batch = await consumer.getmany(timeout_ms=15000, max_records=5)
        messages = [m for ms in batch.values() for m in ms]
        if not messages:
            raise RuntimeError(f"topik {TOPIC} je prázdny alebo sa nič neprečítalo")
        event = json.loads(messages[0].value)
        first = messages[0]
        return (f"prečítaných {len(messages)}, prvá správa {event['event_type']} "
                f"(offset {first.offset})")
    finally:
        await consumer.stop()


async def write_is_denied(args) -> str:
    producer = AIOKafkaProducer(**common(args))
    await producer.start()
    try:
        await producer.send_and_wait(TOPIC, b"{}")
    except KafkaError as e:
        return f"odmietnuté ako treba: {type(e).__name__}"
    finally:
        await producer.stop()
    raise RuntimeError("ZÁPIS PREŠIEL: používateľ nesmie mať právo zapisovať!")


async def foreign_group_is_denied(args) -> str:
    consumer = AIOKafkaConsumer(TOPIC, group_id="cudzia-skupina", auto_offset_reset="earliest",
                                **common(args))
    try:
        await consumer.start()
        await consumer.getmany(timeout_ms=8000)
    except KafkaError as e:
        return f"odmietnuté ako treba: {type(e).__name__}"
    finally:
        await consumer.stop()
    raise RuntimeError("cudzia consumer group je povolená: ACL pre skupiny nefunguje")


async def wrong_password_is_denied(args) -> str:
    consumer = AIOKafkaConsumer(
        TOPIC, group_id=args.group, **common(args, password="nespravne-heslo"))
    try:
        await consumer.start()
    except Exception as e:  # noqa: BLE001 (typ chyby závisí od verzie klienta)
        return f"odmietnuté ako treba: {type(e).__name__}"
    finally:
        try:
            await consumer.stop()
        except Exception:  # noqa: BLE001, S110
            pass
    raise RuntimeError("nesprávne heslo bolo prijaté!")


async def main_async(args) -> int:
    checks = [
        (f"čítanie od offsetu 0 (skupina {args.group})", can_read),
        ("zápis je zakázaný", write_is_denied),
        ("cudzia consumer group je zakázaná", foreign_group_is_denied),
        ("nesprávne heslo sa odmietne", wrong_password_is_denied),
    ]
    failed = 0
    for name, fn in checks:
        try:
            detail = await asyncio.wait_for(fn(args), 60)
            print(f"  [PASS] {name}: {detail}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  [FAIL] {name}: {type(e).__name__}: {e}")
    print(f"\nVýsledok: {len(checks) - failed} z {len(checks)} kontrol v poriadku")
    return 1 if failed else 0


def main() -> None:
    p = argparse.ArgumentParser(description="Kontrola externého prístupu ku Kafke (SASL_SSL)")
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--user", default="teams")
    p.add_argument("--password", required=True)
    p.add_argument("--cafile", help="CA certifikát (ca.crt), ak ho server nepodpísala verejná CA")
    p.add_argument("--group", default="team-check-external")
    sys.exit(asyncio.run(main_async(p.parse_args())))


if __name__ == "__main__":
    main()
