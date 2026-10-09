# Ako získať dáta tímu Kraków DI (TUKE)

Zbierame **letenky do Krakowa (KRK)** a **divadlo Teatr im. J. Słowackiego** a všetko streamujeme. Pripojiť sa dá štyrmi spôsobmi, vyberte si najjednoduchší pre vás.

Server: **`34.118.112.234`**

| Spôsob | Na čo | Adresa |
|---|---|---|
| REST | pozrieť si dáta, história | `http://34.118.112.234/api/v1/...` (dokumentácia: `/docs`) |
| SSE | živý prúd cez `curl` alebo prehliadač | `http://34.118.112.234/stream` |
| WebSocket | živý prúd v aplikácii | `ws://34.118.112.234/ws` |
| Kafka | hlavný stream, celá história | `34.118.112.234:9094` (treba heslo) |

## Čo posielame

Každá správa je JSON v rovnakom obale, `data` závisí od typu:

```json
{
  "event_id": "5b94b7be-...",  "event_type": "flight.ticket.sold",  "event_version": 1,
  "occurred_at": "2026-10-14T09:31:05Z",  "producer": "tuke-di-krakow",
  "source": "generator",
  "data": { "...": "..." }
}
```

`event_id` je jedinečný (podľa neho odstránite duplicity), `source` je pôvod: `travelpayouts` | `ryanair` | `theater` | `generator`.

| Typ udalosti | Čo znamená | Najdôležitejšie polia v `data` |
|---|---|---|
| `flight.offer.found` / `.observed` | nová / opakovane nájdená ponuka | `offer_id`, `origin_iata`, `destination_iata` (KRK), `departure_at`, `airline_iata`, `price`, `currency` (EUR), `seats_left` |
| `flight.offer.price_changed` | zmena ceny | `offer_id`, `old_price`, `new_price`, `reason` |
| `flight.offer.sold_out` / `.expired` | vypredaná / odletená ponuka | `offer_id` |
| `flight.ticket.sold` | **predaná letenka** | `sale_id`, `offer_id`, `quantity` (1–3), `unit_price`, `total_price`, `sold_at` |
| `theater.performance.found` / `.updated` | predstavenie v programe | `performance_id`, `title`, `stage`, `starts_at`, `status` |
| `theater.availability.snapshot` | voľné miesta podľa kategórií | `performance_id`, `categories[]` (`category`, `price`, `price_eur`, `seats_available`) |
| `theater.tickets.sold` | **predané vstupenky** | `performance_id`, `category`, `quantity`, `unit_price`, `unit_price_eur`, `simulated` |

## Čo je skutočné a čo simulované

- **Skutočné:** ponuky a ceny leteniek (Travelpayouts, Ryanair), program, ceny a voľné miesta v divadle.
- **Simulované generátorom:** predané letenky a počet miest (`source: generator`, v ponukách `seats_simulated: true`).
- **Predaje vstupeniek do divadla sú dvoch druhov:** skutočné, zistené z poklesu voľných miest (`simulated: false`, `source: theater`), a simulované (`simulated: true`, `source: generator`). Záporné `quantity` je vrátenie miest.
- Ceny leteniek sú v **EUR**. Divadlo predáva v **PLN** a pri cene je aj prepočet `price_eur` (kurz ECB v deň snímky).

## 1. REST (najjednoduchšie)

V prehliadači otvorte **`http://34.118.112.234/docs`**: každý dopyt si tam môžete vyskúšať kliknutím. V termináli:

```bash
curl 'http://34.118.112.234/api/v1/stats'                          # koľko čoho máme
curl 'http://34.118.112.234/api/v1/offers?origin=BCN&limit=5'      # ponuky leteniek
curl 'http://34.118.112.234/api/v1/sales?limit=5'                  # predané letenky
curl 'http://34.118.112.234/api/v1/theater/performances?limit=5'   # predstavenia
curl 'http://34.118.112.234/api/v1/theater/sales?limit=5'          # predané vstupenky (aj ?source=generator)
```

Zoznamy majú tvar `{"items": [...], "total": N, "limit": L, "offset": O}`; ďalšie stránky cez `offset`.

## 2. SSE (živý prúd)

```bash
curl -N 'http://34.118.112.234/stream?types=flight.ticket.sold'
```

`types` je voliteľný filter (viac typov oddeľte čiarkou, `theater.*` = všetky divadelné). Chcete aj históriu? Pridajte `last_event_id=0`. Po výpadku sa knižnica pripojí s `Last-Event-ID` a nič neprídete.

## 3. WebSocket

`ws://34.118.112.234/ws?types=flight.ticket.sold`, rovnaké filtre. Správa: `{"seq": 123, "event": {...}}`.

## 4. Kafka

Od nás potrebujete **heslo** a súbor **`ca.crt`** (napíšte nám).

- `34.118.112.234:9094`, `SASL_SSL`, mechanizmus `SCRAM-SHA-512`, používateľ `teams` (iba čítanie)
- consumer group musí začínať **`team-`** (napr. `team-05-reader`)
- topiky: `krakow.flights.offers`, `krakow.flights.sales`, `krakow.theater.performances`, `krakow.theater.availability`, `krakow.theater.sales` (kľúč správy = `offer_id` / `performance_id`)
- história sa nemaže: `auto_offset_reset=earliest` prečíta všetko od začiatku

```python
import asyncio, json, ssl
from aiokafka import AIOKafkaConsumer

async def main():
    consumer = AIOKafkaConsumer(
        "krakow.flights.sales", "krakow.theater.sales",
        bootstrap_servers="34.118.112.234:9094", group_id="team-05-reader",
        auto_offset_reset="earliest", security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512", sasl_plain_username="teams",
        sasl_plain_password="<HESLO>", ssl_context=ssl.create_default_context(cafile="ca.crt"),
    )
    await consumer.start()
    async for msg in consumer:
        print(json.loads(msg.value)["event_type"])

asyncio.run(main())
```

Otázky alebo problémy s pripojením? Napíšte nám.
