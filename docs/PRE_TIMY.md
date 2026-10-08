# Ako sa pripojiť k našim dátam (pre ostatné tímy)

Tím **Kraków DI** (TUKE) publikuje dva druhy dát:

- **Letenky do Krakova (KRK)**: reálne ponuky z Travelpayouts a Ryanairu. **Predaje leteniek a počet miest sú simulované** generátorom (označené `seats_simulated: true`, `source: generator`).
- **Divadlo Teatr im. J. Słowackiego**: reálny program, ceny a dostupnosť miest. Udalosť `theater.tickets.sold` má dva druhy: **reálne predaje** zistené z rozdielu snímok (`source: theater`, `simulated: false`) a **simulované predaje** generátora (`source: generator`, `simulated: true`). Ceny sú v PLN aj v EUR.

| Kanál | Na čo | Adresa |
|---|---|---|
| **Apache Kafka** (hlavný) | stream, celá história od offsetu 0 | `<HOST>:9094` (SASL_SSL), lokálne `localhost:9092` |
| **SSE** (`GET /stream`) | stream cez HTTP, `curl` stačí | `https://<HOST>/stream` |
| **WebSocket** (`/ws`) | stream cez WebSocket | `wss://<HOST>/ws` |
| **REST** (`/api/v1/...`) | dopyty na uložené dáta, stránkovanie | `https://<HOST>/api/v1/...`, dokumentácia `/docs` |

`<HOST>`, používateľa a heslo dostanete od nás (pozri „Prístupové údaje“ nižšie). Formát správ je v [UDALOSTI.md](UDALOSTI.md), strojovo čitateľný kontrakt v [asyncapi.yaml](asyncapi.yaml) a JSON Schema v `schemas/events/`.

## 1. Formát správy

Hodnota správy je JSON (UTF-8) v jednotnom obale (envelope):

```json
{
  "event_id": "5b94b7be-4db2-47b8-94ad-dd16897ccdd7",
  "event_type": "flight.ticket.sold",
  "event_version": 1,
  "occurred_at": "2026-10-04T15:01:03Z",
  "producer": "tuke-di-krakow",
  "source": "generator",
  "data": {
    "sale_id": "f1278f4e-fd89-40a7-8e00-319878fffae2",
    "offer_id": "0f922af3394cd5c6",
    "origin_iata": "CIA", "destination_iata": "KRK",
    "departure_at": "2026-11-26T16:10:00+00:00",
    "airline_iata": "FR", "flight_number": "FR82",
    "quantity": 1, "unit_price": 44.99, "total_price": 44.99, "currency": "EUR",
    "seats_left_after": 190, "sold_at": "2026-10-04T15:01:03Z"
  }
}
```

- **Kľúč správy** je id entity (`offer_id` / `performance_id`): všetky udalosti jednej ponuky idú do jednej partície, v správnom poradí.
- **Hlavička** `event_type` duplikuje typ udalosti (filtrovanie bez parsovania tela).
- **Doručenie je at-least-once**: ak chcete odstraňovať duplicity, použite `event_id`.

### Topiky

| Topik | Udalosti | Kľúč | Partície |
|---|---|---|---|
| `krakow.flights.offers` | `flight.offer.found`, `.observed`, `.price_changed`, `.sold_out`, `.expired` | `offer_id` | 3 |
| `krakow.flights.sales` | `flight.ticket.sold` | `offer_id` | 3 |
| `krakow.theater.performances` | `theater.performance.found`, `.updated` | `performance_id` | 1 |
| `krakow.theater.availability` | `theater.availability.snapshot` | `performance_id` | 1 |
| `krakow.theater.sales` | `theater.tickets.sold` | `performance_id` | 1 |

Retencia je **neobmedzená**: ktorýkoľvek tím sa môže pripojiť kedykoľvek a prečítať všetko od začiatku.

## 2. Kafka

**Prístupové údaje:** SASL_SSL, mechanizmus `SCRAM-SHA-512`, používateľ `teams` (iba na čítanie), heslo dostanete od nás (nie je v repozitári). Consumer group musí začínať **`team-`** (napr. `team-05-lake`). Zapisovať k nám nemožno.

### kcat

```bash
# celá história od začiatku
kcat -C -b <HOST>:9094 -t krakow.flights.sales -o beginning -e \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username=teams -X sasl.password='<HESLO>' \
  -f '%t[%p]@%o key=%k  %s\n'

# všetky naše topiky naraz so skupinou (pokračuje tam, kde skončila)
kcat -b <HOST>:9094 -G team-05-reader \
  krakow.flights.offers krakow.flights.sales krakow.theater.performances \
  krakow.theater.availability krakow.theater.sales \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username=teams -X sasl.password='<HESLO>'
```

### Python (aiokafka)

```python
import asyncio, json, ssl
from aiokafka import AIOKafkaConsumer

async def main():
    consumer = AIOKafkaConsumer(
        "krakow.flights.sales", "krakow.theater.sales",
        bootstrap_servers="<HOST>:9094",
        group_id="team-05-reader",
        auto_offset_reset="earliest",            # od offsetu 0
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username="teams",
        sasl_plain_password="<HESLO>",
        ssl_context=ssl.create_default_context(),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            print(msg.topic, msg.partition, msg.offset, event["event_type"], event["occurred_at"])
    finally:
        await consumer.stop()

asyncio.run(main())
```

### Python (confluent-kafka)

```python
import json
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "<HOST>:9094", "group.id": "team-05-reader",
    "auto.offset.reset": "earliest",
    "security.protocol": "SASL_SSL", "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": "teams", "sasl.password": "<HESLO>",
})
c.subscribe(["krakow.flights.sales"])
while True:
    msg = c.poll(1.0)
    if msg is not None and not msg.error():
        print(json.loads(msg.value())["event_type"])
```

### Lokálne (vývojový broker bez autentifikácie)

Ak si náš projekt spustíte lokálne (`docker compose up -d`), broker počúva na `localhost:9092` bez prihlásenia a topiky vytvoríte príkazom `uv run python tools/create_topics.py`. Kontrolný consumer: `uv run python tools/consume.py --from-beginning`.

## 3. SSE (HTTP, bez Kafka klienta)

```bash
# živý prúd iba predajov leteniek
curl -N 'https://<HOST>/stream?types=flight.ticket.sold'

# vzory: všetky divadelné udalosti + predaje leteniek
curl -N 'https://<HOST>/stream?types=flight.ticket.sold,theater.*'

# dobehnutie: všetko s id vyšším ako 1000 a potom živý prúd
curl -N -H 'Last-Event-ID: 1000' 'https://<HOST>/stream'

# skúška: skončiť po 5 udalostiach
curl -N 'https://<HOST>/stream?last_event_id=0&max=5'
```

Každý rámec má `id` (poradové číslo udalosti v našom žurnáli), `event` (typ udalosti) a `data` (celá udalosť ako JSON). Prehliadač a väčšina SSE klientov pri výpadku sama pošle `Last-Event-ID`, takže nič neprídete. Spojenie je udržiavané komentárom `: keepalive` každých 15 s.

```python
import json, httpx

with httpx.stream("GET", "https://<HOST>/stream", params={"types": "theater.*", "last_event_id": 0},
                  timeout=None) as r:
    for line in r.iter_lines():
        if line.startswith("data: "):
            event = json.loads(line[6:])
            print(event["event_type"], event["data"].get("performance_id"))
```

## 4. WebSocket

```python
import asyncio, json, websockets

async def main():
    url = "wss://<HOST>/ws?types=flight.ticket.sold&last_event_id=0"
    async with websockets.connect(url) as ws:
        async for message in ws:
            m = json.loads(message)          # {"seq": 123, "event": {...}}
            print(m["seq"], m["event"]["event_type"])

asyncio.run(main())
```

Parametre sú rovnaké ako pri SSE (`types`, `last_event_id`, `max`). Pri neplatnom filtri server zavrie spojenie kódom 1008.

## 5. REST

Interaktívna dokumentácia (OpenAPI) je na `https://<HOST>/docs`.

```bash
curl 'https://<HOST>/health'
curl 'https://<HOST>/api/v1/stats'

# ponuky: filtre source, origin, status, departure_from/to; stránkovanie limit/offset
curl 'https://<HOST>/api/v1/offers?origin=BCN&status=active&limit=20'
curl 'https://<HOST>/api/v1/offers/<offer_id>'
curl 'https://<HOST>/api/v1/offers/<offer_id>/history'     # cena a miesta v čase

# predaje leteniek (simulované)
curl 'https://<HOST>/api/v1/sales?since=2026-10-04T00:00:00Z&limit=100'

# divadlo
curl 'https://<HOST>/api/v1/theater/performances?status=on_sale'
curl 'https://<HOST>/api/v1/theater/performances/<performance_id>/snapshots'
curl 'https://<HOST>/api/v1/theater/sales'

# žurnál všetkých udalostí s kurzorom (next_after_seq)
curl 'https://<HOST>/api/v1/events?types=flight.ticket.sold&after_seq=0&limit=500'
```

Zoznamové odpovede majú tvar `{"items": [...], "total": N, "limit": L, "offset": O}`; žurnál udalostí vracia `{"items": [{"seq", "topic", "event"}], "next_after_seq": ...}`.

## 6. Čo treba vedieť o dátach

- **Ceny leteniek** sú v EUR. `price` v `flight.offer.*` je cena zo zdroja; `unit_price` v `flight.ticket.sold` je predajná cena v čase predaja (cena zo zdroja × prirážka podľa obsadenosti), rozdiel vysvetľujú udalosti `flight.offer.price_changed` s `reason: load_factor`.
- **Počet miest a predaje leteniek sú simulácia**; v ponukách je `seats_simulated: true`. Kým generátor ponuke nepriradí kapacitu, sú `seats_total`/`seats_left` `null`.
- **Divadlo:** `price` je v PLN, `price_eur` je prepočet podľa kurzu ECB z `fx_rate` (PLN za 1 EUR). Simulované predaje (`simulated: true`) sú vždy kladné. Záporné `quantity` v `theater.tickets.sold` znamená vrátenie miest (aj dočasné uvoľnenie z cudzieho košíka).
- **Časy:** `occurred_at` je UTC (`...Z`); `departure_at` a `starts_at` majú posun miestneho času.
- **Verzie:** pridanie nepovinného poľa nemení `event_version`; zmenu, ktorá láme kompatibilitu, publikujeme ako novú verziu a starú ponecháme paralelne aspoň 2 týždne.

## Prístupové údaje

Adresu servera, používateľa a heslo pre Kafku posielame na požiadanie priamo (nie sú súčasťou repozitára). Do nasadenia na server (fáza 9) sú dostupné iba lokálne adresy uvedené vyššie.
