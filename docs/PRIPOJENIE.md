# Možnosti pripojenia k dátam

Tento dokument stačí na to, aby ste sa k našim dátam pripojili a vedeli, čo v nich je.

## Aké údaje a odkiaľ ich zbierame

Zbierame dve veci a oznamujeme ich ako **udalosti** (správy vo formáte JSON):

- **Letenky do Krakowa (KRK)** z viacerých letísk Európy: ponuky, zmeny ceny a predané letenky.
- **Divadlo Teatr im. J. Słowackiego v Krakowe**: program, ceny, voľné miesta a predané vstupenky.

Nové udalosti pribúdajú počas celého dňa. Všetko, čo sme kedy poslali, je uložené, takže sa môžete pripojiť kedykoľvek a stiahnuť si aj históriu.

**Adresa servera:** `34.118.112.234`

## Ktorý spôsob si vybrať

| Chcem... | Použite | Adresa |
|---|---|---|
| len sa pozrieť, aké dáta máte | **REST** | `http://34.118.112.234/api/v1/...` |
| živý prúd udalostí, najjednoduchšie (stačí `curl` alebo prehliadač) | **SSE** | `http://34.118.112.234/stream` |
| živý prúd v aplikácii cez WebSocket | **WebSocket** | `ws://34.118.112.234/ws` |
| spoľahlivý stream s celou históriou (pre tých, čo používajú Kafku) | **Kafka** | `34.118.112.234:9094` |

REST, SSE a WebSocket fungujú hneď, bez hesla. Kafka potrebuje heslo a certifikát, ktoré vám pošleme na požiadanie.

## Ako sa pripojiť

### REST: pozrieť si dáta

Najjednoduchšie: otvorte v prehliadači `http://34.118.112.234/docs`. Je tam zoznam všetkých dopytov a každý si môžete vyskúšať kliknutím.

V termináli:

```bash
curl 'http://34.118.112.234/api/v1/stats'                          # koľko čoho máme
curl 'http://34.118.112.234/api/v1/offers?origin=BCN&limit=5'      # ponuky leteniek z Barcelony
curl 'http://34.118.112.234/api/v1/sales?limit=5'                  # predané letenky
curl 'http://34.118.112.234/api/v1/theater/performances?limit=5'   # divadelné predstavenia
curl 'http://34.118.112.234/api/v1/theater/sales?limit=5'          # predané vstupenky
```

Odpoveď má tvar `{"items": [...], "total": 120, "limit": 5, "offset": 0}`. Ďalšie stránky získate zvýšením `offset` (napr. `&offset=5`).

### SSE: živý prúd

```bash
curl -N 'http://34.118.112.234/stream?types=flight.ticket.sold'
```

Spojenie ostane otvorené a vždy, keď predáme letenku, dostanete novú udalosť.

- `types` je filter. Typy oddeľte čiarkou, `theater.*` znamená všetky divadelné udalosti. Bez filtra dostanete všetko.
- Chcete začať od začiatku, nie len od teraz? Pridajte `&last_event_id=0`.
- Ak spojenie padne, väčšina knižníc a prehliadačov sa pripojí sama a pokračuje tam, kde skončila.

Príklad v Pythone:

```python
import json, httpx

with httpx.stream("GET", "http://34.118.112.234/stream",
                  params={"types": "flight.ticket.sold"}, timeout=None) as r:
    for line in r.iter_lines():
        if line.startswith("data: "):
            event = json.loads(line[6:])
            print(event["event_type"], event["data"])
```

### WebSocket

```python
import asyncio, json, websockets

async def main():
    async with websockets.connect("ws://34.118.112.234/ws?types=flight.ticket.sold") as ws:
        async for message in ws:
            print(json.loads(message)["event"])      # správa: {"seq": 123, "event": {...}}

asyncio.run(main())
```

Filter `types` a `last_event_id` fungujú rovnako ako pri SSE.

### Kafka

Heslo a certifikát `ca.crt` vám pošleme. Potom:

- adresa `34.118.112.234:9094`, protokol `SASL_SSL`, mechanizmus `SCRAM-SHA-512`
- používateľ `teams`, heslo od nás (môžete iba čítať, zapisovať k nám nejde)
- názov vašej **consumer group** musí začínať `team-`, napr. `team-05-reader`
- `auto_offset_reset=earliest` prečíta všetko od začiatku; história sa nemaže

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

Topiky (v každom sú správy jednej témy, kľúč správy je id letenky alebo predstavenia):

| Topik | Čo obsahuje |
|---|---|
| `krakow.flights.offers` | ponuky leteniek a ich zmeny |
| `krakow.flights.sales` | predané letenky |
| `krakow.theater.performances` | program divadla |
| `krakow.theater.availability` | voľné miesta v divadle |
| `krakow.theater.sales` | predané vstupenky do divadla |

## Ako vyzerá správa

Každá správa je JSON v rovnakom obale, v poli `data` sú samotné údaje:

```json
{
  "event_id": "5b94b7be-4db2-47b8-94ad-dd16897ccdd7",
  "event_type": "flight.ticket.sold",
  "event_version": 1,
  "occurred_at": "2026-10-14T09:31:05Z",
  "producer": "tuke-di-krakow",
  "source": "generator",
  "data": {
    "sale_id": "f1278f4e-fd89-40a7-8e00-319878fffae2",
    "offer_id": "0f922af3394cd5c6",
    "origin_iata": "BCN", "destination_iata": "KRK",
    "departure_at": "2026-11-26T16:10:00+00:00",
    "airline_iata": "FR", "flight_number": "FR82",
    "quantity": 2, "unit_price": 44.99, "total_price": 89.98, "currency": "EUR",
    "seats_left_after": 188, "sold_at": "2026-10-14T09:31:05Z"
  }
}
```

- `event_id` je jedinečný: ak dostanete tú istú správu dvakrát, poznáte ju podľa neho.
- `event_type` hovorí, čo sa stalo (tabuľka nižšie). `occurred_at` je čas udalosti v UTC.

### Typy udalostí

| `event_type` | Čo sa stalo | Hlavné údaje v `data` |
|---|---|---|
| `flight.offer.found` | našli sme novú ponuku letenky | `offer_id`, `origin_iata`, `destination_iata`, `departure_at`, `airline_iata`, `price`, `currency`, `seats_left` |
| `flight.offer.observed` | znova sme videli známu ponuku (pre históriu) | rovnaké ako `found` |
| `flight.offer.price_changed` | zmenila sa cena | `offer_id`, `old_price`, `new_price` |
| `flight.offer.sold_out` | ponuka je vypredaná | `offer_id`, `last_price` |
| `flight.offer.expired` | let už odletel | `offer_id` |
| `flight.ticket.sold` | **predali sme letenku** | `sale_id`, `offer_id`, `quantity` (1–3), `unit_price`, `total_price`, `sold_at` |
| `theater.performance.found`, `theater.performance.updated` | predstavenie v programe | `performance_id`, `title`, `stage`, `starts_at`, `status` |
| `theater.availability.snapshot` | koľko miest je voľných | `performance_id`, `categories[]` (`category`, `price`, `price_eur`, `seats_available`) |
| `theater.tickets.sold` | **predali sa vstupenky** | `performance_id`, `category`, `quantity`, `unit_price`, `unit_price_eur`, `simulated` |

## Čo je dobré vedieť

- **Čo je skutočné a čo simulované.** Ponuky leteniek, ceny a program divadla sú skutočné. **Predané letenky a počet voľných miest pri letenkách sú simulované** generátorom (udalosť má `source: generator`). Pri divadle sú dva druhy predaja: skutočný (z poklesu voľných miest, `simulated: false`) a simulovaný (`simulated: true`). Podľa tohto poľa ich oddelíte.
- **Meny.** Letenky sú v **EUR**. Divadlo predáva v **PLN**; pri cene je aj `price_eur` prepočítané kurzom ECB.
- **Čas.** `occurred_at` a `sold_at` sú v UTC (končia na `Z`). `departure_at` a `starts_at` majú posun miestneho času (napr. `+01:00`).
- **Záporné množstvo.** Pri vstupenkách znamená záporné `quantity` vrátenie miest.
- **Duplicity.** Správa vám môže prísť dvakrát (napr. po výpadku spojenia). Odstránite ich podľa `event_id`.
- **Poradie.** Udalosti jednej letenky (alebo jedného predstavenia) idú vždy za sebou v správnom poradí.

Potrebujete heslo ku Kafke alebo niečo nefunguje? Napíšte nám.
