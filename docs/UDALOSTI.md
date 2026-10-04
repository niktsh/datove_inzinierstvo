# Katalóg udalostí (náš verejný kontrakt)

Toto vidia ostatné tímy. Každú zmenu synchronizovať so `schemas/events/*.json` a `docs/asyncapi.yaml`.

Transport: **Apache Kafka**. Hodnota správy je JSON (UTF-8) s envelope nižšie. Kľúč je id entity. Hlavička `event_type` duplikuje typ udalosti.

## Envelope (spoločný obal každej správy)

```json
{
  "event_id": "0f8b6c1e-2d4a-4b8e-9a51-7f3c2e1d9b00",
  "event_type": "flight.ticket.sold",
  "event_version": 1,
  "occurred_at": "2026-10-14T09:31:05Z",
  "producer": "tuke-di-krakow",
  "source": "generator",
  "data": { }
}
```

| Pole | Typ | Popis |
|---|---|---|
| `event_id` | UUID v4 | Jedinečné id udalosti. Konzument podľa neho môže deduplikovať. |
| `event_type` | string | Typ udalosti (aj v Kafka hlavičke `event_type`). |
| `event_version` | int | Verzia schémy `data` pre tento typ. |
| `occurred_at` | ISO 8601 UTC | Kedy udalosť nastala (nie kedy bola odoslaná). |
| `producer` | string | Náš identifikátor. |
| `source` | enum | `travelpayouts` \| `ryanair` \| `theater` \| `generator` |
| `data` | object | Telo udalosti, závisí od typu. |

## Letenky

### `flight.offer.found` — zberač našiel novú ponuku
```json
{
  "offer_id": "a1b2c3d4e5f60718",
  "origin_iata": "BCN",
  "destination_iata": "KRK",
  "departure_at": "2026-11-20T06:15:00+01:00",
  "arrival_at": "2026-11-20T09:10:00+01:00",
  "airline_iata": "FR",
  "flight_number": "FR3035",
  "stops": 0,
  "price": 54.99,
  "currency": "EUR",
  "seats_total": 189,
  "seats_left": 141,
  "seats_simulated": true,
  "observed_at": "2026-10-14T06:00:12Z"
}
```
`flight_number` môže byť `null` (Travelpayouts ho nie vždy poskytuje). `seats_*` sú vždy simulované generátorom, čo je férovo označené poľom `seats_simulated`; kým generátor ponuke nepriradí kapacitu (fáza 6), sú `seats_total` a `seats_left` `null`.

### `flight.offer.observed` — opakovaný zber už známej ponuky
Rovnaké `data` ako pri `offer.found`. Posiela sa pri každom zbere, aj keď sa nič nezmenilo (pre históriu v data lake).

### `flight.offer.price_changed`
```json
{ "offer_id": "a1b2c3d4e5f60718", "old_price": 54.99, "new_price": 61.49, "currency": "EUR", "reason": "load_factor" }
```
`reason`: `load_factor` (generátor) | `scrape` (reálna cena u zdroja sa zmenila).

### `flight.ticket.sold` — kľúčová udalosť generátora
```json
{
  "sale_id": "5d0c9f3a-...",
  "offer_id": "a1b2c3d4e5f60718",
  "origin_iata": "BCN",
  "destination_iata": "KRK",
  "departure_at": "2026-11-20T06:15:00+01:00",
  "airline_iata": "FR",
  "flight_number": "FR3035",
  "quantity": 2,
  "unit_price": 54.99,
  "total_price": 109.98,
  "currency": "EUR",
  "seats_left_after": 139,
  "sold_at": "2026-10-14T09:31:05Z"
}
```
Zámerne denormalizované: konzument nemusí poznať `offer.found`, aby pochopil predaj.

### `flight.offer.sold_out`
```json
{ "offer_id": "a1b2c3d4e5f60718", "last_price": 89.99, "currency": "EUR" }
```

### `flight.offer.expired`
```json
{ "offer_id": "a1b2c3d4e5f60718", "reason": "departed" }
```
`reason`: `departed` | `not_found_at_source`.

## Divadlo

### `theater.performance.found` / `theater.performance.updated`
```json
{
  "performance_id": "wielki-gatsby-2026-11-03-19-00",
  "title": "Wielki Gatsby",
  "stage": "Scena MOS",
  "starts_at": "2026-11-03T19:00:00+01:00",
  "url": "https://bilety.teatrwkrakowie.pl/kup-bilet/wielki-gatsby-2026-11-03-19-00",
  "status": "on_sale"
}
```
`url` môže byť `null` pri vypredaných predstaveniach (pokladňa odkaz neposkytuje). `performance_id` je stabilné: pri prechode medzi `on_sale` a `sold_out` sa nemení.
`status`: `on_sale` | `sold_out` | `cancelled` | `past`.

### `theater.availability.snapshot` — reálna snímka dostupnosti
```json
{
  "performance_id": "krakow-narodowej-sztuce-czyli-tryumf-miernoty-2026-10-27-19-00",
  "observed_at": "2026-10-14T12:00:03Z",
  "fx_rate": 4.3775,
  "categories": [
    { "category": "Normalny", "price": 120.00, "currency": "PLN", "price_eur": 27.41, "seats_available": 4 },
    { "category": "strefa C /ograniczona widoczność", "price": 50.00, "currency": "PLN", "price_eur": 11.42, "seats_available": 9 }
  ],
  "seats_available_total": 13
}
```
Kategóriu určuje dvojica `(category, price)`: jedno predstavenie môže mať dve kategórie „Normalny“ s rôznou cenou. `price` je v mene pokladne (PLN), `price_eur` podľa kurzu ECB `fx_rate` (PLN za 1 EUR); ak kurz nie je k dispozícii, `price_eur` aj `fx_rate` sú `null`.

### `theater.tickets.sold` — **reálne** predaje vypočítané z rozdielu snímok
```json
{
  "performance_id": "krakow-narodowej-sztuce-czyli-tryumf-miernoty-2026-10-27-19-00",
  "category": "Normalny",
  "quantity": 3,
  "unit_price": 120.00,
  "currency": "PLN",
  "unit_price_eur": 27.41,
  "fx_rate": 4.3775,
  "detected_between": ["2026-10-14T08:00:01Z", "2026-10-14T12:00:03Z"]
}
```
Predaj sa eviduje s presnosťou na interval medzi snímkami. Ak miest pribudlo (vrátenie), `quantity` je záporné. Miesto, ktoré dočasne drží cudzí košík, vyzerá ako predané a neskôr ako vrátené; pri intervale 2–6 hodín sa to do snímky dostane zriedka. Prvá snímka predstavenia je iba základná: predaje sa podľa nej nepočítajú.

## Topiky Kafky

| Topik | Typy udalostí | Kľúč |
|---|---|---|
| `krakow.flights.offers` | `flight.offer.found`, `flight.offer.observed`, `flight.offer.price_changed`, `flight.offer.sold_out`, `flight.offer.expired` | `offer_id` |
| `krakow.flights.sales` | `flight.ticket.sold` | `offer_id` |
| `krakow.theater.performances` | `theater.performance.found`, `theater.performance.updated` | `performance_id` |
| `krakow.theater.availability` | `theater.availability.snapshot` | `performance_id` |
| `krakow.theater.sales` | `theater.tickets.sold` | `performance_id` |

Príklady pre konzumenta:
- iba predaje: odber `krakow.flights.sales` a `krakow.theater.sales`
- všetko: odber podľa vzoru `^krakow\..*`
- celá história od začiatku: nová consumer group + `auto_offset_reset=earliest` (retencia bez obmedzenia)

Pripojenie: SASL_SSL, mechanizmus SCRAM-SHA-512, používateľ iba na čítanie, consumer group musí začínať `team-` (napr. `team-05-lake`).
Adresa a prihlasovacie údaje sú v `docs/PRE_TIMY.md` (vznikne vo fáze 7).

## Verzionovanie

- Pridanie nepovinného poľa: tá istá verzia.
- Odstránenie/premenovanie poľa alebo zmena typu: `event_version + 1`, starú verziu publikujeme paralelne minimálne 2 týždne.
