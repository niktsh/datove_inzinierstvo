# Tím 4

Zdroj: ich dokument „Dokumentácia k API pre divadlá“ a „Dokumentácia k API pre letenky“ spolu so
skriptom na zber dát. Adaptér: [`team_4.py`](../../src/krakow_di/lake/adapters/team_4.py), zdroje v
[`config/lake_sources.yaml`](../../config/lake_sources.yaml) (tím `team_4`, kanály `rest_theatre`
a `rest_flights`). Oba zdroje sú REST, preto sa kanály líšia názvom: runner vyžaduje jedinečnú
dvojicu tím/kanál.

## Ako svoje dáta poskytujú

### Divadlo: Supabase REST (`rest_theatre`)
- Základ: `https://zvopbhzdgtxtujobyfgs.supabase.co/rest/v1`, iba čítanie (HTTP GET).
- Prihlásenie: verejný („publishable“) kľúč v hlavičke **`apikey`**. Hlavička
  `Authorization: Bearer` s týmto kľúčom skončí chybou 401, preto sa nedá použiť univerzálny
  `rest` adaptér. Kľúč nie je v repozitári: patrí do `.env` ako `TEAM4_SUPABASE_KEY`.
- Dva pohľady:
  - `theatre_availability_latest`: posledná zaznamenaná dostupnosť (`event_name`, `instance_id`,
    `performance_datetime`, `checked_at`, `price_band_name`, `ticket_type_name`, `price`,
    `currency`, `available`). V čase pridania 304 riadkov.
  - `tickets_sold`: predaj podľa predstavenia, dátumu a ceny (`event`, `performance_date`,
    `ticket_price`, `tickets_sold`). V čase pridania 15 riadkov.
- Odpoveď je pole JSON. Supabase vracia najviac 1000 riadkov, preto sa stránkuje `limit`/`offset`.
- Aktualizácia podľa nich každých 12 hodín (okolo 00:00 a 12:00 UTC+2), s možným oneskorením.

### Letenky: FastAPI na Renderi (`rest_flights`)
- `GET https://d-tov-in-inierstvo.onrender.com/api/flights`, bez prihlasovania (dokumentácia
  `/docs`). Ich `POST /api/flights` je pre ich zberový skript, my ho nevoláme.
- Tvar: `{"team": "...", "flights": [{origin, destination, flight_number, price, airline,
  departure_at}]}`. Ceny v eurách, čas ISO 8601. Doteraz sme videli `{"team": "Tím 4",
  "flights": []}` (zoznam prázdny); ich dokument uvádza cieľ Dublin (DUB).
- Poskytuje **iba poslednú dávku** letov: zberový skript ju každých 15 minút nahradí novou, história
  neexistuje. Hosting Render po nečinnosti „zaspí“, prvá odpoveď môže trvať dlhšie.

## Ako to ukladáme (bez normalizácie)

Každá odpoveď (stránka) sa uloží ako surové bajty; `payload_json` sa vyplní, keďže ide o JSON.
`source_ref` = `{url, offset, sha256}`. Nezmenená odpoveď sa neuloží znova (odtlačok obsahu),
zmena vytvorí novú správu, takže vznikne história snímok, ktorú ich API samo nemá.

- **Divadlo:** raz za 6 hodín (ich obnova je dvakrát denne). Pohľady sa čítajú s pevným `order`, aby
  sa poradie riadkov medzi dopytmi nemenilo a odtlačok bol stabilný.
- **Letenky:** raz za 15 minút (ich interval), časový limit 90 s kvôli studenému štartu.
- Chýbajúci kľúč v `.env` nezhodí lake: kanál `rest_theatre` sa opakovane reštartuje s chybovou
  hláškou, ostatné zdroje bežia.

## Obmedzenia
- Z letenkového API vidíme iba aktuálnu dávku: ak sa medzi dvoma našimi dopytmi dávka zmení dvakrát,
  prvá sa nedostane k nám.
- Stĺpec `available` a `tickets_sold` sú ich údaje; nevieme, či ide o skutočné alebo simulované
  hodnoty. Warehouse s tým musí rátať.

Vzorky ich odpovedí pre testy: `tests/fixtures/team_4/` (zaznamenané 2026-10-09; `flights_sample.json`
je príklad z ich dokumentu, nie skutočná odpoveď).
