# Zdroj: Ryanair (spike, 2026-10-03)

**Záver: funguje obyčajným HTTP bez prehliadača, tokenu a kontroly „či ste človek“. Hodí sa ako druhý zdroj namiesto Wizz Air (pozri `wizzair.md`).** Je to neoficiálny, ale otvorený endpoint „Fare Finder“, ktorý používa samotný web.

## Endpointy

### 1. Ceny: `GET https://www.ryanair.com/api/farfnd/v4/oneWayFares`
(to isté odpovedá aj na `https://services-api.ryanair.com/farfnd/v4/oneWayFares`)

| Parameter | Hodnota |
|---|---|
| `departureAirportIataCode` | letisko odletu, napr. `BCN` |
| `arrivalAirportIataCode` | `KRK` |
| `outboundDepartureDateFrom` / `...To` | okno dátumov `YYYY-MM-DD` |
| `market`, `currency` | `en-gb`, `EUR` |
| `limit` | prijíma sa, ale nemá vplyv (pozri nižšie) |

- Hlavičky: stačí `User-Agent`. Používame čestný identifikátor `tuke-di-krakow/0.1 (student data engineering project)` bez napodobňovania prehliadača; Ryanair odpovedá rovnako. Cookies ani token netreba.
- **Odpoveď vždy obsahuje jednu najlacnejšiu cestu v okne dátumov** (`size: 1`), aj pri okne 30+ dní a `limit=100`. Ak chceme let na každý dátum, treba **osobitný dopyt na každý deň** (`From = To`). Stránkovanie nie je (`nextPage: null`).
- Iba priame lety. Ak v okne nie sú lety alebo trasa neexistuje: `200` a `{"fares": [], "size": 0}` (chyba nie je, aj nesprávny kód letiska dáva prázdnu odpoveď).

Polia `fares[].outbound`:
`departureAirport`/`arrivalAirport` (`iataCode`, `name`, `city`), `departureDate`, `arrivalDate`, `price.value` (číslo), `price.currencyCode`, `flightNumber` (s kódom aerolínie, napr. `FR3035`), `flightKey`, `previousPrice` (zvyčajne `null`), `priceUpdated` (epoch ms, kedy sa cena aktualizovala).

**Ktoré polia chýbajú:**
- Počet voľných miest, tarifná trieda a počet prestupov (vždy 0).
- Časové pásmo: `departureDate` je **lokálny čas letiska bez posunu** (`2026-11-10T11:05:00`). Pásmo sa musí vziať z číselníka (bod 2).

Fixtures: `tests/fixtures/ryanair/bcn_krk_window.json`, `bcn_krk_two_days.json`, `empty.json`.

### 2. Trasy: `GET https://www.ryanair.com/api/views/locate/searchWidget/routes/en/airport/KRK`
Zoznam 88 letísk, s ktorými Ryanair spája KRK; pre každé je tu `arrivalAirport.code`, `name`, `country`, **`timeZone`** (IANA), `coordinates`. Trasy sú symetrické, preto zoznam používame ako „odkiaľ lietajú do KRK“. Fixture: `routes_from_krk.json` (13 letísk z nášho zoznamu).

Prienik s našimi letiskami: **ARN, BCN, BGY, CPH, CRL, DUB, EIN, LTN, MAD, MLA, STN, TRF, VIE** (13). Rím (FCO) a Oslo (OSL) tam nie sú: Ryanair má CIA a TRF.

## Čo nepoužívame
- `https://www.ryanair.com/api/booking/v4/en-gb/availability` (úplný rozpis dňa s počtom miest): odpoveď `409 {"message":"Availability declined"}`, teda ochrana pred automatickými dopytmi. Obchádzať ju nebudeme.

## Obmedzenia a pozorovania
- Za ~25 dopytov s pauzou 1,5 s nebola žiadna chyba, 429 ani blokácia. Oficiálne limity nie sú, preto pauza aspoň 1–1,5 s, opakovania s backoffom.
- Jeden let denne na trasu: pri dvoch letoch denne je vidieť iba najlacnejší (pri BCN sa čísla letov striedajú FR3035/FR3049). Je to obmedzenie zdroja.
- Ceny sú „živé“ (v `priceUpdated` je čerstvý čas), ale ide o minimálnu cenu, nie o celú sadu taríf.
- Endpoint je neoficiálny: štruktúra sa môže zmeniť; píšeme parser so striktnou kontrolou a surovú odpoveď ukladáme do `raw.fetch_log`.

## Stratégia zberača
1. Zoznam trás: z `routes/.../airport/KRK` v prieniku s `config/routes_ryanair.yaml` (≥10, teraz je dostupných 13).
2. Pre každé letisko a každý dátum horizontu (90 dní) jeden dopyt `From = To`, pauza 1,5 s. 13 letísk × 90 dní ≈ 1170 dopytov ≈ 30 minút na celý prechod. Raz za 12 hodín, alebo horizontom prechádzať postupne (bližšie dátumy častejšie, vzdialenejšie zriedkavejšie).
3. Parser: `departureDate` → tz-aware podľa `timeZone` letiska, `flightNumber`, `price.value` → `Decimal`, `stops=0`.
4. `offer_id` podľa spoločného vzorca (`source=ryanair`), upsert do `core.flight_offer`.
5. Testy na fixtures a `httpx.MockTransport`, ako pri Travelpayouts.

## Riziká
- Endpoint môžu zatvoriť alebo začať chrániť kontrolou „či ste človek“. Vtedy hľadáme iný zdroj (záložné: Aviationstack, Amadeus Self-Service).
- Veľa dopytov (≈1200 na prechod): držíme pauzy, pri 429 rešpektujeme `Retry-After`, pri blokácii zber ukončíme.
- Objem: Ryanair dá do ~13 × 90 ≈ 1000 ponúk za horizont (odhad pri jednom lete denne).
