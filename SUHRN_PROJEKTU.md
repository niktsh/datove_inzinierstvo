# Súhrn projektu: ako všetko spolu funguje

## 1. Čo projekt robí

Projekt je školský dátový systém (predmet Dátové inžinierstvo, TUKE) pre mesto **Kraków**. Zbiera **reálne** dáta z dvoch domén, letenky a divadlo, a **streamuje** ich ďalej. Súčasne ukladá do **data lake** dáta ostatných tímov.

- **Letenky:** lety jedným smerom z rôznych letísk do Krakowa (KRK). Zdroje: Travelpayouts a Ryanair.
- **Divadlo:** Teatr im. Juliusza Słowackiego. Program, ceny a dostupnosť vstupeniek, ceny v PLN aj prepočítané na eurá (kurz ECB).
- **Generátory (2):** simulujú predaj leteniek a predaj vstupeniek do divadla a oznamujú ho na vlastnom rozhraní (povinná časť zadania). Simulované divadelné predaje sú v `core.theater_sale` so `source='generator'` a v udalosti `theater.tickets.sold` s `simulated=true`.
- **Streaming:** povinný na 100 %. Databáza a REST sú len doplnok.

## 2. Hlavný tok dát

```
Externé zdroje ─► Zberače ─► PostgreSQL (raw, core) ─► Publisher ─► Kafka ─► Ostatné tímy
(Travelpayouts,   (collectors)        ▲                              │
 Ryanair, divadlo)                    │                              ├─► API (REST / SSE / WebSocket)
                                 Generátor                           └─► Data lake (lake.*)
```

1. **Zberače** (`collectors/`) volajú zdroje. Surovú odpoveď uložia do `raw.fetch_log`, spracované dáta do `core.*` a vrátia zoznam zmien.
2. **Generátor** (`generator/`) číta `core.flight_offer`, každých 5 minút „predá“ miesta a mení ceny podľa obsadenosti. Zapisuje späť do `core`, surové dáta sa nedotýka. Druhý generátor (`generator/theater.py`) simuluje predaje vstupeniek nad reálnymi snímkami dostupnosti divadla.
3. **Publisher** zmeny overí podľa JSON Schema (`schemas/events/`), zapíše do `core.event_log` (`published_at = NULL`) a odošle do Kafky. Po potvrdení brokerom nastaví `published_at`. Ak Kafka nie je dostupná, udalosti zostanú v `event_log` a odošlú sa pri ďalšom behu.
4. **Kafka** rozdeľuje udalosti do topikov podľa entít (nižšie).
5. **API** číta Kafku a pre ostatné tímy ponúka REST, SSE aj WebSocket.
6. **Data lake** číta správy ostatných tímov (a naše vlastné) a ukladá ich do `lake.message` bez zmeny.

## 3. Komponenty

| Komponent | Úloha | Frekvencia |
|---|---|---|
| `collector: travelpayouts` | Cache ceny z Aviasales (REST, token v `.env`) | každých 6 h |
| `collector: ryanair` | Verejné Fare Finder API, bez tokenu | každých 12 h |
| `collector: theater` | Program a dostupnosť miest divadla; predaj sa odvádza zo zmeny počtu voľných miest | program 24 h, snímky 3 h |
| `generator` | Simulácia predaja a dynamickej ceny | každých 5 min |
| `publisher` | Validácia, zápis do `event_log`, odoslanie do Kafky | priebežne |
| `api` | REST, SSE (`/stream`), WebSocket (`/ws`), `/health`, `/api/v1/stats` | stále |
| `lake` | Adaptéry pre cudzie tímy, zápis do `lake.message` | stále |
| `backup` | Denná záloha DB (`pg_dump`, 14 dní) | denne |

Zberače, generátor a publisher bežia v jednom procese (`app`, plánovač). `api` a `lake` sú osobitné kontajnery z toho istého obrazu.

## 4. Kafka

- Apache Kafka 4.1.0, režim KRaft, jeden broker.
- **Topiky:**
  - `krakow.flights.offers`: ponuky (`found`, `observed`, `price_changed`, `sold_out`, `expired`)
  - `krakow.flights.sales`: predané letenky (`flight.ticket.sold`)
  - `krakow.theater.performances`, `krakow.theater.availability`, `krakow.theater.sales`
- **Kľúč správy** je id entity, takže všetky udalosti jednej letenky sú v jednej partícii a čítajú sa v správnom poradí.
- **Retencia** neobmedzená: tím, ktorý sa pripojí neskôr, prečíta históriu od offsetu 0.
- **Dva listenery:** `INTERNAL` (v docker sieti, bez autentifikácie, používajú ho naše služby) a `EXTERNAL` (port `9094`, SASL_SSL + SCRAM-SHA-512) pre ostatné tímy.
- Používateľ `teams` má **iba čítanie** topikov `krakow.*`. Zapisovať k nám nemôže nikto.

## 5. Databáza (PostgreSQL)

- **`raw`**: surové odpovede zo zdrojov (`raw.fetch_log`).
- **`core`**: spracované dáta: `flight_offer`, `flight_offer_history`, `ticket_sale`, `theater_performance`, `theater_snapshot`, `theater_sale`, `fx_rate` a `event_log` (všetky publikované udalosti, vrátane poradia `seq`).
- **`lake`**: správy ostatných tímov (`lake.message`: pôvod, čas prijatia, surové dáta a JSON, ak je správa platný JSON).

## 6. Ako k dátam pristupujú ostatní

| Spôsob | Pre koho | Adresa |
|---|---|---|
| Kafka (SASL_SSL) | Tímy s Kafka klientom | `34.118.112.234:9094`, používateľ `teams`, certifikát `ca.crt` |
| SSE `/stream` | Kto chce `curl` alebo prehliadač | `http://34.118.112.234/stream` (filter `?types=...`, podpora `Last-Event-ID`) |
| WebSocket `/ws` | Interaktívne aplikácie | `ws://34.118.112.234/ws` |
| REST `/api/v1/...` | Prezeranie dát, história | `http://34.118.112.234/docs` |

## 7. Nasadenie

Systém beží na Google Cloud VM `krakow-di` (e2-standard-2, Ubuntu, európsky región) so statickou IP. Spúšťa sa cez `docker-compose.prod.yml`:

`postgres`, `kafka`, `kafka-init` (jednorazovo: používateľ a ACL), `migrate` (jednorazovo: migrácie a topiky), `app`, `api`, `lake`, `caddy` (reverse proxy), `backup`.

- **Otvorené porty:** `80`, `443` (API cez Caddy) a `9094` (Kafka). `5432` ani `8080` (kafka-ui) sa von neotvárajú.
- Overené z vonkajšej siete: `tools/check_external.py` vrátil 4 z 4 kontrol (čítanie od offsetu 0, zápis zakázaný, cudzia consumer group zakázaná, zlé heslo odmietnuté).
- Stav v čase písania (`/api/v1/stats`): 550 ponúk, 761 udalostí a 761 správ v lake. Rovnaký počet ukazuje, že reťazec zberač → Kafka → lake je uzavretý.

## 8. Dôležité návrhové rozhodnutia

- **Kafka namiesto jednoduchej fronty:** je to log (správy zostávajú), umožňuje replay, zachováva poradie podľa kľúča a každý tím má vlastnú consumer group. Nevýhoda je zložitejšie externé nastavenie, preto existuje aj SSE/WebSocket/REST brána.
- **Bez outboxu a Schema Registry:** pre rozsah projektu je to zbytočné. Poistkou je `event_log`.
- **Vlastné ceny generátora oddelené od zdrojových:** `price` je cena zo zdroja, prirážka je v `price_markup`. Zber teda neprepíše generátor a naopak.
- **Lake ukladá dáta „tak, ako sú“:** normalizáciu robí až data warehouse (po 6. týždni).
