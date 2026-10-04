# Plán implementácie

Pracujeme po fázach v poradí, jeden riešiteľ. Na konci každej fázy skontrolovať kritériá a označiť `[x]`.
Fázy 2–4 (zberače) robíme postupne.
Cieľ je iba fungujúce riešenie (prezentácia nie je).

Zafixované verzie: Python 3.12, PostgreSQL `postgres:16.15-alpine`, Kafka `apache/kafka:4.1.0`, `kafbat/kafka-ui:v1.4.2`.

---

## Fáza 0 — Kostra projektu
- [x] `uv init`, `pyproject.toml` (Python 3.12), balík `src/krakow_di`, nastavené ruff + pytest
- [x] `config.py` na pydantic-settings, `.env.example` aktuálny
- [x] `docker-compose.yml`: postgres 16.15, kafka 4.1.0 (KRaft, 1 broker, `auto.create.topics.enable=false`), kafka-ui, healthchecky, volumes
- [x] `README.md`: ako spustiť od nuly za 5 minút
- [x] Prázdny test prejde, `ruff check` je čistý

**Hotové, keď:** `docker compose up -d && uv run pytest` funguje na čistom stroji.

## Fáza 1 — Databáza
- [x] Alembic, schémy `raw`, `core`, `lake` a tabuľky z `ARCHITEKTURA.md`
- [x] Funkcia generovania deterministického `offer_id` (`source + origin + destination + departure_at s presnosťou na minútu + airline_iata`, bez `flight_number`/`fare_key`) + test
- [x] Repozitár/funkcie upsert pre `flight_offer` so zápisom do `flight_offer_history`

**Hotové, keď:** migrácie sa dajú nasadiť aj vrátiť; test upsertu prejde na reálnej DB z docker compose (bez testcontainers).

## Fáza 2 — Zberač Travelpayouts
- [x] Registrácia, token v `.env` (`TRAVELPAYOUTS_TOKEN`)
- [x] `config/routes.yaml`: ≥10 letísk odletu → KRK
- [x] Zapísať reálne odpovede do `tests/fixtures/travelpayouts/`
- [x] fetch → `raw.fetch_log` → parse → upsert, ošetrenie prázdnych odpovedí a limitov
- [x] Overiť, na ktorých trasách cache reálne dáva dáta; záver zapísať do `OTVORENE_OTAZKY.md`

**Hotové, keď:** jedno spustenie naplní `core.flight_offer` reálnymi ponukami na ≥5 trasách.

## Fáza 3 — Zberač Ryanair (namiesto Wizz Air)
- [x] **Spike (najprv!)**: Wizz Air je chránený kontrolou „či ste človek“ (`docs/zdroje/wizzair.md`), nahradený Ryanairom; nájdené otvorené Fare Finder API, fixtures v `tests/fixtures/ryanair/`, popis v `docs/zdroje/ryanair.md`
- [x] Zoznam trás Ryanairu do KRK z ich oficiálneho zoznamu (`config/routes_ryanair.yaml`: kandidáti ∩ trasy Ryanairu)
- [x] Zberač s pauzami, opakovaniami, backoffom, prechodom dátumov horizontu a zastavením pri sérii chýb
- [x] Parser pokrytý testami na fixtures

**Riziko:** Wizz Air je zamietnutý (kontrola „či ste človek“), nahradil ho Ryanair. Endpoint Ryanairu je neoficiálny a môže sa zatvoriť; potom záložné zdroje: Aviationstack, Amadeus Self-Service.

**Hotové, keď:** stabilné spustenie zbiera ponuky na ≥5 trasách Ryanairu do KRK bez blokácie. Overené: 6 letísk, 24 dopytov, 0 chýb.

## Fáza 4 — Zberač divadla
- [x] Vybrať divadlo (pozri `OTVORENE_OTAZKY.md`), overiť, že predajný systém ukazuje dostupnosť miest
- [x] **Spike**: ako získať program a dostupnosť miest podľa kategórií; popísať v `docs/zdroje/divadlo.md`
- [x] Zber programu → `core.theater_performance`
- [x] Snímky dostupnosti → `core.theater_snapshot`
- [x] Výpočet `theater.tickets.sold` z rozdielu snímok + testy

**Hotové, keď:** za deň prevádzky vidno snímky a aspoň jeden zistený reálny predaj.

**Stav:** kód, testy a reálne spustenie sú hotové. Počas niekoľkých hodín opakovaných snímok boli zistené 3 predaje (Wielki Gatsby 3 + 2 miesta, O!peretka 2 miesta); posledný z nich sa o 3 minúty vrátil (držanie v košíku). **Kritérium „za deň“ je splnené na úrovni „aspoň jeden reálny predaj“, plný 24-hodinový nepretržitý beh sa neoveroval** (plánovač príde vo fáze 9).

## Fáza 5 — Kafka publisher a kontrakt
- [x] JSON Schema v `schemas/events/` pre všetky typy z `UDALOSTI.md`
- [x] `tools/create_topics.py`: topiky z `ARCHITEKTURA.md` s partíciami a `retention.ms=-1` (idempotentne)
- [x] Envelope, validácia, producer na aiokafka (`acks=all`, idempotencia, kľúč = id entity, hlavička `event_type`), zápis do `core.event_log`
- [x] Zberače publikujú `offer.found` / `offer.observed` / `price_changed` / divadelné udalosti
- [x] `docs/asyncapi.yaml` (protokol kafka): topiky, kľúče, hlavičky, správy
- [x] Testovací consumer `tools/consume.py` (výber topikov, `--from-beginning`)

**Hotové, keď:** `tools/consume.py --from-beginning` vidí všetky typy udalostí s platnými schémami a udalosti jedného `offer_id` idú v poradí.

**Stav:** hotové a overené na reálnej Kafke (370 správ, 0 neplatných, 0 porušení poradia; 5 z 10 typov udalostí: `found`, `observed`, `price_changed`, `availability.snapshot`, `tickets.sold`). Typy `flight.ticket.sold`, `sold_out`, `expired` vzniknú až s generátorom (fáza 6); `theater.performance.*` sa publikujú pri behu s `--publish`, keď sa program zmení.

## Fáza 6 — Generátor predaja
- [ ] Model z `ARCHITEKTURA.md` (kapacita, pravdepodobnosť predaja, 1–3 miesta, dynamická cena)
- [ ] Parametre v konfigurácii; `seed` a injekcia času; režim zrýchleného času pre demo
- [ ] Udalosti `ticket.sold`, `price_changed`, `sold_out`, `expired`
- [ ] Testy: determinizmus pri rovnakom seede, miesta nejdú do mínusu, predaj na uplynulý let je nemožný
- [ ] Porovnať odhad objemu (`OTVORENE_OTAZKY.md`, urobený po spiku oboch zdrojov) so skutočnými parametrami: ~10 000 leteniek = predané miesta + ponuky; udalosti počítame osobitne

**Hotové, keď:** za hodinu zrýchleného režimu vznikne vierohodný tok predajov.

## Fáza 7 — API a brána streamu
- [ ] FastAPI: REST pre ponuky, históriu, predaje, divadlo, žurnál udalostí (stránkovanie, filtre)
- [ ] `GET /stream` (SSE) s filtrom podľa typov a `Last-Event-ID`
- [ ] `WS /ws`
- [ ] Stránka `docs/PRE_TIMY.md`: ako sa k nám pripojiť (Kafka, SSE, WS, REST) s príkladmi v Pythone (aiokafka/confluent-kafka), kcat a curl

**Hotové, keď:** iný človek sa podľa samotného `PRE_TIMY.md` pripojí a dostane udalosti.

## Fáza 8 — Data lake
- [ ] Writer `lake.message`, spoločné rozhranie adaptéra, runner s reštartom spadnutých adaptérov
- [ ] `config/lake_sources.yaml`
- [ ] Adaptér pre vlastné udalosti (ako vzor)
- [ ] Adaptéry pre každý tím podľa toho, ako zverejnia svoje rozhrania (`lake/adapters/team_XX.py` + poznámka `docs/timy/team_XX.md`, čo a ako poskytujú)
- [ ] Jednoduchý report: koľko správ od ktorého tímu za deň (`tools/lake_stats.py`)

**Hotové, keď:** do lake nepretržite prichádzajú dáta aspoň od 2 cudzích tímov.

## Fáza 9 — Nasadenie
- [ ] Server (Hron/ÚVT alebo VPS), docker compose v produkcii, reverse proxy + HTTPS pre API
- [ ] Kafka EXTERNAL listener: SASL_SSL + SCRAM-SHA-512, správne `advertised.listeners`, certifikát
- [ ] Používateľ `teams` + ACL: READ na topiky `krakow.*` a skupiny `team-*`; overiť, že zápis je zakázaný
- [ ] Kontrola pripojenia **zvonku** servera (z notebooku): consumer číta od offsetu 0
- [ ] Plánovač beží 24/7, logy, jednoduchý healthcheck `/health`
- [ ] Záloha DB raz denne

**Hotové, keď:** systém beží deň bez zásahu.

---

## Neskôr (po 6. týždni)
- Data warehouse: normalizácia dát všetkých tímov, spoločný model (hviezda/snehová vločka), ETL z `lake.*`.
