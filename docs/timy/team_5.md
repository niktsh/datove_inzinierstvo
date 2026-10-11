# Tím 5

Zdroj: ich dokument „Dokumentácia pre integráciu dát“. Letenky čítame z ich S3 data lake,
divadlo nám posielajú sami cez webhook (push). Adaptér letenky:
[`team_5.py`](../../src/krakow_di/lake/adapters/team_5.py), príjem divadla:
[`webhooks.py`](../../src/krakow_di/api/webhooks.py), zdroj v
[`config/lake_sources.yaml`](../../config/lake_sources.yaml) (tím `team_5`, kanály `s3` a `webhook`).

## Ako svoje dáta poskytujú

### Letenky: S3 (Cloudflare R2), pull
- Bucket `data-lake` na R2 (S3 API, región `auto`), prefix `bronze/domain=flights/`. Štruktúra
  súborov (Medallion): `source={ryanair|esky|ignav|aviationstack}/year=YYYY/month=MM/day=DD/hour=HH/
  batch_{UUID}.json`. Prístupové kľúče (čítanie) dali všetkým tímom, **nie sú v repozitári**:
  patria do `.env` ako `TEAM5_R2_ACCESS_KEY_ID` a `TEAM5_R2_SECRET_ACCESS_KEY`.
- Každý súbor je pole JSON; položka má `metadata` (`scraper_id`, `target_domain`, `scraped_at_utc`,
  `batch_id`, `source_url`, `item_count`) a `payload` (let: číslo letu, aerolinka, odkiaľ/kam,
  dátum a čas, cena, mena, priamy let, počet predaných lístkov). Polia `payload` sa medzi zdrojmi líšia.
- V čase pridania: ~6 000 objektov (177 MB), rastie približne o 1 600 malých objektov a 45 MB denne
  (väčšina zdroj `esky`, dávky zhruba štyrikrát denne). V bucketi je aj prefix `bronze/domain=theatre/`;
  ten nečítame (divadlo majú odovzdávať webhookom).
- `tickets_sold` v ich dátach je ich simulácia, nie skutočný predaj.

### Divadlo: webhook (push)
- Tím 5 neposkytuje GET: dvakrát denne ich CI pipeline vygeneruje predaje vstupeniek a pošle ich
  POST-om na adresu, ktorú im dáme. Telo je JSON:
  `{event_id, update_timestamp, title, tickets_sold, seats_remaining, status}`.
- Naša adresa: **`http://<IP servera>/webhooks/team_5/<TOKEN>`**. Token je tajná časť adresy
  (`TEAM5_WEBHOOK_TOKEN` v `.env`); bez správneho tokenu endpoint odpovie 404, ak je token
  prázdny, webhook je vypnutý. Token sa generuje napr.
  `python3 -c "import secrets; print(secrets.token_urlsafe(24))"`.

## Ako to ukladáme (bez normalizácie)

- **S3:** každý objekt = jedna správa (`payload_raw` = bajty súboru, `payload_json` = pole, keďže ide o
  JSON), `channel = s3`, `source_ref = {bucket, key, etag, last_modified}`. Ten istý objekt sa uloží raz,
  zmenený (iný `etag`) ako nová správa. Prechod bucketu raz za 30 minút; prvý beh stiahne všetko
  (~5 minút, 8 súbežných sťahovaní), ďalšie behy iba nové objekty.
- **Webhook:** telo POST-u sa uloží tak, ako prišlo (aj keď nie je JSON), `team = team_5`,
  `channel = webhook`, `source_ref = {webhook, sha256}`: rovnaké telo sa uloží raz. Limit tela 256 KB.
  Odpoveď `200 {"ok": true, "stored": true|false}`.
- Chýbajúce kľúče R2 v `.env` nezhodia lake: kanál `team_5/s3` sa opakovane reštartuje s chybovou
  hláškou, ostatné zdroje bežia.

## Obmedzenia
- Webhook ide cez HTTP bez šifrovania (nemáme doménu), token v adrese je preto ochrana pred náhodným
  zápisom, nie silné zabezpečenie.
- Divadlo dostávame len dvakrát denne a len ak im náš endpoint pošleme; kým nám pošlú správu, nemáme
  čo uložiť. Po reštarte `lake` sa S3 prechádza znova od začiatku (duplicity zahodí unikátny index).

Vzorky objektov z ich bucketu pre testy: `tests/fixtures/team_5/` (zaznamenané 2026-10-11).
