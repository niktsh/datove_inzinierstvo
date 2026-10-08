# Tím 7

Zdroj: ich popis rozhrania „Pripojenie k dátam: letenky (SSE) a divadlo (REST)“. Adaptér:
[`team_7.py`](../../src/krakow_di/lake/adapters/team_7.py), zdroje v
[`config/lake_sources.yaml`](../../config/lake_sources.yaml) (tím `team_7`, kanály `sse` a `rest`).

## Ako svoje dáta poskytujú

Server: `https://university-data-api.artur-yevdokymov.workers.dev` (Cloudflare Worker nad
Supabase). Verejné GET požiadavky, **bez prihlasovania**, preto v `.env` nie sú žiadne údaje.

| Účel | Endpoint | Výstup |
|---|---|---|
| Letenky | `GET /api/stream?limit=N` (N 1–100) | SSE, 6 tabuliek z DB1, DB3, DB4 |
| Divadlo | `GET /api/theatre?limit=N&offset=M` (N 1–1000) | JSON `{success, database, table, count, data[]}`, tabuľka `db2.ticket_snapshots` |
| Všetko | `GET /api/all` | JSON, všetkých 7 tabuliek (nepoužívame) |

### SSE (letenky)
- Udalosti `ready` (zoznam zdrojov a interval kontroly 10 s), `snapshot` (`{database, table, rows[]}`
  pre každú tabuľku po pripojení), `change` (zmena zistená približne každých 10 s), `warning`
  (napr. „No time/id column detected; recent records may be missed“) a `heartbeat`
  (`{"time": ...}`) a `reconnect` (oznam, že server spojenie zatvára).
- Správy **nemajú `id`** a nepodporujú `Last-Event-ID`; úplná história zmien neexistuje.
  Spojenie sa po ~40–50 s zatvorí a klient sa musí pripojiť znova, pričom dostane znova snímky.
- Tabuľky: `db1/flight_observations`, `db1/flight_searches` (Pelikan), `db3/flightwatch_observations`,
  `db3/flightwatch_searches` (Ryanair a ďalší), `db4/de_kiwi_observations`, `db4/de_kiwi_searches` (Kiwi).
  Stĺpce sa medzi tabuľkami líšia. Riadky majú doplnené `source_database` a `source_table`.

### REST (divadlo)
- Pri každom GET sa čítajú aktuálne údaje zo Supabase. Záznam: `captured_at`, `event_title`,
  `venue`, `starts_at`, `currency`, `available_total`, `zones[]` (zóny s tarifami v centoch) a `is_sold`.
- `count` je počet riadkov v odpovedi (nie celkový počet). V čase pridania bolo v tabuľke 85 záznamov.

### Simulovaný `is_sold`
V ich DB má `is_sold` predvolenú hodnotu `random() < 0.9` (90 % `true`), uloženú pri zázname
a pri čítaní sa nemení. Pri divadle ide o stav celého snímku, nie sedadiel. **Nie sú to
reálne predaje**: pri analýze v data warehouse s tým treba rátať.

## Ako to ukladáme (bez normalizácie)

Každá správa sa uloží ako surové bajty (`payload_raw`), `payload_json` sa vyplní, keďže ide o JSON.

- **SSE:** jeden SSE rámec = jedna správa (`payload_raw` je text za `data:`). `source_ref` =
  `{url, event, sha256}`. Keďže po každom pripojení prídu tie isté snímky, `sha256` obsahu
  odstráni duplicity: nezmenený snímok sa uloží raz, zmenený (alebo `change`) ako nová správa.
  Pri ich zatvorení spojenia sa adaptér po 2 s pripojí znova (bežné správanie, nie chyba);
  skutočná chyba (HTTP != 2xx, sieť) ide cez reštart runnera s backoffom.
  **Vynechávame iba `heartbeat`**: iba udržiava spojenie (čas každých ~10 s) a neobsahuje dáta.
- **REST:** raz za hodinu (`interval_seconds: 3600`) sa stiahnu stránky `limit=1000&offset=…`,
  kým nepríde neplná stránka. Každá stránka sa uloží celá; `source_ref` = `{url, offset, sha256}`.
  Nezmenená odpoveď sa neuloží znova, zmena obsahu vytvorí novú správu (história snímkov).

## Obmedzenia
- Pri dlhom výpadku zmeny medzi pripojeniami neuvidíme (nemajú históriu, snímky obmedzuje `limit=100`
  na tabuľku): ukladáme len to, čo server po pripojení pošle.
- Ich `warning` o chýbajúcom časovom stĺpci znamená, že `change` môže pre niektoré tabuľky chýbať;
  pravidelné snímky po pripojení to čiastočne dopĺňajú.
- Ak sa objem ukáže veľký, zvýšime `interval_seconds` alebo znížime `limit` v URL.

Vzorky ich odpovedí na testy: `tests/fixtures/team_7/` (zaznamenané 2026-10-08 s `limit=1`
resp. `limit=2`).
