# Otvorené otázky a rozhodnutia

Formát: otázka → stav → rozhodnutie/kto rozhoduje. Uzavreté otázky nemazať, ale označiť ✅.

## Pre vyučujúceho (Ján Genči)
- [ ] Je povinné ukladať dáta iných tímov vo vlastnej DB, alebo stačí data lake? (v každom prípade ukladáme: `lake.*`)
- [ ] Potrebujeme pre divadlo osobitný stream/udalosť, alebo je stream povinný iba pre letenky?
- [ ] Existujú požiadavky na minimálnu dobu prevádzky systému (24/7 počas celého semestra?)
- [ ] Hosting na Hrone cez ÚVT: na koho sa obrátiť, aké sú obmedzenia na otvorené porty?
- [ ] Nie je vybrané divadlo (Teatr im. J. Słowackiego) už obsadené iným tímom? Bez koordinácie medzi tímami to vieme zistiť iba od vyučujúceho.

## Naše rozhodnutia
- [x] ✅ **Ktoré divadlo?** Vybrané Teatr im. J. Słowackiego (spike 2026-10-04). Kandidáti boli: Narodowy Stary Teatr, Teatr im. J. Słowackiego, Teatr Bagatela, Teatr Ludowy, Opera Krakowska.
      Kritérium: online predaj ukazuje dostupnosť miest podľa kategórií/schému sály.
- [ ] Zoznam letísk odletu (≥10) → `config/routes.yaml`
- [ ] Horizont dátumov: 90 dní?
- [x] ✅ Mena: letenky pýtame v EUR; divadlo (PLN) sa ukladá v origináli + `price_eur` podľa kurzu ECB v deň snímky (rozhodnutie používateľa: ceny majú byť v eurách)
- [ ] Parametre generátora: dĺžka tiku, základná pravdepodobnosť predaja
- [ ] **Odhad objemu 10 000** (letenky = predané miesta + ponuky; udalosti osobitne): urobiť **po spiku oboch zdrojov, pred fázou 6**.
- [ ] Kde hostujeme
- [x] ✅ Mesto: Kraków (KRK)
- [x] ✅ Zdroje leteniek: Travelpayouts + Ryanair (Wizz Air zamietnutý: interaktívna kontrola „či ste človek“)
- [x] ✅ Sledujeme celé divadlo, nie jednu inscenáciu
- [x] ✅ Dáta iných tímov ukladáme (data lake)
- [x] ✅ Ticketmaster pre divadlo nevyhovuje (chýbajú ceny a dostupnosť): iba prieskumný skript `tools/check_apis.py`; divadlo berieme z webu samotného divadla
- [x] ✅ `offer_id` bez `flight_number`/`fare_key` (pozri `ARCHITEKTURA.md`)
- [x] ✅ Outbox a Schema Registry vyškrtnuté
- [x] ✅ Záložný druhý zdroj leteniek, ak Wizz Air neprelomíme: Ryanair (zafungoval). Ak Ryanair zatvoria: Aviationstack alebo Amadeus Self-Service
- [x] ✅ Verzie: Python 3.12, `postgres:16.15-alpine`, `apache/kafka:4.1.0`; testy DB cez docker compose
- [x] ✅ Prezentácia netreba, pracuje jeden človek, fázy 2–4 postupne
- [x] ✅ Protokol streamu: Apache Kafka (KRaft) + SSE/WebSocket/REST ako doplnkové kanály
- [x] ✅ Všetky súbory projektu (dokumentácia, komentáre, správy logov, konfigurácia) sú v slovenčine; identifikátory, názvy testov a hodnoty udalostí zostávajú anglicky; názvy commitov sú v slovenčine
- [ ] Hosting musí umožniť otvoriť port pre Kafku navonok (SASL_SSL). Overiť na Hrone/ÚVT.

## Poznámky k zdrojom (dopĺňať priebežne)
- Travelpayouts (overené 2026-10-03, `market=sk`, `currency=eur`, v3 `prices_for_dates`, `one_way=true`):
  - Cache dáva dáta pre všetkých 18 overených letísk, ale v novembri je odpoveď prázdna pre LTN/STN a v decembri pre FCO/MLA: pokrytie je nerovnomerné, čím ďalší dátum, tým menej ponúk.
  - Viac ako polovica odpovedí sú lety s prestupmi (spolu 535, priamych 318). Pre ATH, BER, BRU (mesto) priame lety nie sú vôbec. Preto je v `routes.yaml` `max_stops: 0`; surová odpoveď sa v `raw.fetch_log` ukladá celá.
  - Dopyt podľa kódu mesta vracia iné letiská: BRU → CRL, OSL → TRF, LON, STO, MIL, ROM → LTN/STN, ARN, BGY, FCO. Berieme `origin_airport` z odpovede.
  - Jedno `offer_id` môže dať viac variantov (23 z 535): ponechávame najlacnejší.
  - `flight_number` z API je iba číslo bez kódu aerolínie; ukladáme `airline + number` (napr. `FR3035`).
  - Reálny plný beh: 56 dopytov, 0 chýb, 307 priamych ponúk z 15 letísk, približne 40 s na beh. Na limity sme nenarazili.
  - Záver pre odhad objemu: jeden zber dá ~300 priamych ponúk (horizont 90 dní), preto sa 10 000 „leteniek“ naberie hlavne predanými miestami generátora, nie ponukami.
- Wizz Air: **zamietnutý** (2026-10-03). Web odpovedá 405 a interaktívnou kontrolou „Human Verification“; obchádzať ju znamená obchádzať ochranu, to nerobíme. Pozri `docs/zdroje/wizzair.md`.
- Ryanair (overené 2026-10-03, reálny beh: 24 dopytov, 0 chýb, 20 ponúk zo 6 letísk):
  - Fare Finder API je otvorené, token netreba. Na okno dátumov vracia jednu najlacnejšiu cestu, preto robíme dopyt na každý deň (≈13 letísk × 90 dní ≈ 1170 dopytov, ≈ 30 min pri pauze 1,5 s).
  - Endpoint `availability` (rozpis dňa, miesta) je chránený (409), nepoužívame ho. Počet miest Ryanair v dostupných dátach nemá, miesta simuluje generátor.
  - Dostupné letiská: ARN BCN BGY CPH CRL DUB EIN LTN MAD MLA STN TRF VIE (13). FCO, OSL, BRU nie sú u Ryanairu trasy do KRK.
  - Prekrytie s Travelpayouts: obidva ukazujú lety FR. Rôzne zdroje dávajú rôzne `offer_id`, takže obsahové duplicity sú prípustné.
  - Odhad objemu: do ~1000 ponúk Ryanairu za 90 dní plus ~300 Travelpayouts.
- Divadlo: Teatr im. J. Słowackiego (overené 2026-10-04, reálny beh: 2 mesiace programu, 70 predstavení, 8 snímok, 0 chýb). Podrobnosti: `docs/zdroje/divadlo.md`.
  - Vypredané predstavenia sú v programe zobrazené bez odkazu na pokladňu („Bilety do teatru wyprzedane“); ukladáme ich ako `sold_out` (zo 70 nájdených je 24 vypredaných).
  - Preskakujeme: cudzie podujatia (vstupenky na inom webe, napr. goingapp.pl) a uzavreté predstavenia („Spektakl zarezerwowany“): dostupnosť miest u nich nie je vidno. Je to vedomé rozhodnutie.
  - Košík kupujúceho: miesto v košíku vyzerá ako predané, potom ako vrátené (`quantity < 0`). Nefiltrujeme to, lebo pri intervale 2–6 hodín sa to dostane do snímky zriedka; vrátenia sú v udalosti výslovne povolené.
  - Do `raw.fetch_log` sa mapa sály neukladá celá (≈100 KB na udalosť, ≈100 MB denne), iba súhrn: id, počet miest, legenda.
  - Plný prechod: ≈ 70 predstavení na 2 mesiace, ≈ 250 na 6 mesiacov; jeden prechod snímok ≈ 6–8 minút pri pauze 1,5 s.
