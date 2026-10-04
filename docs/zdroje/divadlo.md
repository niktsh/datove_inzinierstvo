# Zdroj: divadlo (spike, 2026-10-04)

**Odporúčanie: Teatr im. Juliusza Słowackiego w Krakowie** (`teatrwkrakowie.pl`, pokladňa `bilety.teatrwkrakowie.pl`).
Dáta o miestach sú otvorené, bez autorizácie a bez kontroly „či ste človek“, na úrovni každého sedadla. Reálne ceny a reálna dostupnosť.

Porovnanie kandidátov (podľa hlavičiek a stránok, bez hlbokej analýzy):
- **Słowacki**: pokladňa „System Biletowy“, JSON s miestami pre každú udalosť. Hodí sa.
- **Teatr Ludowy**: pokladňa iKsoris (`bilety.ludowy.pl`), miesta sa načítavajú skriptom cez `core.js`; endpoint nebol preskúmaný. Záložná možnosť.
- **Bagatela, Stary Teatr, Opera Krakowska**: neskúmané (web Starého divadla sa cez HTTPS neotvoril, Bagatela a Opera majú odkazy na všeobecné stránky pokladní).

## Endpointy

### 1. Program: `POST https://teatrwkrakowie.pl/ajax/pl/repertoireList`
Telo (form-urlencoded):
```
filters[0][type]=type        filters[0][value]=current
filters[1][type]=EventType   filters[1][value]=all
filters[2][type]=Event       filters[2][value]=all
filters[3][type]=search      (prázdne)
startDate=2026-11-01
```
Hlavičky: `User-Agent` (čestný identifikátor projektu), `X-Requested-With: XMLHttpRequest`, `Referer: https://teatrwkrakowie.pl/repertuar`.
Odpoveď: JSON objekt `{"template": "<html>", "templateCalendar": "<html>"}` (skript webu robí `JSON.parse` nad textom, u nás `json.loads` zafungoval hneď). V `template` sú HTML bloky predstavení: názov (`<h2><a href="/spektakl/...">`), scéna (`/sceny/...`), dátum v bloku „day“, odkaz pokladne `https://bilety.teatrwkrakowie.pl/kup-bilet/<slug>-<YYYY-MM-DD>-<HH>-<MM>[-N]`.
Za november 2026: 38 udalostí. Mesiace na prechod: zoznam `data-date` v `#mobile-month-picker-select` na `/repertuar` (6 mesiacov dopredu).
Bloky bez odkazu na pokladňu sú troch druhov (overené na reálnych dátach, október a november 2026, 70 predstavení): (1) tlačidlo „Bilety do teatru wyprzedane“ (`btn-disabled`): **vypredané predstavenie**, ukladáme ako `sold_out`, odkaz chýba; (2) cudzie podujatia („Wydarzenie zewnętrzne“, odkaz na iný web, napr. goingapp.pl): preskakujeme; (3) „Spektakl zarezerwowany“ (uzavreté predstavenie): preskakujeme. Všetky bloky majú stabilné `data-instance-tickets` (id predstavenia na webe); podľa neho sa vypredané predstavenie priradí k tomu istému `performance_id`, keď sa vstupenky vrátia.

### 2. Identifikátor udalosti: `GET https://bilety.teatrwkrakowie.pl/kup-bilet/<slug>`
HTML stránky obsahuje `var currentRepertoireId = 1926;` (identifikátor repertoárového záznamu) a blok `Lokalizacja spektaklu: ...`. Slug je jedinečný a stabilný, `performance_id` stavíme podľa slugu.
Stránka má približne 170 KB, jeden dopyt na udalosť pri jej prvom objavení (identifikátor si zapamätáme; ukladá sa do stĺpca `repertoire_id`).
Niektoré slugy vracajú 302 (udalosť stiahnutá).

### 3. Miesta: `GET https://bilety.teatrwkrakowie.pl/sbLocationService/forSale.json?id=<repertoireId>`
Hlavičky: `X-Requested-With: XMLHttpRequest`, `Referer` stránky udalosti, `User-Agent`. Cookies netreba.
Odpoveď `{"location": {...}}`:
- `location.shapes[]`: všetky útvary sály. Miesto: `isPlace: 1`, `row`, `place`, `inCart`. **Kľúč `sale` (`{"avaiablePlaces":1,"price":"100,00","product":7}`) = miesto je dostupné na predaj.** Bez `sale` = obsadené, predané, zablokované alebo dočasne v cudzom košíku.
- `location.legend[]`: kategórie `{name, price ("100,00"), places (počet voľných miest v kategórii), color, saleMultiple, discounts[]}`. Súčet `places` v legende = počet miest so `sale`.
- Príklad malej scény (MOS, id 1926): 203 miest, 81 na predaj. Hlavná scéna (Duża Scena, id 1702): 557 miest, 13 na predaj, 6 kategórií (dve kategórie „Normalny“ s rôznou cenou, preto kategória = dvojica `(name, price)`).
- Parameter `sector_id` treba iba pre „textovú verziu“ sály; na počítanie miest netreba.
Fixtures: `tests/fixtures/slowacki/` (geometria miest je vyrezaná).

Ceny v zlotých (`PLN`), desatinná čiarka.

## Čo znamená „reálny predaj“
`theater.availability.snapshot` podľa kategórií = `legend[].places`, spolu = počet miest so `sale`.
Zníženie počtu voľných miest medzi snímkami = predaj (`theater.tickets.sold`, `quantity = rozdiel`), zvýšenie = vrátenie (záporné množstvo).
Obmedzenie: miesto v košíku kupujúceho na istý čas zrejme tiež vyzerá ako „bez `sale`“ (v kóde stránky sa dôvod nedostupnosti miesta volá „w koszyku“; dĺžka držania nebola overená), preto rýchle výkyvy dajú falošný predaj a potom vrátenie. Rozhodnutie (implementované): rozdiel počítame tak, ako je, vrátenie dáva záporné množstvo (v `UDALOSTI.md` je to výslovne povolené); pri intervale 2–6 hodín sa držanie v košíku dostane do snímky zriedka. Prvá snímka predstavenia je iba základná. Ak v praxi uvidíme veľa falošných dvojíc „predaj/vrátenie“, pridáme potvrdenie dvoma snímkami za sebou.

## Obmedzenia a riziká
- Neoficiálne rozhranie (to isté, ktoré používa web): môže sa zmeniť. Parsujeme striktne, surové dáta ukladáme do `raw.fetch_log`.
- `robots.txt` pokladne nič nezakazuje (`Disallow:` je prázdny, riadky sú zakomentované); hlavný web na `/robots.txt` vracia HTML stránku. Záťaž držíme malú.
- Objem: ~40 udalostí mesačne, na 6 mesiacov dopredu ≈ 250 udalostí. Dopytovanie dostupnosti každé 2–6 hodín s pauzou 1,5 s ≈ 6–8 minút na prechod. Program: raz denne (6 dopytov zoznamu + stránky nových udalostí).
- Udalosti, ktoré sa vypredajú za minúty: podľa zadania ich neberieme. Pri Słowackom predaj trvá mesiace; na hlavnej scéne na najbližšie dátumy je už takmer všetko obsadené (13 z 557), čo je v poriadku, ale „snímka s takmer všetkými miestami vypredanými“ bude pri blízkych dátumoch častá.
- Divadlo musí byť v skupine jedinečné: bez koordinácie to overiť nemožno, otázka pre vyučujúceho (`OTVORENE_OTAZKY.md`).

## Stratégia zberača
1. Raz denne: `repertoireList` pre každý mesiac → `core.theater_performance` (udalosť `theater.performance.found/updated`).
2. Pre nové udalosti: stránka pokladne → `repertoireId`, scéna. `repertoireId` sa ukladá do stĺpca `core.theater_performance.repertoire_id`.
3. Každé 2–6 hodín: `forSale.json` pre všetky budúce udalosti → `core.theater_snapshot` (kategória, cena, `seats_available`) → `theater.availability.snapshot`.
4. Rozdiel snímok → `theater.tickets.sold` (podľa kategórií).
5. Pauza 1,5 s, opakovania, zastavenie pri sérii chýb (ako pri Ryanairu).
