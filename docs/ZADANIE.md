# Zadanie (zafixované požiadavky)

Zdroj: poznámky z cvičenia + správy vyučujúceho (Ján Genči) v Teams. Neupravovať bez nových pokynov vyučujúceho.

## Všeobecne

- Technológie a nástroje sú na našej voľbe („hlavné je, aby sme sa v nich vyznali“).
- Domény pre celú skupinu: **letenky** a **divadlo**.
- Naše mesto: **Kraków**. Naše divadlo: **Teatr im. Juliusza Słowackiego w Krakowie** (vybrané na základe spiku, `docs/zdroje/divadlo.md`; v rámci skupiny musí byť jedinečné, bez koordinácie to nemožno overiť).
- Tím s 2 členmi → **2 zdroje leteniek** (v rámci tímu musia byť rôzne; medzi tímami sa môžu opakovať) + **1 divadlo**.
  - Zdroj 1: **Travelpayouts** (Aviasales Data API)
  - Zdroj 2: **Ryanair** (verejné Fare Finder API). Pôvodne bol plánovaný Wizz Air, ale web je chránený interaktívnou kontrolou „či ste človek“ (`docs/zdroje/wizzair.md`), preto ho nahradil Ryanair.

## Letenky

- Zbierame lety **do mesta divadla (KRK) z rôznych letísk**. NIE z Košíc.
- Iba **jedným smerom** (prílet do Krakova).
- Objem: minimálne **~10 letov/trás**. Orientačne **~10 000 leteniek** = predané miesta + ponuky (počítame osobitne od počtu **udalostí**: udalosti sú správy v streame vrátane `offer.observed`, ich bude viac).
- Každú nájdenú ponuku ukladáme.
- **Generátor udalostí je povinný.** Sami rozhodujeme, že letenka je „predaná“, a **oznamujeme to na vlastnom rozhraní**:
  ktorá letenka, za akú cenu, kedy (a ďalšie údaje podľa nášho uváženia).
- Jedna ponuka sa môže predať viackrát (zo zápisu: „môžeme ju kúpiť 3-krát“).
- Predané letenky ukladáme aj lokálne.

## Divadlo

- Ceny zverejňujeme **v eurách**: pokladňa divadla predáva v PLN, preto uchovávame originál (`price`, `currency=PLN`) a pridávame `price_eur` podľa kurzu ECB v deň snímky. Pri zberačoch leteniek si ceny pýtame rovno v EUR.

- **100 % reálne dáta**: inscenácie, ceny, dostupnosť vstupeniek.
- Sledujeme **celé divadlo** (všetky inscenácie v jeho programe).
- Nebrať udalosti, ktoré sa vypredajú za pár minút.

## Prenos dát

- **Streaming dát je povinný na 100 %.**
- Uloženie v DB + prístup pre iné tímy je **doplnkový** spôsob získania dát, nie náhrada streamu.
- **Žiadna koordinácia medzi tímami** v oblasti protokolov, formátov, kódov. Každý tím si sám navrhuje
  svoje rozhranie a formát správ. Konzumenti sa prispôsobujú dodávateľovi (ako v reálnej praxi).
- Voľbu protokolu treba vedieť zdôvodniť (vyučujúci je proti „MQTT len preto, že poznáme iba to“).

## Zber dát iných tímov

- Zbierame dáta **všetkých** ostatných tímov → **data lake** (surové dáta, do polovice semestra).
- Po 6. týždni — **data warehouse** postavený nad data lake.

## Termíny

- Najbližšie cvičenie: **prezentácia** — čo zbierame, odkiaľ, ako streamujeme, formát správ.
- Polovica semestra: fungujúci zber + stream + data lake.
- Po 6. týždni: data warehouse.

## Hosting

- Možnosti: klaster **Hron** (žiadosť cez ÚVT), vlastný server/VPS, bezplatné cloudové tarify.
