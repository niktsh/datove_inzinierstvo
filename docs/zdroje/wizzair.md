# Zdroj: Wizz Air (spike, 2026-10-03)

**Záver: web je chránený interaktívnou kontrolou „Human Verification“. Zberač na Playwrighte bez obchádzania ochrany nie je možný. Odporúčanie: nahradiť ho Ryanairom (záložný zdroj, pozri `PLAN.md`, fáza 3).**

## Čo sa overilo
- Playwright 1.63, Chromium 153, headless, bežný kontext `en-GB`, jeden dopyt `GET https://www.wizzair.com/en-gb`.
- Odpoveď: **HTTP 405**, titulok stránky `Human Verification`, text „Let's confirm you are human“ s tlačidlom „Begin“ (štýl AWS WAF CAPTCHA/challenge). Je to interaktívna kontrola, či ide o človeka, nie tichý JS challenge.
- Pokusy o spustenie v okne (headful, WSLg) tiež ukázali kontrolu, ani jedna stránka sa nenačítala (potvrdil používateľ). Interné JSON dopyty na vyhľadávanie a rozpis sa nepodarilo vidieť, preto endpointy, parametre a polia nie sú popísané a fixtures neexistujú.

## Prečo nepokračujeme
- Ďalšia automatizácia by znamenala prejsť alebo obísť kontrolu, či ide o človeka (riešenie captchy, podvrhnutie odtlačku prehliadača, proxy). To je obchádzanie ochrany webu, nerobíme to.
- Aj pri ručnom prejdení kontroly cookie dlho nežije a zberač bežiaci každých 6–12 hodín by vyžadoval človeka.

## Rozhodnutie
Wizz Air bol nahradený Ryanairom (`ryanair.md`): spike prebehol, zberač je implementovaný vo fáze 3.

## Prostredie (na zopakovanie)
- Vo WSL chýbala `libasound.so.2`. Bez roota sa nainštalovala cez `apt-get download libasound2t64` a `LD_LIBRARY_PATH`. V Docker image Playwrightu knižnice sú.
