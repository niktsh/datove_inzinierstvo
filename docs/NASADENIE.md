# Nasadenie na server (fáza 9)

Všetko potrebné je v repozitári: [`docker-compose.prod.yml`](../docker-compose.prod.yml), [`Dockerfile`](../Dockerfile), `deploy/` a `scripts/`. Samotný server (Hron/ÚVT alebo VPS) treba zabezpečiť zvlášť: pozri „Server“.

## Čo sa nasadí

| Služba | Úloha | Dostupnosť |
|---|---|---|
| `postgres` | databáza (`raw`, `core`, `lake`) | iba docker sieť |
| `kafka` | broker (KRaft). `INTERNAL` bez autentifikácie pre naše služby, `EXTERNAL` **SASL_SSL + SCRAM-SHA-512** pre ostatné tímy | **jediný** verejný port Kafky: `9094` |
| `kafka-init` | jednorazovo: používateľ `teams` + ACL iba na čítanie | – |
| `migrate` | jednorazovo: migrácie DB + vytvorenie topikov | – |
| `app` | plánovač: Travelpayouts (6 h), Ryanair (12 h), program divadla (24 h), snímky divadla (3 h), generátor (5 min) + publikovanie | – |
| `api` | REST + SSE + WebSocket | cez `caddy` |
| `lake` | data lake (adaptéry z `config/lake_sources.yaml`) | – |
| `caddy` | reverse proxy, automatické HTTPS | `80`, `443` |
| `backup` | denná záloha DB (`pg_dump`, 14 dní) | – |
| `kafka-ui` | prehliadač Kafky (profil `tools`) | iba `127.0.0.1:8080`, **von neotvárať** |

## Server

**Odporúčanie:** 2 vCPU, **4 GB RAM**, SSD **50–100 GB**, verejná IPv4 a (pre HTTPS) doménové meno. Odôvodnenie z meraní: naše dáta rastú ≈ 25 MB/deň v Postgrese (≈ 2 GB za semester) a ≈ 3 MB/deň v Kafke; Kafka zaberá ≈ 300 MB RAM, kafka-ui ≈ 320 MB, Python služby 150–250 MB každá. Najväčšia neznáma je data lake (dáta ostatných tímov; pri 10 tímoch po 50 tisíc správ denne ≈ 0,5 GB/deň, preto diskový priestor navyše). RAM Kafky sa dá obmedziť premennou `KAFKA_HEAP_OPTS` (predvolene `-Xms512m -Xmx1g`).

**Otvorené porty zvonku:** `80` a `443` (API, Caddy) a `9094` (Kafka SASL_SSL). Nič iné (najmä nie `5432`, `19092`, `8080`).

### Žiadosť pre ÚVT (Hron)

> Dobrý deň, pre predmet Dátové inžinierstvo (TUKE) potrebujeme server pre projekt „Kraków DI“ (zber dát o letenkách a divadle, streaming cez Apache Kafka, data lake). Požiadavky: 2 vCPU, 4 GB RAM, 50–100 GB disk, Docker + Docker Compose, verejná IPv4 adresa (alebo DNS meno) a možnosť otvoriť zvonku porty **9094/TCP** (Kafka, SASL_SSL) a **80, 443/TCP** (HTTP API). Ostatné tímy sa k nášmu streamu pripájajú zvonku (bez koordinácie s nami), preto je otvorený port 9094 nevyhnutný. Beh 24/7 počas semestra. Ďakujeme.

Ak ÚVT porty neotvorí alebo odpoveď potrvá dlho, použite bežný VPS s rovnakými parametrami; postup je rovnaký.

## Postup krok za krokom

1. **Príprava servera:** nainštalujte Docker a Docker Compose, naklonujte repozitár.
2. **`.env`:** `cp .env.example .env` a vyplňte (`.env` nikdy necommitovať):
   - `POSTGRES_PASSWORD`: silné náhodné heslo (`openssl rand -base64 24`),
   - `KAFKA_TEAMS_PASSWORD`: heslo pre ostatné tímy (`openssl rand -base64 24`),
   - `KAFKA_EXTERNAL_HOST`: **verejné meno servera** (to isté meno musí byť v certifikáte),
   - `KAFKA_SSL_PASSWORD`: heslo ku keystore,
   - `API_DOMAIN`: doména pre HTTPS (`:80` = iba HTTP),
   - `TRAVELPAYOUTS_TOKEN`, prípadne `GENERATOR_*` a `SCHEDULER_*`.
3. **Certifikát pre Kafku:** `scripts/gen_kafka_tls.sh kafka.example.org [IP]`. Vznikne `deploy/certs/ca.crt` (**tento súbor dostanú ostatné tímy**, aby dôverovali nášmu certifikátu) a `kafka.keystore.p12`. Namiesto vlastnej CA môžete dodať certifikát od verejnej CA ako PKCS12 keystore s rovnakým názvom.
4. **Firewall:** povoľte iba `80`, `443`, `9094` (a SSH).
5. **Spustenie:**
   ```bash
   docker compose -f docker-compose.prod.yml up -d --build
   docker compose -f docker-compose.prod.yml ps        # všetko Up/healthy, jednorazové služby Exited (0)
   curl https://<API_DOMAIN>/health                    # {"status":"ok",...}
   ```
6. **Kontrola zvonku servera** (z notebooku; overí čítanie od offsetu 0, zákaz zápisu, zákaz cudzej consumer group a odmietnutie zlého hesla):
   ```bash
   uv run python tools/check_external.py --bootstrap <KAFKA_EXTERNAL_HOST>:9094 \
       --user teams --password '<KAFKA_TEAMS_PASSWORD>' --cafile ca.crt
   ```
   Očakávaný výsledok: `4 z 4 kontrol v poriadku`.
7. **Odovzdanie ostatným tímom:** adresa `<KAFKA_EXTERNAL_HOST>:9094`, používateľ `teams`, heslo (bezpečným kanálom, nie cez repozitár), `ca.crt` a odkaz na [PRE_TIMY.md](PRE_TIMY.md).

## Prevádzka

- **Logy:** `docker compose -f docker-compose.prod.yml logs -f app api lake` (rotácia 5 × 10 MB).
- **Zdravie:** `api` a `app` majú healthcheck (`/health`; `app` kontroluje heartbeat plánovača, generátor ho obnovuje každých 5 minút). Všetky služby majú `restart: unless-stopped`.
- **Aktualizácia:** `git pull && docker compose -f docker-compose.prod.yml up -d --build` (migrácie sa spustia automaticky).
- **Pridanie tímu do data lake:** upravte `config/lake_sources.yaml` (+ `.env` s ich údajmi) a `docker compose -f docker-compose.prod.yml restart lake`; pozri [timy/README.md](timy/README.md).
- **kafka-ui:** `docker compose -f docker-compose.prod.yml --profile tools up -d kafka-ui` a SSH tunel `ssh -L 8080:127.0.0.1:8080 server` (nikdy nepublikovať).
- **Štatistika lake:** `docker compose -f docker-compose.prod.yml exec lake python tools/lake_stats.py`.
- **Zálohy:** denne do `./backups/krakow_di_<čas>.dump` (14 dní). Zálohuje celú DB vrátane `core.event_log` a `lake.message`; Kafku osobitne zálohovať netreba (históriu udalostí je možné znova vydať z `event_log`). Obnova do novej DB:
  ```bash
  docker compose -f docker-compose.prod.yml exec backup sh -c \
    'createdb -h postgres -U krakow obnova && pg_restore -h postgres -U krakow -d obnova /backups/<súbor>.dump'
  ```
  Odporúčame zálohy občas skopírovať aj mimo servera.

## Čo je overené a čo nie

Overené lokálne (2026-10-04, `docker compose -p krakowprod`, vlastná CA pre `localhost`):
- obraz sa zostaví a všetky služby naštartujú; `api` je healthy, `caddy` posiela `/health`, `/docs` aj SSE (cez HTTP);
- Kafka EXTERNAL (SASL_SSL, SCRAM-SHA-512) s overením certifikátu cez našu CA: používateľ `teams` číta od offsetu 0, **zápis je zakázaný** (`TopicAuthorizationFailedError`), cudzia consumer group je zakázaná (`GroupAuthorizationFailedError`), nesprávne heslo sa odmietne (`tools/check_external.py`: 4 z 4);
- `lake` číta z Kafky cez INTERNAL listener, plánovač beží v kontajneri a zapisuje heartbeat;
- záloha vznikne a **dá sa obnoviť** (`pg_restore` do novej DB).

**Neoverené (vyžaduje skutočný server):** pripojenie z cudzej siete (správne `advertised.listeners` a otvorený port), HTTPS s verejnou doménou (Let's Encrypt), beh 24 hodín bez zásahu, správanie pod záťažou ostatných tímov.

## Riešenie problémov

- **Klient sa pripojí ku Kafke, ale nič nečíta / časový limit:** takmer vždy zlé `KAFKA_EXTERNAL_HOST` (broker ohlasuje adresu, ktorá z vonka neplatí) alebo zatvorený port 9094. Meno musí byť verejné a rovnaké ako v certifikáte.
- **`SSL handshake` / `certificate verify failed`:** klient nemá `ca.crt` (alebo certifikát nemá meno servera v SAN: znova `scripts/gen_kafka_tls.sh <meno> [IP]` a reštart brokera).
- **`SaslAuthenticationFailed`:** zlé heslo; heslo sa zmení úpravou `KAFKA_TEAMS_PASSWORD` a `docker compose -f docker-compose.prod.yml up -d kafka-init`.
- **Broker nenaštartuje (`controller.listener.names must contain...`):** `KAFKA_LISTENERS` musí byť v `docker-compose.prod.yml` zadané výslovne (obraz inak odvodí listenery len z advertised).
- **Nedostatok pamäte:** znížte `KAFKA_HEAP_OPTS` (napr. `-Xms256m -Xmx512m`) alebo nepoužívajte `kafka-ui`.
