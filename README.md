# Kraków DI

Zber leteniek (Travelpayouts, Ryanair) a dát divadla v Krakove, generátor predaja,
streaming cez Kafku, data lake. Dokumentácia je v [docs/](docs/), plán v [docs/PLAN.md](docs/PLAN.md).

## Spustenie od nuly

Potrebujete: [uv](https://docs.astral.sh/uv/), Docker s Compose v2.

```bash
cp .env.example .env          # doplniť TRAVELPAYOUTS_TOKEN a heslá
uv sync                       # Python 3.12 + závislosti
docker compose up -d          # postgres, kafka, kafka-ui
docker compose ps             # všetky služby musia byť healthy
uv run alembic upgrade head   # schémy raw/core/lake
uv run pytest                 # testy DB bežia v dočasnej DB na compose Postgrese
uv run ruff check .
```

- Kafka UI: http://localhost:8080 (iba localhost)
- PostgreSQL: `localhost:5433` (5432 býva obsadený lokálnym Postgresom), Kafka: `localhost:9092`

Jedno spustenie zberačov leteniek (Travelpayouts potrebuje `TRAVELPAYOUTS_TOKEN` v `.env`):

```bash
uv run python -m krakow_di.collectors.travelpayouts
uv run python -m krakow_di.collectors.ryanair --days 4   # bez --days: celý horizont, ~30 min
```

Kafka: vytvorenie topikov, odoslanie udalostí zo zberu (`--publish` je pri každom zberači) a kontrolný consumer
(overuje JSON Schema, hlavičky, kľúče a poradie):

```bash
uv run python tools/create_topics.py
uv run python -m krakow_di.collectors.travelpayouts --publish
uv run python tools/consume.py --from-beginning
```

Divadlo (program a snímky dostupnosti, `--limit N` obmedzí počet predstavení):

```bash
uv run python -m krakow_di.collectors.slowacki --months 2 --limit 8
```

Zastavenie: `docker compose down` (dáta ostanú vo volumes; `-v` ich zmaže).
