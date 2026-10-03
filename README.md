# Kraków DI

Сбор авиабилетов (Travelpayouts, Wizz Air) и данных театра в Кракове, генератор продаж,
стриминг через Kafka, data lake. Документация — в [docs/](docs/), план — [docs/PLAN.md](docs/PLAN.md).

## Запуск с нуля

Нужны: [uv](https://docs.astral.sh/uv/), Docker с Compose v2.

```bash
cp .env.example .env        # заполнить TRAVELPAYOUTS_TOKEN и пароли
uv sync                     # Python 3.12 + зависимости
docker compose up -d        # postgres, kafka, kafka-ui
docker compose ps           # все сервисы должны быть healthy
uv run pytest
uv run ruff check .
```

- Kafka UI: http://localhost:8080 (только localhost)
- PostgreSQL: `localhost:5433` (5432 часто занят локальным Postgres), Kafka: `localhost:9092`

Остановить: `docker compose down` (данные сохраняются в volumes; `-v` удаляет их).
