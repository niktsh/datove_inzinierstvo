# Каталог событий (наш публичный контракт)

Это то, что видят другие команды. Любое изменение синхронизировать с `schemas/events/*.json` и `docs/asyncapi.yaml`.

Транспорт: **Apache Kafka**. Значение сообщения — JSON (UTF-8) с envelope ниже. Ключ — id сущности. Заголовок `event_type` дублирует тип события.

## Envelope (общая обёртка каждого сообщения)

```json
{
  "event_id": "0f8b6c1e-2d4a-4b8e-9a51-7f3c2e1d9b00",
  "event_type": "flight.ticket.sold",
  "event_version": 1,
  "occurred_at": "2026-10-14T09:31:05Z",
  "producer": "tuke-di-krakow",
  "source": "generator",
  "data": { }
}
```

| Поле | Тип | Описание |
|---|---|---|
| `event_id` | UUID v4 | Уникальный id события. Потребитель может по нему дедуплицировать. |
| `event_type` | string | Тип события (также в Kafka-заголовке `event_type`). |
| `event_version` | int | Версия схемы `data` для этого типа. |
| `occurred_at` | ISO 8601 UTC | Когда событие произошло (не когда отправлено). |
| `producer` | string | Наш идентификатор. |
| `source` | enum | `travelpayouts` \| `ryanair` \| `theater` \| `generator` |
| `data` | object | Тело события, зависит от типа. |

## Авиабилеты

### `flight.offer.found` — сборщик нашёл новое предложение
```json
{
  "offer_id": "a1b2c3d4e5f60718",
  "origin_iata": "BCN",
  "destination_iata": "KRK",
  "departure_at": "2026-11-20T06:15:00+01:00",
  "arrival_at": "2026-11-20T09:10:00+01:00",
  "airline_iata": "FR",
  "flight_number": "FR3035",
  "stops": 0,
  "price": 54.99,
  "currency": "EUR",
  "seats_total": 189,
  "seats_left": 141,
  "seats_simulated": true,
  "observed_at": "2026-10-14T06:00:12Z"
}
```
`flight_number` может быть `null` (Travelpayouts его не всегда даёт). `seats_*` всегда симулированы генератором — честно помечено полем `seats_simulated`.

### `flight.offer.observed` — повторный скрейп уже известного предложения
Тот же `data`, что у `offer.found`. Отправляется при каждом скрейпе, даже если ничего не изменилось (для истории в data lake).

### `flight.offer.price_changed`
```json
{ "offer_id": "a1b2c3d4e5f60718", "old_price": 54.99, "new_price": 61.49, "currency": "EUR", "reason": "load_factor" }
```
`reason`: `load_factor` (генератор) | `scrape` (реальная цена у источника изменилась).

### `flight.ticket.sold` — ключевое событие генератора
```json
{
  "sale_id": "5d0c9f3a-...",
  "offer_id": "a1b2c3d4e5f60718",
  "origin_iata": "BCN",
  "destination_iata": "KRK",
  "departure_at": "2026-11-20T06:15:00+01:00",
  "airline_iata": "FR",
  "flight_number": "FR3035",
  "quantity": 2,
  "unit_price": 54.99,
  "total_price": 109.98,
  "currency": "EUR",
  "seats_left_after": 139,
  "sold_at": "2026-10-14T09:31:05Z"
}
```
Денормализовано специально: потребителю не нужно знать `offer.found`, чтобы понять продажу.

### `flight.offer.sold_out`
```json
{ "offer_id": "a1b2c3d4e5f60718", "last_price": 89.99, "currency": "EUR" }
```

### `flight.offer.expired`
```json
{ "offer_id": "a1b2c3d4e5f60718", "reason": "departed" }
```
`reason`: `departed` | `not_found_at_source`.

## Театр

### `theater.performance.found` / `theater.performance.updated`
```json
{
  "performance_id": "teatr-xyz-2026-11-08-1900-duza-scena",
  "title": "Wesele",
  "stage": "Duża Scena",
  "starts_at": "2026-11-08T19:00:00+01:00",
  "url": "https://...",
  "status": "on_sale"
}
```
`status`: `on_sale` | `sold_out` | `cancelled` | `past`.

### `theater.availability.snapshot` — реальный снимок наличия
```json
{
  "performance_id": "teatr-xyz-2026-11-08-1900-duza-scena",
  "observed_at": "2026-10-14T12:00:03Z",
  "categories": [
    { "category": "Parter I", "price": 120.00, "currency": "PLN", "seats_available": 34 },
    { "category": "Balkon", "price": 60.00, "currency": "PLN", "seats_available": 12 }
  ],
  "seats_available_total": 46
}
```

### `theater.tickets.sold` — **реальные** продажи, вычисленные из разницы снимков
```json
{
  "performance_id": "teatr-xyz-2026-11-08-1900-duza-scena",
  "category": "Parter I",
  "quantity": 3,
  "unit_price": 120.00,
  "currency": "PLN",
  "detected_between": ["2026-10-14T08:00:01Z", "2026-10-14T12:00:03Z"]
}
```
Продажа фиксируется с точностью до интервала между снимками. Если мест стало больше (возврат) — `quantity` отрицательное.

## Топики Kafka

| Топик | Типы событий | Ключ |
|---|---|---|
| `krakow.flights.offers` | `flight.offer.found`, `flight.offer.observed`, `flight.offer.price_changed`, `flight.offer.sold_out`, `flight.offer.expired` | `offer_id` |
| `krakow.flights.sales` | `flight.ticket.sold` | `offer_id` |
| `krakow.theater.performances` | `theater.performance.found`, `theater.performance.updated` | `performance_id` |
| `krakow.theater.availability` | `theater.availability.snapshot` | `performance_id` |
| `krakow.theater.sales` | `theater.tickets.sold` | `performance_id` |

Примеры для потребителя:
- только продажи: подписка на `krakow.flights.sales` и `krakow.theater.sales`
- всё: подписка по шаблону `^krakow\..*`
- вся история с начала: новый consumer group + `auto_offset_reset=earliest` (retention без ограничения)

Подключение: SASL_SSL, механизм SCRAM-SHA-512, пользователь только на чтение, consumer group должен начинаться с `team-` (напр. `team-05-lake`).
Адрес и учётные данные — в `docs/PRE_TIMY.md` (создаётся в фазе 7).

## Версионирование

- Добавление необязательного поля — та же версия.
- Удаление/переименование поля или смена типа — `event_version + 1`, старую версию публикуем параллельно минимум 2 недели.
