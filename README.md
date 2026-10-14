# Payment Processing Service

[![CI](https://github.com/meloch287/payment-processing-service/actions/workflows/ci.yml/badge.svg)](https://github.com/meloch287/payment-processing-service/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-Pydantic%20v2-009688?logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-4169E1?logo=postgresql&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-4.1%20·%20FastStream-FF6600?logo=rabbitmq&logoColor=white)
![mypy](https://img.shields.io/badge/mypy-strict-2A6DB2)
![ruff](https://img.shields.io/badge/lint-ruff-D7FF64?logo=ruff&logoColor=black)

Асинхронный процессинг платежей: приём запроса → transactional outbox → RabbitMQ → эмуляция платёжного шлюза → webhook клиенту. Доставка at-least-once, идемпотентность на каждом шаге, retry с экспоненциальной задержкой, DLQ.

**Стек:** FastAPI · Pydantic v2 · SQLAlchemy 2.0 (async) · asyncpg · PostgreSQL · RabbitMQ · FastStream · Alembic · httpx · Docker Compose · uv

## Архитектура

<p align="center"><img src="docs/architecture.svg" alt="Архитектура" width="100%"></p>

| Сервис | Команда | Роль |
|---|---|---|
| `api` | `alembic upgrade head && uvicorn app.main:create_app --factory` | Миграции при старте (под advisory lock), REST API, идемпотентность, запись платежа и события в одной транзакции. Фоновая задача outbox relay публикует события в RabbitMQ и чистит старые записи |
| `consumer` | `python -m app.consumer` | Один обработчик: шлюз → статус в БД → webhook → retry / DLQ |
| `postgres` | - | Хранилище |
| `rabbitmq` | - | Брокер, management UI на `:15672` |

Ровно четыре сервиса, как в ТЗ. Relay можно вынести в отдельный процесс (`python -m app.outbox`, `OUTBOX_RELAY_ENABLED=false` в api) без изменений кода.

## Показ

<p align="center"><a href="docs/demo.mp4"><img src="docs/demo.gif" alt="Демонстрация работы сервиса" width="100%"></a></p>

<p align="center"><a href="docs/demo.mp4"><b>▶ demo.mp4</b></a> · запись терминала</p>

| Шаг | Что видно |
|---|---|
| 1 | `alembic upgrade head`, запуск `api` (с outbox relay) и `consumer` |
| 2 | `POST` → `202 pending` → relay публикует → шлюз 2-5 с → `succeeded` или `failed` (90/10), webhook с валидной HMAC-подписью |
| 3 | Повтор с тем же `Idempotency-Key` → тот же платёж, другое тело → `409`; `/health/ready`, `/docs`, `/openapi.json`, `/api/v1/*` без ключа → `401` |
| 4 | Webhook `503` → `503` → `200`, повторы через `retry.2000ms` и `retry.4000ms` |
| 5 | Webhook `500` × 3 → `payments.new.dlq` |
| 6 | RabbitMQ остановлен → API принимает платёж, событие ждёт в `outbox` → брокер поднят → платёж обработан |
| 7 | `pytest`: unit + integration |

Слева команды, справа логи outbox relay и consumer и приёмник webhook.

## Жизненный цикл платежа

<p align="center"><img src="docs/sequence.svg" alt="Жизненный цикл платежа" width="100%"></p>

## Гарантии

| Требование | Реализация |
|---|---|
| Гарантированная публикация | **Transactional outbox**: платёж и событие пишутся одной транзакцией. Relay публикует с publisher confirms, `persistent`, `mandatory`, с таймаутом `PUBLISH_TIMEOUT` и ставит `published_at` отдельной транзакцией на каждое событие сразу после подтверждения. Брокер недоступен - события копятся в `outbox`, API продолжает работать |
| Масштабирование relay | `FOR UPDATE SKIP LOCKED` - несколько экземпляров не берут одну и ту же запись |
| Идемпотентность API | `UNIQUE (idempotency_key)` + `INSERT … ON CONFLICT DO NOTHING`. Тело запроса хешируется (SHA-256 канонического JSON): тот же ключ и то же тело → `202` с исходным ответом и `Idempotent-Replayed: true`, другое тело → `409`. Гонка параллельных запросов разрешается на уровне PostgreSQL |
| Идемпотентность consumer | Перед вызовом шлюза платёж захватывается lease-арендой (`processing_lease_until`, условный `UPDATE … RETURNING`). Параллельная копия сообщения не вызывает шлюз, а откладывается в retry-очередь без расхода попытки. Lease истёк (consumer упал) - платёж подхватывает следующая доставка. Статус меняется только из `pending`, доставленный webhook повторно не шлётся. `payment_id` уходит в шлюз как ключ идемпотентности |
| Retry | 3 попытки на сообщение, задержки `RETRY_BASE_DELAY × 2ⁿ` (по умолчанию 2 с → 4 с) через TTL-очереди. Ожидание живёт в брокере, а не в памяти процесса: рестарт consumer ничего не теряет |
| DLQ | После 3-й неудачи сообщение уходит в `payments.new.dlq` с заголовками `x-attempt` и `x-error`. Невалидные сообщения и несуществующие платежи идут в DLQ сразу. Если не удаётся переопубликовать сообщение, оно возвращается в очередь, а при повторном сбое отклоняется в DLQ через `x-dead-letter-exchange` |
| Webhook | At-least-once. Отправку захватывает одна копия сообщения (`webhook_lease_until`), поэтому дубли из брокера не шлют webhook параллельно. Общий дедлайн `WEBHOOK_TIMEOUT` на весь запрос, тело ответа не читается. `X-Webhook-Id` детерминирован (`uuid5(payment_id, status)`) и одинаков во всех попытках - получатель дедуплицирует по нему |
| Аутентификация | `X-API-Key` на всех `/api/v1/*`, сравнение через `secrets.compare_digest` |

## Топология RabbitMQ

| Объект | Тип | Аргументы | Назначение |
|---|---|---|---|
| `payments` | direct exchange | - | Основной обменник |
| `payments.new` | queue | `x-dead-letter-exchange=payments.dlx` | Новые платежи |
| `payments.retry` | direct exchange | - | Отложенные повторы |
| `payments.new.retry.2000ms` | queue | `x-message-ttl=2000`, DLX → `payments` / `payments.new` | Задержка перед 2-й попыткой |
| `payments.new.retry.4000ms` | queue | `x-message-ttl=4000`, DLX → `payments` / `payments.new` | Задержка перед 3-й попыткой |
| `payments.dlx` | direct exchange | - | Dead letter exchange |
| `payments.new.dlq` | queue | - | Окончательно упавшие сообщения |

Отдельная очередь на каждую ступень задержки исключает head-of-line blocking, свойственный per-message TTL. Номер попытки передаётся в заголовке `x-attempt`. Все объекты durable, сообщения persistent.

## Быстрый старт

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```

| Адрес | Что |
|---|---|
| http://localhost:8000/docs | Swagger UI (с заголовком `X-API-Key`) |
| http://localhost:8000/health/ready | Readiness: проверка БД (с заголовком `X-API-Key`) |
| http://localhost:15672 | RabbitMQ management (`RABBITMQ_USER` / `RABBITMQ_PASSWORD`) |

```bash
docker compose logs -f api consumer
docker compose down -v
```

## API

Все эндпоинты без исключения требуют заголовок `X-API-Key`: `/api/v1/*`, `/health/*`, `/docs`, `/openapi.json`. Без ключа или с неверным ключом - `401`. Healthcheck в compose передаёт ключ из `API_KEY`.

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me" \
  -H "Idempotency-Key: order-1042" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #1042",
        "metadata": {"order_id": 1042},
        "webhook_url": "https://merchant.example/webhooks/payments"
      }'
```

```http
HTTP/1.1 202 Accepted
Location: /api/v1/payments/a15d2931-5aeb-44ba-83a8-d23de448a819

{"payment_id":"a15d2931-5aeb-44ba-83a8-d23de448a819","status":"pending","created_at":"2026-10-14T12:23:49.407564Z"}
```

| Поле | Тип | Ограничения |
|---|---|---|
| `amount` | decimal (строка или число) | `> 0`, не более 2 знаков после запятой, ≤ 9 999 999 999 999 999.99 |
| `currency` | enum | `RUB`, `USD`, `EUR` |
| `description` | string | 1-1024 символа |
| `metadata` | object | произвольный JSON, по умолчанию `{}` |
| `webhook_url` | URL | `http` / `https`, до 2048 символов |

| Код | Когда |
|---|---|
| `202` | Платёж принят. Повтор с тем же ключом и телом - тот же ответ + `Idempotent-Replayed: true` |
| `401` | Нет или неверный `X-API-Key` |
| `409` | `Idempotency-Key` уже использован с другим телом |
| `422` | Невалидное тело или нет `Idempotency-Key` |

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/a15d2931-5aeb-44ba-83a8-d23de448a819 -H "X-API-Key: change-me"
```

```json
{
  "payment_id": "a15d2931-5aeb-44ba-83a8-d23de448a819",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #1042",
  "metadata": {"order_id": 1042},
  "status": "succeeded",
  "failure_reason": null,
  "idempotency_key": "order-1042",
  "webhook_url": "https://merchant.example/webhooks/payments",
  "webhook": {"attempts": 1, "delivered_at": "2026-10-14T12:23:53.120417Z", "last_error": null},
  "created_at": "2026-10-14T12:23:49.407564Z",
  "processed_at": "2026-10-14T12:23:53.081233Z"
}
```

`status`: `pending` → `succeeded` | `failed` (`failure_reason: "declined_by_gateway"`). `404` - платёж не найден.

## Webhook

```http
POST {webhook_url}
Content-Type: application/json
X-Webhook-Id: 3d0c6f0e-8a4e-5c55-9a4b-6f1f0b6a2f11
X-Webhook-Timestamp: 1791980633
X-Webhook-Signature: sha256=5f0e…
```

```json
{
  "event_id": "3d0c6f0e-8a4e-5c55-9a4b-6f1f0b6a2f11",
  "event_type": "payment.succeeded",
  "data": {
    "payment_id": "a15d2931-5aeb-44ba-83a8-d23de448a819",
    "amount": "1500.00",
    "currency": "RUB",
    "status": "succeeded",
    "…": "все поля GET /payments/{id}, кроме webhook"
  }
}
```

Успех - любой `2xx`. Таймаут (общий на запрос), сетевая ошибка или другой код - неудачная попытка. Подпись: `HMAC-SHA256(WEBHOOK_SECRET, "{timestamp}.{raw_body}")`. Если `WEBHOOK_SECRET` пуст, заголовок не отправляется.

```python
import hashlib
import hmac


def verify(secret: str, timestamp: str, body: bytes, signature: str) -> bool:
    expected = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return hmac.compare_digest(f"sha256={expected.hexdigest()}", signature)
```

## Конфигурация

Переменные окружения или `.env` (`app/config.py`).

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | - (обязательна) | Статический ключ для `X-API-Key` |
| `WEBHOOK_SECRET` | пусто | Секрет HMAC-подписи webhook |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | Адрес PostgreSQL |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | `payments` / `payments` / `payments` | Учётные данные и база. Спецсимволы в пароле допустимы, DSN собирается с URL-кодированием |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` / `RABBITMQ_VHOST` | `localhost` / `5672` / `/` | Адрес RabbitMQ |
| `RABBITMQ_USER` / `RABBITMQ_PASSWORD` | `guest` / `guest` | Учётные данные RabbitMQ, кодируются так же |
| `DATABASE_URL` / `RABBITMQ_URL` | собираются из переменных выше | Готовый DSN целиком, если задан, имеет приоритет |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` | `10` / `10` | Пул соединений SQLAlchemy |
| `OUTBOX_RELAY_ENABLED` | `true` | Запускать outbox relay внутри api |
| `OUTBOX_POLL_INTERVAL` | `0.5` | Пауза relay между пустыми выборками, с |
| `OUTBOX_BATCH_SIZE` | `100` | Событий за одну итерацию relay |
| `PUBLISH_TIMEOUT` | `10` | Таймаут публикации в RabbitMQ с подтверждением, с |
| `OUTBOX_RETENTION_HOURS` | `72` | Через сколько удалять опубликованные события |
| `OUTBOX_CLEANUP_INTERVAL` | `300` | Период очистки outbox, с |
| `CONSUMER_PREFETCH` | `16` | Сообщений в обработке одновременно на consumer (QoS канала) |
| `PROCESSING_LEASE` | `30` | Аренда платежа на время вызова шлюза, с |
| `MAX_ATTEMPTS` | `3` | Попыток до DLQ, минимум 2 |
| `RETRY_BASE_DELAY` | `2` | База экспоненты, с: `2, 4, …` |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2` / `5` | Задержка эмулятора шлюза, с |
| `GATEWAY_SUCCESS_RATE` | `0.9` | Доля успешных платежей |
| `WEBHOOK_TIMEOUT` | `5` | Общий дедлайн HTTP-запроса webhook, с |
| `LOG_LEVEL` | `INFO` | Уровень логирования |

Docker Compose передаёт `POSTGRES_*` и `RABBITMQ_*` из `.env` и в контейнеры БД и брокера, и в приложение. CI прогоняет e2e с паролями вида `p@ss:w/rd+=#1`.

## Тесты

| Уровень | Где | Что проверяет | Окружение |
|---|---|---|---|
| Unit | `tests/unit` | Валидация и fingerprint, топология и задержки, эмулятор шлюза (границы задержки, 90/10), подпись и ошибки webhook, маршрутизация retry / DLQ / requeue | - |
| Integration | `tests/integration` | API на живой БД: 202 / 401 / 404 / 409 / 422 (включая NaN и NUL, которые не принимает PostgreSQL), 20 параллельных запросов с одним ключом → 1 платёж и 1 событие, outbox relay с RabbitMQ, retention, процессор: успех, отказ, повторная доставка, гонка двух consumer без двойного списания и двойного webhook, перехват истёкшего lease | PostgreSQL, RabbitMQ |
| E2E | `tests/e2e` | Реальные процессы `api` (с outbox relay) + `consumer` и HTTP-приёмник webhook: подпись, дедупликация, retry 503 → 503 → 200 с проверкой интервалов, DLQ после 3 × 500, параллельная обработка, рестарт consumer и остановка RabbitMQ без потерь, `401` на каждом эндпоинте | PostgreSQL, RabbitMQ или поднятый `docker compose` |
| Quickstart | `scripts/quickstart-check.sh` | Шаги раздела «Быстрый старт» дословно на чистой машине: `cp .env.example .env` → `docker compose up -d --build` → ровно 4 контейнера `running` → `401` без ключа на каждом эндпоинте → примеры curl из README: 202, replay, 409, обработка, 3 неудачных webhook → DLQ → рестарт без потери данных | Docker |

```bash
export DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/
bash scripts/verify.sh
bash scripts/e2e-compose.sh
bash scripts/quickstart-check.sh
```

`verify.sh`: `ruff check` → `ruff format --check` → `mypy --strict` → `alembic upgrade head` → `alembic check` → `pytest`.
`e2e-compose.sh` собирает образы, поднимает стек и прогоняет `tests/e2e` против контейнеров; рестарт consumer и остановка RabbitMQ выполняются через `docker compose stop/start`.
CI (GitHub Actions) на чистом раннере: lint и типы → тесты с service-контейнерами → e2e против `docker compose` → проверка Quickstart из README.

## Структура

```text
app/
├── api/              payments.py · health.py · deps.py (X-API-Key, сессия)
├── consumer/         __main__.py · handler.py (retry/DLQ) · processor.py · gateway.py · webhook.py
├── outbox/           relay.py · runner.py (фоновая задача api) · __main__.py (отдельный процесс)
├── config.py         pydantic-settings
├── messaging.py      топология RabbitMQ, брокер
├── models.py         Payment, OutboxEvent
├── schemas.py        Pydantic-модели API
└── services.py       создание платежа: идемпотентность + outbox
migrations/           Alembic (async)
tests/                unit · integration · e2e
scripts/              verify.sh · e2e-compose.sh · quickstart-check.sh
docs/                 architecture.svg · sequence.svg · demo.gif · demo.mp4
```

## Запуск

### Требования

| Инструмент | Версия | Для чего |
|---|---|---|
| Docker + Compose v2 | 24+ | весь стек или только PostgreSQL и RabbitMQ |
| [uv](https://docs.astral.sh/uv/) | 0.5+ | разработка и тесты, Python 3.12 ставится сам |
| curl | любая | примеры запросов |

Свободные порты: `8000` (api), `127.0.0.1:5432` (PostgreSQL), `127.0.0.1:5672` и `127.0.0.1:15672` (RabbitMQ).

### Весь стек в Docker

```bash
git clone https://github.com/meloch287/payment-processing-service.git
cd payment-processing-service
cp .env.example .env
docker compose up -d --build
docker compose ps
curl -s http://localhost:8000/health/ready -H "X-API-Key: change-me"
```

Ожидаемо: 4 контейнера `postgres`, `rabbitmq`, `api (healthy)`, `consumer` и ответ `{"status":"ok"}`. Миграции применяются при старте `api`.

Первый платёж:

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me" \
  -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.00", "currency": "RUB", "description": "demo", "webhook_url": "https://httpbin.org/post"}'

curl -s http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: change-me"
```

Через 2-5 секунд `status` станет `succeeded` или `failed`, а `webhook.delivered_at` заполнится.

| Действие | Команда |
|---|---|
| Логи | `docker compose logs -f api consumer` |
| Обновление | `git pull && docker compose up -d --build` |
| Миграции вручную | `docker compose exec api alembic upgrade head` |
| Остановка | `docker compose down` |
| Остановка с удалением данных | `docker compose down -v` |

Перед запуском вне локальной машины замените в `.env` значения `API_KEY`, `WEBHOOK_SECRET`, `POSTGRES_PASSWORD`, `RABBITMQ_PASSWORD`.

### Разработка: приложение локально, PostgreSQL и RabbitMQ в Docker

```bash
cp .env.example .env
docker compose up -d postgres rabbitmq
uv sync
uv run alembic upgrade head
uv run uvicorn app.main:create_app --factory --reload
```

Consumer во втором терминале:

```bash
uv run python -m app.consumer
```

Настройки читаются из `.env`: хосты по умолчанию `localhost`, учётные данные совпадают с контейнерами.

### Тесты

```bash
export DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments
export RABBITMQ_URL=amqp://payments:payments@localhost:5672/
bash scripts/verify.sh
```

`verify.sh` прогоняет ruff, mypy, миграции и все тесты. Integration-тесты очищают таблицы `payments` и `outbox` в этой базе.
