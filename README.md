# white proxy Bot (Telegram)

Telegram-бот для продажи персональных SOCKS5-прокси со сроком действия.

## Что уже сделано

- Тарифы:
  - `1 месяц` — `99₽`
  - `3 месяца` — `249₽`
  - `6 месяцев` — `499₽`
  - `12 месяцев` — `999₽`
- Оплата через `ЮKassa`: сначала кнопка оплаты, потом кнопка активации.
- Добавлена оплата `Telegram Stars` (курс в боте: `1₽ = 1.3⭐`, округление вверх до целой звезды).
- Добавлена реферальная программа: с оплаты приглашенного пользователя рефереру начисляется `50%` на внутренний реферальный баланс.
- Добавлена веб-админ-панель (`/admin`) с авторизацией по паролю.
- Новый flow покупки: выбор `месяцев` -> выбор `тарифа` -> `себе или другу`.
- Для покупки другу можно выбрать пользователя кнопкой Telegram (`request_user`), либо указать `tg_user_id`/`@username`/контакт.
- После активации бот выдаёт SOCKS5 в формате Telegram:
  - `socks5://login:password@SERVER_IP:PORT`
  - отдельно показывает `host / port / login / pass`
- Есть быстрая кнопка активации для первой прокси.
- Ссылки привязаны к Telegram-профилю пользователя (включая покупки для друзей по `tg_user_id`).
- Срок действия каждой покупки — `30 * количество месяцев`.
- Истёкшие подписки автоматически деактивируются, пользователь получает уведомление.

## docker-compose (бот + PostgreSQL + генерация SOCKS)

1. Создайте `.env`:

```bash
cp .env.example .env
```

2. Заполните минимум:

- `BOT_TOKEN` — токен бота
- `SERVER_IP` — публичный IP вашего сервера
- `ADMIN_TG_IDS` — ваш Telegram ID (или список через запятую)

3. Запустите:

```bash
docker compose up -d --build
```

Что поднимется:

- `postgres` — основная БД бота (PostgreSQL).
- `socks-farm` — сервис, который генерирует пул SOCKS5 (`port/login/password`) и запускает сами SOCKS5-прокси.
- `bot` — Telegram-бот, который берёт прокси из этого пула и выдаёт пользователям.
- Порт Postgres публикуется наружу: `${POSTGRES_BIND_HOST}:${POSTGRES_PORT}` (по умолчанию `0.0.0.0:5433`).

Важно:
- В `docker-compose` для `socks-farm` используется `network_mode: host` (Linux VDS) — это заметно ускоряет старт больших диапазонов (например, 1000 портов).
- Если часть портов уже занята, `socks-farm` их автоматически пропускает и помечает как `active: false` в `proxy_pool.json`.
- Бот использует только прокси с `active: true`, поэтому занятые порты автоматически отсеиваются.

## Переменные окружения

- `BOT_TOKEN` — токен Telegram-бота
- `ADMIN_TG_IDS` — список Telegram ID админов через запятую, например `123,456`
- `DATABASE_URL` — DSN PostgreSQL (если указан, бот работает с Postgres)
- `DATABASE_PATH` — путь к SQLite БД (fallback, когда `DATABASE_URL` пустой)
- `POSTGRES_DB` — имя БД контейнера Postgres
- `POSTGRES_USER` — пользователь Postgres
- `POSTGRES_PASSWORD` — пароль Postgres
- `POSTGRES_PORT` — порт Postgres на хосте
- `POSTGRES_BIND_HOST` — адрес bind для публикации порта (например `0.0.0.0` или `127.0.0.1`)
- `PROXY_PUBLIC_HOST` — хост/IP, который бот вставляет в ссылки
- `PROXY_POOL_FILE` — путь к JSON-пулу прокси
- `EXPIRATION_CHECK_INTERVAL` — интервал проверки истечения (сек.)
- `YOOKASSA_SHOP_ID` — ID магазина ЮKassa
- `YOOKASSA_SECRET_KEY` — секретный ключ ЮKassa
- `YOOKASSA_RETURN_URL` — URL возврата после оплаты (redirect)
- `YOOKASSA_RECEIPT_EMAIL` — email для отправки чеков ЮKassa (фискализация)
- `POLLING_PAYMENT` — включить фоновую проверку платежа после отправки ссылки (`1`/`0`)
- `WEBHOOK_HOST` — bind host HTTP-сервера вебхуков
- `WEBHOOK_PORT` — bind port HTTP-сервера вебхуков
- `WEBHOOK_BIND_HOST` — host bind для публикации webhook-порта в `docker-compose`
- `TELEGRAM_WEBHOOK_URL` — внешний base URL для Telegram webhook (если пусто, бот работает через polling)
- `TELEGRAM_WEBHOOK_SECRET_TOKEN` — секретный токен для Telegram webhook
- `ADMIN_PANEL_PASSWORD` — пароль входа в веб-админку (если пусто, веб-админка отключена)
- `ADMIN_PANEL_PATH` — путь веб-админки (по умолчанию `/admin`)
- `SERVER_IP` — публичный IP сервера (используется в `docker-compose`)
- `SOCKS_BIND_HOST` — интерфейс bind SOCKS-сервиса
- `SOCKS_PORT_RANGE` — диапазон портов SOCKS, например `30000-30199`
- `SOCKS_POOL_FILE` — путь к файлу пула для socks-сервиса

HTTP-маршруты вебхуков:
- `POST /webhook/` — вебхуки ЮKassa
- `POST /telewebhook/` — вебхуки Telegram
- `GET /admin` — веб-админ-панель (или ваш `ADMIN_PANEL_PATH`)

## Команды бота

- `/start` — главное меню
- `/buy` — оформить доступ (выбор месяцев и тарифа)
- `/my_links` — мои прокси
- `/status` — активные подписки и остаток времени
- `/ref` — реферальная программа, ваша ссылка и баланс
- `/admin` — админ-панель (только для `ADMIN_TG_IDS`)

## Миграция SQLite -> PostgreSQL

Если у вас уже есть рабочая SQLite БД (`data/bot.db`), используйте скрипт:

```bash
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite-path data/bot.db \
  --postgres-url postgresql://proxybot:proxybot@localhost:5432/proxybot
```

По умолчанию скрипт очищает целевые таблицы Postgres перед копированием (`TRUNCATE ... CASCADE`) и переносит данные с сохранением `id`.

Через Docker (рекомендуется на сервере):

```bash
docker compose run --rm bot python scripts/migrate_sqlite_to_postgres.py \
  --sqlite-path /data/bot.db \
  --postgres-url postgresql://proxybot:proxybot@postgres:5432/proxybot
```

## Локальный запуск без Docker

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Для локального запуска тоже нужен `PROXY_POOL_FILE` с валидным пулом прокси.
