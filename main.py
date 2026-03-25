from __future__ import annotations

import asyncio
from contextlib import suppress
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand

from proxybot.config import load_settings
from proxybot.database_factory import create_database
from proxybot.handlers import create_router
from proxybot.proxy_pool_loader import load_proxy_pool
from proxybot.webhook_server import WebhookServer
from proxybot.worker import expiration_worker, proxy_pool_sync_worker
from proxybot.yookassa import YooKassaClient


async def setup_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Главное меню"),
            BotCommand(command="help", description="Помощь"),
            BotCommand(command="plans", description="Тарифы"),
            BotCommand(command="buy", description="Купить тариф"),
            BotCommand(command="my_links", description="Мои прокси"),
            BotCommand(command="status", description="Статус подписки"),
        ]
    )


async def run() -> None:
    settings = load_settings()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    db = create_database(
        database_url=settings.database_url,
        database_path=settings.database_path,
    )
    await db.connect()
    await db.init_schema()
    pool = load_proxy_pool(settings.proxy_pool_file)
    await db.sync_proxy_pool(pool)
    if pool:
        logging.info("Loaded %d SOCKS proxies into DB pool", len(pool))
    else:
        logging.warning(
            "SOCKS proxy pool file '%s' is empty or missing. Purchases will fail until pool is generated.",
            settings.proxy_pool_file,
        )

    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    await setup_bot_commands(bot)
    dispatcher = Dispatcher()
    dispatcher.include_router(
        create_router(
            db=db,
            proxy_public_host=settings.proxy_public_host,
            admin_tg_ids=settings.admin_tg_ids,
            yookassa_client=YooKassaClient(
                shop_id=settings.yookassa_shop_id,
                secret_key=settings.yookassa_secret_key,
                return_url=settings.yookassa_return_url,
            ),
        )
    )
    allowed_updates = dispatcher.resolve_used_update_types()
    webhook_server = WebhookServer(
        db=db,
        bot=bot,
        dispatcher=dispatcher,
        host=settings.webhook_host,
        port=settings.webhook_port,
        telegram_webhook_secret_token=settings.telegram_webhook_secret_token,
    )
    await webhook_server.start()

    telegram_webhook_url = settings.telegram_webhook_url.strip().rstrip("/")
    if telegram_webhook_url:
        webhook_url = f"{telegram_webhook_url}/telewebhook/"
        await bot.set_webhook(
            url=webhook_url,
            secret_token=settings.telegram_webhook_secret_token or None,
            drop_pending_updates=False,
            allowed_updates=allowed_updates,
        )
        logging.info("Telegram webhook mode enabled: %s", webhook_url)
    else:
        await bot.delete_webhook(drop_pending_updates=False)
        logging.info("Telegram polling mode enabled.")

    worker_task = asyncio.create_task(
        expiration_worker(bot=bot, db=db, check_interval=settings.expiration_check_interval)
    )
    sync_task = asyncio.create_task(
        proxy_pool_sync_worker(db=db, pool_file=settings.proxy_pool_file, check_interval=30)
    )
    try:
        if telegram_webhook_url:
            await asyncio.Event().wait()
        else:
            await dispatcher.start_polling(bot, allowed_updates=allowed_updates)
    finally:
        sync_task.cancel()
        worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await sync_task
        with suppress(asyncio.CancelledError):
            await worker_task
        await webhook_server.stop()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(run())
