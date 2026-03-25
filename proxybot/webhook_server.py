from __future__ import annotations

import logging
from typing import Any

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.types import Update

from .database import Database
from .database_postgres import PostgresDatabase

logger = logging.getLogger(__name__)


class WebhookServer:
    def __init__(
        self,
        *,
        db: Database | PostgresDatabase,
        bot: Bot,
        dispatcher: Dispatcher,
        host: str,
        port: int,
        telegram_webhook_secret_token: str = "",
    ) -> None:
        self.db = db
        self.bot = bot
        self.dispatcher = dispatcher
        self.host = host
        self.port = port
        self.telegram_webhook_secret_token = telegram_webhook_secret_token.strip()

        self._app = web.Application()
        self._app.add_routes(
            [
                web.post("/webhook/", self._handle_yookassa_webhook),
                web.post("/telewebhook/", self._handle_telegram_webhook),
            ]
        )
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host=self.host, port=self.port)
        await self._site.start()
        logger.info("Webhook HTTP server started on %s:%s", self.host, self.port)
        logger.info("YooKassa webhook endpoint: /webhook/")
        logger.info("Telegram webhook endpoint: /telewebhook/")

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None

    async def _handle_yookassa_webhook(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            logger.warning("Invalid YooKassa webhook payload (not JSON).")
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

        if not isinstance(payload, dict):
            return web.json_response({"ok": False, "error": "invalid_payload"}, status=400)

        await self._process_yookassa_payload(payload)
        return web.json_response({"ok": True})

    async def _process_yookassa_payload(self, payload: dict[str, Any]) -> None:
        event = str(payload.get("event") or "").strip()
        obj = payload.get("object")
        if not isinstance(obj, dict):
            logger.info("YooKassa webhook without object: event=%s", event or "-")
            return

        payment_id = str(obj.get("id") or "").strip()
        remote_status = str(obj.get("status") or "").strip()
        if not payment_id:
            logger.info("YooKassa webhook without payment id: event=%s", event or "-")
            return

        payment = await self.db.get_payment_by_yookassa_payment_id(payment_id)
        if payment is None:
            logger.info("YooKassa webhook for unknown payment_id=%s", payment_id)
            return

        logger.info(
            "YooKassa webhook received: event=%s payment_id=%s remote_status=%s local_payment_id=%s",
            event or "-",
            payment_id,
            remote_status or "-",
            payment.get("id"),
        )

        if event != "payment.succeeded" and remote_status != "succeeded":
            return
        if str(payment.get("status") or "") != "pending":
            return

        user_row = await self.db.get_user_by_id(int(payment["user_id"]))
        if user_row is None:
            logger.warning("Cannot notify payer for payment %s: user not found", payment.get("id"))
            return

        buyer_tg_user_id = int(user_row["tg_user_id"])
        target_tg_user_id = int(payment.get("target_tg_user_id") or buyer_tg_user_id)
        target_label = "Себе" if target_tg_user_id == buyer_tg_user_id else "Другу"

        try:
            await self.bot.send_message(
                buyer_tg_user_id,
                (
                    "🎉 Платеж подтвержден в ЮKassa.\n"
                    f"ID платежа: {payment['id']}\n"
                    f"Покупка: {target_label}\n\n"
                    "Для выдачи прокси нажмите «Активировать»."
                ),
                parse_mode=None,
            )
        except Exception as exc:
            logger.warning("Could not notify payer %s: %s", buyer_tg_user_id, exc)

    async def _handle_telegram_webhook(self, request: web.Request) -> web.Response:
        if self.telegram_webhook_secret_token:
            got_token = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if got_token != self.telegram_webhook_secret_token:
                logger.warning("Rejected Telegram webhook: invalid secret token")
                return web.Response(status=403, text="forbidden")

        try:
            payload = await request.json()
        except Exception:
            logger.warning("Invalid Telegram webhook payload (not JSON).")
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

        if not isinstance(payload, dict):
            return web.json_response({"ok": False, "error": "invalid_payload"}, status=400)

        try:
            update = Update.model_validate(payload)
            await self.dispatcher.feed_update(self.bot, update)
        except Exception as exc:
            logger.exception("Failed to process Telegram webhook update: %s", exc)
            return web.json_response({"ok": False}, status=500)
        return web.json_response({"ok": True})
