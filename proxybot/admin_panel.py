from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html import escape
import os
import secrets
from typing import Any
from urllib.parse import urlencode

from aiohttp import web
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.types import BufferedInputFile

from .database import Database
from .database_postgres import PostgresDatabase


DEFAULT_BAN_TEXT = "Доступ к боту ограничен администратором."
BLOCKED_TG_USER_ID = 1664076316
SESSION_COOKIE_NAME = "whiteprox_admin_session"

SECTIONS: tuple[tuple[str, str, str], ...] = (
    ("dashboard", "Дашборд", "📊"),
    ("users", "Пользователи", "👤"),
    ("messages", "Сообщения", "✉️"),
    ("subscriptions", "Подписки", "📅"),
    ("payments", "Платежи", "💰"),
    ("statistics", "Статистика", "📈"),
    ("contents", "Контент", "📋"),
    ("verify-identity", "Проверка", "🔍"),
)
SECTIONS_MAP = {key: (title, icon) for key, title, icon in SECTIONS}
SECTION_PATTERN = "dashboard|users|messages|subscriptions|payments|statistics|contents|verify-identity"


def now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def parse_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw.strip())
    except ValueError:
        return None


def telegram_socks_link(server: str, port: int, username: str, password: str) -> str:
    query = urlencode(
        {
            "server": server,
            "port": port,
            "user": username,
            "pass": password,
        }
    )
    return f"https://t.me/socks?{query}"


def format_ts(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%d.%m.%Y %H:%M UTC")


def _page_template(*, title: str, content: str) -> str:
    return (
        "<!doctype html>"
        "<html lang='ru'>"
        "<head>"
        "<meta charset='utf-8' />"
        "<meta name='viewport' content='width=device-width, initial-scale=1' />"
        f"<title>{escape(title)}</title>"
        "<style>"
        ":root{--bg:#0b1320;--panel:#101d30;--panel2:#0f1a2a;--txt:#e2e8f0;--muted:#8ea3bf;--acc:#00c2ff;--ok:#22c55e;--err:#ef4444;--warn:#f59e0b;--br:#22344d;--chip:#13243a;}"
        "*{box-sizing:border-box;}"
        "html,body{margin:0;padding:0;background:radial-gradient(1200px 500px at 10% -10%,#163056 0%,transparent 55%),radial-gradient(900px 450px at 110% -20%,#0a3a49 0%,transparent 55%),var(--bg);color:var(--txt);font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;}"
        "a{color:inherit;text-decoration:none;}"
        ".layout{display:grid;grid-template-columns:280px 1fr;min-height:100vh;}"
        ".sidebar{border-right:1px solid var(--br);background:linear-gradient(180deg,#101b2c 0%,#0c1524 100%);padding:18px 14px;position:sticky;top:0;height:100vh;}"
        ".brand{display:flex;align-items:center;gap:10px;font-weight:700;font-size:18px;margin-bottom:14px;}"
        ".brand small{display:block;color:var(--muted);font-weight:500;font-size:12px;}"
        ".nav{display:grid;gap:8px;margin-top:12px;}"
        ".nav a{display:flex;align-items:center;gap:10px;padding:11px 12px;border:1px solid var(--br);border-radius:12px;background:var(--panel2);color:#c9d5e5;font-size:14px;}"
        ".nav a.active{background:linear-gradient(180deg,#173256,#102543);border-color:#2d4f7b;color:#fff;box-shadow:0 8px 20px rgba(0,0,0,.25);}"
        ".sidebar .hint{margin-top:14px;color:var(--muted);font-size:12px;line-height:1.45;}"
        ".main{padding:18px 20px 28px;}"
        ".topbar{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px;}"
        ".title{font-size:24px;font-weight:800;letter-spacing:.2px;}"
        ".title small{display:block;font-size:12px;color:var(--muted);font-weight:500;margin-top:4px;}"
        ".flash{padding:10px 12px;border-radius:10px;margin:0 0 12px;border:1px solid transparent;font-size:14px;}"
        ".flash.ok{background:#052e1b;color:#86efac;border-color:#166534;}"
        ".flash.err{background:#3a1111;color:#fecaca;border-color:#7f1d1d;}"
        ".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px;}"
        ".card{background:linear-gradient(180deg,var(--panel) 0%,#0d1929 100%);border:1px solid var(--br);border-radius:14px;padding:14px;box-shadow:0 10px 30px rgba(0,0,0,.25);}"
        ".card h2{margin:0 0 8px;font-size:16px;}"
        ".card h3{margin:0 0 8px;font-size:14px;}"
        ".stat{font-size:13px;color:var(--muted);margin:6px 0;}"
        ".stat b{color:#fff;}"
        ".chip{display:inline-flex;align-items:center;gap:6px;background:var(--chip);border:1px solid var(--br);border-radius:999px;padding:5px 10px;font-size:12px;color:#bfd2e6;}"
        "table{width:100%;border-collapse:collapse;font-size:13px;}"
        "th,td{border-bottom:1px solid var(--br);padding:8px;text-align:left;vertical-align:top;}"
        "th{color:#d3dfef;font-weight:600;background:#0f1d30;}"
        "td{color:#c2d2e5;}"
        "form{display:grid;gap:8px;}"
        "input,textarea,button{font:inherit;}"
        "input,textarea{border:1px solid var(--br);border-radius:10px;padding:10px;background:#0c1828;color:#e2e8f0;}"
        "textarea{min-height:95px;resize:vertical;}"
        "button{border:none;border-radius:10px;padding:10px 12px;background:linear-gradient(180deg,#00b7ff 0%,#0099dd 100%);color:#fff;cursor:pointer;font-weight:600;}"
        "button.alt{background:linear-gradient(180deg,#22c55e 0%,#16a34a 100%);}"
        "button.warn{background:linear-gradient(180deg,#f59e0b 0%,#d97706 100%);}"
        "button.danger{background:linear-gradient(180deg,#ef4444 0%,#dc2626 100%);}"
        "button.ghost{background:#2b3f5d;}"
        "code{background:#16283f;border:1px solid var(--br);padding:2px 6px;border-radius:6px;}"
        ".muted{color:var(--muted);font-size:13px;}"
        ".stack{display:grid;gap:12px;}"
        "@media (max-width:980px){.layout{grid-template-columns:1fr;}.sidebar{height:auto;position:relative;border-right:none;border-bottom:1px solid var(--br);} .main{padding:14px;}}"
        "</style>"
        "</head>"
        "<body>"
        f"{content}"
        "</body>"
        "</html>"
    )


class AdminWebPanel:
    def __init__(
        self,
        *,
        db: Database | PostgresDatabase,
        bot: Bot,
        proxy_public_host: str,
        password: str,
        path: str = "/admin",
        session_ttl_sec: int = 12 * 60 * 60,
    ) -> None:
        clean = path.strip()
        if not clean:
            clean = "/admin"
        if not clean.startswith("/"):
            clean = f"/{clean}"
        self.path = clean.rstrip("/") or "/admin"
        self.db = db
        self.bot = bot
        self.proxy_public_host = proxy_public_host
        self.password = password.strip()
        self.session_ttl_sec = max(3600, int(session_ttl_sec))
        self._sessions: dict[str, int] = {}
        self.enabled = bool(self.password)

    def register(self, app: web.Application) -> None:
        app.add_routes(
            [
                web.get(self.path, self._handle_index),
                web.get(f"{self.path}/", self._handle_index),
                web.get(f"{self.path}/{{section:{SECTION_PATTERN}}}", self._handle_section),
                web.post(f"{self.path}/login", self._handle_login),
                web.post(f"{self.path}/logout", self._handle_logout),
                web.post(f"{self.path}/action/{{action}}", self._handle_action),
            ]
        )

    def _route_for(self, section: str) -> str:
        safe = section if section in SECTIONS_MAP else "dashboard"
        return f"{self.path}/{safe}"

    def _normalize_section(self, section: str | None) -> str:
        if section and section in SECTIONS_MAP:
            return section
        return "dashboard"

    def _purge_sessions(self) -> None:
        ts = now_ts()
        stale = [token for token, exp in self._sessions.items() if exp <= ts]
        for token in stale:
            self._sessions.pop(token, None)

    def _is_authenticated(self, request: web.Request) -> bool:
        if not self.enabled:
            return False
        self._purge_sessions()
        token = request.cookies.get(SESSION_COOKIE_NAME, "")
        if not token:
            return False
        exp = self._sessions.get(token)
        if exp is None or exp <= now_ts():
            self._sessions.pop(token, None)
            return False
        return True

    def _redirect(
        self,
        *,
        section: str = "dashboard",
        message: str = "",
        error: str = "",
        inspect_tg: str = "",
    ) -> web.Response:
        params: dict[str, str] = {}
        if message:
            params["m"] = message
        if error:
            params["e"] = error
        if inspect_tg:
            params["inspect_tg"] = inspect_tg
        query = f"?{urlencode(params)}" if params else ""
        raise web.HTTPFound(f"{self._route_for(section)}{query}")

    async def _handle_login(self, request: web.Request) -> web.Response:
        if not self.enabled:
            raise web.HTTPServiceUnavailable(text="Админ-панель отключена. Укажите ADMIN_PANEL_PASSWORD.")
        form = await request.post()
        password = str(form.get("password") or "").strip()
        if not secrets.compare_digest(password, self.password):
            self._redirect(section="dashboard", error="Неверный пароль.")
        token = secrets.token_urlsafe(32)
        self._sessions[token] = now_ts() + self.session_ttl_sec
        response = web.HTTPFound(self._route_for("dashboard"))
        response.set_cookie(
            SESSION_COOKIE_NAME,
            token,
            max_age=self.session_ttl_sec,
            httponly=True,
            secure=False,
            samesite="Lax",
            path=self.path,
        )
        return response

    async def _handle_logout(self, request: web.Request) -> web.Response:
        token = request.cookies.get(SESSION_COOKIE_NAME, "")
        if token:
            self._sessions.pop(token, None)
        response = web.HTTPFound(self.path)
        response.del_cookie(SESSION_COOKIE_NAME, path=self.path)
        return response

    async def _handle_index(self, request: web.Request) -> web.Response:
        if not self.enabled:
            return web.Response(
                text=_page_template(
                    title="Админ-панель отключена",
                    content=(
                        "<div class='layout'><main class='main'>"
                        "<div class='card'><h1>Админ-панель отключена</h1>"
                        "<p class='muted'>Укажите <code>ADMIN_PANEL_PASSWORD</code>, чтобы включить веб-админку.</p>"
                        "</div></main></div>"
                    ),
                ),
                content_type="text/html",
            )
        if not self._is_authenticated(request):
            return web.Response(text=self._render_login(error=str(request.query.get("e") or "")), content_type="text/html")
        section = self._normalize_section(str(request.query.get("section") or "dashboard"))
        self._redirect(section=section)
        return web.Response(text="")

    async def _handle_section(self, request: web.Request) -> web.Response:
        section = self._normalize_section(str(request.match_info.get("section") or "dashboard"))
        if not self._is_authenticated(request):
            return web.Response(text=self._render_login(error=str(request.query.get("e") or "")), content_type="text/html")
        message = str(request.query.get("m") or "").strip()
        error = str(request.query.get("e") or "").strip()
        inspect_tg = parse_int(str(request.query.get("inspect_tg") or "").strip())
        html = await self._render_section_page(
            section=section,
            message=message,
            error=error,
            inspect_tg=inspect_tg,
        )
        return web.Response(text=html, content_type="text/html")

    async def _handle_action(self, request: web.Request) -> web.Response:
        if not self._is_authenticated(request):
            raise web.HTTPFound(self.path)
        action = str(request.match_info.get("action") or "").strip()
        form = await request.post()
        section = self._normalize_section(str(form.get("section") or "dashboard"))
        inspect_tg = str(form.get("inspect_tg") or "").strip()
        try:
            if action == "broadcast_all":
                message = await self._action_broadcast_all(form)
                self._redirect(section=section, message=message, inspect_tg=inspect_tg)
            if action == "broadcast_user":
                message = await self._action_broadcast_user(form)
                self._redirect(section=section, message=message, inspect_tg=inspect_tg)
            if action == "ban":
                message = await self._action_ban(form)
                self._redirect(section=section, message=message, inspect_tg=inspect_tg)
            if action == "unban":
                message = await self._action_unban(form)
                self._redirect(section=section, message=message, inspect_tg=inspect_tg)
            if action == "grant_proxies":
                message = await self._action_grant_proxies(form)
                self._redirect(section=section, message=message, inspect_tg=inspect_tg)
            if action == "remove_proxies":
                message = await self._action_remove_proxies(form)
                self._redirect(section=section, message=message, inspect_tg=inspect_tg)
            if action == "referral_debit":
                message = await self._action_referral_debit(form)
                self._redirect(section=section, message=message, inspect_tg=inspect_tg)
            self._redirect(section=section, error=f"Неизвестное действие: {action}", inspect_tg=inspect_tg)
        except ValueError as exc:
            self._redirect(section=section, error=str(exc), inspect_tg=inspect_tg)
        return web.Response(text="")

    async def _action_broadcast_all(self, form: Any) -> str:
        text = str(form.get("text") or "").strip()
        media_kind, media_bytes, media_filename = self._extract_media(form)
        if not text and media_kind is None:
            raise ValueError("Введите текст или прикрепите фото/видео.")
        targets = await self.db.get_all_tg_user_ids()
        sent_ok = 0
        sent_fail = 0
        for tg_user_id in targets:
            ok = await self._send_payload(
                tg_user_id=int(tg_user_id),
                text=text,
                media_kind=media_kind,
                media_bytes=media_bytes,
                media_filename=media_filename,
            )
            if ok:
                sent_ok += 1
            else:
                sent_fail += 1
        return f"Рассылка всем завершена. Успешно: {sent_ok}, ошибок: {sent_fail}."

    async def _action_broadcast_user(self, form: Any) -> str:
        tg_user_id = parse_int(str(form.get("tg_user_id") or ""))
        text = str(form.get("text") or "").strip()
        media_kind, media_bytes, media_filename = self._extract_media(form)
        if tg_user_id is None or tg_user_id <= 0:
            raise ValueError("tg_user_id должен быть положительным числом.")
        if not text and media_kind is None:
            raise ValueError("Введите текст или прикрепите фото/видео.")
        ok = await self._send_payload(
            tg_user_id=tg_user_id,
            text=text,
            media_kind=media_kind,
            media_bytes=media_bytes,
            media_filename=media_filename,
        )
        if not ok:
            raise ValueError("Не удалось отправить сообщение пользователю.")
        return f"Сообщение отправлено пользователю {tg_user_id}."

    def _extract_media(self, form: Any) -> tuple[str | None, bytes | None, str | None]:
        media = form.get("media")
        if media is None:
            return None, None, None

        file_obj = getattr(media, "file", None)
        filename_raw = str(getattr(media, "filename", "") or "").strip()
        if file_obj is None or not filename_raw:
            return None, None, None

        content_type = str(getattr(media, "content_type", "") or "").lower()
        filename = os.path.basename(filename_raw)
        extension = os.path.splitext(filename)[1].lower()

        media_kind: str | None = None
        if content_type.startswith("image/") or extension in {".jpg", ".jpeg", ".png", ".webp"}:
            media_kind = "photo"
        elif content_type.startswith("video/") or extension in {".mp4", ".mov", ".mkv", ".webm"}:
            media_kind = "video"
        if media_kind is None:
            raise ValueError("Поддерживаются только фото и видео.")

        media_bytes = file_obj.read()
        if not media_bytes:
            raise ValueError("Загруженный файл пустой.")
        return media_kind, media_bytes, filename

    async def _send_payload(
        self,
        *,
        tg_user_id: int,
        text: str,
        media_kind: str | None,
        media_bytes: bytes | None,
        media_filename: str | None,
    ) -> bool:
        try:
            if media_kind == "photo":
                if media_bytes is None:
                    return False
                photo = BufferedInputFile(media_bytes, filename=media_filename or "image.jpg")
                await self.bot.send_photo(
                    tg_user_id,
                    photo=photo,
                    caption=text or None,
                    parse_mode=None,
                )
                return True
            if media_kind == "video":
                if media_bytes is None:
                    return False
                video = BufferedInputFile(media_bytes, filename=media_filename or "video.mp4")
                await self.bot.send_video(
                    tg_user_id,
                    video=video,
                    caption=text or None,
                    parse_mode=None,
                )
                return True
            await self.bot.send_message(tg_user_id, text, parse_mode=None)
            return True
        except (TelegramBadRequest, TelegramForbiddenError):
            return False

    async def _action_ban(self, form: Any) -> str:
        tg_user_id = parse_int(str(form.get("tg_user_id") or ""))
        reason = str(form.get("reason") or "").strip() or DEFAULT_BAN_TEXT
        if tg_user_id is None or tg_user_id <= 0:
            raise ValueError("tg_user_id должен быть положительным числом.")
        await self.db.ban_user(tg_user_id=tg_user_id, reason=reason, blocked_by=None)
        try:
            await self.bot.send_message(tg_user_id, reason, parse_mode=None)
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        return f"Пользователь {tg_user_id} заблокирован."

    async def _action_unban(self, form: Any) -> str:
        tg_user_id = parse_int(str(form.get("tg_user_id") or ""))
        if tg_user_id is None or tg_user_id <= 0:
            raise ValueError("tg_user_id должен быть положительным числом.")
        if tg_user_id == BLOCKED_TG_USER_ID:
            raise ValueError("Этого пользователя нельзя разбанить из панели.")
        changed = await self.db.unban_user(tg_user_id)
        return (
            f"Пользователь {tg_user_id} разблокирован."
            if changed
            else f"Пользователь {tg_user_id} не был в бане."
        )

    async def _action_grant_proxies(self, form: Any) -> str:
        tg_user_id = parse_int(str(form.get("tg_user_id") or ""))
        devices_count = parse_int(str(form.get("devices_count") or ""))
        days = parse_int(str(form.get("days") or "30"))
        if tg_user_id is None or tg_user_id <= 0:
            raise ValueError("tg_user_id должен быть положительным числом.")
        if devices_count is None or devices_count < 1:
            raise ValueError("Количество прокси должно быть больше 0.")
        if days is None or days < 1 or days > 3650:
            raise ValueError("Дни должны быть в диапазоне 1..3650.")

        user_row = await self.db.get_user_by_tg_user_id(tg_user_id)
        if user_row is None:
            raise ValueError("Пользователь не найден.")
        user_id = int(user_row["id"])

        plans = await self.db.get_plans()
        plan = next((item for item in plans if item.devices_count == devices_count), None)
        if plan is None:
            raise ValueError("Не найден подходящий тариф для выбранного количества.")

        payment_id = await self.db.create_payment(user_id=user_id, plan_code=plan.code, amount_rub=0)
        expires_at = int((datetime.now(tz=timezone.utc) + timedelta(days=days)).timestamp())
        activated = await self.db.activate_payment_and_create_subscription_from_pool(
            payment_id=payment_id,
            payer_user_id=user_id,
            recipient_user_id=user_id,
            plan_code=plan.code,
            expires_at=expires_at,
            devices_count=devices_count,
            proxy_public_host=self.proxy_public_host,
        )
        if activated is None:
            free_count = await self.db.count_free_pool()
            raise ValueError(f"Не удалось начислить прокси. Свободно в пуле: {free_count}.")
        subscription_id, created_proxies = activated

        lines = [
            f"Админ начислил вам {devices_count} прокси.",
            f"Подписка: #{subscription_id}",
            "",
        ]
        for item in created_proxies:
            port = int(item["port"])
            username = str(item["username"])
            password = str(item["password"])
            proxy_id = int(item["proxy_id"])
            tg_link = telegram_socks_link(self.proxy_public_host, port, username, password)
            lines.append(f"Proxy ID: {proxy_id}")
            lines.append(tg_link)
            lines.append("")
        payload = "\n".join(lines).strip()
        try:
            await self.bot.send_message(tg_user_id, payload, parse_mode=None, disable_web_page_preview=True)
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        return f"Начислено {devices_count} прокси пользователю {tg_user_id}."

    async def _action_remove_proxies(self, form: Any) -> str:
        tg_user_id = parse_int(str(form.get("tg_user_id") or ""))
        proxy_token = str(form.get("proxy_id") or "").strip().lower()
        if tg_user_id is None or tg_user_id <= 0:
            raise ValueError("tg_user_id должен быть положительным числом.")
        if not proxy_token:
            raise ValueError("Укажите proxy_id или all.")

        user_row = await self.db.get_user_by_tg_user_id(tg_user_id)
        if user_row is None:
            raise ValueError("Пользователь не найден.")
        user_id = int(user_row["id"])

        removed_count = 0
        if proxy_token == "all":
            removed_count = await self.db.revoke_all_active_links_for_user(user_id)
        else:
            proxy_id = parse_int(proxy_token)
            if proxy_id is None:
                raise ValueError("proxy_id должен быть числом или all.")
            removed = await self.db.revoke_proxy_link_for_user(user_id, proxy_id)
            removed_count = 1 if removed else 0

        if removed_count > 0:
            try:
                await self.bot.send_message(
                    tg_user_id,
                    "Часть ваших прокси была деактивирована администратором.",
                )
            except (TelegramBadRequest, TelegramForbiddenError):
                pass
        return f"Удалено прокси: {removed_count}."

    async def _action_referral_debit(self, form: Any) -> str:
        tg_user_id = parse_int(str(form.get("tg_user_id") or ""))
        amount_rub = parse_int(str(form.get("amount_rub") or ""))
        comment = str(form.get("comment") or "").strip()
        if tg_user_id is None or tg_user_id <= 0:
            raise ValueError("tg_user_id должен быть положительным числом.")
        if amount_rub is None or amount_rub <= 0:
            raise ValueError("Сумма должна быть положительным числом.")
        changed, new_balance = await self.db.debit_referral_balance_by_tg_user_id(
            tg_user_id=tg_user_id,
            amount_rub=amount_rub,
            comment=comment or "admin web panel",
        )
        if not changed:
            raise ValueError(f"Не удалось списать. Доступный баланс: {new_balance}₽.")
        return f"Списано {amount_rub}₽ у пользователя {tg_user_id}. Новый баланс: {new_balance}₽."

    def _render_login(self, *, error: str = "") -> str:
        flash = f"<div class='flash err'>{escape(error)}</div>" if error else ""
        body = (
            "<div class='layout'><main class='main' style='max-width:520px;margin:80px auto;'>"
            "<div class='card'>"
            "<h1 style='margin:0 0 10px;'>🔑 Вход</h1>"
            "<p class='muted'>Вход в веб-админку whiteprox.</p>"
            f"{flash}"
            f"<form method='post' action='{escape(self.path)}/login'>"
            "<input type='password' name='password' placeholder='Пароль' required />"
            "<button type='submit'>Войти</button>"
            "</form>"
            "</div></main></div>"
        )
        return _page_template(title="Вход в админ-панель", content=body)

    def _render_sidebar(self, *, section: str) -> str:
        links: list[str] = []
        for key, title, icon in SECTIONS:
            cls = "active" if key == section else ""
            links.append(
                f"<a class='{cls}' href='{escape(self._route_for(key))}'>"
                f"<span>{escape(icon)}</span><span>{escape(title)}</span>"
                "</a>"
            )
        return (
            "<aside class='sidebar'>"
            "<div class='brand'>"
            "<div style='font-size:24px;'>🛡️</div>"
            "<div>"
            "WhiteProxy Админка"
            "<small>в стиле разделов TeleAdminPanel</small>"
            "</div>"
            "</div>"
            f"<nav class='nav'>{''.join(links)}</nav>"
            "<p class='hint'>Панель для модерации, рассылок и управления жизненным циклом прокси.</p>"
            "</aside>"
        )

    def _render_flash(self, *, message: str, error: str) -> str:
        out = ""
        if message:
            out += f"<div class='flash ok'>{escape(message)}</div>"
        if error:
            out += f"<div class='flash err'>{escape(error)}</div>"
        return out

    def _hidden_inputs(self, *, section: str, inspect_tg: int | None) -> str:
        inspect_value = "" if inspect_tg is None else str(inspect_tg)
        return (
            f"<input type='hidden' name='section' value='{escape(section)}' />"
            f"<input type='hidden' name='inspect_tg' value='{escape(inspect_value)}' />"
        )

    def _users_table_html(self, users: list[dict[str, Any]]) -> str:
        rows: list[str] = []
        for row in users:
            tg_user_id = int(row["tg_user_id"])
            username = f"@{row['username']}" if row.get("username") else "-"
            active_count = int(row.get("active_proxies") or 0)
            banned_flag = int(row.get("is_banned") or 0) == 1 or tg_user_id == BLOCKED_TG_USER_ID
            rows.append(
                "<tr>"
                f"<td>{tg_user_id}</td>"
                f"<td>{escape(username)}</td>"
                f"<td>{active_count}</td>"
                f"<td>{'да' if banned_flag else 'нет'}</td>"
                "</tr>"
            )
        return (
            "<table><thead><tr>"
            "<th>tg_user_id</th><th>username</th><th>Активных прокси</th><th>Бан</th>"
            "</tr></thead><tbody>"
            + ("".join(rows) if rows else "<tr><td colspan='4'>Пользователей пока нет.</td></tr>")
            + "</tbody></table>"
        )

    def _inspect_links_html(
        self,
        *,
        inspect_tg: int | None,
        user_row: dict[str, Any] | None,
        ban_row: dict[str, Any] | None,
        links: list[dict[str, Any]],
    ) -> str:
        if inspect_tg is None or inspect_tg <= 0:
            return ""
        if user_row is None:
            return "<div class='card'><h2>Прокси пользователя</h2><p class='muted'>Пользователь не найден.</p></div>"

        rows: list[str] = []
        for row in links:
            rows.append(
                "<tr>"
                f"<td>{int(row['id'])}</td>"
                f"<td>{int(row['subscription_id'])}</td>"
                f"<td>{int(row['device_number'])}</td>"
                f"<td>{escape(str(row['status']))}</td>"
                f"<td>{escape(format_ts(int(row['expires_at'])))}</td>"
                f"<td><code>{escape(str(row['link']))}</code></td>"
                "</tr>"
            )
        table = (
            "<table><thead><tr>"
            "<th>ID</th><th>Sub</th><th>Device</th><th>Status</th><th>Expires</th><th>Link</th>"
            "</tr></thead><tbody>"
            + ("".join(rows) if rows else "<tr><td colspan='6'>Прокси отсутствуют.</td></tr>")
            + "</tbody></table>"
        )
        banned = ban_row is not None or inspect_tg == BLOCKED_TG_USER_ID
        return (
            "<div class='card'>"
            "<h2>Прокси пользователя</h2>"
            f"<p class='stat'>tg_user_id: <b>{inspect_tg}</b> | бан: <b>{'да' if banned else 'нет'}</b></p>"
            f"{table}"
            "</div>"
        )

    async def _render_section_page(
        self,
        *,
        section: str,
        message: str,
        error: str,
        inspect_tg: int | None,
    ) -> str:
        referral = await self.db.get_referral_admin_summary()
        free_pool = await self.db.count_free_pool()
        users = await self.db.list_users_with_stats(limit=500, offset=0)

        inspect_user_row: dict[str, Any] | None = None
        inspect_ban_row: dict[str, Any] | None = None
        inspect_links: list[dict[str, Any]] = []
        if inspect_tg is not None and inspect_tg > 0:
            inspect_user_row = await self.db.get_user_by_tg_user_id(inspect_tg)
            if inspect_user_row is not None:
                inspect_ban_row = await self.db.get_user_ban(inspect_tg)
                inspect_links = await self.db.get_all_links_for_user(int(inspect_user_row["id"]))

        total_users = len(users)
        total_active_proxies = sum(int(row.get("active_proxies") or 0) for row in users)
        total_banned = sum(
            1
            for row in users
            if int(row.get("is_banned") or 0) == 1 or int(row["tg_user_id"]) == BLOCKED_TG_USER_ID
        )

        title, icon = SECTIONS_MAP.get(section, ("Дашборд", "📊"))
        flash = self._render_flash(message=message, error=error)
        hidden = self._hidden_inputs(section=section, inspect_tg=inspect_tg)

        if section == "dashboard":
            section_content = (
                "<div class='grid'>"
                "<div class='card'><h2>Система</h2>"
                f"<p class='stat'>Пользователей в выборке: <b>{total_users}</b></p>"
                f"<p class='stat'>Активных прокси: <b>{total_active_proxies}</b></p>"
                f"<p class='stat'>Заблокированных: <b>{total_banned}</b></p>"
                f"<p class='stat'>Свободных в пуле: <b>{int(free_pool)}</b></p></div>"
                "<div class='card'><h2>Рефералы</h2>"
                f"<p class='stat'>Пользователей с реферером: <b>{int(referral.get('users_with_referrer', 0))}</b></p>"
                f"<p class='stat'>Начислено всего: <b>{int(referral.get('total_earned_rub', 0))}₽</b></p>"
                f"<p class='stat'>Списано всего: <b>{int(referral.get('total_debited_rub', 0))}₽</b></p>"
                f"<p class='stat'>Текущий реф. баланс: <b>{int(referral.get('total_balance_rub', 0))}₽</b></p>"
                "</div>"
                "<div class='card'><h2>Быстрая проверка</h2>"
                f"<form method='get' action='{escape(self._route_for('users'))}'>"
                "<input name='inspect_tg' placeholder='tg_user_id' required />"
                "<button class='alt' type='submit'>Открыть прокси пользователя</button>"
                "</form></div>"
                "</div>"
            )

        elif section == "users":
            section_content = (
                "<div class='stack'>"
                "<div class='card'><h2>Список пользователей</h2>"
                f"{self._users_table_html(users)}"
                "</div>"
                "<div class='card'><h2>Проверка прокси пользователя</h2>"
                f"<form method='get' action='{escape(self._route_for('users'))}'>"
                "<input name='inspect_tg' placeholder='tg_user_id' required />"
                "<button type='submit' class='alt'>Проверить</button>"
                "</form></div>"
                f"{self._inspect_links_html(inspect_tg=inspect_tg, user_row=inspect_user_row, ban_row=inspect_ban_row, links=inspect_links)}"
                "</div>"
            )

        elif section == "messages":
            section_content = (
                "<div class='grid'>"
                "<div class='card'><h2>Рассылка всем</h2>"
                f"<form method='post' enctype='multipart/form-data' action='{escape(self.path)}/action/broadcast_all'>"
                f"{hidden}"
                "<textarea name='text' placeholder='Текст/подпись (необязательно, если есть медиа)'></textarea>"
                "<input type='file' name='media' accept='image/*,video/*' />"
                "<button type='submit'>Отправить всем</button></form></div>"
                "<div class='card'><h2>Рассылка пользователю</h2>"
                f"<form method='post' enctype='multipart/form-data' action='{escape(self.path)}/action/broadcast_user'>"
                f"{hidden}"
                "<input name='tg_user_id' placeholder='tg_user_id' required />"
                "<textarea name='text' placeholder='Текст/подпись (необязательно, если есть медиа)'></textarea>"
                "<input type='file' name='media' accept='image/*,video/*' />"
                "<button type='submit' class='alt'>Отправить пользователю</button></form></div>"
                "</div>"
            )

        elif section == "subscriptions":
            section_content = (
                "<div class='grid'>"
                "<div class='card'><h2>Начислить прокси</h2>"
                f"<form method='post' action='{escape(self.path)}/action/grant_proxies'>"
                f"{hidden}"
                "<input name='tg_user_id' placeholder='tg_user_id' required />"
                "<input name='devices_count' placeholder='кол-во прокси (например 1)' required />"
                "<input name='days' placeholder='дней (по умолчанию 30)' />"
                "<button class='alt' type='submit'>Начислить</button></form></div>"
                "<div class='card'><h2>Удалить прокси</h2>"
                f"<form method='post' action='{escape(self.path)}/action/remove_proxies'>"
                f"{hidden}"
                "<input name='tg_user_id' placeholder='tg_user_id' required />"
                "<input name='proxy_id' placeholder='proxy_id или all' required />"
                "<button class='danger' type='submit'>Удалить</button></form></div>"
                "</div>"
            )

        elif section == "payments":
            section_content = (
                "<div class='grid'>"
                "<div class='card'><h2>Реферальные финансы</h2>"
                f"<p class='stat'>Начислено: <b>{int(referral.get('total_earned_rub', 0))}₽</b></p>"
                f"<p class='stat'>Списано: <b>{int(referral.get('total_debited_rub', 0))}₽</b></p>"
                f"<p class='stat'>Текущий баланс: <b>{int(referral.get('total_balance_rub', 0))}₽</b></p>"
                "</div>"
                "<div class='card'><h2>Списание рефералки</h2>"
                f"<form method='post' action='{escape(self.path)}/action/referral_debit'>"
                f"{hidden}"
                "<input name='tg_user_id' placeholder='tg_user_id' required />"
                "<input name='amount_rub' placeholder='сумма ₽' required />"
                "<input name='comment' placeholder='комментарий (необязательно)' />"
                "<button class='warn' type='submit'>Списать</button></form></div>"
                "</div>"
            )

        elif section == "statistics":
            section_content = (
                "<div class='grid'>"
                "<div class='card'><h2>Глобальные метрики</h2>"
                f"<p class='stat'>Всего пользователей: <b>{total_users}</b></p>"
                f"<p class='stat'>Всего активных прокси: <b>{total_active_proxies}</b></p>"
                f"<p class='stat'>Заблокированных: <b>{total_banned}</b></p>"
                f"<p class='stat'>Свободных прокси в пуле: <b>{int(free_pool)}</b></p>"
                "</div>"
                "<div class='card'><h2>Реферальные метрики</h2>"
                f"<p class='stat'>Пользователей с реферером: <b>{int(referral.get('users_with_referrer', 0))}</b></p>"
                f"<p class='stat'>Начислено всего: <b>{int(referral.get('total_earned_rub', 0))}₽</b></p>"
                f"<p class='stat'>Списано всего: <b>{int(referral.get('total_debited_rub', 0))}₽</b></p>"
                "</div>"
                "</div>"
            )

        elif section == "contents":
            section_content = (
                "<div class='stack'>"
                "<div class='card'><h2>Прокси-контент (ссылки)</h2>"
                "<p class='muted'>Раздел для просмотра выданных прокси-ссылок по пользователю.</p>"
                f"<form method='get' action='{escape(self._route_for('contents'))}'>"
                "<input name='inspect_tg' placeholder='tg_user_id' required />"
                "<button class='alt' type='submit'>Загрузить ссылки пользователя</button>"
                "</form></div>"
                f"{self._inspect_links_html(inspect_tg=inspect_tg, user_row=inspect_user_row, ban_row=inspect_ban_row, links=inspect_links)}"
                "</div>"
            )

        else:
            section_content = (
                "<div class='grid'>"
                "<div class='card'><h2>Заблокировать пользователя</h2>"
                f"<form method='post' action='{escape(self.path)}/action/ban'>"
                f"{hidden}"
                "<input name='tg_user_id' placeholder='tg_user_id' required />"
                "<textarea name='reason' placeholder='Причина (необязательно)'></textarea>"
                "<button class='danger' type='submit'>Заблокировать</button></form></div>"
                "<div class='card'><h2>Разблокировать пользователя</h2>"
                f"<form method='post' action='{escape(self.path)}/action/unban'>"
                f"{hidden}"
                "<input name='tg_user_id' placeholder='tg_user_id' required />"
                "<button class='alt' type='submit'>Разблокировать</button></form></div>"
                "</div>"
            )

        body = (
            "<div class='layout'>"
            f"{self._render_sidebar(section=section)}"
            "<main class='main'>"
            "<div class='topbar'>"
            f"<form method='post' action='{escape(self.path)}/logout'><button class='ghost' type='submit'>Выйти</button></form>"
            "</div>"
            f"{flash}"
            f"{section_content}"
            "</main>"
            "</div>"
        )
        return _page_template(title=f"WhiteProxy Админка • {title}", content=body)
