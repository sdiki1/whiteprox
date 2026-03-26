from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html import escape
import secrets
from typing import Any
from urllib.parse import urlencode

from aiohttp import web
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

from .database import Database
from .database_postgres import PostgresDatabase


DEFAULT_BAN_TEXT = "Доступ к боту ограничен администратором."
BLOCKED_TG_USER_ID = 1664076316
SESSION_COOKIE_NAME = "whiteprox_admin_session"


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
        ":root{--bg:#f6f7fb;--panel:#fff;--txt:#0f172a;--muted:#64748b;--acc:#0ea5e9;--ok:#16a34a;--err:#dc2626;--br:#e2e8f0;}"
        "html,body{margin:0;padding:0;background:var(--bg);color:var(--txt);font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;}"
        ".wrap{max-width:1200px;margin:24px auto;padding:0 16px;}"
        ".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px;}"
        ".card{background:var(--panel);border:1px solid var(--br);border-radius:14px;padding:14px;box-shadow:0 6px 20px rgba(15,23,42,.04);}"
        ".head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px;}"
        "h1{font-size:24px;margin:0;}h2{font-size:16px;margin:0 0 8px;}h3{font-size:14px;margin:0 0 8px;}"
        "p{margin:6px 0;color:var(--muted);}small{color:var(--muted);}"
        "form{display:grid;gap:8px;}"
        "input,textarea,button{font:inherit;}"
        "input,textarea{border:1px solid var(--br);border-radius:10px;padding:10px;background:#fff;}"
        "textarea{min-height:90px;resize:vertical;}"
        "button{border:none;border-radius:10px;padding:10px 12px;background:var(--acc);color:#fff;cursor:pointer;}"
        "button.alt{background:#0f766e;}button.danger{background:var(--err);}button.ghost{background:#475569;}"
        ".flash{padding:10px 12px;border-radius:10px;margin:0 0 12px;}"
        ".flash.ok{background:#ecfdf5;color:#166534;border:1px solid #bbf7d0;}"
        ".flash.err{background:#fef2f2;color:#991b1b;border:1px solid #fecaca;}"
        "table{width:100%;border-collapse:collapse;font-size:13px;}"
        "th,td{border-bottom:1px solid var(--br);padding:8px;text-align:left;vertical-align:top;}"
        "th{color:#334155;font-weight:600;background:#f8fafc;}"
        "code{background:#f1f5f9;padding:2px 6px;border-radius:6px;}"
        ".row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;}"
        ".row>*{flex:1;min-width:120px;}"
        ".mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;}"
        "@media (max-width:640px){.wrap{margin:10px auto;}.card{padding:12px;}}"
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
                web.post(f"{self.path}/login", self._handle_login),
                web.post(f"{self.path}/logout", self._handle_logout),
                web.post(f"{self.path}/action/{{action}}", self._handle_action),
            ]
        )

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

    def _redirect(self, *, message: str = "", error: str = "", inspect_tg: str = "") -> web.Response:
        params: dict[str, str] = {}
        if message:
            params["m"] = message
        if error:
            params["e"] = error
        if inspect_tg:
            params["inspect_tg"] = inspect_tg
        query = f"?{urlencode(params)}" if params else ""
        raise web.HTTPFound(f"{self.path}{query}")

    async def _handle_login(self, request: web.Request) -> web.Response:
        if not self.enabled:
            raise web.HTTPServiceUnavailable(text="Admin panel disabled. Set ADMIN_PANEL_PASSWORD.")
        form = await request.post()
        password = str(form.get("password") or "").strip()
        if not secrets.compare_digest(password, self.password):
            self._redirect(error="Неверный пароль.")
        token = secrets.token_urlsafe(32)
        self._sessions[token] = now_ts() + self.session_ttl_sec
        response = web.HTTPFound(self.path)
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
                    title="Admin Panel Disabled",
                    content=(
                        "<div class='wrap'><div class='card'>"
                        "<h1>Admin Panel Disabled</h1>"
                        "<p>Set <code>ADMIN_PANEL_PASSWORD</code> to enable web admin panel.</p>"
                        "</div></div>"
                    ),
                ),
                content_type="text/html",
            )

        if not self._is_authenticated(request):
            return web.Response(text=self._render_login(error=str(request.query.get("e") or "")), content_type="text/html")

        message = str(request.query.get("m") or "").strip()
        error = str(request.query.get("e") or "").strip()
        inspect_tg_raw = str(request.query.get("inspect_tg") or "").strip()
        inspect_tg = parse_int(inspect_tg_raw)
        html = await self._render_dashboard(message=message, error=error, inspect_tg=inspect_tg)
        return web.Response(text=html, content_type="text/html")

    async def _handle_action(self, request: web.Request) -> web.Response:
        if not self._is_authenticated(request):
            raise web.HTTPFound(self.path)
        action = str(request.match_info.get("action") or "").strip()
        form = await request.post()
        inspect_tg = str(form.get("inspect_tg") or "").strip()
        try:
            if action == "broadcast_all":
                message = await self._action_broadcast_all(form)
                self._redirect(message=message, inspect_tg=inspect_tg)
            if action == "broadcast_user":
                message = await self._action_broadcast_user(form)
                self._redirect(message=message, inspect_tg=inspect_tg)
            if action == "ban":
                message = await self._action_ban(form)
                self._redirect(message=message, inspect_tg=inspect_tg)
            if action == "unban":
                message = await self._action_unban(form)
                self._redirect(message=message, inspect_tg=inspect_tg)
            if action == "grant_proxies":
                message = await self._action_grant_proxies(form)
                self._redirect(message=message, inspect_tg=inspect_tg)
            if action == "remove_proxies":
                message = await self._action_remove_proxies(form)
                self._redirect(message=message, inspect_tg=inspect_tg)
            if action == "referral_debit":
                message = await self._action_referral_debit(form)
                self._redirect(message=message, inspect_tg=inspect_tg)
            self._redirect(error=f"Неизвестное действие: {action}", inspect_tg=inspect_tg)
        except ValueError as exc:
            self._redirect(error=str(exc), inspect_tg=inspect_tg)
        return web.Response(text="")

    async def _action_broadcast_all(self, form: Any) -> str:
        text = str(form.get("text") or "").strip()
        if not text:
            raise ValueError("Введите текст рассылки.")
        targets = await self.db.get_all_tg_user_ids()
        sent_ok = 0
        sent_fail = 0
        for tg_user_id in targets:
            try:
                await self.bot.send_message(int(tg_user_id), text, parse_mode=None)
                sent_ok += 1
            except (TelegramBadRequest, TelegramForbiddenError):
                sent_fail += 1
        return f"Рассылка всем завершена. Успешно: {sent_ok}, ошибок: {sent_fail}."

    async def _action_broadcast_user(self, form: Any) -> str:
        tg_user_id = parse_int(str(form.get("tg_user_id") or ""))
        text = str(form.get("text") or "").strip()
        if tg_user_id is None or tg_user_id <= 0:
            raise ValueError("tg_user_id должен быть положительным числом.")
        if not text:
            raise ValueError("Введите текст сообщения.")
        try:
            await self.bot.send_message(tg_user_id, text, parse_mode=None)
        except (TelegramBadRequest, TelegramForbiddenError):
            raise ValueError("Не удалось отправить сообщение пользователю.")
        return f"Сообщение отправлено пользователю {tg_user_id}."

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
            "<div class='wrap'>"
            "<div class='card' style='max-width:420px;margin:80px auto;'>"
            "<h1>Web Admin</h1>"
            "<p>Войдите для управления ботом.</p>"
            f"{flash}"
            f"<form method='post' action='{escape(self.path)}/login'>"
            "<input type='password' name='password' placeholder='Пароль админ-панели' required />"
            "<button type='submit'>Войти</button>"
            "</form>"
            "</div>"
            "</div>"
        )
        return _page_template(title="Web Admin Login", content=body)

    async def _render_dashboard(self, *, message: str = "", error: str = "", inspect_tg: int | None = None) -> str:
        referral = await self.db.get_referral_admin_summary()
        free_pool = await self.db.count_free_pool()
        users = await self.db.list_users_with_stats(limit=500, offset=0)

        inspect_block = ""
        inspect_form_value = str(inspect_tg) if inspect_tg is not None else ""
        if inspect_tg is not None and inspect_tg > 0:
            user_row = await self.db.get_user_by_tg_user_id(inspect_tg)
            if user_row is None:
                inspect_block = "<div class='card'><h2>Прокси пользователя</h2><p>Пользователь не найден.</p></div>"
            else:
                user_id = int(user_row["id"])
                ban = await self.db.get_user_ban(inspect_tg)
                links = await self.db.get_all_links_for_user(user_id)
                rows: list[str] = []
                for row in links:
                    rows.append(
                        "<tr>"
                        f"<td>{int(row['id'])}</td>"
                        f"<td>{int(row['subscription_id'])}</td>"
                        f"<td>{int(row['device_number'])}</td>"
                        f"<td>{escape(str(row['status']))}</td>"
                        f"<td>{escape(format_ts(int(row['expires_at'])))}</td>"
                        f"<td class='mono'>{escape(str(row['link']))}</td>"
                        "</tr>"
                    )
                table = (
                    "<table><thead><tr>"
                    "<th>ID</th><th>Sub</th><th>Device</th><th>Status</th><th>Expires</th><th>Link</th>"
                    "</tr></thead><tbody>"
                    + ("".join(rows) if rows else "<tr><td colspan='6'>Прокси отсутствуют.</td></tr>")
                    + "</tbody></table>"
                )
                inspect_block = (
                    "<div class='card'>"
                    "<h2>Прокси пользователя</h2>"
                    f"<p>tg_user_id: <code>{inspect_tg}</code>, бан: <b>{'да' if ban is not None or inspect_tg == BLOCKED_TG_USER_ID else 'нет'}</b></p>"
                    f"{table}"
                    "</div>"
                )

        user_rows: list[str] = []
        for row in users:
            tg_user_id = int(row["tg_user_id"])
            username = f"@{row['username']}" if row.get("username") else "-"
            active_count = int(row.get("active_proxies") or 0)
            banned_flag = int(row.get("is_banned") or 0) == 1 or tg_user_id == BLOCKED_TG_USER_ID
            user_rows.append(
                "<tr>"
                f"<td>{tg_user_id}</td>"
                f"<td>{escape(username)}</td>"
                f"<td>{active_count}</td>"
                f"<td>{'да' if banned_flag else 'нет'}</td>"
                "</tr>"
            )
        users_table = (
            "<table><thead><tr>"
            "<th>tg_user_id</th><th>username</th><th>Активных прокси</th><th>Бан</th>"
            "</tr></thead><tbody>"
            + ("".join(user_rows) if user_rows else "<tr><td colspan='4'>Пользователей пока нет.</td></tr>")
            + "</tbody></table>"
        )

        flash = ""
        if message:
            flash += f"<div class='flash ok'>{escape(message)}</div>"
        if error:
            flash += f"<div class='flash err'>{escape(error)}</div>"

        body = (
            "<div class='wrap'>"
            "<div class='head'>"
            "<h1>Web Admin Panel</h1>"
            f"<form method='post' action='{escape(self.path)}/logout'><button class='ghost' type='submit'>Выйти</button></form>"
            "</div>"
            f"{flash}"
            "<div class='grid'>"
            "<div class='card'>"
            "<h2>Рефералы</h2>"
            f"<p>Пользователей с реферером: <b>{int(referral.get('users_with_referrer', 0))}</b></p>"
            f"<p>Начислено всего: <b>{int(referral.get('total_earned_rub', 0))}₽</b></p>"
            f"<p>Списано/выведено: <b>{int(referral.get('total_debited_rub', 0))}₽</b></p>"
            f"<p>Текущий реф. баланс: <b>{int(referral.get('total_balance_rub', 0))}₽</b></p>"
            "</div>"
            "<div class='card'>"
            "<h2>Система</h2>"
            f"<p>Свободно прокси в пуле: <b>{int(free_pool)}</b></p>"
            f"<p>Пользователей в выборке: <b>{len(users)}</b></p>"
            "<p><small>Последние 500 пользователей из БД.</small></p>"
            "</div>"
            "<div class='card'>"
            "<h2>Прокси пользователя</h2>"
            f"<form method='get' action='{escape(self.path)}'>"
            "<input name='inspect_tg' placeholder='tg_user_id' required />"
            "<button type='submit' class='alt'>Открыть</button>"
            "</form>"
            "</div>"
            "</div>"
            "<div class='grid' style='margin-top:14px;'>"
            "<div class='card'><h3>Рассылка всем</h3>"
            f"<form method='post' action='{escape(self.path)}/action/broadcast_all'>"
            f"<input type='hidden' name='inspect_tg' value='{escape(inspect_form_value)}' />"
            "<textarea name='text' placeholder='Текст рассылки' required></textarea>"
            "<button type='submit'>Отправить</button></form></div>"
            "<div class='card'><h3>Рассылка пользователю</h3>"
            f"<form method='post' action='{escape(self.path)}/action/broadcast_user'>"
            f"<input type='hidden' name='inspect_tg' value='{escape(inspect_form_value)}' />"
            "<input name='tg_user_id' placeholder='tg_user_id' required />"
            "<textarea name='text' placeholder='Текст сообщения' required></textarea>"
            "<button type='submit'>Отправить</button></form></div>"
            "<div class='card'><h3>Блокировка</h3>"
            f"<form method='post' action='{escape(self.path)}/action/ban'>"
            f"<input type='hidden' name='inspect_tg' value='{escape(inspect_form_value)}' />"
            "<input name='tg_user_id' placeholder='tg_user_id' required />"
            "<textarea name='reason' placeholder='Причина (необязательно)'></textarea>"
            "<button type='submit' class='danger'>Заблокировать</button></form>"
            f"<form method='post' action='{escape(self.path)}/action/unban' style='margin-top:8px;'>"
            f"<input type='hidden' name='inspect_tg' value='{escape(inspect_form_value)}' />"
            "<input name='tg_user_id' placeholder='tg_user_id' required />"
            "<button type='submit' class='alt'>Разблокировать</button></form></div>"
            "<div class='card'><h3>Начислить прокси</h3>"
            f"<form method='post' action='{escape(self.path)}/action/grant_proxies'>"
            f"<input type='hidden' name='inspect_tg' value='{escape(inspect_form_value)}' />"
            "<input name='tg_user_id' placeholder='tg_user_id' required />"
            "<input name='devices_count' placeholder='кол-во прокси (например 1)' required />"
            "<input name='days' placeholder='дней (по умолчанию 30)' />"
            "<button type='submit' class='alt'>Начислить</button></form></div>"
            "<div class='card'><h3>Удалить прокси</h3>"
            f"<form method='post' action='{escape(self.path)}/action/remove_proxies'>"
            f"<input type='hidden' name='inspect_tg' value='{escape(inspect_form_value)}' />"
            "<input name='tg_user_id' placeholder='tg_user_id' required />"
            "<input name='proxy_id' placeholder='proxy_id или all' required />"
            "<button type='submit' class='danger'>Удалить</button></form></div>"
            "<div class='card'><h3>Списать реферал</h3>"
            f"<form method='post' action='{escape(self.path)}/action/referral_debit'>"
            f"<input type='hidden' name='inspect_tg' value='{escape(inspect_form_value)}' />"
            "<input name='tg_user_id' placeholder='tg_user_id' required />"
            "<input name='amount_rub' placeholder='сумма ₽' required />"
            "<input name='comment' placeholder='комментарий (необязательно)' />"
            "<button type='submit' class='danger'>Списать</button></form></div>"
            "</div>"
            "<div class='card' style='margin-top:14px;'>"
            "<h2>Пользователи</h2>"
            f"{users_table}"
            "</div>"
            f"{inspect_block}"
            "</div>"
        )
        return _page_template(title="WhiteProxy Web Admin", content=body)
