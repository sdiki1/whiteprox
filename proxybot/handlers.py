from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
import logging
import math
import re
from typing import Iterable
from urllib.parse import unquote, urlencode, urlparse

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    ReplyKeyboardRemove,
    User as TelegramUser,
)

from .database import Database, Plan
from .keyboards import (
    EMOJI_BOX,
    EMOJI_DEV,
    EMOJI_GEM,
    EMOJI_SHIELD,
    admin_cancel_keyboard,
    admin_panel_keyboard,
    activate_first_proxy_keyboard,
    activate_proxy_keyboard,
    back_to_menu_keyboard,
    devices_keyboard,
    friend_target_input_keyboard,
    friend_user_picker_keyboard,
    main_menu_keyboard,
    months_keyboard,
    payment_keyboard,
    purchase_target_keyboard,
    subscriptions_actions_keyboard,
)
from .pricing import total_price_rub
from .yookassa import YooKassaClient, YooKassaError

logger = logging.getLogger(__name__)

BOT_BRAND = "white proxy Bot"
PROXY_FOOTER = f"Сделано в {BOT_BRAND}"
TEMP_KIND_PROXY_OUTPUT = "proxy_output"
BLOCKED_TG_USER_ID = 1664076316
BLOCKED_USER_TEXT = "ЛАВРЕНТ ИДИ НАХУЙ, СУКА!\n\nЗа 25₽ мне на карту ты помилован"
DEFAULT_BAN_TEXT = "Доступ к боту ограничен администратором."
EMOJI_KEY = "5330115548900501467"
STARS_PER_RUB = 1.3
SUPPORTED_MONTH_OPTIONS = (1, 3, 6, 12)
REFERRAL_REWARD_PERCENT = 50


class AdminStates(StatesGroup):
    broadcast_all = State()
    broadcast_user = State()
    ban_user = State()
    unban_user = State()
    user_configs = State()
    grant_proxies = State()
    remove_proxies = State()
    referral_debit = State()


class PurchaseStates(StatesGroup):
    waiting_friend_tg_id = State()


@dataclass(frozen=True)
class UserProfile:
    id: int
    tg_user_id: int
    username: str | None
    first_name: str | None
    last_name: str | None


def format_ts(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%d.%m.%Y %H:%M UTC")


def format_remaining(expires_at: int) -> str:
    delta = expires_at - int(datetime.now(tz=timezone.utc).timestamp())
    if delta <= 0:
        return "истекло"
    days, rest = divmod(delta, 86400)
    hours, _ = divmod(rest, 3600)
    if days > 0:
        return f"{days} д. {hours} ч."
    return f"{hours} ч."


def tg_emoji(emoji_id: str, fallback: str) -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


def build_instruction_text() -> str:
    pay_line = "3) Оплатите заказ и активируйте выдачу."
    return (
        f"{tg_emoji(EMOJI_GEM, '💎')} <b>Инструкция</b>\n"
        "1) Нажмите «Оформить доступ».\n"
        "2) Выберите срок, пакет и получателя.\n"
        f"{pay_line}\n"
        "4) Откройте «Мои конфиги» и активируйте прокси."
    )


def build_welcome_text() -> str:
    return (
        f"{tg_emoji(EMOJI_SHIELD, '🛡')} <b>{BOT_BRAND}</b> - панель персональных SOCKS5 в Telegram.\n\n"
        f"{tg_emoji(EMOJI_KEY, '🔑')} Оформление и активация прокси прямо в чате.\n\n"
        "<blockquote>Можно оформить доступ для себя или подарить доступ другу "
        "по tg_user_id/@username.</blockquote>\n\n"
        f"{build_instruction_text()}"
    )


def build_help_text() -> str:
    return (
        f"{tg_emoji(EMOJI_SHIELD, '🛡')} <b>Команды бота</b>\n\n"
        "/start — главное меню\n"
        "/plans — оформление доступа\n"
        "/buy — оформление доступа\n"
        "/my_links — мои прокси\n"
        "/status — сроки и статусы\n"
        "/ref — реферальная программа\n"
        "/help — помощь\n\n"
        f"{build_instruction_text()}"
    )


def build_plans_text(plans: list[Plan]) -> str:
    lines = [
        f"{tg_emoji(EMOJI_SHIELD, '🛡')} <b>Тарифы {BOT_BRAND}</b>",
        "",
        "Базовая цена за <b>1 месяц</b>:",
        "",
    ]
    for plan in plans:
        lines.append(
            f"• <b>{plan.devices_count} прокси</b> — <b>{plan.price_rub}₽ / месяц</b>"
        )
    lines.extend(
        [
            "",
            f"{tg_emoji(EMOJI_GEM, '💎')} При покупке: сначала выберите срок, затем тариф.",
        ]
    )
    return "\n".join(lines)


def month_word(count: int) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return "месяц"
    if count % 10 in (2, 3, 4) and count % 100 not in (12, 13, 14):
        return "месяца"
    return "месяцев"


def rub_to_stars(amount_rub: int) -> int:
    return int(math.ceil(max(0, amount_rub) * STARS_PER_RUB))


def is_supported_months(months_count: int) -> bool:
    return months_count in SUPPORTED_MONTH_OPTIONS


def build_buy_months_text() -> str:
    return (
        f"{tg_emoji(EMOJI_SHIELD, '🛡')} <b>Оформление доступа</b>\n\n"
        "Этап 1/3: выберите срок действия."
    )


def build_devices_step_text(*, months_count: int) -> str:
    return (
        f"{tg_emoji(EMOJI_SHIELD, '🛡')} <b>Этап 2/3</b>\n\n"
        f"Срок: <b>{months_count} {month_word(months_count)}</b>\n"
        "Теперь выберите пакет прокси."
    )


def build_admin_panel_text(*, referral_summary: dict[str, int] | None = None) -> str:
    base = (
        f"{tg_emoji(EMOJI_SHIELD, '🛡')} <b>Админ-панель</b>\n\n"
        "Выберите действие из меню ниже."
    )
    if referral_summary is None:
        return base
    return (
        f"{base}\n\n"
        f"{tg_emoji(EMOJI_GEM, '💎')} <b>Рефералы</b>\n"
        f"Пользователей с реферером: <b>{referral_summary.get('users_with_referrer', 0)}</b>\n"
        f"Начислено всего: <b>{referral_summary.get('total_earned_rub', 0)}₽</b>\n"
        f"Списано/выведено: <b>{referral_summary.get('total_debited_rub', 0)}₽</b>\n"
        f"Текущий реф. баланс: <b>{referral_summary.get('total_balance_rub', 0)}₽</b>"
    )


def build_referral_text(
    *,
    referral_link: str,
    summary: dict[str, int],
) -> str:
    return (
        f"{tg_emoji(EMOJI_GEM, '💎')} <b>Реферальная программа</b>\n\n"
        f"Возврат: <b>{REFERRAL_REWARD_PERCENT}%</b> от оплаты приглашенного пользователя.\n\n"
        f"Ваша ссылка:\n<code>{referral_link}</code>\n\n"
        f"Приглашено: <b>{summary.get('referrals_count', 0)}</b>\n"
        f"Реф. баланс: <b>{summary.get('balance_rub', 0)}₽</b>\n"
        f"Начислено всего: <b>{summary.get('earned_rub', 0)}₽</b>\n"
        f"Списано/выведено: <b>{summary.get('debited_rub', 0)}₽</b>"
    )


def normalize_user_profile(row: dict) -> UserProfile:
    return UserProfile(
        id=int(row["id"]),
        tg_user_id=int(row["tg_user_id"]),
        username=str(row["username"]) if row.get("username") else None,
        first_name=str(row["first_name"]) if row.get("first_name") else None,
        last_name=str(row["last_name"]) if row.get("last_name") else None,
    )


def user_proxy_label_from_profile(profile: UserProfile) -> str:
    if profile.username:
        return f"{profile.username}/{profile.tg_user_id}"
    return str(profile.tg_user_id)


def user_display_name(profile: UserProfile) -> str:
    parts = [item for item in [profile.first_name, profile.last_name] if item]
    if parts:
        return " ".join(parts)
    if profile.username:
        return f"@{profile.username}"
    return str(profile.tg_user_id)


def chunk_lines(lines: Iterable[str], max_len: int = 3500) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in lines:
        line_len = len(line) + 1
        if current and current_len + line_len > max_len:
            chunks.append("\n".join(current))
            current = [line]
            current_len = line_len
        else:
            current.append(line)
            current_len += line_len
    if current:
        chunks.append("\n".join(current))
    return chunks


def is_admin(tg_user_id: int, admin_tg_ids: set[int]) -> bool:
    return tg_user_id in admin_tg_ids


def extract_text_payload(message: Message) -> str | None:
    if message.text:
        return message.text
    if message.caption:
        return message.caption
    return None


def parse_int(raw: str) -> int | None:
    try:
        return int(raw.strip())
    except ValueError:
        return None


def parse_start_referrer_tg_user_id(message: Message) -> int | None:
    raw = (message.text or "").strip()
    if not raw:
        return None
    parts = raw.split(maxsplit=1)
    if len(parts) != 2:
        return None
    payload = parts[1].strip()
    if not payload.startswith("ref_"):
        return None
    referral_raw = payload[len("ref_") :].strip()
    if not referral_raw.isdigit():
        return None
    referral_tg_user_id = int(referral_raw)
    return referral_tg_user_id if referral_tg_user_id > 0 else None


def normalize_username_candidate(raw: str) -> str | None:
    value = raw.strip()
    if not value:
        return None

    lowered = value.lower()
    if lowered.startswith("https://t.me/"):
        value = value[len("https://t.me/") :]
    elif lowered.startswith("http://t.me/"):
        value = value[len("http://t.me/") :]
    elif lowered.startswith("t.me/"):
        value = value[len("t.me/") :]

    if value.startswith("@"):
        value = value[1:]
    value = value.strip()
    if not value:
        return None

    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", value):
        return None
    return value


def payment_target_label(*, buyer_tg_user_id: int, target_tg_user_id: int) -> str:
    if buyer_tg_user_id == target_tg_user_id:
        return "Себе"
    return "Другу"


async def ensure_user(
    db: Database,
    telegram_user: TelegramUser,
    *,
    bot=None,
    admin_tg_ids: set[int] | None = None,
) -> int:
    existed = await db.get_user_by_tg_user_id(telegram_user.id)
    user_id = await db.upsert_user(
        tg_user_id=telegram_user.id,
        username=telegram_user.username,
        first_name=telegram_user.first_name,
        last_name=telegram_user.last_name,
    )
    if existed is None and bot is not None and admin_tg_ids:
        username = f"@{telegram_user.username}" if telegram_user.username else "без username"
        full_name = " ".join(
            item for item in [telegram_user.first_name, telegram_user.last_name] if item
        ).strip() or "без имени"
        text = (
            "Новый пользователь в боте.\n"
            f"ID: {telegram_user.id}\n"
            f"Username: {username}\n"
            f"Имя: {full_name}"
        )
        for admin_id in admin_tg_ids:
            if admin_id == telegram_user.id:
                continue
            try:
                await bot.send_message(admin_id, text, parse_mode=None)
            except (TelegramBadRequest, TelegramForbiddenError):
                logger.warning("Could not send new-user notification to admin %s", admin_id)
    return user_id


async def blocked_text_for_user(db: Database, tg_user_id: int) -> str | None:
    if tg_user_id == BLOCKED_TG_USER_ID:
        return BLOCKED_USER_TEXT
    ban = await db.get_user_ban(tg_user_id)
    if ban is None:
        return None
    reason = str(ban.get("reason") or "").strip()
    return reason or DEFAULT_BAN_TEXT


async def handle_blocked_message(db: Database, message: Message) -> bool:
    if message.from_user is None:
        return False
    blocked_text = await blocked_text_for_user(db, message.from_user.id)
    if blocked_text is None:
        return False
    await message.answer(blocked_text)
    return True


async def handle_blocked_callback(db: Database, callback: CallbackQuery) -> bool:
    blocked_text = await blocked_text_for_user(db, callback.from_user.id)
    if blocked_text is None:
        return False
    if callback.message is not None:
        try:
            await callback.message.edit_text(blocked_text, reply_markup=None, parse_mode=None)
        except TelegramBadRequest:
            await callback.bot.send_message(callback.from_user.id, blocked_text)
    else:
        await callback.bot.send_message(callback.from_user.id, blocked_text)
    await callback.answer()
    return True


async def hide_friend_picker_reply_keyboard_if_needed(*, state: FSMContext, bot, tg_user_id: int) -> None:
    if await state.get_state() != PurchaseStates.waiting_friend_tg_id.state:
        return
    await bot.send_message(
        tg_user_id,
        "Выход из выбора пользователя.",
        reply_markup=ReplyKeyboardRemove(),
    )


def profile_label(telegram_user: TelegramUser) -> str:
    if telegram_user.username:
        return f"{telegram_user.username}/{telegram_user.id}"
    return str(telegram_user.id)


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


def parse_socks5_url(link: str) -> tuple[str, int, str, str] | None:
    parsed = urlparse(link)
    if parsed.scheme != "socks5":
        return None
    if parsed.hostname is None or parsed.port is None:
        return None
    if parsed.username is None or parsed.password is None:
        return None
    return parsed.hostname, parsed.port, unquote(parsed.username), unquote(parsed.password)


def build_proxy_block(*, proxy_index: int, user_proxy_label: str, proxy_id: int, tg_link: str) -> str:
    safe_tg_link = escape(tg_link, quote=True)
    return (
        f"PROXY-{proxy_index}-{user_proxy_label}\n"
        f"Proxy ID: {proxy_id}\n\n"
        f"{tg_emoji('5433653135799228968', '✅')} Нажмите на ссылку, чтобы подключить прокси:\n"
        f"{safe_tg_link}\n\n"
        f"{PROXY_FOOTER}"
    )


async def log_proxy_delivery(
    *,
    db: Database,
    proxy_id: int,
    user_id: int,
    tg_user_id: int,
    user_proxy_label: str,
    subscription_id: int | None,
    device_number: int | None,
    delivery_source: str,
    tg_link: str,
) -> None:
    await db.log_proxy_delivery(
        proxy_link_id=proxy_id,
        user_id=user_id,
        tg_user_id=tg_user_id,
        user_label=user_proxy_label,
        subscription_id=subscription_id,
        device_number=device_number,
        delivery_source=delivery_source,
        proxy_url=tg_link,
    )
    logger.info(
        "Delivered proxy: tg_user_id=%s user_id=%s proxy_id=%s subscription_id=%s source=%s url=%s",
        tg_user_id,
        user_id,
        proxy_id,
        subscription_id,
        delivery_source,
        tg_link,
    )


async def cleanup_proxy_output_messages(*, db: Database, bot, user_id: int) -> None:
    rows = await db.pop_temp_messages(user_id=user_id, kind=TEMP_KIND_PROXY_OUTPUT)
    for row in rows:
        try:
            await bot.delete_message(int(row["tg_user_id"]), int(row["message_id"]))
        except TelegramBadRequest:
            pass


async def edit_or_send(
    callback: CallbackQuery,
    *,
    text: str,
    reply_markup: InlineKeyboardMarkup | None,
    parse_mode: str | None,
) -> None:
    if callback.message is not None:
        try:
            await callback.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            return
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return
    await callback.bot.send_message(callback.from_user.id, text, reply_markup=reply_markup, parse_mode=parse_mode)


async def send_links_list(
    *,
    db: Database,
    bot_chat_id: int,
    bot,
    user_id: int,
    tg_user_id: int,
    user_proxy_label: str,
    source_message: Message | None = None,
) -> None:
    links = await db.get_active_links_for_user(user_id)
    if not links:
        text = (
            f"{tg_emoji(EMOJI_DEV, '📱')} У вас пока нет активных прокси.\n"
            "Выберите тариф через /buy или кнопку «Оформить доступ»."
        )
        if source_message is not None:
            await source_message.edit_text(text, reply_markup=back_to_menu_keyboard())
        else:
            await bot.send_message(bot_chat_id, text, reply_markup=back_to_menu_keyboard())
        return

    proxies: list[dict[str, int | str | None]] = []
    for index, row in enumerate(links, start=1):
        parsed = parse_socks5_url(str(row["link"]))
        if parsed is None:
            continue
        host, port, username, password = parsed
        tg_link = telegram_socks_link(host, port, username, password)
        proxy_id = int(row["id"])
        proxies.append(
            {
                "index": index,
                "proxy_id": proxy_id,
                "tg_link": tg_link,
                "subscription_id": int(row["subscription_id"]),
                "device_number": int(row["device_number"]),
            }
        )

    await send_proxy_sequence(
        db=db,
        bot=bot,
        bot_chat_id=bot_chat_id,
        user_id=user_id,
        tg_user_id=tg_user_id,
        user_proxy_label=user_proxy_label,
        proxies=proxies,
        delivery_source="my_links",
        source_message=source_message,
    )


async def send_proxy_sequence(
    *,
    db: Database,
    bot,
    bot_chat_id: int,
    user_id: int,
    tg_user_id: int,
    user_proxy_label: str,
    proxies: list[dict[str, int | str | None]],
    delivery_source: str,
    source_message: Message | None = None,
    include_first_proxy_button: bool = False,
) -> None:
    await cleanup_proxy_output_messages(db=db, bot=bot, user_id=user_id)

    if source_message is not None:
        try:
            await source_message.delete()
        except TelegramBadRequest:
            pass

    if not proxies:
        await bot.send_message(
            bot_chat_id,
            "Не удалось подготовить ссылки для Telegram из сохраненных прокси.",
            reply_markup=back_to_menu_keyboard(),
        )
        return

    if include_first_proxy_button:
        first_proxy_link = str(proxies[0]["tg_link"])
        first_proxy_msg = await bot.send_message(
            bot_chat_id,
            "Быстрая активация прокси:",
            reply_markup=activate_first_proxy_keyboard(first_proxy_link),
        )
        await db.add_temp_message(
            user_id=user_id,
            tg_user_id=tg_user_id,
            message_id=first_proxy_msg.message_id,
            kind=TEMP_KIND_PROXY_OUTPUT,
        )

    for item in proxies:
        text = build_proxy_block(
            proxy_index=int(item["index"]),
            user_proxy_label=user_proxy_label,
            proxy_id=int(item["proxy_id"]),
            tg_link=str(item["tg_link"]),
        )
        sent = await bot.send_message(
            bot_chat_id,
            text,
            parse_mode="HTML",
            reply_markup=activate_proxy_keyboard(str(item["tg_link"])),
        )
        await db.add_temp_message(
            user_id=user_id,
            tg_user_id=tg_user_id,
            message_id=sent.message_id,
            kind=TEMP_KIND_PROXY_OUTPUT,
        )
        await log_proxy_delivery(
            db=db,
            proxy_id=int(item["proxy_id"]),
            user_id=user_id,
            tg_user_id=tg_user_id,
            user_proxy_label=user_proxy_label,
            subscription_id=int(item["subscription_id"]) if item["subscription_id"] is not None else None,
            device_number=int(item["device_number"]) if item["device_number"] is not None else None,
            delivery_source=delivery_source,
            tg_link=str(item["tg_link"]),
        )

    control = await bot.send_message(
        bot_chat_id,
        "Перейти в главное меню:",
        reply_markup=back_to_menu_keyboard(),
    )
    await db.add_temp_message(
        user_id=user_id,
        tg_user_id=tg_user_id,
        message_id=control.message_id,
        kind=TEMP_KIND_PROXY_OUTPUT,
    )


async def send_status(
    *,
    db: Database,
    bot_chat_id: int,
    bot,
    user_id: int,
    edit_message: Message | None = None,
) -> None:
    subscriptions = await db.get_active_subscriptions_for_user(user_id)
    if not subscriptions:
        text = f"{tg_emoji(EMOJI_BOX, '📦')} У вас нет активной подписки.\nОформите тариф через /buy."
        if edit_message is not None:
            try:
                await edit_message.edit_text(text, reply_markup=subscriptions_actions_keyboard())
            except TelegramBadRequest as exc:
                if "message is not modified" not in str(exc).lower():
                    raise
        else:
            await bot.send_message(bot_chat_id, text, reply_markup=subscriptions_actions_keyboard())
        return

    lines = [f"{tg_emoji(EMOJI_BOX, '📦')} <b>Активные прокси</b>", ""]
    for sub in subscriptions:
        expires_at = int(sub["expires_at"])
        lines.append(
            f"• #{sub['id']} — {sub['plan_title']} — до {format_ts(expires_at)} "
            f"(осталось {format_remaining(expires_at)})"
        )

    text = "\n".join(lines)
    if edit_message is not None:
        try:
            await edit_message.edit_text(text, reply_markup=subscriptions_actions_keyboard())
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                raise
    else:
        await bot.send_message(bot_chat_id, text, reply_markup=subscriptions_actions_keyboard())


def create_router(
    db: Database,
    proxy_public_host: str,
    admin_tg_ids: tuple[int, ...] = (),
    yookassa_client: YooKassaClient | None = None,
) -> Router:
    router = Router()
    admin_ids = set(admin_tg_ids)
    yk = yookassa_client or YooKassaClient(shop_id="", secret_key="", return_url="https://t.me")

    async def build_admin_panel_text_with_referrals() -> str:
        referral_summary = await db.get_referral_admin_summary()
        return build_admin_panel_text(referral_summary=referral_summary)

    async def build_checkout_context_text() -> str:
        return build_buy_months_text()

    async def build_referral_message_for_user(*, tg_user_id: int, user_id: int, bot) -> str:
        summary = await db.get_referral_summary_for_user(user_id)
        me = await bot.get_me()
        username = (me.username or "").strip()
        referral_link = (
            f"https://t.me/{username}?start=ref_{tg_user_id}"
            if username
            else f"ref_{tg_user_id}"
        )
        return build_referral_text(referral_link=referral_link, summary=summary)

    async def build_payment_message(
        *,
        plan: Plan,
        months_count: int,
        amount_rub: int,
        payment_id: int,
        buyer_tg_user_id: int,
        target_tg_user_id: int,
        has_yookassa: bool,
    ) -> str:
        stars_amount = rub_to_stars(amount_rub)
        stars_line = f"Звезды: <b>{stars_amount}⭐</b>"
        return (
            f"{tg_emoji(EMOJI_SHIELD, '🛡')} <b>Платеж создан</b>\n\n"
            f"Срок: <b>{months_count} {month_word(months_count)}</b>\n"
            f"Прокси: <b>{plan.devices_count}</b>\n"
            f"Кому: <b>{payment_target_label(buyer_tg_user_id=buyer_tg_user_id, target_tg_user_id=target_tg_user_id)}</b>\n"
            f"Сумма: <b>{amount_rub}₽</b>\n"
            f"{stars_line}\n"
            f"ID: <code>{payment_id}</code>\n\n"
        )

    async def ensure_recipient_profile(tg_user_id: int) -> UserProfile:
        user_row = await db.get_user_by_tg_user_id(tg_user_id)
        if user_row is None:
            await db.upsert_user(
                tg_user_id=tg_user_id,
                username=None,
                first_name=None,
                last_name=None,
            )
            user_row = await db.get_user_by_tg_user_id(tg_user_id)
            if user_row is None:
                raise RuntimeError("Failed to create recipient user profile.")
        return normalize_user_profile(user_row)

    async def create_checkout_payment(
        *,
        buyer_user_id: int,
        buyer_tg_user_id: int,
        recipient_tg_user_id: int,
        plan: Plan,
        months_count: int,
    ) -> tuple[int, str | None]:
        amount_rub = total_price_rub(
            monthly_price_rub=plan.price_rub,
            months_count=months_count,
        )
        yookassa_payment_id: str | None = None
        yookassa_confirmation_url: str | None = None
        if yk.enabled:
            description = (
                f"{BOT_BRAND}: {plan.devices_count} прокси, "
                f"{months_count} {month_word(months_count)}, "
                f"{payment_target_label(buyer_tg_user_id=buyer_tg_user_id, target_tg_user_id=recipient_tg_user_id)}"
            )
            yk_payment = await yk.create_payment(
                amount_rub=amount_rub,
                description=description,
                metadata={
                    "buyer_tg_user_id": str(buyer_tg_user_id),
                    "target_tg_user_id": str(recipient_tg_user_id),
                    "plan_code": plan.code,
                    "months_count": str(months_count),
                },
            )
            yookassa_payment_id = yk_payment.payment_id
            yookassa_confirmation_url = yk_payment.confirmation_url

        payment_id = await db.create_payment(
            user_id=buyer_user_id,
            plan_code=plan.code,
            amount_rub=amount_rub,
            months_count=months_count,
            target_tg_user_id=recipient_tg_user_id,
            yookassa_payment_id=yookassa_payment_id,
            yookassa_confirmation_url=yookassa_confirmation_url,
        )
        return payment_id, yookassa_confirmation_url

    @router.message(CommandStart())
    async def cmd_start(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if message.from_user is None:
            return
        await hide_friend_picker_reply_keyboard_if_needed(
            state=state,
            bot=message.bot,
            tg_user_id=message.from_user.id,
        )
        await state.clear()
        referrer_tg_user_id = parse_start_referrer_tg_user_id(message)
        await ensure_user(
            db,
            message.from_user,
            bot=message.bot,
            admin_tg_ids=admin_ids,
        )
        referral_bound = False
        if (
            referrer_tg_user_id is not None
            and referrer_tg_user_id != message.from_user.id
        ):
            referral_bound = await db.bind_referrer_by_tg_user_ids(
                referred_tg_user_id=message.from_user.id,
                referrer_tg_user_id=referrer_tg_user_id,
            )
        await message.answer(
            build_welcome_text(),
            reply_markup=main_menu_keyboard(),
        )
        if referral_bound:
            await message.answer("Реферальная привязка сохранена.")

    @router.message(Command("help"))
    async def cmd_help(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if message.from_user is not None:
            await hide_friend_picker_reply_keyboard_if_needed(
                state=state,
                bot=message.bot,
                tg_user_id=message.from_user.id,
            )
            await state.clear()
            await ensure_user(
                db,
                message.from_user,
                bot=message.bot,
                admin_tg_ids=admin_ids,
            )
        await message.answer(build_help_text())

    @router.message(Command("plans"))
    @router.message(Command("buy"))
    async def cmd_plans(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if message.from_user is None:
            return
        await hide_friend_picker_reply_keyboard_if_needed(
            state=state,
            bot=message.bot,
            tg_user_id=message.from_user.id,
        )
        await state.clear()
        await ensure_user(
            db,
            message.from_user,
            bot=message.bot,
            admin_tg_ids=admin_ids,
        )
        await message.answer(
            await build_checkout_context_text(),
            reply_markup=months_keyboard(),
            parse_mode="HTML",
        )

    @router.message(Command("my_links"))
    async def cmd_links(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if message.from_user is None:
            return
        await hide_friend_picker_reply_keyboard_if_needed(
            state=state,
            bot=message.bot,
            tg_user_id=message.from_user.id,
        )
        await state.clear()
        user_id = await ensure_user(
            db,
            message.from_user,
            bot=message.bot,
            admin_tg_ids=admin_ids,
        )
        await send_links_list(
            db=db,
            bot_chat_id=message.chat.id,
            bot=message.bot,
            user_id=user_id,
            tg_user_id=message.from_user.id,
            user_proxy_label=profile_label(message.from_user),
        )

    @router.message(Command("status"))
    async def cmd_status(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if message.from_user is None:
            return
        await hide_friend_picker_reply_keyboard_if_needed(
            state=state,
            bot=message.bot,
            tg_user_id=message.from_user.id,
        )
        await state.clear()
        user_id = await ensure_user(
            db,
            message.from_user,
            bot=message.bot,
            admin_tg_ids=admin_ids,
        )
        await send_status(db=db, bot_chat_id=message.chat.id, bot=message.bot, user_id=user_id)

    @router.message(Command("ref"))
    async def cmd_ref(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if message.from_user is None:
            return
        await hide_friend_picker_reply_keyboard_if_needed(
            state=state,
            bot=message.bot,
            tg_user_id=message.from_user.id,
        )
        await state.clear()
        user_id = await ensure_user(
            db,
            message.from_user,
            bot=message.bot,
            admin_tg_ids=admin_ids,
        )
        await message.answer(
            await build_referral_message_for_user(
                tg_user_id=message.from_user.id,
                user_id=user_id,
                bot=message.bot,
            ),
            reply_markup=back_to_menu_keyboard(),
            parse_mode="HTML",
        )

    @router.message(Command("admin"))
    async def cmd_admin(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if message.from_user is None:
            return
        await ensure_user(
            db,
            message.from_user,
            bot=message.bot,
            admin_tg_ids=admin_ids,
        )
        if not is_admin(message.from_user.id, admin_ids):
            await message.answer("Доступ запрещен.")
            return
        await state.clear()
        await message.answer(
            await build_admin_panel_text_with_referrals(),
            reply_markup=admin_panel_keyboard(),
            parse_mode="HTML",
        )

    async def ensure_admin_message_access(message: Message, state: FSMContext) -> bool:
        if message.from_user is None:
            return False
        await ensure_user(
            db,
            message.from_user,
            bot=message.bot,
            admin_tg_ids=admin_ids,
        )
        if not is_admin(message.from_user.id, admin_ids):
            await state.clear()
            await message.answer("Доступ запрещен.")
            return False
        return True

    async def ensure_admin_callback_access(callback: CallbackQuery, state: FSMContext) -> bool:
        await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        if not is_admin(callback.from_user.id, admin_ids):
            await state.clear()
            await callback.answer("Доступ запрещен.", show_alert=True)
            return False
        return True

    @router.callback_query(F.data == "admin:menu")
    async def cb_admin_menu(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.clear()
        await edit_or_send(
            callback,
            text=await build_admin_panel_text_with_referrals(),
            reply_markup=admin_panel_keyboard(),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == "admin:cancel")
    async def cb_admin_cancel(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.clear()
        await edit_or_send(
            callback,
            text=await build_admin_panel_text_with_referrals(),
            reply_markup=admin_panel_keyboard(),
            parse_mode="HTML",
        )
        await callback.answer("Отменено")

    @router.callback_query(F.data == "admin:close")
    async def cb_admin_close(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.clear()
        if callback.message is not None:
            try:
                await callback.message.delete()
            except TelegramBadRequest:
                pass
        await callback.answer("Закрыто")

    @router.callback_query(F.data == "admin:broadcast_all")
    async def cb_admin_broadcast_all(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.set_state(AdminStates.broadcast_all)
        await edit_or_send(
            callback,
            text="Отправьте текст для рассылки всем пользователям.",
            reply_markup=admin_cancel_keyboard(),
            parse_mode=None,
        )
        await callback.answer()

    @router.callback_query(F.data == "admin:broadcast_user")
    async def cb_admin_broadcast_user(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.set_state(AdminStates.broadcast_user)
        await edit_or_send(
            callback,
            text="Формат: <tg_user_id> <текст>\nПример: 123456789 Тестовое сообщение",
            reply_markup=admin_cancel_keyboard(),
            parse_mode=None,
        )
        await callback.answer()

    @router.callback_query(F.data == "admin:ban")
    async def cb_admin_ban(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.set_state(AdminStates.ban_user)
        await edit_or_send(
            callback,
            text=(
                "Формат: <tg_user_id> [текст блокировки]\n"
                "Пример: 123456789 Доступ к боту ограничен."
            ),
            reply_markup=admin_cancel_keyboard(),
            parse_mode=None,
        )
        await callback.answer()

    @router.callback_query(F.data == "admin:unban")
    async def cb_admin_unban(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.set_state(AdminStates.unban_user)
        await edit_or_send(
            callback,
            text="Формат: <tg_user_id>\nПример: 123456789",
            reply_markup=admin_cancel_keyboard(),
            parse_mode=None,
        )
        await callback.answer()

    @router.callback_query(F.data == "admin:list_users")
    async def cb_admin_list_users(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.clear()
        rows = await db.list_users_with_stats(limit=500, offset=0)
        if not rows:
            await edit_or_send(
                callback,
                text="Пользователей пока нет.",
                reply_markup=admin_panel_keyboard(),
                parse_mode=None,
            )
            await callback.answer()
            return

        lines = [f"Пользователи: {len(rows)}", ""]
        for row in rows:
            username = f"@{row['username']}" if row.get("username") else "без username"
            active_count = int(row.get("active_proxies") or 0)
            banned_flag = int(row.get("is_banned") or 0) == 1 or int(row["tg_user_id"]) == BLOCKED_TG_USER_ID
            banned = "да" if banned_flag else "нет"
            lines.append(
                f"tg:{row['tg_user_id']} | {username} | активных:{active_count} | бан:{banned}"
            )

        chunks = chunk_lines(lines)
        await edit_or_send(
            callback,
            text=chunks[0],
            reply_markup=admin_panel_keyboard(),
            parse_mode=None,
        )
        for chunk in chunks[1:]:
            await callback.bot.send_message(callback.from_user.id, chunk)
        await callback.answer()

    @router.callback_query(F.data == "admin:user_configs")
    async def cb_admin_user_configs(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.set_state(AdminStates.user_configs)
        await edit_or_send(
            callback,
            text="Введите tg_user_id пользователя для просмотра конфигов.",
            reply_markup=admin_cancel_keyboard(),
            parse_mode=None,
        )
        await callback.answer()

    @router.callback_query(F.data == "admin:grant_proxies")
    async def cb_admin_grant_proxies(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.set_state(AdminStates.grant_proxies)
        await edit_or_send(
            callback,
            text=(
                "Формат: <tg_user_id> <кол-во> [дней]\n"
                "Кол-во должно соответствовать доступному тарифу (сейчас: 1)."
            ),
            reply_markup=admin_cancel_keyboard(),
            parse_mode=None,
        )
        await callback.answer()

    @router.callback_query(F.data == "admin:remove_proxies")
    async def cb_admin_remove_proxies(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.set_state(AdminStates.remove_proxies)
        await edit_or_send(
            callback,
            text=(
                "Формат: <tg_user_id> <proxy_id|all>\n"
                "Пример: 123456789 42 или 123456789 all"
            ),
            reply_markup=admin_cancel_keyboard(),
            parse_mode=None,
        )
        await callback.answer()

    @router.callback_query(F.data == "admin:ref_debit")
    async def cb_admin_ref_debit(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        if not await ensure_admin_callback_access(callback, state):
            return
        await state.set_state(AdminStates.referral_debit)
        await edit_or_send(
            callback,
            text=(
                "Формат: <tg_user_id> <сумма_руб> [комментарий]\n"
                "Пример: 123456789 500 Выплата за март"
            ),
            reply_markup=admin_cancel_keyboard(),
            parse_mode=None,
        )
        await callback.answer()

    @router.message(AdminStates.broadcast_all)
    async def admin_state_broadcast_all(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if not await ensure_admin_message_access(message, state):
            return
        payload = extract_text_payload(message)
        if payload is None:
            await message.answer("Отправьте текстовое сообщение.")
            return

        targets = await db.get_all_tg_user_ids()
        sent_ok = 0
        sent_fail = 0
        for tg_user_id in targets:
            try:
                await message.bot.send_message(tg_user_id, payload, parse_mode=None)
                sent_ok += 1
            except (TelegramBadRequest, TelegramForbiddenError):
                sent_fail += 1

        await state.clear()
        await message.answer(
            f"Рассылка завершена.\nУспешно: {sent_ok}\nОшибок: {sent_fail}",
            reply_markup=admin_panel_keyboard(),
        )

    @router.message(AdminStates.broadcast_user)
    async def admin_state_broadcast_user(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if not await ensure_admin_message_access(message, state):
            return
        payload = extract_text_payload(message)
        if payload is None:
            await message.answer("Отправьте текст в формате: <tg_user_id> <текст>.")
            return
        parts = payload.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer("Неверный формат. Пример: 123456789 Тест")
            return
        tg_user_id = parse_int(parts[0])
        if tg_user_id is None:
            await message.answer("tg_user_id должен быть числом.")
            return
        text = parts[1].strip()
        if not text:
            await message.answer("Текст рассылки пустой.")
            return

        try:
            await message.bot.send_message(tg_user_id, text, parse_mode=None)
        except (TelegramBadRequest, TelegramForbiddenError) as exc:
            await message.answer(f"Не удалось отправить сообщение: {exc}")
            return

        await state.clear()
        await message.answer("Сообщение отправлено.", reply_markup=admin_panel_keyboard())

    @router.message(AdminStates.ban_user)
    async def admin_state_ban_user(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if not await ensure_admin_message_access(message, state):
            return
        payload = extract_text_payload(message)
        if payload is None:
            await message.answer("Неверный формат.")
            return
        parts = payload.split(maxsplit=1)
        tg_user_id = parse_int(parts[0])
        if tg_user_id is None:
            await message.answer("tg_user_id должен быть числом.")
            return
        reason = parts[1].strip() if len(parts) > 1 else DEFAULT_BAN_TEXT
        reason = reason or DEFAULT_BAN_TEXT
        await db.ban_user(tg_user_id=tg_user_id, reason=reason, blocked_by=message.from_user.id)

        try:
            await message.bot.send_message(tg_user_id, reason, parse_mode=None)
        except (TelegramBadRequest, TelegramForbiddenError):
            pass

        await state.clear()
        await message.answer(
            f"Пользователь {tg_user_id} заблокирован.",
            reply_markup=admin_panel_keyboard(),
        )

    @router.message(AdminStates.unban_user)
    async def admin_state_unban_user(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if not await ensure_admin_message_access(message, state):
            return
        payload = extract_text_payload(message)
        if payload is None:
            await message.answer("Неверный формат.")
            return
        tg_user_id = parse_int(payload)
        if tg_user_id is None:
            await message.answer("tg_user_id должен быть числом.")
            return
        if tg_user_id == BLOCKED_TG_USER_ID:
            await message.answer("Этого пользователя нельзя разбанить из панели.")
            return
        changed = await db.unban_user(tg_user_id)
        await state.clear()
        if changed:
            await message.answer(
                f"Пользователь {tg_user_id} разблокирован.",
                reply_markup=admin_panel_keyboard(),
            )
        else:
            await message.answer(
                f"Пользователь {tg_user_id} не был в бане.",
                reply_markup=admin_panel_keyboard(),
            )

    @router.message(AdminStates.user_configs)
    async def admin_state_user_configs(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if not await ensure_admin_message_access(message, state):
            return
        payload = extract_text_payload(message)
        if payload is None:
            await message.answer("Введите tg_user_id.")
            return
        tg_user_id = parse_int(payload)
        if tg_user_id is None:
            await message.answer("tg_user_id должен быть числом.")
            return

        user_row = await db.get_user_by_tg_user_id(tg_user_id)
        if user_row is None:
            await message.answer("Пользователь не найден.")
            return
        profile = normalize_user_profile(user_row)
        ban = await db.get_user_ban(profile.tg_user_id)
        links = await db.get_all_links_for_user(profile.id)

        lines = [
            f"Пользователь: {user_display_name(profile)}",
            f"tg_user_id: {profile.tg_user_id}",
            f"username: @{profile.username}" if profile.username else "username: -",
            f"Бан: {'да' if ban is not None or profile.tg_user_id == BLOCKED_TG_USER_ID else 'нет'}",
            f"Всего конфигов: {len(links)}",
            "",
        ]
        if not links:
            lines.append("Конфиги отсутствуют.")
        else:
            for row in links:
                lines.append(
                    f"ID:{row['id']} | sub:{row['subscription_id']} | device:{row['device_number']} | "
                    f"status:{row['status']} | exp:{format_ts(int(row['expires_at']))}"
                )
                lines.append(str(row["link"]))
                lines.append("")

        await state.clear()
        chunks = chunk_lines(lines)
        await message.answer(chunks[0], reply_markup=admin_panel_keyboard())
        for chunk in chunks[1:]:
            await message.bot.send_message(message.from_user.id, chunk)

    @router.message(AdminStates.grant_proxies)
    async def admin_state_grant_proxies(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if not await ensure_admin_message_access(message, state):
            return
        payload = extract_text_payload(message)
        if payload is None:
            await message.answer("Неверный формат.")
            return
        parts = payload.split()
        if len(parts) < 2:
            await message.answer("Формат: <tg_user_id> <кол-во> [дней]")
            return

        tg_user_id = parse_int(parts[0])
        devices_count = parse_int(parts[1])
        days = parse_int(parts[2]) if len(parts) > 2 else 30
        if tg_user_id is None or devices_count is None or days is None:
            await message.answer("tg_user_id, кол-во и дни должны быть числами.")
            return
        if devices_count < 1:
            await message.answer("Количество прокси должно быть больше 0.")
            return
        if days < 1 or days > 3650:
            await message.answer("Дни должны быть в диапазоне 1..3650.")
            return

        user_row = await db.get_user_by_tg_user_id(tg_user_id)
        if user_row is None:
            await message.answer("Пользователь не найден.")
            return
        profile = normalize_user_profile(user_row)

        plans = await db.get_plans()
        plan = next((item for item in plans if item.devices_count == devices_count), None)
        if plan is None:
            await message.answer("Не найден подходящий тариф для выбранного количества.")
            return

        payment_id = await db.create_payment(
            user_id=profile.id,
            plan_code=plan.code,
            amount_rub=0,
        )
        expires_at = int((datetime.now(tz=timezone.utc) + timedelta(days=days)).timestamp())
        activated = await db.activate_payment_and_create_subscription_from_pool(
            payment_id=payment_id,
            payer_user_id=profile.id,
            recipient_user_id=profile.id,
            plan_code=plan.code,
            expires_at=expires_at,
            devices_count=devices_count,
            proxy_public_host=proxy_public_host,
        )
        if activated is None:
            free_count = await db.count_free_pool()
            await message.answer(
                f"Не удалось начислить прокси. Свободно в пуле: {free_count}.",
            )
            return
        subscription_id, created_proxies = activated

        proxies: list[dict[str, int | str | None]] = []
        for index, proxy in enumerate(created_proxies, start=1):
            tg_link = telegram_socks_link(
                proxy_public_host,
                int(proxy["port"]),
                str(proxy["username"]),
                str(proxy["password"]),
            )
            proxies.append(
                {
                    "index": index,
                    "proxy_id": int(proxy["proxy_id"]),
                    "tg_link": tg_link,
                    "subscription_id": subscription_id,
                    "device_number": int(proxy["device_number"]),
                }
            )

        await send_proxy_sequence(
            db=db,
            bot=message.bot,
            bot_chat_id=profile.tg_user_id,
            user_id=profile.id,
            tg_user_id=profile.tg_user_id,
            user_proxy_label=user_proxy_label_from_profile(profile),
            proxies=proxies,
            delivery_source="purchase",
        )

        await state.clear()
        await message.answer(
            f"Начислено {devices_count} прокси пользователю {profile.tg_user_id}.",
            reply_markup=admin_panel_keyboard(),
        )

    @router.message(AdminStates.remove_proxies)
    async def admin_state_remove_proxies(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if not await ensure_admin_message_access(message, state):
            return
        payload = extract_text_payload(message)
        if payload is None:
            await message.answer("Неверный формат.")
            return
        parts = payload.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer("Формат: <tg_user_id> <proxy_id|all>")
            return
        tg_user_id = parse_int(parts[0])
        if tg_user_id is None:
            await message.answer("tg_user_id должен быть числом.")
            return
        user_row = await db.get_user_by_tg_user_id(tg_user_id)
        if user_row is None:
            await message.answer("Пользователь не найден.")
            return
        profile = normalize_user_profile(user_row)
        token = parts[1].strip().lower()

        removed_count = 0
        if token == "all":
            removed_count = await db.revoke_all_active_links_for_user(profile.id)
        else:
            proxy_id = parse_int(token)
            if proxy_id is None:
                await message.answer("proxy_id должен быть числом или all.")
                return
            removed = await db.revoke_proxy_link_for_user(profile.id, proxy_id)
            removed_count = 1 if removed else 0

        if removed_count > 0:
            try:
                await message.bot.send_message(
                    profile.tg_user_id,
                    "Часть ваших прокси была деактивирована администратором.",
                )
            except (TelegramBadRequest, TelegramForbiddenError):
                pass

        await state.clear()
        await message.answer(
            f"Удалено прокси: {removed_count}.",
            reply_markup=admin_panel_keyboard(),
        )

    @router.message(AdminStates.referral_debit)
    async def admin_state_referral_debit(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if not await ensure_admin_message_access(message, state):
            return
        payload = extract_text_payload(message)
        if payload is None:
            await message.answer("Неверный формат.")
            return
        parts = payload.split(maxsplit=2)
        if len(parts) < 2:
            await message.answer("Формат: <tg_user_id> <сумма_руб> [комментарий]")
            return
        tg_user_id = parse_int(parts[0])
        amount_rub = parse_int(parts[1])
        if tg_user_id is None or amount_rub is None or amount_rub <= 0:
            await message.answer("tg_user_id и сумма должны быть положительными числами.")
            return
        comment = parts[2].strip() if len(parts) > 2 else ""
        admin_marker = f"admin:{message.from_user.id}"
        if comment:
            comment = f"{comment} | {admin_marker}"
        else:
            comment = admin_marker

        changed, new_balance = await db.debit_referral_balance_by_tg_user_id(
            tg_user_id=tg_user_id,
            amount_rub=amount_rub,
            comment=comment,
        )
        await state.clear()
        if not changed:
            await message.answer(
                (
                    "Не удалось списать реферальный баланс.\n"
                    f"Текущий доступный баланс: {new_balance}₽."
                ),
                reply_markup=admin_panel_keyboard(),
            )
            return
        await message.answer(
            (
                f"Списано: {amount_rub}₽ у пользователя {tg_user_id}.\n"
                f"Новый реф. баланс: {new_balance}₽."
            ),
            reply_markup=admin_panel_keyboard(),
        )

    @router.callback_query(F.data == "menu:home_clear")
    async def cb_home_clear(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        await hide_friend_picker_reply_keyboard_if_needed(
            state=state,
            bot=callback.bot,
            tg_user_id=callback.from_user.id,
        )
        await state.clear()
        user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        await cleanup_proxy_output_messages(db=db, bot=callback.bot, user_id=user_id)
        if callback.message is not None:
            try:
                await callback.message.delete()
            except TelegramBadRequest:
                pass
        await callback.bot.send_message(
            callback.from_user.id,
            build_welcome_text(),
            reply_markup=main_menu_keyboard(),
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:guide")
    async def cb_guide(callback: CallbackQuery) -> None:
        if await handle_blocked_callback(db, callback):
            return
        await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        await edit_or_send(
            callback,
            text=build_instruction_text(),
            reply_markup=back_to_menu_keyboard(),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:ref")
    async def cb_ref(callback: CallbackQuery) -> None:
        if await handle_blocked_callback(db, callback):
            return
        user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        await edit_or_send(
            callback,
            text=await build_referral_message_for_user(
                tg_user_id=callback.from_user.id,
                user_id=user_id,
                bot=callback.bot,
            ),
            reply_markup=back_to_menu_keyboard(),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:plans")
    async def cb_plans(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        await hide_friend_picker_reply_keyboard_if_needed(
            state=state,
            bot=callback.bot,
            tg_user_id=callback.from_user.id,
        )
        await state.clear()
        user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        if user_id <= 0:
            await callback.answer("Ошибка профиля", show_alert=True)
            return
        await edit_or_send(
            callback,
            text=await build_checkout_context_text(),
            reply_markup=months_keyboard(),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:links")
    async def cb_links(callback: CallbackQuery) -> None:
        if await handle_blocked_callback(db, callback):
            return
        user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        await send_links_list(
            db=db,
            bot_chat_id=callback.from_user.id,
            bot=callback.bot,
            user_id=user_id,
            tg_user_id=callback.from_user.id,
            user_proxy_label=profile_label(callback.from_user),
            source_message=callback.message,
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:status")
    async def cb_status(callback: CallbackQuery) -> None:
        if await handle_blocked_callback(db, callback):
            return
        user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        await send_status(
            db=db,
            bot_chat_id=callback.from_user.id,
            bot=callback.bot,
            user_id=user_id,
            edit_message=callback.message,
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:activate")
    async def cb_activate(callback: CallbackQuery) -> None:
        if await handle_blocked_callback(db, callback):
            return
        user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        links = await db.get_active_links_for_user(user_id)
        if not links:
            await edit_or_send(
                callback,
                text="У вас нет активных прокси для активации.",
                reply_markup=main_menu_keyboard(),
                parse_mode=None,
            )
            await callback.answer("Нет активных прокси")
            return

        first_link = str(links[0]["link"])
        parsed = parse_socks5_url(first_link)
        if parsed is None:
            await edit_or_send(
                callback,
                text="Не удалось подготовить ссылку активации для первой прокси.",
                reply_markup=main_menu_keyboard(),
                parse_mode=None,
            )
            await callback.answer("Ошибка ссылки", show_alert=True)
            return

        host, port, username, password = parsed
        tg_link = telegram_socks_link(host, port, username, password)
        await edit_or_send(
            callback,
            text="Нажмите кнопку ниже, чтобы активировать первую прокси.",
            reply_markup=activate_first_proxy_keyboard(tg_link),
            parse_mode=None,
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("buymonths:"))
    async def cb_buy_months(callback: CallbackQuery) -> None:
        if await handle_blocked_callback(db, callback):
            return
        parts = callback.data.split(":", maxsplit=1)
        if len(parts) != 2:
            await callback.answer("Неверный формат выбора", show_alert=True)
            return
        months_count = parse_int(parts[1])
        if months_count is None or not is_supported_months(months_count):
            await callback.answer("Некорректный срок", show_alert=True)
            return

        user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        if user_id <= 0:
            await callback.answer("Ошибка профиля", show_alert=True)
            return
        plans = await db.get_plans()
        if not plans:
            await callback.answer("Тарифы не настроены", show_alert=True)
            return

        await edit_or_send(
            callback,
            text=build_devices_step_text(months_count=months_count),
            reply_markup=devices_keyboard(
                plans,
                months_count=months_count,
            ),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("buycfg:"))
    async def cb_buy_cfg(callback: CallbackQuery) -> None:
        if await handle_blocked_callback(db, callback):
            return
        parts = callback.data.split(":", maxsplit=2)
        if len(parts) != 3:
            await callback.answer("Неверный выбор", show_alert=True)
            return
        months_count = parse_int(parts[1])
        plan_code = parts[2]
        if months_count is None or not is_supported_months(months_count):
            await callback.answer("Некорректный срок", show_alert=True)
            return

        await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        plan = await db.get_plan(plan_code)
        if plan is None:
            await callback.answer("Тариф не найден", show_alert=True)
            return

        await edit_or_send(
            callback,
            text=(
                f"{tg_emoji(EMOJI_SHIELD, '🛡')} <b>Этап 3/3</b>\n\n"
                f"Срок: <b>{months_count} {month_word(months_count)}</b>\n"
                f"Прокси в пакете: <b>{plan.devices_count}</b>\n"
                f"Сумма: <b>{total_price_rub(monthly_price_rub=plan.price_rub, months_count=months_count)}₽</b>\n\n"
                "Выберите, на кого оформить доступ."
            ),
            reply_markup=purchase_target_keyboard(months_count=months_count, plan_code=plan.code),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("buytarget:"))
    async def cb_buy_target(callback: CallbackQuery, state: FSMContext) -> None:
        if await handle_blocked_callback(db, callback):
            return
        parts = callback.data.split(":", maxsplit=3)
        if len(parts) != 4:
            await callback.answer("Неверный выбор", show_alert=True)
            return

        action = parts[1]
        months_count = parse_int(parts[2])
        plan_code = parts[3]
        if months_count is None or not is_supported_months(months_count):
            await callback.answer("Некорректный срок", show_alert=True)
            return

        buyer_user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        plan = await db.get_plan(plan_code)
        if plan is None:
            await callback.answer("Тариф не найден", show_alert=True)
            return

        if action != "friend":
            await hide_friend_picker_reply_keyboard_if_needed(
                state=state,
                bot=callback.bot,
                tg_user_id=callback.from_user.id,
            )
            await state.clear()

        if action == "back":
            plans = await db.get_plans()
            if callback.message is not None:
                try:
                    await callback.message.delete()
                except TelegramBadRequest:
                    pass
            await callback.bot.send_message(
                callback.from_user.id,
                build_devices_step_text(months_count=months_count),
                reply_markup=devices_keyboard(
                    plans,
                    months_count=months_count,
                ),
                parse_mode="HTML",
            )
            await callback.answer()
            return

        if action == "friend":
            await state.set_state(PurchaseStates.waiting_friend_tg_id)
            await state.update_data(months_count=months_count, plan_code=plan.code)
            await edit_or_send(
                callback,
                text=(
                    "Отправьте данные друга, для которого оформить покупку:\n"
                    "• <b>tg_user_id</b>\n"
                    "• <b>@username</b>\n"
                    "• или <b>контакт</b> пользователя\n\n"
                    "Пример: <code>123456789</code> или <code>@friend_username</code>"
                ),
                reply_markup=friend_target_input_keyboard(months_count=months_count, plan_code=plan.code),
                parse_mode="HTML",
            )
            await callback.bot.send_message(
                callback.from_user.id,
                f"{tg_emoji('5226501399914755198', '⬇️')} Нажмите кнопку ниже, чтобы выбрать пользователя в Telegram.",
                reply_markup=friend_user_picker_keyboard(),
                parse_mode="HTML",
            )
            await callback.answer()
            return

        if action != "self":
            await callback.answer("Неизвестный вариант", show_alert=True)
            return

        amount_rub = total_price_rub(
            monthly_price_rub=plan.price_rub,
            months_count=months_count,
        )
        try:
            payment_id, confirmation_url = await create_checkout_payment(
                buyer_user_id=buyer_user_id,
                buyer_tg_user_id=callback.from_user.id,
                recipient_tg_user_id=callback.from_user.id,
                plan=plan,
                months_count=months_count,
            )
        except YooKassaError as exc:
            logger.warning("Could not create YooKassa payment: %s", exc)
            await edit_or_send(
                callback,
                text=f"Не удалось создать платеж в ЮKassa.\n{exc}",
                reply_markup=main_menu_keyboard(),
                parse_mode=None,
            )
            await callback.answer("Ошибка оплаты", show_alert=True)
            return
        await edit_or_send(
            callback,
            text=await build_payment_message(
                plan=plan,
                months_count=months_count,
                amount_rub=amount_rub,
                payment_id=payment_id,
                buyer_tg_user_id=callback.from_user.id,
                target_tg_user_id=callback.from_user.id,
                has_yookassa=bool(confirmation_url),
            ),
            reply_markup=payment_keyboard(
                payment_id,
                confirmation_url=confirmation_url,
            ),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.message(PurchaseStates.waiting_friend_tg_id)
    async def state_waiting_friend_tg_id(message: Message, state: FSMContext) -> None:
        if await handle_blocked_message(db, message):
            return
        if message.from_user is None:
            return

        target_tg_user_id: int | None = None
        if message.user_shared is not None:
            shared_user_id = int(message.user_shared.user_id)
            if shared_user_id <= 0:
                await message.answer("Не удалось получить user_id выбранного пользователя.")
                return
            target_tg_user_id = shared_user_id
        elif message.contact is not None:
            contact_user_id = int(message.contact.user_id) if message.contact.user_id is not None else None
            if contact_user_id is None or contact_user_id <= 0:
                await message.answer(
                    "В этом контакте нет Telegram user_id.\n"
                    "Отправьте tg_user_id или @username пользователя."
                )
                return
            target_tg_user_id = contact_user_id
        else:
            payload = extract_text_payload(message)
            if payload is None:
                await message.answer("Отправьте tg_user_id, @username или контакт пользователя.")
                return
            if payload.strip().lower() in {"отмена", "отмена выбора", "cancel"}:
                await state.clear()
                await message.answer("Выбор пользователя отменен.", reply_markup=ReplyKeyboardRemove())
                await message.answer(
                    build_welcome_text(),
                    reply_markup=main_menu_keyboard(),
                )
                return

            parsed_id = parse_int(payload)
            if parsed_id is not None and parsed_id > 0:
                target_tg_user_id = parsed_id
            else:
                username_candidate = normalize_username_candidate(payload)
                if username_candidate is None:
                    await message.answer(
                        "Не удалось распознать пользователя.\n"
                        "Отправьте tg_user_id, @username либо контакт."
                    )
                    return
                username_row = await db.get_user_by_username(username_candidate)
                if username_row is None:
                    await message.answer(
                        "Пользователь с таким username не найден в базе бота.\n"
                        "Попросите его сначала нажать /start, либо отправьте его tg_user_id."
                    )
                    return
                target_tg_user_id = int(username_row["tg_user_id"])

        data = await state.get_data()
        months_count = parse_int(str(data.get("months_count", "")))
        plan_code = str(data.get("plan_code") or "").strip()
        if months_count is None or not is_supported_months(months_count) or not plan_code:
            await state.clear()
            await message.answer(
                "Сессия покупки устарела. Начните заново через /buy.",
                reply_markup=ReplyKeyboardRemove(),
            )
            await message.answer(
                build_welcome_text(),
                reply_markup=main_menu_keyboard(),
            )
            return

        buyer_user_id = await ensure_user(
            db,
            message.from_user,
            bot=message.bot,
            admin_tg_ids=admin_ids,
        )
        plan = await db.get_plan(plan_code)
        if plan is None:
            await state.clear()
            await message.answer(
                "Тариф не найден. Начните заново через /buy.",
                reply_markup=ReplyKeyboardRemove(),
            )
            return

        recipient_profile = await ensure_recipient_profile(target_tg_user_id)

        amount_rub = total_price_rub(
            monthly_price_rub=plan.price_rub,
            months_count=months_count,
        )
        try:
            payment_id, confirmation_url = await create_checkout_payment(
                buyer_user_id=buyer_user_id,
                buyer_tg_user_id=message.from_user.id,
                recipient_tg_user_id=target_tg_user_id,
                plan=plan,
                months_count=months_count,
            )
        except YooKassaError as exc:
            logger.warning("Could not create YooKassa payment for friend: %s", exc)
            await message.answer(
                f"Не удалось создать платеж в ЮKassa.\n{exc}",
                reply_markup=ReplyKeyboardRemove(),
            )
            await message.answer(
                build_welcome_text(),
                reply_markup=main_menu_keyboard(),
            )
            return

        await state.clear()
        await message.answer("Пользователь выбран.", reply_markup=ReplyKeyboardRemove())
        await message.answer(
            await build_payment_message(
                plan=plan,
                months_count=months_count,
                amount_rub=amount_rub,
                payment_id=payment_id,
                buyer_tg_user_id=message.from_user.id,
                target_tg_user_id=target_tg_user_id,
                has_yookassa=bool(confirmation_url),
            ),
            reply_markup=payment_keyboard(
                payment_id,
                confirmation_url=confirmation_url,
            ),
            parse_mode="HTML",
        )

    @router.callback_query(F.data.startswith("paystars:"))
    async def cb_pay_stars(callback: CallbackQuery) -> None:
        if await handle_blocked_callback(db, callback):
            return
        payment_id_raw = callback.data.split(":", maxsplit=1)[1]
        if not payment_id_raw.isdigit():
            await callback.answer("Некорректный платеж", show_alert=True)
            return

        payment_id = int(payment_id_raw)
        user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        payment = await db.get_payment_for_user(payment_id=payment_id, user_id=user_id)
        if payment is None:
            await callback.answer("Платеж не найден", show_alert=True)
            return
        if payment["status"] != "pending":
            await callback.answer("Платеж уже обработан", show_alert=True)
            return

        plan = await db.get_plan(payment["plan_code"])
        if plan is None:
            await callback.answer("Тариф не найден", show_alert=True)
            return

        recipient_tg_user_id = int(payment.get("target_tg_user_id") or callback.from_user.id)
        recipient_profile = await ensure_recipient_profile(recipient_tg_user_id)

        amount_rub = int(payment["amount_rub"])
        months_count = max(1, int(payment.get("months_count") or 1))
        stars_amount = rub_to_stars(amount_rub)
        description = (
            f"{plan.devices_count} прокси, {months_count} {month_word(months_count)}. "
            f"Кому: {payment_target_label(buyer_tg_user_id=callback.from_user.id, target_tg_user_id=recipient_tg_user_id)}"
        )
        try:
            await callback.bot.send_invoice(
                chat_id=callback.from_user.id,
                title=f"{BOT_BRAND} - оплата звездами",
                description=description,
                payload=f"stars:{payment_id}",
                provider_token="",
                currency="XTR",
                prices=[LabeledPrice(label=f"Подписка {BOT_BRAND}", amount=stars_amount)],
                start_parameter=f"whiteproxy-stars-{payment_id}",
            )
        except TelegramBadRequest as exc:
            logger.warning("Could not send Stars invoice for payment %s: %s", payment_id, exc)
            await callback.answer("Не удалось отправить счет в звездах", show_alert=True)
            return

        await callback.answer("Счет в звездах отправлен")

    @router.pre_checkout_query()
    async def pre_checkout(pre_checkout_query: PreCheckoutQuery) -> None:
        try:
            blocked_text = await blocked_text_for_user(db, pre_checkout_query.from_user.id)
            if blocked_text is not None:
                await pre_checkout_query.answer(ok=False, error_message="Доступ к боту ограничен.")
                return

            payload = str(pre_checkout_query.invoice_payload or "")
            if not payload.startswith("stars:"):
                await pre_checkout_query.answer(ok=False, error_message="Некорректный тип платежа.")
                return
            payment_id_raw = payload.split(":", maxsplit=1)[1]
            if not payment_id_raw.isdigit():
                await pre_checkout_query.answer(ok=False, error_message="Некорректный платеж.")
                return
            if pre_checkout_query.currency != "XTR":
                await pre_checkout_query.answer(ok=False, error_message="Поддерживается только оплата звездами.")
                return

            user_id = await ensure_user(
                db,
                pre_checkout_query.from_user,
                bot=pre_checkout_query.bot,
                admin_tg_ids=admin_ids,
            )
            payment = await db.get_payment_for_user(payment_id=int(payment_id_raw), user_id=user_id)
            if payment is None or payment["status"] != "pending":
                await pre_checkout_query.answer(ok=False, error_message="Платеж недоступен.")
                return

            expected_stars = rub_to_stars(int(payment["amount_rub"]))
            if int(pre_checkout_query.total_amount) != expected_stars:
                await pre_checkout_query.answer(ok=False, error_message="Некорректная сумма платежа.")
                return

            await pre_checkout_query.answer(ok=True)
        except Exception:
            logger.exception(
                "Pre-checkout handler failed: user_id=%s payload=%s",
                pre_checkout_query.from_user.id,
                pre_checkout_query.invoice_payload,
            )
            try:
                await pre_checkout_query.answer(
                    ok=False,
                    error_message="Не удалось проверить платеж. Попробуйте еще раз.",
                )
            except TelegramBadRequest:
                pass

    @router.message(F.successful_payment)
    async def successful_payment(message: Message) -> None:
        if message.from_user is None or message.successful_payment is None:
            return
        payment_info = message.successful_payment
        if payment_info.currency != "XTR":
            return

        payload = str(payment_info.invoice_payload or "")
        if not payload.startswith("stars:"):
            return
        payment_id_raw = payload.split(":", maxsplit=1)[1]
        if not payment_id_raw.isdigit():
            await message.answer("Некорректный payload звездного платежа.")
            return

        payment_id = int(payment_id_raw)
        user_id = await ensure_user(
            db,
            message.from_user,
            bot=message.bot,
            admin_tg_ids=admin_ids,
        )
        payment = await db.get_payment_for_user(payment_id=payment_id, user_id=user_id)
        if payment is None:
            await message.answer("Платеж не найден.")
            return
        if payment["status"] != "pending":
            return

        expected_stars = rub_to_stars(int(payment["amount_rub"]))
        if int(payment_info.total_amount) != expected_stars:
            logger.warning(
                "Stars amount mismatch: payment_id=%s got=%s expected=%s",
                payment_id,
                payment_info.total_amount,
                expected_stars,
            )
            await message.answer("Оплата получена с некорректной суммой. Обратитесь в поддержку.")
            return

        plan = await db.get_plan(payment["plan_code"])
        if plan is None:
            await message.answer("Тариф не найден.")
            return

        recipient_tg_user_id = int(payment.get("target_tg_user_id") or message.from_user.id)
        recipient_profile = await ensure_recipient_profile(recipient_tg_user_id)

        months_count = max(1, int(payment.get("months_count") or 1))
        expires_at = int(
            (datetime.now(tz=timezone.utc) + timedelta(days=plan.duration_days * months_count)).timestamp()
        )
        activated = await db.activate_payment_and_create_subscription_from_pool(
            payment_id=payment_id,
            payer_user_id=user_id,
            recipient_user_id=recipient_profile.id,
            plan_code=plan.code,
            expires_at=expires_at,
            devices_count=plan.devices_count,
            proxy_public_host=proxy_public_host,
        )
        if activated is None:
            free_count = await db.count_free_pool()
            await message.answer(
                (
                    "Оплата получена, но сейчас недостаточно свободных прокси.\n"
                    f"Свободно: {free_count}.\n"
                    "Обратитесь в поддержку."
                ),
                reply_markup=main_menu_keyboard(),
            )
            return
        subscription_id, created_proxies = activated

        user_proxy_label = user_proxy_label_from_profile(recipient_profile)
        proxies: list[dict[str, int | str | None]] = []
        for index, proxy in enumerate(created_proxies, start=1):
            tg_link = telegram_socks_link(
                proxy_public_host,
                int(proxy["port"]),
                str(proxy["username"]),
                str(proxy["password"]),
            )
            proxies.append(
                {
                    "index": index,
                    "proxy_id": int(proxy["proxy_id"]),
                    "tg_link": tg_link,
                    "subscription_id": subscription_id,
                    "device_number": int(proxy["device_number"]),
                }
            )

        if recipient_profile.tg_user_id == message.from_user.id:
            await send_proxy_sequence(
                db=db,
                bot=message.bot,
                bot_chat_id=message.from_user.id,
                user_id=user_id,
                tg_user_id=message.from_user.id,
                user_proxy_label=user_proxy_label,
                proxies=proxies,
                delivery_source="purchase",
                source_message=None,
                include_first_proxy_button=True,
            )
            await message.answer("Звездный платеж подтвержден, прокси выданы.")
            return

        delivery_text = (
            f"Звездный платеж подтвержден.\n"
            f"Подарок активирован для пользователя <code>{recipient_profile.tg_user_id}</code>."
        )
        try:
            await send_proxy_sequence(
                db=db,
                bot=message.bot,
                bot_chat_id=recipient_profile.tg_user_id,
                user_id=recipient_profile.id,
                tg_user_id=recipient_profile.tg_user_id,
                user_proxy_label=user_proxy_label,
                proxies=proxies,
                delivery_source="purchase",
                source_message=None,
                include_first_proxy_button=True,
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            delivery_text += "\nНе удалось отправить прокси получателю (пусть запустит /start)."
        await message.answer(delivery_text, reply_markup=main_menu_keyboard(), parse_mode="HTML")

    @router.callback_query(F.data.startswith("cancelpay:"))
    async def cb_cancel_payment(callback: CallbackQuery) -> None:
        if await handle_blocked_callback(db, callback):
            return
        payment_id_raw = callback.data.split(":", maxsplit=1)[1]
        if not payment_id_raw.isdigit():
            await callback.answer("Некорректный платеж", show_alert=True)
            return

        user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        payment = await db.get_payment_for_user(payment_id=int(payment_id_raw), user_id=user_id)
        if payment is None:
            await callback.answer("Платеж не найден", show_alert=True)
            return
        if payment["status"] != "pending":
            await callback.answer("Платеж уже обработан", show_alert=True)
            return

        yookassa_payment_id = str(payment.get("yookassa_payment_id") or "").strip()
        if not yookassa_payment_id:
            if not yk.enabled:
                cancelled = await db.cancel_pending_payment(int(payment_id_raw), user_id)
                if cancelled:
                    await edit_or_send(
                        callback,
                        text="Заявка отменена.",
                        reply_markup=main_menu_keyboard(),
                        parse_mode="HTML",
                    )
                    await callback.answer("Отменено")
                else:
                    await callback.answer("Заявка уже обработана", show_alert=True)
                return
            await callback.answer(
                "Для платежа звездами отмена недоступна. Просто не оплачивайте счет.",
                show_alert=True,
            )
            return
        if yookassa_payment_id:
            try:
                remote_status = await yk.get_payment_status(yookassa_payment_id)
            except YooKassaError:
                remote_status = ""
            if remote_status == "succeeded":
                await callback.answer("Платеж уже оплачен. Нажмите «Активировать».", show_alert=True)
                return

        cancelled = await db.cancel_pending_payment(int(payment_id_raw), user_id)
        if cancelled:
            await edit_or_send(
                callback,
                text="Платеж отменен.",
                reply_markup=main_menu_keyboard(),
                parse_mode="HTML",
            )
            await callback.answer("Отменено")
        else:
            await callback.answer("Платеж уже обработан", show_alert=True)

    @router.callback_query(F.data.startswith("pay:"))
    async def cb_pay(callback: CallbackQuery) -> None:
        if await handle_blocked_callback(db, callback):
            return
        payment_id_raw = callback.data.split(":", maxsplit=1)[1]
        if not payment_id_raw.isdigit():
            await callback.answer("Некорректный платеж", show_alert=True)
            return

        payment_id = int(payment_id_raw)
        user_id = await ensure_user(
            db,
            callback.from_user,
            bot=callback.bot,
            admin_tg_ids=admin_ids,
        )
        payment = await db.get_payment_for_user(payment_id=payment_id, user_id=user_id)
        if payment is None:
            await callback.answer("Платеж не найден", show_alert=True)
            return

        if payment["status"] != "pending":
            await callback.answer("Платеж уже обработан", show_alert=True)
            return

        plan = await db.get_plan(payment["plan_code"])
        if plan is None:
            await callback.answer("Тариф не найден", show_alert=True)
            return

        recipient_tg_user_id = int(payment.get("target_tg_user_id") or callback.from_user.id)
        recipient_profile = await ensure_recipient_profile(recipient_tg_user_id)

        yookassa_payment_id = str(payment.get("yookassa_payment_id") or "").strip()
        if yookassa_payment_id:
            try:
                remote_status = await yk.get_payment_status(yookassa_payment_id)
            except YooKassaError as exc:
                logger.warning("Could not check YooKassa payment %s: %s", yookassa_payment_id, exc)
                await callback.answer("Не удалось проверить статус оплаты", show_alert=True)
                return

            if remote_status != "succeeded":
                confirmation_url = str(payment.get("yookassa_confirmation_url") or "")
                if confirmation_url and callback.message is not None:
                    try:
                        await callback.message.edit_reply_markup(
                            reply_markup=payment_keyboard(
                                payment_id,
                                confirmation_url=confirmation_url,
                            )
                        )
                    except TelegramBadRequest:
                        pass
                await callback.answer("Платеж пока не завершен", show_alert=True)
                return
        else:
            if not yk.enabled:
                # Mock-режим: ЮKassa не настроена, считаем подтверждение кнопкой "Мнимо оплатил".
                pass
            else:
                await callback.answer(
                    "Этот платеж не через ЮKassa. Оплатите звездами ⭐️ (кнопка выше), выдача пройдет автоматически.",
                    show_alert=True,
                )
                return

        months_count = max(1, int(payment.get("months_count") or 1))
        expires_at = int(
            (datetime.now(tz=timezone.utc) + timedelta(days=plan.duration_days * months_count)).timestamp()
        )
        activated = await db.activate_payment_and_create_subscription_from_pool(
            payment_id=payment_id,
            payer_user_id=user_id,
            recipient_user_id=recipient_profile.id,
            plan_code=plan.code,
            expires_at=expires_at,
            devices_count=plan.devices_count,
            proxy_public_host=proxy_public_host,
        )
        if activated is None:
            free_count = await db.count_free_pool()
            await edit_or_send(
                callback,
                text=(
                    "Сейчас в пуле недостаточно свободных прокси для этого тарифа.\n"
                    f"Свободно прямо сейчас: {free_count}.\n"
                    "Попробуйте позже."
                ),
                reply_markup=main_menu_keyboard(),
                parse_mode=None,
            )
            await callback.answer("Недостаточно свободных прокси")
            return
        subscription_id, created_proxies = activated

        user_proxy_label = user_proxy_label_from_profile(recipient_profile)
        proxies: list[dict[str, int | str | None]] = []
        for index, proxy in enumerate(created_proxies, start=1):
            tg_link = telegram_socks_link(
                proxy_public_host,
                int(proxy["port"]),
                str(proxy["username"]),
                str(proxy["password"]),
            )
            proxy_id = int(proxy["proxy_id"])
            proxies.append(
                {
                    "index": index,
                    "proxy_id": proxy_id,
                    "tg_link": tg_link,
                    "subscription_id": subscription_id,
                    "device_number": int(proxy["device_number"]),
                }
            )

        if recipient_profile.tg_user_id == callback.from_user.id:
            await send_proxy_sequence(
                db=db,
                bot=callback.bot,
                bot_chat_id=callback.from_user.id,
                user_id=user_id,
                tg_user_id=callback.from_user.id,
                user_proxy_label=user_proxy_label,
                proxies=proxies,
                delivery_source="purchase",
                source_message=callback.message,
                include_first_proxy_button=True,
            )
            await callback.answer("Готово")
            return

        delivery_text = (
            f"Подарок активирован для пользователя <code>{recipient_profile.tg_user_id}</code>.\n"
            "Прокси отправлены получателю."
        )
        try:
            await send_proxy_sequence(
                db=db,
                bot=callback.bot,
                bot_chat_id=recipient_profile.tg_user_id,
                user_id=recipient_profile.id,
                tg_user_id=recipient_profile.tg_user_id,
                user_proxy_label=user_proxy_label,
                proxies=proxies,
                delivery_source="purchase",
                source_message=None,
                include_first_proxy_button=True,
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            delivery_text = (
                f"Подарок активирован для пользователя <code>{recipient_profile.tg_user_id}</code>.\n"
                "Не удалось отправить сообщение получателю. "
                "Пусть пользователь запустит бота командой /start и нажмет «Мои прокси»."
            )
        await edit_or_send(
            callback,
            text=delivery_text,
            reply_markup=main_menu_keyboard(),
            parse_mode="HTML",
        )
        await callback.answer("Готово")
        return

    return router
