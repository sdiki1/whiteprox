from __future__ import annotations

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    KeyboardButtonRequestUser,
    ReplyKeyboardMarkup,
)

from .database import Plan
from .pricing import total_price_rub


EMOJI_SHIELD = "5407025283456835913"
EMOJI_GEM = "5330319637156479518"
EMOJI_DEV = "5418063924933173277"
EMOJI_BOX = "5298975240708187753"
EMOJI_GLASSES = "5474385437403395055"
EMOJI_STAR = "5463289097336405244"
EMOJI_CARD = "5472250091332993630"
EMOJI_DONE="5427009714745517609"
EMOJI_CANCEL = "5465665476971471368"
EMOJI_DOCS = "5433653135799228968"

def _button(
    *,
    text: str,
    callback_data: str | None = None,
    url: str | None = None,
    style: str | None = None,
    icon_custom_emoji_id: str | None = None,
) -> InlineKeyboardButton:
    if (callback_data is None) == (url is None):
        raise ValueError("Specify exactly one of callback_data or url.")
    kwargs: dict[str, str] = {}
    if style:
        kwargs["style"] = style
    if icon_custom_emoji_id:
        kwargs["icon_custom_emoji_id"] = icon_custom_emoji_id
    if callback_data is not None:
        kwargs["callback_data"] = callback_data
    if url is not None:
        kwargs["url"] = url
    return InlineKeyboardButton(text=text, **kwargs)


def _month_word(count: int) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return "месяц"
    if count % 10 in (2, 3, 4) and count % 100 not in (12, 13, 14):
        return "месяца"
    return "месяцев"


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                _button(
                    text="Оформить доступ",
                    callback_data="menu:plans",
                    style="success",
                    icon_custom_emoji_id=EMOJI_SHIELD,
                ),
            ],
            [
                _button(
                    text="Мои конфиги",
                    callback_data="menu:links",
                    style="primary",
                    icon_custom_emoji_id=EMOJI_DOCS,
                ),
                _button(
                    text="Проверить статус",
                    callback_data="menu:status",
                    icon_custom_emoji_id=EMOJI_BOX,
                )
            ],
            [
                _button(
                    text="Инструкция",
                    callback_data="menu:guide",
                    style="primary",
                    icon_custom_emoji_id=EMOJI_DOCS,
                )
            ]
        ]
    )


def months_keyboard() -> InlineKeyboardMarkup:
    options = (1, 3, 6, 12)
    rows: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []
    for value in options:
        current_row.append(
            _button(
                text=f"{value} {_month_word(value)}",
                callback_data=f"buymonths:{value}",
                style="primary",
                icon_custom_emoji_id=EMOJI_BOX,
            )
        )
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append(
        [
            _button(
                text="Меню",
                callback_data="menu:home_clear",
                icon_custom_emoji_id=EMOJI_SHIELD,
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def devices_keyboard(
    plans: list[Plan],
    *,
    months_count: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for plan in plans:
        total_amount = total_price_rub(
            monthly_price_rub=plan.price_rub,
            months_count=months_count,
        )
        button_style = "primary"
        button_icon = EMOJI_BOX
        total_label = f"{total_amount}₽ за {months_count} {_month_word(months_count)}"
        rows.append(
            [
                _button(
                    text=(
                        f"{plan.devices_count} прокси"
                        f" • {total_label}"
                    ),
                    callback_data=f"buycfg:{months_count}:{plan.code}",
                    style=button_style,
                    icon_custom_emoji_id=button_icon,
                )
            ]
        )
    rows.append(
        [
            _button(
                text="Назад",
                callback_data="menu:plans",
            )
        ]
    )
    rows.append(
        [
            _button(
                text="Меню",
                callback_data="menu:home_clear",
                icon_custom_emoji_id=EMOJI_SHIELD,
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def plans_keyboard(plans: list[Plan]) -> InlineKeyboardMarkup:
    return devices_keyboard(plans, months_count=1)


def purchase_target_keyboard(*, months_count: int, plan_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                _button(
                    text="Себе",
                    callback_data=f"buytarget:self:{months_count}:{plan_code}",
                    style="success",
                ),
                _button(
                    text="Другу",
                    callback_data=f"buytarget:friend:{months_count}:{plan_code}",
                    style="primary",
                ),
            ],
            [
                _button(
                    text="Назад",
                    callback_data=f"buymonths:{months_count}",
                )
            ],
            [
                _button(
                    text="Меню",
                    callback_data="menu:home_clear",
                    icon_custom_emoji_id=EMOJI_SHIELD
                )
            ],
        ]
    )


def friend_target_input_keyboard(*, months_count: int, plan_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                _button(
                    text="Назад",
                    callback_data=f"buytarget:back:{months_count}:{plan_code}",
                )
            ],
            [
                _button(
                    text="Меню",
                    callback_data="menu:home_clear",
                    icon_custom_emoji_id=EMOJI_SHIELD
                )
            ],
        ]
    )


def friend_user_picker_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(
                    text="Выбрать пользователя",
                    request_user=KeyboardButtonRequestUser(
                        request_id=1,
                        user_is_bot=False,
                    ),
                )
            ]
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
        selective=True,
        input_field_placeholder="",
    )


def payment_keyboard(
    payment_id: int,
    *,
    confirmation_url: str | None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if confirmation_url:
        rows.append(
            [
                _button(
                    text="Оплатить через ЮKassa",
                    url=confirmation_url,
                    style="primary",
                    icon_custom_emoji_id=EMOJI_CARD,
                )
            ]
        )
    rows.append(
        [
            _button(
                text="TG STARS ⭐️",
                callback_data=f"paystars:{payment_id}",
                style="primary",
                icon_custom_emoji_id=EMOJI_STAR,
            )
        ]
    )
    rows.append(
        [
            _button(
                text="СБП 🇷🇺" if confirmation_url else "СБП 🇷🇺",
                callback_data=f"pay:{payment_id}",
                style="success",
                icon_custom_emoji_id=EMOJI_DONE,
            )
        ]
    )
    rows.append(
        [
            _button(
                text="Отменить",
                callback_data=f"cancelpay:{payment_id}",
                style="danger",
                icon_custom_emoji_id=EMOJI_CANCEL,
            )
        ]
    )
    rows.append(
        [
            _button(
                text="Меню",
                callback_data="menu:home_clear",
                icon_custom_emoji_id=EMOJI_SHIELD,
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def activate_first_proxy_keyboard(first_proxy_link: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                _button(
                    text="Активировать",
                    url=first_proxy_link,
                    style="success",
                    icon_custom_emoji_id=EMOJI_DONE,
                )
            ],
            [
                _button(
                    text="Меню",
                    callback_data="menu:home_clear",
                    icon_custom_emoji_id=EMOJI_SHIELD,
                )
            ],
        ]
    )


def activate_proxy_keyboard(proxy_link: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                _button(
                    text="Активировать",
                    url=proxy_link,
                    style="success",
                    icon_custom_emoji_id=EMOJI_DONE,
                )
            ]
        ]
    )


def subscriptions_actions_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                _button(
                    text="Оформить доступ",
                    callback_data="menu:plans",
                    style="success",
                    icon_custom_emoji_id=EMOJI_SHIELD,
                ),
            ],
            [
                _button(
                    text="Мои конфиги",
                    callback_data="menu:links",
                    style="primary",
                    icon_custom_emoji_id=EMOJI_DOCS,
                ),
                _button(
                    text="Проверить статус",
                    callback_data="menu:status",
                    icon_custom_emoji_id=EMOJI_BOX,
                )
            ],
            [
                _button(
                    text="Инструкция",
                    callback_data="menu:guide",
                    style="primary",
                    icon_custom_emoji_id=EMOJI_DOCS,
                )
            ],
            [
                _button(
                    text="Меню",
                    callback_data="menu:home_clear",
                    icon_custom_emoji_id=EMOJI_SHIELD,
                )
            ],
        ]
    )


def back_to_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                _button(
                    text="Меню",
                    callback_data="menu:home_clear",
                    icon_custom_emoji_id=EMOJI_SHIELD,
                )
            ]
        ]
    )


def admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                _button(text="1) Рассылка всем", callback_data="admin:broadcast_all", style="primary"),
                _button(text="2) Рассылка юзеру", callback_data="admin:broadcast_user", style="primary"),
            ],
            [
                _button(text="3) Забанить", callback_data="admin:ban", style="danger"),
                _button(text="4) Разбанить", callback_data="admin:unban", style="success"),
            ],
            [
                _button(text="5) Список юзеров", callback_data="admin:list_users"),
            ],
            [
                _button(text="6) Конфиги юзера", callback_data="admin:user_configs"),
            ],
            [
                _button(text="7) Начислить прокси", callback_data="admin:grant_proxies", style="success"),
                _button(text="8) Удалить прокси", callback_data="admin:remove_proxies", style="danger"),
            ],
            [
                _button(text="Закрыть", callback_data="admin:close"),
            ],
        ]
    )


def admin_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                _button(text="Отмена", callback_data="admin:cancel", style="danger"),
                _button(text="Меню админа", callback_data="admin:menu", style="primary"),
            ]
        ]
    )
