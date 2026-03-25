from __future__ import annotations

from dataclasses import dataclass
import base64
import uuid

import aiohttp


class YooKassaError(RuntimeError):
    pass


@dataclass(frozen=True)
class YooKassaCreateResult:
    payment_id: str
    status: str
    confirmation_url: str


class YooKassaClient:
    def __init__(self, *, shop_id: str, secret_key: str, return_url: str) -> None:
        self.shop_id = shop_id.strip()
        self.secret_key = secret_key.strip()
        self.return_url = return_url.strip() or "https://t.me"

    @property
    def enabled(self) -> bool:
        return bool(self.shop_id and self.secret_key)

    def _auth_header(self) -> str:
        token = f"{self.shop_id}:{self.secret_key}".encode("utf-8")
        encoded = base64.b64encode(token).decode("ascii")
        return f"Basic {encoded}"

    async def create_payment(
        self,
        *,
        amount_rub: int,
        description: str,
        metadata: dict[str, str] | None = None,
    ) -> YooKassaCreateResult:
        amount_value = f"{amount_rub:.2f}"
        payload: dict[str, object] = {
            "amount": {"value": amount_value, "currency": "RUB"},
            "capture": True,
            "confirmation": {"type": "redirect", "return_url": self.return_url},
            "description": description,
        }
        if metadata:
            payload["metadata"] = metadata

        idempotence_key = str(uuid.uuid4())
        headers = {
            "Authorization": self._auth_header(),
            "Idempotence-Key": idempotence_key,
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.post(
                "https://api.yookassa.ru/v3/payments",
                json=payload,
                headers=headers,
            ) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    description_text = data.get("description") if isinstance(data, dict) else None
                    raise YooKassaError(
                        f"ЮKassa create payment failed: HTTP {response.status} {description_text or ''}".strip()
                    )

        payment_id = str(data.get("id") or "")
        status = str(data.get("status") or "")
        confirmation = data.get("confirmation") if isinstance(data, dict) else None
        confirmation_url = ""
        if isinstance(confirmation, dict):
            confirmation_url = str(confirmation.get("confirmation_url") or "")

        if not payment_id or not confirmation_url:
            raise YooKassaError("ЮKassa вернула неполный ответ (нет id или confirmation_url).")

        return YooKassaCreateResult(
            payment_id=payment_id,
            status=status,
            confirmation_url=confirmation_url,
        )

    async def get_payment_status(self, payment_id: str) -> str:
        headers = {"Authorization": self._auth_header()}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.get(
                f"https://api.yookassa.ru/v3/payments/{payment_id}",
                headers=headers,
            ) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    description_text = data.get("description") if isinstance(data, dict) else None
                    raise YooKassaError(
                        f"ЮKassa status failed: HTTP {response.status} {description_text or ''}".strip()
                    )

        status = str(data.get("status") or "")
        if not status:
            raise YooKassaError("ЮKassa вернула пустой статус платежа.")
        return status
