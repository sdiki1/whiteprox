from __future__ import annotations

import secrets
from typing import Any
from urllib.parse import quote

import psycopg
from psycopg.rows import dict_row

from .database import DEFAULT_PLANS, Plan, ProxyPoolEntry, now_ts


class PostgresDatabase:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn: psycopg.AsyncConnection | None = None

    @property
    def conn(self) -> psycopg.AsyncConnection:
        if self._conn is None:
            raise RuntimeError("Database is not connected.")
        return self._conn

    async def connect(self) -> None:
        self._conn = await psycopg.AsyncConnection.connect(self.dsn, row_factory=dict_row)
        await self._conn.set_autocommit(True)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def init_schema(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    tg_user_id BIGINT NOT NULL UNIQUE,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    referral_code TEXT UNIQUE,
                    referrer_user_id BIGINT REFERENCES users(id),
                    referral_balance_rub INTEGER NOT NULL DEFAULT 0,
                    referral_total_earned_rub INTEGER NOT NULL DEFAULT 0,
                    referral_total_debited_rub INTEGER NOT NULL DEFAULT 0,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plans (
                    code TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    devices_count INTEGER NOT NULL,
                    price_rub INTEGER NOT NULL,
                    duration_days INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS payments (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(id),
                    plan_code TEXT NOT NULL REFERENCES plans(code),
                    amount_rub INTEGER NOT NULL,
                    months_count INTEGER NOT NULL DEFAULT 1,
                    target_tg_user_id BIGINT,
                    yookassa_payment_id TEXT,
                    yookassa_confirmation_url TEXT,
                    status TEXT NOT NULL CHECK(status IN ('pending', 'paid', 'cancelled')),
                    created_at BIGINT NOT NULL,
                    paid_at BIGINT
                );

                CREATE TABLE IF NOT EXISTS subscriptions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(id),
                    plan_code TEXT NOT NULL REFERENCES plans(code),
                    payment_id BIGINT NOT NULL UNIQUE REFERENCES payments(id),
                    status TEXT NOT NULL CHECK(status IN ('active', 'expired')),
                    created_at BIGINT NOT NULL,
                    expires_at BIGINT NOT NULL,
                    notified_expired INTEGER NOT NULL DEFAULT 0,
                    notified_expiring_2days INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS proxy_links (
                    id BIGSERIAL PRIMARY KEY,
                    subscription_id BIGINT NOT NULL REFERENCES subscriptions(id),
                    user_id BIGINT NOT NULL REFERENCES users(id),
                    device_number INTEGER NOT NULL,
                    token TEXT NOT NULL UNIQUE,
                    link TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('active', 'expired')),
                    created_at BIGINT NOT NULL,
                    expires_at BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS proxy_pool (
                    id BIGSERIAL PRIMARY KEY,
                    port INTEGER NOT NULL UNIQUE,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('free', 'assigned')),
                    assigned_link_id BIGINT UNIQUE REFERENCES proxy_links(id),
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS proxy_delivery_logs (
                    id BIGSERIAL PRIMARY KEY,
                    proxy_link_id BIGINT NOT NULL REFERENCES proxy_links(id),
                    user_id BIGINT NOT NULL REFERENCES users(id),
                    tg_user_id BIGINT NOT NULL,
                    user_label TEXT NOT NULL,
                    subscription_id BIGINT REFERENCES subscriptions(id),
                    device_number INTEGER,
                    delivery_source TEXT NOT NULL CHECK(delivery_source IN ('purchase', 'my_links')),
                    proxy_url TEXT NOT NULL,
                    delivered_at BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS user_temp_messages (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(id),
                    tg_user_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    kind TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    UNIQUE(user_id, message_id, kind)
                );

                CREATE TABLE IF NOT EXISTS banned_users (
                    id BIGSERIAL PRIMARY KEY,
                    tg_user_id BIGINT NOT NULL UNIQUE,
                    reason TEXT NOT NULL,
                    blocked_by BIGINT,
                    blocked_at BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS referral_transactions (
                    id BIGSERIAL PRIMARY KEY,
                    referrer_user_id BIGINT NOT NULL REFERENCES users(id),
                    referred_user_id BIGINT REFERENCES users(id),
                    payment_id BIGINT REFERENCES payments(id),
                    amount_rub INTEGER NOT NULL,
                    direction TEXT NOT NULL CHECK(direction IN ('credit', 'debit')),
                    comment TEXT,
                    created_at BIGINT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_users_tg_user_id ON users(tg_user_id);
                CREATE INDEX IF NOT EXISTS idx_users_referrer_user_id ON users(referrer_user_id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_users_referral_code
                    ON users(referral_code)
                    WHERE referral_code IS NOT NULL AND referral_code <> '';
                CREATE INDEX IF NOT EXISTS idx_payments_user_status ON payments(user_id, status);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_yookassa_payment_id
                    ON payments(yookassa_payment_id)
                    WHERE yookassa_payment_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_subscriptions_user_status ON subscriptions(user_id, status);
                CREATE INDEX IF NOT EXISTS idx_subscriptions_expires_at ON subscriptions(expires_at);
                CREATE INDEX IF NOT EXISTS idx_proxy_links_user_status ON proxy_links(user_id, status);
                CREATE INDEX IF NOT EXISTS idx_proxy_links_expires_at ON proxy_links(expires_at);
                CREATE INDEX IF NOT EXISTS idx_proxy_pool_status ON proxy_pool(status);
                CREATE INDEX IF NOT EXISTS idx_proxy_delivery_logs_tg_user_id ON proxy_delivery_logs(tg_user_id);
                CREATE INDEX IF NOT EXISTS idx_proxy_delivery_logs_proxy_link_id ON proxy_delivery_logs(proxy_link_id);
                CREATE INDEX IF NOT EXISTS idx_user_temp_messages_user_kind ON user_temp_messages(user_id, kind);
                CREATE INDEX IF NOT EXISTS idx_banned_users_tg_user_id ON banned_users(tg_user_id);
                CREATE INDEX IF NOT EXISTS idx_referral_transactions_referrer
                    ON referral_transactions(referrer_user_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_referral_transactions_payment
                    ON referral_transactions(payment_id);
                """
            )
        await self.ensure_payments_columns()
        await self.ensure_subscriptions_columns()
        await self.ensure_users_referral_columns()
        await self.ensure_referral_transactions_table()
        await self.seed_plans()
        await self.conn.commit()

    async def ensure_payments_columns(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                ALTER TABLE payments
                ADD COLUMN IF NOT EXISTS months_count INTEGER NOT NULL DEFAULT 1
                """
            )
            await cur.execute(
                """
                ALTER TABLE payments
                ADD COLUMN IF NOT EXISTS target_tg_user_id BIGINT
                """
            )
            await cur.execute(
                """
                ALTER TABLE payments
                ADD COLUMN IF NOT EXISTS yookassa_payment_id TEXT
                """
            )
            await cur.execute(
                """
                ALTER TABLE payments
                ADD COLUMN IF NOT EXISTS yookassa_confirmation_url TEXT
                """
            )
            await cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_yookassa_payment_id
                ON payments(yookassa_payment_id)
                WHERE yookassa_payment_id IS NOT NULL
                """
            )
        await self.conn.commit()

    async def ensure_subscriptions_columns(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                ALTER TABLE subscriptions
                ADD COLUMN IF NOT EXISTS notified_expired INTEGER NOT NULL DEFAULT 0
                """
            )
            await cur.execute(
                """
                ALTER TABLE subscriptions
                ADD COLUMN IF NOT EXISTS notified_expiring_2days INTEGER NOT NULL DEFAULT 0
                """
            )
        await self.conn.commit()

    async def ensure_users_referral_columns(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS referral_code TEXT
                """
            )
            await cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS referrer_user_id BIGINT REFERENCES users(id)
                """
            )
            await cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS referral_balance_rub INTEGER NOT NULL DEFAULT 0
                """
            )
            await cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS referral_total_earned_rub INTEGER NOT NULL DEFAULT 0
                """
            )
            await cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS referral_total_debited_rub INTEGER NOT NULL DEFAULT 0
                """
            )
            await cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_users_referrer_user_id ON users(referrer_user_id)
                """
            )
            await cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_users_referral_code
                    ON users(referral_code)
                    WHERE referral_code IS NOT NULL AND referral_code <> ''
                """
            )
        await self.conn.commit()

    async def ensure_referral_transactions_table(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS referral_transactions (
                    id BIGSERIAL PRIMARY KEY,
                    referrer_user_id BIGINT NOT NULL REFERENCES users(id),
                    referred_user_id BIGINT REFERENCES users(id),
                    payment_id BIGINT REFERENCES payments(id),
                    amount_rub INTEGER NOT NULL,
                    direction TEXT NOT NULL CHECK(direction IN ('credit', 'debit')),
                    comment TEXT,
                    created_at BIGINT NOT NULL
                )
                """
            )
            await cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_referral_transactions_referrer
                    ON referral_transactions(referrer_user_id, created_at DESC)
                """
            )
            await cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_referral_transactions_payment
                    ON referral_transactions(payment_id)
                """
            )
        await self.conn.commit()

    async def seed_plans(self) -> None:
        async with self.conn.cursor() as cur:
            for plan in DEFAULT_PLANS:
                await cur.execute(
                    """
                    INSERT INTO plans (code, title, devices_count, price_rub, duration_days)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(code) DO UPDATE SET
                        title = EXCLUDED.title,
                        devices_count = EXCLUDED.devices_count,
                        price_rub = EXCLUDED.price_rub,
                        duration_days = EXCLUDED.duration_days
                    """,
                    (plan.code, plan.title, plan.devices_count, plan.price_rub, plan.duration_days),
                )
        await self.conn.commit()

    async def sync_proxy_pool(self, entries: list[ProxyPoolEntry]) -> None:
        timestamp = now_ts()
        async with self.conn.cursor() as cur:
            for item in entries:
                await cur.execute(
                    """
                    INSERT INTO proxy_pool (port, username, password, status, created_at, updated_at)
                    VALUES (%s, %s, %s, 'free', %s, %s)
                    ON CONFLICT(port) DO UPDATE SET
                        username = EXCLUDED.username,
                        password = EXCLUDED.password,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (item.port, item.username, item.password, timestamp, timestamp),
                )

            ports = [item.port for item in entries]
            if ports:
                await cur.execute(
                    """
                    DELETE FROM proxy_pool
                    WHERE status = 'free' AND NOT (port = ANY(%s))
                    """,
                    (ports,),
                )
            else:
                await cur.execute(
                    """
                    DELETE FROM proxy_pool
                    WHERE status = 'free'
                    """
                )
        await self.conn.commit()

    async def upsert_user(
        self,
        tg_user_id: int,
        username: str | None,
        first_name: str | None,
        last_name: str | None,
    ) -> int:
        timestamp = now_ts()
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO users (tg_user_id, username, first_name, last_name, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(tg_user_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    updated_at = EXCLUDED.updated_at
                RETURNING id
                """,
                (tg_user_id, username, first_name, last_name, timestamp, timestamp),
            )
            row = await cur.fetchone()
        await self.conn.commit()
        if row is None:
            raise RuntimeError("Failed to upsert user.")
        return int(row["id"])

    async def get_user_by_tg_user_id(self, tg_user_id: int) -> dict[str, Any] | None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, tg_user_id, username, first_name, last_name, created_at, updated_at
                FROM users
                WHERE tg_user_id = %s
                """,
                (tg_user_id,),
            )
            row = await cur.fetchone()
        return dict(row) if row is not None else None

    async def get_user_by_username(self, username: str) -> dict[str, Any] | None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, tg_user_id, username, first_name, last_name, created_at, updated_at
                FROM users
                WHERE username IS NOT NULL AND LOWER(username) = LOWER(%s)
                LIMIT 1
                """,
                (username,),
            )
            row = await cur.fetchone()
        return dict(row) if row is not None else None

    async def bind_referrer_by_tg_user_ids(
        self,
        *,
        referred_tg_user_id: int,
        referrer_tg_user_id: int,
    ) -> bool:
        if referred_tg_user_id <= 0 or referrer_tg_user_id <= 0:
            return False
        if referred_tg_user_id == referrer_tg_user_id:
            return False

        async with self.conn.cursor() as cur:
            await cur.execute(
                "SELECT id FROM users WHERE tg_user_id = %s",
                (referred_tg_user_id,),
            )
            referred_row = await cur.fetchone()
            if referred_row is None:
                return False
            referred_user_id = int(referred_row["id"])

            await cur.execute(
                "SELECT id FROM users WHERE tg_user_id = %s",
                (referrer_tg_user_id,),
            )
            referrer_row = await cur.fetchone()
            if referrer_row is None:
                return False
            referrer_user_id = int(referrer_row["id"])
            if referrer_user_id == referred_user_id:
                return False

            await cur.execute(
                """
                UPDATE users
                SET referrer_user_id = %s, updated_at = %s
                WHERE id = %s AND referrer_user_id IS NULL
                """,
                (referrer_user_id, now_ts(), referred_user_id),
            )
            changed = cur.rowcount > 0
        await self.conn.commit()
        return changed

    @staticmethod
    def _generate_referral_code() -> str:
        return secrets.token_hex(5).upper()

    async def get_or_create_referral_code(self, *, user_id: int) -> str:
        async with self.conn.cursor() as cur:
            await cur.execute(
                "SELECT referral_code FROM users WHERE id = %s",
                (user_id,),
            )
            row = await cur.fetchone()
            if row is not None and row["referral_code"]:
                return str(row["referral_code"])

        for _ in range(20):
            candidate = self._generate_referral_code()
            try:
                async with self.conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE users
                        SET referral_code = %s, updated_at = %s
                        WHERE id = %s AND (referral_code IS NULL OR referral_code = '')
                        """,
                        (candidate, now_ts(), user_id),
                    )
                    if cur.rowcount > 0:
                        await self.conn.commit()
                        return candidate
                    await cur.execute(
                        "SELECT referral_code FROM users WHERE id = %s",
                        (user_id,),
                    )
                    row = await cur.fetchone()
                if row is not None and row["referral_code"]:
                    return str(row["referral_code"])
            except psycopg.errors.UniqueViolation:
                continue

        raise RuntimeError("Failed to create referral code.")

    async def rotate_referral_code(self, *, user_id: int) -> str:
        for _ in range(20):
            candidate = self._generate_referral_code()
            try:
                async with self.conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE users
                        SET referral_code = %s, updated_at = %s
                        WHERE id = %s
                        """,
                        (candidate, now_ts(), user_id),
                    )
                    if cur.rowcount <= 0:
                        raise RuntimeError("User not found for referral code rotation.")
                await self.conn.commit()
                return candidate
            except psycopg.errors.UniqueViolation:
                continue

        raise RuntimeError("Failed to rotate referral code.")

    async def bind_referrer_by_code(
        self,
        *,
        referred_tg_user_id: int,
        referral_code: str,
    ) -> bool:
        code = (referral_code or "").strip().upper()
        if referred_tg_user_id <= 0 or not code:
            return False

        async with self.conn.cursor() as cur:
            await cur.execute(
                "SELECT id FROM users WHERE tg_user_id = %s",
                (referred_tg_user_id,),
            )
            referred_row = await cur.fetchone()
            if referred_row is None:
                return False
            referred_user_id = int(referred_row["id"])

            await cur.execute(
                "SELECT id FROM users WHERE referral_code = %s LIMIT 1",
                (code,),
            )
            referrer_row = await cur.fetchone()
            if referrer_row is None:
                return False
            referrer_user_id = int(referrer_row["id"])
            if referred_user_id == referrer_user_id:
                return False

            await cur.execute(
                """
                UPDATE users
                SET referrer_user_id = %s, updated_at = %s
                WHERE id = %s AND referrer_user_id IS NULL
                """,
                (referrer_user_id, now_ts(), referred_user_id),
            )
            changed = cur.rowcount > 0
        await self.conn.commit()
        return changed

    async def get_referral_summary_for_user(self, user_id: int) -> dict[str, int]:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    referral_balance_rub,
                    referral_total_earned_rub,
                    referral_total_debited_rub
                FROM users
                WHERE id = %s
                """,
                (user_id,),
            )
            row = await cur.fetchone()
            await cur.execute(
                "SELECT COUNT(*) AS cnt FROM users WHERE referrer_user_id = %s",
                (user_id,),
            )
            referrals_row = await cur.fetchone()

        return {
            "balance_rub": int(row["referral_balance_rub"]) if row is not None else 0,
            "earned_rub": int(row["referral_total_earned_rub"]) if row is not None else 0,
            "debited_rub": int(row["referral_total_debited_rub"]) if row is not None else 0,
            "referrals_count": int(referrals_row["cnt"]) if referrals_row is not None else 0,
        }

    async def get_referral_admin_summary(self) -> dict[str, int]:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE referrer_user_id IS NOT NULL) AS users_with_referrer,
                    COALESCE(SUM(referral_balance_rub), 0) AS total_balance_rub,
                    COALESCE(SUM(referral_total_earned_rub), 0) AS total_earned_rub,
                    COALESCE(SUM(referral_total_debited_rub), 0) AS total_debited_rub
                FROM users
                """
            )
            row = await cur.fetchone()
        if row is None:
            return {
                "users_with_referrer": 0,
                "total_balance_rub": 0,
                "total_earned_rub": 0,
                "total_debited_rub": 0,
            }
        return {
            "users_with_referrer": int(row["users_with_referrer"]),
            "total_balance_rub": int(row["total_balance_rub"]),
            "total_earned_rub": int(row["total_earned_rub"]),
            "total_debited_rub": int(row["total_debited_rub"]),
        }

    async def debit_referral_balance_by_tg_user_id(
        self,
        *,
        tg_user_id: int,
        amount_rub: int,
        comment: str | None = None,
    ) -> tuple[bool, int]:
        amount = max(0, int(amount_rub))
        if tg_user_id <= 0 or amount <= 0:
            return False, 0

        await self.conn.execute("BEGIN")
        try:
            async with self.conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, referral_balance_rub
                    FROM users
                    WHERE tg_user_id = %s
                    FOR UPDATE
                    """,
                    (tg_user_id,),
                )
                row = await cur.fetchone()
                if row is None:
                    await self.conn.rollback()
                    return False, 0

                user_id = int(row["id"])
                balance = int(row["referral_balance_rub"])
                if balance < amount:
                    await self.conn.rollback()
                    return False, balance

                timestamp = now_ts()
                await cur.execute(
                    """
                    UPDATE users
                    SET
                        referral_balance_rub = referral_balance_rub - %s,
                        referral_total_debited_rub = referral_total_debited_rub + %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (amount, amount, timestamp, user_id),
                )
                await cur.execute(
                    """
                    INSERT INTO referral_transactions (
                        referrer_user_id,
                        referred_user_id,
                        payment_id,
                        amount_rub,
                        direction,
                        comment,
                        created_at
                    )
                    VALUES (%s, NULL, NULL, %s, 'debit', %s, %s)
                    """,
                    (user_id, amount, (comment or "").strip() or None, timestamp),
                )
                await cur.execute(
                    "SELECT referral_balance_rub FROM users WHERE id = %s",
                    (user_id,),
                )
                new_row = await cur.fetchone()
            await self.conn.commit()
            return True, int(new_row["referral_balance_rub"]) if new_row is not None else 0
        except Exception:
            await self.conn.rollback()
            raise

    async def get_all_tg_user_ids(self) -> list[int]:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT tg_user_id
                FROM users
                ORDER BY id ASC
                """
            )
            rows = await cur.fetchall()
        return [int(row["tg_user_id"]) for row in rows]

    async def list_users_with_stats(self, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
        timestamp = now_ts()
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    u.id,
                    u.tg_user_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.created_at,
                    u.updated_at,
                    SUM(CASE WHEN pl.status = 'active' AND pl.expires_at > %s THEN 1 ELSE 0 END) AS active_proxies,
                    CASE WHEN bu.tg_user_id IS NULL THEN 0 ELSE 1 END AS is_banned
                FROM users u
                LEFT JOIN proxy_links pl ON pl.user_id = u.id
                LEFT JOIN banned_users bu ON bu.tg_user_id = u.tg_user_id
                GROUP BY u.id, bu.tg_user_id
                ORDER BY u.created_at DESC
                LIMIT %s OFFSET %s
                """,
                (timestamp, max(1, limit), max(0, offset)),
            )
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_plan(self, code: str) -> Plan | None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT code, title, devices_count, price_rub, duration_days
                FROM plans
                WHERE code = %s
                """,
                (code,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return Plan(
            code=row["code"],
            title=row["title"],
            devices_count=int(row["devices_count"]),
            price_rub=int(row["price_rub"]),
            duration_days=int(row["duration_days"]),
        )

    async def get_plans(self) -> list[Plan]:
        plan_codes = [plan.code for plan in DEFAULT_PLANS]
        if not plan_codes:
            return []
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT code, title, devices_count, price_rub, duration_days
                FROM plans
                WHERE code = ANY(%s)
                ORDER BY devices_count ASC
                """,
                (plan_codes,),
            )
            rows = await cur.fetchall()
        return [
            Plan(
                code=row["code"],
                title=row["title"],
                devices_count=int(row["devices_count"]),
                price_rub=int(row["price_rub"]),
                duration_days=int(row["duration_days"]),
            )
            for row in rows
        ]

    async def create_payment(
        self,
        user_id: int,
        plan_code: str,
        amount_rub: int,
        *,
        months_count: int = 1,
        target_tg_user_id: int | None = None,
        yookassa_payment_id: str | None = None,
        yookassa_confirmation_url: str | None = None,
    ) -> int:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO payments (
                    user_id,
                    plan_code,
                    amount_rub,
                    months_count,
                    target_tg_user_id,
                    yookassa_payment_id,
                    yookassa_confirmation_url,
                    status,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s)
                RETURNING id
                """,
                (
                    user_id,
                    plan_code,
                    amount_rub,
                    max(1, months_count),
                    target_tg_user_id,
                    yookassa_payment_id,
                    yookassa_confirmation_url,
                    now_ts(),
                ),
            )
            row = await cur.fetchone()
        await self.conn.commit()
        if row is None:
            raise RuntimeError("Failed to create payment.")
        return int(row["id"])

    async def get_payment_for_user(self, payment_id: int, user_id: int) -> dict[str, Any] | None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    id,
                    user_id,
                    plan_code,
                    amount_rub,
                    months_count,
                    target_tg_user_id,
                    yookassa_payment_id,
                    yookassa_confirmation_url,
                    status,
                    created_at,
                    paid_at
                FROM payments
                WHERE id = %s AND user_id = %s
                """,
                (payment_id, user_id),
            )
            row = await cur.fetchone()
        return dict(row) if row is not None else None

    async def get_payment_by_yookassa_payment_id(self, yookassa_payment_id: str) -> dict[str, Any] | None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    id,
                    user_id,
                    plan_code,
                    amount_rub,
                    months_count,
                    target_tg_user_id,
                    yookassa_payment_id,
                    yookassa_confirmation_url,
                    status,
                    created_at,
                    paid_at
                FROM payments
                WHERE yookassa_payment_id = %s
                """,
                (yookassa_payment_id,),
            )
            row = await cur.fetchone()
        return dict(row) if row is not None else None

    async def cancel_pending_payment(self, payment_id: int, user_id: int) -> bool:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE payments
                SET status = 'cancelled'
                WHERE id = %s AND user_id = %s AND status = 'pending'
                """,
                (payment_id, user_id),
            )
            changed = cur.rowcount > 0
        await self.conn.commit()
        return changed

    async def count_free_pool(self) -> int:
        async with self.conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) AS cnt FROM proxy_pool WHERE status = 'free'"
            )
            row = await cur.fetchone()
        return int(row["cnt"]) if row is not None else 0

    async def activate_payment_and_create_subscription_from_pool(
        self,
        *,
        payment_id: int,
        payer_user_id: int,
        recipient_user_id: int,
        plan_code: str,
        expires_at: int,
        devices_count: int,
        proxy_public_host: str,
    ) -> tuple[int, list[dict[str, Any]]] | None:
        timestamp = now_ts()
        await self.conn.execute("BEGIN")
        try:
            async with self.conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE payments
                    SET status = 'paid', paid_at = %s
                    WHERE id = %s AND user_id = %s AND status = 'pending'
                    """,
                    (timestamp, payment_id, payer_user_id),
                )
                if cur.rowcount == 0:
                    await self.conn.rollback()
                    return None

                await cur.execute(
                    "SELECT amount_rub FROM payments WHERE id = %s",
                    (payment_id,),
                )
                payment_row = await cur.fetchone()
                payment_amount_rub = int(payment_row["amount_rub"]) if payment_row is not None else 0
                if payment_amount_rub > 0:
                    await cur.execute(
                        "SELECT referrer_user_id FROM users WHERE id = %s",
                        (payer_user_id,),
                    )
                    ref_row = await cur.fetchone()
                    referrer_user_id = (
                        int(ref_row["referrer_user_id"])
                        if ref_row is not None and ref_row["referrer_user_id"] is not None
                        else None
                    )
                    referral_amount = payment_amount_rub // 2
                    if (
                        referrer_user_id is not None
                        and referrer_user_id > 0
                        and referrer_user_id != payer_user_id
                        and referral_amount > 0
                    ):
                        await cur.execute(
                            """
                            UPDATE users
                            SET
                                referral_balance_rub = referral_balance_rub + %s,
                                referral_total_earned_rub = referral_total_earned_rub + %s,
                                updated_at = %s
                            WHERE id = %s
                            """,
                            (referral_amount, referral_amount, timestamp, referrer_user_id),
                        )
                        await cur.execute(
                            """
                            INSERT INTO referral_transactions (
                                referrer_user_id,
                                referred_user_id,
                                payment_id,
                                amount_rub,
                                direction,
                                comment,
                                created_at
                            )
                            VALUES (%s, %s, %s, %s, 'credit', %s, %s)
                            """,
                            (
                                referrer_user_id,
                                payer_user_id,
                                payment_id,
                                referral_amount,
                                "referral reward 50%",
                                timestamp,
                            ),
                        )

                await cur.execute(
                    """
                    SELECT id, port, username, password
                    FROM proxy_pool
                    WHERE status = 'free'
                    ORDER BY port ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (devices_count,),
                )
                proxy_rows = await cur.fetchall()
                if len(proxy_rows) < devices_count:
                    await self.conn.rollback()
                    return None

                await cur.execute(
                    """
                    INSERT INTO subscriptions (user_id, plan_code, payment_id, status, created_at, expires_at)
                    VALUES (%s, %s, %s, 'active', %s, %s)
                    RETURNING id
                    """,
                    (recipient_user_id, plan_code, payment_id, timestamp, expires_at),
                )
                sub_row = await cur.fetchone()
                if sub_row is None:
                    raise RuntimeError("Failed to create subscription.")
                subscription_id = int(sub_row["id"])

                created: list[dict[str, Any]] = []
                for device_number, proxy_row in enumerate(proxy_rows, start=1):
                    port = int(proxy_row["port"])
                    username = str(proxy_row["username"])
                    password = str(proxy_row["password"])

                    username_safe = quote(username, safe="")
                    password_safe = quote(password, safe="")
                    link = f"socks5://{username_safe}:{password_safe}@{proxy_public_host}:{port}"

                    await cur.execute(
                        """
                        INSERT INTO proxy_links (
                            subscription_id, user_id, device_number, token, link, status, created_at, expires_at
                        )
                        VALUES (%s, %s, %s, %s, %s, 'active', %s, %s)
                        RETURNING id
                        """,
                        (
                            subscription_id,
                            recipient_user_id,
                            device_number,
                            secrets.token_urlsafe(18),
                            link,
                            timestamp,
                            expires_at,
                        ),
                    )
                    link_row = await cur.fetchone()
                    if link_row is None:
                        raise RuntimeError("Failed to create proxy link.")
                    link_id = int(link_row["id"])

                    await cur.execute(
                        """
                        UPDATE proxy_pool
                        SET status = 'assigned', assigned_link_id = %s, updated_at = %s
                        WHERE id = %s AND status = 'free'
                        """,
                        (link_id, timestamp, int(proxy_row["id"])),
                    )
                    if cur.rowcount == 0:
                        raise RuntimeError("Failed to assign proxy from pool")

                    created.append(
                        {
                            "proxy_id": link_id,
                            "device_number": device_number,
                            "port": port,
                            "username": username,
                            "password": password,
                            "link": link,
                        }
                    )

            await self.conn.commit()
            return subscription_id, created
        except Exception:
            await self.conn.rollback()
            raise

    async def get_user_by_id(self, user_id: int) -> dict[str, Any] | None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, tg_user_id, username, first_name, last_name, created_at, updated_at
                FROM users
                WHERE id = %s
                """,
                (user_id,),
            )
            row = await cur.fetchone()
        return dict(row) if row is not None else None

    async def log_proxy_delivery(
        self,
        *,
        proxy_link_id: int,
        user_id: int,
        tg_user_id: int,
        user_label: str,
        subscription_id: int | None,
        device_number: int | None,
        delivery_source: str,
        proxy_url: str,
    ) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO proxy_delivery_logs (
                    proxy_link_id,
                    user_id,
                    tg_user_id,
                    user_label,
                    subscription_id,
                    device_number,
                    delivery_source,
                    proxy_url,
                    delivered_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    proxy_link_id,
                    user_id,
                    tg_user_id,
                    user_label,
                    subscription_id,
                    device_number,
                    delivery_source,
                    proxy_url,
                    now_ts(),
                ),
            )
        await self.conn.commit()

    async def add_temp_message(
        self,
        *,
        user_id: int,
        tg_user_id: int,
        message_id: int,
        kind: str,
    ) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO user_temp_messages (user_id, tg_user_id, message_id, kind, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, message_id, kind) DO NOTHING
                """,
                (user_id, tg_user_id, message_id, kind, now_ts()),
            )
        await self.conn.commit()

    async def pop_temp_messages(self, *, user_id: int, kind: str) -> list[dict[str, Any]]:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, tg_user_id, message_id
                FROM user_temp_messages
                WHERE user_id = %s AND kind = %s
                ORDER BY id ASC
                """,
                (user_id, kind),
            )
            rows = await cur.fetchall()
            if rows:
                await cur.execute(
                    """
                    DELETE FROM user_temp_messages
                    WHERE user_id = %s AND kind = %s
                    """,
                    (user_id, kind),
                )
        await self.conn.commit()
        return [dict(row) for row in rows]

    async def get_user_ban(self, tg_user_id: int) -> dict[str, Any] | None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT tg_user_id, reason, blocked_by, blocked_at
                FROM banned_users
                WHERE tg_user_id = %s
                """,
                (tg_user_id,),
            )
            row = await cur.fetchone()
        return dict(row) if row is not None else None

    async def ban_user(self, tg_user_id: int, reason: str, blocked_by: int | None = None) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO banned_users (tg_user_id, reason, blocked_by, blocked_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tg_user_id) DO UPDATE SET
                    reason = EXCLUDED.reason,
                    blocked_by = EXCLUDED.blocked_by,
                    blocked_at = EXCLUDED.blocked_at
                """,
                (tg_user_id, reason, blocked_by, now_ts()),
            )
        await self.conn.commit()

    async def unban_user(self, tg_user_id: int) -> bool:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM banned_users
                WHERE tg_user_id = %s
                """,
                (tg_user_id,),
            )
            changed = cur.rowcount > 0
        await self.conn.commit()
        return changed

    async def get_all_links_for_user(self, user_id: int) -> list[dict[str, Any]]:
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    pl.id,
                    pl.subscription_id,
                    pl.device_number,
                    pl.link,
                    pl.status,
                    pl.created_at,
                    pl.expires_at,
                    p.title AS plan_title
                FROM proxy_links pl
                LEFT JOIN subscriptions s ON s.id = pl.subscription_id
                LEFT JOIN plans p ON p.code = s.plan_code
                WHERE pl.user_id = %s
                ORDER BY pl.created_at DESC, pl.id DESC
                """,
                (user_id,),
            )
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_active_links_for_user(self, user_id: int) -> list[dict[str, Any]]:
        timestamp = now_ts()
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    pl.id,
                    pl.subscription_id,
                    pl.device_number,
                    pl.link,
                    pl.expires_at,
                    p.title AS plan_title
                FROM proxy_links pl
                JOIN subscriptions s ON s.id = pl.subscription_id
                JOIN plans p ON p.code = s.plan_code
                WHERE
                    pl.user_id = %s
                    AND pl.status = 'active'
                    AND pl.expires_at > %s
                    AND s.status = 'active'
                ORDER BY pl.expires_at ASC, pl.subscription_id ASC, pl.device_number ASC
                """,
                (user_id, timestamp),
            )
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_active_subscriptions_for_user(self, user_id: int) -> list[dict[str, Any]]:
        timestamp = now_ts()
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    s.id,
                    s.plan_code,
                    s.expires_at,
                    p.title AS plan_title,
                    p.price_rub,
                    p.devices_count
                FROM subscriptions s
                JOIN plans p ON p.code = s.plan_code
                WHERE s.user_id = %s AND s.status = 'active' AND s.expires_at > %s
                ORDER BY s.expires_at ASC
                """,
                (user_id, timestamp),
            )
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def revoke_proxy_link_for_user(self, user_id: int, proxy_link_id: int) -> bool:
        timestamp = now_ts()
        await self.conn.execute("BEGIN")
        try:
            async with self.conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, subscription_id
                    FROM proxy_links
                    WHERE id = %s AND user_id = %s AND status = 'active'
                    """,
                    (proxy_link_id, user_id),
                )
                row = await cur.fetchone()
                if row is None:
                    await self.conn.rollback()
                    return False

                subscription_id = int(row["subscription_id"])

                await cur.execute(
                    """
                    UPDATE proxy_links
                    SET status = 'expired', expires_at = %s
                    WHERE id = %s
                    """,
                    (timestamp, proxy_link_id),
                )
                await cur.execute(
                    """
                    UPDATE proxy_pool
                    SET status = 'free', assigned_link_id = NULL, updated_at = %s
                    WHERE assigned_link_id = %s
                    """,
                    (timestamp, proxy_link_id),
                )
                await cur.execute(
                    """
                    UPDATE subscriptions
                    SET status = 'expired'
                    WHERE id = %s AND status = 'active' AND NOT EXISTS (
                        SELECT 1
                        FROM proxy_links
                        WHERE subscription_id = %s AND status = 'active' AND expires_at > %s
                    )
                    """,
                    (subscription_id, subscription_id, timestamp),
                )
            await self.conn.commit()
            return True
        except Exception:
            await self.conn.rollback()
            raise

    async def revoke_all_active_links_for_user(self, user_id: int) -> int:
        timestamp = now_ts()
        await self.conn.execute("BEGIN")
        try:
            async with self.conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id
                    FROM proxy_links
                    WHERE user_id = %s AND status = 'active' AND expires_at > %s
                    """,
                    (user_id, timestamp),
                )
                rows = await cur.fetchall()
                if not rows:
                    await self.conn.rollback()
                    return 0

                link_ids = [int(row["id"]) for row in rows]

                await cur.execute(
                    """
                    UPDATE proxy_links
                    SET status = 'expired', expires_at = %s
                    WHERE id = ANY(%s)
                    """,
                    (timestamp, link_ids),
                )
                await cur.execute(
                    """
                    UPDATE proxy_pool
                    SET status = 'free', assigned_link_id = NULL, updated_at = %s
                    WHERE assigned_link_id = ANY(%s)
                    """,
                    (timestamp, link_ids),
                )
                await cur.execute(
                    """
                    UPDATE subscriptions
                    SET status = 'expired'
                    WHERE user_id = %s AND status = 'active' AND NOT EXISTS (
                        SELECT 1
                        FROM proxy_links
                        WHERE subscription_id = subscriptions.id AND status = 'active' AND expires_at > %s
                    )
                    """,
                    (user_id, timestamp),
                )
            await self.conn.commit()
            return len(link_ids)
        except Exception:
            await self.conn.rollback()
            raise

    async def get_expiring_in_two_days_and_mark_notified_users(self) -> list[int]:
        timestamp = now_ts()
        threshold = timestamp + 2 * 24 * 60 * 60
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                SELECT s.id AS subscription_id, u.tg_user_id
                FROM subscriptions s
                JOIN users u ON u.id = s.user_id
                WHERE s.status = 'active'
                  AND s.expires_at > %s
                  AND s.expires_at <= %s
                  AND s.notified_expiring_2days = 0
                """,
                (timestamp, threshold),
            )
            rows = await cur.fetchall()
            if not rows:
                return []

            subscription_ids = [int(row["subscription_id"]) for row in rows]
            await cur.execute(
                """
                UPDATE subscriptions
                SET notified_expiring_2days = 1
                WHERE id = ANY(%s)
                """,
                (subscription_ids,),
            )
        await self.conn.commit()
        return list(dict.fromkeys(int(row["tg_user_id"]) for row in rows))

    async def expire_due_and_get_notified_users(self) -> list[int]:
        timestamp = now_ts()
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE subscriptions
                SET status = 'expired'
                WHERE status = 'active' AND expires_at <= %s
                """,
                (timestamp,),
            )
            await cur.execute(
                """
                UPDATE proxy_links
                SET status = 'expired'
                WHERE status = 'active' AND expires_at <= %s
                """,
                (timestamp,),
            )
            await cur.execute(
                """
                UPDATE proxy_pool
                SET status = 'free', assigned_link_id = NULL, updated_at = %s
                WHERE assigned_link_id IN (
                    SELECT id FROM proxy_links WHERE status = 'expired'
                )
                """,
                (timestamp,),
            )

            await cur.execute(
                """
                SELECT DISTINCT u.tg_user_id
                FROM subscriptions s
                JOIN users u ON u.id = s.user_id
                WHERE s.status = 'expired' AND s.notified_expired = 0
                """
            )
            rows = await cur.fetchall()

            await cur.execute(
                """
                UPDATE subscriptions
                SET notified_expired = 1
                WHERE status = 'expired' AND notified_expired = 0
                """
            )
        await self.conn.commit()
        return [int(row["tg_user_id"]) for row in rows]
