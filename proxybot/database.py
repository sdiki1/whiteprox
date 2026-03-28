from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import secrets
import time
from typing import Any
from urllib.parse import quote

import aiosqlite


@dataclass(frozen=True)
class Plan:
    code: str
    title: str
    devices_count: int
    price_rub: int
    duration_days: int


@dataclass(frozen=True)
class ProxyPoolEntry:
    port: int
    username: str
    password: str


DEFAULT_PLANS = (
    Plan(code="one", title="1 прокси", devices_count=1, price_rub=99, duration_days=30),
)
DEFAULT_PLAN_CODES = tuple(plan.code for plan in DEFAULT_PLANS)


def now_ts() -> int:
    return int(time.time())


class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: aiosqlite.Connection | None = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database is not connected.")
        return self._conn

    async def connect(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON;")
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def init_schema(self) -> None:
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_user_id INTEGER NOT NULL UNIQUE,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                referral_code TEXT UNIQUE,
                referrer_user_id INTEGER REFERENCES users(id),
                referral_balance_rub INTEGER NOT NULL DEFAULT 0,
                referral_total_earned_rub INTEGER NOT NULL DEFAULT 0,
                referral_total_debited_rub INTEGER NOT NULL DEFAULT 0,
                purchase_nudge_sent_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS plans (
                code TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                devices_count INTEGER NOT NULL,
                price_rub INTEGER NOT NULL,
                duration_days INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(id),
                plan_code TEXT NOT NULL REFERENCES plans(code),
                amount_rub INTEGER NOT NULL,
                months_count INTEGER NOT NULL DEFAULT 1,
                target_tg_user_id INTEGER,
                yookassa_payment_id TEXT,
                yookassa_confirmation_url TEXT,
                status TEXT NOT NULL CHECK(status IN ('pending', 'paid', 'cancelled')),
                created_at INTEGER NOT NULL,
                paid_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(id),
                plan_code TEXT NOT NULL REFERENCES plans(code),
                payment_id INTEGER NOT NULL UNIQUE REFERENCES payments(id),
                status TEXT NOT NULL CHECK(status IN ('active', 'expired')),
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                notified_expired INTEGER NOT NULL DEFAULT 0,
                notified_expiring_2days INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS proxy_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
                user_id INTEGER NOT NULL REFERENCES users(id),
                device_number INTEGER NOT NULL,
                token TEXT NOT NULL UNIQUE,
                link TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('active', 'expired')),
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS proxy_pool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                port INTEGER NOT NULL UNIQUE,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('free', 'assigned')),
                assigned_link_id INTEGER UNIQUE REFERENCES proxy_links(id),
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS proxy_delivery_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proxy_link_id INTEGER NOT NULL REFERENCES proxy_links(id),
                user_id INTEGER NOT NULL REFERENCES users(id),
                tg_user_id INTEGER NOT NULL,
                user_label TEXT NOT NULL,
                subscription_id INTEGER REFERENCES subscriptions(id),
                device_number INTEGER,
                delivery_source TEXT NOT NULL CHECK(delivery_source IN ('purchase', 'my_links')),
                proxy_url TEXT NOT NULL,
                delivered_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS user_temp_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(id),
                tg_user_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                UNIQUE(user_id, message_id, kind)
            );

            CREATE TABLE IF NOT EXISTS banned_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_user_id INTEGER NOT NULL UNIQUE,
                reason TEXT NOT NULL,
                blocked_by INTEGER,
                blocked_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS referral_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_user_id INTEGER NOT NULL REFERENCES users(id),
                referred_user_id INTEGER REFERENCES users(id),
                payment_id INTEGER REFERENCES payments(id),
                amount_rub INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('credit', 'debit')),
                comment TEXT,
                created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_users_tg_user_id ON users(tg_user_id);
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
        cursor = await self.conn.execute("PRAGMA table_info(payments)")
        rows = await cursor.fetchall()
        await cursor.close()
        existing = {str(row["name"]) for row in rows}

        if "months_count" not in existing:
            await self.conn.execute(
                "ALTER TABLE payments ADD COLUMN months_count INTEGER NOT NULL DEFAULT 1"
            )
        if "target_tg_user_id" not in existing:
            await self.conn.execute(
                "ALTER TABLE payments ADD COLUMN target_tg_user_id INTEGER"
            )
        if "yookassa_payment_id" not in existing:
            await self.conn.execute(
                "ALTER TABLE payments ADD COLUMN yookassa_payment_id TEXT"
            )
        if "yookassa_confirmation_url" not in existing:
            await self.conn.execute(
                "ALTER TABLE payments ADD COLUMN yookassa_confirmation_url TEXT"
            )

        await self.conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_yookassa_payment_id
            ON payments(yookassa_payment_id)
            WHERE yookassa_payment_id IS NOT NULL
            """
        )

    async def ensure_subscriptions_columns(self) -> None:
        cursor = await self.conn.execute("PRAGMA table_info(subscriptions)")
        rows = await cursor.fetchall()
        await cursor.close()
        existing = {str(row["name"]) for row in rows}

        if "notified_expired" not in existing:
            await self.conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN notified_expired INTEGER NOT NULL DEFAULT 0"
            )
        if "notified_expiring_2days" not in existing:
            await self.conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN notified_expiring_2days INTEGER NOT NULL DEFAULT 0"
            )

    async def ensure_users_referral_columns(self) -> None:
        cursor = await self.conn.execute("PRAGMA table_info(users)")
        rows = await cursor.fetchall()
        await cursor.close()
        existing = {str(row["name"]) for row in rows}

        if "referral_code" not in existing:
            await self.conn.execute(
                "ALTER TABLE users ADD COLUMN referral_code TEXT"
            )
        if "referrer_user_id" not in existing:
            await self.conn.execute(
                "ALTER TABLE users ADD COLUMN referrer_user_id INTEGER"
            )
        if "referral_balance_rub" not in existing:
            await self.conn.execute(
                "ALTER TABLE users ADD COLUMN referral_balance_rub INTEGER NOT NULL DEFAULT 0"
            )
        if "referral_total_earned_rub" not in existing:
            await self.conn.execute(
                "ALTER TABLE users ADD COLUMN referral_total_earned_rub INTEGER NOT NULL DEFAULT 0"
            )
        if "referral_total_debited_rub" not in existing:
            await self.conn.execute(
                "ALTER TABLE users ADD COLUMN referral_total_debited_rub INTEGER NOT NULL DEFAULT 0"
            )
        if "purchase_nudge_sent_at" not in existing:
            await self.conn.execute(
                "ALTER TABLE users ADD COLUMN purchase_nudge_sent_at INTEGER"
            )

        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_referrer_user_id ON users(referrer_user_id)"
        )
        await self.conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_users_referral_code
            ON users(referral_code)
            WHERE referral_code IS NOT NULL AND referral_code <> ''
            """
        )

    async def ensure_referral_transactions_table(self) -> None:
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS referral_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_user_id INTEGER NOT NULL REFERENCES users(id),
                referred_user_id INTEGER REFERENCES users(id),
                payment_id INTEGER REFERENCES payments(id),
                amount_rub INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('credit', 'debit')),
                comment TEXT,
                created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_referral_transactions_referrer
                ON referral_transactions(referrer_user_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_referral_transactions_payment
                ON referral_transactions(payment_id);
            """
        )

    async def seed_plans(self) -> None:
        for plan in DEFAULT_PLANS:
            await self.conn.execute(
                """
                INSERT INTO plans (code, title, devices_count, price_rub, duration_days)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    title = excluded.title,
                    devices_count = excluded.devices_count,
                    price_rub = excluded.price_rub,
                    duration_days = excluded.duration_days
                """,
                (plan.code, plan.title, plan.devices_count, plan.price_rub, plan.duration_days),
            )

    async def sync_proxy_pool(self, entries: list[ProxyPoolEntry]) -> None:
        timestamp = now_ts()
        for item in entries:
            await self.conn.execute(
                """
                INSERT INTO proxy_pool (port, username, password, status, created_at, updated_at)
                VALUES (?, ?, ?, 'free', ?, ?)
                ON CONFLICT(port) DO UPDATE SET
                    username = excluded.username,
                    password = excluded.password,
                    updated_at = excluded.updated_at
                """,
                (item.port, item.username, item.password, timestamp, timestamp),
            )

        ports = [item.port for item in entries]
        if ports:
            placeholders = ",".join("?" for _ in ports)
            await self.conn.execute(
                f"""
                DELETE FROM proxy_pool
                WHERE status = 'free' AND port NOT IN ({placeholders})
                """,
                tuple(ports),
            )
        else:
            await self.conn.execute(
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
        await self.conn.execute(
            """
            INSERT INTO users (tg_user_id, username, first_name, last_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(tg_user_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                updated_at = excluded.updated_at
            """,
            (tg_user_id, username, first_name, last_name, timestamp, timestamp),
        )
        cursor = await self.conn.execute(
            "SELECT id FROM users WHERE tg_user_id = ?",
            (tg_user_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        await self.conn.commit()
        if row is None:
            raise RuntimeError("Failed to upsert user.")
        return int(row["id"])

    async def get_user_by_tg_user_id(self, tg_user_id: int) -> dict[str, Any] | None:
        cursor = await self.conn.execute(
            """
            SELECT id, tg_user_id, username, first_name, last_name, created_at, updated_at
            FROM users
            WHERE tg_user_id = ?
            """,
            (tg_user_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row is not None else None

    async def get_user_by_username(self, username: str) -> dict[str, Any] | None:
        cursor = await self.conn.execute(
            """
            SELECT id, tg_user_id, username, first_name, last_name, created_at, updated_at
            FROM users
            WHERE username IS NOT NULL AND LOWER(username) = LOWER(?)
            LIMIT 1
            """,
            (username,),
        )
        row = await cursor.fetchone()
        await cursor.close()
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

        cursor = await self.conn.execute(
            "SELECT id FROM users WHERE tg_user_id = ?",
            (referred_tg_user_id,),
        )
        referred_row = await cursor.fetchone()
        await cursor.close()
        if referred_row is None:
            return False

        cursor = await self.conn.execute(
            "SELECT id FROM users WHERE tg_user_id = ?",
            (referrer_tg_user_id,),
        )
        referrer_row = await cursor.fetchone()
        await cursor.close()
        if referrer_row is None:
            return False

        referred_user_id = int(referred_row["id"])
        referrer_user_id = int(referrer_row["id"])
        if referred_user_id == referrer_user_id:
            return False

        cursor = await self.conn.execute(
            """
            UPDATE users
            SET referrer_user_id = ?, updated_at = ?
            WHERE id = ? AND referrer_user_id IS NULL
            """,
            (referrer_user_id, now_ts(), referred_user_id),
        )
        await self.conn.commit()
        return cursor.rowcount > 0

    @staticmethod
    def _generate_referral_code() -> str:
        return secrets.token_hex(5).upper()

    async def get_or_create_referral_code(self, *, user_id: int) -> str:
        cursor = await self.conn.execute(
            "SELECT referral_code FROM users WHERE id = ?",
            (user_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is not None and row["referral_code"]:
            return str(row["referral_code"])

        for _ in range(20):
            candidate = self._generate_referral_code()
            try:
                cursor = await self.conn.execute(
                    """
                    UPDATE users
                    SET referral_code = ?, updated_at = ?
                    WHERE id = ? AND (referral_code IS NULL OR referral_code = '')
                    """,
                    (candidate, now_ts(), user_id),
                )
            except aiosqlite.IntegrityError:
                continue
            if cursor.rowcount > 0:
                await self.conn.commit()
                return candidate
            cursor = await self.conn.execute(
                "SELECT referral_code FROM users WHERE id = ?",
                (user_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is not None and row["referral_code"]:
                return str(row["referral_code"])

        raise RuntimeError("Failed to create referral code.")

    async def rotate_referral_code(self, *, user_id: int) -> str:
        for _ in range(20):
            candidate = self._generate_referral_code()
            try:
                cursor = await self.conn.execute(
                    """
                    UPDATE users
                    SET referral_code = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (candidate, now_ts(), user_id),
                )
            except aiosqlite.IntegrityError:
                continue
            if cursor.rowcount <= 0:
                raise RuntimeError("User not found for referral code rotation.")
            await self.conn.commit()
            return candidate
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

        cursor = await self.conn.execute(
            "SELECT id FROM users WHERE tg_user_id = ?",
            (referred_tg_user_id,),
        )
        referred_row = await cursor.fetchone()
        await cursor.close()
        if referred_row is None:
            return False
        referred_user_id = int(referred_row["id"])

        cursor = await self.conn.execute(
            "SELECT id FROM users WHERE referral_code = ? LIMIT 1",
            (code,),
        )
        referrer_row = await cursor.fetchone()
        await cursor.close()
        if referrer_row is None:
            return False
        referrer_user_id = int(referrer_row["id"])
        if referred_user_id == referrer_user_id:
            return False

        cursor = await self.conn.execute(
            """
            UPDATE users
            SET referrer_user_id = ?, updated_at = ?
            WHERE id = ? AND referrer_user_id IS NULL
            """,
            (referrer_user_id, now_ts(), referred_user_id),
        )
        await self.conn.commit()
        return cursor.rowcount > 0

    async def get_referral_summary_for_user(self, user_id: int) -> dict[str, int]:
        cursor = await self.conn.execute(
            """
            SELECT
                referral_balance_rub,
                referral_total_earned_rub,
                referral_total_debited_rub
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()

        cursor = await self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM users WHERE referrer_user_id = ?",
            (user_id,),
        )
        referrals_row = await cursor.fetchone()
        await cursor.close()

        return {
            "balance_rub": int(row["referral_balance_rub"]) if row is not None else 0,
            "earned_rub": int(row["referral_total_earned_rub"]) if row is not None else 0,
            "debited_rub": int(row["referral_total_debited_rub"]) if row is not None else 0,
            "referrals_count": int(referrals_row["cnt"]) if referrals_row is not None else 0,
        }

    async def get_referral_admin_summary(self) -> dict[str, int]:
        cursor = await self.conn.execute(
            """
            SELECT
                COUNT(*) AS users_with_referrer,
                COALESCE(SUM(referral_balance_rub), 0) AS total_balance_rub,
                COALESCE(SUM(referral_total_earned_rub), 0) AS total_earned_rub,
                COALESCE(SUM(referral_total_debited_rub), 0) AS total_debited_rub
            FROM users
            """
        )
        row = await cursor.fetchone()
        await cursor.close()
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

    async def get_admin_finance_summary(self) -> dict[str, int]:
        cursor = await self.conn.execute(
            """
            SELECT
                COALESCE((SELECT COUNT(*) FROM payments), 0) AS purchases_total,
                COALESCE((SELECT COUNT(*) FROM payments WHERE status = 'pending'), 0) AS purchases_pending,
                COALESCE((SELECT COUNT(*) FROM payments WHERE status = 'paid'), 0) AS purchases_paid,
                COALESCE((SELECT COUNT(*) FROM payments WHERE status = 'cancelled'), 0) AS purchases_cancelled,
                COALESCE((SELECT SUM(amount_rub) FROM payments WHERE status = 'paid'), 0) AS revenue_gross_rub,
                COALESCE((SELECT SUM(amount_rub) FROM referral_transactions WHERE direction = 'credit'), 0) AS referral_credit_rub,
                COALESCE((SELECT SUM(amount_rub) FROM referral_transactions WHERE direction = 'debit'), 0) AS referral_debit_rub
            """
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return {
                "purchases_total": 0,
                "purchases_pending": 0,
                "purchases_paid": 0,
                "purchases_cancelled": 0,
                "revenue_gross_rub": 0,
                "referral_credit_rub": 0,
                "referral_debit_rub": 0,
                "profit_after_referral_credit_rub": 0,
                "profit_after_referral_debit_rub": 0,
            }

        revenue_gross_rub = int(row["revenue_gross_rub"] or 0)
        referral_credit_rub = int(row["referral_credit_rub"] or 0)
        referral_debit_rub = int(row["referral_debit_rub"] or 0)
        return {
            "purchases_total": int(row["purchases_total"] or 0),
            "purchases_pending": int(row["purchases_pending"] or 0),
            "purchases_paid": int(row["purchases_paid"] or 0),
            "purchases_cancelled": int(row["purchases_cancelled"] or 0),
            "revenue_gross_rub": revenue_gross_rub,
            "referral_credit_rub": referral_credit_rub,
            "referral_debit_rub": referral_debit_rub,
            "profit_after_referral_credit_rub": revenue_gross_rub - referral_credit_rub,
            "profit_after_referral_debit_rub": revenue_gross_rub - referral_debit_rub,
        }

    async def list_admin_payments(self, limit: int = 100) -> list[dict[str, Any]]:
        cursor = await self.conn.execute(
            """
            SELECT
                p.id,
                p.user_id,
                u.tg_user_id,
                u.username,
                p.plan_code,
                p.months_count,
                p.amount_rub,
                p.status,
                p.target_tg_user_id,
                p.yookassa_payment_id,
                p.created_at,
                p.paid_at
            FROM payments p
            JOIN users u ON u.id = p.user_id
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT ?
            """,
            (max(1, limit),),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def list_admin_referral_transactions(self, limit: int = 100) -> list[dict[str, Any]]:
        cursor = await self.conn.execute(
            """
            SELECT
                rt.id,
                rt.referrer_user_id,
                ru.tg_user_id AS referrer_tg_user_id,
                ru.username AS referrer_username,
                rt.referred_user_id,
                su.tg_user_id AS referred_tg_user_id,
                su.username AS referred_username,
                rt.payment_id,
                rt.amount_rub,
                rt.direction,
                rt.comment,
                rt.created_at
            FROM referral_transactions rt
            JOIN users ru ON ru.id = rt.referrer_user_id
            LEFT JOIN users su ON su.id = rt.referred_user_id
            ORDER BY rt.created_at DESC, rt.id DESC
            LIMIT ?
            """,
            (max(1, limit),),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

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

        cursor = await self.conn.execute(
            """
            SELECT id, referral_balance_rub
            FROM users
            WHERE tg_user_id = ?
            """,
            (tg_user_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return False, 0

        user_id = int(row["id"])
        balance = int(row["referral_balance_rub"])
        if balance < amount:
            return False, balance

        timestamp = now_ts()
        await self.conn.execute("BEGIN IMMEDIATE")
        try:
            await self.conn.execute(
                """
                UPDATE users
                SET
                    referral_balance_rub = referral_balance_rub - ?,
                    referral_total_debited_rub = referral_total_debited_rub + ?,
                    updated_at = ?
                WHERE id = ? AND referral_balance_rub >= ?
                """,
                (amount, amount, timestamp, user_id, amount),
            )
            await self.conn.execute(
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
                VALUES (?, NULL, NULL, ?, 'debit', ?, ?)
                """,
                (user_id, amount, (comment or "").strip() or None, timestamp),
            )
            cursor = await self.conn.execute(
                "SELECT referral_balance_rub FROM users WHERE id = ?",
                (user_id,),
            )
            new_row = await cursor.fetchone()
            await cursor.close()
            await self.conn.commit()
            return True, int(new_row["referral_balance_rub"]) if new_row is not None else 0
        except Exception:
            await self.conn.rollback()
            raise

    async def get_all_tg_user_ids(self) -> list[int]:
        cursor = await self.conn.execute(
            """
            SELECT tg_user_id
            FROM users
            ORDER BY id ASC
            """
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [int(row["tg_user_id"]) for row in rows]

    async def list_users_with_stats(self, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
        timestamp = now_ts()
        cursor = await self.conn.execute(
            """
            SELECT
                u.id,
                u.tg_user_id,
                u.username,
                u.first_name,
                u.last_name,
                u.created_at,
                u.updated_at,
                SUM(CASE WHEN pl.status = 'active' AND pl.expires_at > ? THEN 1 ELSE 0 END) AS active_proxies,
                CASE WHEN bu.tg_user_id IS NULL THEN 0 ELSE 1 END AS is_banned
            FROM users u
            LEFT JOIN proxy_links pl ON pl.user_id = u.id
            LEFT JOIN banned_users bu ON bu.tg_user_id = u.tg_user_id
            GROUP BY u.id, bu.tg_user_id
            ORDER BY u.created_at DESC
            LIMIT ? OFFSET ?
            """,
            (timestamp, max(1, limit), max(0, offset)),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_plan(self, code: str) -> Plan | None:
        cursor = await self.conn.execute(
            """
            SELECT code, title, devices_count, price_rub, duration_days
            FROM plans
            WHERE code = ?
            """,
            (code,),
        )
        row = await cursor.fetchone()
        await cursor.close()
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
        if not DEFAULT_PLAN_CODES:
            return []
        placeholders = ",".join("?" for _ in DEFAULT_PLAN_CODES)
        cursor = await self.conn.execute(
            f"""
            SELECT code, title, devices_count, price_rub, duration_days
            FROM plans
            WHERE code IN ({placeholders})
            ORDER BY devices_count ASC
            """,
            DEFAULT_PLAN_CODES,
        )
        rows = await cursor.fetchall()
        await cursor.close()
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
        cursor = await self.conn.execute(
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
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
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
        payment_id = int(cursor.lastrowid)
        await self.conn.commit()
        return payment_id

    async def get_payment_for_user(self, payment_id: int, user_id: int) -> dict[str, Any] | None:
        cursor = await self.conn.execute(
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
            WHERE id = ? AND user_id = ?
            """,
            (payment_id, user_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row is not None else None

    async def get_payment_by_yookassa_payment_id(self, yookassa_payment_id: str) -> dict[str, Any] | None:
        cursor = await self.conn.execute(
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
            WHERE yookassa_payment_id = ?
            """,
            (yookassa_payment_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row is not None else None

    async def cancel_pending_payment(self, payment_id: int, user_id: int) -> bool:
        cursor = await self.conn.execute(
            """
            UPDATE payments
            SET status = 'cancelled'
            WHERE id = ? AND user_id = ? AND status = 'pending'
            """,
            (payment_id, user_id),
        )
        await self.conn.commit()
        return cursor.rowcount > 0

    async def count_free_pool(self) -> int:
        cursor = await self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM proxy_pool WHERE status = 'free'"
        )
        row = await cursor.fetchone()
        await cursor.close()
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
        await self.conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = await self.conn.execute(
                """
                UPDATE payments
                SET status = 'paid', paid_at = ?
                WHERE id = ? AND user_id = ? AND status = 'pending'
                """,
                (timestamp, payment_id, payer_user_id),
            )
            if cursor.rowcount == 0:
                await self.conn.rollback()
                return None

            cursor = await self.conn.execute(
                "SELECT amount_rub FROM payments WHERE id = ?",
                (payment_id,),
            )
            payment_row = await cursor.fetchone()
            await cursor.close()
            payment_amount_rub = int(payment_row["amount_rub"]) if payment_row is not None else 0
            if payment_amount_rub > 0:
                cursor = await self.conn.execute(
                    "SELECT referrer_user_id FROM users WHERE id = ?",
                    (payer_user_id,),
                )
                ref_row = await cursor.fetchone()
                await cursor.close()
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
                    await self.conn.execute(
                        """
                        UPDATE users
                        SET
                            referral_balance_rub = referral_balance_rub + ?,
                            referral_total_earned_rub = referral_total_earned_rub + ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (referral_amount, referral_amount, timestamp, referrer_user_id),
                    )
                    await self.conn.execute(
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
                        VALUES (?, ?, ?, ?, 'credit', ?, ?)
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

            cursor = await self.conn.execute(
                """
                SELECT id, port, username, password
                FROM proxy_pool
                WHERE status = 'free'
                ORDER BY port ASC
                LIMIT ?
                """,
                (devices_count,),
            )
            proxy_rows = await cursor.fetchall()
            await cursor.close()
            if len(proxy_rows) < devices_count:
                await self.conn.rollback()
                return None

            cursor = await self.conn.execute(
                """
                INSERT INTO subscriptions (user_id, plan_code, payment_id, status, created_at, expires_at)
                VALUES (?, ?, ?, 'active', ?, ?)
                """,
                (recipient_user_id, plan_code, payment_id, timestamp, expires_at),
            )
            subscription_id = int(cursor.lastrowid)

            created: list[dict[str, Any]] = []
            for device_number, proxy_row in enumerate(proxy_rows, start=1):
                port = int(proxy_row["port"])
                username = str(proxy_row["username"])
                password = str(proxy_row["password"])

                username_safe = quote(username, safe="")
                password_safe = quote(password, safe="")
                link = f"socks5://{username_safe}:{password_safe}@{proxy_public_host}:{port}"

                cursor = await self.conn.execute(
                    """
                    INSERT INTO proxy_links (
                        subscription_id, user_id, device_number, token, link, status, created_at, expires_at
                    )
                    VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
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
                link_id = int(cursor.lastrowid)

                updated = await self.conn.execute(
                    """
                    UPDATE proxy_pool
                    SET status = 'assigned', assigned_link_id = ?, updated_at = ?
                    WHERE id = ? AND status = 'free'
                    """,
                    (link_id, timestamp, int(proxy_row["id"])),
                )
                if updated.rowcount == 0:
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
        cursor = await self.conn.execute(
            """
            SELECT id, tg_user_id, username, first_name, last_name, created_at, updated_at
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
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
        await self.conn.execute(
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        await self.conn.execute(
            """
            INSERT OR IGNORE INTO user_temp_messages (user_id, tg_user_id, message_id, kind, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, tg_user_id, message_id, kind, now_ts()),
        )
        await self.conn.commit()

    async def pop_temp_messages(self, *, user_id: int, kind: str) -> list[dict[str, Any]]:
        cursor = await self.conn.execute(
            """
            SELECT id, tg_user_id, message_id
            FROM user_temp_messages
            WHERE user_id = ? AND kind = ?
            ORDER BY id ASC
            """,
            (user_id, kind),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        if rows:
            await self.conn.execute(
                """
                DELETE FROM user_temp_messages
                WHERE user_id = ? AND kind = ?
                """,
                (user_id, kind),
            )
            await self.conn.commit()
        return [dict(row) for row in rows]

    async def get_user_ban(self, tg_user_id: int) -> dict[str, Any] | None:
        cursor = await self.conn.execute(
            """
            SELECT tg_user_id, reason, blocked_by, blocked_at
            FROM banned_users
            WHERE tg_user_id = ?
            """,
            (tg_user_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row is not None else None

    async def ban_user(self, tg_user_id: int, reason: str, blocked_by: int | None = None) -> None:
        await self.conn.execute(
            """
            INSERT INTO banned_users (tg_user_id, reason, blocked_by, blocked_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(tg_user_id) DO UPDATE SET
                reason = excluded.reason,
                blocked_by = excluded.blocked_by,
                blocked_at = excluded.blocked_at
            """,
            (tg_user_id, reason, blocked_by, now_ts()),
        )
        await self.conn.commit()

    async def unban_user(self, tg_user_id: int) -> bool:
        cursor = await self.conn.execute(
            """
            DELETE FROM banned_users
            WHERE tg_user_id = ?
            """,
            (tg_user_id,),
        )
        await self.conn.commit()
        return cursor.rowcount > 0

    async def get_all_links_for_user(self, user_id: int) -> list[dict[str, Any]]:
        cursor = await self.conn.execute(
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
            WHERE pl.user_id = ?
            ORDER BY pl.created_at DESC, pl.id DESC
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_active_links_for_user(self, user_id: int) -> list[dict[str, Any]]:
        timestamp = now_ts()
        cursor = await self.conn.execute(
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
                pl.user_id = ?
                AND pl.status = 'active'
                AND pl.expires_at > ?
                AND s.status = 'active'
            ORDER BY pl.expires_at ASC, pl.subscription_id ASC, pl.device_number ASC
            """,
            (user_id, timestamp),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_active_subscriptions_for_user(self, user_id: int) -> list[dict[str, Any]]:
        timestamp = now_ts()
        cursor = await self.conn.execute(
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
            WHERE s.user_id = ? AND s.status = 'active' AND s.expires_at > ?
            ORDER BY s.expires_at ASC
            """,
            (user_id, timestamp),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def revoke_proxy_link_for_user(self, user_id: int, proxy_link_id: int) -> bool:
        timestamp = now_ts()
        await self.conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = await self.conn.execute(
                """
                SELECT id, subscription_id
                FROM proxy_links
                WHERE id = ? AND user_id = ? AND status = 'active'
                """,
                (proxy_link_id, user_id),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is None:
                await self.conn.rollback()
                return False

            subscription_id = int(row["subscription_id"])

            await self.conn.execute(
                """
                UPDATE proxy_links
                SET status = 'expired', expires_at = ?
                WHERE id = ?
                """,
                (timestamp, proxy_link_id),
            )
            await self.conn.execute(
                """
                UPDATE proxy_pool
                SET status = 'free', assigned_link_id = NULL, updated_at = ?
                WHERE assigned_link_id = ?
                """,
                (timestamp, proxy_link_id),
            )
            await self.conn.execute(
                """
                UPDATE subscriptions
                SET status = 'expired'
                WHERE id = ? AND status = 'active' AND NOT EXISTS (
                    SELECT 1
                    FROM proxy_links
                    WHERE subscription_id = ? AND status = 'active' AND expires_at > ?
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
        await self.conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = await self.conn.execute(
                """
                SELECT id
                FROM proxy_links
                WHERE user_id = ? AND status = 'active' AND expires_at > ?
                """,
                (user_id, timestamp),
            )
            rows = await cursor.fetchall()
            await cursor.close()
            if not rows:
                await self.conn.rollback()
                return 0

            link_ids = [int(row["id"]) for row in rows]
            placeholders = ",".join("?" for _ in link_ids)

            await self.conn.execute(
                f"""
                UPDATE proxy_links
                SET status = 'expired', expires_at = ?
                WHERE id IN ({placeholders})
                """,
                (timestamp, *link_ids),
            )
            await self.conn.execute(
                f"""
                UPDATE proxy_pool
                SET status = 'free', assigned_link_id = NULL, updated_at = ?
                WHERE assigned_link_id IN ({placeholders})
                """,
                (timestamp, *link_ids),
            )
            await self.conn.execute(
                """
                UPDATE subscriptions
                SET status = 'expired'
                WHERE user_id = ? AND status = 'active' AND NOT EXISTS (
                    SELECT 1
                    FROM proxy_links
                    WHERE subscription_id = subscriptions.id AND status = 'active' AND expires_at > ?
                )
                """,
                (user_id, timestamp),
            )
            await self.conn.commit()
            return len(link_ids)
        except Exception:
            await self.conn.rollback()
            raise

    async def get_purchase_nudge_candidates_and_mark_notified_users(
        self,
        *,
        delay_seconds: int = 15 * 60,
    ) -> list[int]:
        timestamp = now_ts()
        cutoff = timestamp - max(60, delay_seconds)
        cursor = await self.conn.execute(
            """
            SELECT u.id AS user_id, u.tg_user_id
            FROM users u
            WHERE (u.purchase_nudge_sent_at IS NULL)
              AND u.updated_at <= ?
              AND NOT EXISTS (
                    SELECT 1
                    FROM payments p
                    WHERE p.user_id = u.id AND p.status = 'paid'
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM subscriptions s
                    WHERE s.user_id = u.id
                      AND s.status = 'active'
                      AND s.expires_at > ?
              )
            """,
            (cutoff, timestamp),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        if not rows:
            return []

        user_ids = [int(row["user_id"]) for row in rows]
        placeholders = ",".join("?" for _ in user_ids)
        await self.conn.execute(
            f"""
            UPDATE users
            SET purchase_nudge_sent_at = ?, updated_at = ?
            WHERE id IN ({placeholders}) AND purchase_nudge_sent_at IS NULL
            """,
            (timestamp, timestamp, *user_ids),
        )
        await self.conn.commit()
        return list(dict.fromkeys(int(row["tg_user_id"]) for row in rows))

    async def get_expiring_in_three_days_and_mark_notified_users(self) -> list[int]:
        timestamp = now_ts()
        threshold = timestamp + 3 * 24 * 60 * 60
        cursor = await self.conn.execute(
            """
            SELECT s.id AS subscription_id, u.tg_user_id
            FROM subscriptions s
            JOIN users u ON u.id = s.user_id
            WHERE s.status = 'active'
              AND s.expires_at > ?
              AND s.expires_at <= ?
              AND s.notified_expiring_2days = 0
            """,
            (timestamp, threshold),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        if not rows:
            return []

        subscription_ids = [int(row["subscription_id"]) for row in rows]
        placeholders = ",".join("?" for _ in subscription_ids)
        await self.conn.execute(
            f"""
            UPDATE subscriptions
            SET notified_expiring_2days = 1
            WHERE id IN ({placeholders})
            """,
            tuple(subscription_ids),
        )
        await self.conn.commit()
        return list(dict.fromkeys(int(row["tg_user_id"]) for row in rows))

    async def get_expiring_in_two_days_and_mark_notified_users(self) -> list[int]:
        return await self.get_expiring_in_three_days_and_mark_notified_users()

    async def expire_due_and_get_notified_users(self) -> list[int]:
        timestamp = now_ts()
        await self.conn.execute(
            """
            UPDATE subscriptions
            SET status = 'expired'
            WHERE status = 'active' AND expires_at <= ?
            """,
            (timestamp,),
        )
        await self.conn.execute(
            """
            UPDATE proxy_links
            SET status = 'expired'
            WHERE status = 'active' AND expires_at <= ?
            """,
            (timestamp,),
        )
        await self.conn.execute(
            """
            UPDATE proxy_pool
            SET status = 'free', assigned_link_id = NULL, updated_at = ?
            WHERE assigned_link_id IN (
                SELECT id FROM proxy_links WHERE status = 'expired'
            )
            """,
            (timestamp,),
        )

        cursor = await self.conn.execute(
            """
            SELECT DISTINCT u.tg_user_id
            FROM subscriptions s
            JOIN users u ON u.id = s.user_id
            WHERE s.status = 'expired' AND s.notified_expired = 0
            """
        )
        rows = await cursor.fetchall()
        await cursor.close()

        await self.conn.execute(
            """
            UPDATE subscriptions
            SET notified_expired = 1
            WHERE status = 'expired' AND notified_expired = 0
            """
        )
        await self.conn.commit()

        return [int(row["tg_user_id"]) for row in rows]
