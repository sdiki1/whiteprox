#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
from pathlib import Path

from proxybot.database_postgres import PostgresDatabase

TABLES: list[tuple[str, list[str]]] = [
    ("plans", ["code", "title", "devices_count", "price_rub", "duration_days"]),
    ("users", ["id", "tg_user_id", "username", "first_name", "last_name", "created_at", "updated_at"]),
    ("payments", ["id", "user_id", "plan_code", "amount_rub", "status", "created_at", "paid_at"]),
    (
        "subscriptions",
        ["id", "user_id", "plan_code", "payment_id", "status", "created_at", "expires_at", "notified_expired"],
    ),
    (
        "proxy_links",
        ["id", "subscription_id", "user_id", "device_number", "token", "link", "status", "created_at", "expires_at"],
    ),
    ("proxy_pool", ["id", "port", "username", "password", "status", "assigned_link_id", "created_at", "updated_at"]),
    (
        "proxy_delivery_logs",
        [
            "id",
            "proxy_link_id",
            "user_id",
            "tg_user_id",
            "user_label",
            "subscription_id",
            "device_number",
            "delivery_source",
            "proxy_url",
            "delivered_at",
        ],
    ),
    ("user_temp_messages", ["id", "user_id", "tg_user_id", "message_id", "kind", "created_at"]),
    ("banned_users", ["id", "tg_user_id", "reason", "blocked_by", "blocked_at"]),
]

TABLES_WITH_ID = [
    "users",
    "payments",
    "subscriptions",
    "proxy_links",
    "proxy_pool",
    "proxy_delivery_logs",
    "user_temp_messages",
    "banned_users",
]


async def truncate_postgres(pg: PostgresDatabase) -> None:
    async with pg.conn.cursor() as cur:
        await cur.execute(
            """
            TRUNCATE TABLE
                user_temp_messages,
                proxy_delivery_logs,
                proxy_pool,
                proxy_links,
                subscriptions,
                payments,
                banned_users,
                users,
                plans
            RESTART IDENTITY CASCADE
            """
        )
    await pg.conn.commit()


def read_sqlite_rows(conn: sqlite3.Connection, table: str, columns: list[str]) -> list[tuple]:
    query = f"SELECT {', '.join(columns)} FROM {table}"
    try:
        rows = conn.execute(query).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            return []
        raise
    return [tuple(row[col] for col in columns) for row in rows]


async def insert_rows(pg: PostgresDatabase, table: str, columns: list[str], rows: list[tuple]) -> int:
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    async with pg.conn.cursor() as cur:
        await cur.executemany(sql, rows)
    await pg.conn.commit()
    return len(rows)


async def reset_sequences(pg: PostgresDatabase) -> None:
    async with pg.conn.cursor() as cur:
        for table in TABLES_WITH_ID:
            await cur.execute(
                f"""
                SELECT setval(
                    pg_get_serial_sequence('{table}', 'id'),
                    COALESCE(MAX(id), 1),
                    COALESCE(MAX(id), 0) > 0
                )
                FROM {table}
                """
            )
    await pg.conn.commit()


async def migrate(*, sqlite_path: str, postgres_url: str, truncate_first: bool) -> None:
    sqlite_file = Path(sqlite_path)
    if not sqlite_file.exists():
        raise FileNotFoundError(f"SQLite file not found: {sqlite_file}")

    sqlite_conn = sqlite3.connect(str(sqlite_file))
    sqlite_conn.row_factory = sqlite3.Row

    pg = PostgresDatabase(postgres_url)
    await pg.connect()
    await pg.init_schema()

    try:
        if truncate_first:
            await truncate_postgres(pg)

        migrated_counts: list[tuple[str, int]] = []
        for table, columns in TABLES:
            rows = read_sqlite_rows(sqlite_conn, table, columns)
            inserted = await insert_rows(pg, table, columns, rows)
            migrated_counts.append((table, inserted))

        await reset_sequences(pg)

        total = sum(count for _, count in migrated_counts)
        print("Migration completed.")
        for table, count in migrated_counts:
            print(f"- {table}: {count}")
        print(f"Total rows copied: {total}")
    finally:
        sqlite_conn.close()
        await pg.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate ProxyBot SQLite DB to PostgreSQL.")
    parser.add_argument(
        "--sqlite-path",
        default=os.getenv("DATABASE_PATH", "data/bot.db"),
        help="Path to source SQLite database (default: DATABASE_PATH or data/bot.db)",
    )
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("DATABASE_URL", ""),
        help="Target PostgreSQL DSN (default: DATABASE_URL)",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Do not TRUNCATE target tables before copy",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.postgres_url:
        raise ValueError("PostgreSQL DSN is required. Pass --postgres-url or set DATABASE_URL.")
    asyncio.run(
        migrate(
            sqlite_path=args.sqlite_path,
            postgres_url=args.postgres_url,
            truncate_first=not args.no_truncate,
        )
    )


if __name__ == "__main__":
    main()
