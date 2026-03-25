from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from pathlib import Path
import secrets
from typing import Any


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("socks-farm")


def parse_port_range(value: str) -> tuple[int, int]:
    parts = value.split("-", maxsplit=1)
    if len(parts) != 2:
        raise ValueError("SOCKS_PORT_RANGE must look like start-end")
    start = int(parts[0].strip())
    end = int(parts[1].strip())
    if start < 1 or end > 65535 or start > end:
        raise ValueError("SOCKS_PORT_RANGE has invalid bounds")
    return start, end


def build_pool(start: int, end: int) -> list[dict[str, Any]]:
    pool: list[dict[str, Any]] = []
    for port in range(start, end + 1):
        pool.append(
            {
                "port": port,
                "username": f"u{port}",
                "password": secrets.token_urlsafe(8),
                "active": True,
            }
        )
    return pool


def is_pool_compatible(data: Any, start: int, end: int) -> bool:
    if not isinstance(data, list):
        return False
    if len(data) != (end - start + 1):
        return False

    expected_ports = set(range(start, end + 1))
    seen_ports: set[int] = set()

    for item in data:
        if not isinstance(item, dict):
            return False
        port = item.get("port")
        username = item.get("username")
        password = item.get("password")
        if not isinstance(port, int) or port not in expected_ports:
            return False
        if not isinstance(username, str) or not username:
            return False
        if not isinstance(password, str) or not password:
            return False
        seen_ports.add(port)

    return seen_ports == expected_ports


def load_or_create_pool(path: Path, start: int, end: int) -> list[dict[str, Any]]:
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        if is_pool_compatible(data, start, end):
            logger.info("Using existing pool file: %s (%d entries)", path, len(data))
            return data

    path.parent.mkdir(parents=True, exist_ok=True)
    data = build_pool(start, end)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Generated new pool file: %s (%d entries)", path, len(data))
    return data


async def pipe_stream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()
    except Exception:
        pass
    finally:
        with contextlib.suppress(Exception):
            writer.close()
            await writer.wait_closed()


async def read_exact_or_none(reader: asyncio.StreamReader, size: int) -> bytes | None:
    try:
        return await reader.readexactly(size)
    except Exception:
        return None


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    expected_username: str,
    expected_password: str,
) -> None:
    # Greeting: VER, NMETHODS, METHODS...
    greeting = await read_exact_or_none(reader, 2)
    if greeting is None:
        writer.close()
        return
    ver, nmethods = greeting[0], greeting[1]
    if ver != 5:
        writer.close()
        return

    methods = await read_exact_or_none(reader, nmethods)
    if methods is None:
        writer.close()
        return

    # We only support USERNAME/PASSWORD auth (RFC 1929).
    if 0x02 not in methods:
        writer.write(b"\x05\xff")
        await writer.drain()
        writer.close()
        return
    writer.write(b"\x05\x02")
    await writer.drain()

    auth_header = await read_exact_or_none(reader, 2)
    if auth_header is None:
        writer.close()
        return
    auth_ver, username_len = auth_header[0], auth_header[1]
    if auth_ver != 0x01:
        writer.write(b"\x01\x01")
        await writer.drain()
        writer.close()
        return

    username_raw = await read_exact_or_none(reader, username_len)
    if username_raw is None:
        writer.close()
        return

    password_len_raw = await read_exact_or_none(reader, 1)
    if password_len_raw is None:
        writer.close()
        return
    password_len = password_len_raw[0]
    password_raw = await read_exact_or_none(reader, password_len)
    if password_raw is None:
        writer.close()
        return

    username = username_raw.decode("utf-8", errors="ignore")
    password = password_raw.decode("utf-8", errors="ignore")
    if username != expected_username or password != expected_password:
        writer.write(b"\x01\x01")
        await writer.drain()
        writer.close()
        return

    writer.write(b"\x01\x00")
    await writer.drain()

    req_head = await read_exact_or_none(reader, 4)
    if req_head is None:
        writer.close()
        return

    ver, cmd, _rsv, atyp = req_head
    if ver != 5 or cmd != 1:
        writer.write(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
        await writer.drain()
        writer.close()
        return

    if atyp == 1:  # IPv4
        addr = await read_exact_or_none(reader, 4)
        if addr is None:
            writer.close()
            return
        host = ".".join(str(b) for b in addr)
    elif atyp == 3:  # DOMAIN
        ln = await read_exact_or_none(reader, 1)
        if ln is None:
            writer.close()
            return
        domain_len = ln[0]
        domain = await read_exact_or_none(reader, domain_len)
        if domain is None:
            writer.close()
            return
        try:
            host = domain.decode("idna")
        except UnicodeError:
            writer.write(b"\x05\x04\x00\x01\x00\x00\x00\x00\x00\x00")
            await writer.drain()
            writer.close()
            return
    elif atyp == 4:  # IPv6
        addr = await read_exact_or_none(reader, 16)
        if addr is None:
            writer.close()
            return
        groups = [addr[i : i + 2] for i in range(0, 16, 2)]
        host = ":".join(f"{int.from_bytes(group, 'big'):x}" for group in groups)
    else:
        writer.write(b"\x05\x08\x00\x01\x00\x00\x00\x00\x00\x00")
        await writer.drain()
        writer.close()
        return

    port_raw = await read_exact_or_none(reader, 2)
    if port_raw is None:
        writer.close()
        return
    port = int.from_bytes(port_raw, "big")

    try:
        target_reader, target_writer = await asyncio.open_connection(host, port)
    except Exception:
        writer.write(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00")
        await writer.drain()
        writer.close()
        return

    writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
    await writer.drain()

    await asyncio.gather(
        pipe_stream(reader, target_writer),
        pipe_stream(target_reader, writer),
    )


async def start_proxy_server(
    bind_host: str,
    item: dict[str, Any],
) -> tuple[asyncio.base_events.Server | None, Exception | None]:
    port = int(item["port"])
    username = str(item["username"])
    password = str(item["password"])
    try:
        server = await asyncio.start_server(
            lambda r, w, u=username, p=password: handle_client(r, w, u, p),
            host=bind_host,
            port=port,
            start_serving=True,
        )
        return server, None
    except Exception as exc:
        return None, exc


async def main() -> None:
    bind_host = os.getenv("SOCKS_BIND_HOST", "0.0.0.0").strip() or "0.0.0.0"
    port_range = os.getenv("SOCKS_PORT_RANGE", "30000-30199").strip() or "30000-30199"
    pool_file = Path(os.getenv("SOCKS_POOL_FILE", "/data/proxy_pool.json").strip() or "/data/proxy_pool.json")

    start_port, end_port = parse_port_range(port_range)
    pool = load_or_create_pool(pool_file, start_port, end_port)

    servers: list[asyncio.base_events.Server] = []
    start_results = await asyncio.gather(*(start_proxy_server(bind_host, item) for item in pool))

    active_count = 0
    skipped_count = 0
    for item, (server, error) in zip(pool, start_results):
        port = int(item["port"])
        if server is not None:
            item["active"] = True
            servers.append(server)
            active_count += 1
            continue

        item["active"] = False
        skipped_count += 1
        logger.warning("Skipping busy/unavailable port %s: %s", port, error)

    pool_file.write_text(json.dumps(pool, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "SOCKS farm started on %s, range %s (active=%d, skipped=%d)",
        bind_host,
        port_range,
        active_count,
        skipped_count,
    )

    if active_count == 0:
        raise RuntimeError("No available ports to start SOCKS farm")

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
