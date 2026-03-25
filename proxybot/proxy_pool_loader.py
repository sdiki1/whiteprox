from __future__ import annotations

import json
from pathlib import Path

from .database import ProxyPoolEntry


def load_proxy_pool(path: str) -> list[ProxyPoolEntry]:
    pool_path = Path(path)
    if not pool_path.exists():
        return []

    raw = json.loads(pool_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("Proxy pool file must contain JSON array")

    result: list[ProxyPoolEntry] = []
    seen_ports: set[int] = set()

    for idx, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Proxy item #{idx} must be object")

        is_active = item.get("active", True)
        if isinstance(is_active, bool) and not is_active:
            continue

        port = item.get("port")
        username = item.get("username")
        password = item.get("password")

        if not isinstance(port, int) or port < 1 or port > 65535:
            raise ValueError(f"Proxy item #{idx} has invalid 'port'")
        if not isinstance(username, str) or not username.strip():
            raise ValueError(f"Proxy item #{idx} has invalid 'username'")
        if not isinstance(password, str) or not password.strip():
            raise ValueError(f"Proxy item #{idx} has invalid 'password'")
        if port in seen_ports:
            raise ValueError(f"Duplicate proxy port in pool: {port}")

        seen_ports.add(port)
        result.append(ProxyPoolEntry(port=port, username=username, password=password))

    return result
