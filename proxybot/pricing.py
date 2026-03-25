from __future__ import annotations


BUNDLE_PRICES_RUB: dict[int, int] = {
    1: 99,
    3: 249,
    6: 499,
    12: 999,
}


def total_price_rub(*, monthly_price_rub: int, months_count: int) -> int:
    months = max(1, int(months_count))
    if monthly_price_rub == 99:
        bundled = BUNDLE_PRICES_RUB.get(months)
        if bundled is not None:
            return bundled
    return max(0, int(monthly_price_rub)) * months
