from __future__ import annotations

from typing import Optional


def normalize_sku(value: Optional[str]) -> str:
    if value is None:
        return ""
    cleaned = value.replace("\u00a0", " ").strip().upper()
    if not cleaned:
        return ""
    return " ".join(cleaned.split())
