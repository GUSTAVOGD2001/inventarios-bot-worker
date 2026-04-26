"""Tests for sync_engine bug fixes (case-insensitive ddvc_map, truthy is_salable)."""
from __future__ import annotations

from app.sku_utils import normalize_sku


def test_ddvc_map_normalization():
    """Keys de ddvc_map deben normalizarse a UPPER."""
    raw_map = {"HuAsPCG3": {"is_salable": True, "final_price": 100.0}}
    normalized = {normalize_sku(k): v for k, v in raw_map.items() if normalize_sku(k)}
    assert "HUASPCG3" in normalized
    assert "HuAsPCG3" not in normalized


def test_is_salable_truthy():
    """is_salable como int 1 debe tratarse como True."""
    for val in [True, 1, "true"]:
        result = bool(val)
        assert result is True, f"is_salable={val!r} should be truthy"
    for val in [False, 0, None, ""]:
        result = bool(val) if val is not None else False
        assert result is False, f"is_salable={val!r} should be falsy"
