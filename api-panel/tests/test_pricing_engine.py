import pytest
from unittest.mock import AsyncMock

from app.pricing_engine import calculate_final_price


def _make_pool(override=None, prefix_override=None, rule=None, settings=None):
    pool = AsyncMock()
    settings = settings or {}

    async def mock_fetchrow(sql, *args):
        if "FROM sku_overrides" in sql:
            return override
        if "FROM sku_prefix_overrides" in sql:
            return prefix_override
        if "FROM pricing_rules" in sql:
            return rule
        if "FROM panel_settings" in sql:
            key = args[0]
            return {"value": settings.get(key)} if key in settings else None
        return None

    pool.fetchrow = mock_fetchrow
    return pool


@pytest.mark.asyncio
async def test_prefix_override_percentage():
    pool = _make_pool(prefix_override={"sku_prefix": "AO", "override_type": "percentage", "value": -5.0})
    result = await calculate_final_price("AOSKU001", 1000.0, pool)
    assert result["final_price"] == 950.0
    assert "prefix" in (result["override_applied"] or "").lower()


@pytest.mark.asyncio
async def test_exact_override_beats_prefix():
    pool = _make_pool(
        override={"override_type": "fixed_price", "value": 999.0},
        prefix_override={"sku_prefix": "AO", "override_type": "percentage", "value": -5.0},
    )
    result = await calculate_final_price("AOSKU001", 1000.0, pool)
    assert result["final_price"] == 999.0


@pytest.mark.asyncio
async def test_price_cap_rounding_enabled():
    pool = _make_pool(settings={
        "price_cap_enabled": True,
        "price_cap_max": 10000.0,
        "price_cap_rounding_enabled": True,
        "price_cap_rounding_discount": 0.10,
    })
    result = await calculate_final_price("SKU-CARO", 15000.0, pool)
    assert result["global_rule_applied"] == "price_cap_skip"
    assert result["final_price"] == 14999.90


@pytest.mark.asyncio
async def test_price_cap_rounding_disabled():
    pool = _make_pool(settings={
        "price_cap_enabled": True,
        "price_cap_max": 10000.0,
        "price_cap_rounding_enabled": False,
    })
    result = await calculate_final_price("SKU-CARO", 15000.0, pool)
    assert result["final_price"] == 15000.0


@pytest.mark.asyncio
async def test_price_cap_ignored_with_override():
    pool = _make_pool(
        override={"override_type": "fixed_price", "value": 18500.0},
        settings={
            "price_cap_enabled": True,
            "price_cap_max": 10000.0,
            "price_cap_rounding_enabled": True,
            "price_cap_rounding_discount": 0.10,
        },
    )
    result = await calculate_final_price("SKU-X", 15000.0, pool)
    assert result["final_price"] == 18500.0
