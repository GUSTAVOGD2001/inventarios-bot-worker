import pytest
from unittest.mock import AsyncMock, MagicMock

from app.pricing_engine import calculate_final_price


def _make_pool(override=None, rule=None, rounding_enabled=False):
    """Create a mock asyncpg pool."""
    pool = AsyncMock()

    async def mock_fetchrow(sql, *args):
        if "sku_overrides" in sql:
            if override:
                return override
            return None
        if "pricing_rules" in sql:
            if rule:
                return rule
            return None
        if "panel_settings" in sql:
            return {"value": rounding_enabled}
        return None

    pool.fetchrow = mock_fetchrow
    return pool


@pytest.mark.asyncio
async def test_no_rules():
    """Price should pass through unchanged when no rules are active."""
    pool = _make_pool()
    result = await calculate_final_price("SKU-001", 100.00, pool)
    assert result["final_price"] == 100.00
    assert result["override_applied"] is None
    assert result["global_rule_applied"] is None
    assert result["rounding_applied"] is False


@pytest.mark.asyncio
async def test_global_rule_percentage():
    """Global percentage rule should increase price."""
    rule = {"name": "Markup", "rule_type": "percentage", "value": 5.0}
    pool = _make_pool(rule=rule)
    result = await calculate_final_price("SKU-002", 100.00, pool)
    assert result["final_price"] == 105.00
    assert result["global_rule_applied"] == "Markup 5.0%"
    assert result["override_applied"] is None


@pytest.mark.asyncio
async def test_override_fixed_price():
    """Fixed price override should return exact value and skip rounding."""
    override = {"override_type": "fixed_price", "value": 199.99}
    pool = _make_pool(override=override, rounding_enabled=True)
    result = await calculate_final_price("SKU-003", 150.00, pool)
    assert result["final_price"] == 199.99
    assert result["override_applied"] == "fixed_price: $199.99"
    assert result["rounding_applied"] is False


@pytest.mark.asyncio
async def test_override_percentage_with_rounding():
    """Percentage override + rounding should apply both."""
    override = {"override_type": "percentage", "value": 10.0}
    pool = _make_pool(override=override, rounding_enabled=True)
    result = await calculate_final_price("SKU-004", 150.00, pool)
    # 150 * 1.10 = 165.00 -> round_up_x9_99 -> 169.99
    assert result["final_price"] == 169.99
    assert result["override_applied"] == "percentage: +10.0%"
    assert result["rounding_applied"] is True


@pytest.mark.asyncio
async def test_global_rule_fixed_amount():
    """Global fixed_amount rule should add to price."""
    rule = {"name": "Flat Fee", "rule_type": "fixed_amount", "value": 25.00}
    pool = _make_pool(rule=rule)
    result = await calculate_final_price("SKU-005", 100.00, pool)
    assert result["final_price"] == 125.00
    assert result["global_rule_applied"] == "Flat Fee +$25.00"


@pytest.mark.asyncio
async def test_override_fixed_amount_with_rounding():
    """Fixed amount override + rounding."""
    override = {"override_type": "fixed_amount", "value": 15.00}
    pool = _make_pool(override=override, rounding_enabled=True)
    result = await calculate_final_price("SKU-006", 142.30, pool)
    # 142.30 + 15 = 157.30 -> round_up_x9_99 -> 159.99
    assert result["final_price"] == 159.99
    assert result["rounding_applied"] is True


@pytest.mark.asyncio
async def test_preview_rules():
    """Preview rules should use provided values, not DB."""
    pool = _make_pool()
    result = await calculate_final_price(
        "SKU-007", 100.00, pool,
        preview_rules={"global_markup": 8.0, "rounding_enabled": True},
    )
    # 100 * 1.08 = 108.00 -> round_up_x9_99 -> 109.99
    assert result["final_price"] == 109.99
    assert result["global_rule_applied"] == "Preview markup 8.0%"
    assert result["rounding_applied"] is True


@pytest.mark.asyncio
async def test_margin_calculation():
    """Margin should be correctly calculated."""
    pool = _make_pool()
    result = await calculate_final_price("SKU-008", 80.00, pool)
    assert result["margin_amount"] == 0.0
    assert result["margin_percent"] == 0.0

    rule = {"name": "Markup", "rule_type": "percentage", "value": 25.0}
    pool2 = _make_pool(rule=rule)
    result2 = await calculate_final_price("SKU-009", 80.00, pool2)
    assert result2["final_price"] == 100.00
    assert result2["margin_amount"] == 20.00
    assert result2["margin_percent"] == 25.00
