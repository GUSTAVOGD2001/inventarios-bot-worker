"""Tests for app.pricing.PricingEngine (worker síncrono).

Construimos el engine sin tocar la base de datos: instanciamos PricingEngine
con un engine None y seteamos los atributos manualmente, evitando load_rules().
"""
from __future__ import annotations

import pytest

from app.pricing import PricingEngine


def _make_engine(
    *,
    overrides=None,
    global_rule=None,
    prefix_overrides=None,
    rounding_enabled=False,
    rounding_threshold=200.0,
    rounding_low_mode="nearest_99",
    rounding_high_mode="ceil_x9_99",
    global_markup_enabled=True,
    price_cap_enabled=True,
    price_cap_max=10000.0,
    price_cap_rounding_enabled=False,
    price_cap_rounding_discount=0.10,
) -> PricingEngine:
    engine = PricingEngine(engine=None)  # type: ignore[arg-type]
    engine.overrides = overrides or {}
    engine.global_rule = global_rule
    engine.prefix_overrides = prefix_overrides or []
    engine.rounding_enabled = rounding_enabled
    engine.rounding_threshold = rounding_threshold
    engine.rounding_low_mode = rounding_low_mode
    engine.rounding_high_mode = rounding_high_mode
    engine.global_markup_enabled = global_markup_enabled
    engine.price_cap_enabled = price_cap_enabled
    engine.price_cap_max = price_cap_max
    engine.price_cap_rounding_enabled = price_cap_rounding_enabled
    engine.price_cap_rounding_discount = price_cap_rounding_discount
    return engine


def test_price_cap_blocks_global_rule():
    """Producto arriba del cap no recibe markup global."""
    engine = _make_engine(
        global_rule={"name": "MarkupGlobal", "rule_type": "percentage", "value": 3.0},
        price_cap_enabled=True,
        price_cap_max=10000.0,
    price_cap_rounding_enabled=False,
    price_cap_rounding_discount=0.10,
    )
    result = engine.calculate("SKU-CARO", 15000.0)
    assert result.final_price == 15000.0
    assert result.global_rule_applied == "price_cap_skip"
    assert result.override_applied is None
    assert result.rounding_applied is False


def test_price_cap_blocks_rounding():
    """Producto arriba del cap no recibe redondeo."""
    engine = _make_engine(
        rounding_enabled=True,
        rounding_threshold=200.0,
        price_cap_enabled=True,
        price_cap_max=10000.0,
    price_cap_rounding_enabled=False,
    price_cap_rounding_discount=0.10,
    )
    result = engine.calculate("SKU-CARO", 15000.0)
    assert result.final_price == 15000.0
    assert result.rounding_applied is False


def test_override_ignores_price_cap_fixed_price():
    """Override fixed_price se aplica aunque el producto supere el cap."""
    engine = _make_engine(
        overrides={
            "SKU-X": {"override_type": "fixed_price", "value": 18500.0},
        },
        global_rule={"name": "MarkupGlobal", "rule_type": "percentage", "value": 3.0},
        price_cap_enabled=True,
        price_cap_max=10000.0,
    price_cap_rounding_enabled=False,
    price_cap_rounding_discount=0.10,
    )
    result = engine.calculate("SKU-X", 15000.0)
    assert result.final_price == 18500.0
    assert result.override_applied is not None
    assert "fixed_price" in result.override_applied


def test_override_percentage_ignores_price_cap():
    """Override percentage también ignora el cap.

    Se deshabilita el redondeo en este test para aislar el comportamiento
    del cap (la lógica existente sí aplica redondeo a overrides %).
    """
    engine = _make_engine(
        overrides={
            "SKU-Y": {"override_type": "percentage", "value": 10.0},
        },
        rounding_enabled=False,
        price_cap_enabled=True,
        price_cap_max=10000.0,
    price_cap_rounding_enabled=False,
    price_cap_rounding_discount=0.10,
    )
    result = engine.calculate("SKU-Y", 15000.0)
    assert result.final_price == 16500.0
    assert result.override_applied is not None
    assert "override" in result.override_applied.lower()


def test_price_cap_disabled():
    """Con price_cap_enabled=False el cap no aplica."""
    engine = _make_engine(
        global_rule={"name": "MarkupGlobal", "rule_type": "percentage", "value": 3.0},
        price_cap_enabled=False,
        price_cap_max=10000.0,
    price_cap_rounding_enabled=False,
    price_cap_rounding_discount=0.10,
    )
    result = engine.calculate("SKU-CARO", 15000.0)
    assert result.final_price == pytest.approx(15450.0)
    assert result.global_rule_applied is not None
    assert result.global_rule_applied != "price_cap_skip"


def test_price_below_cap_normal_flow():
    """Precio por debajo del cap sigue el flujo normal (markup + redondeo)."""
    engine = _make_engine(
        global_rule={"name": "MarkupGlobal", "rule_type": "percentage", "value": 3.0},
        rounding_enabled=True,
        rounding_threshold=200.0,
        rounding_low_mode="nearest_99",
        rounding_high_mode="ceil_x9_99",
        price_cap_enabled=True,
        price_cap_max=10000.0,
    price_cap_rounding_enabled=False,
    price_cap_rounding_discount=0.10,
    )
    result = engine.calculate("SKU-NORMAL", 500.0)
    # 500 * 1.03 = 515.0 → redondeo high (>=200) ceil_x9_99 → 519.99
    assert result.final_price == 519.99
    assert result.global_rule_applied is not None
    assert result.global_rule_applied != "price_cap_skip"
    assert result.rounding_applied is True


def test_price_exactly_at_cap_uses_normal_flow():
    """Precio exactamente igual al cap NO dispara el cap (solo > cap)."""
    engine = _make_engine(
        global_rule={"name": "MarkupGlobal", "rule_type": "percentage", "value": 3.0},
        price_cap_enabled=True,
        price_cap_max=10000.0,
    price_cap_rounding_enabled=False,
    price_cap_rounding_discount=0.10,
    )
    result = engine.calculate("SKU-LIMITE", 10000.0)
    assert result.final_price == pytest.approx(10300.0)
    assert result.global_rule_applied != "price_cap_skip"


# --- Prefix overrides ---

def test_prefix_override_percentage():
    engine = _make_engine(
        prefix_overrides=[{"sku_prefix": "AO", "override_type": "percentage", "value": -5.0}],
        rounding_enabled=False,
    )
    result = engine.calculate("AOSKU001", 1000.0)
    assert result.final_price == 950.0
    assert result.override_applied is not None
    assert "prefix" in result.override_applied.lower()


def test_prefix_override_fixed_amount():
    engine = _make_engine(
        prefix_overrides=[{"sku_prefix": "HU", "override_type": "fixed_amount", "value": -30.0}],
        rounding_enabled=True, rounding_threshold=200.0,
    )
    result = engine.calculate("HUASPCG3", 500.0)
    assert result.final_price == 479.99
    assert result.rounding_applied is True


def test_exact_override_beats_prefix():
    engine = _make_engine(
        overrides={"AOSKU001": {"override_type": "fixed_price", "value": 999.0}},
        prefix_overrides=[{"sku_prefix": "AO", "override_type": "percentage", "value": -5.0}],
    )
    result = engine.calculate("AOSKU001", 1000.0)
    assert result.final_price == 999.0
    assert "fixed_price" in result.override_applied


def test_longer_prefix_wins():
    engine = _make_engine(
        prefix_overrides=[
            {"sku_prefix": "AOSP", "override_type": "percentage", "value": 10.0},
            {"sku_prefix": "AO", "override_type": "percentage", "value": 5.0},
        ],
        rounding_enabled=False,
    )
    result = engine.calculate("AOSPABC", 1000.0)
    assert result.final_price == 1100.0


def test_prefix_no_match_falls_to_global():
    engine = _make_engine(
        prefix_overrides=[{"sku_prefix": "ZZ", "override_type": "percentage", "value": 50.0}],
        global_rule={"name": "Markup", "rule_type": "percentage", "value": 3.0},
        rounding_enabled=False,
    )
    result = engine.calculate("AOSKU001", 1000.0)
    assert result.final_price == 1030.0


def test_price_cap_rounding_enabled():
    engine = _make_engine(price_cap_enabled=True, price_cap_max=10000.0, price_cap_rounding_enabled=True, price_cap_rounding_discount=0.10)
    result = engine.calculate("SKU-CARO", 15000.0)
    assert result.final_price == 14999.90


def test_price_cap_rounding_disabled():
    engine = _make_engine(price_cap_enabled=True, price_cap_max=10000.0, price_cap_rounding_enabled=False)
    result = engine.calculate("SKU-CARO", 15000.0)
    assert result.final_price == 15000.0


def test_price_cap_rounding_custom_discount():
    engine = _make_engine(price_cap_enabled=True, price_cap_max=10000.0, price_cap_rounding_enabled=True, price_cap_rounding_discount=1.00)
    result = engine.calculate("SKU-CARO", 15000.0)
    assert result.final_price == 14999.0


def test_price_cap_rounding_override_ignores_cap():
    engine = _make_engine(
        overrides={"SKU-X": {"override_type": "fixed_price", "value": 18500.0}},
        price_cap_enabled=True, price_cap_max=10000.0,
        price_cap_rounding_enabled=True, price_cap_rounding_discount=0.10,
    )
    result = engine.calculate("SKU-X", 15000.0)
    assert result.final_price == 18500.0
