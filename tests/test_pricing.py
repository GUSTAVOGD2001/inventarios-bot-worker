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
    rounding_enabled=False,
    rounding_threshold=200.0,
    rounding_low_mode="nearest_99",
    rounding_high_mode="ceil_x9_99",
    global_markup_enabled=True,
    price_cap_enabled=True,
    price_cap_max=10000.0,
) -> PricingEngine:
    engine = PricingEngine(engine=None)  # type: ignore[arg-type]
    engine.overrides = overrides or {}
    engine.global_rule = global_rule
    engine.rounding_enabled = rounding_enabled
    engine.rounding_threshold = rounding_threshold
    engine.rounding_low_mode = rounding_low_mode
    engine.rounding_high_mode = rounding_high_mode
    engine.global_markup_enabled = global_markup_enabled
    engine.price_cap_enabled = price_cap_enabled
    engine.price_cap_max = price_cap_max
    return engine


def test_price_cap_blocks_global_rule():
    """Producto arriba del cap no recibe markup global."""
    engine = _make_engine(
        global_rule={"name": "MarkupGlobal", "rule_type": "percentage", "value": 3.0},
        price_cap_enabled=True,
        price_cap_max=10000.0,
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
    )
    result = engine.calculate("SKU-Y", 15000.0)
    assert result.final_price == 16500.0
    assert result.override_applied is not None
    assert "percentage" in result.override_applied


def test_price_cap_disabled():
    """Con price_cap_enabled=False el cap no aplica."""
    engine = _make_engine(
        global_rule={"name": "MarkupGlobal", "rule_type": "percentage", "value": 3.0},
        price_cap_enabled=False,
        price_cap_max=10000.0,
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
    )
    result = engine.calculate("SKU-LIMITE", 10000.0)
    assert result.final_price == pytest.approx(10300.0)
    assert result.global_rule_applied != "price_cap_skip"
