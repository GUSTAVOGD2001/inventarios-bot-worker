"""Pricing engine for the sync worker (SQLAlchemy / synchronous)."""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PriceResult:
    sku: str
    ddvc_price: float
    final_price: float
    override_applied: Optional[str]
    global_rule_applied: Optional[str]
    rounding_applied: bool
    steps: List[str]
    margin_amount: float
    margin_percent: float


def round_up_x9_99(price: float) -> float:
    """Redondea hacia ARRIBA al próximo X9.99."""
    if price <= 9.99:
        return 9.99
    tens = math.floor(price / 10)
    target = tens * 10 + 9.99
    if price > target:
        target += 10
    return round(target, 2)


def round_nearest_99_cents(price: float) -> float:
    """Pone los centavos en .99 del MISMO entero, sin bajar.
    Ejemplos:
        56.00 → 56.99
        56.30 → 56.99
        56.99 → 56.99 (ya está)
        57.01 → 57.99
        142.30 → 142.99
    """
    if price <= 0.99:
        return 0.99
    integer_part = math.floor(price)
    candidate = integer_part + 0.99
    return round(candidate, 2)


def apply_rounding(price: float, threshold: float, low_mode: str, high_mode: str) -> tuple[float, str]:
    """Aplica el redondeo según el rango del precio."""
    mode = low_mode if price < threshold else high_mode
    if mode == "nearest_99":
        return round_nearest_99_cents(price), "nearest_99"
    elif mode == "ceil_x9_99":
        return round_up_x9_99(price), "ceil_x9_99"
    else:
        return round(price, 2), "none"


def _get_setting(engine: Engine, key: str):
    """Lee un valor de panel_settings."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT value FROM panel_settings WHERE key = :key"),
            {"key": key},
        ).fetchone()
        if row is None:
            return None
        val = row[0]
        if isinstance(val, str):
            return json.loads(val)
        return val


def _load_overrides(engine: Engine) -> Dict[str, dict]:
    """Carga todos los overrides activos indexados por SKU (ya normalizado a UPPER)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT sku, override_type, value FROM sku_overrides WHERE is_active = true")
        ).fetchall()
    return {row[0].strip().upper(): {"override_type": row[1], "value": float(row[2])} for row in rows}


def _load_global_rule(engine: Engine) -> Optional[dict]:
    """Carga la regla global de mayor prioridad."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT name, rule_type, value FROM pricing_rules "
                "WHERE is_active = true ORDER BY priority DESC LIMIT 1"
            )
        ).fetchone()
    if row is None:
        return None
    return {"name": row[0], "rule_type": row[1], "value": float(row[2])}


class PricingEngine:
    """
    Carga las reglas una vez al inicio del sync y las aplica a cada SKU.
    Esto evita hacer una query por cada SKU.
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self.overrides: Dict[str, dict] = {}
        self.global_rule: Optional[dict] = None
        self.rounding_enabled: bool = False
        self.rounding_threshold: float = 200.0
        self.rounding_low_mode: str = "nearest_99"
        self.rounding_high_mode: str = "ceil_x9_99"
        self.global_markup_enabled: bool = True

    def load_rules(self) -> None:
        """Llamar una vez al inicio de cada sync run."""
        try:
            self.overrides = _load_overrides(self.engine)
            self.global_rule = _load_global_rule(self.engine)
            self.rounding_enabled = bool(_get_setting(self.engine, "rounding_enabled"))
            self.rounding_threshold = float(_get_setting(self.engine, "rounding_threshold") or 200)
            self.rounding_low_mode = _get_setting(self.engine, "rounding_low_mode") or "nearest_99"
            self.rounding_high_mode = _get_setting(self.engine, "rounding_high_mode") or "ceil_x9_99"
            self.global_markup_enabled = bool(_get_setting(self.engine, "global_markup_enabled"))
            logger.info(
                "Pricing rules loaded: overrides=%s global_rule=%s rounding=%s threshold=%s low=%s high=%s markup_enabled=%s",
                len(self.overrides),
                self.global_rule["name"] if self.global_rule else "none",
                self.rounding_enabled,
                self.rounding_threshold,
                self.rounding_low_mode,
                self.rounding_high_mode,
                self.global_markup_enabled,
            )
        except Exception:
            logger.warning("Could not load pricing rules from panel tables. Using DDVC prices directly.")
            self.overrides = {}
            self.global_rule = None
            self.rounding_enabled = False
            self.rounding_threshold = 200.0
            self.rounding_low_mode = "nearest_99"
            self.rounding_high_mode = "ceil_x9_99"
            self.global_markup_enabled = True

    def calculate(self, sku_norm: str, ddvc_price: float) -> PriceResult:
        """
        Calcula el precio final para un SKU dado el precio DDVC.

        Pipeline:
        1. ¿Override fijo? → retornar directamente (salta redondeo)
        2. ¿Override %/monto? → aplicar sobre ddvc_price
        3. ¿Regla global? → aplicar sobre ddvc_price
        4. ¿Redondeo? → aplicar round_up_x9_99
        """
        steps: List[str] = [f"Base: ${ddvc_price:.2f}"]
        override_applied: Optional[str] = None
        global_rule_applied: Optional[str] = None
        rounding_applied = False
        price = ddvc_price

        # Step 1: Check SKU override
        override = self.overrides.get(sku_norm)
        if override:
            otype = override["override_type"]
            oval = override["value"]
            if otype == "fixed_price":
                steps.append(f"Override precio fijo: ${oval:.2f}")
                margin = oval - ddvc_price
                pct = (margin / ddvc_price * 100) if ddvc_price else 0
                return PriceResult(
                    sku=sku_norm,
                    ddvc_price=ddvc_price,
                    final_price=oval,
                    override_applied=f"fixed_price: ${oval:.2f}",
                    global_rule_applied=None,
                    rounding_applied=False,
                    steps=steps,
                    margin_amount=round(margin, 2),
                    margin_percent=round(pct, 2),
                )
            elif otype == "percentage":
                price = ddvc_price * (1 + oval / 100)
                override_applied = f"percentage: +{oval}%"
                steps.append(f"Override +{oval}%: ${price:.2f}")
            elif otype == "fixed_amount":
                price = ddvc_price + oval
                override_applied = f"fixed_amount: +${oval:.2f}"
                steps.append(f"Override +${oval:.2f}: ${price:.2f}")
        else:
            # Step 2: Global rule (solo si no hay override y markup está habilitado)
            if self.global_markup_enabled and self.global_rule:
                rule = self.global_rule
                rtype = rule["rule_type"]
                rval = rule["value"]
                rname = rule["name"]
                if rtype == "percentage":
                    price = ddvc_price * (1 + rval / 100)
                    global_rule_applied = f"{rname} {rval}%"
                    steps.append(f"{rname} {rval}%: ${price:.2f}")
                elif rtype == "fixed_amount":
                    price = ddvc_price + rval
                    global_rule_applied = f"{rname} +${rval:.2f}"
                    steps.append(f"{rname} +${rval:.2f}: ${price:.2f}")

        # Step 3: Rounding por rango
        if self.rounding_enabled:
            rounded, mode_used = apply_rounding(
                price,
                self.rounding_threshold,
                self.rounding_low_mode,
                self.rounding_high_mode,
            )
            if rounded != round(price, 2):
                rounding_applied = True
                mode_label = "al .99 más cercano" if mode_used == "nearest_99" else "X9.99"
                range_label = "<" if price < self.rounding_threshold else "≥"
                steps.append(f"Redondeo {mode_label} (rango {range_label}${self.rounding_threshold:.0f}): ${rounded:.2f}")
            price = rounded

        final_price = round(price, 2)
        margin = final_price - ddvc_price
        pct = (margin / ddvc_price * 100) if ddvc_price else 0

        return PriceResult(
            sku=sku_norm,
            ddvc_price=ddvc_price,
            final_price=final_price,
            override_applied=override_applied,
            global_rule_applied=global_rule_applied,
            rounding_applied=rounding_applied,
            steps=steps,
            margin_amount=round(margin, 2),
            margin_percent=round(pct, 2),
        )


def log_price_change(
    engine: Engine,
    sku: str,
    ddvc_price: float,
    rule_applied: str,
    price_before: float,
    price_after: float,
    was_applied: bool,
) -> None:
    """Inserta un registro en price_change_log."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """INSERT INTO price_change_log (sku, ddvc_price, rule_applied, price_before, price_after, was_applied)
                       VALUES (:sku, :ddvc_price, :rule_applied, :price_before, :price_after, :was_applied)"""
                ),
                {
                    "sku": sku,
                    "ddvc_price": ddvc_price,
                    "rule_applied": rule_applied,
                    "price_before": price_before,
                    "price_after": price_after,
                    "was_applied": was_applied,
                },
            )
    except Exception:
        logger.warning("Failed to log price change for SKU %s", sku, exc_info=True)


def load_sku_exemptions(engine: Engine) -> Dict[str, dict]:
    """Carga todas las exenciones indexadas por SKU normalizado.
    Si la tabla no existe (panel no instalado), retorna dict vacío."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT sku, exempt_inventory, exempt_price FROM sku_exemptions")
            ).fetchall()
        return {
            row[0].strip().upper(): {
                "exempt_inventory": bool(row[1]),
                "exempt_price": bool(row[2]),
            }
            for row in rows
        }
    except Exception:
        logger.warning("Could not load sku_exemptions table, ignoring")
        return {}
