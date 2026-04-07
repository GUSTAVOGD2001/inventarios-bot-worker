import json
import logging

import asyncpg

from .rounding import apply_rounding

logger = logging.getLogger(__name__)


async def _get_setting(pool: asyncpg.Pool, key: str) -> object:
    row = await pool.fetchrow("SELECT value FROM panel_settings WHERE key = $1", key)
    if row is None:
        return None
    return json.loads(row["value"]) if isinstance(row["value"], str) else row["value"]


async def calculate_final_price(
    sku: str,
    ddvc_price: float,
    pool: asyncpg.Pool,
    *,
    preview_rules: dict | None = None,
) -> dict:
    """
    Calculate final price through the full pricing pipeline.

    If preview_rules is provided, uses those instead of DB rules:
        preview_rules = {"global_markup": 5.0, "rounding_enabled": True}
    """
    steps: list[str] = []
    override_applied: str | None = None
    global_rule_applied: str | None = None
    rounding_applied = False
    price = ddvc_price

    steps.append(f"Base: ${ddvc_price:.2f}")

    # --- Step 1: Check SKU override ---
    if preview_rules is None:
        override = await pool.fetchrow(
            "SELECT override_type, value FROM sku_overrides WHERE sku = $1 AND is_active = true",
            sku,
        )
    else:
        override = None

    if override:
        otype = override["override_type"]
        oval = float(override["value"])
        if otype == "fixed_price":
            # Fixed price exits immediately, skips rounding
            override_applied = f"fixed_price: ${oval:.2f}"
            steps.append(f"Override fixed_price: ${oval:.2f}")
            margin_amount = oval - ddvc_price
            margin_pct = (margin_amount / ddvc_price * 100) if ddvc_price else 0
            return {
                "sku": sku,
                "ddvc_price": ddvc_price,
                "override_applied": override_applied,
                "global_rule_applied": None,
                "after_rules": oval,
                "rounding_applied": False,
                "final_price": oval,
                "margin_amount": round(margin_amount, 2),
                "margin_percent": round(margin_pct, 2),
                "steps": steps,
            }
        elif otype == "percentage":
            price = ddvc_price * (1 + oval / 100)
            override_applied = f"percentage: +{oval}%"
            steps.append(f"Override +{oval}%: ${price:.2f}")
        elif otype == "fixed_amount":
            price = ddvc_price + oval
            override_applied = f"fixed_amount: +${oval:.2f}"
            steps.append(f"Override +${oval:.2f}: ${price:.2f}")
    else:
        # --- Step 2: Global pricing rule ---
        if preview_rules is not None:
            markup = preview_rules.get("global_markup")
            if markup is not None and markup != 0:
                price = ddvc_price * (1 + markup / 100)
                global_rule_applied = f"Preview markup {markup}%"
                steps.append(f"Preview markup {markup}%: ${price:.2f}")
        else:
            rule = await pool.fetchrow(
                "SELECT name, rule_type, value FROM pricing_rules "
                "WHERE is_active = true ORDER BY priority DESC LIMIT 1"
            )
            if rule:
                rtype = rule["rule_type"]
                rval = float(rule["value"])
                rname = rule["name"]
                if rtype == "percentage":
                    price = ddvc_price * (1 + rval / 100)
                    global_rule_applied = f"{rname} {rval}%"
                    steps.append(f"{rname} {rval}%: ${price:.2f}")
                elif rtype == "fixed_amount":
                    price = ddvc_price + rval
                    global_rule_applied = f"{rname} +${rval:.2f}"
                    steps.append(f"{rname} +${rval:.2f}: ${price:.2f}")

    after_rules = round(price, 2)

    # --- Step 3: Rounding ---
    if preview_rules is not None:
        do_round = preview_rules.get("rounding_enabled", False)
    else:
        do_round = await _get_setting(pool, "rounding_enabled")
        if do_round is None:
            do_round = False

    if do_round:
        # Leer configuración de redondeo por rango
        threshold_val = await _get_setting(pool, "rounding_threshold")
        low_mode_val = await _get_setting(pool, "rounding_low_mode")
        high_mode_val = await _get_setting(pool, "rounding_high_mode")

        threshold = float(threshold_val) if threshold_val is not None else 200.0
        low_mode = low_mode_val if low_mode_val else "nearest_99"
        high_mode = high_mode_val if high_mode_val else "ceil_x9_99"

        rounded_price, mode_used = apply_rounding(price, threshold, low_mode, high_mode)
        if rounded_price != round(price, 2):
            rounding_applied = True
            mode_label = "al .99 más cercano" if mode_used == "nearest_99" else "X9.99"
            steps.append(f"Redondeo {mode_label} (rango {'<' if price < threshold else '≥'}${threshold:.0f}): ${rounded_price:.2f}")
        price = rounded_price

    final_price = round(price, 2)
    margin_amount = final_price - ddvc_price
    margin_pct = (margin_amount / ddvc_price * 100) if ddvc_price else 0

    return {
        "sku": sku,
        "ddvc_price": ddvc_price,
        "override_applied": override_applied,
        "global_rule_applied": global_rule_applied,
        "after_rules": after_rules,
        "rounding_applied": rounding_applied,
        "final_price": final_price,
        "margin_amount": round(margin_amount, 2),
        "margin_percent": round(margin_pct, 2),
        "steps": steps,
    }
