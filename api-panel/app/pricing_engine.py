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
    steps: list[str] = []
    override_applied: str | None = None
    global_rule_applied: str | None = None
    rounding_applied = False
    price = ddvc_price

    sku = sku.strip().upper()
    steps.append(f"Base: ${ddvc_price:.2f}")

    if preview_rules is None:
        override = await pool.fetchrow(
            "SELECT override_type, value FROM sku_overrides WHERE sku = $1 AND is_active = true",
            sku,
        )
    else:
        override = None

    override_source = "exact" if override else None
    if not override and preview_rules is None:
        prefix_row = await pool.fetchrow(
            """SELECT sku_prefix, override_type, value FROM sku_prefix_overrides
               WHERE is_active = true AND $1 LIKE sku_prefix || '%'
               ORDER BY LENGTH(sku_prefix) DESC LIMIT 1""",
            sku,
        )
        if prefix_row:
            override = {"override_type": prefix_row["override_type"], "value": float(prefix_row["value"])}
            override_source = f"prefix:{prefix_row['sku_prefix']}"

    if override:
        otype = override["override_type"]
        oval = float(override["value"])
        source_label = f"Override (prefix {override_source.split(':')[1]})" if override_source and override_source.startswith("prefix:") else "Override"
        if otype == "fixed_price":
            override_applied = f"fixed_price ({source_label}): ${oval:.2f}"
            steps.append(f"{source_label} fixed_price: ${oval:.2f}")
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
            override_applied = f"{source_label} +{oval}%"
            steps.append(f"{source_label} +{oval}%: ${price:.2f}")
        elif otype == "fixed_amount":
            price = ddvc_price + oval
            override_applied = f"{source_label} +${oval:.2f}"
            steps.append(f"{source_label} +${oval:.2f}: ${price:.2f}")
    else:
        if preview_rules is not None:
            cap_enabled = preview_rules.get("price_cap_enabled", True)
            cap_max = preview_rules.get("price_cap_max", 10000)
        else:
            cap_enabled_val = await _get_setting(pool, "price_cap_enabled")
            cap_max_val = await _get_setting(pool, "price_cap_max")
            cap_enabled = bool(cap_enabled_val) if cap_enabled_val is not None else True
            cap_max = float(cap_max_val) if cap_max_val is not None else 10000.0

        if cap_enabled and ddvc_price > cap_max:
            cap_rounding_val = await _get_setting(pool, "price_cap_rounding_enabled")
            cap_rounding_enabled = bool(cap_rounding_val) if cap_rounding_val is not None else False
            cap_discount_val = await _get_setting(pool, "price_cap_rounding_discount")
            cap_discount = float(cap_discount_val) if cap_discount_val is not None else 0.10

            cap_price = ddvc_price
            cap_rounding = False
            if cap_rounding_enabled:
                cap_price = round(ddvc_price - cap_discount, 2)
                cap_rounding = True
                steps.append(f"Price cap: ${ddvc_price:.2f} > ${cap_max:.2f}, sin markup. Redondeo cap -${cap_discount:.2f}: ${cap_price:.2f}")
            else:
                steps.append(f"Price cap: ${ddvc_price:.2f} > ${cap_max:.2f}, saltando markup y redondeo")

            margin_amount = cap_price - ddvc_price
            margin_pct = (margin_amount / ddvc_price * 100) if ddvc_price else 0
            return {
                "sku": sku, "ddvc_price": ddvc_price,
                "override_applied": None, "global_rule_applied": "price_cap_skip",
                "after_rules": cap_price, "rounding_applied": cap_rounding,
                "final_price": cap_price,
                "margin_amount": round(margin_amount, 2), "margin_percent": round(margin_pct, 2),
                "steps": steps,
            }

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

    if preview_rules is not None:
        do_round = preview_rules.get("rounding_enabled", False)
    else:
        do_round = await _get_setting(pool, "rounding_enabled")
        if do_round is None:
            do_round = False

    if do_round:
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
