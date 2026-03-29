from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import get_pool
from ..models import (
    PricingRuleCreate,
    PricingRuleUpdate,
    SimulateRequest,
    SkuOverrideCreate,
    SkuOverrideUpdate,
)
from ..pricing_engine import calculate_final_price

router = APIRouter(dependencies=[Depends(require_api_key)])


# ───── Pricing Rules CRUD ─────


@router.get("/pricing/rules")
async def list_rules():
    pool = await get_pool()
    rows = await pool.fetch("SELECT * FROM pricing_rules ORDER BY priority DESC")
    return [dict(r) for r in rows]


@router.post("/pricing/rules", status_code=201)
async def create_rule(body: PricingRuleCreate):
    pool = await get_pool()
    row = await pool.fetchrow(
        """INSERT INTO pricing_rules (name, rule_type, value, priority, is_active)
           VALUES ($1, $2, $3, $4, $5) RETURNING *""",
        body.name, body.rule_type, body.value, body.priority, body.is_active,
    )
    return dict(row)


@router.put("/pricing/rules/{rule_id}")
async def update_rule(rule_id: int, body: PricingRuleUpdate):
    pool = await get_pool()
    existing = await pool.fetchrow("SELECT * FROM pricing_rules WHERE id = $1", rule_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Rule not found")

    fields = []
    params = []
    idx = 1
    for field_name in ("name", "rule_type", "value", "priority", "is_active"):
        val = getattr(body, field_name)
        if val is not None:
            fields.append(f"{field_name} = ${idx}")
            params.append(val)
            idx += 1
    if not fields:
        return dict(existing)

    fields.append(f"updated_at = now()")
    params.append(rule_id)
    sql = f"UPDATE pricing_rules SET {', '.join(fields)} WHERE id = ${idx} RETURNING *"
    row = await pool.fetchrow(sql, *params)
    return dict(row)


@router.delete("/pricing/rules/{rule_id}", status_code=204)
async def delete_rule(rule_id: int):
    pool = await get_pool()
    result = await pool.execute("DELETE FROM pricing_rules WHERE id = $1", rule_id)
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Rule not found")


# ───── SKU Overrides CRUD ─────


@router.get("/pricing/overrides")
async def list_overrides(
    sku: str | None = None,
    type: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()
    conditions = []
    params: list = []
    idx = 1

    if sku:
        conditions.append(f"sku = ${idx}")
        params.append(sku)
        idx += 1
    if type:
        conditions.append(f"override_type = ${idx}")
        params.append(type)
        idx += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    total = await pool.fetchval(f"SELECT COUNT(*) FROM sku_overrides {where}", *params)

    offset = (page - 1) * per_page
    params.extend([per_page, offset])
    rows = await pool.fetch(
        f"SELECT * FROM sku_overrides {where} ORDER BY sku LIMIT ${idx} OFFSET ${idx + 1}",
        *params,
    )
    items = [dict(r) for r in rows]
    return {"total": total, "page": page, "per_page": per_page, "items": items}


@router.post("/pricing/overrides", status_code=201)
async def create_override(body: SkuOverrideCreate):
    pool = await get_pool()
    try:
        row = await pool.fetchrow(
            """INSERT INTO sku_overrides (sku, override_type, value, is_active, notes)
               VALUES ($1, $2, $3, $4, $5) RETURNING *""",
            body.sku, body.override_type, body.value, body.is_active, body.notes,
        )
    except Exception as e:
        if "unique" in str(e).lower():
            raise HTTPException(status_code=409, detail=f"Override for SKU '{body.sku}' already exists")
        raise
    return dict(row)


@router.put("/pricing/overrides/{override_id}")
async def update_override(override_id: int, body: SkuOverrideUpdate):
    pool = await get_pool()
    existing = await pool.fetchrow("SELECT * FROM sku_overrides WHERE id = $1", override_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Override not found")

    fields = []
    params = []
    idx = 1
    for field_name in ("sku", "override_type", "value", "is_active", "notes"):
        val = getattr(body, field_name)
        if val is not None:
            fields.append(f"{field_name} = ${idx}")
            params.append(val)
            idx += 1
    if not fields:
        return dict(existing)

    fields.append(f"updated_at = now()")
    params.append(override_id)
    sql = f"UPDATE sku_overrides SET {', '.join(fields)} WHERE id = ${idx} RETURNING *"
    row = await pool.fetchrow(sql, *params)
    return dict(row)


@router.delete("/pricing/overrides/{override_id}", status_code=204)
async def delete_override(override_id: int):
    pool = await get_pool()
    result = await pool.execute("DELETE FROM sku_overrides WHERE id = $1", override_id)
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Override not found")


# ───── Simulate ─────


@router.post("/pricing/simulate")
async def simulate_pricing(body: SimulateRequest):
    pool = await get_pool()
    results = []

    for sku in body.skus:
        row = await pool.fetchrow(
            "SELECT ddvc_price FROM sku_state WHERE UPPER(sku) = UPPER($1)", sku
        )
        ddvc_price = float(row["ddvc_price"]) if row and row["ddvc_price"] else None

        if ddvc_price is None:
            results.append({
                "sku": sku,
                "ddvc_price": None,
                "error": "SKU not found in sku_state or no DDVC price",
            })
            continue

        calc = await calculate_final_price(
            sku, ddvc_price, pool, preview_rules=body.preview_rules
        )
        results.append(calc)

    return {"items": results}
