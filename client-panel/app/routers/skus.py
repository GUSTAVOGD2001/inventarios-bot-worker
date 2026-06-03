from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import get_pool
from ..error_handler import log_endpoint_errors
from ..pricing_engine import calculate_final_price

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/sku/search")
@log_endpoint_errors
async def search_skus(
    q: str = Query("", max_length=100),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()
    offset = (page - 1) * per_page
    pattern = f"%{q.strip().upper()}%"
    total = await pool.fetchval(
        """SELECT COUNT(*) FROM sku_state ss
           FULL OUTER JOIN shopify_variants sv ON sv.sku = ss.sku
           WHERE COALESCE(ss.sku, sv.sku) ILIKE $1""",
        pattern,
    )
    rows = await pool.fetch(
        """SELECT COALESCE(ss.sku, sv.sku) AS sku, ss.ddvc_price, ss.source_price,
                  ss.is_salable, ss.stock_status, ss.target_qty, ss.final_price,
                  ss.last_received_at, sv.title, sv.variant_id, sv.current_price, sv.current_qty
           FROM sku_state ss
           FULL OUTER JOIN shopify_variants sv ON sv.sku = ss.sku
           WHERE COALESCE(ss.sku, sv.sku) ILIKE $1
           ORDER BY COALESCE(ss.sku, sv.sku)
           LIMIT $2 OFFSET $3""",
        pattern,
        per_page,
        offset,
    )
    return {"total": total, "page": page, "per_page": per_page, "items": [dict(r) for r in rows]}


@router.get("/sku/{sku}/analysis")
@log_endpoint_errors
async def sku_analysis(sku: str):
    pool = await get_pool()
    sku_norm = sku.strip().upper()
    state = await pool.fetchrow("SELECT * FROM sku_state WHERE UPPER(sku) = UPPER($1)", sku_norm)
    variant = await pool.fetchrow("SELECT * FROM shopify_variants WHERE UPPER(sku) = UPPER($1)", sku_norm)
    if not state and not variant:
        raise HTTPException(status_code=404, detail="SKU not found")

    pricing = None
    if state and state["ddvc_price"] is not None:
        source_price = float(state["source_price"]) if state["source_price"] is not None else None
        pricing = await calculate_final_price(
            sku_norm,
            float(state["ddvc_price"]),
            pool,
            source_price=source_price,
        )
    history_rows = await pool.fetch(
        """SELECT * FROM price_change_log WHERE UPPER(sku) = UPPER($1)
           ORDER BY created_at DESC LIMIT 20""",
        sku_norm,
    )
    action_rows = await pool.fetch(
        """SELECT * FROM sync_actions WHERE UPPER(sku) = UPPER($1)
           ORDER BY created_at DESC LIMIT 20""",
        sku_norm,
    )
    return {
        "sku": sku_norm,
        "shopify": dict(variant) if variant else None,
        "ddvc": dict(state) if state else None,
        "pricing": pricing,
        "price_history": [dict(r) for r in history_rows],
        "sync_history": [dict(r) for r in action_rows],
    }
