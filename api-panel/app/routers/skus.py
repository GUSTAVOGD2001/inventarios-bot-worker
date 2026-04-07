from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import get_pool
from ..error_handler import log_endpoint_errors
from ..pricing_engine import calculate_final_price

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/skus/mismatches")
@log_endpoint_errors
async def sku_mismatches(
    type: str = Query("all", pattern=r"^(ddvc_only|shopify_only|all)$"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()
    offset = (page - 1) * per_page

    if type == "ddvc_only":
        count_sql = """
            SELECT COUNT(*) FROM sku_state ss
            LEFT JOIN shopify_variants sv ON sv.sku = ss.sku
            WHERE sv.sku IS NULL
        """
        query_sql = """
            SELECT ss.sku, 'ddvc_only' as mismatch_type,
                   ss.ddvc_salable, ss.ddvc_price, ss.updated_at
            FROM sku_state ss
            LEFT JOIN shopify_variants sv ON sv.sku = ss.sku
            WHERE sv.sku IS NULL
            ORDER BY ss.sku
            LIMIT $1 OFFSET $2
        """
        total = await pool.fetchval(count_sql)
        rows = await pool.fetch(query_sql, per_page, offset)
    elif type == "shopify_only":
        count_sql = """
            SELECT COUNT(*) FROM shopify_variants sv
            LEFT JOIN sku_state ss ON ss.sku = sv.sku
            WHERE ss.sku IS NULL
        """
        query_sql = """
            SELECT sv.sku, 'shopify_only' as mismatch_type,
                   sv.variant_id, sv.inventory_item_id, sv.updated_at
            FROM shopify_variants sv
            LEFT JOIN sku_state ss ON ss.sku = sv.sku
            WHERE ss.sku IS NULL
            ORDER BY sv.sku
            LIMIT $1 OFFSET $2
        """
        total = await pool.fetchval(count_sql)
        rows = await pool.fetch(query_sql, per_page, offset)
    else:
        count_sql = """
            SELECT (
                SELECT COUNT(*) FROM sku_state ss
                LEFT JOIN shopify_variants sv ON sv.sku = ss.sku
                WHERE sv.sku IS NULL
            ) + (
                SELECT COUNT(*) FROM shopify_variants sv
                LEFT JOIN sku_state ss ON ss.sku = sv.sku
                WHERE ss.sku IS NULL
            )
        """
        query_sql = """
            SELECT sku, mismatch_type, updated_at FROM (
                SELECT ss.sku, 'ddvc_only' as mismatch_type, ss.updated_at
                FROM sku_state ss
                LEFT JOIN shopify_variants sv ON sv.sku = ss.sku
                WHERE sv.sku IS NULL
                UNION ALL
                SELECT sv.sku, 'shopify_only' as mismatch_type, sv.updated_at
                FROM shopify_variants sv
                LEFT JOIN sku_state ss ON ss.sku = sv.sku
                WHERE ss.sku IS NULL
            ) combined
            ORDER BY sku
            LIMIT $1 OFFSET $2
        """
        total = await pool.fetchval(count_sql)
        rows = await pool.fetch(query_sql, per_page, offset)

    items = [dict(r) for r in rows]
    return {"total": total, "page": page, "per_page": per_page, "items": items}


@router.get("/sku/search")
@log_endpoint_errors
async def sku_search(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()
    pattern = f"%{q.upper()}%"
    offset = (page - 1) * per_page

    count_sql = """
        SELECT COUNT(*) FROM (
            SELECT COALESCE(ss.sku, sv.sku) AS sku
            FROM sku_state ss
            FULL OUTER JOIN shopify_variants sv ON sv.sku = ss.sku
            WHERE COALESCE(ss.sku, sv.sku) ILIKE $1
        ) sub
    """
    total = await pool.fetchval(count_sql, pattern)

    query_sql = """
        SELECT
            COALESCE(ss.sku, sv.sku) AS sku,
            'Sin título' AS title,
            sv.variant_id,
            sv.inventory_item_id,
            ss.ddvc_salable,
            ss.ddvc_price,
            ss.target_qty,
            COALESCE(ss.updated_at, sv.updated_at) AS updated_at
        FROM sku_state ss
        FULL OUTER JOIN shopify_variants sv ON sv.sku = ss.sku
        WHERE COALESCE(ss.sku, sv.sku) ILIKE $1
        ORDER BY COALESCE(ss.sku, sv.sku)
        LIMIT $2 OFFSET $3
    """
    rows = await pool.fetch(query_sql, pattern, per_page, offset)
    items = [dict(r) for r in rows]

    return {"total": total, "page": page, "per_page": per_page, "items": items}


@router.get("/sku/{sku}/analysis")
@log_endpoint_errors
async def sku_analysis(sku: str):
    pool = await get_pool()
    sku_norm = sku.strip().upper()

    # Shopify data
    shopify = await pool.fetchrow(
        "SELECT * FROM shopify_variants WHERE sku = $1", sku_norm
    )
    # DDVC data
    ddvc = await pool.fetchrow(
        "SELECT * FROM sku_state WHERE sku = $1", sku_norm
    )

    if not shopify and not ddvc:
        raise HTTPException(status_code=404, detail=f"SKU '{sku}' no encontrado")

    # Pricing calculation
    pricing = None
    ddvc_price = float(ddvc["ddvc_price"]) if ddvc and ddvc["ddvc_price"] else None
    if ddvc_price is not None:
        pricing = await calculate_final_price(sku_norm, ddvc_price, pool)

    # History: last 20 sync_actions
    actions = await pool.fetch(
        """SELECT id, run_id, action_type, old_value, new_value, status, created_at
           FROM sync_actions WHERE sku_norm = $1
           ORDER BY created_at DESC LIMIT 20""",
        sku_norm,
    )
    # History: last 20 price_change_log
    price_log = await pool.fetch(
        """SELECT id, ddvc_price, rule_applied, price_before, price_after, was_applied, created_at
           FROM price_change_log WHERE sku = $1
           ORDER BY created_at DESC LIMIT 20""",
        sku_norm,
    )

    history = [{"source": "sync_action", **dict(r)} for r in actions] + [
        {"source": "price_change", **dict(r)} for r in price_log
    ]
    history.sort(key=lambda x: x.get("created_at") or "", reverse=True)

    return {
        "sku": sku_norm,
        "shopify": dict(shopify) if shopify else None,
        "ddvc": dict(ddvc) if ddvc else None,
        "pricing": pricing,
        "history": history[:20],
    }
