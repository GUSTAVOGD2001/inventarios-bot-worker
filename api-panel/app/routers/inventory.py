from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query

from ..auth import require_api_key
from ..db import get_pool

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/inventory/changes")
async def inventory_changes(
    direction: str = Query("all", pattern=r"^(stock_to_out|out_to_stock|all)$"),
    since: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()

    if since:
        since_dt = datetime.fromisoformat(since)
    else:
        since_dt = datetime.now(timezone.utc) - timedelta(hours=24)

    conditions = ["sa.action_type = 'inventory'", "sa.created_at >= $1"]
    params: list = [since_dt]
    idx = 2

    if direction == "stock_to_out":
        conditions.append(f"sa.new_value = '0'")
    elif direction == "out_to_stock":
        conditions.append(f"sa.old_value = '0' AND sa.new_value != '0'")

    where = " AND ".join(conditions)

    count_sql = f"""
        SELECT COUNT(*) FROM sync_actions sa WHERE {where}
    """
    total = await pool.fetchval(count_sql, *params)

    offset = (page - 1) * per_page
    query_sql = f"""
        SELECT sa.id, sa.run_id, sa.sku_norm as sku, sa.action_type,
               sa.old_value, sa.new_value, sa.status, sa.error, sa.created_at,
               sv.title, sv.variant_id
        FROM sync_actions sa
        LEFT JOIN shopify_variants sv ON UPPER(sv.sku) = UPPER(sa.sku_norm)
        WHERE {where}
        ORDER BY sa.created_at DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """
    params.extend([per_page, offset])

    rows = await pool.fetch(query_sql, *params)
    items = [dict(r) for r in rows]

    return {"total": total, "page": page, "per_page": per_page, "items": items}
