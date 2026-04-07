from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Query

from ..auth import require_api_key
from ..db import get_pool
from ..error_handler import log_endpoint_errors

router = APIRouter(dependencies=[Depends(require_api_key)])


def _parse_naive_dt(s: str) -> datetime:
    """Parse ISO datetime string and strip timezone info to get a naive datetime."""
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


@router.get("/inventory/changes")
@log_endpoint_errors
async def inventory_changes(
    direction: str = Query("all", pattern=r"^(stock_to_out|out_to_stock|all)$"),
    since: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()

    if since:
        since_dt = _parse_naive_dt(since)
    else:
        since_dt = datetime.now() - timedelta(hours=24)

    conditions = ["sa.action_type = 'inventory'", "sa.created_at >= $1"]
    params: list = [since_dt]
    idx = 2

    if direction == "stock_to_out":
        conditions.append("sa.new_value = '0'")
    elif direction == "out_to_stock":
        conditions.append("sa.old_value = '0' AND sa.new_value != '0'")

    where = " AND ".join(conditions)

    count_sql = f"""
        SELECT COUNT(*) FROM sync_actions sa WHERE {where}
    """
    total = await pool.fetchval(count_sql, *params)

    offset = (page - 1) * per_page
    query_sql = f"""
        SELECT sa.id, sa.run_id, sa.sku_norm as sku, sa.action_type,
               sa.old_value, sa.new_value, sa.status, sa.error, sa.created_at,
               COALESCE(sv.title, 'Sin título') as title, sv.variant_id
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
