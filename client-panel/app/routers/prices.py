from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Query

from ..auth import require_api_key
from ..db import get_pool
from ..error_handler import log_endpoint_errors

router = APIRouter(dependencies=[Depends(require_api_key)])


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


@router.get("/prices/changes")
@log_endpoint_errors
async def price_changes(
    since: str | None = None,
    min_diff: float = Query(0.01, ge=0),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()
    since_dt = _parse_dt(since) if since else datetime.now() - timedelta(hours=24)
    total = await pool.fetchval(
        """SELECT COUNT(*) FROM price_change_log
           WHERE created_at >= $1
             AND ABS(COALESCE(price_after, 0) - COALESCE(price_before, 0)) >= $2""",
        since_dt,
        min_diff,
    )
    offset = (page - 1) * per_page
    rows = await pool.fetch(
        """SELECT pcl.*, COALESCE(sv.title, 'Sin título') AS title, sv.variant_id
           FROM price_change_log pcl
           LEFT JOIN shopify_variants sv ON sv.sku = pcl.sku
           WHERE pcl.created_at >= $1
             AND ABS(COALESCE(pcl.price_after, 0) - COALESCE(pcl.price_before, 0)) >= $2
           ORDER BY pcl.created_at DESC
           LIMIT $3 OFFSET $4""",
        since_dt,
        min_diff,
        per_page,
        offset,
    )
    return {"total": total, "page": page, "per_page": per_page, "items": [dict(r) for r in rows]}
