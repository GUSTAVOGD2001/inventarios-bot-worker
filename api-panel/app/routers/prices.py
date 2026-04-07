import logging
import traceback
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import get_pool

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_api_key)])


def _parse_naive_dt(s: str) -> datetime:
    """Parse ISO datetime string and strip timezone info to get a naive datetime."""
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


@router.get("/prices/changes")
async def price_changes(
    since: str | None = None,
    min_diff: float = Query(0.01, ge=0),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    try:
        pool = await get_pool()

        if since:
            since_dt = _parse_naive_dt(since)
        else:
            since_dt = datetime.now() - timedelta(hours=24)

        count_sql = """
            SELECT COUNT(*) FROM price_change_log pcl
            WHERE pcl.created_at >= $1
              AND ABS(COALESCE(pcl.price_after, 0) - COALESCE(pcl.price_before, 0)) >= $2
        """
        total = await pool.fetchval(count_sql, since_dt, min_diff)

        offset = (page - 1) * per_page
        query_sql = """
            SELECT pcl.id, pcl.sku, pcl.ddvc_price, pcl.rule_applied,
                   pcl.price_before, pcl.price_after, pcl.was_applied, pcl.created_at,
                   COALESCE(sv.title, 'Sin título') as title, sv.variant_id
            FROM price_change_log pcl
            LEFT JOIN shopify_variants sv ON UPPER(sv.sku) = UPPER(pcl.sku)
            WHERE pcl.created_at >= $1
              AND ABS(COALESCE(pcl.price_after, 0) - COALESCE(pcl.price_before, 0)) >= $2
            ORDER BY pcl.created_at DESC
            LIMIT $3 OFFSET $4
        """
        rows = await pool.fetch(query_sql, since_dt, min_diff, per_page, offset)
        items = [dict(r) for r in rows]

        return {"total": total, "page": page, "per_page": per_page, "items": items}
    except Exception as e:
        logger.error("Error in /prices/changes: %s", e)
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
