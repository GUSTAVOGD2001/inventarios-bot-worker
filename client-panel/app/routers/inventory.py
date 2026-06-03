from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Query

from ..auth import require_api_key
from ..db import get_pool
from ..error_handler import log_endpoint_errors

router = APIRouter(dependencies=[Depends(require_api_key)])


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


@router.get("/inventory/changes")
@log_endpoint_errors
async def inventory_changes(
    since: str | None = None,
    status: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()
    since_dt = _parse_dt(since) if since else datetime.now() - timedelta(hours=24)
    conditions = ["action_type = 'inventory'", "created_at >= $1"]
    params: list = [since_dt]
    idx = 2
    if status:
        conditions.append(f"status = ${idx}")
        params.append(status)
        idx += 1
    where = "WHERE " + " AND ".join(conditions)
    total = await pool.fetchval(f"SELECT COUNT(*) FROM sync_actions {where}", *params)
    offset = (page - 1) * per_page
    params.extend([per_page, offset])
    rows = await pool.fetch(
        f"""SELECT sa.*, sv.title, sv.variant_id
            FROM sync_actions sa
            LEFT JOIN shopify_variants sv ON sv.sku = sa.sku
            {where}
            ORDER BY sa.created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}""",
        *params,
    )
    return {"total": total, "page": page, "per_page": per_page, "items": [dict(r) for r in rows]}
