from datetime import datetime

from fastapi import APIRouter, Depends, Query

from ..auth import require_api_key
from ..db import get_pool
from ..error_handler import log_endpoint_errors

router = APIRouter(dependencies=[Depends(require_api_key)])


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


@router.get("/worker/status")
@log_endpoint_errors
async def worker_status():
    pool = await get_pool()
    last_run = await pool.fetchrow("SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT 1")
    status = "idle"
    if last_run and not last_run.get("finished_at"):
        status = "receiving"
    return {"status": status, "last_sync": dict(last_run) if last_run else None}


@router.get("/worker/history")
@log_endpoint_errors
async def worker_history(
    since: str | None = None,
    status: str | None = Query(None, pattern=r"^(ok|error)$"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()
    conditions = []
    params: list = []
    idx = 1
    if since:
        conditions.append(f"started_at >= ${idx}")
        params.append(_parse_dt(since))
        idx += 1
    if status:
        conditions.append(f"error IS {'NULL' if status == 'ok' else 'NOT NULL'}")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    total = await pool.fetchval(f"SELECT COUNT(*) FROM sync_runs {where}", *params)
    offset = (page - 1) * per_page
    params.extend([per_page, offset])
    rows = await pool.fetch(
        f"SELECT * FROM sync_runs {where} ORDER BY started_at DESC LIMIT ${idx} OFFSET ${idx + 1}",
        *params,
    )
    return {"total": total, "page": page, "per_page": per_page, "items": [dict(r) for r in rows]}
