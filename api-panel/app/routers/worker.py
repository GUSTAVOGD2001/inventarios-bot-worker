import json
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query

from ..auth import require_api_key
from ..config import settings
from ..db import get_pool
from ..models import WorkerTrigger

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/worker/status")
async def worker_status():
    pool = await get_pool()

    # Last sync run
    last_run = await pool.fetchrow(
        "SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT 1"
    )

    # Dry run setting from app_kv
    dry_run_row = await pool.fetchrow(
        "SELECT value FROM app_kv WHERE key = 'dry_run'"
    )

    last_sync = None
    next_sync = None
    status = "idle"

    if last_run:
        last_sync = dict(last_run)
        started = last_run.get("started_at")
        finished = last_run.get("finished_at")

        if finished:
            status = "idle"
            next_dt = finished + timedelta(minutes=settings.run_interval_min)
            next_sync = next_dt.isoformat()
        elif started:
            status = "running"

    dry_run = None
    if dry_run_row:
        val = dry_run_row["value"]
        try:
            dry_run = json.loads(val) if isinstance(val, str) else val
        except (json.JSONDecodeError, TypeError):
            dry_run = val

    return {
        "status": status,
        "last_sync": last_sync,
        "next_sync_estimated": next_sync,
        "dry_run": dry_run,
    }


@router.post("/worker/sync")
async def trigger_sync(body: WorkerTrigger):
    pool = await get_pool()
    payload = json.dumps({
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": body.dry_run,
    })
    await pool.execute(
        """INSERT INTO app_kv (key, value)
           VALUES ('trigger_sync', $1)
           ON CONFLICT (key) DO UPDATE SET value = $1""",
        payload,
    )
    return {"message": "Sync trigger saved", "dry_run": body.dry_run}


@router.get("/worker/history")
async def worker_history(
    since: str | None = None,
    status: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()
    conditions = []
    params: list = []
    idx = 1

    if since:
        conditions.append(f"started_at >= ${idx}")
        params.append(datetime.fromisoformat(since))
        idx += 1
    if status:
        conditions.append(f"error IS {'NOT NULL' if status == 'error' else 'NULL'}")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    total = await pool.fetchval(f"SELECT COUNT(*) FROM sync_runs {where}", *params)

    offset = (page - 1) * per_page
    params.extend([per_page, offset])
    rows = await pool.fetch(
        f"SELECT * FROM sync_runs {where} ORDER BY started_at DESC LIMIT ${idx} OFFSET ${idx + 1}",
        *params,
    )
    items = [dict(r) for r in rows]
    return {"total": total, "page": page, "per_page": per_page, "items": items}
