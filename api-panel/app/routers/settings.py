import json

from fastapi import APIRouter, Depends

from ..auth import require_api_key
from ..db import get_pool

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/settings")
async def get_settings():
    pool = await get_pool()
    rows = await pool.fetch("SELECT key, value FROM panel_settings")
    result = {}
    for r in rows:
        val = r["value"]
        result[r["key"]] = json.loads(val) if isinstance(val, str) else val
    return result


@router.patch("/settings")
async def patch_settings(body: dict):
    pool = await get_pool()
    for key, value in body.items():
        json_val = json.dumps(value)
        await pool.execute(
            """INSERT INTO panel_settings (key, value, updated_at)
               VALUES ($1, $2::jsonb, now())
               ON CONFLICT (key) DO UPDATE SET value = $2::jsonb, updated_at = now()""",
            key, json_val,
        )
    # Return updated settings
    rows = await pool.fetch("SELECT key, value FROM panel_settings")
    result = {}
    for r in rows:
        val = r["value"]
        result[r["key"]] = json.loads(val) if isinstance(val, str) else val
    return result
