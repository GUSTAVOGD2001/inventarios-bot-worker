import json
from datetime import date, datetime

from fastapi import APIRouter, Depends

from ..auth import require_api_key
from ..db import get_pool
from ..error_handler import log_endpoint_errors

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/stats/summary")
@log_endpoint_errors
async def stats_summary():
    pool = await get_pool()

    shopify_count = await pool.fetchval("SELECT COUNT(*) FROM shopify_variants")
    sku_state_count = await pool.fetchval("SELECT COUNT(*) FROM sku_state")

    matched = await pool.fetchval(
        """SELECT COUNT(*) FROM shopify_variants sv
           INNER JOIN sku_state ss ON sv.sku = ss.sku"""
    )

    ddvc_only = await pool.fetchval(
        """SELECT COUNT(*) FROM sku_state ss
           LEFT JOIN shopify_variants sv ON sv.sku = ss.sku
           WHERE sv.sku IS NULL"""
    )

    shopify_only = await pool.fetchval(
        """SELECT COUNT(*) FROM shopify_variants sv
           LEFT JOIN sku_state ss ON ss.sku = sv.sku
           WHERE ss.sku IS NULL"""
    )

    in_stock = await pool.fetchval(
        "SELECT COUNT(*) FROM sku_state WHERE ddvc_salable = true"
    )
    out_of_stock = await pool.fetchval(
        "SELECT COUNT(*) FROM sku_state WHERE ddvc_salable = false"
    )

    today_start = datetime.combine(date.today(), datetime.min.time())
    changes_today = await pool.fetchval(
        "SELECT COUNT(*) FROM sync_actions WHERE created_at >= $1",
        today_start,
    )

    # Panel settings
    settings_rows = await pool.fetch("SELECT key, value FROM panel_settings")
    settings_dict = {}
    for r in settings_rows:
        val = r["value"]
        settings_dict[r["key"]] = json.loads(val) if isinstance(val, str) else val

    return {
        "shopify_variants_count": shopify_count,
        "sku_state_count": sku_state_count,
        "matched": matched,
        "ddvc_only": ddvc_only,
        "shopify_only": shopify_only,
        "in_stock": in_stock,
        "out_of_stock": out_of_stock,
        "changes_today": changes_today,
        "settings": settings_dict,
    }
