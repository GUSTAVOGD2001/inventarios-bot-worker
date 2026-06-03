from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from ..auth import require_api_key
from ..config import settings
from ..db import get_pool
from ..error_handler import log_endpoint_errors

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/health")
@log_endpoint_errors
async def health_check():
    pool = await get_pool()
    db_ok = await pool.fetchval("SELECT 1")
    sku_count = await pool.fetchval("SELECT COUNT(*) FROM sku_state")
    variant_count = await pool.fetchval("SELECT COUNT(*) FROM shopify_variants")
    shopify_configured = bool(settings.shopify_shop and settings.shopify_client_id and settings.shopify_client_secret)
    return {
        "overall_status": "healthy" if db_ok == 1 else "critical",
        "checks": {
            "database": {"status": "ok" if db_ok == 1 else "error"},
            "shopify_config": {"status": "ok" if shopify_configured else "warning"},
        },
        "skus_in_db": sku_count,
        "shopify_variants_count": variant_count,
        "last_check_at": datetime.now(timezone.utc).isoformat(),
    }
