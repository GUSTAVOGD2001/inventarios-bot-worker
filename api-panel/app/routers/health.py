import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from ..alerts import auto_resolve_alerts, create_or_update_alert
from ..auth import require_api_key
from ..db import get_pool
from ..error_handler import log_endpoint_errors
from ..health_checks import ALL_CHECKS, run_all_checks

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_api_key)])


def _compute_overall_status(checks: dict) -> str:
    """Derive overall status from individual check results."""
    statuses = [c["status"] for c in checks.values()]
    if "error" in statuses:
        return "critical"
    if "warning" in statuses:
        return "degraded"
    return "healthy"


async def _process_alerts(pool, checks: dict) -> int:
    """
    For each check result, create/update or auto-resolve alerts.
    Returns count of active alerts.
    """
    for check_name, result in checks.items():
        if result["status"] == "ok":
            # Auto-resolve any active alerts for this check type
            await auto_resolve_alerts(pool, check_name)
        elif result.get("alert"):
            alert_info = result["alert"]
            dedupe_key = f"health_{check_name}"
            await create_or_update_alert(
                pool,
                alert_type=check_name,
                severity=alert_info["severity"],
                title=alert_info["title"],
                message=alert_info["message"],
                metadata=alert_info.get("metadata"),
                dedupe_key=dedupe_key,
            )

    active_count = await pool.fetchval(
        "SELECT COUNT(*) FROM worker_alerts WHERE resolved_at IS NULL"
    )
    return active_count or 0


@router.get("/health")
@log_endpoint_errors
async def health_check():
    """
    Calcula estado de salud en vivo. Ejecuta los 8 checks,
    crea/resuelve alertas, y devuelve resumen.
    """
    pool = await get_pool()
    checks = await run_all_checks(pool)
    active_alerts_count = await _process_alerts(pool, checks)

    # Strip the 'alert' key from each check result (internal use only)
    clean_checks = {
        name: {k: v for k, v in result.items() if k != "alert"}
        for name, result in checks.items()
    }

    return {
        "overall_status": _compute_overall_status(checks),
        "checks": clean_checks,
        "active_alerts_count": active_alerts_count,
        "last_check_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/health/alerts")
@log_endpoint_errors
async def list_alerts(
    status: str = Query("active", pattern=r"^(active|all)$"),
    limit: int = Query(50, ge=1, le=200),
):
    """Lista alertas desde worker_alerts."""
    pool = await get_pool()

    if status == "active":
        rows = await pool.fetch(
            """
            SELECT * FROM worker_alerts
            WHERE resolved_at IS NULL
            ORDER BY detected_at DESC
            LIMIT $1
            """,
            limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT * FROM worker_alerts
            ORDER BY detected_at DESC
            LIMIT $1
            """,
            limit,
        )

    items = []
    for r in rows:
        item = dict(r)
        # Ensure metadata is serializable
        if item.get("metadata") and isinstance(item["metadata"], str):
            try:
                item["metadata"] = json.loads(item["metadata"])
            except (json.JSONDecodeError, TypeError):
                pass
        items.append(item)

    return {"total": len(items), "items": items}


@router.post("/health/alerts/{alert_id}/acknowledge")
@log_endpoint_errors
async def acknowledge_alert(alert_id: int, body: dict):
    """Marca una alerta como vista (acknowledged) sin resolverla."""
    pool = await get_pool()

    acknowledged_by = body.get("acknowledged_by")
    if not acknowledged_by:
        raise HTTPException(status_code=422, detail="acknowledged_by es requerido")

    result = await pool.execute(
        """
        UPDATE worker_alerts
        SET acknowledged_at = NOW(), acknowledged_by = $1
        WHERE id = $2 AND resolved_at IS NULL
        """,
        acknowledged_by,
        alert_id,
    )

    count = int(result.split()[-1])
    if count == 0:
        raise HTTPException(status_code=404, detail="Alerta no encontrada o ya resuelta")

    logger.info("Alert acknowledged: id=%d by=%s", alert_id, acknowledged_by)
    return {"message": "Alerta marcada como vista", "id": alert_id}


@router.post("/health/alerts/{alert_id}/resolve")
@log_endpoint_errors
async def resolve_alert(alert_id: int):
    """Marca una alerta como resuelta manualmente."""
    pool = await get_pool()

    result = await pool.execute(
        """
        UPDATE worker_alerts
        SET resolved_at = NOW()
        WHERE id = $1 AND resolved_at IS NULL
        """,
        alert_id,
    )

    count = int(result.split()[-1])
    if count == 0:
        raise HTTPException(status_code=404, detail="Alerta no encontrada o ya resuelta")

    logger.info("Alert manually resolved: id=%d", alert_id)
    return {"message": "Alerta resuelta", "id": alert_id}


@router.post("/health/check")
@log_endpoint_errors
async def force_health_check():
    """
    Fuerza una corrida completa de los 8 checks.
    Crea/actualiza filas en worker_alerts.
    Devuelve mismo formato que GET /health.
    """
    pool = await get_pool()
    checks = await run_all_checks(pool)
    active_alerts_count = await _process_alerts(pool, checks)

    clean_checks = {
        name: {k: v for k, v in result.items() if k != "alert"}
        for name, result in checks.items()
    }

    return {
        "overall_status": _compute_overall_status(checks),
        "checks": clean_checks,
        "active_alerts_count": active_alerts_count,
        "last_check_at": datetime.now(timezone.utc).isoformat(),
    }
