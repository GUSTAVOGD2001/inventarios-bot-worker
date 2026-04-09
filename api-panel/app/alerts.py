import json
import logging

import asyncpg

logger = logging.getLogger(__name__)


async def create_or_update_alert(
    pool: asyncpg.Pool,
    alert_type: str,
    severity: str,
    title: str,
    message: str,
    metadata: dict | None = None,
    dedupe_key: str | None = None,
) -> dict:
    """
    Crea o actualiza una alerta usando dedupe_key para evitar duplicados.

    Si ya existe una alerta activa con ese dedupe_key, actualiza metadata,
    message y detected_at. Si existe pero está resuelta, la reabre.
    """
    meta_json = json.dumps(metadata) if metadata else None

    row = await pool.fetchrow(
        """
        INSERT INTO worker_alerts (alert_type, severity, title, message, metadata, dedupe_key)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        ON CONFLICT (dedupe_key) DO UPDATE SET
            severity = EXCLUDED.severity,
            title = EXCLUDED.title,
            message = EXCLUDED.message,
            metadata = EXCLUDED.metadata,
            detected_at = NOW(),
            resolved_at = NULL
        RETURNING *
        """,
        alert_type,
        severity,
        title,
        message,
        meta_json,
        dedupe_key,
    )

    alert = dict(row)
    logger.info(
        "Alert created/updated: type=%s severity=%s dedupe=%s",
        alert_type,
        severity,
        dedupe_key,
    )
    return alert


async def auto_resolve_alerts(pool: asyncpg.Pool, alert_type: str) -> int:
    """
    Marca como resueltas todas las alertas activas de un tipo dado.
    Retorna la cantidad de alertas resueltas.
    """
    result = await pool.execute(
        """
        UPDATE worker_alerts
        SET resolved_at = NOW()
        WHERE alert_type = $1 AND resolved_at IS NULL
        """,
        alert_type,
    )
    # asyncpg returns "UPDATE N"
    count = int(result.split()[-1])
    if count > 0:
        logger.info("Alert auto-resolved: type=%s count=%d", alert_type, count)
    return count
