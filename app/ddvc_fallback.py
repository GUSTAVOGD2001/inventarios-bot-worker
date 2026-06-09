"""
app/ddvc_fallback.py

Lee el snapshot DDVC más reciente de la tabla ddvc_snapshot.
Solo se usa como fallback cuando fetch_ddvc_full lanza DDVCFetchIntegrityError.
Si el snapshot tiene más de 24h o no existe, retorna None y el worker falla
igual que antes (sin cambio de comportamiento en ese caso).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

MAX_SNAPSHOT_AGE_HOURS = 24


def load_ddvc_snapshot_fallback(engine: Engine) -> Optional[Dict[str, dict]]:
    """
    Retorna el snapshot DDVC más reciente si tiene < 24h de antigüedad.
    Retorna None si no hay snapshot válido (worker falla igual que antes).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=MAX_SNAPSHOT_AGE_HOURS)

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT id, uploaded_at, sku_count, payload
                    FROM ddvc_snapshot
                    WHERE uploaded_at >= :cutoff
                    ORDER BY uploaded_at DESC
                    LIMIT 1
                """),
                {"cutoff": cutoff},
            ).fetchone()
    except Exception as exc:
        logger.warning("Could not query ddvc_snapshot (tabla puede no existir aún): %s", exc)
        return None

    if row is None:
        logger.warning(
            "No hay snapshot DDVC válido en las últimas %sh. "
            "Corre worker_manual_con_snapshot.py para generar uno.",
            MAX_SNAPSHOT_AGE_HOURS,
        )
        return None

    payload = row[3]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            logger.error("ddvc_snapshot id=%s: JSON inválido: %s", row[0], exc)
            return None

    if not isinstance(payload, dict):
        logger.error("ddvc_snapshot id=%s: payload no es dict", row[0])
        return None

    age_hours = (
        datetime.now(timezone.utc) - row[1].replace(tzinfo=timezone.utc)
    ).total_seconds() / 3600

    logger.warning(
        "⚠️  DDVC FALLBACK ACTIVO — usando snapshot guardado: "
        "id=%s  subido_hace=%.1fh  skus=%s",
        row[0], age_hours, row[2],
    )
    return payload
