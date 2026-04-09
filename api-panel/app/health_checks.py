"""
Health check functions for the worker system.

Each check function receives an asyncpg.Pool and returns a dict:
    {
        "status": "ok" | "warning" | "error",
        "message": str,
        "details": dict,
        "alert": {                     # Optional, only if an alert should be created
            "severity": "info" | "warning" | "critical",
            "title": str,
            "message": str,
            "metadata": dict | None,
        } | None,
    }
"""

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import asyncpg

logger = logging.getLogger(__name__)

MEXICO_TZ = ZoneInfo("America/Mexico_City")


def _now_mexico() -> datetime:
    return datetime.now(MEXICO_TZ)


def _in_work_window(dt: datetime | None = None) -> bool:
    """Check if the given time (or now) is within the 7:00-19:00 Mexico window."""
    t = (dt or _now_mexico()).timetz()
    from datetime import time as _time

    return _time(7, 0) <= t <= _time(19, 0)


# ---------------------------------------------------------------------------
# Check 1: worker_running
# ---------------------------------------------------------------------------
async def check_worker_running(pool: asyncpg.Pool) -> dict:
    row = await pool.fetchrow(
        "SELECT MAX(started_at) AS last_started FROM sync_runs WHERE started_at IS NOT NULL"
    )
    last_started = row["last_started"] if row else None
    now_utc = datetime.now(timezone.utc)

    if last_started is None:
        return {
            "status": "error",
            "message": "No hay registros de sync_runs",
            "details": {"last_started": None},
            "alert": {
                "severity": "critical",
                "title": "Worker nunca ha corrido",
                "message": "No se encontraron registros en sync_runs.",
                "metadata": None,
            },
        }

    # Make timezone-aware if naive
    if last_started.tzinfo is None:
        last_started = last_started.replace(tzinfo=timezone.utc)

    hours_ago = (now_utc - last_started).total_seconds() / 3600
    in_window = _in_work_window()

    details = {
        "last_started": last_started.isoformat(),
        "hours_ago": round(hours_ago, 2),
        "in_work_window": in_window,
    }

    if in_window and hours_ago > 3:
        return {
            "status": "error",
            "message": f"Worker no ha corrido en {hours_ago:.1f}h (ventana laboral activa)",
            "details": details,
            "alert": {
                "severity": "critical",
                "title": "Worker detenido por mas de 3 horas",
                "message": f"Ultimo sync hace {hours_ago:.1f} horas durante ventana laboral.",
                "metadata": details,
            },
        }

    if in_window and hours_ago > 1:
        return {
            "status": "warning",
            "message": f"Worker no ha corrido en {hours_ago:.1f}h",
            "details": details,
            "alert": {
                "severity": "warning",
                "title": "Worker sin correr por mas de 1 hora",
                "message": f"Ultimo sync hace {hours_ago:.1f} horas durante ventana laboral.",
                "metadata": details,
            },
        }

    return {"status": "ok", "message": "Worker activo", "details": details, "alert": None}


# ---------------------------------------------------------------------------
# Check 2: rules_loading
# ---------------------------------------------------------------------------
async def check_rules_loading(pool: asyncpg.Pool) -> dict:
    # Check if rounding is enabled
    rounding_row = await pool.fetchrow(
        "SELECT value FROM panel_settings WHERE key = 'rounding_enabled'"
    )
    rounding_enabled = False
    if rounding_row:
        val = rounding_row["value"]
        rounding_enabled = val is True or val == "true" or str(val).lower() == "true"

    if not rounding_enabled:
        return {
            "status": "ok",
            "message": "Redondeo deshabilitado, check no aplica",
            "details": {"rounding_enabled": False},
            "alert": None,
        }

    # Get recent price_update actions
    rows = await pool.fetch(
        """
        SELECT sa.new_value
        FROM sync_actions sa
        WHERE sa.action_type = 'price'
          AND sa.status = 'success'
          AND sa.created_at > NOW() - INTERVAL '24 hours'
        ORDER BY sa.created_at DESC
        LIMIT 20
        """
    )

    if not rows:
        return {
            "status": "ok",
            "message": "Sin cambios de precio recientes para verificar redondeo",
            "details": {"recent_price_updates": 0},
            "alert": None,
        }

    # Check if prices end in .99 (rounding applied)
    total = len(rows)
    not_rounded = 0
    for r in rows:
        try:
            price = float(r["new_value"])
            cents = round(price % 1, 2)
            if cents != 0.99:
                not_rounded += 1
        except (ValueError, TypeError):
            continue

    ratio = not_rounded / total if total > 0 else 0
    details = {
        "rounding_enabled": True,
        "recent_prices_checked": total,
        "not_rounded_count": not_rounded,
        "not_rounded_ratio": round(ratio, 2),
    }

    if ratio > 0.5 and total >= 5:
        return {
            "status": "error",
            "message": f"{not_rounded}/{total} precios recientes no tienen redondeo .99 aplicado",
            "details": details,
            "alert": {
                "severity": "critical",
                "title": "Redondeo no se esta aplicando",
                "message": f"{not_rounded} de {total} precios recientes no terminan en .99 pese a tener redondeo habilitado.",
                "metadata": details,
            },
        }

    return {
        "status": "ok",
        "message": "Redondeo aplicandose correctamente",
        "details": details,
        "alert": None,
    }


# ---------------------------------------------------------------------------
# Check 3: price_changes_applying
# ---------------------------------------------------------------------------
async def check_price_changes_applying(pool: asyncpg.Pool) -> dict:
    # SKUs with potential price discrepancy
    discrepancy_count = await pool.fetchval(
        """
        SELECT COUNT(*) FROM sku_state s
        JOIN shopify_variants v ON s.sku = v.sku
        WHERE s.ddvc_price IS NOT NULL
          AND s.target_qty > 0
        """
    )

    # Price updates in last 24h
    price_updates = await pool.fetchval(
        """
        SELECT COUNT(*) FROM sync_actions
        WHERE action_type = 'price'
          AND created_at > NOW() - INTERVAL '24 hours'
        """
    )

    details = {
        "skus_with_prices": discrepancy_count or 0,
        "price_updates_24h": price_updates or 0,
    }

    if (discrepancy_count or 0) > 100 and (price_updates or 0) < 5:
        return {
            "status": "error",
            "message": f"{discrepancy_count} SKUs con precios activos pero solo {price_updates} actualizaciones en 24h",
            "details": details,
            "alert": {
                "severity": "critical",
                "title": "Worker no esta aplicando cambios de precio",
                "message": f"Hay {discrepancy_count} SKUs con precios de DDVC pero solo {price_updates} price_updates en las ultimas 24 horas.",
                "metadata": details,
            },
        }

    return {
        "status": "ok",
        "message": f"{price_updates} actualizaciones de precio en 24h",
        "details": details,
        "alert": None,
    }


# ---------------------------------------------------------------------------
# Check 4: last_sync_status
# ---------------------------------------------------------------------------
async def check_last_sync_status(pool: asyncpg.Pool) -> dict:
    row = await pool.fetchrow(
        "SELECT run_id, started_at, finished_at, error FROM sync_runs ORDER BY started_at DESC LIMIT 1"
    )

    if not row:
        return {
            "status": "warning",
            "message": "No hay registros de sync",
            "details": {},
            "alert": None,
        }

    details = {
        "run_id": row["run_id"],
        "started_at": row["started_at"].isoformat() if row["started_at"] else None,
        "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
        "error": row["error"],
    }

    if row["error"]:
        return {
            "status": "error",
            "message": f"Ultimo sync fallo: {row['error'][:200]}",
            "details": details,
            "alert": {
                "severity": "critical",
                "title": "Ultimo sync termino con error",
                "message": f"Run {row['run_id']}: {row['error'][:500]}",
                "metadata": details,
            },
        }

    return {
        "status": "ok",
        "message": "Ultimo sync completado exitosamente",
        "details": details,
        "alert": None,
    }


# ---------------------------------------------------------------------------
# Check 5: suspicious_price_changes
# ---------------------------------------------------------------------------
async def check_suspicious_price_changes(pool: asyncpg.Pool) -> dict:
    rows = await pool.fetch(
        """
        SELECT sku, ddvc_price, price_before, price_after, created_at
        FROM price_change_log
        WHERE created_at > NOW() - INTERVAL '24 hours'
          AND price_before > 0
          AND ABS(price_after - price_before) / price_before > 0.50
        ORDER BY ABS(price_after - price_before) / price_before DESC
        LIMIT 10
        """
    )

    total_suspicious = await pool.fetchval(
        """
        SELECT COUNT(*) FROM price_change_log
        WHERE created_at > NOW() - INTERVAL '24 hours'
          AND price_before > 0
          AND ABS(price_after - price_before) / price_before > 0.50
        """
    )

    samples = [
        {
            "sku": r["sku"],
            "ddvc_price": float(r["ddvc_price"]) if r["ddvc_price"] else None,
            "price_before": float(r["price_before"]),
            "price_after": float(r["price_after"]),
            "change_pct": round(
                abs(float(r["price_after"]) - float(r["price_before"]))
                / float(r["price_before"])
                * 100,
                1,
            ),
            "created_at": r["created_at"].isoformat(),
        }
        for r in rows
    ]

    details = {"count": total_suspicious or 0, "samples": samples}

    if (total_suspicious or 0) > 0:
        return {
            "status": "warning",
            "message": f"{total_suspicious} cambios de precio >50% en ultimas 24h",
            "details": details,
            "alert": {
                "severity": "warning",
                "title": "Cambios de precio sospechosos detectados",
                "message": f"{total_suspicious} SKUs con cambios de precio mayores al 50% en las ultimas 24 horas.",
                "metadata": details,
            },
        }

    return {
        "status": "ok",
        "message": "Sin cambios de precio sospechosos",
        "details": details,
        "alert": None,
    }


# ---------------------------------------------------------------------------
# Check 6: selling_at_loss
# ---------------------------------------------------------------------------
async def check_selling_at_loss(pool: asyncpg.Pool) -> dict:
    # Use last reported price from sync_actions as the "current Shopify price"
    rows = await pool.fetch(
        """
        SELECT DISTINCT ON (sa.sku_norm)
            sa.sku_norm AS sku,
            sa.new_value AS shopify_price,
            s.ddvc_price
        FROM sync_actions sa
        JOIN sku_state s ON sa.sku_norm = s.sku
        WHERE sa.action_type = 'price'
          AND sa.status = 'success'
          AND s.ddvc_price IS NOT NULL
          AND s.ddvc_price > 0
        ORDER BY sa.sku_norm, sa.created_at DESC
        """
    )

    at_loss = []
    for r in rows:
        try:
            shopify_price = float(r["shopify_price"])
            ddvc_price = float(r["ddvc_price"])
            if shopify_price < ddvc_price:
                at_loss.append({
                    "sku": r["sku"],
                    "shopify_price": shopify_price,
                    "ddvc_price": ddvc_price,
                    "loss": round(ddvc_price - shopify_price, 2),
                })
        except (ValueError, TypeError):
            continue

    details = {
        "count": len(at_loss),
        "samples": at_loss[:10],
    }

    if len(at_loss) > 0:
        return {
            "status": "error",
            "message": f"{len(at_loss)} SKUs se estan vendiendo por debajo del costo DDVC",
            "details": details,
            "alert": {
                "severity": "critical",
                "title": "SKUs vendiendo a perdida",
                "message": f"{len(at_loss)} SKUs tienen precio de Shopify menor al precio DDVC.",
                "metadata": details,
            },
        }

    return {
        "status": "ok",
        "message": "Ningun SKU vendiendo a perdida",
        "details": details,
        "alert": None,
    }


# ---------------------------------------------------------------------------
# Check 7: ddvc_product_count
# ---------------------------------------------------------------------------
async def check_ddvc_product_count(pool: asyncpg.Pool) -> dict:
    current_count = await pool.fetchval(
        "SELECT COUNT(DISTINCT sku) FROM sku_state WHERE ddvc_price IS NOT NULL"
    )

    # Get historical average from the last 7 days of sync_runs
    avg_row = await pool.fetchrow(
        """
        SELECT AVG(ddvc_rows) AS avg_ddvc_rows
        FROM sync_runs
        WHERE started_at > NOW() - INTERVAL '7 days'
          AND ddvc_rows IS NOT NULL
          AND ddvc_rows > 0
        """
    )
    avg_count = float(avg_row["avg_ddvc_rows"]) if avg_row and avg_row["avg_ddvc_rows"] else None

    details = {
        "current_sku_count": current_count or 0,
        "avg_7d_ddvc_rows": round(avg_count, 1) if avg_count else None,
    }

    if avg_count and current_count and current_count < avg_count * 0.7:
        return {
            "status": "warning",
            "message": f"SKUs actuales ({current_count}) son 30%+ menos que el promedio 7d ({avg_count:.0f})",
            "details": details,
            "alert": {
                "severity": "warning",
                "title": "Caida en cantidad de productos DDVC",
                "message": f"Actualmente hay {current_count} SKUs vs promedio de {avg_count:.0f} en los ultimos 7 dias.",
                "metadata": details,
            },
        }

    return {
        "status": "ok",
        "message": f"{current_count} SKUs activos en DDVC",
        "details": details,
        "alert": None,
    }


# ---------------------------------------------------------------------------
# Check 8: shopify_errors
# ---------------------------------------------------------------------------
async def check_shopify_errors(pool: asyncpg.Pool) -> dict:
    error_count = await pool.fetchval(
        """
        SELECT COUNT(*) FROM sync_actions
        WHERE created_at > NOW() - INTERVAL '24 hours'
          AND status = 'error'
        """
    )

    details = {"error_count_24h": error_count or 0}

    if (error_count or 0) > 50:
        return {
            "status": "error",
            "message": f"{error_count} errores de Shopify en ultimas 24h (critico)",
            "details": details,
            "alert": {
                "severity": "critical",
                "title": "Demasiados errores de Shopify",
                "message": f"{error_count} acciones con error en las ultimas 24 horas.",
                "metadata": details,
            },
        }

    if (error_count or 0) > 10:
        return {
            "status": "warning",
            "message": f"{error_count} errores de Shopify en ultimas 24h",
            "details": details,
            "alert": {
                "severity": "warning",
                "title": "Errores elevados de Shopify",
                "message": f"{error_count} acciones con error en las ultimas 24 horas.",
                "metadata": details,
            },
        }

    return {
        "status": "ok",
        "message": f"{error_count or 0} errores en 24h",
        "details": details,
        "alert": None,
    }


# ---------------------------------------------------------------------------
# Run all checks
# ---------------------------------------------------------------------------

ALL_CHECKS = {
    "worker_running": check_worker_running,
    "rules_loading": check_rules_loading,
    "price_changes_applying": check_price_changes_applying,
    "last_sync_status": check_last_sync_status,
    "suspicious_price_changes": check_suspicious_price_changes,
    "selling_at_loss": check_selling_at_loss,
    "ddvc_product_count": check_ddvc_product_count,
    "shopify_errors": check_shopify_errors,
}


async def run_all_checks(pool: asyncpg.Pool) -> dict[str, dict]:
    """Run all health checks and return results keyed by check name."""
    results = {}
    for name, check_fn in ALL_CHECKS.items():
        try:
            results[name] = await check_fn(pool)
        except Exception as e:
            logger.error("Health check %s failed: %s", name, str(e))
            results[name] = {
                "status": "error",
                "message": f"Check fallo con excepcion: {type(e).__name__}: {str(e)}",
                "details": {},
                "alert": None,
            }
    return results
