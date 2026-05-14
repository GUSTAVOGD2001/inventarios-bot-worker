from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Query

from ..auth import require_api_key
from ..db import get_pool
from ..error_handler import log_endpoint_errors

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/analytics/top-stockouts")
@log_endpoint_errors
async def top_stockouts(
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(10, ge=1, le=50),
):
    pool = await get_pool()
    since = datetime.utcnow() - timedelta(days=days)

    rows = await pool.fetch(
        """
        WITH stockouts AS (
            SELECT
                sa.sku_norm AS sku,
                COUNT(*) AS stockout_count,
                MAX(sa.created_at) AS last_stockout
            FROM sync_actions sa
            WHERE sa.action_type = 'inventory'
              AND sa.new_value = '0'
              AND sa.old_value != '0'
              AND sa.status = 'applied'
              AND sa.created_at >= $1
            GROUP BY sa.sku_norm
            HAVING COUNT(*) >= 2
        ),
        restocks AS (
            SELECT
                sa.sku_norm AS sku,
                COUNT(*) AS restock_count,
                MAX(sa.created_at) AS last_restock
            FROM sync_actions sa
            WHERE sa.action_type = 'inventory'
              AND sa.old_value = '0'
              AND sa.new_value != '0'
              AND sa.status = 'applied'
              AND sa.created_at >= $1
            GROUP BY sa.sku_norm
        )
        SELECT
            s.sku,
            s.stockout_count,
            COALESCE(r.restock_count, 0) AS restock_count,
            LEAST(s.stockout_count, COALESCE(r.restock_count, 0)) AS cycle_count,
            s.last_stockout,
            r.last_restock,
            COALESCE(sv.title, 'Sin título') AS title,
            ss.ddvc_salable AS current_salable,
            ss.ddvc_price,
            ss.target_qty
        FROM stockouts s
        LEFT JOIN restocks r ON r.sku = s.sku
        LEFT JOIN shopify_variants sv ON sv.sku = s.sku
        LEFT JOIN sku_state ss ON ss.sku = s.sku
        ORDER BY s.stockout_count DESC, COALESCE(r.restock_count, 0) DESC
        LIMIT $2
        """,
        since,
        limit,
    )

    items = []
    for r in rows:
        current_status = "in_stock" if r["target_qty"] and float(r["target_qty"]) > 0 else "out_of_stock"
        items.append({
            "sku": r["sku"],
            "title": r["title"],
            "stockout_count": r["stockout_count"],
            "restock_count": r["restock_count"],
            "cycle_count": r["cycle_count"],
            "last_stockout": r["last_stockout"].isoformat() if r["last_stockout"] else None,
            "last_restock": r["last_restock"].isoformat() if r["last_restock"] else None,
            "ddvc_price": float(r["ddvc_price"]) if r["ddvc_price"] else None,
            "current_status": current_status,
        })

    return {
        "period_days": days,
        "since": since.isoformat(),
        "total": len(items),
        "items": items,
    }


@router.get("/analytics/stockout-timeline/{sku}")
@log_endpoint_errors
async def stockout_timeline(
    sku: str,
    days: int = Query(30, ge=1, le=365),
):
    pool = await get_pool()
    sku_norm = sku.strip().upper()
    since = datetime.utcnow() - timedelta(days=days)

    rows = await pool.fetch(
        """
        SELECT sa.id, sa.old_value, sa.new_value, sa.created_at, sa.run_id
        FROM sync_actions sa
        WHERE sa.sku_norm = $1
          AND sa.action_type = 'inventory'
          AND sa.status = 'applied'
          AND sa.created_at >= $2
        ORDER BY sa.created_at ASC
        """,
        sku_norm,
        since,
    )

    events = []
    for r in rows:
        old = r["old_value"]
        new = r["new_value"]
        if old != '0' and new == '0':
            event_type = "stockout"
        elif old == '0' and new != '0':
            event_type = "restock"
        else:
            event_type = "adjustment"
        events.append({
            "id": r["id"],
            "event_type": event_type,
            "old_value": old,
            "new_value": new,
            "created_at": r["created_at"].isoformat(),
            "run_id": r["run_id"],
        })

    stockout_durations = []
    last_stockout_time = None
    for e in events:
        if e["event_type"] == "stockout":
            last_stockout_time = datetime.fromisoformat(e["created_at"])
        elif e["event_type"] == "restock" and last_stockout_time is not None:
            restock_time = datetime.fromisoformat(e["created_at"])
            days_out = (restock_time - last_stockout_time).total_seconds() / 86400
            stockout_durations.append(round(days_out, 1))
            last_stockout_time = None

    avg_days_out = round(sum(stockout_durations) / len(stockout_durations), 1) if stockout_durations else None

    sku_info = await pool.fetchrow(
        """
        SELECT ss.ddvc_price, ss.ddvc_salable, ss.target_qty,
               COALESCE(sv.title, 'Sin título') AS title
        FROM sku_state ss
        LEFT JOIN shopify_variants sv ON sv.sku = ss.sku
        WHERE ss.sku = $1
        """,
        sku_norm,
    )

    return {
        "sku": sku_norm,
        "title": sku_info["title"] if sku_info else "Sin título",
        "ddvc_price": float(sku_info["ddvc_price"]) if sku_info and sku_info["ddvc_price"] else None,
        "period_days": days,
        "total_events": len(events),
        "stockout_count": sum(1 for e in events if e["event_type"] == "stockout"),
        "restock_count": sum(1 for e in events if e["event_type"] == "restock"),
        "avg_days_out_of_stock": avg_days_out,
        "stockout_durations": stockout_durations,
        "events": events,
    }
