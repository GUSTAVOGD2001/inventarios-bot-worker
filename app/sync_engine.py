from __future__ import annotations

import datetime as dt
import logging
from typing import Dict, List, Optional, Tuple

from sqlalchemy.engine import Engine

from app import db
from app.config import Settings
from app.ddvc_full import fetch_ddvc_full
from app.shopify_client import ShopifyClient, ShopifyVariantSnapshot
from app.sku_utils import normalize_sku

logger = logging.getLogger(__name__)


def _normalize_snapshot(snapshot: List[ShopifyVariantSnapshot]) -> Dict[str, ShopifyVariantSnapshot]:
    normalized: Dict[str, ShopifyVariantSnapshot] = {}
    for item in snapshot:
        normalized_sku = normalize_sku(item.sku)
        if not normalized_sku:
            continue
        normalized[normalized_sku] = item
    return normalized


def _stringify(value: Optional[float | int]) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def run_sync_once(settings: Settings, engine: Engine, shopify: ShopifyClient, run_id: str) -> None:
    start = dt.datetime.now(dt.timezone.utc)
    logger.info("Starting sync run")

    with engine.connect() as lock_conn:
        if not db.try_lock(lock_conn):
            logger.info("sync already running, skipping")
            return

        location_id = db.get_kv(engine, "location_id")
        if not location_id:
            location_id = shopify.get_location_id()
            db.set_kv(engine, "location_id", location_id)
            logger.info("Stored Shopify location id")

        not_found_action = settings.not_found_action
        if not_found_action not in {"skip", "out_of_stock"}:
            logger.warning("Invalid NOT_FOUND_ACTION=%s, defaulting to skip", not_found_action)
            not_found_action = "skip"

        error_message: Optional[str] = None
        found_count = 0
        not_found_count = 0
        inventory_changes = 0
        price_changes = 0
        ddvc_rows = 0
        shopify_rows = 0
        run_inserted = False

        try:
            db.insert_sync_run(engine, run_id, start, start, settings.dry_run)
            run_inserted = True
            snapshot = shopify.fetch_variant_snapshot(location_id)
            shopify_rows = len(snapshot)
            logger.info("Shopify snapshot rows=%s", shopify_rows)
            db.upsert_variant_map(
                engine,
                [(item.sku, db.VariantInfo(item.variant_id, item.inventory_item_id)) for item in snapshot],
            )

            shopify_map = _normalize_snapshot(snapshot)
            if not shopify_map:
                logger.warning("No SKUs in Shopify snapshot; skipping sync")
                return

            ddvc_map = fetch_ddvc_full(
                graphql_url=settings.ddvc_graphql,
                page_size=settings.ddvc_page_size,
                sleep_seconds=settings.ddvc_sleep_seconds,
                timeout_s=settings.ddvc_timeout,
            )
            ddvc_rows = len(ddvc_map)

            inventory_updates: List[Tuple[str, int]] = []
            price_updates: List[Tuple[str, float]] = []
            inventory_actions: List[Tuple[int, str, int]] = []
            price_actions: List[Tuple[int, str, float]] = []

            for sku_norm, shopify_item in shopify_map.items():
                ddvc_item = ddvc_map.get(sku_norm)
                if ddvc_item:
                    found_count += 1
                    is_salable = ddvc_item.get("is_salable")
                    regular_price = ddvc_item.get("regular_price")
                    qty_target: Optional[int]
                    if is_salable is True:
                        qty_target = settings.in_stock_qty
                    elif is_salable is False:
                        qty_target = settings.out_of_stock_qty
                    else:
                        qty_target = None

                    if qty_target is not None and shopify_item.quantity != qty_target:
                        action_id = db.insert_sync_action(
                            engine,
                            run_id=run_id,
                            sku_norm=sku_norm,
                            action_type="inventory",
                            old_value=_stringify(shopify_item.quantity),
                            new_value=_stringify(qty_target),
                            status="planned",
                        )
                        inventory_actions.append((action_id, shopify_item.inventory_item_id, qty_target))
                        inventory_updates.append((shopify_item.inventory_item_id, qty_target))

                    if regular_price is not None and abs(shopify_item.price - regular_price) > 0.01:
                        action_id = db.insert_sync_action(
                            engine,
                            run_id=run_id,
                            sku_norm=sku_norm,
                            action_type="price",
                            old_value=_stringify(shopify_item.price),
                            new_value=_stringify(regular_price),
                            status="planned",
                        )
                        price_actions.append((action_id, shopify_item.variant_id, regular_price))
                        price_updates.append((shopify_item.variant_id, regular_price))
                else:
                    not_found_count += 1
                    if not_found_action == "out_of_stock" and shopify_item.quantity != settings.out_of_stock_qty:
                        action_id = db.insert_sync_action(
                            engine,
                            run_id=run_id,
                            sku_norm=sku_norm,
                            action_type="inventory",
                            old_value=_stringify(shopify_item.quantity),
                            new_value=_stringify(settings.out_of_stock_qty),
                            status="planned",
                        )
                        inventory_actions.append((action_id, shopify_item.inventory_item_id, settings.out_of_stock_qty))
                        inventory_updates.append((shopify_item.inventory_item_id, settings.out_of_stock_qty))

            inventory_changes = len(inventory_actions)
            price_changes = len(price_actions)

            logger.info(
                "Compare found=%s not_found=%s inventory_changes=%s price_changes=%s",
                found_count,
                not_found_count,
                inventory_changes,
                price_changes,
            )

            if settings.dry_run:
                logger.info("DRY_RUN enabled. Skipping Shopify updates.")
            else:
                logger.info("Applying updates...")
                if inventory_updates:
                    try:
                        shopify.update_inventory(location_id, inventory_updates)
                        for action_id, _, _ in inventory_actions:
                            db.update_sync_action_status(engine, action_id, "applied")
                    except Exception as exc:
                        error = str(exc)
                        for action_id, _, _ in inventory_actions:
                            db.update_sync_action_status(engine, action_id, "failed", error)
                        raise
                if price_updates:
                    try:
                        shopify.update_prices(price_updates)
                        for action_id, _, _ in price_actions:
                            db.update_sync_action_status(engine, action_id, "applied")
                    except Exception as exc:
                        error = str(exc)
                        for action_id, _, _ in price_actions:
                            db.update_sync_action_status(engine, action_id, "failed", error)
                        raise
        except Exception as exc:
            error_message = str(exc)
            raise
        finally:
            finished_at = dt.datetime.now(dt.timezone.utc)
            if run_inserted:
                db.update_sync_run(
                    engine,
                    run_id=run_id,
                    finished_at=finished_at,
                    found_count=found_count,
                    not_found_count=not_found_count,
                    inventory_changes=inventory_changes,
                    price_changes=price_changes,
                    ddvc_rows=ddvc_rows,
                    shopify_rows=shopify_rows,
                    error=error_message,
                )
            db.release_lock(lock_conn)

    duration = (dt.datetime.now(dt.timezone.utc) - start).total_seconds()
    logger.info("Sync completed in %.2fs", duration)
