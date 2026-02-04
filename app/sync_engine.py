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

        error_message: Optional[str] = None
        found_count = 0
        not_found_count = 0
        skipped_count = 0
        inventory_changes = 0
        price_changes = 0
        ddvc_rows = 0
        shopify_rows = 0
        MAX_SAMPLES = 20
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

            ddvc_map = fetch_ddvc_full(graphql_url=settings.ddvc_graphql)
            ddvc_rows = len(ddvc_map)
            logger.info("DDVC map ready rows=%s", ddvc_rows)

            sku_states = db.load_sku_states(engine)
            inventory_updates: List[Tuple[str, int]] = []
            price_updates: List[Tuple[str, str, float]] = []
            inventory_actions: List[Tuple[int, str, str, int]] = []
            price_actions: List[Tuple[int, str, str, float]] = []
            sku_status: Dict[str, Dict[str, bool]] = {}
            desired_state: Dict[str, Dict[str, Optional[float | bool | dt.datetime]]] = {}

            for sku_norm, shopify_item in shopify_map.items():
                planned_action = False
                ddvc_item = ddvc_map.get(sku_norm)
                if ddvc_item:
                    found_count += 1
                    is_salable = ddvc_item.get("is_salable")
                    ddvc_price_raw = ddvc_item.get("final_price")
                    ddvc_price: Optional[float]
                    if ddvc_price_raw is None:
                        ddvc_price = None
                    else:
                        try:
                            ddvc_price = float(ddvc_price_raw)
                        except (TypeError, ValueError):
                            ddvc_price = None
                    qty_target = settings.in_stock_qty if is_salable is True else settings.out_of_stock_qty
                    last_seen = dt.datetime.now(dt.timezone.utc)
                else:
                    not_found_count += 1
                    is_salable = None
                    ddvc_price = None
                    qty_target = settings.out_of_stock_qty
                    last_seen = None

                desired_state[sku_norm] = {
                    "ddvc_salable": is_salable,
                    "ddvc_price": ddvc_price,
                    "target_qty": float(qty_target) if qty_target is not None else None,
                    "last_seen_ddvc_at": last_seen,
                }

                prior_state = sku_states.get(sku_norm)
                state_matches = (
                    prior_state is not None
                    and prior_state.ddvc_salable == is_salable
                    and prior_state.ddvc_price == ddvc_price
                    and prior_state.target_qty == desired_state[sku_norm]["target_qty"]
                )
                sku_status[sku_norm] = {
                    "inventory_needed": False,
                    "price_needed": False,
                    "inventory_success": True,
                    "price_success": True,
                }

                qty_needs_update = shopify_item.quantity != qty_target
                if qty_needs_update:
                    action_id = db.insert_sync_action(
                        engine,
                        run_id=run_id,
                        sku_norm=sku_norm,
                        action_type="inventory",
                        old_value=_stringify(shopify_item.quantity),
                        new_value=_stringify(qty_target),
                        status="planned",
                    )
                    inventory_actions.append((action_id, sku_norm, shopify_item.inventory_item_id, qty_target))
                    inventory_updates.append((shopify_item.inventory_item_id, qty_target))
                    sku_status[sku_norm]["inventory_needed"] = True
                    sku_status[sku_norm]["inventory_success"] = False
                    logger.info(
                        "SKU %s inventory %s -> %s",
                        sku_norm,
                        _stringify(shopify_item.quantity),
                        _stringify(qty_target),
                    )
                    planned_action = True

                if ddvc_price is not None and abs(shopify_item.price - ddvc_price) > 0.01:
                    action_id = db.insert_sync_action(
                        engine,
                        run_id=run_id,
                        sku_norm=sku_norm,
                        action_type="price",
                        old_value=_stringify(shopify_item.price),
                        new_value=_stringify(ddvc_price),
                        status="planned",
                    )
                    price_actions.append((action_id, sku_norm, shopify_item.variant_id, ddvc_price))
                    price_updates.append((shopify_item.product_id, shopify_item.variant_id, ddvc_price))
                    sku_status[sku_norm]["price_needed"] = True
                    sku_status[sku_norm]["price_success"] = False
                    logger.info(
                        "SKU %s price %s -> %s",
                        sku_norm,
                        _stringify(shopify_item.price),
                        _stringify(ddvc_price),
                    )
                    planned_action = True

                if not planned_action:
                    if state_matches and not qty_needs_update:
                        skipped_count += 1
                    else:
                        skipped_count += 1

            inventory_changes = len(inventory_actions)
            price_changes = len(price_actions)

            logger.info(
                "COMPARE SUMMARY shopify=%s ddvc=%s found=%s not_found=%s skipped=%s",
                shopify_rows,
                ddvc_rows,
                found_count,
                not_found_count,
                skipped_count,
            )
            logger.info(
                "PLANNED CHANGES inventory=%s price=%s dry_run=%s",
                inventory_changes,
                price_changes,
                settings.dry_run,
            )

            if inventory_actions:
                logger.info("SAMPLE INVENTORY CHANGES:")
                for _, _, inventory_item_id, qty in inventory_actions[:MAX_SAMPLES]:
                    logger.info(" - inventory_item_id=%s -> qty=%s", inventory_item_id, qty)

            if price_actions:
                logger.info("SAMPLE PRICE CHANGES:")
                for _, _, variant_id, price in price_actions[:MAX_SAMPLES]:
                    logger.info(" - variant_id=%s -> price=%s", variant_id, price)

            logger.info("Applying Shopify updates... dry_run=%s", settings.dry_run)
            if settings.dry_run:
                logger.info("DRY_RUN enabled. Skipping Shopify updates.")
            else:
                logger.info("Applying updates...")
                if inventory_updates:
                    try:
                        inventory_results = shopify.update_inventory(location_id, inventory_updates)
                        for action_id, sku_norm, inventory_item_id, _ in inventory_actions:
                            error = inventory_results.get(inventory_item_id)
                            if error:
                                db.update_sync_action_status(engine, action_id, "failed", error)
                                sku_status[sku_norm]["inventory_success"] = False
                            else:
                                db.update_sync_action_status(engine, action_id, "applied")
                                sku_status[sku_norm]["inventory_success"] = True
                    except Exception as exc:
                        error = str(exc)
                        for action_id, sku_norm, _, _ in inventory_actions:
                            db.update_sync_action_status(engine, action_id, "failed", error)
                            sku_status[sku_norm]["inventory_success"] = False
                        raise
                if price_updates:
                    try:
                        price_results = shopify.update_prices(price_updates)
                        for action_id, sku_norm, variant_id, _ in price_actions:
                            error = price_results.get(variant_id)
                            if error:
                                db.update_sync_action_status(engine, action_id, "failed", error)
                                sku_status[sku_norm]["price_success"] = False
                            else:
                                db.update_sync_action_status(engine, action_id, "applied")
                                sku_status[sku_norm]["price_success"] = True
                    except Exception as exc:
                        error = str(exc)
                        for action_id, sku_norm, _, _ in price_actions:
                            db.update_sync_action_status(engine, action_id, "failed", error)
                            sku_status[sku_norm]["price_success"] = False
                        raise

                for sku_norm, desired in desired_state.items():
                    status = sku_status.get(sku_norm)
                    if status and (
                        (status["inventory_needed"] and not status["inventory_success"])
                        or (status["price_needed"] and not status["price_success"])
                    ):
                        continue
                    ddvc_salable = desired["ddvc_salable"]
                    ddvc_price = desired["ddvc_price"]
                    target_qty = desired["target_qty"]
                    last_seen_ddvc_at = desired["last_seen_ddvc_at"]
                    db.upsert_sku_state(
                        engine,
                        sku=sku_norm,
                        ddvc_salable=ddvc_salable if isinstance(ddvc_salable, bool) or ddvc_salable is None else None,
                        ddvc_price=ddvc_price if isinstance(ddvc_price, (float, int)) or ddvc_price is None else None,
                        target_qty=target_qty if isinstance(target_qty, (float, int)) or target_qty is None else None,
                        last_seen_ddvc_at=last_seen_ddvc_at if isinstance(last_seen_ddvc_at, dt.datetime) else None,
                        last_sync_status="applied" if status and (status["inventory_needed"] or status["price_needed"]) else "noop",
                    )
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
