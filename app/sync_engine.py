from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app import db
from app.config import Settings
from app.ddvc_full import fetch_ddvc_full
from app.pricing import PricingEngine, load_sku_exemptions, log_price_change
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
        applied_to_zero = 0
        applied_to_in_stock = 0
        applied_price_changes = 0
        MAX_SAMPLES = 20
        run_inserted = False

        try:
            db.insert_sync_run(engine, run_id, start, start, settings.dry_run)
            run_inserted = True
            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "shopify_fetch",
                "message": "Obteniendo productos de Shopify...",
                "percent": 5,
                "started_at": start.isoformat(),
                "details": {}
            }))
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

            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "ddvc_fetch",
                "message": f"Shopify listo ({len(shopify_map)} productos). Consultando DDVC...",
                "percent": 15,
                "started_at": start.isoformat(),
                "details": {"shopify_rows": len(shopify_map)}
            }))

            ddvc_map = fetch_ddvc_full(graphql_url=settings.ddvc_graphql)
            ddvc_rows = len(ddvc_map)
            logger.info("DDVC map ready rows=%s", ddvc_rows)

            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "comparing",
                "message": f"DDVC listo ({ddvc_rows} productos). Comparando inventarios...",
                "percent": 40,
                "started_at": start.isoformat(),
                "details": {"shopify_rows": len(shopify_map), "ddvc_rows": ddvc_rows}
            }))

            # Load pricing rules from panel
            pricing_engine = PricingEngine(engine)
            pricing_engine.load_rules()

            # Load SKU exemptions from panel
            sku_exemptions = load_sku_exemptions(engine)
            if sku_exemptions:
                logger.info("Loaded %s SKU exemptions from panel", len(sku_exemptions))

            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "pricing",
                "message": "Reglas de precios cargadas. Calculando precios y comparando...",
                "percent": 45,
                "started_at": start.isoformat(),
                "details": {"shopify_rows": len(shopify_map), "ddvc_rows": ddvc_rows}
            }))

            sku_states = db.load_sku_states(engine)
            inventory_updates: List[Tuple[str, int]] = []
            price_updates: List[Tuple[str, str, float]] = []
            inventory_actions: List[Tuple[int, str, str, int]] = []
            price_actions: List[Tuple[int, str, str, float]] = []
            sku_status: Dict[str, Dict[str, bool]] = {}
            desired_state: Dict[str, Dict[str, Optional[float | bool | dt.datetime]]] = {}

            for sku_norm, shopify_item in shopify_map.items():
                planned_action = False

                # Check exemptions for this SKU
                exemption = sku_exemptions.get(sku_norm, {})
                exempt_inventory = exemption.get("exempt_inventory", False)
                exempt_price = exemption.get("exempt_price", False)
                if exempt_inventory and exempt_price:
                    skipped_count += 1
                    logger.debug("SKU %s fully exempted, skipping", sku_norm)
                    continue

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
                if qty_needs_update and exempt_inventory:
                    logger.info(
                        "SKU %s inventory exempt, would have changed %s -> %s but skipping",
                        sku_norm,
                        _stringify(shopify_item.quantity),
                        _stringify(qty_target),
                    )
                if qty_needs_update and not exempt_inventory:
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

                if ddvc_price is not None and not exempt_price:
                    price_result = pricing_engine.calculate(sku_norm, ddvc_price)
                    target_price = price_result.final_price

                    if abs(shopify_item.price - target_price) > 0.01:
                        rule_desc = " + ".join(price_result.steps[1:]) if len(price_result.steps) > 1 else "Sin regla"
                        action_id = db.insert_sync_action(
                            engine,
                            run_id=run_id,
                            sku_norm=sku_norm,
                            action_type="price",
                            old_value=_stringify(shopify_item.price),
                            new_value=_stringify(target_price),
                            status="planned",
                        )
                        price_actions.append((action_id, sku_norm, shopify_item.variant_id, target_price))
                        price_updates.append((shopify_item.product_id, shopify_item.variant_id, target_price))
                        sku_status[sku_norm]["price_needed"] = True
                        sku_status[sku_norm]["price_success"] = False
                        logger.info(
                            "SKU %s price %s -> %s (ddvc=%s, steps: %s)",
                            sku_norm,
                            _stringify(shopify_item.price),
                            _stringify(target_price),
                            _stringify(ddvc_price),
                            " | ".join(price_result.steps),
                        )
                        # Log to price_change_log for the panel
                        log_price_change(
                            engine,
                            sku=sku_norm,
                            ddvc_price=ddvc_price,
                            rule_applied=rule_desc,
                            price_before=shopify_item.price,
                            price_after=target_price,
                            was_applied=False,  # Se marca True después de aplicar
                        )
                        planned_action = True
                elif ddvc_price is not None and exempt_price:
                    logger.debug("SKU %s price exempt, keeping current price", sku_norm)

                if not planned_action:
                    if state_matches and not qty_needs_update:
                        skipped_count += 1
                    else:
                        skipped_count += 1

            inventory_changes = len(inventory_actions)
            price_changes = len(price_actions)
            planned_to_zero = sum(
                1 for _, _, _, qty_target in inventory_actions if qty_target == settings.out_of_stock_qty
            )
            planned_to_in_stock = sum(
                1 for _, _, _, qty_target in inventory_actions if qty_target == settings.in_stock_qty
            )
            planned_price = len(price_actions)

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
            logger.info(
                "PLANNED TOTALS inventory_to_0=%s inventory_to_%s=%s price_changed=%s dry_run=%s",
                planned_to_zero,
                settings.in_stock_qty,
                planned_to_in_stock,
                planned_price,
                settings.dry_run,
            )

            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "applying",
                "message": f"Aplicando {inventory_changes} cambios inventario, {price_changes} cambios precio...",
                "percent": 70,
                "started_at": start.isoformat(),
                "details": {
                    "shopify_rows": len(shopify_map),
                    "ddvc_rows": ddvc_rows,
                    "found": found_count,
                    "not_found": not_found_count,
                    "inventory_changes": inventory_changes,
                    "price_changes": price_changes
                }
            }))

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
                inventory_results: Dict[str, Optional[str]] = {}
                price_results: Dict[str, Optional[str]] = {}
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

                for _, _, inventory_item_id, qty_target in inventory_actions:
                    if inventory_results.get(inventory_item_id) is None:
                        if qty_target == settings.out_of_stock_qty:
                            applied_to_zero += 1
                        if qty_target == settings.in_stock_qty:
                            applied_to_in_stock += 1

                for _, _, variant_id, _ in price_actions:
                    if price_results.get(variant_id) is None:
                        applied_price_changes += 1

                # Mark price changes as applied in price_change_log
                for _, sku_norm, variant_id, target_price in price_actions:
                    if price_results.get(variant_id) is None:
                        try:
                            with engine.begin() as conn:
                                conn.execute(
                                    text(
                                        """UPDATE price_change_log
                                           SET was_applied = true
                                           WHERE sku = :sku AND price_after = :price_after
                                             AND was_applied = false
                                             AND created_at >= NOW() - INTERVAL '1 hour'"""
                                    ),
                                    {"sku": sku_norm, "price_after": target_price},
                                )
                        except Exception:
                            logger.warning("Failed to mark price_change_log as applied for %s", sku_norm)

                logger.info(
                    "APPLIED TOTALS inventory_to_0=%s inventory_to_%s=%s price_changed=%s",
                    applied_to_zero,
                    settings.in_stock_qty,
                    applied_to_in_stock,
                    applied_price_changes,
                )

                db.set_kv(engine, "sync_progress", json.dumps({
                    "run_id": run_id,
                    "phase": "saving",
                    "message": "Cambios aplicados en Shopify. Guardando estados en BD...",
                    "percent": 90,
                    "started_at": start.isoformat(),
                    "details": {
                        "shopify_rows": len(shopify_map),
                        "ddvc_rows": ddvc_rows,
                        "found": found_count,
                        "not_found": not_found_count,
                        "inventory_changes": inventory_changes,
                        "price_changes": price_changes,
                        "applied_to_zero": applied_to_zero,
                        "applied_to_in_stock": applied_to_in_stock,
                        "applied_price_changes": applied_price_changes
                    }
                }))

            for sku_norm, desired in desired_state.items():
                status = sku_status.get(sku_norm)
                if status and (
                    (status["inventory_needed"] and not status["inventory_success"])
                    or (status["price_needed"] and not status["price_success"])
                ):
                    continue
                # Skip sku_state update for exempted SKUs to preserve manual state
                exemption = sku_exemptions.get(sku_norm, {})
                if exemption.get("exempt_inventory") or exemption.get("exempt_price"):
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
            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "done",
                "message": "Sincronización completada.",
                "percent": 100,
                "started_at": start.isoformat(),
                "finished_at": finished_at.isoformat(),
                "details": {
                    "shopify_rows": shopify_rows,
                    "ddvc_rows": ddvc_rows,
                    "error": error_message
                }
            }))
            db.release_lock(lock_conn)

    duration = (dt.datetime.now(dt.timezone.utc) - start).total_seconds()
    logger.info("Sync completed in %.2fs", duration)
