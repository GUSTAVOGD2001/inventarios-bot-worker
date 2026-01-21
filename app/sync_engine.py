from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import Dict, List

from sqlalchemy.engine import Engine

from app import db
from app.config import Settings
from app.ddvc_client import DdvcItem, fetch_ddvc_chunks
from app.shopify_client import ShopifyClient

logger = logging.getLogger(__name__)


def _chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _needs_variant_refresh(last_refresh: str | None) -> bool:
    if not last_refresh:
        return True
    try:
        last = dt.datetime.fromisoformat(last_refresh)
    except ValueError:
        return True
    return dt.datetime.now(dt.timezone.utc) - last > dt.timedelta(days=1)


def run_sync_once(settings: Settings, engine: Engine, shopify: ShopifyClient) -> None:
    start = dt.datetime.now(dt.timezone.utc)
    logger.info("Starting sync run")

    location_id = db.get_kv(engine, "location_id")
    if not location_id:
        location_id = shopify.get_location_id()
        db.set_kv(engine, "location_id", location_id)
        logger.info("Stored Shopify location id")

    last_refresh = db.get_kv(engine, "variants_refreshed_at")
    variant_map = db.load_variant_map(engine)
    if not variant_map or _needs_variant_refresh(last_refresh):
        logger.info("Refreshing Shopify variant map")
        variants = shopify.fetch_variant_map()
        db.upsert_variant_map(
            engine, [(variant.sku, db.VariantInfo(variant.variant_id, variant.inventory_item_id)) for variant in variants]
        )
        db.set_kv(engine, "variants_refreshed_at", dt.datetime.now(dt.timezone.utc).isoformat())
        variant_map = db.load_variant_map(engine)
        logger.info("Variant map refreshed: %s items", len(variant_map))

    skus = list(variant_map.keys())
    if not skus:
        logger.warning("No SKUs in variant map; skipping sync")
        return

    logger.info("Fetching DDVC data for %s SKUs", len(skus))
    chunks = _chunk_list(skus, settings.chunk_size)
    ok_count, total_chunks, ddvc_results, failed_chunks = asyncio.run(
        fetch_ddvc_chunks(settings.ddvc_graphql, skus, settings.chunk_size, settings.concurrency)
    )
    success_rate = ok_count / total_chunks if total_chunks else 0
    logger.info(
        "DDVC chunks ok=%s fail=%s success_rate=%.2f",
        ok_count,
        total_chunks - ok_count,
        success_rate,
    )

    states = db.load_sku_states(engine)
    inventory_updates: List[tuple[str, int]] = []
    price_updates: List[tuple[str, float]] = []
    not_found_count = 0
    skipped_count = 0
    updated_inventory = 0
    updated_price = 0

    now = dt.datetime.now(dt.timezone.utc)
    apply_not_found = success_rate >= 0.95

    failed_sku_set = {sku for idx, chunk in failed_chunks.items() for sku in chunk}

    for sku in skus:
        if sku in ddvc_results:
            item: DdvcItem = ddvc_results[sku]
            target_qty = settings.in_stock_qty if item.is_salable else settings.out_of_stock_qty
            desired_price = item.final_price
            state = states.get(sku)
            if state is None or state.target_qty != target_qty:
                info = variant_map.get(sku)
                if info:
                    inventory_updates.append((info.inventory_item_id, int(target_qty)))
                    updated_inventory += 1
            if state is None or state.ddvc_price != desired_price:
                info = variant_map.get(sku)
                if info:
                    price_updates.append((info.variant_id, desired_price))
                    updated_price += 1
            db.upsert_sku_state(
                engine,
                sku=sku,
                ddvc_salable=item.is_salable,
                ddvc_price=desired_price,
                target_qty=target_qty,
                last_seen_ddvc_at=now,
                last_sync_status="ok",
            )
        else:
            if sku in failed_sku_set and not apply_not_found:
                skipped_count += 1
                continue
            not_found_count += 1
            target_qty = settings.not_found_qty
            state = states.get(sku)
            if state is None or state.target_qty != target_qty:
                info = variant_map.get(sku)
                if info:
                    inventory_updates.append((info.inventory_item_id, int(target_qty)))
                    updated_inventory += 1
            db.upsert_sku_state(
                engine,
                sku=sku,
                ddvc_salable=None,
                ddvc_price=None,
                target_qty=target_qty,
                last_seen_ddvc_at=now,
                last_sync_status="not_found",
            )

    logger.info(
        "Planned updates inventory=%s price=%s not_found=%s skipped=%s",
        updated_inventory,
        updated_price,
        not_found_count,
        skipped_count,
    )

    if settings.dry_run:
        logger.info("DRY_RUN enabled. Skipping Shopify updates.")
    else:
        if inventory_updates:
            shopify.update_inventory(location_id, inventory_updates)
        if price_updates:
            shopify.update_prices(price_updates)

    duration = (dt.datetime.now(dt.timezone.utc) - start).total_seconds()
    logger.info("Sync completed in %.2fs", duration)
