from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.config import Settings, load_settings, validate_settings
from app.db import ensure_schema, get_engine, get_kv, init_db, set_kv
from app.logging_setup import set_run_id, setup_logging
from app.shopify_client import ShopifyClient
from app.sync_engine import run_sync_once

logger = logging.getLogger(__name__)
LAST_RUN_SLOT_KEY = "last_run_slot"


def _slot_id(slot_time: datetime) -> str:
    return slot_time.strftime("%Y-%m-%d %H:%M")


def _window_start_end(now_local: datetime, settings: Settings) -> tuple[datetime, datetime]:
    start = datetime.combine(now_local.date(), settings.run_window_start, tzinfo=now_local.tzinfo)
    end = datetime.combine(now_local.date(), settings.run_window_end, tzinfo=now_local.tzinfo)
    return start, end


def should_run_now(now_local: datetime, settings: Settings) -> tuple[bool, str]:
    start, end = _window_start_end(now_local, settings)
    end_slot_end = end + timedelta(minutes=1)
    if now_local < start or now_local >= end_slot_end:
        return False, ""
    minutes_since_start = int((now_local - start).total_seconds() // 60)
    if minutes_since_start % settings.run_interval_min != 0:
        return False, ""
    slot_time = start + timedelta(minutes=minutes_since_start)
    return True, _slot_id(slot_time)


def seconds_until_next_slot(now_local: datetime, settings: Settings) -> int:
    start, end = _window_start_end(now_local, settings)
    if now_local < start:
        return int((start - now_local).total_seconds())
    if now_local > end:
        next_start = start + timedelta(days=1)
        return int((next_start - now_local).total_seconds())
    minutes_since_start = (now_local - start).total_seconds() / 60
    next_offset = int((minutes_since_start // settings.run_interval_min + 1) * settings.run_interval_min)
    next_slot = start + timedelta(minutes=next_offset)
    if next_slot > end:
        next_slot = start + timedelta(days=1)
    return int((next_slot - now_local).total_seconds())


def main() -> None:
    settings = load_settings()
    validate_settings(settings)
    run_id = uuid.uuid4().hex[:8]
    run_filter = setup_logging(run_id)
    tz = ZoneInfo(settings.tz)

    engine = get_engine(settings.database_url)
    init_db(engine)
    ensure_schema(engine)

    shopify = ShopifyClient(
        shop=settings.shopify_shop,
        client_id=settings.shopify_client_id,
        client_secret=settings.shopify_client_secret,
        api_version=settings.shopify_api_version,
    )

    try:
        while True:
            run_id = uuid.uuid4().hex[:8]
            set_run_id(run_filter, run_id)
            now_local = datetime.now(tz)

            # Check for manual sync trigger from panel API
            trigger_sync = get_kv(engine, "trigger_sync")
            manual_triggered = False
            if trigger_sync:
                import json as _json
                try:
                    trigger_data = _json.loads(trigger_sync)
                    requested_at = trigger_data.get("requested_at", "")
                    logger.info("Manual sync trigger detected, requested_at=%s", requested_at)
                    manual_triggered = True
                    # Clear the trigger
                    set_kv(engine, "trigger_sync", "")
                except (ValueError, TypeError):
                    set_kv(engine, "trigger_sync", "")

            should_run, slot_id = should_run_now(now_local, settings)
            if manual_triggered:
                logger.info("Running manual sync triggered from panel")
                try:
                    run_sync_once(settings, engine, shopify, run_id)
                except Exception:
                    logger.exception("Manual sync run failed")
            elif should_run:
                last_slot = get_kv(engine, LAST_RUN_SLOT_KEY)
                if last_slot != slot_id:
                    set_kv(engine, LAST_RUN_SLOT_KEY, slot_id)
                    logger.info("Starting sync for slot %s", slot_id)
                    try:
                        run_sync_once(settings, engine, shopify, run_id)
                    except Exception:
                        logger.exception("Sync run failed")
                else:
                    logger.info("Slot %s already executed, skipping", slot_id)
            sleep_seconds = seconds_until_next_slot(now_local, settings)
            sleep_seconds = max(5, min(sleep_seconds, 3600))
            logger.info("Sleeping for %s seconds", sleep_seconds)
            time.sleep(sleep_seconds)
    finally:
        shopify.close()


if __name__ == "__main__":
    main()
