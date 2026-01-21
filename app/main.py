from __future__ import annotations

import logging
import time
import uuid

from app.config import load_settings
from app.db import get_engine, init_db
from app.logging_setup import set_run_id, setup_logging
from app.shopify_client import ShopifyClient
from app.sync_engine import run_sync_once

logger = logging.getLogger(__name__)


def main() -> None:
    settings = load_settings()
    run_id = uuid.uuid4().hex[:8]
    run_filter = setup_logging(run_id)

    engine = get_engine(settings.database_url)
    init_db(engine)

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
            try:
                run_sync_once(settings, engine, shopify)
            except Exception:
                logger.exception("Sync run failed")
            sleep_seconds = settings.sync_interval_min * 60
            logger.info("Sleeping for %s seconds", sleep_seconds)
            time.sleep(sleep_seconds)
    finally:
        shopify.close()


if __name__ == "__main__":
    main()
