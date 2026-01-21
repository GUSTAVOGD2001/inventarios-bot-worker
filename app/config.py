from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import time


@dataclass(frozen=True)
class Settings:
    shopify_shop: str
    shopify_client_id: str
    shopify_client_secret: str
    database_url: str
    shopify_api_version: str = "2026-01"
    ddvc_graphql: str = "https://tiendaddvc.mx/graphql"
    chunk_size: int = 150
    concurrency: int = 3
    in_stock_qty: int = 99
    out_of_stock_qty: int = 0
    not_found_qty: int = 0
    dry_run: bool = False
    tz: str = "America/Mexico_City"
    run_window_start: time = time(9, 0)
    run_window_end: time = time(18, 0)
    run_interval_min: int = 15


def _get_env_int(key: str, default: int) -> int:
    value = os.getenv(key, str(default))
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid int for {key}: {value}") from exc


def _get_env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _get_env_time(key: str, default: time) -> time:
    value = os.getenv(key)
    if value is None:
        return default
    try:
        hour_str, minute_str = value.strip().split(":", 1)
        hour = int(hour_str)
        minute = int(minute_str)
        return time(hour=hour, minute=minute)
    except ValueError as exc:
        raise ValueError(f"Invalid time for {key}: {value}. Expected HH:MM format.") from exc


def load_settings() -> Settings:
    missing = [
        key
        for key in [
            "SHOPIFY_SHOP",
            "SHOPIFY_CLIENT_ID",
            "SHOPIFY_CLIENT_SECRET",
            "DATABASE_URL",
        ]
        if not os.getenv(key)
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return Settings(
        shopify_shop=os.environ["SHOPIFY_SHOP"],
        shopify_client_id=os.environ["SHOPIFY_CLIENT_ID"],
        shopify_client_secret=os.environ["SHOPIFY_CLIENT_SECRET"],
        database_url=os.environ["DATABASE_URL"],
        shopify_api_version=os.getenv("SHOPIFY_API_VERSION", "2026-01"),
        ddvc_graphql=os.getenv("DDVC_GRAPHQL", "https://tiendaddvc.mx/graphql"),
        chunk_size=_get_env_int("CHUNK_SIZE", 150),
        concurrency=_get_env_int("CONCURRENCY", 3),
        in_stock_qty=_get_env_int("IN_STOCK_QTY", 99),
        out_of_stock_qty=_get_env_int("OUT_OF_STOCK_QTY", 0),
        not_found_qty=_get_env_int("NOT_FOUND_QTY", 0),
        dry_run=_get_env_bool("DRY_RUN", False),
        tz=os.getenv("TZ", "America/Mexico_City"),
        run_window_start=_get_env_time("RUN_WINDOW_START", time(9, 0)),
        run_window_end=_get_env_time("RUN_WINDOW_END", time(18, 0)),
        run_interval_min=_get_env_int("RUN_INTERVAL_MIN", 15),
    )
