from __future__ import annotations

import logging
import time
from typing import Dict, Optional

import requests

from app.sku_utils import normalize_sku

logger = logging.getLogger(__name__)

QUERY_PRODUCTS_MIN = """
query ($pageSize: Int!, $currentPage: Int!) {
    products(pageSize: $pageSize, currentPage: $currentPage) {
        total_count
        page_info {
            current_page
            total_pages
        }
        items {
            sku
            is_salable
            price_range {
                minimum_price {
                    regular_price {
                        value
                        currency
                    }
                }
            }
        }
    }
}
"""


def gql(graphql_url: str, query: str, variables: dict, timeout_s: float) -> dict:
    response = requests.post(graphql_url, json={"query": query, "variables": variables}, timeout=timeout_s)
    response.raise_for_status()
    data = response.json()
    if data.get("errors"):
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data


def _parse_price(node: Optional[dict]) -> Optional[float]:
    if not node:
        return None
    value = node.get("value")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_ddvc_full(
    graphql_url: str,
    page_size: int,
    sleep_seconds: float,
    timeout_s: float,
) -> Dict[str, Dict[str, Optional[float]]]:
    start_time = time.monotonic()
    current_page = 1
    total_pages = 1
    total_count = 0
    results: Dict[str, Dict[str, Optional[float]]] = {}

    while current_page <= total_pages:
        payload = gql(
            graphql_url,
            QUERY_PRODUCTS_MIN,
            {"pageSize": page_size, "currentPage": current_page},
            timeout_s,
        )
        products = payload.get("data", {}).get("products", {})
        total_count = products.get("total_count") or total_count
        page_info = products.get("page_info") or {}
        total_pages = page_info.get("total_pages") or total_pages
        items = products.get("items") or []

        for item in items:
            if not item:
                continue
            sku = normalize_sku(item.get("sku"))
            if not sku:
                continue
            price_node = (
                item.get("price_range", {})
                .get("minimum_price", {})
                .get("regular_price")
            )
            results[sku] = {
                "is_salable": item.get("is_salable"),
                "regular_price": _parse_price(price_node),
            }

        if current_page >= total_pages:
            break
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        current_page += 1

    elapsed = time.monotonic() - start_time
    logger.info(
        "DDVC full snapshot total_count=%s pages=%s rows_collected=%s elapsed=%.2fs",
        total_count,
        total_pages,
        len(results),
        elapsed,
    )
    return results
