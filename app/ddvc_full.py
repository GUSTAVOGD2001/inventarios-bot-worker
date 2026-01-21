from __future__ import annotations

import logging
import time
from typing import Dict, Optional

import requests

from app.sku_utils import normalize_sku

logger = logging.getLogger(__name__)

QUERY_PRODUCTS_MIN = """
query GetAllProducts($pageSize: Int!, $currentPage: Int!) {
  products(
    filter: {}
    pageSize: $pageSize
    currentPage: $currentPage
  ) {
    items {
      sku
      is_salable
      price_range {
        minimum_price {
          regular_price { value currency }
          final_price { value currency }
        }
      }
    }
    page_info { current_page total_pages }
    total_count
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
    ok_pages = 0
    fail_pages = 0
    results: Dict[str, Dict[str, Optional[float]]] = {}

    logger.info("DDVC full fetch starting page_size=%s", page_size)

    while current_page <= total_pages:
        try:
            payload = gql(
                graphql_url,
                QUERY_PRODUCTS_MIN,
                {"pageSize": page_size, "currentPage": current_page},
                timeout_s,
            )
        except Exception as exc:
            fail_pages += 1
            logger.warning("DDVC full fetch failed page=%s error=%s", current_page, exc)
            current_page += 1
            continue

        ok_pages += 1
        products = payload.get("data", {}).get("products", {})
        total_count = products.get("total_count") or total_count
        page_info = products.get("page_info") or {}
        total_pages = page_info.get("total_pages") or total_pages
        items = products.get("items") or []

        if current_page == 1:
            logger.info("DDVC full fetch first page total_count=%s total_pages=%s", total_count, total_pages)

        for item in items:
            if not item:
                continue
            sku = normalize_sku(item.get("sku"))
            if sku:
                sku = sku.strip()
            if not sku:
                continue
            min_price = item.get("price_range", {}).get("minimum_price", {})
            results[sku] = {
                "is_salable": item.get("is_salable"),
                "regular_price": _parse_price(min_price.get("regular_price")),
                "final_price": _parse_price(min_price.get("final_price")),
            }

        if current_page >= total_pages:
            break
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        current_page += 1

    elapsed = time.monotonic() - start_time
    logger.info(
        "DDVC full fetch done rows=%s ok_pages=%s fail_pages=%s elapsed=%.2fs",
        len(results),
        ok_pages,
        fail_pages,
        elapsed,
    )
    return results
