from __future__ import annotations

import logging
import os
import time
from typing import Dict, Optional

import requests

from app.sku_utils import normalize_sku

logger = logging.getLogger(__name__)

QUERY_PRODUCTS = """
query GetAllProducts($pageSize: Int!, $currentPage: Int!) {
  products(
    filter: {}
    pageSize: $pageSize
    currentPage: $currentPage
  ) {
    items {
      sku
      is_salable
      stock_status
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

# Fallback query used if the DDVC schema does not expose `stock_status`.
QUERY_PRODUCTS_NO_STOCK_STATUS = """
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


class GraphQLError(RuntimeError):
    pass


class DDVCFetchIntegrityError(RuntimeError):
    """Raised when a DDVC full fetch is incomplete or inconsistent."""


def gql(graphql_url: str, query: str, variables: dict, timeout_s: float) -> dict:
    response = requests.post(graphql_url, json={"query": query, "variables": variables}, timeout=timeout_s)
    response.raise_for_status()
    data = response.json()
    errors = data.get("errors")
    if errors:
        logger.error("DDVC GraphQL errors: %s | query=%s | variables=%s", errors, query, variables)
        raise GraphQLError(f"GraphQL errors: {errors}")
    return data


def fetch_ddvc_full(graphql_url: str) -> Dict[str, Dict[str, Optional[float]]]:
    page_size = int(os.getenv("DDVC_PAGE_SIZE", "100"))
    sleep_seconds = float(os.getenv("DDVC_SLEEP_SECONDS", "0.35"))
    timeout = int(os.getenv("DDVC_TIMEOUT", "90"))
    log_every = int(os.getenv("DDVC_LOG_EVERY_PAGES", "5"))
    max_pages = os.getenv("DDVC_MAX_PAGES")
    max_pages = int(max_pages) if max_pages and max_pages.isdigit() else None
    count_tolerance_abs = int(os.getenv("DDVC_TOTAL_COUNT_TOLERANCE_ABS", "0"))
    count_tolerance_pct = float(os.getenv("DDVC_TOTAL_COUNT_TOLERANCE_PCT", "0"))
    retry_limit = 3
    start_time = time.time()
    current_page = 1
    total_pages = 1
    total_count = 0
    ok_pages = 0
    fail_pages = 0
    regular_price_count = 0
    final_price_only_count = 0
    fetched_items_count = 0
    results: Dict[str, Dict[str, Optional[float]]] = {}

    logger.info("DDVC full fetch: page_size=%s timeout=%s", page_size, timeout)

    # Empezamos pidiendo stock_status; si el schema no lo soporta, caemos al
    # query sin ese campo y lo recordamos para todas las siguientes páginas.
    active_query = QUERY_PRODUCTS

    while current_page <= total_pages:
        if max_pages and current_page > max_pages:
            logger.warning("DDVC_MAX_PAGES reached (%s). Stopping early.", max_pages)
            break

        payload = None
        for attempt in range(1, retry_limit + 1):
            try:
                payload = gql(
                    graphql_url,
                    active_query,
                    {"pageSize": page_size, "currentPage": current_page},
                    timeout,
                )
                break
            except GraphQLError as exc:
                # Si el schema no expone stock_status, reintentamos con la
                # query reducida en esta misma iteración.
                if active_query is QUERY_PRODUCTS and "stock_status" in str(exc):
                    logger.warning(
                        "DDVC schema does not expose stock_status, falling back to is_salable only"
                    )
                    active_query = QUERY_PRODUCTS_NO_STOCK_STATUS
                    continue
                logger.warning(
                    "DDVC full fetch failed page=%s attempt=%s/%s error=%s",
                    current_page,
                    attempt,
                    retry_limit,
                    exc,
                )
                if attempt < retry_limit and sleep_seconds > 0:
                    time.sleep(sleep_seconds)
            except Exception as exc:
                logger.warning(
                    "DDVC full fetch failed page=%s attempt=%s/%s error=%s",
                    current_page,
                    attempt,
                    retry_limit,
                    exc,
                )
                if attempt < retry_limit and sleep_seconds > 0:
                    time.sleep(sleep_seconds)

        if payload is None:
            fail_pages += 1
            logger.error(
                "DDVC full fetch aborting: page=%s failed after %s attempts",
                current_page,
                retry_limit,
            )
            raise DDVCFetchIntegrityError(
                f"Failed to fetch DDVC page {current_page} after {retry_limit} attempts"
            )

        ok_pages += 1
        products = payload.get("data", {}).get("products", {})
        total_count = products.get("total_count") or total_count
        page_info = products.get("page_info") or {}
        total_pages = page_info.get("total_pages") or total_pages
        items = products.get("items") or []
        fetched_items_count += len(items)

        if current_page == 1:
            logger.info("DDVC total_count=%s total_pages=%s", total_count, total_pages)
            logger.info(
                "DDVC full fetch started page_size=%s timeout=%s total_count=%s total_pages=%s",
                page_size,
                timeout,
                total_count,
                total_pages,
            )

        for item in items:
            if not item:
                continue
            sku = normalize_sku(item.get("sku"))
            if sku == "":
                continue
            min_price = item.get("price_range", {}).get("minimum_price", {})
            regular_price = min_price.get("regular_price", {}).get("value")
            final_price = min_price.get("final_price", {}).get("value")
            results[sku] = {
                "is_salable": item.get("is_salable"),
                "stock_status": item.get("stock_status"),
                "regular_price": regular_price,
                "final_price": final_price,
            }
            if regular_price is not None:
                regular_price_count += 1
            elif final_price is not None:
                final_price_only_count += 1

        if current_page != 1 and (current_page % log_every == 0 or current_page == total_pages):
            elapsed = time.time() - start_time
            logger.info(
                "DDVC progress page=%s/%s page_items=%s rows=%s fetched_items=%s elapsed=%.1fs",
                current_page,
                total_pages,
                len(items),
                len(results),
                fetched_items_count,
                elapsed,
            )

        if current_page >= total_pages:
            break
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        current_page += 1

    elapsed = time.time() - start_time
    expected_pages = total_pages
    if ok_pages != expected_pages:
        logger.error(
            "DDVC full fetch inconsistency: expected_pages=%s ok_pages=%s fail_pages=%s",
            expected_pages,
            ok_pages,
            fail_pages,
        )
        raise DDVCFetchIntegrityError(
            f"Incomplete DDVC pagination: expected {expected_pages} pages, got {ok_pages}"
        )

    diff = abs((total_count or 0) - fetched_items_count)
    allowed_diff = max(
        count_tolerance_abs,
        int((total_count or 0) * (count_tolerance_pct / 100.0)),
    )
    logger.info(
        "DDVC totals expected_total_count=%s fetched_items=%s unique_skus=%s diff=%s allowed_diff=%s",
        total_count,
        fetched_items_count,
        len(results),
        diff,
        allowed_diff,
    )
    if diff > allowed_diff:
        raise DDVCFetchIntegrityError(
            f"DDVC total_count mismatch: expected={total_count} fetched={fetched_items_count} diff={diff}"
        )

    logger.info("DDVC full fetch done rows=%s pages=%s", len(results), total_pages)
    logger.info(
        "DDVC full fetch summary ok_pages=%s fail_pages=%s elapsed=%.2fs",
        ok_pages,
        fail_pages,
        elapsed,
    )
    logger.info(
        "DDVC fetch completed rows=%s regular_price=%s final_price_only=%s",
        len(results),
        regular_price_count,
        final_price_only_count,
    )
    return results
