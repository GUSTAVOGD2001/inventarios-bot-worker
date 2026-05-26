from __future__ import annotations

import logging
import os
import time
from typing import Dict, List, Optional

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

QUERY_PRODUCT_BY_SKU = """
query GetProductBySku($sku: String!) {
  products(filter: { sku: { eq: $sku } }, pageSize: 1, currentPage: 1) {
    items {
      sku
      is_salable
    }
  }
}
"""


class GraphQLError(RuntimeError):
    pass


class DDVCFetchIntegrityError(RuntimeError):
    """Raised when a DDVC full fetch is incomplete or inconsistent."""


def gql(graphql_url: str, query: str, variables: dict, timeout_s: float) -> dict:
    response = requests.post(
        graphql_url,
        json={"query": query, "variables": variables},
        timeout=timeout_s,
        headers={"User-Agent": "Agente-Portales-Libertad"},
    )
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


def validar_skus_batch(
    graphql_url: str,
    skus: List[str],
    chunk_size: int = 150,
) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Consulta múltiples SKUs en batch usando el filtro { sku: { in: [...] } }.
    Retorna un dict con los SKUs encontrados y sus datos (mismo formato que fetch_ddvc_full).
    Los SKUs no encontrados simplemente no aparecen en el resultado.
    """
    from app.sku_utils import normalize_sku

    timeout = float(os.getenv("DDVC_TIMEOUT", "90"))
    sleep_seconds = float(os.getenv("DDVC_SLEEP_SECONDS", "0.35"))

    # Normalizar y deduplicar
    normalized = list({normalize_sku(s) for s in skus if normalize_sku(s)})
    if not normalized:
        return {}

    results: Dict[str, Dict[str, Optional[float]]] = {}
    chunks = [normalized[i:i + chunk_size] for i in range(0, len(normalized), chunk_size)]

    logger.info("Batch validating %s SKUs in %s chunks", len(normalized), len(chunks))

    BATCH_QUERY = """
    query ($skus: [String!]) {
        products(filter: { sku: { in: $skus } }, pageSize: 200) {
            items {
                sku
                is_salable
                price_range {
                    minimum_price {
                        regular_price { value }
                        final_price { value }
                    }
                }
            }
        }
    }
    """

    for idx, chunk in enumerate(chunks):
        for attempt in range(3):
            try:
                payload = gql(graphql_url, BATCH_QUERY, {"skus": chunk}, timeout)
                items = payload.get("data", {}).get("products", {}).get("items") or []
                for item in items:
                    if not item:
                        continue
                    sku = normalize_sku(item.get("sku"))
                    if not sku:
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
                logger.info(
                    "Batch chunk %s/%s: sent=%s found=%s",
                    idx + 1, len(chunks), len(chunk), len([s for s in chunk if s in results])
                )
                break
            except Exception as exc:
                if attempt >= 2:
                    logger.error("Batch validation chunk %s failed after retries: %s", idx, exc)
                else:
                    logger.warning("Batch validation chunk %s attempt %s failed: %s", idx, attempt + 1, exc)
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)

    logger.info("Batch validation complete: %s/%s SKUs found", len(results), len(normalized))
    return results


def validar_sku_directo(
    graphql_url: str,
    sku: str,
    cache: Optional[Dict[str, tuple[Optional[bool], Optional[bool]]]] = None,
) -> tuple[Optional[bool], Optional[bool]]:
    """Valida un SKU directamente contra GraphQL cuando falta en snapshot.

    Retorna:
      - (True, is_salable) si el SKU existe.
      - (False, None) si el SKU no existe.
      - (None, None) si hubo error (fail-safe).
    """
    normalized_sku = normalize_sku(sku)
    if not normalized_sku:
        return False, None

    if cache is not None and normalized_sku in cache:
        return cache[normalized_sku]

    timeout = float(os.getenv("DDVC_DIRECT_CHECK_TIMEOUT", os.getenv("DDVC_TIMEOUT", "30")))
    try:
        payload = gql(
            graphql_url=graphql_url,
            query=QUERY_PRODUCT_BY_SKU,
            variables={"sku": normalized_sku},
            timeout_s=timeout,
        )
        items = payload.get("data", {}).get("products", {}).get("items") or []
        if not items:
            result: tuple[Optional[bool], Optional[bool]] = (False, None)
        else:
            is_salable = items[0].get("is_salable")
            result = (True, bool(is_salable) if is_salable is not None else None)
    except Exception as exc:
        logger.error("Fallback direct SKU validation failed sku=%s error=%s", normalized_sku, exc)
        result = (None, None)

    if cache is not None:
        cache[normalized_sku] = result
    return result
