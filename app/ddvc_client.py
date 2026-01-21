from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import httpx

from app.sku_utils import normalize_sku

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DdvcItem:
    sku: str
    is_salable: bool
    ddvc_price: float
    has_regular_price: bool


class DdvcClient:
    def __init__(self, graphql_url: str, concurrency: int, timeout_s: float = 20.0) -> None:
        self.graphql_url = graphql_url
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout_s = timeout_s

    async def fetch_chunk(self, client: httpx.AsyncClient, skus: List[str]) -> Tuple[bool, Dict[str, DdvcItem]]:
        query = """
        query ($skus: [String!]) {
            products(filter: { sku: { in: $skus } }) {
                items {
                    sku
                    is_salable
                    price_range {
                        minimum_price {
                            regular_price {
                                value
                            }
                            final_price {
                                value
                            }
                        }
                    }
                }
            }
        }
        """
        variables = {"skus": skus}
        payload = {"query": query, "variables": variables}

        async with self.semaphore:
            for attempt in range(4):
                try:
                    response = await client.post(self.graphql_url, json=payload)
                    response.raise_for_status()
                    data = response.json()
                    if "errors" in data:
                        raise RuntimeError(f"GraphQL errors: {data['errors']}")
                    items = self._extract_items(data.get("data", {}))
                    return True, {item.sku: item for item in items}
                except (httpx.TimeoutException, httpx.HTTPError, RuntimeError) as exc:
                    if attempt >= 3:
                        logger.warning("DDVC chunk failed after retries: %s", exc)
                        return False, {}
                    backoff = 2 ** attempt
                    logger.warning("DDVC chunk error (attempt %s): %s. Retrying in %ss", attempt + 1, exc, backoff)
                    await asyncio.sleep(backoff)
        return False, {}

    def _extract_items(self, data: dict) -> List[DdvcItem]:
        products = data.get("products")
        if isinstance(products, dict):
            items = products.get("items") or products.get("nodes") or []
        elif isinstance(products, list):
            items = products
        else:
            items = []

        results: List[DdvcItem] = []
        for item in items or []:
            if not item:
                continue
            sku = normalize_sku(item.get("sku"))
            if not sku:
                continue
            is_salable = bool(item.get("is_salable"))
            regular_price, final_price = self._extract_prices(item)
            ddvc_price = regular_price if regular_price is not None else final_price
            if ddvc_price is None:
                continue
            results.append(
                DdvcItem(
                    sku=sku,
                    is_salable=is_salable,
                    ddvc_price=ddvc_price,
                    has_regular_price=regular_price is not None,
                )
            )
        return results

    def _extract_prices(self, item: dict) -> Tuple[Optional[float], Optional[float]]:
        price_range = item.get("price_range") or {}
        minimum_price = price_range.get("minimum_price") or {}
        regular_price = self._parse_price(minimum_price.get("regular_price") or {})
        final_price = self._parse_price(minimum_price.get("final_price") or {})
        return regular_price, final_price

    def _parse_price(self, price_node: dict) -> Optional[float]:
        value = price_node.get("value")
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


async def fetch_ddvc_chunks(
    graphql_url: str,
    skus: Iterable[str],
    chunk_size: int,
    concurrency: int,
) -> Tuple[int, int, Dict[str, DdvcItem], Dict[int, List[str]]]:
    skus_list = [normalize_sku(sku) for sku in skus]
    skus_list = [sku for sku in skus_list if sku]
    chunks = [skus_list[i : i + chunk_size] for i in range(0, len(skus_list), chunk_size)]
    client = DdvcClient(graphql_url, concurrency)

    results: Dict[str, DdvcItem] = {}
    failed_chunks: Dict[int, List[str]] = {}
    async with httpx.AsyncClient(timeout=client.timeout_s) as http_client:
        tasks = [client.fetch_chunk(http_client, chunk) for chunk in chunks]
        responses = await asyncio.gather(*tasks)

    ok_count = 0
    for idx, (ok, items) in enumerate(responses):
        chunk = chunks[idx]
        returned_skus = set(items.keys()) if ok else set()
        missing = [sku for sku in chunk if sku not in returned_skus]
        logger.info(
            "DDVC chunk index=%s size_sent=%s items_returned=%s missing_count_in_chunk=%s sample_missing=%s",
            idx,
            len(chunk),
            len(returned_skus),
            len(missing),
            missing[:10],
        )
        if ok:
            ok_count += 1
            results.update(items)
        else:
            failed_chunks[idx] = chunks[idx]

    found_list = [sku for sku in skus_list if sku in results]
    not_found_list = [sku for sku in skus_list if sku not in results]
    logger.info(
        "DDVC found_count=%s not_found_count=%s sample_found=%s sample_not_found=%s",
        len(found_list),
        len(not_found_list),
        found_list[:10],
        not_found_list[:10],
    )

    return ok_count, len(chunks), results, failed_chunks
