from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import httpx

from app.sku_utils import normalize_sku

logger = logging.getLogger(__name__)


@dataclass
class ShopifyVariant:
    sku: str
    variant_id: str
    inventory_item_id: str


@dataclass
class ShopifyVariantSnapshot:
    sku: str
    variant_id: str
    product_id: str
    inventory_item_id: str
    price: float
    quantity: Optional[int]


class ShopifyClient:
    def __init__(self, shop: str, client_id: str, client_secret: str, api_version: str) -> None:
        self.shop = shop
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_version = api_version
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0
        self._http = httpx.Client(timeout=30.0)
        self._inventory_quantity_field_name: Optional[str] = None
        self._inventory_quantity_field_resolved = False

    def close(self) -> None:
        self._http.close()

    def get_access_token(self) -> str:
        now = time.time()
        if self._access_token and now < self._expires_at - 60:
            return self._access_token

        url = f"https://{self.shop}/admin/oauth/access_token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        response = self._http.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        token = data.get("access_token")
        expires_in = data.get("expires_in", 86400)
        if not token:
            raise RuntimeError("Missing access_token in Shopify response")
        self._access_token = token
        self._expires_at = now + int(expires_in)
        return token

    def _post_graphql(self, query: str, variables: Optional[dict] = None) -> dict:
        url = f"https://{self.shop}/admin/api/{self.api_version}/graphql.json"
        headers = {"X-Shopify-Access-Token": self.get_access_token()}
        payload = {"query": query, "variables": variables or {}}
        for attempt in range(5):
            response = self._http.post(url, headers=headers, json=payload)
            if response.status_code in {429, 502, 503}:
                backoff = 2 ** attempt
                logger.warning("Shopify throttled (%s). Sleeping %ss", response.status_code, backoff)
                time.sleep(backoff)
                continue
            response.raise_for_status()
            data = response.json()
            if data.get("errors"):
                logger.warning("Shopify GraphQL errors: %s", data["errors"])
            throttle_status = (
                data.get("extensions", {})
                .get("cost", {})
                .get("throttleStatus", {})
            )
            currently_available = throttle_status.get("currentlyAvailable")
            if isinstance(currently_available, int) and currently_available < 50:
                backoff = 2 ** attempt
                logger.warning("Shopify cost low (%s). Sleeping %ss", currently_available, backoff)
                time.sleep(backoff)
                continue
            return data
        raise RuntimeError("Shopify GraphQL request failed after retries")

    def _resolve_inventory_quantity_field(self) -> Optional[str]:
        if self._inventory_quantity_field_resolved:
            return self._inventory_quantity_field_name

        query = """
        query {
            __type(name: "InventorySetOnHandQuantitiesSetQuantityInput") {
                inputFields {
                    name
                }
            }
        }
        """
        data = self._post_graphql(query)
        field_name: Optional[str] = None
        if not data.get("errors"):
            type_info = data.get("data", {}).get("__type")
            fields = type_info.get("inputFields") if isinstance(type_info, dict) else None
            field_names = {field.get("name") for field in fields or [] if isinstance(field, dict)}
            for candidate in ("onHandQuantity", "availableQuantity", "quantity"):
                if candidate in field_names:
                    field_name = candidate
                    break

        self._inventory_quantity_field_name = field_name
        self._inventory_quantity_field_resolved = True
        return field_name

    def get_location_id(self) -> str:
        query = """
        query {
            locations(first: 10) {
                nodes {
                    id
                    name
                    isActive
                }
            }
        }
        """
        data = self._post_graphql(query)
        locations = data.get("data", {}).get("locations", {}).get("nodes", [])
        if not locations:
            raise RuntimeError("No Shopify locations returned")
        active = next((loc for loc in locations if loc.get("isActive")), locations[0])
        location_id = active.get("id")
        if not location_id:
            raise RuntimeError("Invalid Shopify location id")
        return location_id

    def fetch_variant_map(self) -> List[ShopifyVariant]:
        variants: List[ShopifyVariant] = []
        cursor: Optional[str] = None
        while True:
            query = """
            query ($cursor: String) {
                productVariants(first: 250, after: $cursor) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    nodes {
                        id
                        sku
                        inventoryItem {
                            id
                        }
                    }
                }
            }
            """
            data = self._post_graphql(query, {"cursor": cursor})
            payload = data.get("data", {}).get("productVariants", {})
            nodes = payload.get("nodes", [])
            for node in nodes:
                sku = normalize_sku(node.get("sku"))
                variant_id = node.get("id")
                inventory_item_id = node.get("inventoryItem", {}).get("id")
                if not sku or not variant_id or not inventory_item_id:
                    continue
                variants.append(
                    ShopifyVariant(sku=sku, variant_id=variant_id, inventory_item_id=inventory_item_id)
                )
            page_info = payload.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return variants

    def fetch_variant_snapshot(self, location_id: Optional[str]) -> List[ShopifyVariantSnapshot]:
        variants: List[ShopifyVariantSnapshot] = []
        cursor: Optional[str] = None
        while True:
            query = """
            query ($cursor: String) {
                productVariants(first: 250, after: $cursor) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    nodes {
                        id
                        sku
                        price
                        product {
                            id
                        }
                        inventoryItem {
                            id
                            inventoryLevels(first: 10) {
                                edges {
                                    node {
                                        location {
                                            id
                                        }
                                        quantities(names: ["available"]) {
                                            name
                                            quantity
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """
            data = self._post_graphql(query, {"cursor": cursor})
            if data.get("errors"):
                logger.error("Shopify GraphQL errors: %s", json.dumps(data["errors"]))
                raise RuntimeError("Shopify GraphQL errors while fetching snapshot")
            payload = data.get("data", {}).get("productVariants", {})
            nodes = payload.get("nodes", [])
            for node in nodes:
                sku = normalize_sku(node.get("sku"))
                variant_id = node.get("id")
                product_id = (node.get("product") or {}).get("id")
                inventory_item = node.get("inventoryItem") or {}
                inventory_item_id = inventory_item.get("id")
                inventory_levels = inventory_item.get("inventoryLevels", {}).get("edges", [])
                level_node: Optional[dict] = None
                if location_id:
                    for edge in inventory_levels:
                        node_data = edge.get("node") or {}
                        location = node_data.get("location") or {}
                        if location.get("id") == location_id:
                            level_node = node_data
                            break
                if level_node is None and inventory_levels:
                    level_node = (inventory_levels[0].get("node") or {})
                available: Optional[int] = None
                if level_node:
                    quantities = level_node.get("quantities") or []
                    for entry in quantities:
                        if entry.get("name") == "available":
                            try:
                                available = int(entry.get("quantity"))
                            except (TypeError, ValueError):
                                available = None
                            break
                price_raw = node.get("price")
                if not sku or not variant_id or not product_id or not inventory_item_id:
                    continue
                try:
                    price = float(price_raw)
                except (TypeError, ValueError):
                    continue
                quantity = available
                variants.append(
                    ShopifyVariantSnapshot(
                        sku=sku,
                        variant_id=variant_id,
                        product_id=product_id,
                        inventory_item_id=inventory_item_id,
                        price=price,
                        quantity=quantity,
                    )
                )
            page_info = payload.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return variants

    def update_inventory(self, location_id: str, updates: List[Tuple[str, int]]) -> Dict[str, Optional[str]]:
        results: Dict[str, Optional[str]] = {}
        if not updates:
            return results
        quantity_field = self._resolve_inventory_quantity_field()
        fallback_candidates = ["onHandQuantity", "availableQuantity", "quantity"]
        batch_size = 50
        for i in range(0, len(updates), batch_size):
            batch = updates[i : i + batch_size]
            for inventory_item_id, _ in batch:
                results[inventory_item_id] = None
            mutation = """
            mutation($input: InventorySetOnHandQuantitiesInput!) {
                inventorySetOnHandQuantities(input: $input) {
                    userErrors { field message }
                }
            }
            """
            candidates = [quantity_field] if quantity_field else fallback_candidates
            attempted = 0
            applied_field: Optional[str] = None
            while candidates:
                field_name = candidates.pop(0)
                if field_name is None:
                    continue
                set_quantities = [
                    {
                        "inventoryItemId": inventory_item_id,
                        "locationId": location_id,
                        field_name: int(quantity),
                    }
                    for inventory_item_id, quantity in batch
                ]
                variables = {"input": {"reason": "correction", "setQuantities": set_quantities}}
                data = self._post_graphql(mutation, variables)
                errors = data.get("errors") or []
                if errors:
                    error_messages = " ".join(str(err.get("message", "")) for err in errors if isinstance(err, dict))
                    invalid_argument = "argumentNotAccepted" in error_messages or "isn't defined" in error_messages
                    if quantity_field is None and attempted == 0 and invalid_argument:
                        attempted += 1
                        continue
                    raise RuntimeError(f"Shopify GraphQL errors while updating inventory: {errors}")
                applied_field = field_name
                payload = data.get("data", {}).get("inventorySetOnHandQuantities", {})
                user_errors = payload.get("userErrors") or []
                if user_errors:
                    logger.warning("Inventory update userErrors: %s", user_errors)
                    for error in user_errors:
                        field = error.get("field") or []
                        message = error.get("message") or "Unknown inventory error"
                        index = next((int(item) for item in field if isinstance(item, int) or str(item).isdigit()), None)
                        if index is None or index >= len(batch):
                            for inventory_item_id, _ in batch:
                                results[inventory_item_id] = message
                            continue
                        inventory_item_id = batch[index][0]
                        results[inventory_item_id] = message
                break
            if applied_field and quantity_field is None:
                self._inventory_quantity_field_name = applied_field
                self._inventory_quantity_field_resolved = True
        return results

    def update_prices(self, updates: List[Tuple[str, str, float]]) -> Dict[str, Optional[str]]:
        results: Dict[str, Optional[str]] = {}
        if not updates:
            return results
        grouped: Dict[str, List[Tuple[str, float]]] = {}
        for product_id, variant_id, price in updates:
            grouped.setdefault(product_id, []).append((variant_id, price))
        logger.info("Updating prices: products=%s variants=%s", len(grouped), len(updates))

        mutation = """
        mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
            productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                product {
                    id
                }
                productVariants {
                    id
                    price
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        updated_count = 0
        batch_size = 100
        for product_id, variants in grouped.items():
            for i in range(0, len(variants), batch_size):
                batch = variants[i : i + batch_size]
                for variant_id, _ in batch:
                    results[variant_id] = None
                variables = {
                    "productId": product_id,
                    "variants": [{"id": variant_id, "price": str(price)} for variant_id, price in batch],
                }
                data = self._post_graphql(mutation, variables)
                if data.get("errors"):
                    logger.warning("Price update errors: %s", json.dumps(data))
                    raise RuntimeError("Shopify GraphQL errors while updating prices")
                payload = data.get("data", {}).get("productVariantsBulkUpdate", {})
                user_errors = payload.get("userErrors") or []
                if user_errors:
                    logger.warning("Price update errors: %s", json.dumps(user_errors))
                    for error in user_errors:
                        field = error.get("field") or []
                        message = error.get("message") or "Unknown price error"
                        index = next((int(item) for item in field if isinstance(item, int) or str(item).isdigit()), None)
                        if index is None or index >= len(batch):
                            for variant_id, _ in batch:
                                results[variant_id] = message
                            continue
                        variant_id = batch[index][0]
                        results[variant_id] = message
                updated_count += len(batch)
        logger.info("Price updates applied ok: %s variants", updated_count)
        return results
