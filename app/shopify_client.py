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


class ShopifyClient:
    def __init__(self, shop: str, client_id: str, client_secret: str, api_version: str) -> None:
        self.shop = shop
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_version = api_version
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0
        self._http = httpx.Client(timeout=30.0)

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

    def update_inventory(self, location_id: str, updates: List[Tuple[str, int]]) -> None:
        batch_size = 50
        for i in range(0, len(updates), batch_size):
            batch = updates[i : i + batch_size]
            set_quantities = [
                {
                    "inventoryItemId": inventory_item_id,
                    "availableQuantity": int(quantity),
                }
                for inventory_item_id, quantity in batch
            ]
            mutation = """
            mutation ($locationId: ID!, $setQuantities: [InventorySetOnHandQuantityInput!]!) {
                inventorySetOnHandQuantities(input: {locationId: $locationId, setQuantities: $setQuantities}) {
                    userErrors { field message }
                }
            }
            """
            data = self._post_graphql(mutation, {"locationId": location_id, "setQuantities": set_quantities})
            if data.get("data", {}).get("inventorySetOnHandQuantities", {}).get("userErrors"):
                logger.warning("Inventory update userErrors: %s", data["data"]["inventorySetOnHandQuantities"]["userErrors"])

    def update_prices(self, updates: List[Tuple[str, float]]) -> None:
        batch_size = 25
        for i in range(0, len(updates), batch_size):
            batch = updates[i : i + batch_size]
            mutation_parts = []
            variables: Dict[str, dict] = {}
            for idx, (variant_id, price) in enumerate(batch):
                var_name = f"input{idx}"
                mutation_parts.append(
                    f"m{idx}: productVariantUpdate(input: ${var_name}) {{ userErrors {{ field message }} }}"
                )
                variables[var_name] = {"id": variant_id, "price": str(price)}
            mutation = "mutation(" + ", ".join(f"${k}: ProductVariantInput!" for k in variables) + ") {" + " ".join(mutation_parts) + "}"
            data = self._post_graphql(mutation, variables)
            if data.get("errors"):
                logger.warning("Price update errors: %s", json.dumps(data["errors"]))
