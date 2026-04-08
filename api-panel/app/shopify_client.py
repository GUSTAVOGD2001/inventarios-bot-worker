import logging
import time
from typing import Optional

import httpx

from .config import settings

logger = logging.getLogger(__name__)


class ShopifyClient:
    """Cliente async para hablar con Shopify GraphQL Admin API.

    Usa OAuth client_credentials para autenticarse y cachea el token
    hasta que expira.
    """

    def __init__(self):
        self.shop = settings.shopify_shop
        self.client_id = settings.shopify_client_id
        self.client_secret = settings.shopify_client_secret
        self.api_version = settings.shopify_api_version
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0

    def is_configured(self) -> bool:
        """Verifica si las credenciales de Shopify están configuradas."""
        return bool(self.shop and self.client_id and self.client_secret)

    async def _get_access_token(self) -> str:
        now = time.time()
        if self._access_token and now < self._expires_at - 60:
            return self._access_token

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                f"https://{self.shop}/admin/oauth/access_token",
                json={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                },
            )
            response.raise_for_status()
            data = response.json()

        token = data.get("access_token")
        expires_in = data.get("expires_in", 86400)
        if not token:
            raise RuntimeError("Shopify did not return an access_token")

        self._access_token = token
        self._expires_at = now + int(expires_in)
        return token

    async def _post_graphql(self, query: str, variables: Optional[dict] = None) -> dict:
        token = await self._get_access_token()
        url = f"https://{self.shop}/admin/api/{self.api_version}/graphql.json"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                url,
                headers={"X-Shopify-Access-Token": token},
                json={"query": query, "variables": variables or {}},
            )
            response.raise_for_status()
            return response.json()

    async def search_products(self, query: str, limit: int = 20) -> list[dict]:
        """Busca productos en Shopify por nombre o SKU.

        Retorna lista de productos con sus variantes:
        [
            {
                "product_id": "gid://shopify/Product/123",
                "title": "SCANNER 3 SHAPE E3",
                "vendor": "3Shape",
                "image_url": "...",
                "variants": [
                    {
                        "variant_id": "gid://shopify/ProductVariant/456",
                        "sku": "3SHAPEE3",
                        "price": 159000.00,
                        "compare_at_price": 198750.00,
                        "inventory_quantity": 0,
                        "available": false,
                    }
                ]
            }
        ]
        """
        # Shopify usa el query language: title:*scanner* OR sku:*3shape*
        # Para buscar por nombre Y SKU usamos OR
        # Escapamos comillas para evitar inyección
        safe_query = query.replace('"', '').replace('\\', '')
        search_query = f'title:*{safe_query}* OR sku:*{safe_query}*'

        graphql_query = """
        query searchProducts($query: String!, $first: Int!) {
            products(first: $first, query: $query) {
                nodes {
                    id
                    title
                    vendor
                    featuredImage {
                        url
                    }
                    variants(first: 10) {
                        nodes {
                            id
                            sku
                            price
                            compareAtPrice
                            availableForSale
                            inventoryQuantity
                        }
                    }
                }
            }
        }
        """

        data = await self._post_graphql(
            graphql_query,
            {"query": search_query, "first": limit},
        )

        if data.get("errors"):
            logger.error("Shopify search errors: %s", data["errors"])
            raise RuntimeError(f"Shopify error: {data['errors']}")

        products = data.get("data", {}).get("products", {}).get("nodes", [])
        results = []
        for p in products:
            variants = []
            for v in p.get("variants", {}).get("nodes", []):
                try:
                    price = float(v.get("price") or 0)
                except (ValueError, TypeError):
                    price = 0.0
                try:
                    compare_at = float(v.get("compareAtPrice") or 0) if v.get("compareAtPrice") else None
                except (ValueError, TypeError):
                    compare_at = None
                variants.append({
                    "variant_id": v.get("id"),
                    "sku": (v.get("sku") or "").strip().upper(),
                    "price": price,
                    "compare_at_price": compare_at,
                    "inventory_quantity": v.get("inventoryQuantity"),
                    "available": v.get("availableForSale"),
                })
            results.append({
                "product_id": p.get("id"),
                "title": p.get("title"),
                "vendor": p.get("vendor"),
                "image_url": (p.get("featuredImage") or {}).get("url"),
                "variants": variants,
            })
        return results


# Instancia global (singleton)
shopify_client = ShopifyClient()
