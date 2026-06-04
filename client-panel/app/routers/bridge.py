import json
import logging
import uuid
from datetime import datetime
from decimal import Decimal

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from ..config import settings
from ..db import get_pool
from ..error_handler import log_endpoint_errors
from ..pricing_engine import calculate_final_price

logger = logging.getLogger(__name__)
router = APIRouter()


async def require_bridge_key(request: Request):
    """Autenticación para el worker de Libertad."""
    key = request.headers.get("X-Bridge-Key")
    if not key or key != settings.bridge_api_key:
        raise HTTPException(status_code=401, detail="Invalid bridge key")
    return key


def _json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


async def _load_panel_settings(pool) -> dict:
    rows = await pool.fetch("SELECT key, value FROM panel_settings")
    result = {}
    for row in rows:
        val = row["value"]
        result[row["key"]] = json.loads(val) if isinstance(val, str) else val
    return result


@router.get("/bridge/health", dependencies=[Depends(require_bridge_key)])
async def bridge_health():
    """Health check que el worker llama para verificar que el panel responde."""
    pool = await get_pool()
    count = await pool.fetchval("SELECT COUNT(*) FROM sku_state")
    return {
        "status": "ok",
        "skus_in_db": count,
        "shop": settings.shopify_shop,
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/bridge/pricing-config", dependencies=[Depends(require_bridge_key)])
@log_endpoint_errors
async def get_pricing_config():
    """Devuelve la configuración de precios del cliente para que el worker la use."""
    pool = await get_pool()
    panel_settings = await _load_panel_settings(pool)
    rule = await pool.fetchrow(
        "SELECT name, rule_type, value FROM pricing_rules WHERE is_active = true ORDER BY priority DESC LIMIT 1"
    )
    overrides = await pool.fetch(
        "SELECT sku, override_type, value FROM sku_overrides WHERE is_active = true"
    )
    prefix_overrides = await pool.fetch(
        "SELECT sku_prefix, override_type, value FROM sku_prefix_overrides WHERE is_active = true ORDER BY LENGTH(sku_prefix) DESC"
    )
    return {
        "settings": panel_settings,
        "global_rule": dict(rule) if rule else None,
        "overrides": {
            r["sku"]: {"override_type": r["override_type"], "value": float(r["value"])}
            for r in overrides
        },
        "prefix_overrides": [
            {"sku_prefix": r["sku_prefix"], "override_type": r["override_type"], "value": float(r["value"])}
            for r in prefix_overrides
        ],
    }


@router.post("/bridge/push-sync", dependencies=[Depends(require_bridge_key)])
@log_endpoint_errors
async def push_sync(body: dict):
    """Recibe datos DDVC, aplica reglas del cliente y actualiza Shopify por chunk."""
    pool = await get_pool()
    skus_data = body.get("skus", {})
    chunk_index = int(body.get("chunk_index", 0))
    total_chunks = int(body.get("total_chunks", 1))
    price_mode = body.get("price_mode", "raw_ddvc")
    dashboard_only = body.get("dashboard_only", False)
    run_id = uuid.uuid4().hex[:8]

    logger.info(
        "Bridge push-sync: chunk=%s/%s skus=%s price_mode=%s",
        chunk_index + 1,
        total_chunks,
        len(skus_data),
        price_mode,
    )

    panel_settings = await _load_panel_settings(pool)
    in_stock_qty = int(panel_settings.get("in_stock_qty", settings.in_stock_qty))
    out_of_stock_qty = int(panel_settings.get("out_of_stock_qty", settings.out_of_stock_qty))

    variant_count = await pool.fetchval("SELECT COUNT(*) FROM shopify_variants")
    if variant_count == 0 or chunk_index == 0:
        try:
            await _refresh_shopify_snapshot(pool)
            logger.info("Shopify snapshot refreshed")
        except Exception as exc:
            logger.error("Failed to refresh Shopify snapshot: %s", exc)

    inventory_changes = []
    price_changes = []
    skus_processed = 0

    for raw_sku, data in skus_data.items():
        sku = str(raw_sku).strip().upper()
        if not sku:
            continue

        ddvc_price = data.get("ddvc_price")
        source_price = data.get("source_price", ddvc_price)
        is_salable = data.get("is_salable")
        stock_status = data.get("stock_status")
        stock_status_norm = (stock_status or "").upper() if isinstance(stock_status, str) else None
        available = stock_status_norm == "IN_STOCK" or is_salable is True
        explicit_oos = stock_status_norm == "OUT_OF_STOCK" or is_salable is False
        target_qty = in_stock_qty if available else (out_of_stock_qty if explicit_oos else in_stock_qty)

        final_price = None
        calc = None
        if source_price is not None or ddvc_price is not None:
            base_ddvc = float(ddvc_price if ddvc_price is not None else source_price)
            source_float = float(source_price) if source_price is not None else None
            calc = await calculate_final_price(sku, base_ddvc, pool, source_price=source_float)
            final_price = calc["final_price"]

        await pool.execute(
            """INSERT INTO sku_state (sku, ddvc_price, source_price, is_salable, stock_status,
                   target_qty, final_price, last_received_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())
               ON CONFLICT (sku) DO UPDATE SET
                   ddvc_price = EXCLUDED.ddvc_price,
                   source_price = EXCLUDED.source_price,
                   is_salable = EXCLUDED.is_salable,
                   stock_status = EXCLUDED.stock_status,
                   target_qty = EXCLUDED.target_qty,
                   final_price = EXCLUDED.final_price,
                   last_received_at = now(),
                   updated_at = now()""",
            sku,
            ddvc_price,
            source_price,
            is_salable,
            stock_status,
            target_qty,
            final_price,
        )

        sv = await pool.fetchrow("SELECT * FROM shopify_variants WHERE sku = $1", sku)
        if sv:
            if sv["current_qty"] != target_qty:
                inventory_changes.append({
                    "sku": sku,
                    "inventory_item_id": sv["inventory_item_id"],
                    "old_qty": sv["current_qty"],
                    "new_qty": target_qty,
                })
                await pool.execute(
                    """INSERT INTO sync_actions (run_id, sku, action_type, old_value, new_value)
                       VALUES ($1, $2, 'inventory', $3, $4)""",
                    run_id,
                    sku,
                    str(sv["current_qty"]),
                    str(target_qty),
                )
            if final_price is not None and sv["current_price"] is not None:
                old_price = float(sv["current_price"])
                if abs(old_price - final_price) > 0.01:
                    price_changes.append({
                        "sku": sku,
                        "product_id": sv["product_id"],
                        "variant_id": sv["variant_id"],
                        "old_price": old_price,
                        "new_price": final_price,
                    })
                    await pool.execute(
                        """INSERT INTO price_change_log
                           (sku, ddvc_price, source_price, rule_applied, price_before, price_after)
                           VALUES ($1, $2, $3, $4, $5, $6)""",
                        sku,
                        ddvc_price,
                        source_price,
                        (calc or {}).get("override_applied") or (calc or {}).get("global_rule_applied"),
                        old_price,
                        final_price,
                    )
                    await pool.execute(
                        """INSERT INTO sync_actions (run_id, sku, action_type, old_value, new_value)
                           VALUES ($1, $2, 'price', $3, $4)""",
                        run_id,
                        sku,
                        str(old_price),
                        str(final_price),
                    )
        skus_processed += 1

    applied_inv = 0
    applied_price = 0
    apply_error = None
    is_last_chunk = (chunk_index + 1) >= total_chunks

    if not dashboard_only and (inventory_changes or price_changes):
        try:
            token = await _get_shopify_token()
            if token:
                if inventory_changes:
                    applied_inv = await _apply_inventory_changes(token, inventory_changes)
                if price_changes:
                    applied_price = await _apply_price_changes(token, price_changes)
                # Actualizar snapshot local con los cambios aplicados
                for change in inventory_changes:
                    await pool.execute(
                        "UPDATE shopify_variants SET current_qty = $1 WHERE sku = $2",
                        change["new_qty"],
                        change["sku"],
                    )
                for change in price_changes:
                    await pool.execute(
                        "UPDATE shopify_variants SET current_price = $1 WHERE sku = $2",
                        change["new_price"],
                        change["sku"],
                    )
                await pool.execute(
                    "UPDATE price_change_log SET was_applied = true WHERE was_applied = false"
                )
        except Exception as exc:
            apply_error = str(exc)
            logger.error("Shopify apply failed: %s", exc)

    logger.info(
        "Chunk %s/%s done: received=%s inv_changes=%s price_changes=%s "
        "applied_inv=%s applied_prices=%s error=%s",
        chunk_index + 1, total_chunks, skus_processed,
        len(inventory_changes), len(price_changes),
        applied_inv, applied_price, apply_error,
    )

    await pool.execute(
        """INSERT INTO sync_runs (run_id, source, skus_received, inventory_changes, price_changes, finished_at, error, details)
           VALUES ($1, $2, $3, $4, $5, now(), $6, $7::jsonb)""",
        run_id,
        body.get("source", "unknown"),
        skus_processed,
        len(inventory_changes),
        len(price_changes),
        apply_error,
        json.dumps(
            {"applied_inv": applied_inv, "applied_price": applied_price, "chunk": chunk_index, "total_chunks": total_chunks},
            default=_json_default,
        ),
    )

    return {
        "run_id": run_id,
        "skus_received": skus_processed,
        "inventory_changes": len(inventory_changes),
        "price_changes": len(price_changes),
        "applied_inventory": applied_inv,
        "applied_prices": applied_price,
        "is_last_chunk": is_last_chunk,
        "error": apply_error,
    }


async def _get_shopify_token() -> str | None:
    """Obtiene token de Shopify del cliente."""
    if not settings.shopify_shop or settings.shopify_shop == "placeholder":
        return None
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            f"https://{settings.shopify_shop}/admin/oauth/access_token",
            json={
                "client_id": settings.shopify_client_id,
                "client_secret": settings.shopify_client_secret,
                "grant_type": "client_credentials",
            },
        )
        resp.raise_for_status()
        return resp.json().get("access_token")


async def _refresh_shopify_snapshot(pool):
    """Actualiza el mapa de variantes de Shopify del cliente."""
    token = await _get_shopify_token()
    if not token:
        return
    cursor = None
    query = """
    query ($cursor: String) {
        productVariants(first: 250, after: $cursor) {
            pageInfo { hasNextPage endCursor }
            nodes {
                id sku price
                product { id title }
                inventoryItem {
                    id
                    inventoryLevels(first: 5) {
                        edges { node { quantities(names: ["available"]) { name quantity } } }
                    }
                }
            }
        }
    }
    """
    base_url = f"https://{settings.shopify_shop}/admin/api/{settings.shopify_api_version}/graphql.json"
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            resp = await client.post(
                base_url,
                headers={"X-Shopify-Access-Token": token},
                json={"query": query, "variables": {"cursor": cursor}},
            )
            resp.raise_for_status()
            payload = resp.json().get("data", {}).get("productVariants", {})
            for node in payload.get("nodes", []):
                sku = (node.get("sku") or "").strip().upper()
                if not sku:
                    continue
                inv = node.get("inventoryItem") or {}
                qty = None
                for level in inv.get("inventoryLevels", {}).get("edges", []):
                    for quantity in level.get("node", {}).get("quantities", []):
                        if quantity.get("name") == "available":
                            qty = int(quantity["quantity"])
                            break
                    if qty is not None:
                        break
                try:
                    price = float(node.get("price", 0))
                except (TypeError, ValueError):
                    price = 0
                product = node.get("product") or {}
                await pool.execute(
                    """INSERT INTO shopify_variants (sku, variant_id, product_id, inventory_item_id, title, current_price, current_qty)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)
                       ON CONFLICT (sku) DO UPDATE SET
                           variant_id = EXCLUDED.variant_id,
                           product_id = EXCLUDED.product_id,
                           inventory_item_id = EXCLUDED.inventory_item_id,
                           title = EXCLUDED.title,
                           current_price = EXCLUDED.current_price,
                           current_qty = EXCLUDED.current_qty,
                           updated_at = now()""",
                    sku,
                    node["id"],
                    product.get("id", ""),
                    inv.get("id", ""),
                    product.get("title"),
                    price,
                    qty,
                )
            page_info = payload.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")


async def _apply_inventory_changes(token: str, changes: list) -> int:
    """Aplica cambios de inventario en Shopify del cliente."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        loc_resp = await client.post(
            f"https://{settings.shopify_shop}/admin/api/{settings.shopify_api_version}/graphql.json",
            headers={"X-Shopify-Access-Token": token},
            json={"query": "{ locations(first: 5) { nodes { id isActive } } }"},
        )
        loc_resp.raise_for_status()
        locations = loc_resp.json().get("data", {}).get("locations", {}).get("nodes", [])
        if not locations:
            raise RuntimeError("No Shopify locations returned")
        location_id = next((loc["id"] for loc in locations if loc.get("isActive")), locations[0]["id"])

        mutation = """
        mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
            inventorySetQuantities(input: $input) {
                inventoryAdjustmentGroup { id }
                userErrors { field message }
            }
        }
        """
        applied = 0
        for i in range(0, len(changes), 50):
            batch = changes[i:i + 50]
            quantities = [
                {"inventoryItemId": c["inventory_item_id"], "locationId": location_id, "quantity": c["new_qty"]}
                for c in batch
            ]
            resp = await client.post(
                f"https://{settings.shopify_shop}/admin/api/{settings.shopify_api_version}/graphql.json",
                headers={"X-Shopify-Access-Token": token},
                json={
                    "query": mutation,
                    "variables": {
                        "input": {
                            "reason": "correction",
                            "name": "available",
                            "ignoreCompareQuantity": True,
                            "quantities": quantities,
                        }
                    },
                },
            )
            resp.raise_for_status()
            applied += len(batch)
        return applied


async def _apply_price_changes(token: str, changes: list) -> int:
    """Aplica cambios de precio en Shopify del cliente."""
    from collections import defaultdict

    grouped = defaultdict(list)
    for change in changes:
        grouped[change["product_id"]].append(change)
    mutation = """
    mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
        productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            userErrors { field message }
        }
    }
    """
    applied = 0
    async with httpx.AsyncClient(timeout=30.0) as client:
        for product_id, variants in grouped.items():
            resp = await client.post(
                f"https://{settings.shopify_shop}/admin/api/{settings.shopify_api_version}/graphql.json",
                headers={"X-Shopify-Access-Token": token},
                json={
                    "query": mutation,
                    "variables": {
                        "productId": product_id,
                        "variants": [{"id": v["variant_id"], "price": str(v["new_price"])} for v in variants],
                    },
                },
            )
            resp.raise_for_status()
            applied += len(variants)
    return applied
