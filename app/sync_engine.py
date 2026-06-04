from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Dict, List, Optional, Tuple

import requests as sync_requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app import db
from app.config import Settings
from app.ddvc_full import DDVCFetchIntegrityError, fetch_ddvc_full
from app.pricing import PricingEngine, load_sku_exemptions, log_price_change
from app.shopify_client import ShopifyClient, ShopifyVariantSnapshot
from app.sku_utils import normalize_sku

logger = logging.getLogger(__name__)


def _normalize_snapshot(snapshot: List[ShopifyVariantSnapshot]) -> Dict[str, ShopifyVariantSnapshot]:
    normalized: Dict[str, ShopifyVariantSnapshot] = {}
    for item in snapshot:
        normalized_sku = normalize_sku(item.sku)
        if not normalized_sku:
            continue
        normalized[normalized_sku] = item
    return normalized


def _stringify(value: Optional[float | int]) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _distribute_to_shops(engine: Engine, ddvc_map: dict, settings: Settings) -> None:
    """Distribuye datos DDVC a tiendas secundarias aplicando cambios directo a su Shopify."""
    with engine.connect() as conn:
        shops = conn.execute(
            text("""
                SELECT id, name, slug, shopify_shop, shopify_client_id,
                       shopify_client_secret, shopify_api_version,
                       in_stock_qty, out_of_stock_qty,
                       api_panel_url, bridge_api_key, price_mode
                FROM shops
                WHERE is_active = true AND is_primary = false
                  AND shopify_shop IS NOT NULL
                  AND shopify_shop != ''
                  AND shopify_shop != 'placeholder'
            """)
        ).fetchall()

    if not shops:
        logger.info("No secondary shops to distribute to")
        return

    logger.info("Distributing to %s secondary shops", len(shops))

    # Cargar pricing engine de Libertad para mode=with_my_markup
    pricing_engine = PricingEngine(engine)
    pricing_engine.load_rules()

    for shop_row in shops:
        shop_id       = shop_row[0]
        shop_name     = shop_row[1]
        shopify_shop  = shop_row[3]
        client_id     = shop_row[4]
        client_secret = shop_row[5]
        api_version   = shop_row[6] or "2026-01"
        in_stock_qty  = shop_row[7] or 100
        out_stock_qty = shop_row[8] or 0
        api_panel_url = shop_row[9]
        bridge_key    = shop_row[10]
        price_mode    = shop_row[11] or "raw_ddvc"

        logger.info(
            "Shop %s (%s): fetching Shopify snapshot...", shop_id, shop_name
        )
        sync_error = None
        inv_applied = 0
        price_applied = 0

        try:
            # Crear ShopifyClient para esta tienda
            shop_client = ShopifyClient(
                shop=shopify_shop,
                client_id=client_id,
                client_secret=client_secret,
                api_version=api_version,
            )

            # Obtener location_id
            location_id = shop_client.get_location_id()
            logger.info("Shop %s location_id=%s", shop_id, location_id)

            # Snapshot optimizado: payload liviano con inventoryLevel singular
            shopify_map = shop_client.fetch_variant_map_for_distribution(location_id)
            logger.info("Shop %s snapshot: %s variants", shop_id, len(shopify_map))

            # Construir inventario target y precio target para cada SKU
            inventory_updates = []   # (inventory_item_id, qty)
            price_updates = []       # (product_id, variant_id, price)

            for sku_norm, shopify_item in shopify_map.items():
                ddvc_item = ddvc_map.get(sku_norm)
                if not ddvc_item:
                    # SKU no en DDVC → poner en 0
                    if shopify_item.quantity != out_stock_qty:
                        inventory_updates.append(
                            (shopify_item.inventory_item_id, out_stock_qty)
                        )
                    continue

                # Disponibilidad
                is_salable = ddvc_item.get("is_salable")
                stock_status = ddvc_item.get("stock_status")
                stock_norm = (stock_status or "").upper() if isinstance(stock_status, str) else None
                available = stock_norm == "IN_STOCK" or is_salable is True
                explicit_oos = stock_norm == "OUT_OF_STOCK" or is_salable is False
                target_qty = (
                    in_stock_qty if available else
                    out_stock_qty if explicit_oos else
                    in_stock_qty
                )

                # Inventario
                if shopify_item.quantity != target_qty:
                    inventory_updates.append(
                        (shopify_item.inventory_item_id, target_qty)
                    )

                # Precio
                regular_price = ddvc_item.get("regular_price")
                final_price_raw = ddvc_item.get("final_price")
                ddvc_price = regular_price if regular_price is not None else final_price_raw
                if ddvc_price is None:
                    continue
                try:
                    ddvc_price = float(ddvc_price)
                except (TypeError, ValueError):
                    continue

                if price_mode == "with_my_markup":
                    try:
                        result = pricing_engine.calculate(sku_norm, ddvc_price)
                        target_price = result.final_price
                    except Exception:
                        target_price = ddvc_price
                else:
                    target_price = ddvc_price

                if abs(shopify_item.price - target_price) > 0.01:
                    price_updates.append(
                        (shopify_item.product_id, shopify_item.variant_id, target_price)
                    )

            logger.info(
                "Shop %s planned: inventory_changes=%s price_changes=%s",
                shop_id, len(inventory_updates), len(price_updates),
            )

            # Aplicar cambios de inventario
            if inventory_updates:
                shop_client.update_inventory(location_id, inventory_updates)
                inv_applied = len(inventory_updates)
                logger.info("Shop %s inventory applied: %s", shop_id, inv_applied)

            # Aplicar cambios de precio
            if price_updates:
                shop_client.update_prices(price_updates)
                price_applied = len(price_updates)
                logger.info("Shop %s prices applied: %s", shop_id, price_applied)

            shop_client.close()

            # Notificar al client-panel (solo para dashboard, sin esperar Shopify)
            if api_panel_url and api_panel_url != "placeholder" and bridge_key:
                try:
                    _notify_client_panel(
                        api_panel_url=api_panel_url,
                        bridge_key=bridge_key,
                        ddvc_map=ddvc_map,
                        price_mode=price_mode,
                        pricing_engine=pricing_engine,
                    )
                except Exception as exc:
                    logger.warning(
                        "Shop %s client-panel notify failed (non-critical): %s",
                        shop_id, exc,
                    )

        except Exception as exc:
            sync_error = str(exc)[:500]
            logger.error("Shop %s sync failed: %s", shop_id, exc, exc_info=True)

        # Actualizar estado en BD
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE shops SET
                        last_sync_at = NOW(),
                        last_sync_status = :status,
                        last_sync_error = :error,
                        last_sync_details = CAST(:details AS JSONB)
                    WHERE id = :id
                """),
                {
                    "id": shop_id,
                    "status": "error" if sync_error else "ok",
                    "error": sync_error,
                    "details": json.dumps({
                        "inventory_applied": inv_applied,
                        "prices_applied": price_applied,
                        "price_mode": price_mode,
                    }),
                },
            )

        if not sync_error:
            logger.info(
                "Shop %s sync OK: inv=%s prices=%s",
                shop_id, inv_applied, price_applied,
            )


def _notify_client_panel(
    api_panel_url: str,
    bridge_key: str,
    ddvc_map: dict,
    price_mode: str,
    pricing_engine: PricingEngine,
) -> None:
    """Envía datos DDVC al client-panel solo para actualizar el dashboard.
    El client-panel NO debe aplicar a Shopify (el worker ya lo hizo).
    """
    sku_items = []
    for sku, data in ddvc_map.items():
        if not sku:
            continue
        regular_price = data.get("regular_price")
        final_price_raw = data.get("final_price")
        ddvc_price = regular_price if regular_price is not None else final_price_raw
        if ddvc_price is None:
            continue
        try:
            ddvc_price = float(ddvc_price)
        except (TypeError, ValueError):
            continue

        if price_mode == "with_my_markup":
            try:
                result = pricing_engine.calculate(sku, ddvc_price)
                source_price = result.final_price
            except Exception:
                source_price = ddvc_price
        else:
            source_price = ddvc_price

        sku_items.append((sku, {
            "ddvc_price": ddvc_price,
            "source_price": source_price,
            "is_salable": data.get("is_salable"),
            "stock_status": data.get("stock_status"),
        }))

    chunk_size = 2000
    chunks = [sku_items[i:i + chunk_size] for i in range(0, len(sku_items), chunk_size)]
    total_chunks = len(chunks)

    for chunk_idx, chunk in enumerate(chunks):
        try:
            resp = sync_requests.post(
                f"{api_panel_url.rstrip('/')}/api/v1/bridge/push-sync",
                json={
                    "skus": dict(chunk),
                    "chunk_index": chunk_idx,
                    "total_chunks": total_chunks,
                    "source": "libertad-worker",
                    "price_mode": price_mode,
                    "dashboard_only": True,   # El client-panel NO aplica a Shopify
                },
                headers={
                    "X-Bridge-Key": bridge_key,
                    "User-Agent": "Agente-Portales-Libertad",
                },
                timeout=60,
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("Client panel notify chunk %s failed: %s", chunk_idx + 1, exc)
            break


def run_sync_once(settings: Settings, engine: Engine, shopify: ShopifyClient, run_id: str) -> None:
    start = dt.datetime.now(dt.timezone.utc)
    logger.info("Starting sync run")

    with engine.connect() as lock_conn:
        if not db.try_lock(lock_conn):
            logger.info("sync already running, skipping")
            return

        location_id = db.get_kv(engine, "location_id")
        if not location_id:
            location_id = shopify.get_location_id()
            db.set_kv(engine, "location_id", location_id)
            logger.info("Stored Shopify location id")

        error_message: Optional[str] = None
        found_count = 0
        not_found_count = 0
        skipped_count = 0
        inventory_changes = 0
        price_changes = 0
        ddvc_rows = 0
        shopify_rows = 0
        batch_validated_count = 0
        batch_found_count = 0
        batch_not_found_count = 0
        applied_to_zero = 0
        applied_to_in_stock = 0
        applied_price_changes = 0
        MAX_SAMPLES = 20
        run_inserted = False

        try:
            db.insert_sync_run(engine, run_id, start, start, settings.dry_run)
            run_inserted = True
            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "shopify_fetch",
                "message": "Obteniendo productos de Shopify...",
                "percent": 5,
                "started_at": start.isoformat(),
                "details": {}
            }))
            snapshot = shopify.fetch_variant_snapshot(location_id)
            shopify_rows = len(snapshot)
            logger.info("Shopify snapshot rows=%s", shopify_rows)
            db.upsert_variant_map(
                engine,
                [
                    (
                        item.sku,
                        db.VariantInfo(item.variant_id, item.inventory_item_id, getattr(item, 'title', None)),
                    )
                    for item in snapshot
                ],
            )

            shopify_map = _normalize_snapshot(snapshot)
            if not shopify_map:
                logger.warning("No SKUs in Shopify snapshot; skipping sync")
                return

            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "ddvc_fetch",
                "message": f"Shopify listo ({len(shopify_map)} productos). Consultando DDVC...",
                "percent": 15,
                "started_at": start.isoformat(),
                "details": {"shopify_rows": len(shopify_map)}
            }))

            ddvc_map = fetch_ddvc_full(graphql_url=settings.ddvc_graphql)
            ddvc_rows = len(ddvc_map)
            logger.info("DDVC map ready rows=%s", ddvc_rows)

            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "comparing",
                "message": f"DDVC listo ({ddvc_rows} productos). Comparando inventarios...",
                "percent": 40,
                "started_at": start.isoformat(),
                "details": {"shopify_rows": len(shopify_map), "ddvc_rows": ddvc_rows}
            }))

            # Load pricing rules from panel
            pricing_engine = PricingEngine(engine)
            pricing_engine.load_rules()

            # Load SKU exemptions from panel
            sku_exemptions = load_sku_exemptions(engine)
            if sku_exemptions:
                logger.info("Loaded %s SKU exemptions from panel", len(sku_exemptions))

            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "pricing",
                "message": "Reglas de precios cargadas. Calculando precios y comparando...",
                "percent": 45,
                "started_at": start.isoformat(),
                "details": {"shopify_rows": len(shopify_map), "ddvc_rows": ddvc_rows}
            }))

            sku_states = db.load_sku_states(engine)
            inventory_updates: List[Tuple[str, int]] = []
            price_updates: List[Tuple[str, str, float]] = []
            inventory_actions: List[Tuple[int, str, str, int]] = []
            price_actions: List[Tuple[int, str, str, float]] = []
            pending_validation: List[str] = []
            sku_status: Dict[str, Dict[str, bool]] = {}
            desired_state: Dict[str, Dict[str, Optional[float | bool | str | dt.datetime]]] = {}

            for sku_norm, shopify_item in shopify_map.items():
                planned_action = False

                # Check exemptions for this SKU
                exemption = sku_exemptions.get(sku_norm, {})
                exempt_inventory = exemption.get("exempt_inventory", False)
                exempt_price = exemption.get("exempt_price", False)
                if exempt_inventory and exempt_price:
                    skipped_count += 1
                    logger.debug("SKU %s fully exempted, skipping", sku_norm)
                    continue

                ddvc_item = ddvc_map.get(sku_norm)
                stock_status: Optional[str] = None
                if ddvc_item:
                    found_count += 1
                    is_salable = ddvc_item.get("is_salable")
                    stock_status = ddvc_item.get("stock_status")
                    ddvc_price_raw = ddvc_item.get("final_price")
                    ddvc_price: Optional[float]
                    if ddvc_price_raw is None:
                        ddvc_price = None
                    else:
                        try:
                            ddvc_price = float(ddvc_price_raw)
                        except (TypeError, ValueError):
                            ddvc_price = None
                    # En DDVC el SKU está presente. Lo tratamos como disponible
                    # si CUALQUIERA de las señales lo dice: stock_status=IN_STOCK
                    # o is_salable=True. Solo cae a agotado cuando ambas señales
                    # son explícitamente negativas (OUT_OF_STOCK / False).
                    stock_status_norm = (stock_status or "").upper() if isinstance(stock_status, str) else None
                    available = stock_status_norm == "IN_STOCK" or is_salable is True
                    explicit_oos = (
                        stock_status_norm == "OUT_OF_STOCK"
                        or is_salable is False
                    )
                    if available:
                        qty_target = settings.in_stock_qty
                    elif explicit_oos:
                        qty_target = settings.out_of_stock_qty
                    else:
                        # Sin señal clara (is_salable=None y stock_status=None):
                        # el SKU existe en DDVC, asumimos disponible para no
                        # marcar como agotado por falta de información.
                        qty_target = settings.in_stock_qty
                    last_seen = dt.datetime.now(dt.timezone.utc)
                else:
                    # SKU no está en el fetch completo de DDVC.
                    # No hacer validación individual aquí — se hará en batch después.
                    # Por ahora, marcar como pendiente de validación.
                    pending_validation.append(sku_norm)
                    continue  # Saltar este SKU por ahora, se procesa en Fase 2

                desired_state[sku_norm] = {
                    "ddvc_salable": is_salable,
                    "ddvc_price": ddvc_price,
                    "target_qty": float(qty_target) if qty_target is not None else None,
                    "last_seen_ddvc_at": last_seen,
                }

                prior_state = sku_states.get(sku_norm)
                state_matches = (
                    prior_state is not None
                    and prior_state.ddvc_salable == is_salable
                    and prior_state.ddvc_price == ddvc_price
                    and prior_state.target_qty == desired_state[sku_norm]["target_qty"]
                )
                sku_status[sku_norm] = {
                    "inventory_needed": False,
                    "price_needed": False,
                    "inventory_success": True,
                    "price_success": True,
                }

                qty_needs_update = shopify_item.quantity != qty_target
                if qty_needs_update and exempt_inventory:
                    logger.info(
                        "SKU %s inventory exempt, would have changed %s -> %s but skipping",
                        sku_norm,
                        _stringify(shopify_item.quantity),
                        _stringify(qty_target),
                    )
                if qty_needs_update and not exempt_inventory:
                    action_id = db.insert_sync_action(
                        engine,
                        run_id=run_id,
                        sku_norm=sku_norm,
                        action_type="inventory",
                        old_value=_stringify(shopify_item.quantity),
                        new_value=_stringify(qty_target),
                        status="planned",
                    )
                    inventory_actions.append((action_id, sku_norm, shopify_item.inventory_item_id, qty_target))
                    inventory_updates.append((shopify_item.inventory_item_id, qty_target))
                    sku_status[sku_norm]["inventory_needed"] = True
                    sku_status[sku_norm]["inventory_success"] = False
                    if ddvc_item is not None and qty_target == settings.out_of_stock_qty:
                        reason = f"is_salable={is_salable!r} stock_status={stock_status!r}"
                        logger.info(
                            "SKU %s inventory %s -> %s (reason=%s)",
                            sku_norm,
                            _stringify(shopify_item.quantity),
                            _stringify(qty_target),
                            reason,
                        )
                    else:
                        logger.info(
                            "SKU %s inventory %s -> %s",
                            sku_norm,
                            _stringify(shopify_item.quantity),
                            _stringify(qty_target),
                        )
                    planned_action = True

                if ddvc_price is not None and not exempt_price:
                    price_result = pricing_engine.calculate(sku_norm, ddvc_price)
                    target_price = price_result.final_price

                    if abs(shopify_item.price - target_price) > 0.01:
                        rule_desc = " + ".join(price_result.steps[1:]) if len(price_result.steps) > 1 else "Sin regla"
                        action_id = db.insert_sync_action(
                            engine,
                            run_id=run_id,
                            sku_norm=sku_norm,
                            action_type="price",
                            old_value=_stringify(shopify_item.price),
                            new_value=_stringify(target_price),
                            status="planned",
                        )
                        price_actions.append((action_id, sku_norm, shopify_item.variant_id, target_price))
                        price_updates.append((shopify_item.product_id, shopify_item.variant_id, target_price))
                        sku_status[sku_norm]["price_needed"] = True
                        sku_status[sku_norm]["price_success"] = False
                        logger.info(
                            "SKU %s price %s -> %s (ddvc=%s, steps: %s)",
                            sku_norm,
                            _stringify(shopify_item.price),
                            _stringify(target_price),
                            _stringify(ddvc_price),
                            " | ".join(price_result.steps),
                        )
                        # Log to price_change_log for the panel
                        log_price_change(
                            engine,
                            sku=sku_norm,
                            ddvc_price=ddvc_price,
                            rule_applied=rule_desc,
                            price_before=shopify_item.price,
                            price_after=target_price,
                            was_applied=False,  # Se marca True después de aplicar
                        )
                        planned_action = True
                elif ddvc_price is not None and exempt_price:
                    logger.debug("SKU %s price exempt, keeping current price", sku_norm)

                if not planned_action:
                    if state_matches and not qty_needs_update:
                        skipped_count += 1
                    else:
                        skipped_count += 1

            # ── FASE 2: Batch validation de SKUs no encontrados en DDVC ──
            if pending_validation:
                from app.ddvc_full import validar_skus_batch

                logger.info("Starting batch validation for %s SKUs not in DDVC full fetch", len(pending_validation))

                batch_results = validar_skus_batch(
                    graphql_url=settings.ddvc_graphql,
                    skus=pending_validation,
                )

                batch_validated_count = len(pending_validation)
                batch_found_count = 0
                batch_not_found_count = 0

                for sku_norm in pending_validation:
                    shopify_item = shopify_map[sku_norm]

                    # Check exemptions
                    exemption = sku_exemptions.get(sku_norm, {})
                    exempt_inventory = exemption.get("exempt_inventory", False)
                    exempt_price = exemption.get("exempt_price", False)

                    batch_item = batch_results.get(sku_norm)

                    if batch_item:
                        # Encontrado en batch — procesar normalmente
                        batch_found_count += 1
                        found_count += 1
                        is_salable = batch_item.get("is_salable")
                        stock_status = batch_item.get("stock_status")
                        ddvc_price_raw = batch_item.get("final_price")
                        ddvc_price = None
                        if ddvc_price_raw is not None:
                            try:
                                ddvc_price = float(ddvc_price_raw)
                            except (TypeError, ValueError):
                                pass

                        # Misma lógica de disponibilidad que Fase 1
                        stock_status_norm = (stock_status or "").upper() if isinstance(stock_status, str) else None
                        available = stock_status_norm == "IN_STOCK" or is_salable is True
                        explicit_oos = stock_status_norm == "OUT_OF_STOCK" or is_salable is False

                        if available:
                            qty_target = settings.in_stock_qty
                        elif explicit_oos:
                            qty_target = settings.out_of_stock_qty
                        else:
                            qty_target = settings.in_stock_qty  # Existe en DDVC, asumir disponible

                        last_seen = dt.datetime.now(dt.timezone.utc)
                    else:
                        # No encontrado ni en batch — realmente no existe en DDVC
                        batch_not_found_count += 1
                        not_found_count += 1
                        is_salable = None
                        ddvc_price = None
                        qty_target = settings.out_of_stock_qty
                        last_seen = None

                    desired_state[sku_norm] = {
                        "ddvc_salable": is_salable,
                        "ddvc_price": ddvc_price,
                        "target_qty": float(qty_target) if qty_target is not None else None,
                        "last_seen_ddvc_at": last_seen,
                    }
                    if batch_item is None:
                        desired_state[sku_norm]["last_sync_status_override"] = "discontinued"

                    sku_status[sku_norm] = {
                        "inventory_needed": False,
                        "price_needed": False,
                        "inventory_success": True,
                        "price_success": True,
                    }

                    # Comparar inventario
                    qty_needs_update = shopify_item.quantity != qty_target
                    if qty_needs_update and not exempt_inventory:
                        action_id = db.insert_sync_action(
                            engine, run_id=run_id, sku_norm=sku_norm,
                            action_type="inventory",
                            old_value=_stringify(shopify_item.quantity),
                            new_value=_stringify(qty_target),
                            status="planned",
                        )
                        inventory_actions.append((action_id, sku_norm, shopify_item.inventory_item_id, qty_target))
                        inventory_updates.append((shopify_item.inventory_item_id, qty_target))
                        sku_status[sku_norm]["inventory_needed"] = True
                        sku_status[sku_norm]["inventory_success"] = False
                        logger.info("SKU %s (batch) inventory %s -> %s", sku_norm,
                                    _stringify(shopify_item.quantity), _stringify(qty_target))

                    # Comparar precio (solo si encontrado en batch con precio)
                    if ddvc_price is not None and not exempt_price:
                        price_result = pricing_engine.calculate(sku_norm, ddvc_price)
                        target_price = price_result.final_price
                        if abs(shopify_item.price - target_price) > 0.01:
                            rule_desc = " + ".join(price_result.steps[1:]) if len(price_result.steps) > 1 else "Sin regla"
                            action_id = db.insert_sync_action(
                                engine, run_id=run_id, sku_norm=sku_norm,
                                action_type="price",
                                old_value=_stringify(shopify_item.price),
                                new_value=_stringify(target_price),
                                status="planned",
                            )
                            price_actions.append((action_id, sku_norm, shopify_item.variant_id, target_price))
                            price_updates.append((shopify_item.product_id, shopify_item.variant_id, target_price))
                            sku_status[sku_norm]["price_needed"] = True
                            sku_status[sku_norm]["price_success"] = False
                            log_price_change(engine, sku=sku_norm, ddvc_price=ddvc_price,
                                             rule_applied=rule_desc, price_before=shopify_item.price,
                                             price_after=target_price, was_applied=False)

                logger.info(
                    "Batch validation results: pending=%s found=%s not_found=%s",
                    len(pending_validation), batch_found_count, batch_not_found_count
                )

            inventory_changes = len(inventory_actions)
            price_changes = len(price_actions)
            planned_to_zero = sum(
                1 for _, _, _, qty_target in inventory_actions if qty_target == settings.out_of_stock_qty
            )
            planned_to_in_stock = sum(
                1 for _, _, _, qty_target in inventory_actions if qty_target == settings.in_stock_qty
            )
            planned_price = len(price_actions)

            logger.info(
                "COMPARE SUMMARY shopify=%s ddvc=%s found=%s not_found=%s skipped=%s",
                shopify_rows,
                ddvc_rows,
                found_count,
                not_found_count,
                skipped_count,
            )
            logger.info(
                "PLANNED CHANGES inventory=%s price=%s dry_run=%s",
                inventory_changes,
                price_changes,
                settings.dry_run,
            )
            logger.info(
                "PLANNED TOTALS inventory_to_0=%s inventory_to_%s=%s price_changed=%s dry_run=%s",
                planned_to_zero,
                settings.in_stock_qty,
                planned_to_in_stock,
                planned_price,
                settings.dry_run,
            )

            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "applying",
                "message": f"Aplicando {inventory_changes} cambios inventario, {price_changes} cambios precio...",
                "percent": 70,
                "started_at": start.isoformat(),
                "details": {
                    "shopify_rows": len(shopify_map),
                    "ddvc_rows": ddvc_rows,
                    "found": found_count,
                    "not_found": not_found_count,
                    "inventory_changes": inventory_changes,
                    "price_changes": price_changes
                }
            }))

            if inventory_actions:
                logger.info("SAMPLE INVENTORY CHANGES:")
                for _, _, inventory_item_id, qty in inventory_actions[:MAX_SAMPLES]:
                    logger.info(" - inventory_item_id=%s -> qty=%s", inventory_item_id, qty)

            if price_actions:
                logger.info("SAMPLE PRICE CHANGES:")
                for _, _, variant_id, price in price_actions[:MAX_SAMPLES]:
                    logger.info(" - variant_id=%s -> price=%s", variant_id, price)

            logger.info("Applying Shopify updates... dry_run=%s", settings.dry_run)
            if settings.dry_run:
                logger.info("DRY_RUN enabled. Skipping Shopify updates.")
            else:
                logger.info("Applying updates...")
                inventory_results: Dict[str, Optional[str]] = {}
                price_results: Dict[str, Optional[str]] = {}
                if inventory_updates:
                    try:
                        inventory_results = shopify.update_inventory(location_id, inventory_updates)
                        for action_id, sku_norm, inventory_item_id, _ in inventory_actions:
                            error = inventory_results.get(inventory_item_id)
                            if error:
                                db.update_sync_action_status(engine, action_id, "failed", error)
                                sku_status[sku_norm]["inventory_success"] = False
                            else:
                                db.update_sync_action_status(engine, action_id, "applied")
                                sku_status[sku_norm]["inventory_success"] = True
                    except Exception as exc:
                        error = str(exc)
                        for action_id, sku_norm, _, _ in inventory_actions:
                            db.update_sync_action_status(engine, action_id, "failed", error)
                            sku_status[sku_norm]["inventory_success"] = False
                        raise
                if price_updates:
                    try:
                        price_results = shopify.update_prices(price_updates)
                        for action_id, sku_norm, variant_id, _ in price_actions:
                            error = price_results.get(variant_id)
                            if error:
                                db.update_sync_action_status(engine, action_id, "failed", error)
                                sku_status[sku_norm]["price_success"] = False
                            else:
                                db.update_sync_action_status(engine, action_id, "applied")
                                sku_status[sku_norm]["price_success"] = True
                    except Exception as exc:
                        error = str(exc)
                        for action_id, sku_norm, _, _ in price_actions:
                            db.update_sync_action_status(engine, action_id, "failed", error)
                            sku_status[sku_norm]["price_success"] = False
                        raise

                for _, _, inventory_item_id, qty_target in inventory_actions:
                    if inventory_results.get(inventory_item_id) is None:
                        if qty_target == settings.out_of_stock_qty:
                            applied_to_zero += 1
                        if qty_target == settings.in_stock_qty:
                            applied_to_in_stock += 1

                for _, _, variant_id, _ in price_actions:
                    if price_results.get(variant_id) is None:
                        applied_price_changes += 1

                # Mark price changes as applied in price_change_log
                for _, sku_norm, variant_id, target_price in price_actions:
                    if price_results.get(variant_id) is None:
                        try:
                            with engine.begin() as conn:
                                conn.execute(
                                    text(
                                        """UPDATE price_change_log
                                           SET was_applied = true
                                           WHERE sku = :sku AND price_after = :price_after
                                             AND was_applied = false
                                             AND created_at >= NOW() - INTERVAL '1 hour'"""
                                    ),
                                    {"sku": sku_norm, "price_after": target_price},
                                )
                        except Exception:
                            logger.warning("Failed to mark price_change_log as applied for %s", sku_norm)

                logger.info(
                    "APPLIED TOTALS inventory_to_0=%s inventory_to_%s=%s price_changed=%s",
                    applied_to_zero,
                    settings.in_stock_qty,
                    applied_to_in_stock,
                    applied_price_changes,
                )

                db.set_kv(engine, "sync_progress", json.dumps({
                    "run_id": run_id,
                    "phase": "saving",
                    "message": "Cambios aplicados en Shopify. Guardando estados en BD...",
                    "percent": 90,
                    "started_at": start.isoformat(),
                    "details": {
                        "shopify_rows": len(shopify_map),
                        "ddvc_rows": ddvc_rows,
                        "found": found_count,
                        "not_found": not_found_count,
                        "inventory_changes": inventory_changes,
                        "price_changes": price_changes,
                        "applied_to_zero": applied_to_zero,
                        "applied_to_in_stock": applied_to_in_stock,
                        "applied_price_changes": applied_price_changes
                    }
                }))

            for sku_norm, desired in desired_state.items():
                status = sku_status.get(sku_norm)
                if status and (
                    (status["inventory_needed"] and not status["inventory_success"])
                    or (status["price_needed"] and not status["price_success"])
                ):
                    continue
                # Skip sku_state update for exempted SKUs to preserve manual state
                exemption = sku_exemptions.get(sku_norm, {})
                if exemption.get("exempt_inventory") or exemption.get("exempt_price"):
                    continue
                ddvc_salable = desired["ddvc_salable"]
                ddvc_price = desired["ddvc_price"]
                target_qty = desired["target_qty"]
                last_seen_ddvc_at = desired["last_seen_ddvc_at"]
                db.upsert_sku_state(
                    engine,
                    sku=sku_norm,
                    ddvc_salable=ddvc_salable if isinstance(ddvc_salable, bool) or ddvc_salable is None else None,
                    ddvc_price=ddvc_price if isinstance(ddvc_price, (float, int)) or ddvc_price is None else None,
                    target_qty=target_qty if isinstance(target_qty, (float, int)) or target_qty is None else None,
                    last_seen_ddvc_at=last_seen_ddvc_at if isinstance(last_seen_ddvc_at, dt.datetime) else None,
                    last_sync_status=desired.get(
                        "last_sync_status_override",
                        "applied" if status and (status["inventory_needed"] or status["price_needed"]) else "noop",
                    ),
                )

            # ── Registrar SKUs que están en DDVC pero no en Shopify ──
            # Esto permite que el dashboard muestre "Solo en DDVC" correctamente.
            ddvc_only_count = 0
            for ddvc_sku_raw, ddvc_data in ddvc_map.items():
                ddvc_sku_norm = normalize_sku(ddvc_sku_raw)
                if not ddvc_sku_norm:
                    continue
                if ddvc_sku_norm in shopify_map:
                    continue  # Ya fue procesado en el loop principal
                ddvc_only_count += 1
                ddvc_price_raw = ddvc_data.get("final_price")
                ddvc_price_val: Optional[float] = None
                if ddvc_price_raw is not None:
                    try:
                        ddvc_price_val = float(ddvc_price_raw)
                    except (TypeError, ValueError):
                        pass
                is_salable = ddvc_data.get("is_salable")
                db.upsert_sku_state(
                    engine,
                    sku=ddvc_sku_norm,
                    ddvc_salable=bool(is_salable) if is_salable is not None else None,
                    ddvc_price=ddvc_price_val,
                    target_qty=None,
                    last_seen_ddvc_at=dt.datetime.now(dt.timezone.utc),
                    last_sync_status="ddvc_only",
                )
            if ddvc_only_count > 0:
                logger.info("Registered %s DDVC-only SKUs in sku_state", ddvc_only_count)

            # ── FASE 3: Distribuir a tiendas secundarias ──
            try:
                _distribute_to_shops(engine, ddvc_map, settings)
            except Exception as exc:
                logger.error("Shop distribution failed: %s", exc, exc_info=True)
                # No hacer raise — la sync de Libertad ya terminó OK
        except DDVCFetchIntegrityError:
            raise
        except Exception as exc:
            error_message = str(exc)
            raise
        finally:
            finished_at = dt.datetime.now(dt.timezone.utc)
            if run_inserted:
                db.update_sync_run(
                    engine,
                    run_id=run_id,
                    finished_at=finished_at,
                    found_count=found_count,
                    not_found_count=not_found_count,
                    inventory_changes=inventory_changes,
                    price_changes=price_changes,
                    ddvc_rows=ddvc_rows,
                    shopify_rows=shopify_rows,
                    error=error_message,
                    batch_validated=batch_validated_count,
                    batch_confirmed=batch_found_count,
                    batch_not_found=batch_not_found_count,
                )
            db.set_kv(engine, "sync_progress", json.dumps({
                "run_id": run_id,
                "phase": "done",
                "message": "Sincronización completada.",
                "percent": 100,
                "started_at": start.isoformat(),
                "finished_at": finished_at.isoformat(),
                "details": {
                    "shopify_rows": shopify_rows,
                    "ddvc_rows": ddvc_rows,
                    "error": error_message
                }
            }))
            # Trim history tables to keep DB lean (max 100K each)
            try:
                deleted_actions = db.trim_table_to_max_rows(engine, "sync_actions", 100000)
                deleted_prices = db.trim_table_to_max_rows(engine, "price_change_log", 100000)
                if deleted_actions > 0 or deleted_prices > 0:
                    logger.info(
                        "DB trim: deleted %s sync_actions, %s price_change_log (cap=100K)",
                        deleted_actions,
                        deleted_prices,
                    )
            except Exception:
                logger.warning("Failed to trim history tables", exc_info=True)
            db.release_lock(lock_conn)

    duration = (dt.datetime.now(dt.timezone.utc) - start).total_seconds()
    logger.info("Sync completed in %.2fs", duration)
