from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine


metadata = MetaData()

shopify_variants = Table(
    "shopify_variants",
    metadata,
    Column("sku", String, primary_key=True),
    Column("variant_id", String, nullable=False),
    Column("inventory_item_id", String, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

sku_state = Table(
    "sku_state",
    metadata,
    Column("sku", String, primary_key=True),
    Column("ddvc_salable", Boolean, nullable=True),
    Column("ddvc_price", Numeric(12, 4), nullable=True),
    Column("target_qty", Numeric(12, 2), nullable=True),
    Column("last_seen_ddvc_at", DateTime(timezone=True), nullable=True),
    Column("last_sync_status", String, nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

app_kv = Table(
    "app_kv",
    metadata,
    Column("key", String, primary_key=True),
    Column("value", String, nullable=False),
)


@dataclass(frozen=True)
class VariantInfo:
    variant_id: str
    inventory_item_id: str


@dataclass(frozen=True)
class SkuState:
    sku: str
    ddvc_salable: Optional[bool]
    ddvc_price: Optional[float]
    target_qty: Optional[float]
    last_seen_ddvc_at: Optional[dt.datetime]
    last_sync_status: Optional[str]
    updated_at: dt.datetime


def get_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True, future=True)


def init_db(engine: Engine) -> None:
    metadata.create_all(engine)


def get_kv(engine: Engine, key: str) -> Optional[str]:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT value FROM app_kv WHERE key = :key"), {"key": key}).fetchone()
        return row[0] if row else None


def set_kv(engine: Engine, key: str, value: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO app_kv (key, value)
                VALUES (:key, :value)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """
            ),
            {"key": key, "value": value},
        )


def load_variant_map(engine: Engine) -> Dict[str, VariantInfo]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT sku, variant_id, inventory_item_id FROM shopify_variants")).fetchall()
        return {row[0]: VariantInfo(variant_id=row[1], inventory_item_id=row[2]) for row in rows}


def upsert_variant_map(engine: Engine, entries: Iterable[tuple[str, VariantInfo]]) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    with engine.begin() as conn:
        for sku, info in entries:
            conn.execute(
                text(
                    """
                    INSERT INTO shopify_variants (sku, variant_id, inventory_item_id, updated_at)
                    VALUES (:sku, :variant_id, :inventory_item_id, :updated_at)
                    ON CONFLICT (sku) DO UPDATE
                    SET variant_id = EXCLUDED.variant_id,
                        inventory_item_id = EXCLUDED.inventory_item_id,
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                {
                    "sku": sku,
                    "variant_id": info.variant_id,
                    "inventory_item_id": info.inventory_item_id,
                    "updated_at": now,
                },
            )


def load_sku_states(engine: Engine) -> Dict[str, SkuState]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT sku, ddvc_salable, ddvc_price, target_qty, last_seen_ddvc_at, last_sync_status, updated_at
                FROM sku_state
                """
            )
        ).fetchall()
    return {
        row[0]: SkuState(
            sku=row[0],
            ddvc_salable=row[1],
            ddvc_price=float(row[2]) if row[2] is not None else None,
            target_qty=float(row[3]) if row[3] is not None else None,
            last_seen_ddvc_at=row[4],
            last_sync_status=row[5],
            updated_at=row[6],
        )
        for row in rows
    }


def upsert_sku_state(
    engine: Engine,
    sku: str,
    ddvc_salable: Optional[bool],
    ddvc_price: Optional[float],
    target_qty: Optional[float],
    last_seen_ddvc_at: Optional[dt.datetime],
    last_sync_status: Optional[str],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO sku_state (sku, ddvc_salable, ddvc_price, target_qty, last_seen_ddvc_at, last_sync_status, updated_at)
                VALUES (:sku, :ddvc_salable, :ddvc_price, :target_qty, :last_seen_ddvc_at, :last_sync_status, :updated_at)
                ON CONFLICT (sku) DO UPDATE
                SET ddvc_salable = EXCLUDED.ddvc_salable,
                    ddvc_price = EXCLUDED.ddvc_price,
                    target_qty = EXCLUDED.target_qty,
                    last_seen_ddvc_at = EXCLUDED.last_seen_ddvc_at,
                    last_sync_status = EXCLUDED.last_sync_status,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                "sku": sku,
                "ddvc_salable": ddvc_salable,
                "ddvc_price": ddvc_price,
                "target_qty": target_qty,
                "last_seen_ddvc_at": last_seen_ddvc_at,
                "last_sync_status": last_sync_status,
                "updated_at": now,
            },
        )
