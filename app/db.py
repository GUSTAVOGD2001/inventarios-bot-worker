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
from sqlalchemy.engine import Connection, Engine


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
    if database_url.startswith("postgres://"):
        database_url = "postgresql://" + database_url[len("postgres://") :]
    return create_engine(database_url, pool_pre_ping=True, future=True)


def init_db(engine: Engine) -> None:
    metadata.create_all(engine)


def ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sync_runs (
                  run_id TEXT PRIMARY KEY,
                  slot_ts TIMESTAMP,
                  started_at TIMESTAMP NOT NULL,
                  finished_at TIMESTAMP,
                  dry_run BOOLEAN NOT NULL,
                  found_count INT DEFAULT 0,
                  not_found_count INT DEFAULT 0,
                  inventory_changes INT DEFAULT 0,
                  price_changes INT DEFAULT 0,
                  ddvc_rows INT DEFAULT 0,
                  shopify_rows INT DEFAULT 0,
                  error TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sync_actions (
                  id BIGSERIAL PRIMARY KEY,
                  run_id TEXT NOT NULL,
                  sku_norm TEXT NOT NULL,
                  action_type TEXT NOT NULL,
                  old_value TEXT,
                  new_value TEXT,
                  status TEXT NOT NULL,
                  error TEXT,
                  created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sync_actions_run ON sync_actions(run_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sync_actions_sku ON sync_actions(sku_norm)"))


def try_lock(conn: Connection) -> bool:
    result = conn.execute(text("SELECT pg_try_advisory_lock(987654321)")).scalar()
    return bool(result)


def release_lock(conn: Connection) -> None:
    conn.execute(text("SELECT pg_advisory_unlock(987654321)"))


def insert_sync_run(
    engine: Engine,
    run_id: str,
    slot_ts: dt.datetime,
    started_at: dt.datetime,
    dry_run: bool,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO sync_runs (run_id, slot_ts, started_at, dry_run)
                VALUES (:run_id, :slot_ts, :started_at, :dry_run)
                """
            ),
            {
                "run_id": run_id,
                "slot_ts": slot_ts,
                "started_at": started_at,
                "dry_run": dry_run,
            },
        )


def update_sync_run(
    engine: Engine,
    run_id: str,
    finished_at: dt.datetime,
    found_count: int,
    not_found_count: int,
    inventory_changes: int,
    price_changes: int,
    ddvc_rows: int,
    shopify_rows: int,
    error: Optional[str],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE sync_runs
                SET finished_at = :finished_at,
                    found_count = :found_count,
                    not_found_count = :not_found_count,
                    inventory_changes = :inventory_changes,
                    price_changes = :price_changes,
                    ddvc_rows = :ddvc_rows,
                    shopify_rows = :shopify_rows,
                    error = :error
                WHERE run_id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "finished_at": finished_at,
                "found_count": found_count,
                "not_found_count": not_found_count,
                "inventory_changes": inventory_changes,
                "price_changes": price_changes,
                "ddvc_rows": ddvc_rows,
                "shopify_rows": shopify_rows,
                "error": error,
            },
        )


def insert_sync_action(
    engine: Engine,
    run_id: str,
    sku_norm: str,
    action_type: str,
    old_value: Optional[str],
    new_value: Optional[str],
    status: str,
) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO sync_actions (run_id, sku_norm, action_type, old_value, new_value, status)
                VALUES (:run_id, :sku_norm, :action_type, :old_value, :new_value, :status)
                RETURNING id
                """
            ),
            {
                "run_id": run_id,
                "sku_norm": sku_norm,
                "action_type": action_type,
                "old_value": old_value,
                "new_value": new_value,
                "status": status,
            },
        )
        return int(result.scalar_one())


def update_sync_action_status(engine: Engine, action_id: int, status: str, error: Optional[str] = None) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE sync_actions
                SET status = :status,
                    error = :error
                WHERE id = :id
                """
            ),
            {"id": action_id, "status": status, "error": error},
        )


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


def trim_table_to_max_rows(engine: Engine, table_name: str, max_rows: int = 100000) -> int:
    """
    Mantiene una tabla con como máximo max_rows registros.
    Si excede, elimina los más viejos por created_at.
    Retorna la cantidad de registros eliminados.

    Solo permite tablas pre-aprobadas para evitar SQL injection.
    """
    ALLOWED_TABLES = {"sync_actions", "price_change_log"}
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Table {table_name} not allowed for trimming")

    with engine.begin() as conn:
        # Contar filas actuales
        total = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0
        if total <= max_rows:
            return 0

        # Calcular cuántas filas eliminar
        to_delete = total - max_rows

        # Eliminar las más viejas usando subquery
        result = conn.execute(
            text(
                f"""
                DELETE FROM {table_name}
                WHERE id IN (
                    SELECT id FROM {table_name}
                    ORDER BY created_at ASC
                    LIMIT :limit
                )
                """
            ),
            {"limit": to_delete},
        )
        return result.rowcount or 0
