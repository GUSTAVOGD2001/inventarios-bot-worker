import logging
import pathlib

import asyncpg

from .config import get_asyncpg_params

logger = logging.getLogger(__name__)

pool: asyncpg.Pool | None = None

MIGRATIONS_DIR = pathlib.Path(__file__).resolve().parent.parent / "migrations"


async def create_pool() -> asyncpg.Pool:
    global pool
    params = get_asyncpg_params()
    pool = await asyncpg.create_pool(**params, min_size=2, max_size=10)
    logger.info("Database pool created")
    return pool


async def close_pool() -> None:
    global pool
    if pool:
        await pool.close()
        pool = None
        logger.info("Database pool closed")


async def run_migrations() -> None:
    """Execute all SQL migration files in order (idempotent)."""
    if pool is None:
        raise RuntimeError("Pool not initialized")
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    async with pool.acquire() as conn:
        for migration_file in migration_files:
            sql = migration_file.read_text()
            await conn.execute(sql)
            logger.info("Migration executed: %s", migration_file.name)
    logger.info("All migrations executed successfully")


async def get_pool() -> asyncpg.Pool:
    if pool is None:
        raise RuntimeError("Pool not initialized")
    return pool
