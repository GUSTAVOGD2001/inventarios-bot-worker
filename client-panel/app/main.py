import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .db import close_pool, create_pool, run_migrations
from .routers import bridge, health, inventory, prices, pricing, settings as settings_router, skus, stats, worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_pool()
    await run_migrations()
    yield
    await close_pool()


app = FastAPI(title="Client Panel API", version="1.0.0", lifespan=lifespan)
origins = [o.strip() for o in settings.cors_origins.split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["X-API-Key", "X-Bridge-Key", "Content-Type"],
)

app.include_router(bridge.router, prefix="/api/v1", tags=["bridge"])
app.include_router(pricing.router, prefix="/api/v1", tags=["pricing"])
app.include_router(settings_router.router, prefix="/api/v1", tags=["settings"])
app.include_router(stats.router, prefix="/api/v1", tags=["stats"])
app.include_router(skus.router, prefix="/api/v1", tags=["skus"])
app.include_router(inventory.router, prefix="/api/v1", tags=["inventory"])
app.include_router(prices.router, prefix="/api/v1", tags=["prices"])
app.include_router(worker.router, prefix="/api/v1", tags=["worker"])
app.include_router(health.router, prefix="/api/v1", tags=["health"])


@app.get("/health")
async def health_ping():
    return {"status": "ok"}
