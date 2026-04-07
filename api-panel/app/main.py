import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from .config import settings
from .db import close_pool, create_pool, run_migrations
from .routers import exemptions, inventory, prices, pricing, settings as settings_router, skus, stats, worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting api-panel...")
    await create_pool()
    await run_migrations()
    logger.info("api-panel ready")
    yield
    await close_pool()
    logger.info("api-panel stopped")


app = FastAPI(
    title="Inventarios Panel API",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
origins = [o.strip() for o in settings.cors_origins.split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["X-API-Key", "Content-Type"],
)

# Routers
app.include_router(inventory.router, prefix="/api/v1", tags=["inventory"])
app.include_router(prices.router, prefix="/api/v1", tags=["prices"])
app.include_router(skus.router, prefix="/api/v1", tags=["skus"])
app.include_router(pricing.router, prefix="/api/v1", tags=["pricing"])
app.include_router(settings_router.router, prefix="/api/v1", tags=["settings"])
app.include_router(worker.router, prefix="/api/v1", tags=["worker"])
app.include_router(stats.router, prefix="/api/v1", tags=["stats"])
app.include_router(exemptions.router, prefix="/api/v1", tags=["exemptions"])


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/v1/docs")
async def docs_redirect():
    return RedirectResponse(url="/docs")
