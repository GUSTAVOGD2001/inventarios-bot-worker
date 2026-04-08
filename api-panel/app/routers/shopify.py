from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..error_handler import log_endpoint_errors
from ..shopify_client import shopify_client

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/shopify/search")
@log_endpoint_errors
async def search_shopify_products(
    q: str = Query(..., min_length=2, description="Búsqueda por nombre o SKU"),
    limit: int = Query(20, ge=1, le=50),
):
    """Busca productos en Shopify en vivo. NO usa caché.

    Retorna productos con su título, precio actual, stock, etc.
    Útil para el buscador del dashboard donde el usuario quiere
    encontrar un producto y aplicarle un precio fijo.
    """
    if not shopify_client.is_configured():
        raise HTTPException(
            status_code=503,
            detail="Shopify no está configurado en el api-panel. Faltan SHOPIFY_SHOP, SHOPIFY_CLIENT_ID o SHOPIFY_CLIENT_SECRET en las variables de entorno."
        )

    products = await shopify_client.search_products(q, limit)
    return {
        "query": q,
        "count": len(products),
        "items": products,
    }
