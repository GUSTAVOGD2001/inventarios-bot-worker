from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import get_pool
from ..error_handler import log_endpoint_errors
from ..models import SkuExemptionCreate, SkuExemptionUpdate

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/exemptions")
@log_endpoint_errors
async def list_exemptions(
    sku: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    pool = await get_pool()
    conditions = []
    params: list = []
    idx = 1
    if sku:
        conditions.append(f"sku ILIKE ${idx}")
        params.append(f"%{sku.upper()}%")
        idx += 1
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    total = await pool.fetchval(f"SELECT COUNT(*) FROM sku_exemptions {where}", *params)
    offset = (page - 1) * per_page
    params.extend([per_page, offset])
    rows = await pool.fetch(
        f"SELECT * FROM sku_exemptions {where} ORDER BY sku LIMIT ${idx} OFFSET ${idx + 1}",
        *params,
    )
    return {"total": total, "page": page, "per_page": per_page, "items": [dict(r) for r in rows]}


@router.post("/exemptions", status_code=201)
@log_endpoint_errors
async def create_exemption(body: SkuExemptionCreate):
    pool = await get_pool()
    sku_norm = body.sku.strip().upper()
    if not body.exempt_inventory and not body.exempt_price:
        raise HTTPException(status_code=422, detail="Al menos uno de exempt_inventory o exempt_price debe ser true")
    try:
        row = await pool.fetchrow(
            "INSERT INTO sku_exemptions (sku, exempt_inventory, exempt_price, notes) VALUES ($1, $2, $3, $4) RETURNING *",
            sku_norm, body.exempt_inventory, body.exempt_price, body.notes,
        )
    except Exception as e:
        if "unique" in str(e).lower():
            raise HTTPException(status_code=409, detail=f"Ya existe una exención para '{sku_norm}'")
        raise
    return dict(row)


@router.put("/exemptions/{exemption_id}")
@log_endpoint_errors
async def update_exemption(exemption_id: int, body: SkuExemptionUpdate):
    pool = await get_pool()
    existing = await pool.fetchrow("SELECT * FROM sku_exemptions WHERE id = $1", exemption_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Exención no encontrada")
    fields = []
    params = []
    idx = 1
    for field_name in ("exempt_inventory", "exempt_price", "notes"):
        val = getattr(body, field_name)
        if val is not None:
            fields.append(f"{field_name} = ${idx}")
            params.append(val)
            idx += 1
    if not fields:
        return dict(existing)
    fields.append("updated_at = now()")
    params.append(exemption_id)
    sql = f"UPDATE sku_exemptions SET {', '.join(fields)} WHERE id = ${idx} RETURNING *"
    row = await pool.fetchrow(sql, *params)
    return dict(row)


@router.delete("/exemptions/{exemption_id}", status_code=204)
@log_endpoint_errors
async def delete_exemption(exemption_id: int):
    pool = await get_pool()
    result = await pool.execute("DELETE FROM sku_exemptions WHERE id = $1", exemption_id)
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Exención no encontrada")
