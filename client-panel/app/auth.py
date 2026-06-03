from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

from .config import settings

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(api_key: str | None = Security(_api_key_header)) -> str:
    if api_key is None or api_key != settings.panel_api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return api_key
