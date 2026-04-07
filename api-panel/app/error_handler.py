import logging
import traceback
from functools import wraps

from fastapi import HTTPException

logger = logging.getLogger(__name__)


def log_endpoint_errors(func):
    """Decorador que captura y loggea cualquier error en un endpoint."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPException:
            raise
        except Exception as e:
            logger.error("Error in %s: %s", func.__name__, str(e))
            logger.error("Traceback: %s", traceback.format_exc())
            raise HTTPException(
                status_code=500,
                detail=f"Error en {func.__name__}: {type(e).__name__}: {str(e)}",
            )
    return wrapper
