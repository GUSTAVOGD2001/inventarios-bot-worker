from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    panel_api_key: str = "changeme"          # Para el dashboard Lovable del cliente
    bridge_api_key: str = "bridge-changeme"  # Para el worker de Libertad
    cors_origins: str = "*"
    shopify_shop: str = ""
    shopify_client_id: str = ""
    shopify_client_secret: str = ""
    shopify_api_version: str = "2026-01"
    in_stock_qty: int = 100
    out_of_stock_qty: int = 0

    class Config:
        env_file = ".env"


settings = Settings()


def get_asyncpg_params() -> dict:
    """Parse DATABASE_URL into asyncpg-compatible connection parameters."""
    url = settings.database_url
    for prefix in ("postgresql+asyncpg://", "postgresql://", "postgres://"):
        if url.startswith(prefix):
            url = url[len(prefix):]
            break

    auth_host, database = url.rsplit("/", 1)
    if "?" in database:
        database = database.split("?")[0]

    if "@" in auth_host:
        auth, host_part = auth_host.rsplit("@", 1)
    else:
        auth, host_part = "", auth_host

    user, password = (auth.split(":", 1) if ":" in auth else (auth, "")) if auth else ("", "")

    if ":" in host_part:
        host, port_str = host_part.rsplit(":", 1)
        port = int(port_str)
    else:
        host = host_part
        port = 5432

    params: dict = {"host": host, "port": port, "database": database}
    if user:
        params["user"] = user
    if password:
        params["password"] = password
    return params
