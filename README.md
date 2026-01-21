# Inventarios Bot Worker

Worker en Python para sincronizar INVENTARIO y PRECIO en Shopify usando como fuente DDVC (GraphQL).

## Qué hace

- Consulta DDVC por chunks de SKUs y determina `is_salable` y `precio_final`.
- Actualiza inventario y precio en Shopify (GraphQL Admin) con batching y backoff.
- Guarda estados en Postgres para aplicar solo deltas.
- Corre en loop con intervalo configurable.

## Variables de entorno

**Obligatorias** (configúralas en EasyPanel):

- `SHOPIFY_SHOP` (ej. `mi-tienda.myshopify.com`)
- `SHOPIFY_CLIENT_ID`
- `SHOPIFY_CLIENT_SECRET`
- `DATABASE_URL` (cadena Postgres; la base de datos no debe exponerse públicamente)

**Opcionales**:

- `SHOPIFY_API_VERSION=2026-01`
- `DDVC_GRAPHQL=https://tiendaddvc.mx/graphql`
- `CHUNK_SIZE=150`
- `CONCURRENCY=3`
- `SYNC_INTERVAL_MIN=15`
- `IN_STOCK_QTY=99`
- `OUT_OF_STOCK_QTY=0`
- `NOT_FOUND_QTY=0`
- `DRY_RUN=0` (usar `1` para simular sin escribir en Shopify)

## Despliegue en EasyPanel

1. Crea un proyecto Docker desde este repositorio.
2. Configura las variables de entorno anteriores.
3. Conecta un Postgres interno (no expongas el puerto públicamente) y define `DATABASE_URL`.
4. Despliega. El contenedor ejecutará `python -m app.main`.

## Cómo funciona el primer sync

- En la primera corrida se construye el mapa de variantes Shopify (SKU -> variantId, inventoryItemId).
- Se guardan estados por SKU en Postgres.
- Las siguientes corridas solo envían cambios (delta updates).

## Logs

- Se imprimen a stdout en formato legible.
- Cada corrida tiene un `run_id` corto.
- Si `DRY_RUN=1`, solo se loguean los cambios planeados.
