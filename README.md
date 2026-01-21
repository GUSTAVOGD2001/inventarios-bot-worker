# Inventarios Bot Worker

Worker en Python para sincronizar INVENTARIO y PRECIO en Shopify usando como fuente DDVC (GraphQL).

## Qué hace

- Consulta DDVC por chunks de SKUs y determina `is_salable` y `precio_final`.
- Actualiza inventario y precio en Shopify (GraphQL Admin) con batching y backoff.
- Guarda estados en Postgres para aplicar solo deltas.
- Corre en loop con un scheduler por ventana y slots fijos.

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
- `IN_STOCK_QTY=99`
- `OUT_OF_STOCK_QTY=0`
- `NOT_FOUND_QTY=0`
- `DRY_RUN=0` (usar `1` para simular sin escribir en Shopify)
- `TZ=America/Mexico_City` (zona horaria para el scheduler)
- `RUN_WINDOW_START=09:00` (inicio de la ventana de ejecución)
- `RUN_WINDOW_END=18:00` (fin de la ventana de ejecución; incluye el slot de las 18:00)
- `RUN_INTERVAL_MIN=15` (intervalo en minutos dentro de la ventana)

## Programación por ventana

- El worker ejecuta sync solo dentro de la ventana definida por `RUN_WINDOW_START` y `RUN_WINDOW_END`.
- Dentro de esa ventana, corre exactamente en los slots: 09:00, 09:15, 09:30, ... 17:45, 18:00.
- Fuera de la ventana no ejecuta sync y queda inactivo hasta el siguiente slot válido.
- Se usa la zona horaria configurada en `TZ` (por defecto America/Mexico_City) para manejar DST.

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
