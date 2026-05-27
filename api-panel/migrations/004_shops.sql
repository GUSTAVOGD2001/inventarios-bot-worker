-- Tabla de tiendas para multi-store
CREATE TABLE IF NOT EXISTS shops (
    id                    SERIAL PRIMARY KEY,
    name                  VARCHAR(200) NOT NULL,
    slug                  VARCHAR(50) NOT NULL UNIQUE,
    shopify_shop          VARCHAR(200) NOT NULL,
    shopify_client_id     VARCHAR(200) NOT NULL,
    shopify_client_secret VARCHAR(200) NOT NULL,
    shopify_api_version   VARCHAR(20) DEFAULT '2026-01',
    in_stock_qty          INT DEFAULT 100,
    out_of_stock_qty      INT DEFAULT 0,
    is_active             BOOLEAN DEFAULT true,
    is_primary            BOOLEAN DEFAULT false,
    last_sync_at          TIMESTAMPTZ,
    notes                 TEXT,
    created_at            TIMESTAMPTZ DEFAULT now(),
    updated_at            TIMESTAMPTZ DEFAULT now()
);

-- Insertar Libertad como tienda primaria (credenciales placeholder, se editan desde el panel)
INSERT INTO shops (name, slug, shopify_shop, shopify_client_id, shopify_client_secret, is_primary, is_active)
VALUES ('Depósito Dental Libertad', 'libertad', 'placeholder', 'placeholder', 'placeholder', true, true)
ON CONFLICT (slug) DO NOTHING;
