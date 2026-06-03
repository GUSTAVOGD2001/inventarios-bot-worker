-- Variantes Shopify del cliente (mapa SKU → variant)
CREATE TABLE IF NOT EXISTS shopify_variants (
    sku                 VARCHAR(100) PRIMARY KEY,
    variant_id          VARCHAR(100) NOT NULL,
    product_id          VARCHAR(100) NOT NULL,
    inventory_item_id   VARCHAR(100) NOT NULL,
    title               TEXT,
    current_price       NUMERIC(12,4),
    current_qty         INT,
    updated_at          TIMESTAMPTZ DEFAULT now()
);

-- Estado de SKU (datos recibidos del worker)
CREATE TABLE IF NOT EXISTS sku_state (
    sku             VARCHAR(100) PRIMARY KEY,
    ddvc_price      NUMERIC(12,4),
    source_price    NUMERIC(12,4),
    is_salable      BOOLEAN,
    stock_status    VARCHAR(20),
    target_qty      INT,
    final_price     NUMERIC(12,4),
    last_received_at TIMESTAMPTZ,
    last_sync_status VARCHAR(20),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- Reglas de pricing del cliente
CREATE TABLE IF NOT EXISTS pricing_rules (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    rule_type   VARCHAR(20) NOT NULL CHECK (rule_type IN ('percentage', 'fixed_amount')),
    value       NUMERIC(10,2) NOT NULL,
    priority    INT NOT NULL DEFAULT 0,
    is_active   BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Overrides por SKU del cliente
CREATE TABLE IF NOT EXISTS sku_overrides (
    id              SERIAL PRIMARY KEY,
    sku             VARCHAR(100) NOT NULL UNIQUE,
    override_type   VARCHAR(20) NOT NULL CHECK (override_type IN ('fixed_price', 'percentage', 'fixed_amount')),
    value           NUMERIC(10,2) NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- Overrides por prefijo
CREATE TABLE IF NOT EXISTS sku_prefix_overrides (
    id              SERIAL PRIMARY KEY,
    sku_prefix      VARCHAR(50) NOT NULL UNIQUE,
    override_type   VARCHAR(20) NOT NULL CHECK (override_type IN ('fixed_price', 'percentage', 'fixed_amount')),
    value           NUMERIC(10,2) NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- Configuración del panel
CREATE TABLE IF NOT EXISTS panel_settings (
    key         VARCHAR(100) PRIMARY KEY,
    value       JSONB NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

INSERT INTO panel_settings (key, value) VALUES
    ('rounding_enabled', 'false'),
    ('rounding_threshold', '200'),
    ('rounding_low_mode', '"nearest_99"'),
    ('rounding_high_mode', '"ceil_x9_99"'),
    ('global_markup_enabled', 'true'),
    ('price_cap_enabled', 'false'),
    ('price_cap_max', '10000'),
    ('in_stock_qty', '100'),
    ('out_of_stock_qty', '0')
ON CONFLICT (key) DO NOTHING;

-- Historial de sync recibidos
CREATE TABLE IF NOT EXISTS sync_runs (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(20),
    source          VARCHAR(50) DEFAULT 'libertad-worker',
    started_at      TIMESTAMPTZ DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    skus_received   INT DEFAULT 0,
    inventory_changes INT DEFAULT 0,
    price_changes   INT DEFAULT 0,
    error           TEXT,
    details         JSONB
);

-- Acciones de sync
CREATE TABLE IF NOT EXISTS sync_actions (
    id          BIGSERIAL PRIMARY KEY,
    run_id      VARCHAR(20),
    sku         VARCHAR(100) NOT NULL,
    action_type VARCHAR(20) NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    status      VARCHAR(20) NOT NULL DEFAULT 'planned',
    error       TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sync_actions_sku ON sync_actions(sku);
CREATE INDEX IF NOT EXISTS idx_sync_actions_created ON sync_actions(created_at DESC);

-- Historial de cambios de precio
CREATE TABLE IF NOT EXISTS price_change_log (
    id              SERIAL PRIMARY KEY,
    sku             VARCHAR(100) NOT NULL,
    ddvc_price      NUMERIC(10,2),
    source_price    NUMERIC(10,2),
    rule_applied    VARCHAR(200),
    price_before    NUMERIC(10,2),
    price_after     NUMERIC(10,2),
    was_applied     BOOLEAN DEFAULT false,
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_price_change_log_created ON price_change_log(created_at DESC);

-- KV store
CREATE TABLE IF NOT EXISTS app_kv (
    key     VARCHAR(100) PRIMARY KEY,
    value   TEXT NOT NULL
);
