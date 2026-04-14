-- Reglas globales de precios
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

-- Precios fijos y reglas por SKU
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

-- Configuración del panel
CREATE TABLE IF NOT EXISTS panel_settings (
    key         VARCHAR(100) PRIMARY KEY,
    value       JSONB NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

INSERT INTO panel_settings (key, value) VALUES
    ('rounding_enabled', 'true'),
    ('rounding_target', '"X9.99"'),
    ('dry_run', 'true'),
    ('global_markup_enabled', 'true'),
    ('rounding_threshold', '200'),
    ('rounding_low_mode', '"nearest_99"'),
    ('rounding_high_mode', '"ceil_x9_99"'),
    ('price_cap_enabled', 'true'),
    ('price_cap_max', '10000')
ON CONFLICT (key) DO NOTHING;

-- Historial de cambios de precio
CREATE TABLE IF NOT EXISTS price_change_log (
    id              SERIAL PRIMARY KEY,
    sku             VARCHAR(100) NOT NULL,
    ddvc_price      NUMERIC(10,2),
    rule_applied    VARCHAR(200),
    price_before    NUMERIC(10,2),
    price_after     NUMERIC(10,2),
    was_applied     BOOLEAN DEFAULT false,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_price_change_log_sku ON price_change_log(sku);
CREATE INDEX IF NOT EXISTS idx_price_change_log_created ON price_change_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sku_overrides_sku ON sku_overrides(sku);

-- Exenciones de SKU (no sincronizar inventario y/o precio)
CREATE TABLE IF NOT EXISTS sku_exemptions (
    id                  SERIAL PRIMARY KEY,
    sku                 VARCHAR(100) NOT NULL UNIQUE,
    exempt_inventory    BOOLEAN NOT NULL DEFAULT false,
    exempt_price        BOOLEAN NOT NULL DEFAULT false,
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sku_exemptions_sku ON sku_exemptions(sku);
