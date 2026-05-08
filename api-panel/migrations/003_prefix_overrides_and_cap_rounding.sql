-- Overrides por prefijo de SKU
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

CREATE INDEX IF NOT EXISTS idx_sku_prefix_overrides_prefix ON sku_prefix_overrides(sku_prefix);

-- Settings para redondeo del price cap
INSERT INTO panel_settings (key, value) VALUES
    ('price_cap_rounding_enabled', 'false'),
    ('price_cap_rounding_discount', '0.10')
ON CONFLICT (key) DO NOTHING;
