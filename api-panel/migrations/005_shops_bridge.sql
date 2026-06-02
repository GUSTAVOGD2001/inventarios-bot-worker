-- Campos nuevos para comunicación con paneles de clientes
DO $$ BEGIN
    ALTER TABLE shops ADD COLUMN IF NOT EXISTS api_panel_url VARCHAR(500);
    ALTER TABLE shops ADD COLUMN IF NOT EXISTS bridge_api_key VARCHAR(200);
    ALTER TABLE shops ADD COLUMN IF NOT EXISTS price_mode VARCHAR(20) DEFAULT 'raw_ddvc';
    ALTER TABLE shops ADD COLUMN IF NOT EXISTS last_sync_status VARCHAR(20);
    ALTER TABLE shops ADD COLUMN IF NOT EXISTS last_sync_error TEXT;
    ALTER TABLE shops ADD COLUMN IF NOT EXISTS last_sync_details JSONB;
EXCEPTION WHEN others THEN NULL;
END $$;

-- price_mode: 'raw_ddvc' = precio directo de DDVC, 'with_my_markup' = precio ya con markup de Libertad
COMMENT ON COLUMN shops.price_mode IS 'raw_ddvc = precio DDVC crudo, with_my_markup = precio con markup de Libertad aplicado';
