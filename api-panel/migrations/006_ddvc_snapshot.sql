-- Snapshot manual de DDVC para fallback cuando el origen GraphQL falla
-- Guardado por worker_manual_con_snapshot.py o por el endpoint POST /api/v1/ddvc-snapshot
CREATE TABLE IF NOT EXISTS ddvc_snapshot (
    id          BIGSERIAL PRIMARY KEY,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source      VARCHAR(50)  NOT NULL DEFAULT 'manual',
    sku_count   INT          NOT NULL DEFAULT 0,
    payload     JSONB        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ddvc_snapshot_uploaded ON ddvc_snapshot(uploaded_at DESC);

-- Mantiene solo los últimos 5 snapshots automáticamente
CREATE OR REPLACE FUNCTION trim_ddvc_snapshot() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM ddvc_snapshot
    WHERE id IN (
        SELECT id FROM ddvc_snapshot
        ORDER BY uploaded_at DESC
        OFFSET 5
    );
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_trim_ddvc_snapshot ON ddvc_snapshot;
CREATE TRIGGER trg_trim_ddvc_snapshot
    AFTER INSERT ON ddvc_snapshot
    FOR EACH ROW EXECUTE FUNCTION trim_ddvc_snapshot();
