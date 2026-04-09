-- Alertas del worker para health checks
CREATE TABLE IF NOT EXISTS worker_alerts (
    id              BIGSERIAL PRIMARY KEY,
    alert_type      VARCHAR(60) NOT NULL,
    severity        VARCHAR(20) NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    title           VARCHAR(200) NOT NULL,
    message         TEXT NOT NULL,
    metadata        JSONB,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by VARCHAR(100),
    resolved_at     TIMESTAMPTZ,
    dedupe_key      VARCHAR(200) UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_worker_alerts_detected ON worker_alerts(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_worker_alerts_unresolved ON worker_alerts(resolved_at) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_worker_alerts_severity ON worker_alerts(severity, detected_at DESC) WHERE resolved_at IS NULL;
