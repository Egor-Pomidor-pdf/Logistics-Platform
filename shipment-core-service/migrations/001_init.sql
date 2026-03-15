CREATE TABLE IF NOT EXISTS shipment_history (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    shipment_id UUID NOT NULL,
    status_code TEXT NOT NULL,
    status_ts TIMESTAMPTZ NOT NULL,
    source_system TEXT NOT NULL,
    location_code TEXT NULL,
    partner_id TEXT NULL,
    route_id TEXT NULL,
    payload JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_shipment_history_shipment_status_ts
    ON shipment_history (shipment_id, status_ts DESC);
CREATE INDEX IF NOT EXISTS idx_shipment_history_status_code
    ON shipment_history (status_code);
CREATE INDEX IF NOT EXISTS idx_shipment_history_source_system
    ON shipment_history (source_system);

CREATE TABLE IF NOT EXISTS shipment_state (
    shipment_id UUID PRIMARY KEY,
    last_event_id TEXT NOT NULL,
    status_code TEXT NOT NULL,
    status_ts TIMESTAMPTZ NOT NULL,
    source_system TEXT NOT NULL,
    location_code TEXT NULL,
    partner_id TEXT NULL,
    route_id TEXT NULL,
    payload JSONB NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_shipment_state_status_code
    ON shipment_state (status_code);
CREATE INDEX IF NOT EXISTS idx_shipment_state_route_id
    ON shipment_state (route_id);
CREATE INDEX IF NOT EXISTS idx_shipment_state_status_ts
    ON shipment_state (status_ts DESC);
