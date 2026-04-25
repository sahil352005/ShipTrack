-- ============================================================
-- Real-Time Logistics & Supply Chain Analytics Platform
-- PostgreSQL Schema
-- ============================================================

-- Raw shipments table (stores every processed event)
CREATE TABLE IF NOT EXISTS shipments (
    id                      SERIAL PRIMARY KEY,
    shipment_id             VARCHAR(20)     NOT NULL,
    vehicle_id              VARCHAR(20)     NOT NULL,
    warehouse_id            VARCHAR(20),
    region                  VARCHAR(50),
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,
    status                  VARCHAR(20)     NOT NULL,
    weight_kg               NUMERIC(10, 2),
    distance_km             NUMERIC(10, 2),
    is_delayed              BOOLEAN         DEFAULT FALSE,
    created_at              TIMESTAMPTZ,
    estimated_delivery_hours INTEGER,
    actual_delivery_hours   INTEGER,
    event_timestamp         TIMESTAMPTZ,
    processed_at            TIMESTAMPTZ     DEFAULT NOW()
);

-- Delivery metrics table (aggregated per shipment)
CREATE TABLE IF NOT EXISTS delivery_metrics (
    id                      SERIAL PRIMARY KEY,
    shipment_id             VARCHAR(20)     NOT NULL UNIQUE,
    region                  VARCHAR(50),
    status                  VARCHAR(20),
    estimated_delivery_hours INTEGER,
    actual_delivery_hours   INTEGER,
    delay_hours             NUMERIC(10, 2),
    is_delayed              BOOLEAN         DEFAULT FALSE,
    on_time                 BOOLEAN         DEFAULT TRUE,
    processed_at            TIMESTAMPTZ     DEFAULT NOW()
);

-- Route performance table (aggregated per region per window)
CREATE TABLE IF NOT EXISTS route_performance (
    id                      SERIAL PRIMARY KEY,
    region                  VARCHAR(50)     NOT NULL,
    window_start            TIMESTAMPTZ,
    window_end              TIMESTAMPTZ,
    total_shipments         INTEGER         DEFAULT 0,
    delayed_shipments       INTEGER         DEFAULT 0,
    delivered_shipments     INTEGER         DEFAULT 0,
    avg_delivery_hours      NUMERIC(10, 2),
    avg_distance_km         NUMERIC(10, 2),
    on_time_rate            NUMERIC(5, 2),
    processed_at            TIMESTAMPTZ     DEFAULT NOW()
);

-- Indexes for Grafana query performance
CREATE INDEX IF NOT EXISTS idx_shipments_status        ON shipments(status);
CREATE INDEX IF NOT EXISTS idx_shipments_region        ON shipments(region);
CREATE INDEX IF NOT EXISTS idx_shipments_timestamp     ON shipments(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_shipments_is_delayed    ON shipments(is_delayed);
CREATE INDEX IF NOT EXISTS idx_delivery_metrics_region ON delivery_metrics(region);
CREATE INDEX IF NOT EXISTS idx_route_perf_region       ON route_performance(region);
CREATE INDEX IF NOT EXISTS idx_route_perf_window       ON route_performance(window_start);

-- 4. Users Table for Authentication
CREATE TABLE IF NOT EXISTS users (
    id                      SERIAL PRIMARY KEY,
    full_name               VARCHAR(100)    NOT NULL,
    email                   VARCHAR(100)    NOT NULL UNIQUE,
    password_hash           TEXT            NOT NULL,
    role                    VARCHAR(20)     NOT NULL DEFAULT 'Analyst', -- Admin, Manager, Analyst
    is_active               BOOLEAN         DEFAULT TRUE,
    created_at              TIMESTAMPTZ     DEFAULT NOW()
);
