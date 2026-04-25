-- ============================================================
-- Smart Logistics Intelligence Platform
-- SQL Schema Migration - Version 2.0
-- ============================================================

-- ============================================================
-- EXISTING TABLES (already in schema.sql)
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

-- ============================================================
-- NEW TABLES FOR INTELLIGENCE PLATFORM
-- ============================================================

-- 1. Shipment Risk Assessment Table
CREATE TABLE IF NOT EXISTS shipment_risk (
    id                      SERIAL PRIMARY KEY,
    shipment_id             VARCHAR(20)     NOT NULL,
    risk_score              INTEGER         DEFAULT 0,
    risk_level              VARCHAR(20)     DEFAULT 'Low',
    reason                  TEXT,
    rules_triggered         TEXT[],
    created_at              TIMESTAMPTZ     DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     DEFAULT NOW(),
    UNIQUE(shipment_id)
);

-- 2. Warehouse Health Table
CREATE TABLE IF NOT EXISTS warehouse_health (
    id                      SERIAL PRIMARY KEY,
    warehouse_id            VARCHAR(20)     NOT NULL UNIQUE,
    bottleneck              BOOLEAN         DEFAULT FALSE,
    severity                VARCHAR(20)     DEFAULT 'None',
    reason                  TEXT,
    avg_processing_hours    NUMERIC(10, 2),
    delay_rate              NUMERIC(5, 2),
    total_shipments         INTEGER         DEFAULT 0,
    capacity                INTEGER         DEFAULT 100,
    volume_ratio            NUMERIC(5, 2),
    last_checked            TIMESTAMPTZ     DEFAULT NOW()
);

-- 3. Alerts Table
CREATE TABLE IF NOT EXISTS alerts (
    id                      SERIAL PRIMARY KEY,
    type                    VARCHAR(50)     NOT NULL,
    severity                VARCHAR(20)     NOT NULL,
    message                 TEXT,
    shipment_id             VARCHAR(20),
    warehouse_id            VARCHAR(20),
    region                  VARCHAR(50),
    created_at              TIMESTAMPTZ     DEFAULT NOW(),
    resolved                BOOLEAN         DEFAULT FALSE,
    resolved_at             TIMESTAMPTZ
);

-- 4. Region Performance Table
CREATE TABLE IF NOT EXISTS region_performance (
    id                      SERIAL PRIMARY KEY,
    region                  VARCHAR(50)     NOT NULL,
    total_shipments         INTEGER         DEFAULT 0,
    delivered               INTEGER         DEFAULT 0,
    delayed                 INTEGER         DEFAULT 0,
    in_transit              INTEGER         DEFAULT 0,
    on_time_rate            NUMERIC(5, 2),
    avg_delivery_hours      NUMERIC(10, 2),
    trend                   VARCHAR(20)     DEFAULT 'stable',
    calculated_at           TIMESTAMPTZ     DEFAULT NOW()
);

-- 5. SLA Violations Table
CREATE TABLE IF NOT EXISTS sla_violations (
    id                      SERIAL PRIMARY KEY,
    shipment_id             VARCHAR(20)     NOT NULL,
    warehouse_id            VARCHAR(20),
    region                  VARCHAR(50),
    estimated_hours         INTEGER,
    actual_hours            INTEGER,
    violation_hours         NUMERIC(10, 2),
    severity                VARCHAR(20),
    created_at              TIMESTAMPTZ     DEFAULT NOW()
);

-- 6. Executive Summary Cache Table
CREATE TABLE IF NOT EXISTS executive_summary (
    id                      SERIAL PRIMARY KEY,
    total_shipments         INTEGER,
    delayed_shipments       INTEGER,
    on_time_rate            NUMERIC(5, 2),
    critical_risk_shipments INTEGER,
    bottleneck_warehouses   INTEGER,
    worst_region            VARCHAR(50),
    top_warehouse           VARCHAR(20),
    sla_violations          INTEGER,
    active_alerts           INTEGER,
    calculated_at           TIMESTAMPTZ     DEFAULT NOW()
);

-- 7. Users Table for Authentication
CREATE TABLE IF NOT EXISTS users (
    id                      SERIAL PRIMARY KEY,
    full_name               VARCHAR(100)    NOT NULL,
    email                   VARCHAR(100)    NOT NULL UNIQUE,
    password_hash           TEXT            NOT NULL,
    role                    VARCHAR(20)     NOT NULL DEFAULT 'Analyst', -- Admin, Manager, Analyst
    is_active               BOOLEAN         DEFAULT TRUE,
    created_at              TIMESTAMPTZ     DEFAULT NOW()
);

-- ============================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================

-- Existing indexes
CREATE INDEX IF NOT EXISTS idx_shipments_status        ON shipments(status);
CREATE INDEX IF NOT EXISTS idx_shipments_region        ON shipments(region);
CREATE INDEX IF NOT EXISTS idx_shipments_timestamp     ON shipments(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_shipments_is_delayed    ON shipments(is_delayed);
CREATE INDEX IF NOT EXISTS idx_delivery_metrics_region ON delivery_metrics(region);
CREATE INDEX IF NOT EXISTS idx_route_perf_region       ON route_performance(region);
CREATE INDEX IF NOT EXISTS idx_route_perf_window       ON route_performance(window_start);

-- New indexes for intelligence queries
CREATE INDEX IF NOT EXISTS idx_shipment_risk_level     ON shipment_risk(risk_level);
CREATE INDEX IF NOT EXISTS idx_shipment_risk_score     ON shipment_risk(risk_score);
CREATE INDEX IF NOT EXISTS idx_warehouse_health_bottleneck ON warehouse_health(bottleneck);
CREATE INDEX IF NOT EXISTS idx_alerts_type_severity    ON alerts(type, severity);
CREATE INDEX IF NOT EXISTS idx_alerts_resolved         ON alerts(resolved);
CREATE INDEX IF NOT EXISTS idx_sla_violations_warehouse ON sla_violations(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_sla_violations_region   ON sla_violations(region);

-- ============================================================
-- VIEWS FOR DASHBOARDS
-- ============================================================

-- View: Current Risk Distribution
CREATE OR REPLACE VIEW v_risk_distribution AS
SELECT 
    risk_level,
    COUNT(*) as count
FROM shipment_risk
GROUP BY risk_level;

-- View: Warehouse Performance Ranking
CREATE OR REPLACE VIEW v_warehouse_ranking AS
SELECT 
    warehouse_id,
    total_shipments,
    delay_rate,
    CASE 
        WHEN delay_rate > 20 THEN 'Critical'
        WHEN delay_rate > 15 THEN 'High'
        WHEN delay_rate > 10 THEN 'Medium'
        ELSE 'Good'
    END as performance_tier
FROM warehouse_health
ORDER BY delay_rate DESC;

-- View: Active Alerts Summary
CREATE OR REPLACE VIEW v_active_alerts AS
SELECT 
    type,
    severity,
    COUNT(*) as count,
    MAX(created_at) as latest
FROM alerts
WHERE resolved = FALSE
GROUP BY type, severity;

-- View: SLA Summary
CREATE OR REPLACE VIEW v_sla_summary AS
SELECT 
    COUNT(*) as total_violations,
    AVG(violation_hours) as avg_violation_hours,
    MAX(violation_hours) as max_violation_hours
FROM sla_violations
WHERE created_at > NOW() - INTERVAL '24 hours';

-- ============================================================
-- FUNCTIONS FOR AUTOMATION
-- ============================================================

-- Function: Calculate risk score for a shipment
CREATE OR REPLACE FUNCTION calculate_risk_score(
    p_distance_km NUMERIC,
    p_status VARCHAR,
    p_estimated_hours INTEGER,
    p_is_delayed BOOLEAN
) RETURNS INTEGER AS $$
DECLARE
    v_score INTEGER := 0;
BEGIN
    -- Distance rule
    IF p_distance_km > 1000 AND p_status = 'in_transit' THEN
        v_score := v_score + 30;
    END IF;
    
    -- Previous delay rule
    IF p_is_delayed THEN
        v_score := v_score + 35;
    END IF;
    
    -- Unrealistic SLA rule
    IF p_estimated_hours < 5 AND p_distance_km > 800 THEN
        v_score := v_score + 25;
    END IF;
    
    RETURN LEAST(v_score, 100);
END;
$$ LANGUAGE plpgsql;

-- Function: Get risk level from score
CREATE OR REPLACE FUNCTION get_risk_level(p_score INTEGER) RETURNS VARCHAR AS $$
BEGIN
    IF p_score >= 80 THEN
        RETURN 'Critical';
    ELSIF p_score >= 60 THEN
        RETURN 'High';
    ELSIF p_score >= 30 THEN
        RETURN 'Medium';
    ELSE
        RETURN 'Low';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- SEED DATA FOR TESTING
-- ============================================================

-- Insert sample warehouse health data
INSERT INTO warehouse_health (warehouse_id, bottleneck, severity, reason, avg_processing_hours, delay_rate, total_shipments, capacity, volume_ratio) VALUES
    ('WH-NYC', TRUE, 'High', 'Delay rate exceeds threshold', 5.5, 28, 150, 100, 1.5),
    ('WH-LAX', FALSE, 'Low', 'Operating normally', 2.1, 8, 85, 100, 0.85),
    ('WH-CHI', FALSE, 'Medium', 'Processing time above average', 4.2, 12, 95, 100, 0.95),
    ('WH-HOU', TRUE, 'Critical', 'High delay rate and capacity stress', 6.0, 32, 120, 100, 1.2),
    ('WH-PHX', FALSE, 'Low', 'Operating normally', 1.8, 5, 60, 100, 0.6)
ON CONFLICT (warehouse_id) DO NOTHING;

-- Insert sample alerts
INSERT INTO alerts (type, severity, message, shipment_id, warehouse_id, region) VALUES
    ('delayed_shipment', 'High', 'Delayed shipments exceed threshold', NULL, 'WH-NYC', NULL),
    ('warehouse_bottleneck', 'Critical', 'Warehouse WH-HOU has critical bottleneck', NULL, 'WH-HOU', NULL),
    ('critical_risk', 'Critical', 'Shipment SHP-000002 has critical risk score', 'SHP-000002', NULL, 'north'),
    ('region_performance', 'Medium', 'Region south on-time rate dropped', NULL, NULL, 'south')
ON CONFLICT DO NOTHING;

-- Insert sample users (all passwords are 'password123')
INSERT INTO users (full_name, email, password_hash, role) VALUES
    ('System Admin', 'admin@logistics.com', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36XoR7A58E.T2Wl6tE3Z2.q', 'Admin'),
    ('Operations Manager', 'manager@logistics.com', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36XoR7A58E.T2Wl6tE3Z2.q', 'Manager'),
    ('Data Analyst', 'analyst@logistics.com', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36XoR7A58E.T2Wl6tE3Z2.q', 'Analyst')
ON CONFLICT (email) DO NOTHING;

-- Insert sample region performance
INSERT INTO region_performance (region, total_shipments, delivered, delayed, in_transit, on_time_rate, avg_delivery_hours, trend) VALUES
    ('north', 120, 85, 25, 10, 70.8, 14.2, 'declining'),
    ('south', 95, 78, 12, 5, 82.1, 10.5, 'improving'),
    ('east', 80, 60, 15, 5, 75.0, 12.8, 'stable'),
    ('west', 110, 88, 18, 4, 80.0, 11.2, 'stable'),
    ('central', 75, 55, 12, 8, 73.3, 13.5, 'declining')
ON CONFLICT DO NOTHING;