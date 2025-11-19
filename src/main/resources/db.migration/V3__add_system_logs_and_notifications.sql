-- V3__add_system_logs_and_notifications.sql

-- =====================================================
-- System Logs Table
-- =====================================================
CREATE TABLE IF NOT EXISTS system_logs (
    id BIGSERIAL PRIMARY KEY,
    log_level VARCHAR(20) NOT NULL,
    source_service VARCHAR(50) NOT NULL,
    category VARCHAR(50),
    message TEXT,
    details TEXT,
    correlation_id VARCHAR(100),
    user_id VARCHAR(50),
    ip_address VARCHAR(50),
    log_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_system_logs_timestamp ON system_logs (log_timestamp DESC);
CREATE INDEX idx_system_logs_level ON system_logs (log_level);
CREATE INDEX idx_system_logs_source ON system_logs (source_service);
CREATE INDEX idx_system_logs_correlation ON system_logs (correlation_id);
CREATE INDEX idx_system_logs_user ON system_logs (user_id);

-- =====================================================
-- System Notifications Table
-- =====================================================
CREATE TABLE IF NOT EXISTS system_notifications (
    id BIGSERIAL PRIMARY KEY,
    notification_id VARCHAR(100) UNIQUE NOT NULL,
    level VARCHAR(20) NOT NULL,
    category VARCHAR(50) NOT NULL,
    title VARCHAR(255) NOT NULL,
    message TEXT,
    source_service VARCHAR(50),
    target_service VARCHAR(50),
    target_user_id VARCHAR(50),
    read_status BOOLEAN NOT NULL DEFAULT FALSE,
    read_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

CREATE INDEX idx_notifications_timestamp ON system_notifications (created_at DESC);
CREATE INDEX idx_notifications_level ON system_notifications (level);
CREATE INDEX idx_notifications_target ON system_notifications (target_user_id, read_status);
CREATE INDEX idx_notifications_category ON system_notifications (category);
CREATE INDEX idx_notifications_read_status ON system_notifications (read_status);

-- =====================================================
-- Add region columns to existing tables for regional reporting
-- =====================================================

-- Add region to dim_users if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'dim_users' AND column_name = 'region') THEN
        ALTER TABLE dim_users ADD COLUMN region VARCHAR(50);
        CREATE INDEX idx_dim_users_region ON dim_users (region);
    END IF;
END $$;

-- Add region to fact_trade if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'fact_trade' AND column_name = 'region') THEN
        ALTER TABLE fact_trade ADD COLUMN region VARCHAR(50);
        CREATE INDEX idx_fact_trade_region ON fact_trade (region);
    END IF;
END $$;

-- Add region to fact_issuance if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'fact_issuance' AND column_name = 'region') THEN
        ALTER TABLE fact_issuance ADD COLUMN region VARCHAR(50);
        CREATE INDEX idx_fact_issuance_region ON fact_issuance (region);
    END IF;
END $$;

-- =====================================================
-- Regional aggregation views
-- =====================================================

-- Regional CO2 statistics
CREATE OR REPLACE VIEW v_regional_co2_stats AS
SELECT 
    COALESCE(fi.region, 'Unknown') as region,
    COUNT(*) as issuance_count,
    SUM(fi.quantity_tco2e) as total_tco2e,
    SUM(fi.distance_km) as total_distance_km,
    SUM(fi.energy_kwh) as total_energy_kwh,
    SUM(fi.co2_avoided_kg) as total_co2_avoided_kg,
    DATE_TRUNC('day', fi.issued_at) as date
FROM fact_issuance fi
WHERE fi.issued_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY COALESCE(fi.region, 'Unknown'), DATE_TRUNC('day', fi.issued_at);

-- Regional trade statistics
CREATE OR REPLACE VIEW v_regional_trade_stats AS
SELECT 
    COALESCE(ft.region, 'Unknown') as region,
    COUNT(*) as trade_count,
    SUM(ft.quantity) as total_quantity,
    SUM(ft.amount) as total_amount,
    ft.currency,
    DATE_TRUNC('day', ft.executed_at) as date
FROM fact_trade ft
WHERE ft.executed_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY COALESCE(ft.region, 'Unknown'), ft.currency, DATE_TRUNC('day', ft.executed_at);

-- Regional user statistics
CREATE OR REPLACE VIEW v_regional_user_stats AS
SELECT 
    COALESCE(region, 'Unknown') as region,
    role,
    COUNT(*) as user_count
FROM dim_users
GROUP BY COALESCE(region, 'Unknown'), role;

COMMENT ON TABLE system_logs IS 'Store all system logs from all services';
COMMENT ON TABLE system_notifications IS 'Store system notifications and alerts';
COMMENT ON VIEW v_regional_co2_stats IS 'Regional CO2 emission statistics';
COMMENT ON VIEW v_regional_trade_stats IS 'Regional trade volume and revenue statistics';
COMMENT ON VIEW v_regional_user_stats IS 'Regional user distribution statistics';
