-- V4__align_events_schema.sql
-- Sync reporting tables with new event payloads

-- ============================
-- Consumed events audit trail
-- ============================
ALTER TABLE consumed_events
    ADD COLUMN IF NOT EXISTS event_type TEXT,
    ADD COLUMN IF NOT EXISTS payload JSONB;

CREATE INDEX IF NOT EXISTS idx_consumed_events_event_type
    ON consumed_events(event_type);

-- ============================
-- Dimension updates
-- ============================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'dim_users' AND column_name = 'org'
    ) THEN
        ALTER TABLE dim_users RENAME COLUMN org TO organization_name;
    END IF;
END $$;

ALTER TABLE dim_users
    ADD COLUMN IF NOT EXISTS enabled BOOLEAN DEFAULT true,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS phone_number TEXT;

CREATE INDEX IF NOT EXISTS idx_dim_users_enabled ON dim_users(enabled);

-- ============================
-- Fact trade adjustments
-- ============================
ALTER TABLE fact_trade
    ADD COLUMN IF NOT EXISTS order_status TEXT,
    ADD COLUMN IF NOT EXISTS is_auction BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS status_changed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fact_trade_status
    ON fact_trade(order_status, executed_at);

-- ============================
-- Fact payment adjustments
-- ============================
ALTER TABLE fact_payment
    ADD COLUMN IF NOT EXISTS payer_id TEXT,
    ADD COLUMN IF NOT EXISTS payee_id TEXT,
    ADD COLUMN IF NOT EXISTS region TEXT,
    ADD COLUMN IF NOT EXISTS status_changed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fact_payment_region
    ON fact_payment(region);

CREATE INDEX IF NOT EXISTS idx_fact_payment_payment_id
    ON fact_payment(payment_id);

-- ============================
-- Fact issuance additions
-- ============================
ALTER TABLE fact_issuance
    ADD COLUMN IF NOT EXISTS vehicle_id TEXT;

-- ============================
-- Comments
-- ============================
COMMENT ON COLUMN fact_trade.status_changed_at IS 'Timestamp of the latest order status change';
COMMENT ON COLUMN fact_payment.status_changed_at IS 'Timestamp of latest payment status update';

