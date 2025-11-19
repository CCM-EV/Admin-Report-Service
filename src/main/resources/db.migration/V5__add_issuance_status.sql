-- ============================================
-- V5: Add status column to fact_issuance table
-- for tracking issuance lifecycle (PENDING, APPROVED, REJECTED)
-- ============================================

-- Add status column to fact_issuance table
ALTER TABLE fact_issuance 
ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'APPROVED';

-- Add comment
COMMENT ON COLUMN fact_issuance.status IS 'Issuance status: PENDING (requested), APPROVED (issued), REJECTED (denied)';

-- Create index for filtering and analytics by status
CREATE INDEX IF NOT EXISTS idx_fact_issuance_status 
ON fact_issuance(status, issued_at);

-- Update consumed_events table to store event_type and payload separately for better querying
ALTER TABLE consumed_events
ADD COLUMN IF NOT EXISTS event_type VARCHAR(50),
ADD COLUMN IF NOT EXISTS payload JSONB;

-- Create index on event_type for faster filtering
CREATE INDEX IF NOT EXISTS idx_consumed_events_type 
ON consumed_events(event_type, received_at);

-- Add comment
COMMENT ON COLUMN consumed_events.event_type IS 'Type of event: USER_EVENT, ISSUANCE_EVENT, TRADE_EVENT, PAYMENT_EVENT';
COMMENT ON COLUMN consumed_events.payload IS 'Full event payload in JSON format for debugging and auditing';
