-- ============================================
-- V6: Fix currency from USD to VND
-- Align with marketplace-service standard
-- ============================================

-- Update default currency in fact_trade table
ALTER TABLE fact_trade ALTER COLUMN currency SET DEFAULT 'VND';

-- Update default currency in fact_payment table  
ALTER TABLE fact_payment ALTER COLUMN currency SET DEFAULT 'VND';

-- Update existing records to VND (if any exist with USD)
-- Note: This assumes all transactions are in Vietnam and should be VND
UPDATE fact_trade SET currency = 'VND' WHERE currency = 'USD';
UPDATE fact_payment SET currency = 'VND' WHERE currency = 'USD';

-- Add index for currency filtering (optional, for performance)
CREATE INDEX IF NOT EXISTS idx_fact_trade_currency ON fact_trade(currency);
CREATE INDEX IF NOT EXISTS idx_fact_payment_currency ON fact_payment(currency);
