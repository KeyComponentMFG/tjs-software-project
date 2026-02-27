-- ============================================================================
-- Migration 001: Add dedup constraints and convert amount columns to NUMERIC
-- ============================================================================
--
-- WARNING: This migration is IRREVERSIBLE.
-- Take a full Supabase backup before running this script.
--
-- What this does:
--   1. Adds unique indexes on etsy_transactions and bank_transactions to
--      prevent duplicate row inserts.
--   2. Converts amount-related TEXT columns to NUMERIC(12,2), safely handling
--      placeholder values ('--', '', NULL) by coercing them to 0.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Unique constraint for etsy_transactions dedup
-- ---------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS idx_etsy_dedup
  ON etsy_transactions(date, type, title, info, amount, net);

-- ---------------------------------------------------------------------------
-- 2. Unique constraint for bank_transactions dedup
-- ---------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_dedup
  ON bank_transactions(date, description, amount, type);

-- ---------------------------------------------------------------------------
-- 3. ALTER amount columns from TEXT to NUMERIC (with safe CASE handling)
-- ---------------------------------------------------------------------------

-- etsy_transactions.amount
ALTER TABLE etsy_transactions
  ALTER COLUMN amount TYPE NUMERIC(12,2) USING
    CASE WHEN amount IN ('--', '', NULL) THEN 0
         ELSE REPLACE(REPLACE(amount, '$', ''), ',', '')::NUMERIC END;

-- etsy_transactions.net
ALTER TABLE etsy_transactions
  ALTER COLUMN net TYPE NUMERIC(12,2) USING
    CASE WHEN net IN ('--', '', NULL) THEN 0
         ELSE REPLACE(REPLACE(net, '$', ''), ',', '')::NUMERIC END;

-- etsy_transactions.fees_and_taxes
ALTER TABLE etsy_transactions
  ALTER COLUMN fees_and_taxes TYPE NUMERIC(12,2) USING
    CASE WHEN fees_and_taxes IN ('--', '', NULL) THEN 0
         ELSE REPLACE(REPLACE(fees_and_taxes, '$', ''), ',', '')::NUMERIC END;
