-- ============================================================================
-- Supabase Migration: Create missing inventory tables
-- Run this in: Supabase Dashboard → SQL Editor → New query → Paste → Run
-- Project: xmypdvbfjgpymvygldkk
-- ============================================================================

-- 1. inventory_usage — tracks when items are marked as "used"
CREATE TABLE IF NOT EXISTS inventory_usage (
  id BIGSERIAL PRIMARY KEY,
  item_name TEXT NOT NULL,
  qty INTEGER NOT NULL DEFAULT 1,
  note TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 2. inventory_quick_add — manually added purchases (no receipt)
CREATE TABLE IF NOT EXISTS inventory_quick_add (
  id BIGSERIAL PRIMARY KEY,
  item_name TEXT NOT NULL,
  category TEXT DEFAULT 'Other',
  qty INTEGER DEFAULT 1,
  unit_price NUMERIC(10,2) DEFAULT 0,
  location TEXT DEFAULT '',
  source TEXT DEFAULT 'Manual',
  date TEXT DEFAULT '',
  image_url TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 3. Enable public access (no RLS) — matches existing tables
ALTER TABLE inventory_usage ENABLE ROW LEVEL SECURITY;
ALTER TABLE inventory_quick_add ENABLE ROW LEVEL SECURITY;

-- Allow all operations for anon/authenticated (matches existing table policies)
CREATE POLICY "Allow all on inventory_usage" ON inventory_usage
  FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Allow all on inventory_quick_add" ON inventory_quick_add
  FOR ALL USING (true) WITH CHECK (true);

-- ============================================================================
-- Verify: after running, check that tables exist
-- ============================================================================
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
AND table_name IN ('inventory_usage', 'inventory_quick_add', 'inventory_item_details')
ORDER BY table_name;
