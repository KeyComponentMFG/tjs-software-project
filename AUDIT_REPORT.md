# FULL FORENSIC FINANCIAL AUDIT REPORT
## TJ's Software — Etsy Financial Dashboard
### Date: February 25, 2026 | Auditor: Claude (Senior Financial Systems Auditor)

---

## EXECUTIVE SUMMARY

After a complete forensic audit of the 13,438-line `etsy_dashboard.py`, `supabase_loader.py` (836 lines), `_parse_bank_statements.py` (570 lines), and supporting modules, I identified **12 bugs/issues** across the system. Of these:

- **3 are HIGH SEVERITY** (will produce incorrect financial numbers)
- **4 are MEDIUM SEVERITY** (produce incorrect numbers in specific scenarios)
- **5 are LOW SEVERITY** (minor inconsistencies or risks)

The most critical finding: **the tax year calculations and partner K-1 shares are incorrect** because the Payment transaction type is excluded from the yearly Etsy net formula.

---

## STEP 1 — SYSTEM MAP

### Data Sources (9 Supabase Tables)
| Table | Source | Records |
|---|---|---|
| `etsy_transactions` | Etsy CSV statements | All sales, fees, shipping, refunds, etc. |
| `bank_transactions` | Capital One PDFs + CSVs | Deposits, debits, categorized |
| `inventory_orders` | Invoice PDFs (Key Component, Amazon) | Order headers |
| `inventory_items` | Invoice PDFs | Line items with qty, price |
| `inventory_item_details` | Manual user edits | Rename/categorize/qty overrides |
| `inventory_location_overrides` | Manual user edits | Location assignments |
| `inventory_usage` | Manual user entries | Item consumption tracking |
| `inventory_quick_add` | Manual user entries | Ad-hoc inventory |
| `config` | `data/config.json` | All settings, overrides, manual txns |

### Transformation Layers
1. **PDF/CSV Parsing** → raw transactions
2. **`_add_computed_columns()`** → Amount_Clean, Net_Clean, Date_Parsed, Month, Week
3. **`auto_categorize()`** → bank transaction categories
4. **`apply_overrides()`** → transaction splits and recategorizations
5. **Module-level aggregations** → all financial metrics
6. **3 reload functions** → recalculate after uploads (`_reload_etsy_data`, `_reload_bank_data`, `_reload_inventory_data`)
7. **`_cascade_reload()`** → cross-source metric sync
8. **`api_reload()`** → full Supabase refresh (Railway)

### Key Formulas
| Metric | Formula | Location |
|---|---|---|
| `etsy_net_earned` | gross_sales - fees - shipping - marketing - refunds - taxes - buyer_fees + payments | Line 642-643 |
| `real_profit` | bank_cash_on_hand + bank_owner_draw_total | Line 664 |
| `bank_cash_on_hand` | bank_net_cash + etsy_balance | Line 662 |
| `etsy_balance` | max(0, all_net_sum - deposits + starting_balance) | Line 269 |
| `partner_share` | ordinary_income / 2 | Line 7559 |
| `buyer_paid_shipping` | shipping_transaction_fees / 0.065 | Line 1823 |

---

## STEP 2 — CALCULATION TRACE & VERIFICATION

### Confirmed Correct Calculations
1. **`etsy_net_earned` (line 642-643)** — Correctly includes all 8 transaction types (Sale, Fee, Shipping, Marketing, Refund, Tax, Buyer Fee, Payment). Verified by self-check at line 651-655 comparing itemized formula to direct `DATA["Net_Clean"].sum()`.
2. **`real_profit` (line 664)** — Correctly defines profit as Cash + Draws. Bank-verified, dual-entry consistent.
3. **`etsy_balance` (line 269)** — Correctly uses deposit parsing from Title field + starting balance offset.
4. **`bank_net_cash` (line 611)** — Simple deposits - debits. Verified.
5. **`bank_by_cat` (line 614-618)** — Correctly aggregates debits only by category.
6. **`total_inventory_cost` (line 576)** — Correctly sums INV_DF grand_total.
7. **`bb_cc_balance` (line 284)** — Correctly computed as charged - paid.
8. **`draw_diff` / `draw_owed_to` (lines 677-681)** — Correctly computes settlement direction.
9. **Etsy dedup logic (supabase_loader line 694-732)** — 6-tuple key is sufficiently unique.
10. **Tax allocation of inventory items (line 422-427)** — Proportional tax allocation is correct.

---

## STEP 3-6 — BUGS, INCONSISTENCIES, AND RISKS

### BUG #1 — CRITICAL: Tax Year K-1 Missing Payment Type
**Severity: HIGH | Impact: Incorrect partner K-1 shares and tax obligations**
**File:** `etsy_dashboard.py` line 3308
**Description:** The `_recompute_tax_years()` function computes yearly Etsy net as:
```python
yr_etsy_net = yr_gross - yr_fees - yr_shipping - yr_marketing - yr_refunds - yr_taxes - yr_buyer_fees
```
This is **MISSING `+ yr_payments`** (Payment type transactions). The main `etsy_net_earned` formula at line 642-643 correctly includes `+ total_payments`, but the tax year split does not.

**Impact:** Every downstream tax calculation is affected:
- Schedule K-1 partner shares (line 7559)
- Schedule SE self-employment tax (line 7625)
- Capital account tracking (lines 7571-7575)
- Tax strategy recommendations (line 7927-7931)

**Fix:**
```python
# Line 3308: Add payment_df filter and include in formula
_pay = payment_df[payment_df["Date_Parsed"].dt.year == _yr]
yr_payments = _pay["Net_Clean"].sum() if len(_pay) else 0.0
yr_etsy_net = yr_gross - yr_fees - yr_shipping - yr_marketing - yr_refunds - yr_taxes - yr_buyer_fees + yr_payments
```

Also add `yr_payments` to the TAX_YEARS dict (line 3348) and update the combined income formula at line 7927-7931.

---

### BUG #2 — CRITICAL: monthly_net_revenue Missing 3 Transaction Types
**Severity: HIGH | Impact: Monthly profit charts and monthly profit-per-order are wrong**
**File:** `etsy_dashboard.py` lines 1934-1942, 915-922
**Description:** The `monthly_net_revenue` formula computes:
```python
monthly_net_revenue[m] = sales + raw_fees + raw_shipping + raw_marketing + raw_refunds
```
This is **MISSING** three transaction types that are in `etsy_net_earned`:
1. **Tax** — sales tax collected (negative, should be deducted)
2. **Buyer Fee** — fees charged to buyers (negative, should be deducted)
3. **Payment** — refund credits (positive, should be added)

**Impact:** `monthly_net_revenue` will be HIGHER than actual net revenue because taxes, buyer fees, and payments are excluded. This affects:
- Monthly net revenue charts
- Monthly profit-per-order calculations (line 1985)
- Revenue projections via LinearRegression (line 2021)
- Analytics cost ratio calculations (line 2013)

**Fix:**
```python
# Add these lines after line 1932:
monthly_raw_taxes = DATA[DATA["Type"] == "Tax"].groupby("Month")["Net_Clean"].sum()
monthly_raw_buyer_fees = DATA[DATA["Type"] == "Buyer Fee"].groupby("Month")["Net_Clean"].sum()
monthly_raw_payments = DATA[DATA["Type"] == "Payment"].groupby("Month")["Net_Clean"].sum()

# Update lines 1934-1942:
monthly_net_revenue = {}
for m in months_sorted:
    monthly_net_revenue[m] = (
        monthly_sales.get(m, 0)
        + monthly_raw_fees.get(m, 0)
        + monthly_raw_shipping.get(m, 0)
        + monthly_raw_marketing.get(m, 0)
        + monthly_raw_refunds.get(m, 0)
        + monthly_raw_taxes.get(m, 0)
        + monthly_raw_buyer_fees.get(m, 0)
        + monthly_raw_payments.get(m, 0)
    )
```

Apply the same fix in `_reload_etsy_data()` (lines 910-922).

---

### BUG #3 — CRITICAL: api_reload Missing Monthly Raw Metrics
**Severity: HIGH | Impact: After Railway Supabase refresh, monthly charts show stale data**
**File:** `etsy_dashboard.py` lines 7150-7158
**Description:** The `api_reload()` function declares globals for `monthly_raw_fees`, `monthly_raw_shipping`, `monthly_raw_marketing`, `monthly_raw_refunds`, and `monthly_net_revenue` (lines 7098-7099) but **NEVER COMPUTES THEM**. After an `api_reload`, these variables retain stale values from the previous data load.

**Impact:** All monthly net revenue charts, monthly profit-per-order, and projection charts display OLD data on Railway after a Supabase reload.

**Fix:** Add after line 7158:
```python
monthly_raw_fees = fee_df.groupby("Month")["Net_Clean"].sum()
monthly_raw_shipping = ship_df.groupby("Month")["Net_Clean"].sum()
monthly_raw_marketing = mkt_df.groupby("Month")["Net_Clean"].sum()
monthly_raw_refunds = refund_df.groupby("Month")["Net_Clean"].sum()

monthly_net_revenue = {}
for m in months_sorted:
    monthly_net_revenue[m] = (
        monthly_sales.get(m, 0) + monthly_raw_fees.get(m, 0)
        + monthly_raw_shipping.get(m, 0) + monthly_raw_marketing.get(m, 0)
        + monthly_raw_refunds.get(m, 0)
    )
```

---

### BUG #4 — MEDIUM: api_reload Sets total_fees_gross Incorrectly
**Severity: MEDIUM | Impact: Gross fee display wrong after Railway reload**
**File:** `etsy_dashboard.py` line 7138
**Description:** Line 7138 sets:
```python
total_fees_gross = abs(fee_df["Net_Clean"].sum())
```
This is `total_fees` (net of credits), NOT gross fees. The correct computation (done at line 1789 and 838) is:
```python
total_fees_gross = listing_fees + transaction_fees_product + transaction_fees_shipping + processing_fees
```

**Impact:** After `api_reload`, `total_fees_gross` equals `total_fees`, understating gross fees. This affects the fee breakdown display and the "Net Fees After Credits" calculation at line 10247.

**Fix:** Remove line 7138 and let `_recompute_shipping_details()` / `_cascade_reload()` handle it, OR add the full fee breakdown computation before the `_cascade_reload()` call.

---

### BUG #5 — MEDIUM: payment_df and total_payments Not Declared Global in _reload_etsy_data
**Severity: MEDIUM | Impact: Module-level payment_df stale after Etsy upload**
**File:** `etsy_dashboard.py` lines 725-726, 777, 787
**Description:** `_reload_etsy_data()` creates a local `payment_df` (line 777) and local `total_payments` (line 787). Neither is declared `global`. While the local values are used correctly within `_reload_etsy_data()` for `etsy_net_earned`, the module-level `payment_df` and `total_payments` variables are never updated.

**Impact:** Any code that reads the module-level `payment_df` or `total_payments` after a reload will see stale values. This includes `_recompute_tax_years()` which filters `payment_df` by year (BUG #1 area), and any display code referencing `total_payments`.

**Fix:** Add to line 725-726:
```python
global DATA, sales_df, fee_df, ship_df, mkt_df, refund_df, tax_df
global deposit_df, buyer_fee_df, payment_df
```
And add `total_payments` to the globals at line 728.

---

### BUG #6 — MEDIUM: Manual Transactions Silently Dropped
**Severity: MEDIUM | Impact: Missing expense entries when bank statements cover the same month**
**File:** `_parse_bank_statements.py` lines 533-542
**Description:** Manual transactions from `config.json["manual_transactions"]` are only added when their month is NOT already covered by a parsed bank statement. Several manual entries represent "split from Discover" transactions that do NOT appear in the Capital One statement at all. When a February 2026 Capital One statement is uploaded, ALL February manual transactions are silently dropped, causing:
- $131.20 in Amazon Inventory expenses to vanish
- $350.00 in Owner Draw to vanish
- $23.98 in Etsy Fees to vanish
- $483.28 in Etsy Payout deposit to vanish
- **Total: ~$988.46 in missing transactions**

**Fix:** Add a `permanent` flag to manual transactions that should always be included:
```python
for mt in MANUAL_TRANSACTIONS:
    parts = mt["date"].split("/")
    mt_month = f"{parts[2]}-{parts[0]}"
    if mt_month in all_covered_months and not mt.get("permanent", False):
        manual_skipped += 1
        continue
    # ... append transaction
```
Or better: remove manual transactions once the real bank statements are uploaded and the transactions appear in them.

---

### BUG #7 — MEDIUM: Bank Dedup Key Fragility (Amount Type Coercion)
**Severity: MEDIUM | Impact: Potential duplicate bank transactions in Supabase**
**File:** `supabase_loader.py` lines 757, 765
**Description:** The dedup key for bank transactions uses `str(amount)` to compare amounts. Supabase NUMERIC might return `100` (int) while Python has `100.0` (float), producing string keys `"100"` vs `"100.0"` that don't match, causing duplicate inserts.

**Fix:** Normalize amounts to float before string conversion:
```python
# Line 757:
existing_keys.add((r.get("date", ""), f"{float(r.get('amount', 0)):.2f}", r.get("type", ""), ...))
# Line 765:
key = (t.get("date", ""), f"{float(t.get('amount', 0)):.2f}", t.get("type", ""), t.get("desc", ""))
```

---

### BUG #8 — LOW: days_active Off-By-One Inconsistency
**Severity: LOW | Impact: Revenue/Day metric off by ~0.7% (for 150 days)**
**File:** `etsy_dashboard.py` lines 1991 vs 7166
**Description:** Module-level and `_reload_etsy_data` use `(max - min).days` (line 1991, 969). `api_reload` uses `(max - min).days + 1` (line 7166). The `+1` version is correct (inclusive count).

**Fix:** Change lines 969 and 1991 to:
```python
days_active = max((DATA["Date_Parsed"].max() - DATA["Date_Parsed"].min()).days + 1, 1)
```

---

### BUG #9 — LOW: Transaction Split Amount Not Validated
**Severity: LOW | Impact: Silent data change if split amounts don't sum to original**
**File:** `_parse_bank_statements.py` line 437-446
**Description:** When a transaction is split via `transaction_overrides`, the split amounts replace the original amount with no validation that `sum(splits) == original_amount`.

**Fix:** Add validation:
```python
if action == "split":
    split_total = sum(s["amount"] for s in override["splits"])
    if abs(split_total - t["amount"]) > 0.01:
        print(f"WARNING: Split total ${split_total:.2f} != original ${t['amount']:.2f}")
    for split in override["splits"]:
        result.append({**t, "amount": split["amount"], "category": split["category"]})
```

---

### BUG #10 — LOW: PDF Parser Deposits Determined by Keywords Only
**Severity: LOW (currently) | Impact: Would misclassify non-Etsy deposits as debits**
**File:** `_parse_bank_statements.py` lines 218-225
**Description:** The PDF parser determines deposit vs debit solely by description keywords ("ACH DEPOSIT" or "ETSY...PAYOUT"). Capital One statements have separate columns for deposits and debits, but the parser reads amounts without distinguishing which column they came from. Any non-Etsy deposit (wire transfer, check, etc.) would be classified as a debit.

**Risk Level:** Currently low because all deposits are Etsy ACH payouts. Becomes HIGH if other deposit types occur.

**Fix:** Improve the PDF parsing state machine to track whether an amount appears in the "Deposits/Credits" column vs "Withdrawals/Debits" column based on the column header positions.

---

### BUG #11 — LOW: best_buy_cc Config Payments Array Unused
**Severity: LOW | Impact: Dead config data, potential confusion**
**File:** `etsy_dashboard.py` lines 274-286, `data/config.json` lines 66-69
**Description:** The `best_buy_cc.payments` array in config.json contains two payment records ($100 each), but the code ignores this array and instead auto-detects CC payments from bank transactions. This creates confusion about where payment data comes from.

**Fix:** Either remove the unused `payments` array from config, or use it as a fallback when bank statements haven't been imported for those months.

---

### BUG #12 — LOW: _parse_bank_statements Loads Config at Import Time — FIXED
**Severity: LOW | Impact: Config changes require app restart**
**File:** `_parse_bank_statements.py` lines 12-17
**Description:** `CONFIG`, `CATEGORY_OVERRIDES`, `TRANSACTION_OVERRIDES`, and `MANUAL_TRANSACTIONS` are loaded at module import time from `data/config.json`. If config is updated (e.g., new category overrides added via dashboard), the parser will use stale config until the Python process restarts.

**Fix applied:** Replaced module-level config loading with a `_load_config()` function. Each public function (`parse_bank_pdf`, `parse_bank_csv`, `apply_overrides`, `main`) now reloads config from disk on each call. `auto_categorize` accepts an optional `category_overrides` parameter (loaded from disk if not provided) and callers pass pre-loaded overrides to avoid redundant reads within a single parse run.

---

## STEP 5 — DATA INTEGRITY VERIFICATION

### Balance Equation Check
```
Revenue - Expenses = Profit?
```
- `real_profit = bank_cash_on_hand + bank_owner_draw_total`
- `bank_cash_on_hand = bank_net_cash + etsy_balance`
- `bank_net_cash = bank_total_deposits - bank_total_debits`

This is internally consistent: profit = (deposits - debits) + etsy_balance + owner_draws. This measures "what you have plus what you took" which is a valid profit definition.

However, `profit ≠ etsy_net_earned - bank_all_expenses` because:
1. `real_profit` is bank-verified (actual cash flows)
2. `etsy_net_earned` is Etsy-statement-verified (what Etsy says you earned)
3. The difference is the `etsy_csv_gap` — timing differences between Etsy earnings and bank deposits

### Partner Split Reconciliation
- Draws are tracked: `tulsa_draw_total` vs `texas_draw_total`
- Settlement direction is computed correctly
- **BUT**: Partner K-1 income is wrong (BUG #1)

### Import Idempotency
- Etsy sync: `sync_etsy_transactions()` clears all and re-inserts (idempotent)
- Etsy append: `append_etsy_transactions()` uses 6-tuple dedup (idempotent)
- Bank sync: `sync_bank_transactions()` clears all and re-inserts (idempotent)
- Bank append: `append_bank_transactions()` uses 4-tuple dedup (FRAGILE — BUG #7)
- Inventory: `save_new_order()` inserts without dedup check — uploading the same receipt twice creates duplicate items

---

## STEP 6 — PERFORMANCE & LOGIC RISKS

### Race Conditions
1. **`save_item_details()`** (supabase_loader.py line 311): Has explicit race condition handling — if INSERT fails due to concurrent write, falls back to UPDATE. This is correctly handled.
2. **Module-level globals**: The entire application uses `global` variables for state. If Dash callbacks fire concurrently (which they can via multiple browser tabs), two callbacks could be mid-way through `_reload_etsy_data()` simultaneously, leaving globals in an inconsistent state.

### Stale Cache
1. **`_supabase_client_cache["failed"]`**: If Supabase is temporarily down at startup, it's marked as failed permanently. No retry mechanism exists — the entire session uses local fallback even if Supabase recovers.
2. **Module-level computed metrics**: All metrics are computed at import time. If the underlying data changes (e.g., new Supabase rows), stale values persist until a reload function is explicitly called.

### Floating Point
- All financial calculations use Python `float` (IEEE 754 double precision). For the dollar amounts in this system ($0 - $50,000 range), precision to 15 significant digits is more than adequate. The `round(..., 2)` calls at display/storage boundaries prevent accumulation errors.
- **No Decimal module usage** — technically, `Decimal` is preferred for financial calculations, but the risk is negligible at this scale.

### Silent Failures
1. **Every `except Exception: pass`** in the module-level code (lines 231-232, 457-458, 468-469, etc.) silently swallows errors. If a bank PDF fails to parse, the exception is caught and the file is skipped without any user notification in the dashboard.
2. **Supabase write failures** return `False` but the caller often doesn't check the return value.

---

## STEP 7 — RECOMMENDATIONS

### Priority 1: Fix Critical Bugs (Do Immediately)

**1a. Fix Tax Year Payment Type Omission (BUG #1)**
```python
# In _recompute_tax_years(), around line 3281, add:
_pay = payment_df[payment_df["Date_Parsed"].dt.year == _yr]
yr_payments = _pay["Net_Clean"].sum() if len(_pay) else 0.0

# Change line 3308 to:
yr_etsy_net = yr_gross - yr_fees - yr_shipping - yr_marketing - yr_refunds - yr_taxes - yr_buyer_fees + yr_payments

# Add to TAX_YEARS dict at line 3348:
"payments": yr_payments,

# Update combined income formula at line 7927-7931 to include payments
```

**1b. Fix monthly_net_revenue Formula (BUG #2)**
Add `monthly_raw_taxes`, `monthly_raw_buyer_fees`, `monthly_raw_payments` to the monthly_net_revenue computation in all 3 locations (module-level, _reload_etsy_data, api_reload).

**1c. Fix api_reload Missing Monthly Metrics (BUG #3)**
Add the monthly_raw_* and monthly_net_revenue computations to `api_reload()` between lines 7158-7169.

### Priority 2: Fix Medium Bugs (This Week)

**2a. Add payment_df and total_payments to globals in _reload_etsy_data (BUG #5)**
**2b. Fix total_fees_gross in api_reload (BUG #4)**
**2c. Fix manual transactions dropping logic (BUG #6)**
**2d. Fix bank dedup amount normalization (BUG #7)**

### Priority 3: Fix Low Bugs (When Convenient)

**3a. Fix days_active off-by-one (BUG #8)**
**3b. Add split amount validation (BUG #9)**
**3c. Improve PDF deposit detection (BUG #10)**
**3d. Clean up unused config data (BUG #11)**
**3e. Fix config loading at import time (BUG #12)**

---

## VALIDATION QUERIES (SQL for Supabase)

### 1. Verify Etsy Transaction Type Completeness
```sql
SELECT type, COUNT(*) as cnt, SUM(CAST(REPLACE(REPLACE(net, '$', ''), ',', '') AS NUMERIC)) as net_sum
FROM etsy_transactions
WHERE net != '--'
GROUP BY type
ORDER BY type;
```

### 2. Verify Bank Transaction Balance
```sql
SELECT
  SUM(CASE WHEN type = 'deposit' THEN amount ELSE 0 END) as total_deposits,
  SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) as total_debits,
  SUM(CASE WHEN type = 'deposit' THEN amount ELSE -amount END) as net_cash
FROM bank_transactions;
```

### 3. Verify No Duplicate Etsy Transactions
```sql
SELECT date, type, title, info, amount, net, COUNT(*) as cnt
FROM etsy_transactions
GROUP BY date, type, title, info, amount, net
HAVING COUNT(*) > 1;
```

### 4. Verify No Duplicate Bank Transactions
```sql
SELECT date, description, amount, type, COUNT(*) as cnt
FROM bank_transactions
GROUP BY date, description, amount, type
HAVING COUNT(*) > 1;
```

### 5. Verify Inventory Order Totals
```sql
SELECT o.order_num, o.grand_total, o.subtotal, o.tax,
       SUM(i.price * i.qty) as item_total,
       o.grand_total - o.tax - SUM(i.price * i.qty) as discrepancy
FROM inventory_orders o
JOIN inventory_items i ON o.order_num = i.order_num
GROUP BY o.order_num, o.grand_total, o.subtotal, o.tax
HAVING ABS(o.subtotal - SUM(i.price * i.qty)) > 0.02;
```

### 6. Verify Category Coverage (No Uncategorized Debits)
```sql
SELECT category, COUNT(*) as cnt, SUM(amount) as total
FROM bank_transactions
WHERE type = 'debit'
GROUP BY category
ORDER BY total DESC;
```

### 7. Verify Owner Draw Parity
```sql
SELECT category, SUM(amount) as total
FROM bank_transactions
WHERE category LIKE 'Owner Draw%'
GROUP BY category;
```

### 8. Cross-Check Etsy Deposits vs Bank Deposits
```sql
-- Etsy side: deposit amounts from title
SELECT title, date FROM etsy_transactions WHERE type = 'Deposit';

-- Bank side: Etsy payouts
SELECT date, amount FROM bank_transactions
WHERE category = 'Etsy Payout'
ORDER BY date;
```

---

## SUGGESTED AUTOMATED RECONCILIATION TESTS

### Test 1: Formula Parity Check (add to startup)
```python
def validate_etsy_net():
    """Verify etsy_net_earned equals DATA Net_Clean sum."""
    direct_sum = round(DATA["Net_Clean"].sum(), 2)
    formula_sum = round(etsy_net_earned, 2)
    assert abs(direct_sum - formula_sum) < 0.02, \
        f"PARITY FAIL: direct={direct_sum}, formula={formula_sum}, diff={direct_sum - formula_sum}"
```

### Test 2: Monthly Sum Check
```python
def validate_monthly_totals():
    """Verify sum of monthly_net_revenue equals etsy_net_earned."""
    monthly_total = sum(monthly_net_revenue.values())
    # Should equal etsy_net_earned (minus deposits which have Net=0)
    assert abs(monthly_total - etsy_net_earned) < 1.00, \
        f"MONTHLY SUM FAIL: monthly={monthly_total:.2f}, earned={etsy_net_earned:.2f}"
```

### Test 3: Bank Balance Check
```python
def validate_bank_balance():
    """Verify bank aggregates are self-consistent."""
    computed_net = bank_total_deposits - bank_total_debits
    assert abs(computed_net - bank_net_cash) < 0.01, \
        f"BANK NET FAIL: computed={computed_net}, stored={bank_net_cash}"
```

### Test 4: Partner Draw Reconciliation
```python
def validate_draws():
    """Verify draw totals match bank records."""
    tulsa = sum(t["amount"] for t in bank_debits if t["category"] == "Owner Draw - Tulsa")
    texas = sum(t["amount"] for t in bank_debits if t["category"] == "Owner Draw - Texas")
    assert abs(tulsa - tulsa_draw_total) < 0.01
    assert abs(texas - texas_draw_total) < 0.01
    assert abs(tulsa + texas - bank_owner_draw_total) < 0.01
```

### Test 5: Inventory Consistency
```python
def validate_inventory():
    """Verify inventory DF totals match invoice sums."""
    inv_sum = sum(inv["grand_total"] for inv in INVOICES)
    df_sum = INV_DF["grand_total"].sum()
    assert abs(inv_sum - df_sum) < 0.01, \
        f"INVENTORY FAIL: invoices={inv_sum}, df={df_sum}"
```

### Test 6: Tax Year Completeness
```python
def validate_tax_years():
    """Verify tax year splits sum to totals."""
    for metric in ["gross_sales", "refunds", "fees", "shipping", "marketing"]:
        yr_sum = sum(TAX_YEARS[yr][metric] for yr in TAX_YEARS)
        # Compare to module-level total
        expected = {"gross_sales": gross_sales, "refunds": total_refunds,
                    "fees": total_fees, "shipping": total_shipping_cost,
                    "marketing": total_marketing}[metric]
        assert abs(yr_sum - expected) < 0.02, \
            f"TAX YEAR FAIL: {metric} yr_sum={yr_sum}, expected={expected}"
```

### Test 7: Dedup Integrity
```python
def validate_no_duplicates():
    """Check for duplicate transactions."""
    etsy_keys = DATA.apply(lambda r: (r["Date"], r["Type"], r["Title"], r["Info"], r["Amount"], r["Net"]), axis=1)
    dupes = etsy_keys[etsy_keys.duplicated()]
    assert len(dupes) == 0, f"DUPLICATE ETSY ROWS: {len(dupes)}"

    bank_keys = [(t["date"], t["amount"], t["type"], t["desc"]) for t in BANK_TXNS]
    bank_dupes = [k for k in bank_keys if bank_keys.count(k) > 1]
    assert len(bank_dupes) == 0, f"DUPLICATE BANK ROWS: {len(set(bank_dupes))}"
```

---

## SUMMARY OF FINDINGS

| # | Severity | Description | Impact |
|---|----------|-------------|--------|
| 1 | **HIGH** | Tax year K-1 missing Payment type | Wrong partner shares and tax obligations |
| 2 | **HIGH** | monthly_net_revenue missing Tax/BuyerFee/Payment | Wrong monthly profit charts and projections |
| 3 | **HIGH** | api_reload missing monthly raw metrics | Stale monthly data on Railway after refresh |
| 4 | MEDIUM | api_reload total_fees_gross = total_fees | Wrong fee breakdown after Railway reload |
| 5 | MEDIUM | payment_df/total_payments not global in reload | Stale payment data after Etsy upload |
| 6 | MEDIUM | Manual transactions dropped when month covered | ~$988 in transactions silently vanish |
| 7 | MEDIUM | Bank dedup amount type mismatch | Potential duplicate bank entries |
| 8 | LOW | days_active off-by-one inconsistency | ~0.7% error in revenue/day metric |
| 9 | LOW | Split amount not validated | Silent data modification risk |
| 10 | LOW | PDF deposits detected by keywords only | Would misclassify non-Etsy deposits |
| 11 | LOW | Config payments array unused | Dead data, potential confusion |
| 12 | LOW | Config loaded at import time | Stale config until restart |

---

*End of Audit Report*
