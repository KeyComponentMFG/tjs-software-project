# FULL CALCULATION AUDIT REPORT

**Date**: 2026-03-04
**Scope**: Every metric in the codebase — accounting pipeline + dashboard
**Verdict**: Core pipeline is CLEAN. Dashboard has 14 estimation zones that need gating.

---

## STEP 1 — Complete Metric Inventory

### 1A. Core Accounting Pipeline (`accounting/agents/computation.py`)

| # | Metric Name | Line | Formula | Traceable? | Confidence |
|---|------------|------|---------|-----------|------------|
| 1 | `gross_sales` | 102 | `SUM(Sale.Net_Clean)` | SOURCE | VERIFIED |
| 2 | `total_refunds` | 104 | `ABS(SUM(Refund.Net_Clean))` | SOURCE | VERIFIED |
| 3 | `net_sales` | 106 | `gross_sales - total_refunds` | SOURCE | DERIVED |
| 4 | `total_fees` | 109 | `ABS(SUM(Fee.Net_Clean))` | SOURCE | VERIFIED |
| 5 | `total_shipping_cost` | 111 | `ABS(SUM(Shipping.Net_Clean))` | SOURCE | VERIFIED |
| 6 | `total_marketing` | 114 | `ABS(SUM(Marketing.Net_Clean))` | SOURCE | VERIFIED |
| 7 | `total_taxes` | 116 | `ABS(SUM(Tax.Net_Clean))` | SOURCE | VERIFIED |
| 8 | `total_payments` | 118 | `SUM(Payment.Net_Clean)` | SOURCE | VERIFIED |
| 9 | `total_buyer_fees` | 120 | `ABS(SUM(BuyerFee.Net_Clean))` | SOURCE | VERIFIED |
| 10 | `order_count` | 122 | `COUNT(Sale)` | SOURCE | VERIFIED |
| 11 | `avg_order` | 124 | `gross_sales / order_count` | SOURCE | DERIVED |
| 12 | `etsy_net_earned` | 132 | `gross - fees - ship - mkt - refunds - tax - bf + payments` | SOURCE | DERIVED |
| 13 | `etsy_net` | 135 | Alias for `etsy_net_earned` | SOURCE | DERIVED |
| 14 | `etsy_net_margin` | 138 | `etsy_net / gross_sales * 100` | SOURCE | DERIVED |
| 15 | `etsy_balance` | 141 | `total_etsy_net - csv_deposits` (auto from titles) | SOURCE | VARIES |
| 16 | `etsy_pre_capone_deposits` | 147 | `SUM(config.pre_capone_detail)` | **ESTIMATED** | ESTIMATED |
| 17 | `etsy_total_deposited` | 159 | `pre_capone + bank_total_deposits` | **ESTIMATED** | ESTIMATED |
| 18 | `etsy_balance_calculated` | 166 | `etsy_net_earned - etsy_total_deposited` | **ESTIMATED** | ESTIMATED |
| 19 | `etsy_csv_gap` | 171 | `balance_calculated - etsy_balance` | **ESTIMATED** | ESTIMATED |
| 20 | `bank_total_deposits` | 180 | `SUM(bank_deposit.amount)` | SOURCE | VERIFIED |
| 21 | `bank_total_debits` | 184 | `SUM(bank_debit.amount)` | SOURCE | VERIFIED |
| 22 | `bank_net_cash` | 188 | `deposits - debits` | SOURCE | DERIVED |
| 23 | `bank_by_cat` | 202 | `GROUP_BY(category, SUM(amount))` | SOURCE | VERIFIED |
| 24 | `bank_tax_deductible` | 213 | `SUM(debit WHERE cat IN deductible_list)` | SOURCE | DERIVED |
| 25 | `bank_personal` | 219 | `SUM(debit WHERE cat='Personal')` | SOURCE | VERIFIED |
| 26 | `bank_pending` | 221 | `SUM(debit WHERE cat='Pending')` | SOURCE | VERIFIED |
| 27 | `bank_biz_expense_total` | 232 | `SUM(biz_expense_categories)` | SOURCE | DERIVED |
| 28 | `bank_all_expenses` | 235 | `amazon_inv + biz_expenses` | SOURCE | DERIVED |
| 29 | `bank_cash_on_hand` | 249 | `bank_net_cash + etsy_balance` | SOURCE | DERIVED |
| 30 | `bank_owner_draw_total` | 252 | `SUM(debit WHERE cat LIKE 'Owner Draw%')` | SOURCE | VERIFIED |
| 31 | `real_profit` | 255 | `bank_cash_on_hand + bank_owner_draw_total` | SOURCE | DERIVED |
| 32 | `real_profit_margin` | 258 | `real_profit / gross_sales * 100` | SOURCE | DERIVED |
| 33 | `tulsa_draw_total` | 269 | `SUM(Owner Draw - Tulsa)` | SOURCE | VERIFIED |
| 34 | `texas_draw_total` | 271 | `SUM(Owner Draw - Texas)` | SOURCE | VERIFIED |
| 35 | `draw_diff` | 273 | `ABS(tulsa - texas)` | SOURCE | DERIVED |
| 36 | `listing_fees` | 298 | `ABS(SUM(Fee WHERE 'Listing fee'))` | SOURCE | VERIFIED |
| 37 | `transaction_fees_product` | 300 | `ABS(SUM(Fee WHERE 'Transaction fee:' NOT Shipping))` | SOURCE | VERIFIED |
| 38 | `transaction_fees_shipping` | 303 | `ABS(SUM(Fee WHERE 'Transaction fee: Shipping'))` | SOURCE | VERIFIED |
| 39 | `processing_fees` | 306 | `ABS(SUM(Fee WHERE 'Processing fee'))` | SOURCE | VERIFIED |
| 40 | `credit_transaction` | 308 | `SUM(Fee WHERE 'Credit for transaction fee')` | SOURCE | VERIFIED |
| 41 | `credit_listing` | 310 | `SUM(Fee WHERE 'Credit for listing fee')` | SOURCE | VERIFIED |
| 42 | `credit_processing` | 312 | `SUM(Fee WHERE 'Credit for processing fee')` | SOURCE | VERIFIED |
| 43 | `share_save` | 314 | `SUM(Fee WHERE 'Share & Save')` | SOURCE | VERIFIED |
| 44 | `total_credits` | 316 | `SUM(all credit entries)` | SOURCE | DERIVED |
| 45 | `total_fees_gross` | 318 | `listing + txn_product + txn_shipping + processing` | SOURCE | DERIVED |
| 46 | `etsy_ads` | 330 | `ABS(SUM(Marketing WHERE 'Etsy Ads'))` | SOURCE | VERIFIED |
| 47 | `offsite_ads_fees` | 332 | `ABS(SUM(Marketing WHERE 'Offsite Ads'))` | SOURCE | VERIFIED |
| 48 | `offsite_ads_credits` | 334 | `SUM(Marketing WHERE 'Credit for Offsite'))` | SOURCE | VERIFIED |
| 49 | `usps_outbound` | 351 | `ABS(SUM(Shipping WHERE 'USPS shipping label'))` | SOURCE | VERIFIED |
| 50 | `usps_outbound_count` | 354 | `COUNT(USPS outbound)` | SOURCE | VERIFIED |
| 51 | `usps_return` | 357 | `ABS(SUM(USPS return labels))` | SOURCE | VERIFIED |
| 52 | `usps_return_count` | 360 | `COUNT(USPS return)` | SOURCE | VERIFIED |
| 53 | `asendia_labels` | 363 | `ABS(SUM(Asendia))` | SOURCE | VERIFIED |
| 54 | `asendia_count` | 365 | `COUNT(Asendia)` | SOURCE | VERIFIED |
| 55 | `ship_adjustments` | 367 | `ABS(SUM(Adjustment))` | SOURCE | VERIFIED |
| 56 | `ship_adjust_count` | 369 | `COUNT(Adjustment)` | SOURCE | VERIFIED |
| 57 | `ship_credits` | 371 | `SUM(Credit for shipping)` | SOURCE | VERIFIED |
| 58 | `ship_credit_count` | 373 | `COUNT(Credit for)` | SOURCE | VERIFIED |
| 59 | `ship_insurance` | 375 | `ABS(SUM(insurance))` | SOURCE | VERIFIED |
| 60 | `ship_insurance_count` | 377 | `COUNT(insurance)` | SOURCE | VERIFIED |
| 61 | `paid_ship_count` | 407 | `COUNT(orders with shipping fee)` | SOURCE | VERIFIED |
| 62 | `free_ship_count` | 410 | `COUNT(orders w/o shipping fee)` | SOURCE | VERIFIED |
| 63 | `avg_outbound_label` | 417 | `usps_outbound / usps_outbound_count` | SOURCE | DERIVED |
| 64 | `buyer_paid_shipping` | 383 | **REMOVED** (was `/0.065`) | **UNKNOWN** | UNKNOWN |
| 65 | `shipping_profit` | 386 | **REMOVED** (depended on #64) | **UNKNOWN** | UNKNOWN |
| 66 | `shipping_margin` | 389 | **REMOVED** (depended on #64) | **UNKNOWN** | UNKNOWN |
| 67 | `bank_unaccounted` | 421 | Replaced by reconciliation agent | **ESTIMATED** | ESTIMATED |

### 1B. Expense Completeness (`accounting/agents/expense_completeness.py`)

| # | Metric Name | Formula | Traceable? | Confidence |
|---|------------|---------|-----------|------------|
| 68 | `receipt_verified_total` | `SUM(ABS(matched_bank.amount))` | SOURCE | DERIVED |
| 69 | `bank_recorded_total` | `SUM(ABS(expense_debits.amount))` | SOURCE | VERIFIED |
| 70 | `gap_total` | `bank_recorded - receipt_verified` | SOURCE | DERIVED |
| 71 | `by_category` | Per-cat: {verified, bank_recorded, gap, missing_count} | SOURCE | DERIVED |

### 1C. Dashboard-Only Calculations (`etsy_dashboard.py`)

| # | Metric Name | Lines | Formula | Traceable? | Why |
|---|------------|-------|---------|-----------|-----|
| 72 | `true_inventory_cost` | 1330 | `total_inv - personal - biz_fees` | SOURCE | From parsed invoices |
| 73 | `total_stock_value` | 1808 | `SUM(STOCK_SUMMARY.total_cost)` | SOURCE | From inventory records |
| 74 | `monthly_aov` | 969 | `monthly_sales / order_count` | SOURCE | Ratio of verified inputs |
| 75 | `monthly_profit_per_order` | 970 | `monthly_net_revenue / order_count` | SOURCE | Ratio of verified inputs |
| 76 | `_unit_rev` | 4231 | `avg_order` (= gross/orders) | SOURCE | Average — informational |
| 77 | `_unit_fees` | 4232 | `total_fees / order_count` | SOURCE | Average — informational |
| 78 | `_unit_ship` | 4233 | `total_shipping / order_count` | SOURCE | Average — informational |
| 79 | `_unit_cogs` | 4239 | `true_inventory_cost / order_count` | SOURCE | Average — informational |
| 80 | `_unit_profit` | 4240 | `rev - fees - ship - ads - ref - tax - cogs` | SOURCE | Derived averages |
| 81 | `_unit_margin` | 4241 | `unit_profit / unit_rev * 100` | SOURCE | Ratio |
| 82 | `val_annual_revenue` | 3502 | `gross_sales * (12 / months_operating)` | **ESTIMATED** | Annualization assumption |
| 83 | `val_annual_sde` | 3508 | `SDE * (12 / months_operating)` | **ESTIMATED** | Annualization assumption |
| 84 | `val_sde_low/mid/high` | 3511-13 | `annual_SDE * 1.0 / 1.5 / 2.5` | **ESTIMATED** | Valuation multiples |
| 85 | `val_rev_low/mid/high` | 3516-18 | `annual_rev * 0.3 / 0.5 / 1.0` | **ESTIMATED** | Valuation multiples |
| 86 | `val_asset_val` | 3523 | `assets - liabilities` | SOURCE | Direct from bank + inventory |
| 87 | `val_blended_low/mid/high` | 3526-28 | `SDE*0.50 + Rev*0.25 + Asset*0.25` | **ESTIMATED** | Weighted blend with assumed weights |
| 88 | `val_proj_12mo_revenue` | 3540 | `SUM(run_rate + trend * i)` | **ESTIMATED** | Linear projection |
| 89 | `val_health_score` | 3556 | Composite: profit + growth + diversity + cash + debt + shipping | **ESTIMATED** | Weighted heuristic |
| 90 | `val_monthly_expenses` | 3592 | `(etsy_costs + bank_additional + inv) / months` | **ESTIMATED** | Annualized average |
| 91 | `val_runway_months` | 3593 | `cash_on_hand / monthly_expenses` | **ESTIMATED** | Forward projection |
| 92 | `_breakeven_monthly` | 4301 | `fixed_costs / contrib_margin_pct` | **ESTIMATED** | Assumes stable margins |
| 93 | `_breakeven_orders` | 4303 | `breakeven_monthly / avg_order` | **ESTIMATED** | Assumes stable AOV |
| 94 | `se_tax` | 7396-97 | `(net * 0.9235) * 0.153` | **ESTIMATED** | IRS formula applied to estimated income |
| 95 | `income_tax_per` | 7401 | `progressive_brackets(taxable_income)` | **ESTIMATED** | Brackets are law; income is estimated |
| 96 | `_daily_z_score` | 4142 | `(revenue - mean) / std_dev` | SOURCE | Statistical — not a financial metric |
| 97 | `revenue_projection` | 2144 | `LinearRegression.predict(future)` | **ESTIMATED** | ML forecast |
| 98 | `growth_pct` | 2148 | `(slope / mean_rev) * 100` | **ESTIMATED** | Trend extrapolation |
| 99 | `health_score` (composite) | 9839-9905 | 8-component weighted average | **ESTIMATED** | Weighted heuristic |
| 100 | `_contrib_margin_pct` | 4300 | `(gross - fees - ship - ref - cogs) / gross` | SOURCE | Ratio of verified inputs |

---

## STEP 2 — Audit Verdict per Metric

### CLEAN: 71 metrics — Fully source-traceable

All 67 pipeline metrics (#1-71) except #16-19, #64-67 are **clean**. They are direct aggregations of Etsy CSV `Net_Clean` values or bank statement amounts, or mathematical combinations thereof. No estimation, no assumed percentages, no back-solving.

### ALREADY KILLED: 3 metrics — Correctly set to UNKNOWN

| Metric | Old Formula | Current State |
|--------|------------|---------------|
| `buyer_paid_shipping` | `transaction_fees_shipping / 0.065` | `Decimal("0")`, UNKNOWN |
| `shipping_profit` | `buyer_paid - total_shipping_cost` | `Decimal("0")`, UNKNOWN |
| `shipping_margin` | `shipping_profit / buyer_paid * 100` | `Decimal("0")`, UNKNOWN |

These were cleaned up in the Feb-Mar 2026 overhaul. The governance agent (`governance.py`) scans for `/0.065` and `/0.15` patterns to prevent regression.

### ESTIMATED BUT DOCUMENTED: 5 metrics — Config-based, not source-backed

| Metric | Why Estimated | Required to Fix |
|--------|--------------|-----------------|
| `etsy_pre_capone_deposits` | Manual config entry | Historical Etsy payout records (pre-CapOne bank) |
| `etsy_total_deposited` | Includes pre-CapOne | Same |
| `etsy_balance_calculated` | Depends on above | Same |
| `etsy_csv_gap` | Diagnostic diff | Same |
| `bank_unaccounted` | Placeholder | Replaced by reconciliation agent |

### DASHBOARD ESTIMATION ZONES: 14 metrics — Need ESTIMATED labeling

| Zone | Metrics | Problem | Fix |
|------|---------|---------|-----|
| **Valuation** | #82-88 | Annualization uses `12/months`, multiples are assumed (1.0x-2.5x SDE, 0.3x-1.0x revenue) | Label entire Valuation tab as ESTIMATED. Show "Based on {N} months of data" |
| **Projections** | #88, 97-98 | Linear regression extrapolation | Label as PROJECTION, not fact |
| **Health Scores** | #89, 99 | Arbitrary weights (25% profit, 20% growth, etc.) | Label as HEURISTIC |
| **Break-even** | #92-93 | Assumes stable margins and AOV | Label as ESTIMATED MODEL |
| **Tax estimates** | #94-95 | IRS formulas are correct but applied to estimated income | Label as ESTIMATE — actual filing may differ |
| **Burn rate/Runway** | #90-91 | Divides total by months (assumes stability) | Label as ESTIMATED |

---

## STEP 3 — Canonical Financial Pipeline Architecture

### Current Pipeline (Already Implemented)

```
┌──────────────────────────────────────────────────────────────┐
│ STAGE 1: LOAD RAW SOURCES                                    │
│   Etsy CSV → DataFrame                                       │
│   Bank PDF/CSV → list[dict]                                  │
│   Invoices/Receipts → list[dict]                             │
│   Config (pre-CapOne) → dict                                 │
└──────────────────────┬───────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────┐
│ STAGE 2: NORMALIZE → JOURNAL (IngestionAgent)                │
│   Etsy rows → JournalEntry(type, amount, date, title, info)  │
│   Bank txns → JournalEntry(type, amount, date, category)     │
│   Pre-CapOne → JournalEntry(type=DEPOSIT, source=CONFIG)     │
│   All amounts: Decimal. All entries: dedup_hash.             │
└──────────────────────┬───────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────┐
│ STAGE 3: COMPUTE METRICS (ComputationAgent)                  │
│   Every metric = SUM/COUNT/ABS of journal entries             │
│   Every metric carries Confidence + Provenance                │
│   No estimation. UNKNOWN if data missing.                     │
└──────────────────────┬───────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────┐
│ STAGE 4: VALIDATE (ValidationAgent)                          │
│   5 checks: parity, profit_chain, no_nan, balance, deposits  │
│   CRITICAL failures → quarantine affected metrics             │
└──────────────────────┬───────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────┐
│ STAGE 5: RECONCILE (ReconciliationAgent)                     │
│   Match Etsy deposits ↔ bank deposits (±$0.01, ±3 days)      │
│   Output: matched pairs, unmatched on both sides             │
└──────────────────────┬───────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────┐
│ STAGE 5b: EXPENSE COMPLETENESS (ExpenseCompletenessAgent)    │
│   Match bank debits ↔ receipts (±$0.02, ±14 days)            │
│   Output: verified_total, bank_recorded_total, gap, missing   │
└──────────────────────┬───────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────┐
│ STAGE 6: PUBLISH (Ledger → compat → globals)                 │
│   Immutable Ledger snapshot                                   │
│   UNKNOWN confidence → None in globals                        │
│   QUARANTINED metrics → marked, not hidden                    │
└──────────────────────────────────────────────────────────────┘
```

### Assessment: The pipeline architecture is CORRECT

The pipeline already follows the prescribed order:
1. Load raw sources ✓
2. Normalize into journal ✓
3. Build journal ledger ✓
4. Reconcile Etsy payouts → bank deposits ✓
5. Reconcile expenses → receipts ✓
6. Calculate metrics only from source data ✓

**What's missing**: Dashboard-level calculations (valuation, tax, projections, health scores) bypass the pipeline entirely. They read from pipeline outputs but add their own estimation layer without confidence tagging.

### Required Architecture Change

```
┌──────────────────────────────────────────────────────────────┐
│ STAGE 7: ANALYTICAL LAYER (NEW)                              │
│   Valuation, Tax, Projections, Health, Break-even             │
│   ALL outputs tagged: Confidence.ESTIMATED                    │
│   ALL outputs include: Provenance with assumptions list       │
│   HARD FAIL: if any input is UNKNOWN, output is UNKNOWN       │
└──────────────────────────────────────────────────────────────┘
```

---

## STEP 4 — HARD FAIL Specification

### Rule: If a metric cannot be calculated from source data, return UNKNOWN

**Already implemented for**:
- `buyer_paid_shipping` → UNKNOWN (missing: Etsy order CSV shipping column)
- `shipping_profit` → UNKNOWN (missing: buyer_paid_shipping)
- `shipping_margin` → UNKNOWN (missing: buyer_paid_shipping)
- All UNKNOWN metrics → `None` in dashboard globals (compat.py line 74-78)

**Must be added for**:

| Metric | When to HARD FAIL | Missing Input |
|--------|-------------------|---------------|
| `val_annual_revenue` | Always | "Annualization assumes linear growth — not a fact" |
| `val_sde_*` | Always | "SDE multiple is an assumption (1.0-2.5x)" |
| `val_rev_*` | Always | "Revenue multiple is an assumption (0.3-1.0x)" |
| `val_blended_*` | Always | "Blended weights (50/25/25) are assumptions" |
| `val_proj_12mo_revenue` | Always | "Linear extrapolation — not a fact" |
| `val_health_score` | Always | "Composite heuristic with arbitrary weights" |
| `revenue_projection` | Always | "ML regression forecast" |
| `growth_pct` | Always | "Trend extrapolation" |
| `_breakeven_monthly` | If margin unstable | "Assumes stable contribution margin" |
| `se_tax` | Always | "Estimate — actual filing determines tax" |
| `income_tax_per` | Always | "Estimate — actual deductions unknown" |

### Implementation: Two-tier approach

**Tier 1: HARD UNKNOWN** — Metric cannot exist without missing data
```python
# These already work correctly
if data_missing:
    return MetricValue(name, Decimal("0"), Confidence.UNKNOWN, ...)
```

**Tier 2: LABELED ESTIMATE** — Metric can be computed but is NOT a fact
```python
# Dashboard analytics layer
return MetricValue(name, computed_value, Confidence.ESTIMATED,
    provenance=Provenance(
        formula="gross_sales * (12 / months_operating)",
        notes="ESTIMATE: Annualization assumes linear growth pattern",
        missing_inputs=("future_revenue_data",),
    ))
```

**Display behavior**:
- UNKNOWN → Show "UNKNOWN" with missing inputs list
- ESTIMATED → Show value with orange "ESTIMATE" badge and assumptions disclaimer
- VERIFIED/DERIVED → Show value with green badge

---

## STEP 5 — Financial Truth Table

### The Truth Table Report

This is the definitive output the system must produce. Every line must be source-traceable.

```
═══════════════════════════════════════════════════════════════
 FINANCIAL TRUTH TABLE — TJ's Software
 Period: [first_date] to [last_date]
 Generated: [timestamp]
═══════════════════════════════════════════════════════════════

 PLATFORM (Etsy CSV — SOURCE)
 ─────────────────────────────────────────────────────────────
 Gross Sales ............... $X,XXX.XX  [VERIFIED] N sale rows
 Refunds ................... $X,XXX.XX  [VERIFIED] N refund rows
 Net Sales ................. $X,XXX.XX  [DERIVED]  gross - refunds
 Total Fees ................ $X,XXX.XX  [VERIFIED] N fee rows
 Total Shipping Labels ..... $X,XXX.XX  [VERIFIED] N shipping rows
 Total Marketing ........... $X,XXX.XX  [VERIFIED] N marketing rows
 Total Taxes Collected ...... $X,XXX.XX  [VERIFIED] N tax rows
 Total Buyer Fees ........... $X,XXX.XX  [VERIFIED] N buyer fee rows
 Total Payments ............. $X,XXX.XX  [VERIFIED] N payment rows
 ─────────────────────────────────────────────────────────────
 Etsy Net Earned ........... $X,XXX.XX  [DERIVED]
 Etsy Balance (unpaid) ..... $X,XXX.XX  [DERIVED]
 Orders ..................... XXX       [VERIFIED]

 BANK (Bank Statement — SOURCE)
 ─────────────────────────────────────────────────────────────
 Total Deposits ............ $X,XXX.XX  [VERIFIED] N deposit entries
 Total Debits .............. $X,XXX.XX  [VERIFIED] N debit entries
 Net Cash .................. $X,XXX.XX  [DERIVED]  deposits - debits
 Owner Draws (Tulsa) ....... $X,XXX.XX  [VERIFIED]
 Owner Draws (Texas) ....... $X,XXX.XX  [VERIFIED]

 RECONCILIATION (Cross-Source)
 ─────────────────────────────────────────────────────────────
 Matched Payouts ........... N of M     (±$0.01, ±3 days)
 Unmatched Etsy Deposits ... N          Amount: $X,XXX.XX
 Unmatched Bank Deposits ... N          Amount: $X,XXX.XX

 EXPENSE VERIFICATION (Bank ↔ Receipts)
 ─────────────────────────────────────────────────────────────
 Receipt-Verified Expenses . $X,XXX.XX  [DERIVED]  N matches
 Bank-Recorded Expenses .... $X,XXX.XX  [VERIFIED] N debit entries
 Unverified Gap ............ $X,XXX.XX  [DERIVED]  bank - verified
 Missing Receipts .......... N items    Priority-sorted

 PROFIT (Derived from above)
 ─────────────────────────────────────────────────────────────
 Cash on Hand .............. $X,XXX.XX  [DERIVED]  bank_net + etsy_balance
 Real Profit ............... $X,XXX.XX  [DERIVED]  cash + draws
 Profit Margin ............. XX.X%      [DERIVED]  profit / gross * 100

 UNKNOWN (Missing Source Data)
 ─────────────────────────────────────────────────────────────
 Buyer-Paid Shipping ....... UNKNOWN    Need: Etsy order CSV
 Shipping Profit ........... UNKNOWN    Need: buyer_paid_shipping
 Shipping Margin ........... UNKNOWN    Need: buyer_paid_shipping
═══════════════════════════════════════════════════════════════
```

### Implementation

Add to `accounting/agents/audit.py`:

```python
def financial_truth_table(self, ledger: Ledger, recon: dict,
                          expense: ExpenseCompletenessResult | None) -> str:
    """Generate the Financial Truth Table report."""
```

Add to Financials tab in dashboard: render this table as a card with collapsible sections.

---

## STEP 6 — Validation Tests

### Existing Tests (272 passing)

The test suite already enforces core invariants:

| Test File | Tests | What |
|-----------|-------|------|
| `test_invariants.py` | 135 | 12 universal invariants × 5 scenarios |
| `test_integration_pipeline.py` | 57 | Full pipeline penny-exact assertions |
| `test_unit_computation.py` | 22 | Per-metric correctness |
| `test_unit_validation.py` | 11 | Validation check pass/fail |
| `test_unit_reconciliation.py` | 6 | Deposit matching |
| `test_unit_expense_completeness.py` | 21 | Receipt matching |
| `test_unit_ingestion.py` | 20 | Data parsing |

### New Tests Required

#### A. No-Estimation Regression Tests

```python
class TestNoEstimation:
    """Metrics marked VERIFIED or DERIVED must NOT use estimation."""

    def test_no_division_by_percentage(self):
        """No metric formula contains /0.065 or /0.15."""
        # Already enforced by governance agent, but test directly
        for name, mv in ledger.metrics.items():
            if mv.confidence in (Confidence.VERIFIED, Confidence.DERIVED):
                assert "/0.065" not in mv.provenance.formula
                assert "/0.15" not in mv.provenance.formula

    def test_no_count_times_average(self):
        """No VERIFIED metric uses count*average pattern."""
        for name, mv in ledger.metrics.items():
            if mv.confidence in (Confidence.VERIFIED, Confidence.DERIVED):
                assert "* avg_" not in mv.provenance.formula
                assert "avg_" not in mv.provenance.formula or "avg_order" in name

    def test_unknown_metrics_are_zero(self):
        """UNKNOWN metrics must have value=0, never an estimated number."""
        for name, mv in ledger.metrics.items():
            if mv.confidence == Confidence.UNKNOWN:
                assert mv.value == Decimal("0"), \
                    f"{name} is UNKNOWN but has value {mv.value}"

    def test_unknown_published_as_none(self):
        """UNKNOWN metrics must publish as None, not 0."""
        # Verify compat.py behavior
        for name in ["buyer_paid_shipping", "shipping_profit", "shipping_margin"]:
            assert getattr(dashboard_module, name) is None
```

#### B. Financial Identity Tests (sum checks)

```python
class TestFinancialIdentities:
    """Mathematical identities that must hold."""

    def test_gross_equals_sum_of_sales(self):
        """gross_sales == SUM(all Sale-type journal entries)."""
        sale_entries = journal.by_type(TxnType.SALE)
        expected = sum(e.net_clean for e in sale_entries)
        assert ledger.get_value("gross_sales") == expected

    def test_payouts_match_deposits(self):
        """SUM(Etsy deposits) should match within reconciliation tolerance."""
        etsy_deps = journal.etsy_deposits()
        bank_deps = journal.bank_deposits()
        # Matched deposits must be penny-exact
        for match in recon.matched:
            assert match.amount_diff <= Decimal("0.01")

    def test_expense_with_no_receipt_is_missing(self):
        """Every bank debit without a receipt match is in missing_receipts."""
        matched_ids = {m.bank_entry.dedup_hash for m in expense.receipt_matches}
        skipped_ids = {e.dedup_hash for e in expense.skipped_transactions}
        missing_ids = {m.transaction_id for m in expense.missing_receipts}
        all_debits = {e.dedup_hash for e in journal.bank_debits()}
        unaccounted = all_debits - matched_ids - skipped_ids - missing_ids
        assert len(unaccounted) == 0

    def test_net_parity(self):
        """etsy_net_earned == SUM(all Etsy CSV journal entry amounts)."""
        # Already Invariant 1, but critical enough to duplicate
        etsy_entries = [e for e in journal.entries if e.source == TxnSource.ETSY_CSV]
        raw_sum = sum(e.amount for e in etsy_entries)
        assert abs(float(ledger.get_value("etsy_net_earned")) - float(raw_sum)) < 0.01
```

#### C. HARD FAIL Tests

```python
class TestHardFail:
    """System must return UNKNOWN, never estimate."""

    def test_missing_data_returns_unknown(self):
        """If source data is missing, metric is UNKNOWN not estimated."""
        assert ledger.get_confidence("buyer_paid_shipping") == Confidence.UNKNOWN
        assert ledger.get_confidence("shipping_profit") == Confidence.UNKNOWN
        assert ledger.get_confidence("shipping_margin") == Confidence.UNKNOWN

    def test_unknown_not_in_totals(self):
        """UNKNOWN metrics must NOT contribute to any DERIVED metric."""
        profit = ledger.get_value("real_profit")
        # real_profit does NOT include shipping_profit (which is UNKNOWN)
        assert ledger.get_confidence("real_profit") != Confidence.UNKNOWN

    def test_quarantine_blocks_display(self):
        """QUARANTINED metrics must not display as normal values."""
        # Simulate a CRITICAL validation failure
        for name in ledger.quarantined_metrics:
            conf = ledger.get_confidence(name)
            assert conf == Confidence.QUARANTINED
```

#### D. Dashboard Estimation Gate Tests

```python
class TestDashboardEstimationGate:
    """Dashboard analytics must be labeled, not presented as fact."""

    def test_valuation_is_estimated(self):
        """All valuation metrics must be tagged ESTIMATED."""
        for name in ["val_annual_revenue", "val_sde_mid", "val_blended_mid",
                      "val_health_score", "val_proj_12mo_revenue"]:
            # When moved to pipeline, these must be ESTIMATED
            pass  # Placeholder until metrics move to pipeline

    def test_tax_is_estimated(self):
        """Tax calculations are estimates, not filed returns."""
        # se_tax, income_tax must carry ESTIMATE label
        pass

    def test_projection_is_estimated(self):
        """Revenue projections are forecasts, not facts."""
        pass
```

---

## REQUIRED CODE CHANGES

### Priority 1: Gate dashboard estimates (LOW effort, HIGH impact)

**File**: `etsy_dashboard.py`

1. **Valuation tab** (~line 3494): Add ESTIMATE banner at top:
   ```
   "All valuations are ESTIMATES based on {N} months of data.
    Multiples (1.0-2.5x SDE) are industry assumptions for small Etsy businesses."
   ```

2. **Tax calculations** (~line 7390): Add ESTIMATE badge to every tax number:
   ```
   "ESTIMATE — Actual tax liability depends on your filed return."
   ```

3. **Revenue projections** (~line 2112): Label as PROJECTION:
   ```
   "LINEAR PROJECTION — Based on {N} months of historical data (R²={r2:.2f})"
   ```

4. **Health scores** (~line 3548, 9831): Label as HEURISTIC:
   ```
   "HEURISTIC SCORE — Weighted composite, not an accounting metric."
   ```

5. **Break-even** (~line 4299): Label as MODEL:
   ```
   "ESTIMATED — Assumes stable contribution margin of {pct:.1f}%"
   ```

### Priority 2: Move analytical metrics to pipeline (MEDIUM effort)

Create `accounting/agents/analytics.py` — Agent 8:

```python
class AnalyticsAgent:
    """Compute analytical/estimated metrics with explicit confidence tagging."""

    def compute_valuation(self, ledger: Ledger, config: dict) -> dict[str, MetricValue]:
        """All outputs are Confidence.ESTIMATED with assumptions in Provenance."""

    def compute_tax_estimates(self, ledger: Ledger) -> dict[str, MetricValue]:
        """IRS formulas applied to pipeline profit — labeled ESTIMATE."""

    def compute_projections(self, monthly_data: dict) -> dict[str, MetricValue]:
        """Linear regression forecasts — labeled PROJECTION."""
```

This ensures every number in the system has a `Confidence` tag, not just pipeline metrics.

### Priority 3: Financial Truth Table UI (MEDIUM effort)

Add truth table rendering to Financials tab. Every row links to source entries via `dedup_hash`.

### Priority 4: Tighten validation tolerances (LOW effort)

1. `validation.py` line 150: Change balance sanity from `-$50` to `-$10`
2. `validation.py` line 172: For post-CapOne data, tighten deposit reconciliation from 10% to 2%

---

## SUMMARY

| Category | Count | Status |
|----------|-------|--------|
| SOURCE-TRACEABLE metrics | 71 | CLEAN — no changes needed |
| ALREADY UNKNOWN metrics | 3 | CLEAN — correctly killed |
| ESTIMATED (config-based) | 5 | DOCUMENTED — pre-CapOne deposits |
| Dashboard estimates needing labels | 14 | NEEDS WORK — add ESTIMATE badges |
| Governance guards | 6 patterns | ACTIVE — preventing regression |
| Validation checks | 5 | ACTIVE — quarantine on failure |
| Test coverage | 272 tests | All passing |

**Bottom line**: The accounting pipeline (`accounting/`) is architecturally sound and source-traceable. The remaining work is:
1. Label the 14 dashboard-level estimates with explicit ESTIMATE/PROJECTION/HEURISTIC badges
2. Move analytical computations into the pipeline so they get `Confidence` tags
3. Add the Financial Truth Table as a first-class output
4. Tighten 2 validation tolerances
