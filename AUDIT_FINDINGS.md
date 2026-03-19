# Codebase Audit: Every Calculation Not Directly Supported by Primary Data

**Audited:** 2026-03-04
**Files scanned:** `etsy_dashboard.py`, `accounting/agents/*.py`, `accounting/compat.py`,
`supabase_loader.py`, `_parse_bank_statements.py`, `_parse_invoices.py`
**Rule:** If a number can't be traced to a specific row in a source document, it is INVALID.

---

## KEY: The "370 sales x avg $51 = $19k" Test

> Any time a count is multiplied by an average to produce a dollar total,
> that total is an ESTIMATE unless the underlying N row-level transactions
> exist and the average is a real computed mean from those same rows.

`avg_order = gross_sales / order_count` is a **valid computed mean** (it divides the
actual sum of 370 real `Net_Clean` values by their count).

Using `order_count * avg_order` to reconstruct `gross_sales` is a **circular identity** —
harmless but pointless. The violation occurs when `avg_X * count_Y` is used to
**infer a new dollar total that was never directly measured**.

---

## SECTION 1: AVERAGES x COUNTS

| # | File:Line | Function/Context | Metric Impacted | Current Logic | Why Invalid | Replacement Logic | Required Data |
|---|-----------|-----------------|-----------------|---------------|-------------|-------------------|---------------|
| 1 | `etsy_dashboard.py:9985` | `_generate_actions()` | Restock cost estimate | `reorder_cost = avg_unit_cost * len(oos_df)` | **VALID** — `avg_unit_cost` is per-SKU `total_cost / total_purchased` from actual invoices. Each SKU has a real cost. Multiplying by count of distinct OOS SKUs is a legitimate reorder estimate. | No change needed. | Actual invoice data (already present in `INV_ITEMS`) |
| 2 | `etsy_dashboard.py:10059` | `_generate_actions()` AOV recovery | Action item impact | `monthly_impact = abs(aov_decline) * val_monthly_run_rate` | **ESTIMATE** — multiplies a % decline by a run rate to guess dollar impact. No per-order data proves this specific dollar loss. | Label as "ESTIMATE" or remove dollar impact. Set `impact = None`. | Per-listing conversion data, actual before/after order values |
| 3 | `etsy_dashboard.py:10028` | `_generate_actions()` ad review | Action item impact | `potential_save = total_marketing * 0.3` | **ESTIMATE** — assumes 30% of ad spend is wasteable. No data supports this specific percentage. | Label as "ESTIMATE" or remove. Set `impact = None`. | Etsy Ads per-listing ROI report (which listings are unprofitable) |
| 4 | `etsy_dashboard.py:10041` | `_generate_actions()` Share & Save | Action item impact | `potential_credits = gross_sales * 0.01` | **ESTIMATE** — assumes 1% savings from Share & Save. Based on Etsy's published program rate, but actual savings depend on sharing behavior. | Label as "ESTIMATE" or set `impact = None`. | Actual Share & Save credit history (already have `share_save` total) |

### REMOVED (Previously Violations, Now Fixed)

| # | File:Line | Was | Status |
|---|-----------|-----|--------|
| R1 | `etsy_dashboard.py:1916` | `paid_ship_count * avg_outbound_label` | `None` — REMOVED |
| R2 | `etsy_dashboard.py:1918` | `free_ship_count * avg_outbound_label` | `None` — REMOVED |
| R3 | `etsy_dashboard.py:1926` | `len(refunded_order_ids) * avg_outbound_label` | `None` — REMOVED |
| R4 | `etsy_dashboard.py:9997` | `len(oos_df) * avg_order * 0.5` | `None` — REMOVED |

---

## SECTION 2: IMPLIED REVENUE FROM FEES/PERCENTAGES

| # | File:Line | Function/Context | Metric Impacted | Current Logic | Why Invalid | Replacement Logic | Required Data |
|---|-----------|-----------------|-----------------|---------------|-------------|-------------------|---------------|
| — | — | — | — | **All /0.065 and /0.15 back-solves have been removed.** | — | — | — |

### REMOVED (Previously Violations, Now Fixed)

| # | File:Line | Was | Status |
|---|-----------|-----|--------|
| R5 | `computation.py:384` | `transaction_fees_shipping / 0.065` | UNKNOWN — REMOVED |
| R6 | `etsy_dashboard.py:1003,1896` | `transaction_fees_shipping / 0.065` (two copies) | `None` — REMOVED |
| R7 | `etsy_dashboard.py:866` | `product_fee_totals / 0.065` | Row-level sum — REPLACED |
| R8 | `etsy_dashboard.py:2267` | `offsite_ads_fees / 0.15` | Text says UNKNOWN — REMOVED |
| R9 | `etsy_dashboard.py:1925` | `refund_ship_fees / 0.065` | `None` — REMOVED |

---

## SECTION 3: FALLBACK DEFAULTS MASKING MISSING DATA

| # | File:Line | Function/Context | Metric Impacted | Current Logic | Why Invalid | Replacement Logic | Required Data |
|---|-----------|-----------------|-----------------|---------------|-------------|-------------------|---------------|
| 5 | `supabase_loader.py:632` | `_parse_money()` | Every monetary field | Returns `0.0` on parse failure (any exception → `return 0.0`) | **SILENT DATA LOSS** — a malformed "$1,23.45" becomes $0.00 with no warning. Impossible to distinguish "legitimately zero" from "parse failure". | Return `None` on failure; let caller decide. Log parse failures. | Already have source CSVs |
| 6 | `supabase_loader.py:634` | `_parse_money()` | Every monetary field | `"--"` → `0.0` | **ACCEPTABLE** — Etsy uses `"--"` to mean "not applicable" (e.g., no fees on a deposit). Zero is semantically correct here. | No change needed. Keep `0.0` for `"--"`. | N/A |
| 7 | `etsy_dashboard.py:3513-3515` | Valuation setup | Growth projections | `analytics_projections.get("growth_pct", 0)`, `.get("r2_sales", 0)`, `.get("sales_trend", 0)` | **DATA MASKING** — if projections fail to compute (< 3 months of data), growth defaults to 0% rather than UNKNOWN. This makes a young business look "stable" instead of "insufficient data". | Return `None` and display "Insufficient data for projection" in valuation. | 3+ months of revenue data |
| 8 | `accounting/compat.py:103` | `publish_to_globals` | bank_unaccounted | `ledger.get_float("bank_unaccounted", 0.0)` | **TRANSITION ARTIFACT** — legacy metric replaced by reconciliation agent. Default 0.0 hides any actual unaccounted amount if metric is missing from ledger. | Remove this metric entirely or compute from reconciliation. | Reconciliation agent output |
| 9 | `computation.py:383-392` | Shipping metrics | buyer_paid_shipping | `Decimal("0")` with `Confidence.UNKNOWN` | **HONEST BUT MISLEADING** — Value is 0 (not None) in the MetricValue. Compat.py correctly publishes `None`, but the Ledger stores `Decimal("0")` which could confuse internal code. | Consider using a sentinel value or optional Decimal field. | Etsy order-level CSV |
| 10 | `etsy_dashboard.py:2094-2096` | `run_analytics()` cost ratios | Monthly cost breakdowns | `if s <= 0: continue` — skips months with zero sales | **ACCEPTABLE** — can't compute fee/sales ratio with zero denominator. Skipping is correct. | No change needed. | N/A |

---

## SECTION 4: HARDCODED ASSUMPTIONS

### 4A: IRS/Government Constants (VALID — Published Facts)

| # | File:Line | Constant | Value | Source | Risk |
|---|-----------|----------|-------|--------|------|
| — | `etsy_dashboard.py:3603` | Federal tax brackets 2026 | 10/12/22/24/32/35/37% | IRS Rev. Proc. | **LOW** — must update annually |
| — | `etsy_dashboard.py:8633` | SS wage base 2025/2026 | $168,600 / $176,100 | SSA announcement | **LOW** — must update annually |
| — | `etsy_dashboard.py:8647` | IRS mileage rate 2025 | $0.70/mi | IRS Notice 2024-08 | **LOW** — must update annually |
| — | `etsy_dashboard.py:7370` | SE tax rate | 15.3% (12.4% + 2.9%) | IRC §1401 | **NONE** — stable law |
| — | `etsy_dashboard.py:8213` | SE income factor | 92.35% | IRC §1402(a) | **NONE** — stable law |
| — | `etsy_dashboard.py:8811` | SEP-IRA limit 2025 | $69,000 | IRS Notice | **LOW** — must update annually |

### 4B: Tax Assumptions (VALID STRUCTURE, WRONG SCOPE)

| # | File:Line | Function/Context | Metric Impacted | Current Logic | Why Invalid | Replacement Logic | Required Data |
|---|-----------|-----------------|-----------------|---------------|-------------|-------------------|---------------|
| 11 | `etsy_dashboard.py:3588-3598` | `_compute_income_tax()` | Estimated income tax | Hardcoded for **single filer**, standard deduction, no other income, federal only | **INCOMPLETE** — assumes filing status. Two partners in an LLC could be MFJ, have W-2 income, live in different states with state income tax. Result could be off by 2-5x. | Accept `filing_status`, `other_income`, `state` as parameters. Display all assumptions prominently. | Filing status, total household income, state of residence, itemized deductions |
| 12 | `etsy_dashboard.py:8485-8490` | Tax per-year build | SE tax per partner | Assumes 50/50 partnership split is equal | **ACCEPTABLE** — LLC operating agreement says 50/50. This is a business fact, not an assumption. | No change needed. | LLC operating agreement (already documented) |

### 4C: Valuation Multiples (SPECULATIVE BY NATURE)

| # | File:Line | Function/Context | Metric Impacted | Current Logic | Why Invalid | Replacement Logic | Required Data |
|---|-----------|-----------------|-----------------|---------------|-------------|-------------------|---------------|
| 13 | `etsy_dashboard.py:3489-3491` | Valuation | Business value (SDE) | SDE x 1.0/1.5/2.5 | **SPECULATIVE** — multiples are industry convention, not data. No two businesses sell at the same multiple. Handmade Etsy shops may sell for 0.5-3x SDE. | Label as SPECULATIVE. Move multiples to `config.json`. Add "Source: BizBuySell small business data" or similar. | Comparable sales data (none available) |
| 14 | `etsy_dashboard.py:3494-3496` | Valuation | Business value (Revenue) | Revenue x 0.3/0.5/1.0 | Same as #13 | Same as #13 | Same as #13 |
| 15 | `etsy_dashboard.py:3504-3506` | Valuation | Blended value | 50% SDE + 25% Revenue + 25% Asset | **UNJUSTIFIED** — no documentation of why these specific weights | Move to config. Document reasoning. | Appraisal methodology reference |
| 16 | `etsy_dashboard.py:7626-7628` | Valuation (alt) | Blended value (v2) | 70% SDE + 25% Profit + 5% Revenue | **CONFLICTING** — different weighting than #15. Two methods, no reconciliation. | Pick one or make configurable. Document which is used where. | N/A |
| 17 | `etsy_dashboard.py:7539-7551` | Valuation | Track record weight | <3mo: 30%, 3-12mo: 60%, 12+mo: 90% | **ASSUMPTION** — reasonable but arbitrary thresholds | Move to config. Document reasoning. | N/A |
| 18 | `etsy_dashboard.py:7581` | Valuation | Risk discount floor | Minimum 50% discount | **ASSUMPTION** — prevents negative valuations | Move to config. | N/A |

### 4D: Health Score Weights (ARBITRARY)

| # | File:Line | Function/Context | Metric Impacted | Current Logic | Why Invalid | Replacement Logic | Required Data |
|---|-----------|-----------------|-----------------|---------------|-------------|-------------------|---------------|
| 19 | `etsy_dashboard.py:9808-9886` | Business health score | Composite health score (0-100) | Hardcoded weights: Profit 25%, Growth 20%, Cash 15%, Fees 10%, Inventory 10%, Orders 10%, Data Quality 5%, Shipping 5% | **ARBITRARY** — no basis for these specific percentages. 25% on profit vs 5% on shipping is an opinion, not a fact. | Move weights to config. Label score as "ADVISORY". | N/A — inherently subjective |
| 20 | `etsy_dashboard.py:3526-3533` | Valuation health score | Health grade (A-D) | Different weights: Profit 25pts, Growth 25pts, Diversity 15pts, Cash 15pts, Debt 10pts, Shipping 10pts | **CONFLICTING** — different weight scheme than #19. Two health scores with different formulas. | Reconcile into one system or label each distinctly. | N/A |
| 21 | `etsy_dashboard.py:9848-9853` | Inventory health sub-score | Inventory health component | OOS penalty: 150x %, Low stock: 50x % | **ARBITRARY** — no basis for 150x vs 50x multipliers | Document reasoning or make configurable. | N/A |

### 4E: Action Item Estimates (ADVISORY)

| # | File:Line | Function/Context | Metric Impacted | Current Logic | Why Invalid | Replacement Logic | Required Data |
|---|-----------|-----------------|-----------------|---------------|-------------|-------------------|---------------|
| 22 | `etsy_dashboard.py:2469` | Goals insight | Advisory text | `gross_sales * 0.10` = "raise prices 10%" | **SCENARIO MATH** — hypothetical, labeled with "~". Not a metric. Assumes same volume at higher price (ignores elasticity). | Acceptable as advisory if labeled "hypothetical scenario". | Price elasticity data (not available for Etsy) |
| 23 | `etsy_dashboard.py:10028` | Ad review action | Impact estimate | `total_marketing * 0.3` = "cut 30% of ad spend" | **GUESS** — no data supports 30% being wasteable | Set `impact = None`. Text: "Review per-listing ad ROI in Etsy Ads dashboard." | Etsy Ads per-listing performance CSV |
| 24 | `etsy_dashboard.py:10041` | Share & Save action | Impact estimate | `gross_sales * 0.01` = "~1% savings" | **ESTIMATE** — based on program structure, not actual behavior | Set `impact = None`. Show actual `share_save` earned to date. | Actual Share & Save history (already have total) |
| 25 | `etsy_dashboard.py:10059` | AOV recovery action | Impact estimate | `abs(aov_decline) * val_monthly_run_rate` | **ESTIMATE** — multiplies a trend by a run rate. No causal proof that recovering AOV yields this exact dollar amount. | Set `impact = None`. Show AOV trend as informational. | Per-order value data (already have it — but causal impact is unknowable) |

---

## SECTION 5: BALANCING/FORCING LOGIC

| # | File:Line | Function/Context | Metric Impacted | Current Logic | Why Invalid | Replacement Logic | Required Data |
|---|-----------|-----------------|-----------------|---------------|-------------|-------------------|---------------|
| 26 | `etsy_dashboard.py:720` | Module-level | `etsy_csv_gap` | `etsy_balance_calculated - etsy_balance` | **VALID** — this is a diagnostic metric that REPORTS the gap, not forces it to zero. Shows difference between formula-derived and deposit-derived balance. | No change. This is correct audit behavior. | N/A |
| 27 | `etsy_dashboard.py:741` | Module-level | `bank_unaccounted` | `etsy_pre_capone_deposits - old_bank_receipted` | **VALID** — reports pre-CapOne gap. Does not force or adjust. | No change. Consider removing if reconciliation agent handles this. | N/A |
| 28 | `computation.py:165-173` | Computation agent | `etsy_csv_gap` | Same formula as #26, computed in pipeline | **VALID** — transparent gap reporting. Confidence = ESTIMATED. | No change. | N/A |

**VERDICT: No balancing/forcing logic found.** All gap variables are diagnostic — they report differences, they do not adjust values to hide them.

---

## SECTION 6: REVENUE PROJECTIONS (sklearn)

| # | File:Line | Function/Context | Metric Impacted | Current Logic | Why Invalid | Replacement Logic | Required Data |
|---|-----------|-----------------|-----------------|---------------|-------------|-------------------|---------------|
| 29 | `etsy_dashboard.py:2118-2138` | `run_analytics()` | Revenue/net projections | `LinearRegression` on 3-6 monthly data points, projects 3 months ahead | **MODEL-BASED ESTIMATE** — inherently speculative. R-squared is reported. With only 3-6 data points, projections have wide confidence intervals. | **Acceptable as-is** IF: (a) clearly labeled "PROJECTION", (b) R-squared displayed, (c) confidence bands shown. All three are already done. | More months of data improve reliability |
| 30 | `etsy_dashboard.py:3517-3520` | Valuation | 12-month projected revenue | `sum(max(0, run_rate + trend * i) for i in 1..12)` | **ESTIMATE** — uses `max(0, ...)` to clamp negative projections. A declining business can't project negative monthly revenue, but the clamp hides the decline's severity. | Report without clamping, or show both clamped and unclamped. Label as "PROJECTED". | N/A |
| 31 | `etsy_dashboard.py:3513` | Valuation | Growth % default | `analytics_projections.get("growth_pct", 0)` | **DATA MASKING** — if projections unavailable, growth = 0% (stable). Should be UNKNOWN. | Return `None`. Display "Insufficient data" in valuation section. | 3+ months of data |

---

## SECTION 7: OLD CODE (Still on Disk)

| # | File:Line | Current Logic | Status |
|---|-----------|---------------|--------|
| 32 | `_etsy_dashboard_old/data_state.py:786` | `buyer_paid_shipping = net_ship_tx_fees / 0.065` | **DEAD CODE** — in backup directory. Not imported. Should be deleted. |
| 33 | `_etsy_dashboard_old/data_state.py:797` | `est_label_cost_paid_orders = paid_ship_count * avg_outbound_label` | Same |
| 34 | `_etsy_dashboard_old/data_state.py:799` | `est_label_cost_free_orders = free_ship_count * avg_outbound_label` | Same |
| 35 | `_etsy_dashboard_old/data_state.py:816` | `est_refund_label_cost = len(refunded_order_ids) * avg_outbound_label` | Same |
| 36 | `_etsy_dashboard_old/data_state.py:856` | `product_revenue_est = (product_fee_totals / 0.065).round(2)` | Same |

---

## SUMMARY SCORECARD

### By Category

| Category | Total Found | VALID | ESTIMATE (label it) | VIOLATION (fix it) | REMOVED (done) |
|----------|-------------|-------|---------------------|-------------------|----------------|
| Avg x Count | 4 active + 4 removed | 1 (#1) | 3 (#2-4) | 0 | 4 (R1-R4) |
| Fee back-solve | 0 active + 5 removed | 0 | 0 | 0 | 5 (R5-R9) |
| Fallback defaults | 6 active | 3 (#6,10,12) | 1 (#7) | 2 (#5,9) | 0 |
| Tax assumptions | 2 | 1 (#12) | 1 (#11) | 0 | 0 |
| Valuation | 6 | 0 | 0 | 6 (#13-18) | 0 |
| Health scores | 3 | 0 | 0 | 3 (#19-21) | 0 |
| Action estimates | 4 | 1 (#22) | 3 (#23-25) | 0 | 0 |
| Balancing logic | 3 | 3 (#26-28) | 0 | 0 | 0 |
| Projections | 3 | 1 (#29) | 1 (#30) | 1 (#31) | 0 |
| Old code | 5 | 0 | 0 | 5 (#32-36) | 0 |
| **TOTAL** | **36 + 9 removed** | **10** | **9** | **17** | **9** |

### Priority Actions

**P0 — Fix Now (Data Integrity):**
- #5: `_parse_money()` silent zero on failure → return None + log
- #9: UNKNOWN metrics store `Decimal("0")` in Ledger → consider sentinel
- #31: Projection growth defaults to 0% → return None

**P1 — Label as ESTIMATE (User Trust):**
- #2, #3, #4, #25: Action item `impact` values → set to None or label "ESTIMATE"
- #11: Tax calculator → add filing status parameter or prominent disclaimer
- #30: 12-month projection `max(0,...)` → show unclamped + label

**P2 — Make Configurable (Maintainability):**
- #13-18: Valuation multiples and weights → move to `config.json`
- #19-21: Health score weights → move to `config.json`
- #15 vs #16: Reconcile two different blended valuation weight schemes

**P3 — Cleanup:**
- #32-36: Delete `_etsy_dashboard_old/` directory

---

## THE "370 x $51" VERDICT

**`product_revenue_est`** in the live code (line 864) computes:
```python
product_revenue_est = _sales_with_product.groupby("Product")["Net_Clean"].sum()
```

This is a **SUM of actual row-level Net_Clean values** grouped by product name.
If there are 370 sales, it sums 370 individual dollar amounts. The result is
**not** `370 * $51`. The $51 average is computed separately as a display metric
(`avg_order = gross_sales / order_count`) and is **never multiplied by a count**
to produce a dollar total in any active code path.

**Status: PASSES the audit.**
