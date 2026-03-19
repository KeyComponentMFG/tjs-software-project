# Audit Agent Team — TJ's Software Financial Dashboard

**Global Rule: NO ESTIMATES.** If data is missing, output `UNKNOWN` and list the
exact source document / API field needed to fill the gap.

---

## Team Roster

| # | Agent | Scope | Depends On |
|---|-------|-------|------------|
| 1 | Data Ingestion Auditor | Parsing, schema, field mapping | — |
| 2 | Etsy Truth Agent | Orders, refunds, shipping, fees, payouts | 1 |
| 3 | Bank Truth Agent | Deposits, withdrawals, categorization | 1 |
| 4 | Receipt/Expense Truth Agent | COGS vs OpEx, vendor normalization | 1 |
| 5 | Reconciliation Agent | Etsy -> Payouts -> Bank matching | 2, 3 |
| 6 | Metrics Integrity Agent | Revenue, profit, tax, valuation | 2, 3, 4, 5 |
| 7 | Fraud/Error Detector Agent | Missing txns, dupes, mismatches | 2, 3, 4 |
| 8 | Governance Agent | Forbidden patterns, code regression | 6 |
| 9 | QA/Test Harness Agent | Unit tests, golden datasets | ALL |

---

## Agent 1: Data Ingestion Auditor

### Mission
Verify that every raw source document (Etsy CSV, bank PDF/CSV, invoice PDF,
config.json) is parsed correctly — no dropped rows, no mangled fields, no
silent zeroes masking parse failures.

### Inputs Needed
- `data/etsy_statements/etsy_statement_*.csv` (6 files, ~3,091 rows)
- `data/bank_statements/*.pdf` (2 PDFs) + `*.csv` (7 CSVs)
- `data/invoices/keycomp/*.pdf` + `data/invoices/personal_amazon/*.pdf`
- `data/config.json` (overrides, manual txns, pre-CapOne detail)
- Source code: `supabase_loader.py`, `_parse_bank_statements.py`,
  `_parse_invoices.py`, `accounting/agents/ingestion.py`

### Outputs Produced

| Output | Format | Description |
|--------|--------|-------------|
| **Row Count Report** | table | Per-file: raw rows, parsed rows, dropped rows, drop reason |
| **Field Parse Report** | table | Per-field: total values, successful parses, `0.0` from `"--"`, `0.0` from parse failure |
| **Schema Drift Report** | list | Any CSV with unexpected columns, missing columns, or type changes |
| **Silent Zero Audit** | list | Every row where `_parse_money()` returned `0.0` — distinguish "legitimately zero" from "parse failure" |
| **Date Parse Report** | list | Any row where `Date_Parsed` is `NaT` (failed parse) |
| **Dedup Hash Collisions** | list | Any two `JournalEntry` objects with same `dedup_hash` — real duplicates or hash collision? |

### Acceptance Criteria
- [ ] `parsed_rows == raw_rows` for every Etsy CSV (zero drops)
- [ ] Zero silent-zero parse failures (every `0.0` maps to a `"--"` in source)
- [ ] Zero `NaT` dates
- [ ] All 6 Etsy CSVs have identical column schemas
- [ ] Bank PDF parser covers every page of every statement (no skipped pages)
- [ ] Bank CSV parser handles embedded commas in descriptions without truncation
- [ ] Invoice parser extracts correct `grand_total` for every order (spot-check 5)
- [ ] `config.json` schema validates: all required keys present, no orphan overrides

### Checks to Run
```
1. For each etsy_statement_*.csv:
   a. wc -l raw file vs len(df) after load
   b. df["Net_Clean"].isna().sum() == 0
   c. df[df["Net_Clean"] == 0.0] → cross-ref raw "Net" column for "--" vs parse error
   d. df["Date_Parsed"].isna().sum() == 0

2. For each bank PDF:
   a. Count transactions in PDF (manual or fitz line count)
   b. Compare to parsed transaction count in bank_transactions.json
   c. Verify statement period matches parsed months

3. For each invoice PDF:
   a. Verify grand_total matches PDF (spot-check)
   b. Verify item count matches PDF
   c. Verify order_num is unique across all invoices

4. Config validation:
   a. Every transaction_override matches at least one real transaction
   b. Every manual_transaction has valid date format and positive amount
   c. pre_capone_detail amounts sum to etsy_pre_capone_deposits
```

---

## Agent 2: Etsy Truth Agent

### Mission
Validate that every Etsy CSV transaction is correctly typed, correctly signed,
and that the fundamental accounting identity holds:
`SUM(Net_Clean) = Sales - Fees - Shipping - Marketing - Refunds - Taxes - BuyerFees + Payments`

### Inputs Needed
- Parsed Etsy DataFrame (from Agent 1)
- `accounting/agents/computation.py` metrics
- Raw CSVs for spot-checking

### Outputs Produced

| Output | Format | Description |
|--------|--------|-------------|
| **Type Distribution** | table | Count and SUM(Net_Clean) per Type (Sale, Fee, Shipping, etc.) |
| **Sign Convention Audit** | list | Any Sale with negative Net, any Fee with positive Net, etc. |
| **Order Completeness** | table | For each Order #: has Sale? has Fee? has Processing Fee? Missing components? |
| **Deposit Title Parsing** | table | Every Deposit row: raw title, parsed amount, confidence |
| **Fee Parity Check** | single value | `listing + transaction + processing + credits == total_fees` (within $0.01) |
| **Net Parity Check** | single value | `SUM(Net_Clean) == etsy_net_earned` (within $0.01) |
| **Refund-to-Sale Matching** | table | Each refund matched to its original sale by Order #, amount delta |
| **Monthly Totals** | table | Per-month: gross, fees, shipping, marketing, refunds, net |
| **UNKNOWN List** | list | Any metric that cannot be computed from CSVs + what source data is needed |

### Acceptance Criteria
- [ ] Net parity: `|SUM(Net_Clean) - etsy_net_earned| <= $0.01`
- [ ] Fee parity: `|sum_of_fee_types - total_fees| <= $0.01`
- [ ] All Sales have positive Net_Clean
- [ ] All Fees have negative Net_Clean (or zero for credits)
- [ ] Every Deposit title successfully parses a dollar amount
- [ ] Every refund maps to a valid Order # that also has a Sale
- [ ] Zero orphan Order #s (Fee without Sale, or Sale without Fee)
- [ ] `buyer_paid_shipping` = UNKNOWN (not back-solved)
- [ ] `shipping_profit` = UNKNOWN
- [ ] `shipping_margin` = UNKNOWN

### Checks to Run
```
1. Type sign convention:
   df[df["Type"]=="Sale"]["Net_Clean"].min() >= 0        # all sales positive
   df[df["Type"]=="Fee"]["Net_Clean"].max() <= 0         # all fees negative (except credits)
   df[df["Type"]=="Refund"]["Net_Clean"].max() <= 0      # all refunds negative

2. Order completeness:
   For each unique Order # in Info column:
     - Has at least 1 Sale row
     - Has at least 1 "Transaction fee:" row
     - Has at least 1 "Processing fee" row

3. Deposit parsing:
   For each row where Type == "Deposit":
     - Extract dollar amount from Title via regex
     - Flag any where amount == 0 or regex fails

4. Monthly reconciliation:
   For each Month:
     SUM(Sale) - ABS(SUM(Fee)) - ABS(SUM(Shipping)) - ABS(SUM(Marketing))
     - ABS(SUM(Refund)) - ABS(SUM(Tax)) - ABS(SUM(BuyerFee)) + SUM(Payment)
     == SUM(Net_Clean for that month)    [within $0.01]
```

---

## Agent 3: Bank Truth Agent

### Mission
Validate that every bank transaction is correctly parsed, correctly categorized,
and that the bank statement balances reconcile:
`opening_balance + deposits - debits = closing_balance`

### Inputs Needed
- `data/generated/bank_transactions.json` (parsed output)
- Raw bank PDFs and CSVs
- `data/config.json` (category_overrides, transaction_overrides, manual_transactions)

### Outputs Produced

| Output | Format | Description |
|--------|--------|-------------|
| **Category Distribution** | table | Count and SUM per category (Etsy Payout, Amazon Inventory, etc.) |
| **Duplicate Detection** | list | Transactions with identical (date, amount, desc) — real dupes or same-day purchases? |
| **Override Audit** | table | Each config override: matched? applied correctly? orphaned? |
| **Manual Transaction Audit** | table | Each manual_transaction: is it also in PDF/CSV? (double-count risk) |
| **Period Coverage** | table | Which months covered by PDF, which by CSV, any gaps? |
| **Uncategorized Transactions** | list | Everything in "Uncategorized" or "Pending" — needs human review |
| **Category Override Conflicts** | list | Any transaction matching multiple override rules |
| **Statement Balance Check** | table | Per-statement: opening, deposits, debits, closing, delta |

### Acceptance Criteria
- [ ] Zero duplicate transactions (same date + amount + desc across PDF and CSV sources)
- [ ] Zero transactions categorized as "Uncategorized" (all reviewed)
- [ ] Every config override matches exactly one transaction (no orphans, no double-matches)
- [ ] Manual transactions do not duplicate PDF/CSV transactions
- [ ] Bank statement period coverage has no gaps (Oct 2025 - present)
- [ ] `bank_total_deposits + bank_total_debits` matches statement closing minus opening
- [ ] Etsy Payout deposits count matches Etsy Deposit row count (within timing tolerance)

### Checks to Run
```
1. Duplicate detection:
   GROUP BY (date, amount, desc) HAVING COUNT > 1
   For each group: check if from different source files (PDF vs CSV overlap)

2. Category audit:
   For each category:
     - Count transactions
     - SUM amounts
     - List any that seem miscategorized (e.g., "AMAZON" in "Personal")

3. Override validation:
   For each transaction_override in config:
     - Find matching transaction by desc_contains + date + amount
     - Verify exactly 1 match (0 = orphan, 2+ = ambiguous)
     - Verify split amounts sum to original amount

4. Period coverage:
   List all months with transactions
   Flag any month with 0 transactions (gap)

5. PDF vs CSV overlap:
   Months covered by both PDF and CSV → check for duplicates
```

---

## Agent 4: Receipt/Expense Truth Agent

### Mission
Validate that inventory costs (COGS) are correctly separated from operating
expenses (OpEx), vendor names are normalized, and every expense has a receipt.

### Inputs Needed
- `data/generated/inventory_orders.json` (parsed invoices)
- Bank transactions categorized as Amazon Inventory, Craft Supplies, AliExpress
- `data/config.json` (`listing_aliases`, `best_buy_cc`)
- Raw invoice PDFs for spot-checking

### Outputs Produced

| Output | Format | Description |
|--------|--------|-------------|
| **COGS vs OpEx Classification** | table | Each expense: is it COGS (resale inventory) or OpEx (tools, supplies, shipping)? |
| **Vendor Normalization Report** | table | Raw vendor name → normalized name, category assignment |
| **Receipt Coverage** | table | Each bank expense: has matching invoice/receipt? (Y/N, file ref) |
| **Missing Receipt List** | list | Bank debits > $25 with no matching invoice — potential audit risk |
| **Item-Level Cost Audit** | table | Each inventory item: qty, unit cost, total, seller, verified? |
| **Personal vs Business Split** | table | Each invoice: business items vs personal items, split correctly? |
| **Credit Card Reconciliation** | table | Best Buy CC charges vs payments, running balance check |

### Acceptance Criteria
- [ ] Every bank expense > $25 has a matching receipt/invoice
- [ ] COGS items are correctly tagged (only resale inventory, not tools/supplies)
- [ ] OpEx items are correctly tagged (shipping supplies, tools, subscriptions)
- [ ] Personal purchases are excluded from business deductions
- [ ] Vendor names are consistent (no "AMAZON MKTPL" vs "Amazon" vs "AMZN" fragmentation)
- [ ] Invoice grand_total matches bank debit amount for same order (within $0.05)
- [ ] No expense is double-counted (once in invoice AND once in bank)
- [ ] Best Buy CC: `SUM(charges) - SUM(payments) == current_balance`

### Checks to Run
```
1. Receipt matching:
   For each bank debit in [Amazon Inventory, Craft Supplies, AliExpress]:
     - Find invoice with matching amount (±$0.05) and date (±7 days)
     - Flag unmatched as "MISSING RECEIPT"

2. COGS validation:
   For each item in inventory_orders.json:
     - Is it resale inventory (filament, components) → COGS
     - Or is it a tool/supply (printer parts, tape) → OpEx
     - Verify classification

3. Personal items:
   For each "Personal Amazon" order:
     - Verify it's excluded from COGS
     - Verify it's in "Personal" or "Owner Draw" category

4. Vendor normalization:
   GROUP BY normalized vendor name
   Flag any vendor appearing under multiple names

5. CC reconciliation:
   SUM(best_buy_cc.charges) - SUM(best_buy_cc.payments)
   == config["best_buy_cc"]["balance"]
```

---

## Agent 5: Reconciliation Agent

### Mission
Trace every dollar from Etsy CSV → Etsy Payouts → Bank Deposits.
Identify every gap with its cause.

### Inputs Needed
- Etsy Deposit rows (from Agent 2)
- Bank Etsy Payout deposits (from Agent 3)
- `config.json` `pre_capone_detail` (pre-CapOne deposits)
- `accounting/agents/reconciliation.py` existing matching

### Outputs Produced

| Output | Format | Description |
|--------|--------|-------------|
| **3-Way Reconciliation** | table | Etsy Net SUM → Etsy Deposits → Bank Deposits with deltas |
| **Deposit Match Table** | table | Each Etsy deposit matched to bank deposit: date, amount, variance |
| **Unmatched Etsy Deposits** | list | Etsy says "sent $X" but no bank deposit found — timing or error? |
| **Unmatched Bank Deposits** | list | Bank received Etsy payout not in CSV — missing CSV or pre-CapOne? |
| **Timing Analysis** | table | For each matched pair: days between Etsy payout and bank receipt |
| **Balance Waterfall** | table | Etsy net → minus deposits → remaining Etsy balance |
| **Pre-CapOne Verification** | table | Each pre-CapOne deposit: amount, date, source (config vs bank) |

### Acceptance Criteria
- [ ] `SUM(Etsy Net_Clean) - SUM(all deposits) == Etsy Balance` (within $0.01)
- [ ] Every Etsy deposit matches exactly one bank deposit (within ±3 days, ±$0.01)
- [ ] Zero unmatched deposits on either side (or each unmatched has documented reason)
- [ ] Pre-CapOne deposits sum matches `config.etsy_pre_capone_deposits`
- [ ] Average deposit timing is 1-3 business days (flag outliers > 5 days)
- [ ] Etsy balance is positive and matches last known Etsy payment account balance

### Checks to Run
```
1. Three-way reconciliation:
   A = SUM(Etsy CSV Net_Clean)                     # What Etsy owes you total
   B = SUM(Etsy Deposit rows, parsed amounts)       # What Etsy says it paid
   C = SUM(Bank deposits categorized "Etsy Payout") # What actually hit bank

   A - B = Etsy balance remaining   (should be positive, ~$93-250)
   B - C = Timing/pre-CapOne gap    (should ≈ pre_capone_deposits)

2. Per-deposit matching:
   For each Etsy Deposit row:
     - Parse amount from title ("$651.33 sent to your bank")
     - Find bank deposit within ±3 days, ±$0.01
     - Record match or flag as unmatched

3. Pre-CapOne audit:
   SUM(pre_capone_detail amounts) == etsy_pre_capone_deposits
   Each pre-CapOne deposit: does it appear in bank statements? (probably not — pre-CapOne)

4. Balance waterfall:
   etsy_net_earned
   - csv_deposit_total (parsed from deposit titles)
   - pre_capone_deposits
   = etsy_balance_remaining
   Compare to known Etsy account balance
```

---

## Agent 6: Metrics Integrity Agent

### Mission
Verify that every displayed metric is computed correctly from verified source
data, that no estimate/back-solve exists, and that the confidence label is accurate.

### Inputs Needed
- `accounting/agents/computation.py` (all 80+ metrics)
- `accounting/compat.py` (publish logic)
- `etsy_dashboard.py` (display code)
- Agent 2, 3, 4, 5 outputs (verified source totals)

### Outputs Produced

| Output | Format | Description |
|--------|--------|-------------|
| **Metric Verification Matrix** | table | Each metric: computed value, formula, confidence, independently verified value, delta |
| **Confidence Audit** | list | Any metric labeled VERIFIED that depends on ESTIMATED inputs |
| **UNKNOWN Completeness** | list | Every metric set to UNKNOWN — what data source would fill it |
| **Tax Calculation Audit** | table | SE tax + income tax: inputs, bracket application, result vs hand-calc |
| **Valuation Audit** | table | Each valuation method: inputs, multiples, result, labeled SPECULATIVE? |
| **Profit Chain Walkthrough** | single doc | Step-by-step: gross_sales → fees → net → bank → profit with verified amounts |
| **Display vs Ledger Check** | list | Any dashboard value that doesn't match its Ledger source |

### Acceptance Criteria
- [ ] Every VERIFIED metric independently verified by re-summing raw data
- [ ] Every DERIVED metric re-computed from its formula and matches (within $0.01)
- [ ] Every UNKNOWN metric shows "UNKNOWN" in UI, not $0 or a guess
- [ ] No metric labeled VERIFIED depends on ESTIMATED/UNKNOWN inputs
- [ ] Profit chain: `real_profit == bank_cash_on_hand + owner_draw_total` (within $0.01)
- [ ] Tax estimate is clearly labeled "ESTIMATE" with assumptions listed
- [ ] Valuation is clearly labeled "SPECULATIVE"
- [ ] `compat.py` publishes `None` for UNKNOWN-confidence metrics (not 0.0)

### Checks to Run
```
1. Independent verification (for each VERIFIED metric):
   Re-sum the raw DataFrame with the same filter criteria
   Compare to computation.py result
   Delta must be <= $0.01

2. Confidence chain:
   For each DERIVED metric:
     - List all input metrics
     - Verify all inputs are VERIFIED or DERIVED (not ESTIMATED/UNKNOWN)
     - If any input is ESTIMATED → metric should be ESTIMATED

3. UNKNOWN display check:
   For buyer_paid_shipping, shipping_profit, shipping_margin:
     - Check compat.py publishes None
     - Check dashboard shows "UNKNOWN" not "$0.00"
     - Check KPI card has status="na"
     - Check API returns null

4. Tax audit:
   Hand-calculate SE tax and income tax for known net_income
   Compare to _compute_income_tax() result
   Verify disclaimer text includes all assumptions

5. Profit chain:
   gross_sales                          = SUM(Sale.Net_Clean)
   - total_fees                         = ABS(SUM(Fee.Net_Clean))
   - total_shipping_cost                = ABS(SUM(Shipping.Net_Clean))
   - total_marketing                    = ABS(SUM(Marketing.Net_Clean))
   - total_refunds                      = ABS(SUM(Refund.Net_Clean))
   - total_taxes                        = ABS(SUM(Tax.Net_Clean))
   - total_buyer_fees                   = ABS(SUM(BuyerFee.Net_Clean))
   + total_payments                     = SUM(Payment.Net_Clean)
   = etsy_net_earned                    [verify]
   - etsy_total_deposited               [verify]
   = etsy_balance                       [verify]
   + bank_net_cash                      [verify]
   = bank_cash_on_hand                  [verify]
   + bank_owner_draw_total              [verify]
   = real_profit                        [verify]
```

---

## Agent 7: Fraud/Error Detector Agent

### Mission
Detect anomalies that indicate missing data, duplicate transactions, or
accounting errors — things the other agents might not catch individually.

### Inputs Needed
- Complete Etsy DataFrame
- Complete bank transaction set
- Inventory orders
- Cross-agent outputs (especially Agents 2, 3, 5)

### Outputs Produced

| Output | Format | Description |
|--------|--------|-------------|
| **Missing Transaction Scan** | list | Expected transactions not found (e.g., Sale without Processing Fee) |
| **Duplicate Transaction Scan** | list | Identical (date, amount, title) appearing more than once |
| **Amount Outlier Report** | list | Transactions > 3x the mean for their type — legitimate or error? |
| **Date Gap Analysis** | table | Calendar days with zero Etsy activity — weekend/holiday or missing data? |
| **Cross-Source Mismatch** | list | Same event appears in multiple sources with different amounts |
| **Negative Balance Detection** | list | Any point where running Etsy balance goes negative |
| **Fee Rate Anomalies** | list | Orders where fee/sale ratio deviates >1% from expected 6.5% + 3% + $0.25 |
| **Refund Without Sale** | list | Refund Order # with no matching Sale |
| **Bank Deposit Without Etsy** | list | Etsy-sourced bank deposits not matching any Etsy Deposit row |

### Acceptance Criteria
- [ ] Zero unexplained duplicate transactions
- [ ] Zero refunds without matching sales
- [ ] Zero fee rate anomalies > 2% from expected (or each explained)
- [ ] Zero gaps > 3 consecutive business days without explanation
- [ ] Running Etsy balance never goes significantly negative (> -$50)
- [ ] Every cross-source mismatch explained (timing, rounding, or data error)
- [ ] Every outlier transaction reviewed and either verified or flagged

### Checks to Run
```
1. Duplicate scan:
   GROUP BY (Date, Type, Title, Net_Clean) HAVING COUNT > 1
   Exclude: listings with multiple of same item (legitimate)
   Flag: identical fee rows, identical deposits

2. Fee rate check:
   For each Sale:
     expected_tx_fee = sale_amount * 0.065
     expected_proc_fee = sale_amount * 0.03 + 0.25
     actual_tx_fee = matching "Transaction fee:" row
     actual_proc_fee = matching "Processing fee" row
     delta_tx = |expected - actual|
     delta_proc = |expected - actual|
     Flag if delta > sale_amount * 0.01

3. Date gap analysis:
   Generate calendar of all dates in dataset range
   Mark each date: has_etsy_activity (Y/N), is_weekend, is_holiday
   Flag business days with no activity and no explanation

4. Running balance:
   Sort all Etsy rows by date
   running_balance = 0
   For each row: running_balance += Net_Clean
   Flag any point where running_balance < -$50

5. Refund matching:
   For each Refund row:
     Extract Order # from Title
     Find Sale row with same Order #
     Flag if no Sale found
     Check: |refund_amount| <= sale_amount (no over-refund)

6. Cross-source:
   For each Etsy Deposit:
     Parsed amount from title
     Find matching bank deposit
     If amounts differ by > $0.01 → flag
```

---

## Agent 8: Governance Agent

### Mission
Scan all source code for forbidden calculation patterns (estimates, back-solving,
hardcoded guesses) and ensure no regressions are introduced.

### Inputs Needed
- `etsy_dashboard.py`
- `accounting/agents/computation.py`
- `accounting/compat.py`
- `accounting/agents/governance.py` (existing scanner)

### Outputs Produced

| Output | Format | Description |
|--------|--------|-------------|
| **Forbidden Pattern Scan** | table | Every match: file, line, pattern, severity, description |
| **False Positive Review** | list | Matches that are NOT violations (with explanation) |
| **Removed Code Audit** | list | Every "was: ..." comment confirming old code is gone |
| **UNKNOWN Variable Check** | list | Every variable set to `None` — verify it displays as "UNKNOWN" not "$0" |
| **Confidence Label Audit** | list | Every `Confidence.UNKNOWN` usage — is it correct? |

### Acceptance Criteria
- [ ] Zero CRITICAL violations (`/0.065`, `/0.15`)
- [ ] Zero HIGH violations (count * avg_outbound_label, count * avg_order)
- [ ] Zero MEDIUM violations (hardcoded deduction amounts)
- [ ] Every UNKNOWN variable renders as "UNKNOWN" in UI (not "$0.00")
- [ ] `compat.py` publishes `None` for UNKNOWN metrics
- [ ] `validation.py` skips UNKNOWN-confidence metrics in NaN check

### Checks to Run
```
1. Run GovernanceAgent.scan_project()
   Verify: 0 violations

2. Grep for residual patterns:
   grep -n "/ 0.065" etsy_dashboard.py accounting/     # CRITICAL
   grep -n "/ 0.15"  etsy_dashboard.py accounting/     # CRITICAL
   grep -n "* avg_outbound" etsy_dashboard.py          # HIGH
   grep -n "est_biz_miles\s*=\s*[0-9]" etsy_dashboard.py  # MEDIUM

3. UNKNOWN display check:
   For each variable set to None:
     - Trace to its KPI card / row_item / f-string
     - Verify it shows "UNKNOWN" not "$0.00"
     - Verify status="na" badge (not "verified")
```

---

## Agent 9: QA/Test Harness Agent

### Mission
Build and run automated tests that codify every acceptance criterion from
Agents 1-8 into repeatable, regression-proof checks.

### Inputs Needed
- All agent acceptance criteria (this document)
- Existing data files as golden dataset
- `accounting/` package source code

### Outputs Produced

| Output | Format | Description |
|--------|--------|-------------|
| **Test Suite** | `tests/` directory | pytest test files covering all agents |
| **Golden Dataset** | `tests/fixtures/` | Known-good inputs and expected outputs |
| **Test Results** | pytest output | PASS/FAIL for every check |
| **Coverage Report** | table | Which acceptance criteria have tests, which don't |
| **Regression Guard** | CI config | Pre-commit or CI hook that runs tests on every change |

### Test Categories

```
tests/
  test_ingestion.py          # Agent 1 checks
    test_etsy_csv_row_count()
    test_no_silent_zero_parse_failures()
    test_no_nat_dates()
    test_bank_pdf_transaction_count()
    test_config_schema_valid()

  test_etsy_truth.py         # Agent 2 checks
    test_net_parity()
    test_fee_parity()
    test_sale_sign_positive()
    test_fee_sign_negative()
    test_deposit_title_parsing()
    test_refund_has_matching_sale()
    test_unknown_metrics_are_none()

  test_bank_truth.py         # Agent 3 checks
    test_no_duplicate_transactions()
    test_no_uncategorized()
    test_override_matches_exist()
    test_manual_txns_not_duplicated()
    test_period_coverage_no_gaps()

  test_receipts.py           # Agent 4 checks
    test_cogs_vs_opex_classification()
    test_receipt_coverage_over_25()
    test_vendor_normalization()
    test_no_double_counted_expenses()

  test_reconciliation.py     # Agent 5 checks
    test_three_way_reconciliation()
    test_deposit_matching()
    test_pre_capone_sum()
    test_etsy_balance_positive()

  test_metrics.py            # Agent 6 checks
    test_verified_metrics_independent()
    test_confidence_chain_valid()
    test_unknown_displays_unknown()
    test_profit_chain()
    test_tax_calculation()

  test_fraud_detection.py    # Agent 7 checks
    test_no_duplicate_etsy_rows()
    test_no_refund_without_sale()
    test_fee_rate_within_tolerance()
    test_running_balance_never_deep_negative()

  test_governance.py         # Agent 8 checks
    test_no_forbidden_patterns()
    test_unknown_variables_display_unknown()
    test_compat_publishes_none_for_unknown()
```

### Acceptance Criteria
- [ ] 100% of agent acceptance criteria have corresponding test(s)
- [ ] All tests pass on current codebase
- [ ] Golden dataset is committed and versioned
- [ ] Tests run in < 30 seconds (no network calls, mock Supabase)
- [ ] Tests can run via `pytest tests/` with zero configuration

---

## Audit Workflow — Order of Operations

```
Phase 1: DATA INTEGRITY (can run in parallel)
  ┌─────────────────────────┐
  │  Agent 1: Ingestion     │  Verify raw data parses correctly
  │  (no dependencies)      │
  └──────────┬──────────────┘
             │
Phase 2: SOURCE TRUTH (can run in parallel, depend on Agent 1)
  ┌──────────┴──────────────┐
  │                         │
  ▼                         ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Agent 2:    │  │ Agent 3:    │  │ Agent 4:    │
│ Etsy Truth  │  │ Bank Truth  │  │ Receipt     │
│             │  │             │  │ Truth       │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
Phase 3: CROSS-SOURCE (depends on Agents 2, 3, 4)
       │                │                │
       ▼                ▼                ▼
  ┌─────────────────────────────────────────┐
  │  Agent 5: Reconciliation                │
  │  (Etsy → Payouts → Bank)               │
  └──────────────────┬──────────────────────┘
                     │
  ┌──────────────────┼──────────────────────┐
  │                  │                      │
  ▼                  ▼                      ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Agent 6:    │  │ Agent 7:    │  │ Agent 8:    │
│ Metrics     │  │ Fraud/Error │  │ Governance  │
│ Integrity   │  │ Detector    │  │             │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
Phase 4: AUTOMATED TESTING
       │                │                │
       ▼                ▼                ▼
  ┌─────────────────────────────────────────┐
  │  Agent 9: QA/Test Harness               │
  │  (codifies all checks into pytest)      │
  └─────────────────────────────────────────┘
```

### Step-by-Step Execution

| Step | Agent(s) | Action | Blocking? |
|------|----------|--------|-----------|
| 1 | Agent 1 | Parse all raw files, produce row counts and parse reports | Yes — all others depend on clean data |
| 2a | Agent 2 | Validate Etsy CSV totals, types, signs, orders | No — parallel with 2b, 2c |
| 2b | Agent 3 | Validate bank transactions, categories, dupes | No — parallel with 2a, 2c |
| 2c | Agent 4 | Validate receipts, COGS/OpEx, vendor names | No — parallel with 2a, 2b |
| 3 | Agent 5 | Match Etsy deposits → bank deposits, 3-way reconciliation | Yes — needs 2a + 2b |
| 4a | Agent 6 | Re-verify all metrics against Agent 2-5 outputs | No — parallel with 4b, 4c |
| 4b | Agent 7 | Anomaly detection across all sources | No — parallel with 4a, 4c |
| 4c | Agent 8 | Code scan for forbidden patterns | No — parallel with 4a, 4b |
| 5 | Agent 9 | Build test suite, run all tests, produce coverage report | Yes — final gate |

---

## Definition of Done

### Per-Agent Done
Each agent is "done" when:
- [ ] All outputs listed above are produced
- [ ] All acceptance criteria are checked (PASS or documented FAIL with root cause)
- [ ] Every FAIL has either a fix applied or an action item with owner
- [ ] UNKNOWN values have documented "what data source is needed"

### Full Audit Done
The audit is complete when:
- [ ] All 9 agents report DONE
- [ ] Zero CRITICAL findings remain open
- [ ] All HIGH findings have action items
- [ ] Agent 9 test suite passes with 0 failures
- [ ] Governance scan returns 0 violations
- [ ] The following golden checks all pass:

```
GOLDEN CHECKS (the 7 commandments)
═══════════════════════════════════
1. PARITY:    |SUM(Etsy CSV Net_Clean) - etsy_net_earned| <= $0.01
2. FEES:      |listing + transaction + processing + credits - total_fees| <= $0.01
3. PROFIT:    |real_profit - (bank_cash_on_hand + owner_draw_total)| <= $0.01
4. BALANCE:   |etsy_net - total_deposited - etsy_balance| <= $0.01
5. NO FAKES:  grep for /0.065, /0.15, *avg_ returns ZERO matches in live code
6. UNKNOWN:   buyer_paid_shipping, shipping_profit, shipping_margin all display "UNKNOWN"
7. RECONCILE: every Etsy deposit matches a bank deposit (or has documented exception)
```

### Sign-Off Checklist

| Check | Status | Verified By |
|-------|--------|-------------|
| All Etsy CSVs parse with zero drops | | Agent 1 |
| Net parity holds (within $0.01) | | Agent 2 |
| Fee breakdown sums correctly | | Agent 2 |
| No duplicate bank transactions | | Agent 3 |
| All bank expenses have receipts (>$25) | | Agent 4 |
| Etsy→Bank deposits fully matched | | Agent 5 |
| Etsy balance matches calculated | | Agent 5 |
| All metrics independently verified | | Agent 6 |
| Profit chain validated | | Agent 6 |
| No anomalous duplicates or gaps | | Agent 7 |
| Zero forbidden code patterns | | Agent 8 |
| pytest suite: all tests pass | | Agent 9 |
| UNKNOWN metrics show "UNKNOWN" in UI | | Agent 8 |

---

## Files Referenced

| File | Role |
|------|------|
| `supabase_loader.py` | Etsy CSV + Supabase data loading |
| `_parse_bank_statements.py` | Bank PDF/CSV parsing |
| `_parse_invoices.py` | Invoice/receipt parsing |
| `accounting/agents/ingestion.py` | Journal entry creation |
| `accounting/agents/computation.py` | 80+ metric calculations |
| `accounting/agents/validation.py` | 5 pre-publish checks |
| `accounting/agents/reconciliation.py` | Deposit matching |
| `accounting/agents/audit.py` | Audit trail generation |
| `accounting/agents/governance.py` | Forbidden pattern scanner |
| `accounting/compat.py` | Ledger → dashboard globals |
| `accounting/models.py` | MetricValue, Confidence, JournalEntry |
| `accounting/journal.py` | Journal with dedup |
| `accounting/ledger.py` | Immutable metric snapshot |
| `accounting/pipeline.py` | Full rebuild orchestrator |
| `etsy_dashboard.py` | Dashboard display + API endpoints |
| `data/config.json` | Overrides, manual txns, pre-CapOne |
