# Strict Reconciliation Pipeline Design

## Overview

Three-stage reconciliation: **Etsy Order Transactions → Etsy Payouts → Bank Deposits**

The pipeline produces a penny-perfect audit trail from every Etsy CSV row to a bank deposit, or marks it as unreconciled with a specific reason code.

---

## A) Canonical Table Schemas

### Table 1: `etsy_orders` — Order-level aggregation of Etsy CSV rows

Derived by grouping raw CSV rows on `Order #` (extracted from Title/Info fields).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `order_id` | TEXT PK | Parsed from `"Payment for Order #3985809306"` | Etsy order number |
| `order_date` | DATE | First row's Date for this order | Date of sale |
| `gross_sale` | DECIMAL | SUM(Amount) WHERE Type=Sale for this order | Buyer payment before fees |
| `transaction_fee` | DECIMAL | SUM(Net) WHERE Type=Fee AND Title LIKE 'Transaction fee%' | 6.5% transaction fee |
| `processing_fee` | DECIMAL | SUM(Net) WHERE Type=Fee AND Title LIKE 'Processing fee%' | Payment processing fee |
| `listing_fee` | DECIMAL | SUM(Net) WHERE Type=Fee AND Title LIKE 'Listing fee%' | $0.20 listing renewal |
| `shipping_label` | DECIMAL | SUM(Net) WHERE Type=Shipping AND Info LIKE 'Label%' | USPS/UPS label cost |
| `tax_collected` | DECIMAL | SUM(Net) WHERE Type=Tax | Sales tax collected (pass-through) |
| `offsite_ads_fee` | DECIMAL | SUM(Net) WHERE Type=Marketing AND Title LIKE 'Offsite%' | 15% offsite ads fee (if any) |
| `buyer_fee` | DECIMAL | SUM(Net) WHERE Type="Buyer Fee" | Buyer-side fee (if any) |
| `net_to_account` | DECIMAL | SUM(Net_Clean) for all rows in this order | What actually hits Etsy payment account |
| `row_count` | INT | COUNT of CSV rows for this order | Expected: 2-5 rows per order |
| `payout_id` | TEXT FK | Assigned during payout batching (nullable) | Links to `etsy_payouts.payout_id` |

**Construction rule:** Every Etsy CSV row with an Order # MUST appear in exactly one `etsy_orders` row. Rows without an Order # (deposits, listing fees, ad charges) go to separate tables.

### Table 2: `etsy_non_order_items` — Etsy CSV rows NOT tied to a specific order

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `item_id` | TEXT PK | Auto-generated hash | Dedup key |
| `item_date` | DATE | CSV Date column | Transaction date |
| `txn_type` | TEXT | CSV Type column | Fee, Marketing, Shipping, etc. |
| `title` | TEXT | CSV Title column | Description |
| `net_amount` | DECIMAL | CSV Net_Clean | Amount hitting payment account |
| `payout_id` | TEXT FK | Assigned during payout batching (nullable) | Links to `etsy_payouts.payout_id` |

**Examples:** Standalone listing fees, Etsy Ads charges, subscription fees, regulatory operating fees.

### Table 3: `etsy_payouts` — Each "$X sent to your bank account" deposit row

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `payout_id` | TEXT PK | Hash of (date, amount) | Unique payout identifier |
| `payout_date` | DATE | CSV Date column | Date Etsy initiated the payout |
| `payout_amount` | DECIMAL | Parsed from Title `"$651.33 sent to your bank account"` | Exact amount sent |
| `expected_amount` | DECIMAL | Computed: SUM(net_to_account) of assigned orders + non-order items | What the batch SHOULD total |
| `batch_diff` | DECIMAL | `payout_amount - expected_amount` | Must be $0.00 when fully reconciled |
| `orders_in_batch` | TEXT[] | List of order_ids assigned to this payout | Audit trail |
| `non_order_items` | TEXT[] | List of item_ids assigned to this payout | Audit trail |
| `bank_match_id` | TEXT FK | Links to `bank_deposits.deposit_id` (nullable) | Bank-side match |
| `match_status` | TEXT | MATCHED / UNMATCHED / NEEDS_REVIEW | Reconciliation state |
| `match_rule` | TEXT | Which rule produced the match | Audit trail |

### Table 4: `bank_deposits` — Etsy payouts received by Capital One

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `deposit_id` | TEXT PK | Hash of (date, amount, raw_desc) | Unique deposit identifier |
| `deposit_date` | DATE | Bank statement Date column | Date deposit hit the bank |
| `deposit_amount` | DECIMAL | Bank statement Amount column | Exact amount received |
| `raw_desc` | TEXT | Full bank description | `"ACH deposit ETSY, INC. PAYOUT 121025 Key Component..."` |
| `ach_date_code` | TEXT | Parsed from raw_desc (MMDDYY) | `"121025"` → 12/10/2025 |
| `source_file` | TEXT | PDF/CSV filename | Which bank statement |
| `payout_match_id` | TEXT FK | Links to `etsy_payouts.payout_id` (nullable) | Etsy-side match |
| `match_status` | TEXT | MATCHED / UNMATCHED / NEEDS_REVIEW / PRE_CAPONE | Reconciliation state |
| `match_rule` | TEXT | Which rule produced the match | Audit trail |

### Table 5: `reconciliation_ledger` — Final audit output

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `entry_id` | TEXT PK | Auto-generated | Row ID |
| `stage` | TEXT | ORDER_TO_PAYOUT / PAYOUT_TO_BANK | Which reconciliation stage |
| `left_id` | TEXT | order_id or payout_id | Left-side entity |
| `right_id` | TEXT | payout_id or deposit_id | Right-side entity |
| `status` | TEXT | MATCHED / UNMATCHED_LEFT / UNMATCHED_RIGHT / NEEDS_REVIEW | Result |
| `rule_applied` | TEXT | Rule name that produced this result | Audit |
| `amount_diff` | DECIMAL | Difference if any | Should be $0.00 for matches |
| `date_diff_days` | INT | Date gap between left and right | Context |
| `notes` | TEXT | Human-readable explanation | Why this status |

---

## B) Join Keys and Matching Strategy

### Stage 1: Etsy CSV Rows → Order Aggregation

**Join key:** `Order #` extracted from Title and Info fields.

**Extraction rules:**
- Sale rows: `"Payment for Order #XXXXXXXXXX"` → extract from Title
- Fee rows: `"Order #XXXXXXXXXX"` → extract from Info field
- Shipping rows: No order # directly — match by date + `"Label #"` reference
- Tax rows: Match by order # in Info field if present
- Buyer Fee rows: Match by order # in Info field

**Algorithm:**
```
1. Parse Order # from every CSV row using regex: r'Order #(\d+)'
2. GROUP BY order_id → aggregate into etsy_orders
3. Rows with no Order # → etsy_non_order_items
4. VERIFY: SUM(all etsy_orders.net_to_account) + SUM(all etsy_non_order_items.net_amount)
   + SUM(all etsy_payouts.payout_amount * -1) == etsy_balance_remaining
```

**Shipping label linkage problem:** Shipping label rows reference `"Label #300806686999"` but NOT the order #. The label is linked to an order by date proximity and sequence position in the CSV. **Rule:** If a shipping label row appears within ±1 day of order rows and no other order claims it, assign it. Otherwise, mark as NEEDS_REVIEW in `etsy_non_order_items`.

### Stage 2: Orders/Items → Payout Batching

**Join key:** Date-range assignment. Orders between two consecutive payout dates belong to the earlier payout.

**Algorithm:**
```
1. Sort etsy_payouts by payout_date ascending
2. For each consecutive pair (payout_N, payout_N+1):
   - All orders/items with order_date in [payout_N-1.payout_date + 1, payout_N.payout_date]
     are candidates for payout_N
3. For the first payout: all orders from business start through payout_1.payout_date
4. For orders after the last payout: these are in the current Etsy balance (unpaid)

VERIFICATION:
   SUM(assigned_orders.net_to_account + assigned_items.net_amount)
   should equal payout_amount within $0.01

   If batch_diff > $0.01: flag as NEEDS_REVIEW — likely missing CSV rows or
   Etsy reserve hold
```

**Why date-range works:** Etsy batches all transactions between payouts into a single deposit. The payout amount equals the exact sum of all Net values for transactions in that window. This is deterministic, not approximate — Etsy's own accounting produces the batch.

**Edge cases:**
- **Reserve holds:** Etsy may withhold funds from new sellers. If batch_diff is consistently negative, flag as "possible reserve hold — check Etsy payment account page."
- **Partial payouts:** If Etsy splits a payout (rare), two deposit rows will appear close together. Match by exact sum.
- **Refunds spanning payouts:** A refund processed after the original sale's payout reduces a future payout. The refund row's date determines which payout batch it belongs to.

### Stage 3: Etsy Payouts → Bank Deposits

**Primary join key:** `exact_amount` (penny-match required)

**Secondary join key:** `ach_date_code` parsed from bank raw_desc

**Algorithm:**
```
1. Parse ACH date code from bank raw_desc:
   regex: r'PAYOUT\s+(\d{6})'  →  "121025" → date(2025, 12, 10)

2. For each etsy_payout, find bank_deposit where:
   RULE 1 (DETERMINISTIC):
     bank_deposit.deposit_amount == etsy_payout.payout_amount  (exact penny match)
     AND bank_deposit.ach_date_code parses to a date within [payout_date, payout_date + 5 days]
     → Status: MATCHED, Rule: "exact_amount + ach_date"

   RULE 2 (DETERMINISTIC):
     bank_deposit.deposit_amount == etsy_payout.payout_amount  (exact penny match)
     AND bank_deposit.deposit_date in [payout_date, payout_date + 5 days]
     AND no other etsy_payout has the same amount in this window
     → Status: MATCHED, Rule: "exact_amount + date_window + unique"

   RULE 3 (NEEDS REVIEW):
     bank_deposit.deposit_amount == etsy_payout.payout_amount  (exact penny match)
     AND bank_deposit.deposit_date in [payout_date, payout_date + 5 days]
     AND another etsy_payout ALSO has the same amount in this window
     → Status: NEEDS_REVIEW, Rule: "exact_amount + ambiguous_date"
     → Note: "Multiple payouts with identical amount $X in same window — manual review needed"

   RULE 4 (UNMATCHED):
     No bank_deposit matches amount exactly
     → Status: UNMATCHED, Rule: "no_amount_match"
     → Check: Is payout_date recent (< 5 days ago)? If so, likely in transit.
```

**Why exact amount is required:** Etsy deposits a specific calculated amount. The bank receives that exact amount via ACH. There is no rounding, no currency conversion, no fee deduction in transit. The amount MUST match to the penny. If it doesn't, something is wrong — do not paper over it.

**ACH date code parsing:** Bank raw descriptions like `"ACH deposit ETSY, INC. PAYOUT 121025"` contain a 6-digit date code (MMDDYY). This is the ACH origination date, which should be the same as or 1-2 days after the Etsy payout date. This gives us a second deterministic key beyond amount.

**Note on CSV-sourced bank data:** CSV bank descriptions (`"ACH deposit ETSY, INC.       PAYOUT Key Component..."`) may NOT contain the date code. In this case, fall back to Rule 2 (date window + uniqueness).

---

## C) Reconciliation Outputs

### Output 1: Matched Pairs Table

| Payout Date | Payout Amount | Bank Date | Bank Amount | Date Gap | Amount Diff | Rule | Orders in Batch |
|-------------|---------------|-----------|-------------|----------|-------------|------|-----------------|

Every row has `amount_diff = $0.00` (enforced by exact-match rule). Date gap is informational.

### Output 2: Unmatched Etsy Payouts

| Payout Date | Payout Amount | Reason | Action Required |
|-------------|---------------|--------|-----------------|

Possible reasons:
- `IN_TRANSIT` — Payout date < 5 business days ago
- `NO_BANK_MATCH` — No bank deposit with this exact amount in expected window
- `PRE_CAPONE` — Before Capital One account opened (Oct-Dec 2025 personal bank)

### Output 3: Unmatched Bank Deposits

| Bank Date | Bank Amount | Raw Description | Reason | Action Required |
|-----------|-------------|-----------------|--------|-----------------|

Possible reasons:
- `NO_ETSY_MATCH` — No Etsy payout with this exact amount
- `POSSIBLE_DUPLICATE` — Amount matches but all matching payouts already claimed
- `NOT_ETSY` — Bank deposit not identified as Etsy payout (excluded from matching)

### Output 4: Payout Batch Detail

For each payout, show the order-level breakdown:

```
PAYOUT: 2026-02-23  Amount: $651.33
├── Order #3985809306  Net: $70.30  (Sale $81.13 - Fees $10.83)
├── Order #3982547891  Net: $45.22  (Sale $52.00 - Fees $6.78)
├── Listing fee (Feb 20)  Net: -$0.40
├── Etsy Ads (Feb 19-22)  Net: -$12.50
├── ...
└── BATCH TOTAL: $651.33  DIFF: $0.00 ✓
```

### Output 5: Summary Statistics

```
RECONCILIATION SUMMARY
═══════════════════════════════════════════
Etsy CSV rows ingested:          1,247
  → Assigned to orders:          1,180  (94.6%)
  → Non-order items:                52  (4.2%)
  → Deposit rows:                   15  (1.2%)

Orders reconciled to payouts:      370  of 370  (100%)
Payout batches verified:            15  of 15   ($0.00 total variance)

Payouts matched to bank:            12  of 15
  → Matched (exact):                12  ($X,XXX.XX)
  → Pre-CapOne (excluded):           2  ($725.27)
  → In transit:                       1  ($651.33)
  → Unmatched:                        0

Bank Etsy deposits matched:         12  of 12  (100%)
  → Unmatched bank deposits:          0

OVERALL STATUS: RECONCILED ✓
```

### Output 6: NEEDS_REVIEW Queue

Items requiring human intervention, with specific instructions:

```
[NEEDS_REVIEW] Payout 2026-01-15 $497.22
  Reason: Batch sum is $495.82, diff = $1.40
  Action: Check Etsy payment account for reserve hold or missing CSV rows.

[NEEDS_REVIEW] Bank deposit 2026-02-03 $483.28
  Reason: Manual transaction (config.json), no Etsy CSV payout row.
  Action: Verify this matches a real Etsy payout. Add CSV data if available.
```

---

## D) Rules

### Rule 1: No Approximate Amount Matching

**Amount tolerance: $0.00.** Etsy payouts are ACH transfers of a specific calculated amount. The bank receives exactly that amount. If the amounts don't match to the penny, they are NOT the same transaction.

Current system uses ±$0.01 tolerance — this is eliminated. Any difference indicates a data error (parsing, rounding, missing row), not a legitimate variance.

### Rule 2: Date Tolerance Is Informational, Not a Matching Criterion

Dates are used to NARROW candidates (reduce the search space) but never to RESOLVE ambiguity. Two transactions match because their amounts are identical and unique in the window, not because their dates are "close enough."

**Date window:** Etsy payout date to bank deposit date should be 0-5 business days (ACH processing time). Anything outside this window is flagged but not auto-rejected — the amount match is definitive.

### Rule 3: Uniqueness Breaks Ties

If two Etsy payouts have the same amount in the same date window and only one bank deposit exists:
- Do NOT auto-assign. Mark both as NEEDS_REVIEW.
- Human must verify using Etsy payment account page or bank statement details.

### Rule 4: Every Dollar Must Be Accounted For

The following identity must hold at all times:

```
SUM(all Etsy CSV Net_Clean)
= SUM(all etsy_payouts.payout_amount) + etsy_balance_remaining
= SUM(all matched bank_deposits) + SUM(pre_capone_deposits) + SUM(unmatched_etsy) + etsy_balance_remaining
```

If this identity fails, the reconciliation has a gap. Report the gap amount and do not mark as RECONCILED.

### Rule 5: Pre-CapOne Deposits Are ESTIMATED, Not VERIFIED

Deposits before the Capital One account (Oct-Dec 2025) are sourced from `config.json` manual entries. They carry `Confidence.ESTIMATED` and cannot be matched to bank records (different bank, no statements loaded).

These are excluded from the payout↔bank matching stage but included in the total cash flow calculation with an explicit "ESTIMATED — no bank verification available" label.

### Rule 6: Manual Transactions Require Source Documentation

Any entry from `config.json` `manual_transactions` (e.g., the $483.28 deposit on 02/03/2026) must be:
- Flagged in the NEEDS_REVIEW queue
- Verified against a real source document before being marked VERIFIED
- Displayed with `Confidence.ESTIMATED` until verified

### Rule 7: No Silent Zeros

If a field cannot be parsed or computed, it must be `None` / UNKNOWN — never silently default to 0. A $0.00 payout amount means "parse failed," not "Etsy sent nothing."

### Rule 8: Immutable Audit Trail

Every match decision is recorded in `reconciliation_ledger` with:
- The rule that produced it
- The exact values compared
- Timestamp of when the match was made
- Whether it was auto-matched or human-reviewed

Matches cannot be silently overwritten. To change a match, create a new ledger entry with `status=OVERRIDE` referencing the old entry.

---

## Implementation Notes

### Parsing the ACH Date Code

```python
import re
from datetime import date

def parse_ach_date(raw_desc: str) -> date | None:
    """Extract MMDDYY date code from bank ACH description.

    Example: "ACH deposit ETSY, INC. PAYOUT 121025 Key Component..."
    → date(2025, 12, 10)
    """
    m = re.search(r'PAYOUT\s+(\d{6})', raw_desc)
    if not m:
        return None
    code = m.group(1)
    try:
        month = int(code[0:2])
        day = int(code[2:4])
        year = 2000 + int(code[4:6])
        return date(year, month, day)
    except ValueError:
        return None
```

### Order ID Extraction

```python
def extract_order_id(title: str, info: str) -> str | None:
    """Extract Etsy order number from CSV row Title or Info fields.

    Sale Title: "Payment for Order #3985809306"
    Fee Info: "Order #3985809306"
    """
    for field in [title, info]:
        m = re.search(r'Order #(\d+)', field or "")
        if m:
            return m.group(1)
    return None
```

### Payout Batching

```python
def assign_orders_to_payouts(orders, non_order_items, payouts):
    """Assign each order/item to the payout that covers its date range.

    Logic: Sort payouts by date. Each payout covers transactions from
    (previous_payout_date + 1) through (this_payout_date).
    """
    sorted_payouts = sorted(payouts, key=lambda p: p.payout_date)

    for i, payout in enumerate(sorted_payouts):
        if i == 0:
            start = date.min  # Everything before first payout
        else:
            start = sorted_payouts[i - 1].payout_date + timedelta(days=1)
        end = payout.payout_date

        batch_orders = [o for o in orders if start <= o.order_date <= end]
        batch_items = [it for it in non_order_items if start <= it.item_date <= end]

        payout.orders_in_batch = [o.order_id for o in batch_orders]
        payout.non_order_items = [it.item_id for it in batch_items]
        payout.expected_amount = sum(o.net_to_account for o in batch_orders) \
                                + sum(it.net_amount for it in batch_items)
        payout.batch_diff = payout.payout_amount - payout.expected_amount
```

### File Changes Required

| File | Change |
|------|--------|
| `accounting/models.py` | Add `PayoutBatch`, `OrderAggregate`, `ReconciliationEntry` dataclasses |
| `accounting/agents/reconciliation.py` | Rewrite with 3-stage pipeline, replace greedy matching |
| `accounting/agents/ingestion.py` | Add `extract_order_id()`, populate order grouping |
| `accounting/agents/computation.py` | Expose payout batch data as metrics |
| `etsy_dashboard.py` (Data Hub) | Add reconciliation detail table with drill-down |

---

## What This Design Does NOT Solve (Known Gaps)

1. **Shipping label ↔ order linking:** Etsy CSV shipping rows reference `Label #`, not `Order #`. Without Etsy API or order-level export, this link is approximate (date-based).

2. **Exact payout batching cutoff time:** Etsy may batch at a specific hour. Orders placed late on payout day might roll to the next batch. The date-range heuristic handles this by flagging batch_diff != $0.00.

3. **Etsy holds/reserves:** New seller reserves are not visible in CSV data. They manifest as batch_diff being consistently negative.

4. **Multi-currency:** Not applicable (all USD), but schema supports adding currency field if needed.

5. **Pre-CapOne bank verification:** Oct-Dec 2025 deposits ($941.99 total) cannot be verified against bank records. They remain ESTIMATED permanently unless personal bank statements are loaded.
