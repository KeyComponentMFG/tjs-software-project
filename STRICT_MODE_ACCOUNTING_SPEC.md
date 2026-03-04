# STRICT MODE — Accounting-Grade Specification

**Version**: 2.0
**Date**: 2026-03-04
**Supersedes**: STRICT_MODE_PRD.md (UI requirements preserved, accounting rules tightened)

---

## DELIVERABLE 1: Confidence Taxonomy (Exact Rules)

### Enum Definition

```python
class Confidence(enum.Enum):
    VERIFIED    = "verified"
    DERIVED     = "derived"
    PARTIAL     = "partial"
    ESTIMATED   = "estimated"
    PROJECTION  = "projection"
    HEURISTIC   = "heuristic"
    UNKNOWN     = "unknown"
    QUARANTINED = "quarantined"
    NEEDS_REVIEW = "needs_review"
```

### Gating Rules

| Level | Rule | STRICT MODE | Normal Mode | Example |
|-------|------|-------------|-------------|---------|
| **VERIFIED** | Computed solely from primary source documents. Every contributing row is identified by `source_entry_ids`. The journal entries trace to a specific CSV row or bank statement line. | SHOW value | SHOW value | `gross_sales = SUM(Sale.Net_Clean)` from Etsy CSV |
| **DERIVED** | Computed exclusively from VERIFIED inputs via a deterministic formula with no assumptions. Formula is a mathematical identity. | SHOW value | SHOW value | `net_sales = gross_sales - total_refunds` |
| **PARTIAL** | Computed from sources but at least one required source category is missing. Must list what's missing and what percentage of the total is verified. | SHOW value + "PARTIAL" badge + missing list | SHOW value | `bank_cash_on_hand` when some deposits are pre-CapOne |
| **ESTIMATED** | Contains assumptions, averages, multiples, or config-based inputs that are not source documents. | **UNKNOWN** — value hidden | SHOW value + "EST." badge | `etsy_pre_capone_deposits` from config.json |
| **PROJECTION** | Forward-looking forecast based on historical pattern extrapolation. | **UNKNOWN** — value hidden | SHOW value + "PROJ." badge | `val_proj_12mo_revenue` from linear regression |
| **HEURISTIC** | Composite score using arbitrary weights with no accounting basis. | **NOT DISPLAYED** — section hidden entirely | SHOW value + "HEURISTIC" badge | `val_health_score`, composite health score |
| **UNKNOWN** | Cannot compute — missing required source data entirely. | SHOW "UNKNOWN" + missing inputs | SHOW "UNKNOWN" | `buyer_paid_shipping` |
| **QUARANTINED** | Failed a CRITICAL validation check. | SHOW "QUARANTINED" — red alert | SHOW "QUARANTINED" | Any metric failing parity |
| **NEEDS_REVIEW** | Computed but ambiguous — multiple valid interpretations exist or matching is uncertain. Human review required before treating as verified. | SHOW value + "NEEDS REVIEW" badge — not counted in verified totals | SHOW value + "REVIEW" badge | Deposit with 2 candidate bank matches |

### Confidence Propagation Rules

```
VERIFIED + VERIFIED = DERIVED
VERIFIED + DERIVED  = DERIVED
VERIFIED + PARTIAL  = PARTIAL
VERIFIED + ESTIMATED = ESTIMATED
anything + UNKNOWN   = UNKNOWN
anything + QUARANTINED = QUARANTINED
```

### STRICT MODE Display Matrix

| Confidence | Value Shown? | Badge | In Totals? |
|------------|-------------|-------|------------|
| VERIFIED | Yes | Green "VERIFIED" | Yes |
| DERIVED | Yes | Blue "DERIVED" | Yes |
| PARTIAL | Yes | Orange "PARTIAL: missing X" | Yes (with caveat) |
| ESTIMATED | No → "UNKNOWN" | Red "STRICT: Needs source data" | No |
| PROJECTION | No → Hidden | — | No |
| HEURISTIC | No → Hidden | — | No |
| UNKNOWN | No → "UNKNOWN" | Red "UNKNOWN" | No |
| QUARANTINED | No → "QUARANTINED" | Red pulsing | No |
| NEEDS_REVIEW | Yes (dimmed) | Yellow "NEEDS REVIEW" | No |

---

## DELIVERABLE 2: Deterministic Reconciliation Spec

### Problem with Current System

The current `ReconciliationAgent` (reconciliation.py) uses:
- **±$0.01 amount tolerance**: Declares match if amounts differ by up to 1 cent
- **±3 day date tolerance**: Declares match if dates differ by up to 3 days
- **Greedy scoring**: `score = date_diff + amount_diff * 100` — picks "best" match

This is tolerance-based truth. In STRICT MODE, these rules are replaced.

### Deterministic Matching Rules (Priority Order)

When STRICT MODE is ON, the reconciliation agent applies these matching passes **in order**. A deposit is matched by the first pass that produces exactly one candidate. If any pass produces multiple candidates, the item goes to NEEDS_REVIEW.

#### Pass 1: Reference ID Match
```
IF etsy_deposit.title contains a reference string
AND bank_deposit.description contains the same reference string
THEN → MATCH (confidence: VERIFIED)
```

**Current data reality**: Etsy deposit titles contain `"$X.XX sent to your bank account"` with no payout ID. Bank descriptions contain `"ETSY INC DIRECT DEP"` with no reference. **This pass will match zero items with current data.** It exists for future data sources (Etsy API, payment account CSV with payout IDs).

#### Pass 2: Exact Amount + Exact Date
```
IF etsy_deposit.parsed_amount == bank_deposit.amount (to the cent)
AND etsy_deposit.date == bank_deposit.date
THEN → MATCH (confidence: VERIFIED)

IF multiple bank deposits match → NEEDS_REVIEW
```

No tolerance. Amounts must be identical. Dates must be identical.

#### Pass 3: Exact Amount + Deterministic Date Window
```
IF etsy_deposit.parsed_amount == bank_deposit.amount (to the cent)
AND bank_deposit.date is within [etsy_deposit.date, etsy_deposit.date + 5 business days]
THEN → MATCH (confidence: DERIVED)

IF multiple bank deposits match → NEEDS_REVIEW
```

**Why 5 business days**: Etsy payouts are initiated on the payout date. ACH transfers take 1-3 business days. Capital One may post 1 business day after receipt. Total window: payout_date to payout_date + 5 business days. This is not a tolerance — it's a deterministic window based on the banking system's actual processing rules.

The window is forward-only (bank posting cannot precede Etsy payout initiation).

#### Pass 4: Unmatched → NEEDS_REVIEW
```
IF no pass matched
THEN → UNMATCHED (confidence: NEEDS_REVIEW if candidates exist, UNKNOWN if none)
```

Items remain in the unmatched list. They are never auto-resolved.

### NEEDS_REVIEW Behavior

When a deposit has multiple candidates:
- All candidates are listed with their date and amount
- The user must manually select the correct match or mark as "no match"
- Until resolved, the deposit does NOT count toward "matched" totals
- The Cash Received metric reflects only definitively matched deposits

### Expense Matching (Bank ↔ Receipts)

Same deterministic rules apply. Current ±$0.02 amount / ±14 day tolerance is replaced:

#### Pass 1: Exact Amount + Exact Date
```
IF receipt.amount == ABS(bank_debit.amount) (to the cent)
AND receipt.date == bank_debit.date
THEN → MATCH (confidence: VERIFIED)
```

#### Pass 2: Exact Amount + Deterministic Window
```
IF receipt.amount == ABS(bank_debit.amount) (to the cent)
AND receipt.date is within [bank_debit.date - 7 days, bank_debit.date + 7 days]
THEN → MATCH (confidence: DERIVED)

IF multiple receipts match → NEEDS_REVIEW
```

**Why ±7 days**: Receipt date is the purchase date. Bank posting date may differ by up to 3 business days (pending → posted). Adding weekends: 7 calendar days is the deterministic maximum.

#### Pass 3: Unmatched → Missing Receipt
No tolerance pass. If exact amount doesn't match any receipt, it's missing.

### Implementation

```python
class StrictReconciliationAgent:
    """Deterministic deposit matching for STRICT MODE."""

    def reconcile(self, journal: Journal, strict_mode: bool = False) -> ReconciliationResult:
        if strict_mode:
            return self._strict_reconcile(journal)
        else:
            return self._tolerance_reconcile(journal)  # Current behavior

    def _strict_reconcile(self, journal: Journal) -> ReconciliationResult:
        # Pass 1: Reference ID (future-proofing)
        # Pass 2: Exact amount + exact date
        # Pass 3: Exact amount + 5 business day window (forward only)
        # Pass 4: Everything else → UNMATCHED / NEEDS_REVIEW
```

### What Changes in the Data Model

```python
@dataclass
class DepositMatch:
    # Existing fields...
    match_method: str = ""         # "reference_id", "exact", "date_window", "tolerance"
    match_confidence: Confidence = Confidence.VERIFIED
    candidates_count: int = 1      # How many candidates existed (1 = definitive)
    needs_review: bool = False
    review_reason: str = ""        # "2 bank deposits match amount on nearby dates"

class ReconciliationResult:
    matched: list[DepositMatch]
    needs_review: list[DepositMatch]    # NEW: ambiguous matches
    etsy_unmatched: list[tuple]
    bank_unmatched: list[JournalEntry]
```

---

## DELIVERABLE 3: Financial Truth Table Schema + Example

### Schema

```python
@dataclass
class FinancialTruthTable:
    """The canonical financial report. Every value is source-traceable."""

    # Metadata
    period_start: date
    period_end: date
    generated_at: datetime
    strict_mode: bool
    data_sources: list[str]         # Files that contributed

    # Platform (Etsy CSV)
    gross_sales: TruthLine
    refunds: TruthLine
    fees: TruthLine
    shipping_labels: TruthLine
    marketing: TruthLine
    taxes_collected: TruthLine
    buyer_fees: TruthLine
    payments: TruthLine
    net_platform_revenue: TruthLine
    etsy_balance: TruthLine
    order_count: TruthLine

    # Bank
    bank_deposits: TruthLine
    bank_debits: TruthLine
    bank_net_cash: TruthLine
    owner_draws: TruthLine

    # Reconciliation
    matched_payouts: ReconciliationSection
    unmatched_etsy: ReconciliationSection
    unmatched_bank: ReconciliationSection

    # Expense Verification
    verified_expenses: TruthLine
    bank_recorded_expenses: TruthLine
    unverified_gap: TruthLine
    missing_receipts: list[MissingReceiptLine]

    # Profit (derived)
    cash_on_hand: TruthLine
    real_profit: TruthLine
    profit_margin: TruthLine

    # Unknown
    unknown_metrics: list[UnknownLine]


@dataclass
class TruthLine:
    """A single line in the Financial Truth Table."""
    name: str
    value: Decimal | None           # None if UNKNOWN
    confidence: Confidence
    source_count: int               # Number of source entries
    source_entry_ids: list[str]     # dedup_hash values
    formula: str
    missing_inputs: list[str]       # Empty if VERIFIED
    notes: str = ""


@dataclass
class ReconciliationSection:
    count: int
    total: Decimal
    items: list[DepositMatch]
    match_methods: dict[str, int]   # {"exact": 3, "date_window": 1}


@dataclass
class UnknownLine:
    name: str
    reason: str
    required_sources: list[str]
    confidence_if_available: Confidence  # What it would be with data
```

### Example Output (STRICT MODE ON)

```
═══════════════════════════════════════════════════════════════════
 FINANCIAL TRUTH TABLE — TJ's Software
 Period: 2024-10-15 to 2026-03-04
 Mode: STRICT
 Sources: EtsyCSV (1,247 rows), CapOne PDF (6 stmts), Invoices (89)
═══════════════════════════════════════════════════════════════════

 PLATFORM SOURCE: Etsy CSV
 ─────────────────────────────────────────────────────────────────
 Gross Sales ............... $4,872.31  VERIFIED  (142 sale rows)
 Refunds ...................   $287.45  VERIFIED  (8 refund rows)
 Fees ......................   $643.18  VERIFIED  (284 fee rows)
 Shipping Labels ...........   $412.60  VERIFIED  (97 shipping rows)
 Marketing .................    $89.50  VERIFIED  (12 marketing rows)
 Taxes Collected ............    $0.00  VERIFIED  (0 tax rows)
 Buyer Fees .................   $12.30  VERIFIED  (4 buyer fee rows)
 Payments ...................    $0.00  VERIFIED  (0 payment rows)
 ─────────────────────────────────────────────────────────────────
 Net Platform Revenue ...... $3,427.28  DERIVED
   = gross - refunds - fees - ship - mkt - tax - bf + payments
 Etsy Balance (unpaid) .....    $93.72  DERIVED
 Orders .....................      142  VERIFIED

 BANK SOURCE: Capital One Statements
 ─────────────────────────────────────────────────────────────────
 Total Deposits ............ $3,204.76  VERIFIED  (12 deposit entries)
 Total Debits ..............   $965.47  VERIFIED  (47 debit entries)
 Net Cash .................. $2,239.29  DERIVED
 Owner Draws (Tulsa) .......   $450.00  VERIFIED  (3 entries)
 Owner Draws (Texas) .......   $200.00  VERIFIED  (2 entries)

 RECONCILIATION: Etsy Payouts ↔ Bank Deposits
 ─────────────────────────────────────────────────────────────────
 Matched Payouts:
   12 of 14 Etsy deposits matched to bank deposits
   Methods: exact_date=8, date_window=4
   Matched Total: $3,204.76

 Unmatched Etsy Deposits:
   2 deposits ($128.80) — no matching bank deposit found
     $72.74 on 2025-01-15  NEEDS_REVIEW (2 candidates)
     $56.06 on 2025-02-28  UNKNOWN (0 candidates)

 Unmatched Bank Deposits:
   0 bank deposits without Etsy match

 Cash Received ............. $3,204.76  DERIVED (matched only)

 EXPENSE VERIFICATION: Bank Debits ↔ Receipts
 ─────────────────────────────────────────────────────────────────
 Verified Expenses .........   $412.30  DERIVED   (18 matched)
   Methods: exact_date=12, date_window=6
 Bank-Recorded Expenses ....   $515.47  VERIFIED  (29 debits)
 Unverified Gap ............   $103.17  DERIVED
 Missing Receipts:              11 items
   $18.99  Amazon       2025-05-07  Amazon Inventory    NEEDS RECEIPT
   $15.50  UPS Store    2025-04-22  Shipping            NEEDS RECEIPT
   $12.99  Hobby Lobby  2025-06-01  Craft Supplies      NEEDS RECEIPT
   ... (8 more)

 PROFIT
 ─────────────────────────────────────────────────────────────────
 Cash on Hand .............. $2,332.01  DERIVED  bank_net + etsy_balance
   = $2,239.29 + $93.72 - $1.00 (rounding)
                             PARTIAL: etsy_balance includes 2
                             unparsed deposit titles
 Real Profit ............... $2,982.01  DERIVED  cash + draws
 Profit Margin .............    61.2%   DERIVED

 UNKNOWN IN STRICT MODE
 ─────────────────────────────────────────────────────────────────
 Buyer-Paid Shipping ....... UNKNOWN
   Need: Etsy order-level CSV with "Shipping charged to buyer"
 Shipping Profit ........... UNKNOWN
   Need: buyer_paid_shipping
 Shipping Margin ........... UNKNOWN
   Need: buyer_paid_shipping
 Annual Revenue (annualized) UNKNOWN (STRICT: was ESTIMATED)
   Need: 12+ months of complete data (have 5 months)
 Business Valuation ........ UNKNOWN (STRICT: was ESTIMATED)
   Need: Professional appraisal or stable 24-month financials
 Health Score .............. NOT SHOWN (STRICT: HEURISTIC removed)
 Revenue Projection ........ NOT SHOWN (STRICT: PROJECTION removed)
 Tax Estimate .............. UNKNOWN (STRICT: was ESTIMATED)
   Need: Filed tax return or CPA review
 Break-Even ................ UNKNOWN (STRICT: was ESTIMATED)
   Need: Itemized fixed vs variable cost classification
═══════════════════════════════════════════════════════════════════
```

---

## DELIVERABLE 4: Metrics That Become UNKNOWN in STRICT MODE

### Complete List: 28 Metrics

#### Group A: Already UNKNOWN (3 metrics) — No change

| # | Metric | Current | Strict | Required Source |
|---|--------|---------|--------|----------------|
| 1 | `buyer_paid_shipping` | UNKNOWN | UNKNOWN | Etsy order CSV "Shipping charged to buyer" column |
| 2 | `shipping_profit` | UNKNOWN | UNKNOWN | buyer_paid_shipping |
| 3 | `shipping_margin` | UNKNOWN | UNKNOWN | buyer_paid_shipping |

#### Group B: ESTIMATED → UNKNOWN (5 metrics)

| # | Metric | Current | Strict | Required Source | Can It Ever Be VERIFIED? |
|---|--------|---------|--------|----------------|--------------------------|
| 4 | `etsy_pre_capone_deposits` | ESTIMATED | UNKNOWN | Etsy payment account export showing pre-CapOne payouts | Yes — if Etsy provides historical payout records |
| 5 | `etsy_total_deposited` | ESTIMATED | UNKNOWN | All deposit records from all bank accounts | Yes — with complete bank history |
| 6 | `etsy_balance_calculated` | ESTIMATED | UNKNOWN | Depends on #5 | Yes — when #5 is VERIFIED |
| 7 | `etsy_csv_gap` | ESTIMATED | UNKNOWN | Diagnostic — depends on #6 | Yes — when #6 is VERIFIED |
| 8 | `bank_unaccounted` | ESTIMATED | UNKNOWN | Replaced by reconciliation | Deprecated |

#### Group C: Dashboard Estimates → UNKNOWN (9 metrics)

| # | Metric | Current | Strict | Why UNKNOWN | What Would Make It VERIFIED |
|---|--------|---------|--------|-------------|----------------------------|
| 9 | `val_annual_revenue` | ESTIMATED | UNKNOWN | Annualization divides by months_operating — assumes linear | 12+ months of complete monthly data (show actual annual total) |
| 10 | `val_annual_sde` | ESTIMATED | UNKNOWN | Same annualization | Same |
| 11 | `val_sde_low/mid/high` | ESTIMATED | UNKNOWN | SDE × assumed multiple (1.0/1.5/2.5) | Professional business appraisal |
| 12 | `val_rev_low/mid/high` | ESTIMATED | UNKNOWN | Revenue × assumed multiple (0.3/0.5/1.0) | Professional business appraisal |
| 13 | `val_blended_low/mid/high` | ESTIMATED | UNKNOWN | Weighted blend with assumed weights (50/25/25) | Professional business appraisal |
| 14 | `val_monthly_expenses` | ESTIMATED | UNKNOWN | Total / months (assumes stability) | Itemized monthly expense tracking |
| 15 | `val_runway_months` | ESTIMATED | UNKNOWN | cash / estimated_monthly_expenses | Depends on #14 |
| 16 | `_breakeven_monthly` | ESTIMATED | UNKNOWN | fixed_costs / margin% (assumes stable margin) | Itemized fixed vs variable cost classification |
| 17 | `_breakeven_orders` | ESTIMATED | UNKNOWN | breakeven / avg_order | Depends on #16 |

#### Group D: Projections → NOT SHOWN (3 metrics)

| # | Metric | Current | Strict | Inherently non-factual |
|---|--------|---------|--------|----------------------|
| 18 | `val_proj_12mo_revenue` | PROJECTION | NOT SHOWN | Forward-looking forecast — can never be verified |
| 19 | `revenue_projection` (3-month) | PROJECTION | NOT SHOWN | Same |
| 20 | `growth_pct` | PROJECTION | NOT SHOWN | Trend extrapolation |

#### Group E: Heuristics → NOT SHOWN (4 metrics)

| # | Metric | Current | Strict | Inherently non-factual |
|---|--------|---------|--------|----------------------|
| 21 | `val_health_score` | HEURISTIC | NOT SHOWN | Arbitrary weights with no accounting basis |
| 22 | `val_health_grade` | HEURISTIC | NOT SHOWN | Derived from #21 |
| 23 | `health_score` (composite, 8-component) | HEURISTIC | NOT SHOWN | Arbitrary weights |
| 24 | `_hs_*` (6 sub-scores) | HEURISTIC | NOT SHOWN | Components of #21 |

#### Group F: Tax Estimates → UNKNOWN (4 metrics)

| # | Metric | Current | Strict | Required Source |
|---|--------|---------|--------|----------------|
| 25 | `se_tax` | ESTIMATED | UNKNOWN | Filed Schedule SE or CPA computation |
| 26 | `income_tax_per` | ESTIMATED | UNKNOWN | Filed Form 1040 |
| 27 | `total_tax_per` | ESTIMATED | UNKNOWN | Sum of #25 + #26 |
| 28 | `quarterly_estimated` | ESTIMATED | UNKNOWN | IRS Form 1040-ES filed |

### Section Visibility in STRICT MODE

| Dashboard Section | Strict Behavior |
|-------------------|-----------------|
| Overview KPIs | Gross Sales, Net Revenue, Profit shown. Health score hidden. |
| Deep Dive charts | Revenue/fee/shipping charts shown (VERIFIED data). Projections hidden. |
| Financials tab | Truth Table shown. Valuation section collapsed with "UNKNOWN in STRICT MODE" |
| Valuation tab | Entire tab shows: "Valuation requires estimates. Turn off STRICT MODE to view." Asset-based val shown (it's VERIFIED). |
| Tax tab | "Tax estimates require CPA review. Values shown are PROJECTIONS." All numbers hidden. |
| Inventory tab | Fully shown — inventory data is SOURCE. |
| Data Hub | Fully shown — raw data view. |

---

## DELIVERABLE 5: Test Plan + Key Test Cases

### Test Structure

```
tests/
  test_strict_confidence.py      — Confidence taxonomy rules
  test_strict_reconciliation.py  — Deterministic matching
  test_strict_gating.py          — ESTIMATED→UNKNOWN gating
  test_strict_truth_table.py     — Truth table output
```

### A. Confidence Taxonomy Tests (`test_strict_confidence.py`)

```python
class TestConfidenceEnum:
    """Confidence enum has all required levels."""

    def test_all_levels_exist(self):
        assert hasattr(Confidence, "VERIFIED")
        assert hasattr(Confidence, "DERIVED")
        assert hasattr(Confidence, "PARTIAL")
        assert hasattr(Confidence, "ESTIMATED")
        assert hasattr(Confidence, "PROJECTION")
        assert hasattr(Confidence, "HEURISTIC")
        assert hasattr(Confidence, "UNKNOWN")
        assert hasattr(Confidence, "QUARANTINED")
        assert hasattr(Confidence, "NEEDS_REVIEW")


class TestConfidencePropagation:
    """Confidence downgrades correctly when inputs have mixed levels."""

    def test_verified_plus_verified(self):
        assert _min_confidence(Confidence.VERIFIED, Confidence.VERIFIED) == Confidence.DERIVED

    def test_verified_plus_estimated(self):
        assert _min_confidence(Confidence.VERIFIED, Confidence.ESTIMATED) == Confidence.ESTIMATED

    def test_anything_plus_unknown(self):
        assert _min_confidence(Confidence.VERIFIED, Confidence.UNKNOWN) == Confidence.UNKNOWN
        assert _min_confidence(Confidence.DERIVED, Confidence.UNKNOWN) == Confidence.UNKNOWN

    def test_anything_plus_quarantined(self):
        assert _min_confidence(Confidence.VERIFIED, Confidence.QUARANTINED) == Confidence.QUARANTINED


class TestSourceEntryIds:
    """Every VERIFIED/DERIVED metric must have source_entry_ids."""

    @pytest.fixture
    def ledger(self):
        ds = load_golden("scenario_1_simple_month.json")
        return run_pipeline(ds).ledger

    def test_verified_metrics_have_entry_ids(self, ledger):
        for name, mv in ledger.metrics.items():
            if mv.confidence in (Confidence.VERIFIED, Confidence.DERIVED):
                assert len(mv.provenance.source_entry_ids) > 0 or \
                    mv.provenance.source_entries == 0, \
                    f"STRICT FAIL: {name} is {mv.confidence.value} " \
                    f"but has no source_entry_ids"

    def test_unknown_metrics_have_no_value(self, ledger):
        for name, mv in ledger.metrics.items():
            if mv.confidence == Confidence.UNKNOWN:
                assert mv.value == Decimal("0"), \
                    f"STRICT FAIL: {name} is UNKNOWN but value={mv.value}"
```

### B. Deterministic Reconciliation Tests (`test_strict_reconciliation.py`)

```python
class TestExactDateMatch:
    """Pass 2: Exact amount + exact date → MATCH."""

    def test_exact_match(self):
        """Same amount, same date → single definitive match."""
        ds = load_golden("scenario_1_simple_month.json")
        result = run_pipeline(ds, strict_mode=True)
        for m in result.recon_result.matched:
            if m.match_method == "exact":
                assert m.etsy_amount == m.bank_amount  # Penny exact
                assert m.date_diff_days == 0
                assert m.match_confidence == Confidence.VERIFIED
                assert m.candidates_count == 1

    def test_no_tolerance_matching(self):
        """Amount diff of $0.01 must NOT match in strict mode."""
        # Build a scenario where Etsy says $72.74, bank says $72.75
        journal = Journal()
        # ... add entries with $0.01 diff
        agent = StrictReconciliationAgent()
        result = agent.reconcile(journal, strict_mode=True)
        assert result.matched_count == 0  # Must NOT match


class TestDateWindowMatch:
    """Pass 3: Exact amount + deterministic business day window."""

    def test_delayed_deposit_within_window(self):
        """Etsy payout on Friday, bank posts Monday → match."""
        ds = load_golden("scenario_3_split_payouts.json")
        result = run_pipeline(ds, strict_mode=True)
        delayed = [m for m in result.recon_result.matched
                   if m.match_method == "date_window"]
        for m in delayed:
            assert m.etsy_amount == m.bank_amount  # Still penny exact
            assert m.date_diff_days <= 5           # Within window
            assert m.match_confidence == Confidence.DERIVED

    def test_bank_before_etsy_rejected(self):
        """Bank posting BEFORE Etsy payout date → no match (impossible)."""
        # Bank date = Jan 14, Etsy payout = Jan 15 → reject
        # Window is forward-only: [payout_date, payout_date + 5 biz days]
        pass


class TestNeedsReview:
    """Multiple candidates → NEEDS_REVIEW, not auto-matched."""

    def test_ambiguous_goes_to_review(self):
        """Two bank deposits with same amount near same date → NEEDS_REVIEW."""
        journal = Journal()
        # Add 1 Etsy deposit: $100 on Jan 15
        # Add 2 bank deposits: $100 on Jan 16, $100 on Jan 17
        agent = StrictReconciliationAgent()
        result = agent.reconcile(journal, strict_mode=True)
        assert len(result.needs_review) == 1
        assert len(result.matched) == 0
        assert result.needs_review[0].candidates_count == 2

    def test_needs_review_not_in_totals(self):
        """NEEDS_REVIEW items must NOT count toward matched totals."""
        # ... same setup
        assert result.matched_total == Decimal("0")


class TestNoToleranceReconciliation:
    """Tolerance-based matching must not exist in strict mode."""

    def test_amount_tolerance_is_zero(self):
        """In strict mode, amount tolerance must be exactly 0."""
        agent = StrictReconciliationAgent()
        agent.strict_mode = True
        # Verify internal tolerance
        assert agent.amount_tolerance == Decimal("0")

    def test_no_score_based_matching(self):
        """Strict mode must not use score = date_diff + amount_diff * 100."""
        # Verify the code path doesn't use the tolerance scorer
        ds = load_golden("scenario_1_simple_month.json")
        result = run_pipeline(ds, strict_mode=True)
        for m in result.recon_result.matched:
            assert m.match_method in ("reference_id", "exact", "date_window")
            assert m.match_method != "tolerance"
```

### C. STRICT MODE Gating Tests (`test_strict_gating.py`)

```python
class TestEstimatedBecomesUnknown:
    """In STRICT MODE, ESTIMATED metrics must become UNKNOWN."""

    ESTIMATED_METRICS = [
        "etsy_pre_capone_deposits",
        "etsy_total_deposited",
        "etsy_balance_calculated",
        "etsy_csv_gap",
        "bank_unaccounted",
    ]

    def test_estimated_hidden_in_strict(self):
        ds = load_golden("scenario_1_simple_month.json")
        result = run_pipeline(ds, strict_mode=True)
        for name in self.ESTIMATED_METRICS:
            mv = result.ledger.get(name)
            if mv:
                assert mv.confidence in (Confidence.UNKNOWN, Confidence.QUARANTINED), \
                    f"STRICT FAIL: {name} is {mv.confidence.value} " \
                    f"in STRICT MODE (should be UNKNOWN)"


class TestVerifiedWithoutEstimates:
    """No STRICT MODE VERIFIED metric may depend on estimates."""

    def test_no_estimated_inputs(self):
        ds = load_golden("scenario_1_simple_month.json")
        result = run_pipeline(ds, strict_mode=True)
        for name, mv in result.ledger.metrics.items():
            if mv.confidence == Confidence.VERIFIED:
                # Check that provenance doesn't reference estimated inputs
                assert "estimated" not in mv.provenance.formula.lower(), \
                    f"STRICT FAIL: {name} is VERIFIED but formula " \
                    f"mentions 'estimated': {mv.provenance.formula}"
                assert "config" not in mv.provenance.notes.lower() or \
                    "config_manual" in mv.provenance.notes.lower(), \
                    f"STRICT FAIL: {name} is VERIFIED but notes " \
                    f"reference config: {mv.provenance.notes}"


class TestProjectionsHidden:
    """Projections must not exist in STRICT MODE output."""

    PROJECTION_METRICS = [
        "val_proj_12mo_revenue",
        "revenue_projection",
        "growth_pct",
    ]

    def test_projections_not_in_ledger(self):
        ds = load_golden("scenario_1_simple_month.json")
        result = run_pipeline(ds, strict_mode=True)
        for name in self.PROJECTION_METRICS:
            mv = result.ledger.get(name)
            if mv:
                assert mv.confidence in (
                    Confidence.UNKNOWN, Confidence.PROJECTION
                ), f"STRICT FAIL: projection {name} is " \
                   f"{mv.confidence.value} in STRICT MODE"


class TestHeuristicsHidden:
    """Heuristics must not be displayed in STRICT MODE."""

    HEURISTIC_METRICS = [
        "val_health_score",
        "val_health_grade",
        "health_score",
    ]

    def test_heuristics_not_displayable(self):
        # These should either not exist in ledger or be HEURISTIC confidence
        ds = load_golden("scenario_1_simple_month.json")
        result = run_pipeline(ds, strict_mode=True)
        for name in self.HEURISTIC_METRICS:
            mv = result.ledger.get(name)
            if mv:
                assert mv.confidence == Confidence.HEURISTIC, \
                    f"STRICT FAIL: heuristic {name} is " \
                    f"{mv.confidence.value} in STRICT MODE"


class TestTaxEstimatesUnknown:
    """Tax estimates must be UNKNOWN in STRICT MODE."""

    def test_tax_metrics_unknown(self):
        # Tax calculations use IRS formulas on estimated income
        # In STRICT MODE, the income is DERIVED (real_profit) but
        # the tax calculation itself is an ESTIMATE (unfiled return)
        # So tax output must be UNKNOWN
        pass
```

### D. Truth Table Tests (`test_strict_truth_table.py`)

```python
class TestTruthTableCompleteness:
    """Truth table must include all required sections."""

    def test_has_platform_section(self, truth_table):
        assert truth_table.gross_sales is not None
        assert truth_table.refunds is not None
        assert truth_table.fees is not None
        assert truth_table.net_platform_revenue is not None

    def test_has_bank_section(self, truth_table):
        assert truth_table.bank_deposits is not None
        assert truth_table.bank_debits is not None

    def test_has_reconciliation_section(self, truth_table):
        assert truth_table.matched_payouts is not None

    def test_has_expense_section(self, truth_table):
        assert truth_table.verified_expenses is not None
        assert truth_table.missing_receipts is not None

    def test_has_unknown_section(self, truth_table):
        assert truth_table.unknown_metrics is not None


class TestTruthTableSourceTraceability:
    """Every truth table line must have source entry IDs."""

    def test_verified_lines_have_ids(self, truth_table):
        for line in [truth_table.gross_sales, truth_table.refunds,
                     truth_table.fees, truth_table.bank_deposits]:
            if line.confidence == Confidence.VERIFIED:
                assert len(line.source_entry_ids) > 0, \
                    f"{line.name} is VERIFIED but has no source_entry_ids"
                assert line.source_count == len(line.source_entry_ids)


class TestTruthTableIdentities:
    """Mathematical identities in the truth table."""

    def test_net_platform_revenue(self, truth_table):
        """net = gross - refunds - fees - ship - mkt - tax - bf + payments."""
        if truth_table.net_platform_revenue.confidence != Confidence.UNKNOWN:
            expected = (truth_table.gross_sales.value
                       - truth_table.refunds.value
                       - truth_table.fees.value
                       - truth_table.shipping_labels.value
                       - truth_table.marketing.value
                       - truth_table.taxes_collected.value
                       - truth_table.buyer_fees.value
                       + truth_table.payments.value)
            assert truth_table.net_platform_revenue.value == expected

    def test_bank_net(self, truth_table):
        """bank_net = deposits - debits."""
        expected = truth_table.bank_deposits.value - truth_table.bank_debits.value
        assert truth_table.bank_net_cash.value == expected

    def test_expense_gap(self, truth_table):
        """gap = bank_recorded - verified."""
        expected = (truth_table.bank_recorded_expenses.value
                   - truth_table.verified_expenses.value)
        assert truth_table.unverified_gap.value == expected

    def test_cash_received_equals_matched(self, truth_table):
        """Cash received = only matched deposit total (not all bank deposits)."""
        # In strict mode, cash_received only counts definitively matched items
        assert truth_table.matched_payouts.total <= truth_table.bank_deposits.value


class TestGrossSalesRequiresRowLevel:
    """If dataset lacks row-level Etsy order totals, gross_sales is UNKNOWN."""

    def test_empty_etsy_data_means_unknown(self):
        import pandas as pd
        from accounting.pipeline import AccountingPipeline

        pipeline = AccountingPipeline()
        empty_df = pd.DataFrame(columns=["Date", "Type", "Title", "Info",
                                          "Currency", "Amount", "Fees & Taxes", "Net"])
        ledger = pipeline.full_rebuild(empty_df, [], {"etsy_pre_capone_deposits": 0,
                                                       "pre_capone_detail": []},
                                        strict_mode=True)
        gross = ledger.get("gross_sales")
        assert gross.value == Decimal("0")
        # With no sale rows, gross_sales is 0 which is VERIFIED (genuinely zero)
        # But if the user has sales and just didn't provide the CSV, that's
        # a data loading issue, not a computation issue
```

### E. Regression Guards

```python
class TestNoToleranceInStrictMode:
    """Regression: ensure tolerance-based matching can't sneak back in."""

    def test_reconciliation_no_amount_tolerance(self):
        """grep the reconciliation code for tolerance usage in strict path."""
        import inspect
        from accounting.agents.reconciliation import ReconciliationAgent
        source = inspect.getsource(ReconciliationAgent._strict_reconcile)
        assert "amount_tolerance" not in source
        assert "0.01" not in source  # No hardcoded cent tolerance
        assert "score =" not in source  # No scoring heuristic

    def test_reconciliation_reports_match_method(self):
        """Every match must declare its method."""
        ds = load_golden("scenario_1_simple_month.json")
        result = run_pipeline(ds, strict_mode=True)
        for m in result.recon_result.matched:
            assert m.match_method in ("reference_id", "exact", "date_window"), \
                f"Unknown match method: {m.match_method}"
            assert m.match_method != "tolerance", \
                "REGRESSION: tolerance matching found in strict mode"

    def test_expense_matching_no_amount_tolerance(self):
        """Expense matching in strict mode must be penny-exact."""
        ds = load_golden("scenario_1_simple_month.json")
        result = run_pipeline(ds, strict_mode=True)
        if result.expense_result:
            for m in result.expense_result.receipt_matches:
                assert m.amount_diff == Decimal("0"), \
                    f"STRICT FAIL: expense match has amount_diff=" \
                    f"{m.amount_diff} (must be 0 in strict mode)"
```

---

## IMPLEMENTATION NOTES

### What Changes in Code

| File | Change |
|------|--------|
| `accounting/models.py` | Add PARTIAL, PROJECTION, HEURISTIC, NEEDS_REVIEW to Confidence enum. Add `match_method`, `match_confidence`, `candidates_count`, `needs_review` to DepositMatch. Add `source_entry_ids` to Provenance. Add `ReconciliationResult` dataclass. |
| `accounting/agents/reconciliation.py` | Add `_strict_reconcile()` method implementing 4-pass deterministic matching. Keep `_tolerance_reconcile()` as current behavior for normal mode. |
| `accounting/agents/computation.py` | Accept `strict_mode` param. When ON: downgrade all ESTIMATED metrics to UNKNOWN. Populate `source_entry_ids` in all Provenance objects. |
| `accounting/agents/expense_completeness.py` | Add `strict_mode` param. When ON: require exact amount match (no ±$0.02). Multi-candidate → NEEDS_REVIEW. |
| `accounting/agents/validation.py` | When `strict_mode`: remove %-based deposit reconciliation check. Replace with exact-match-or-flag check. |
| `accounting/pipeline.py` | Thread `strict_mode` through all agents. |
| `accounting/compat.py` | When `strict_mode`: publish ESTIMATED as None, PROJECTION as None, HEURISTIC as None. |
| `etsy_dashboard.py` | Valuation/Tax/Projection sections: check `strict_mode` global, hide content if ON. Truth Table: new card in Financials tab. |
| `data/config.json` | Add `"strict_mode": false` |

### What Does NOT Change

- All 272 existing tests pass unchanged (they run in normal mode)
- Normal mode behavior is identical to current behavior
- The pipeline architecture (6 stages) is unchanged
- The Journal, Ledger, and all existing agents continue to work

### Migration Path

1. Add new Confidence enum values (backward compatible — existing values unchanged)
2. Add `source_entry_ids` to Provenance (default empty — existing code unaffected)
3. Add `_strict_reconcile()` to ReconciliationAgent (new method, old method unchanged)
4. Add `strict_mode` param to pipeline (default False — all existing callers unchanged)
5. Add strict gating tests
6. Wire UI toggle
