"""
accounting/models.py — Core data models for the accounting pipeline.

All monetary values use Decimal to eliminate floating-point rounding errors.
"""

from __future__ import annotations

import enum
import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Optional


class Confidence(enum.Enum):
    """How trustworthy a metric value is.

    Ordering (highest to lowest trust):
        VERIFIED > DERIVED > PARTIAL > ESTIMATED > PROJECTION > HEURISTIC > UNKNOWN > QUARANTINED > NEEDS_REVIEW

    STRICT MODE rules:
        VERIFIED/DERIVED/PARTIAL → shown
        ESTIMATED/PROJECTION/HEURISTIC → hidden (become UNKNOWN)
        UNKNOWN/QUARANTINED/NEEDS_REVIEW → shown as-is
    """
    VERIFIED = "verified"           # Computed solely from primary sources with row-level trace
    DERIVED = "derived"             # Computed from VERIFIED inputs via deterministic formula
    PARTIAL = "partial"             # Source-backed but missing one required source category
    ESTIMATED = "estimated"         # Contains assumptions or config-based inputs
    PROJECTION = "projection"       # Forward-looking forecast from historical extrapolation
    HEURISTIC = "heuristic"         # Composite score with arbitrary weights — no accounting basis
    UNKNOWN = "unknown"             # Cannot compute without missing data
    QUARANTINED = "quarantined"     # Failed CRITICAL validation — blocked from display
    NEEDS_REVIEW = "needs_review"   # Computed but ambiguous — human review required


class TxnType(enum.Enum):
    """Transaction types from Etsy CSV or bank statement."""
    SALE = "Sale"
    FEE = "Fee"
    SHIPPING = "Shipping"
    MARKETING = "Marketing"
    REFUND = "Refund"
    TAX = "Tax"
    DEPOSIT = "Deposit"
    BUYER_FEE = "Buyer Fee"
    PAYMENT = "Payment"
    # Bank-side types
    BANK_DEPOSIT = "bank_deposit"
    BANK_DEBIT = "bank_debit"
    # Config manual entries
    MANUAL = "manual"


class TxnSource(enum.Enum):
    """Where a transaction originated."""
    ETSY_CSV = "etsy_csv"
    BANK_PDF = "bank_pdf"
    BANK_CSV = "bank_csv"
    CONFIG_MANUAL = "config_manual"
    CONFIG_PRE_CAPONE = "config_pre_capone"


@dataclass(frozen=True)
class JournalEntry:
    """A single financial transaction — the atomic unit of the journal.

    All amounts are Decimal. Positive = money in, negative = money out.
    Etsy CSV 'Net' values are used as-is (fees/shipping are already negative).
    """
    source: TxnSource
    txn_type: TxnType
    txn_date: date
    amount: Decimal          # Net amount (positive = credit, negative = debit)
    gross_amount: Decimal    # Original Amount column (before fees)
    fees: Decimal            # Fees & Taxes column
    title: str
    info: str
    description: str         # Human-readable description
    confidence: Confidence
    source_file: str = ""
    category: str = ""       # Bank category (e.g., "Shipping", "Owner Draw - Tulsa")
    month: str = ""          # YYYY-MM
    currency: str = "USD"
    raw_row: dict = field(default_factory=dict, repr=False)  # Original data for audit

    @property
    def dedup_hash(self) -> str:
        """Hash for deduplication: (source, date, type, amount, title/desc)."""
        key = f"{self.source.value}|{self.txn_date}|{self.txn_type.value}|{self.amount}|{self.title}|{self.info}"
        return hashlib.sha256(key.encode()).hexdigest()[:16]


@dataclass
class Provenance:
    """How a metric was computed — audit trail."""
    formula: str                          # e.g., "gross_sales - total_fees - ..."
    source_entries: int = 0               # Number of journal entries used
    source_types: list[str] = field(default_factory=list)  # e.g., ["Sale", "Fee"]
    notes: str = ""                       # Human explanation
    source_entry_ids: tuple[str, ...] = ()     # dedup_hash values of contributing entries
    missing_inputs: tuple[str, ...] = ()       # What data is needed but absent
    requires_sources: tuple[str, ...] = ()     # Which TxnSource types are required


@dataclass
class MetricValue:
    """A computed metric with its value, confidence, and provenance."""
    name: str
    value: Decimal
    confidence: Confidence
    provenance: Provenance
    display_format: str = "money"  # "money", "count", "percent", "ratio"

    @property
    def as_float(self) -> float:
        """For backward compatibility with existing dashboard code."""
        return float(self.value)

    def __repr__(self) -> str:
        if self.display_format == "money":
            return f"MetricValue({self.name}=${self.value:.2f}, {self.confidence.value})"
        return f"MetricValue({self.name}={self.value}, {self.confidence.value})"


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    check_name: str
    passed: bool
    severity: str            # "CRITICAL" or "HIGH"
    message: str
    expected: Optional[str] = None
    actual: Optional[str] = None
    affected_metrics: list[str] = field(default_factory=list)


@dataclass
class DepositMatch:
    """A matched pair of Etsy deposit ↔ bank deposit."""
    etsy_date: date
    etsy_amount: Decimal
    bank_date: date
    bank_amount: Decimal
    date_diff_days: int
    amount_diff: Decimal
    etsy_entry: Optional[JournalEntry] = None
    bank_entry: Optional[JournalEntry] = None
    match_method: str = "tolerance"     # "reference_id", "exact", "date_window", "tolerance"
    match_confidence: Confidence = Confidence.VERIFIED
    candidates_count: int = 1           # How many candidates existed (1 = definitive)
    needs_review: bool = False
    review_reason: str = ""


@dataclass
class ReconciliationResult:
    """Full result from deposit reconciliation.

    Delta analysis (strict mode):
        delta = etsy_net_earned - matched_total
        If delta != 0, delta_entries must explain the gap (traceable journal entries)
        or reconciliation_confidence drops to NEEDS_REVIEW.
    """
    matched: list[DepositMatch]
    needs_review: list[DepositMatch]
    etsy_unmatched: list                 # list of (JournalEntry, Decimal) tuples
    bank_unmatched: list[JournalEntry]
    matched_total: Decimal = Decimal("0")
    etsy_unmatched_total: Decimal = Decimal("0")
    bank_unmatched_total: Decimal = Decimal("0")
    needs_review_total: Decimal = Decimal("0")
    strict_mode: bool = False
    # Delta analysis
    etsy_net_earned: Decimal = Decimal("0")
    csv_deposit_total: Decimal = Decimal("0")   # Sum of Etsy CSV deposit rows (parsed)
    delta: Decimal = Decimal("0")               # etsy_net_earned - matched_total
    delta_entries: list = field(default_factory=list)  # JournalEntries explaining delta
    delta_explained: bool = False
    delta_explanation: str = ""
    reconciliation_confidence: Confidence = Confidence.UNKNOWN


@dataclass(frozen=True)
class MissingReceipt:
    """A bank debit that has no matching receipt/invoice."""
    transaction_id: str          # JournalEntry.dedup_hash
    vendor: str                  # Canonical vendor name
    raw_desc: str                # Original bank description
    date: date
    amount: Decimal
    bank_category: str           # Auto-categorized bank category
    suggested_category: str      # Best-guess (NOT used in totals)
    suggested_confidence: Confidence
    rationale: str
    tax_deductible: bool
    priority_score: int          # Higher = more urgent
    source_file: str = ""


@dataclass
class ReceiptMatch:
    """A matched pair of bank debit ↔ invoice/receipt."""
    bank_entry: JournalEntry
    receipt_source: str
    receipt_date: date
    receipt_amount: Decimal
    amount_diff: Decimal         # Normal: <= $0.02. Strict: must be $0.00
    date_diff_days: int
    invoice_id: str
    match_method: str = "tolerance"   # "exact", "date_window", "tolerance"


@dataclass
class ExpenseCompletenessResult:
    """Result of the expense completeness agent."""
    receipt_matches: list[ReceiptMatch]
    missing_receipts: list[MissingReceipt]
    skipped_transactions: list[JournalEntry]
    receipt_verified_total: Decimal
    bank_recorded_total: Decimal
    gap_total: Decimal
    by_category: dict             # cat → {verified, bank_recorded, gap, missing_count}
    vendor_map_used: dict
