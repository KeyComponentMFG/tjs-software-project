"""
accounting/agents/expense_completeness.py — Agent 7: Expense Completeness.

Identifies every bank debit missing a receipt, normalizes vendor names,
produces receipt-only expense rollups, and separates cash flow from accounting profit.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from ..journal import Journal
from ..models import (
    Confidence,
    ExpenseCompletenessResult,
    JournalEntry,
    MissingReceipt,
    ReceiptMatch,
)

# Categories to skip — these are not expenses needing receipts
SKIP_CATEGORIES = {
    "Owner Draw - Tulsa",
    "Owner Draw - Texas",
    "Etsy Payout",
    "Other Deposit",
    "Etsy Fees",           # verified by Etsy CSV
    "Business Credit Card",  # payment to CC, not the purchase itself
}

# Bank category → which receipt/invoice sources can match it
CATEGORY_TO_RECEIPT_SOURCES = {
    "Amazon Inventory": ["Key Component Mfg"],
    "AliExpress Supplies": ["SUNLU", "Alibaba"],
    "Craft Supplies": ["Hobby Lobby", "Home Depot"],
}

# Categories whose expenses are tax-deductible
TAX_DEDUCTIBLE_CATEGORIES = {
    "Amazon Inventory", "AliExpress Supplies", "Craft Supplies",
    "Shipping", "Subscriptions",
}


def _parse_date(s: str) -> Optional[datetime]:
    """Parse 'MM/DD/YYYY' or 'Month Day, Year' to datetime."""
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


class ExpenseCompletenessAgent:
    """Matches every bank debit against receipts/invoices with strict tolerances."""

    def __init__(self, vendor_map: Optional[dict[str, str]] = None):
        self.vendor_map = vendor_map or {}

    def _normalize_vendor(self, raw_desc: str) -> str:
        """Map raw bank description to canonical vendor name via substring match."""
        upper = raw_desc.upper()
        for pattern, canonical in self.vendor_map.items():
            if pattern.upper() in upper:
                return canonical
        # Fallback: clean up the raw description
        return raw_desc.strip()[:40]

    def run(self, journal: Journal, invoices: list[dict]) -> ExpenseCompletenessResult:
        """Run expense completeness check.

        Parameters:
            journal: The accounting journal with all bank entries
            invoices: List of parsed invoice dicts with source, grand_total, date, file
        """
        all_debits = journal.bank_debits()

        receipt_matches: list[ReceiptMatch] = []
        missing_receipts: list[MissingReceipt] = []
        skipped: list[JournalEntry] = []

        # Separate debits into skip vs expense
        expense_debits: list[JournalEntry] = []
        for entry in all_debits:
            if entry.category in SKIP_CATEGORIES:
                skipped.append(entry)
            else:
                expense_debits.append(entry)

        # Build receipt pools per category
        category_pools: dict[str, list[dict]] = {}
        for cat, sources in CATEGORY_TO_RECEIPT_SOURCES.items():
            pool = []
            for inv in invoices:
                if inv.get("source") not in sources:
                    continue
                if "Gigi" in inv.get("file", ""):
                    continue
                pool.append({
                    "amount": Decimal(str(inv.get("grand_total", 0))),
                    "date": _parse_date(inv.get("date", "")),
                    "used": False,
                    "invoice_id": inv.get("file", inv.get("id", "")),
                    "source": inv.get("source", ""),
                })
            category_pools[cat] = pool

        # Match debits against receipt pools — strict single pass
        matched_entries: set[str] = set()  # dedup_hash of matched debits

        for cat, sources in CATEGORY_TO_RECEIPT_SOURCES.items():
            cat_debits = sorted(
                [e for e in expense_debits if e.category == cat],
                key=lambda e: e.txn_date,
            )
            pool = category_pools.get(cat, [])

            for entry in cat_debits:
                bank_amt = abs(entry.amount)
                bank_dt = datetime(entry.txn_date.year, entry.txn_date.month, entry.txn_date.day)

                best_idx = -1
                best_score = 999999

                for i, r in enumerate(pool):
                    if r["used"]:
                        continue
                    amt_diff = abs(r["amount"] - bank_amt)
                    # Strict: ±$0.02 only
                    if amt_diff > Decimal("0.02"):
                        continue
                    if r["date"]:
                        day_diff = abs((bank_dt - r["date"]).days)
                        if day_diff > 14:
                            continue
                        score = int(amt_diff * 10000) + day_diff
                    else:
                        score = 50000 + int(amt_diff * 10000)

                    if score < best_score:
                        best_score = score
                        best_idx = i

                if best_idx >= 0:
                    r = pool[best_idx]
                    r["used"] = True
                    matched_entries.add(entry.dedup_hash)

                    receipt_matches.append(ReceiptMatch(
                        bank_entry=entry,
                        receipt_source=r["source"],
                        receipt_date=r["date"].date() if r["date"] else entry.txn_date,
                        receipt_amount=r["amount"],
                        amount_diff=abs(r["amount"] - bank_amt),
                        date_diff_days=abs((bank_dt - r["date"]).days) if r["date"] else 0,
                        invoice_id=r["invoice_id"],
                    ))

        # All unmatched expense debits → MissingReceipt
        for entry in expense_debits:
            if entry.dedup_hash in matched_entries:
                continue

            vendor = self._normalize_vendor(entry.title)
            is_deductible = entry.category in TAX_DEDUCTIBLE_CATEGORIES
            amt = abs(entry.amount)

            missing_receipts.append(MissingReceipt(
                transaction_id=entry.dedup_hash,
                vendor=vendor,
                raw_desc=entry.title,
                date=entry.txn_date,
                amount=amt,
                bank_category=entry.category,
                suggested_category=entry.category,
                suggested_confidence=Confidence.ESTIMATED,
                rationale=f"Auto-categorized from bank pattern: {entry.category}",
                tax_deductible=is_deductible,
                priority_score=int(amt * 2) if is_deductible else int(amt),
                source_file=entry.source_file,
            ))

        # Sort missing by priority (highest first)
        missing_receipts.sort(key=lambda m: m.priority_score, reverse=True)

        # Compute totals
        receipt_verified_total = sum(
            (abs(m.bank_entry.amount) for m in receipt_matches), Decimal("0")
        )
        bank_recorded_total = sum(
            (abs(e.amount) for e in expense_debits), Decimal("0")
        )
        gap_total = bank_recorded_total - receipt_verified_total

        # Per-category breakdown
        by_category: dict[str, dict] = {}
        all_cats = set(e.category for e in expense_debits)
        for cat in sorted(all_cats):
            cat_verified = sum(
                (abs(m.bank_entry.amount) for m in receipt_matches if m.bank_entry.category == cat),
                Decimal("0"),
            )
            cat_bank = sum(
                (abs(e.amount) for e in expense_debits if e.category == cat),
                Decimal("0"),
            )
            cat_missing = sum(1 for m in missing_receipts if m.bank_category == cat)
            by_category[cat] = {
                "verified": cat_verified,
                "bank_recorded": cat_bank,
                "gap": cat_bank - cat_verified,
                "missing_count": cat_missing,
            }

        return ExpenseCompletenessResult(
            receipt_matches=receipt_matches,
            missing_receipts=missing_receipts,
            skipped_transactions=skipped,
            receipt_verified_total=receipt_verified_total,
            bank_recorded_total=bank_recorded_total,
            gap_total=gap_total,
            by_category=by_category,
            vendor_map_used=dict(self.vendor_map),
        )
