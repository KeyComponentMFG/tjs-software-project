"""
accounting/agents/reconciliation.py — Agent 4: Cross-source deposit matching.

Two modes:
  Normal:  Matches by amount (±$0.01) and date (±3 days) with greedy scoring.
  Strict:  Deterministic 4-pass matching. Zero tolerance on amounts.
           Ambiguous matches → NEEDS_REVIEW (never auto-resolved).
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from ..journal import Journal
from ..models import Confidence, DepositMatch, ReconciliationResult, TxnSource, TxnType

# Etsy ACH payout → bank posting: up to 5 business days (forward only)
STRICT_DATE_WINDOW_DAYS = 7  # 5 biz days ≈ 7 calendar days


def _get_etsy_deposits(journal: Journal) -> list[tuple]:
    """Extract Etsy deposits with parsed amounts from journal."""
    etsy_deposits = []
    for e in journal.by_type(TxnType.DEPOSIT):
        parsed_str = e.raw_row.get("deposit_parsed_amount", "0")
        try:
            parsed_amt = Decimal(parsed_str)
        except Exception:
            parsed_amt = Decimal("0")
        if parsed_amt > 0:
            etsy_deposits.append((e, parsed_amt))
    return etsy_deposits


def _get_bank_deposits(journal: Journal) -> list:
    """Get bank deposits excluding pre-CapOne config entries."""
    return [
        e for e in journal.bank_deposits()
        if e.source != TxnSource.CONFIG_PRE_CAPONE
    ]


class ReconciliationAgent:
    """Match Etsy deposits to bank deposits."""

    def __init__(self, date_tolerance_days: int = 3,
                 amount_tolerance: Decimal = Decimal("0.01")):
        self.date_tolerance = timedelta(days=date_tolerance_days)
        self.amount_tolerance = amount_tolerance
        self.matched: list[DepositMatch] = []
        self.etsy_unmatched: list = []
        self.bank_unmatched: list = []

    def reconcile(self, journal: Journal,
                  strict_mode: bool = False) -> ReconciliationResult:
        """Run deposit reconciliation.

        Parameters:
            journal: The populated Journal
            strict_mode: If True, use deterministic matching (no tolerances)

        Returns:
            ReconciliationResult with matched, needs_review, and unmatched.
        """
        if strict_mode:
            return self._strict_reconcile(journal)
        return self._tolerance_reconcile(journal)

    # ── Normal mode: tolerance-based matching (existing behavior) ──

    def _tolerance_reconcile(self, journal: Journal) -> ReconciliationResult:
        """Original greedy matching with ±$0.01 amount and ±3 day tolerances."""
        etsy_deposits = _get_etsy_deposits(journal)
        bank_deposits = _get_bank_deposits(journal)

        self.matched = []
        self.etsy_unmatched = []
        self.bank_unmatched = []

        used_bank = set()
        for etsy_entry, etsy_amount in etsy_deposits:
            best_match = None
            best_score = float("inf")

            for i, bank_entry in enumerate(bank_deposits):
                if i in used_bank:
                    continue
                bank_amount = abs(bank_entry.amount)
                amount_diff = abs(etsy_amount - bank_amount)
                date_diff = abs((etsy_entry.txn_date - bank_entry.txn_date).days)

                if amount_diff <= self.amount_tolerance and date_diff <= self.date_tolerance.days:
                    score = date_diff + float(amount_diff) * 100
                    if score < best_score:
                        best_score = score
                        best_match = (i, bank_entry, bank_amount, date_diff, amount_diff)

            if best_match:
                idx, bank_entry, bank_amount, date_diff, amount_diff = best_match
                used_bank.add(idx)
                self.matched.append(DepositMatch(
                    etsy_date=etsy_entry.txn_date,
                    etsy_amount=etsy_amount,
                    bank_date=bank_entry.txn_date,
                    bank_amount=bank_amount,
                    date_diff_days=date_diff,
                    amount_diff=amount_diff,
                    etsy_entry=etsy_entry,
                    bank_entry=bank_entry,
                    match_method="tolerance",
                    match_confidence=Confidence.DERIVED,
                ))
            else:
                self.etsy_unmatched.append((etsy_entry, etsy_amount))

        for i, bank_entry in enumerate(bank_deposits):
            if i not in used_bank:
                if "etsy" in bank_entry.title.lower() or "etsy" in bank_entry.category.lower():
                    self.bank_unmatched.append(bank_entry)

        return self._build_result(strict_mode=False, journal=journal)

    # ── Strict mode: deterministic 4-pass matching ──

    def _strict_reconcile(self, journal: Journal) -> ReconciliationResult:
        """Deterministic matching with zero amount tolerance.

        Pass 1: Reference ID match (future — no IDs in current data)
        Pass 2: Exact amount + exact date
        Pass 3: Exact amount + deterministic date window (forward only)
        Pass 4: Unmatched → NEEDS_REVIEW if candidates exist, UNKNOWN if none
        """
        etsy_deposits = _get_etsy_deposits(journal)
        bank_deposits = _get_bank_deposits(journal)

        self.matched = []
        self.etsy_unmatched = []
        self.bank_unmatched = []
        needs_review: list[DepositMatch] = []

        used_bank: set[int] = set()
        unresolved: list[tuple] = []  # etsy deposits not yet matched

        # ── Pass 1: Reference ID match ──
        # Current Etsy CSV titles: "$X sent to your bank account" — no reference ID.
        # Current bank descriptions: "ETSY INC DIRECT DEP" — no reference ID.
        # This pass is a no-op with current data but ready for Etsy API data.
        for etsy_entry, etsy_amount in etsy_deposits:
            etsy_ref = self._extract_reference(etsy_entry.title)
            if not etsy_ref:
                unresolved.append((etsy_entry, etsy_amount))
                continue

            candidates = []
            for i, bank_entry in enumerate(bank_deposits):
                if i in used_bank:
                    continue
                bank_ref = self._extract_reference(bank_entry.title)
                if bank_ref and bank_ref == etsy_ref:
                    candidates.append((i, bank_entry))

            if len(candidates) == 1:
                idx, bank_entry = candidates[0]
                used_bank.add(idx)
                bank_amount = abs(bank_entry.amount)
                self.matched.append(DepositMatch(
                    etsy_date=etsy_entry.txn_date,
                    etsy_amount=etsy_amount,
                    bank_date=bank_entry.txn_date,
                    bank_amount=bank_amount,
                    date_diff_days=abs((etsy_entry.txn_date - bank_entry.txn_date).days),
                    amount_diff=abs(etsy_amount - bank_amount),
                    etsy_entry=etsy_entry,
                    bank_entry=bank_entry,
                    match_method="reference_id",
                    match_confidence=Confidence.VERIFIED,
                    candidates_count=1,
                ))
            else:
                unresolved.append((etsy_entry, etsy_amount))

        # ── Pass 2: Exact amount + exact date ──
        still_unresolved = []
        for etsy_entry, etsy_amount in unresolved:
            candidates = []
            for i, bank_entry in enumerate(bank_deposits):
                if i in used_bank:
                    continue
                bank_amount = abs(bank_entry.amount)
                if (etsy_amount == bank_amount
                        and etsy_entry.txn_date == bank_entry.txn_date):
                    candidates.append((i, bank_entry, bank_amount))

            if len(candidates) == 1:
                idx, bank_entry, bank_amount = candidates[0]
                used_bank.add(idx)
                self.matched.append(DepositMatch(
                    etsy_date=etsy_entry.txn_date,
                    etsy_amount=etsy_amount,
                    bank_date=bank_entry.txn_date,
                    bank_amount=bank_amount,
                    date_diff_days=0,
                    amount_diff=Decimal("0"),
                    etsy_entry=etsy_entry,
                    bank_entry=bank_entry,
                    match_method="exact",
                    match_confidence=Confidence.VERIFIED,
                    candidates_count=1,
                ))
            elif len(candidates) > 1:
                # Multiple exact matches → NEEDS_REVIEW
                needs_review.append(DepositMatch(
                    etsy_date=etsy_entry.txn_date,
                    etsy_amount=etsy_amount,
                    bank_date=candidates[0][1].txn_date,
                    bank_amount=candidates[0][2],
                    date_diff_days=0,
                    amount_diff=Decimal("0"),
                    etsy_entry=etsy_entry,
                    bank_entry=None,
                    match_method="exact",
                    match_confidence=Confidence.NEEDS_REVIEW,
                    candidates_count=len(candidates),
                    needs_review=True,
                    review_reason=f"{len(candidates)} bank deposits with exact amount on same date",
                ))
            else:
                still_unresolved.append((etsy_entry, etsy_amount))

        # ── Pass 3: Exact amount + deterministic date window (forward only) ──
        final_unresolved = []
        for etsy_entry, etsy_amount in still_unresolved:
            candidates = []
            for i, bank_entry in enumerate(bank_deposits):
                if i in used_bank:
                    continue
                bank_amount = abs(bank_entry.amount)
                if etsy_amount != bank_amount:
                    continue
                # Forward-only window: bank can post on or after Etsy payout date
                day_diff = (bank_entry.txn_date - etsy_entry.txn_date).days
                if 0 <= day_diff <= STRICT_DATE_WINDOW_DAYS:
                    candidates.append((i, bank_entry, bank_amount, day_diff))

            if len(candidates) == 1:
                idx, bank_entry, bank_amount, day_diff = candidates[0]
                used_bank.add(idx)
                self.matched.append(DepositMatch(
                    etsy_date=etsy_entry.txn_date,
                    etsy_amount=etsy_amount,
                    bank_date=bank_entry.txn_date,
                    bank_amount=bank_amount,
                    date_diff_days=day_diff,
                    amount_diff=Decimal("0"),
                    etsy_entry=etsy_entry,
                    bank_entry=bank_entry,
                    match_method="date_window",
                    match_confidence=Confidence.DERIVED,
                    candidates_count=1,
                ))
            elif len(candidates) > 1:
                needs_review.append(DepositMatch(
                    etsy_date=etsy_entry.txn_date,
                    etsy_amount=etsy_amount,
                    bank_date=candidates[0][1].txn_date,
                    bank_amount=candidates[0][2],
                    date_diff_days=candidates[0][3],
                    amount_diff=Decimal("0"),
                    etsy_entry=etsy_entry,
                    bank_entry=None,
                    match_method="date_window",
                    match_confidence=Confidence.NEEDS_REVIEW,
                    candidates_count=len(candidates),
                    needs_review=True,
                    review_reason=f"{len(candidates)} bank deposits with exact amount within {STRICT_DATE_WINDOW_DAYS}d window",
                ))
            else:
                final_unresolved.append((etsy_entry, etsy_amount))

        # ── Pass 4: Unmatched ──
        self.etsy_unmatched = final_unresolved

        for i, bank_entry in enumerate(bank_deposits):
            if i not in used_bank:
                if "etsy" in bank_entry.title.lower() or "etsy" in bank_entry.category.lower():
                    self.bank_unmatched.append(bank_entry)

        return self._build_result(strict_mode=True, needs_review=needs_review, journal=journal)

    def _build_result(self, strict_mode: bool,
                      needs_review: list[DepositMatch] | None = None,
                      journal: Journal | None = None) -> ReconciliationResult:
        """Build ReconciliationResult with delta analysis.

        Delta = etsy_net_earned - matched_total.
        If delta != 0, it must be explained by traceable journal entries
        (the Etsy deposit rows proving exactly how much was sent).
        """
        nr = needs_review or []
        matched_total = sum((m.etsy_amount for m in self.matched), Decimal("0"))
        etsy_unmatched_total = sum((amt for _, amt in self.etsy_unmatched), Decimal("0"))
        bank_unmatched_total = sum((abs(e.amount) for e in self.bank_unmatched), Decimal("0"))
        nr_total = sum((m.etsy_amount for m in nr), Decimal("0"))

        # ── Delta analysis ──
        etsy_net_earned = Decimal("0")
        csv_deposit_total = Decimal("0")
        delta = Decimal("0")
        delta_entries: list = []
        delta_explained = False
        delta_explanation = ""
        recon_confidence = Confidence.UNKNOWN

        if journal is not None:
            # etsy_net_earned = sum of all Etsy CSV entries
            etsy_csv_entries = journal.by_source(TxnSource.ETSY_CSV)
            etsy_net_earned = sum((e.amount for e in etsy_csv_entries), Decimal("0"))

            # csv_deposit_total = sum of parsed deposit amounts from Etsy deposit titles
            etsy_deposit_tuples = _get_etsy_deposits(journal)
            csv_deposit_total = sum((amt for _, amt in etsy_deposit_tuples), Decimal("0"))

            # delta = earned minus bank-confirmed
            delta = etsy_net_earned - matched_total

            # balance = earned minus Etsy-claimed-deposited
            balance = etsy_net_earned - csv_deposit_total

            if delta == Decimal("0"):
                # Perfect: all earned revenue confirmed in bank
                delta_explained = True
                delta_explanation = "Delta is $0.00 — all earned revenue matched to bank deposits."
                recon_confidence = Confidence.VERIFIED

            elif (etsy_unmatched_total == Decimal("0")
                  and abs(delta - balance) <= Decimal("0.01")):
                # All Etsy CSV deposits matched to bank. Delta = Etsy balance.
                # The deposit entries prove exactly how much was sent.
                deposit_entries_raw = [e for e, _ in etsy_deposit_tuples]
                delta_entries = deposit_entries_raw
                delta_explained = True
                delta_explanation = (
                    f"Delta ${delta:.2f} = Etsy account balance (not yet deposited). "
                    f"Proven by {len(delta_entries)} deposit entries "
                    f"totaling ${csv_deposit_total:.2f}."
                )
                recon_confidence = Confidence.DERIVED

            else:
                # Unexplained gap
                unmatched_gap = csv_deposit_total - matched_total
                delta_explained = False
                delta_explanation = (
                    f"Unexplained delta: ${delta:.2f}. "
                    f"Balance=${balance:.2f}, "
                    f"unmatched_etsy_deposits=${etsy_unmatched_total:.2f}, "
                    f"deposit-to-bank gap=${unmatched_gap:.2f}."
                )
                recon_confidence = Confidence.NEEDS_REVIEW

            # ── Strict mode enforcement ──
            if strict_mode:
                # Ban DERIVED unless delta is zero OR fully explained
                if recon_confidence == Confidence.DERIVED and not delta_explained:
                    recon_confidence = Confidence.NEEDS_REVIEW
                # If there are NEEDS_REVIEW items, overall can't be VERIFIED
                if nr and recon_confidence == Confidence.VERIFIED:
                    recon_confidence = Confidence.DERIVED

        return ReconciliationResult(
            matched=list(self.matched),
            needs_review=nr,
            etsy_unmatched=list(self.etsy_unmatched),
            bank_unmatched=list(self.bank_unmatched),
            matched_total=matched_total,
            etsy_unmatched_total=etsy_unmatched_total,
            bank_unmatched_total=bank_unmatched_total,
            needs_review_total=nr_total,
            strict_mode=strict_mode,
            etsy_net_earned=etsy_net_earned,
            csv_deposit_total=csv_deposit_total,
            delta=delta,
            delta_entries=delta_entries,
            delta_explained=delta_explained,
            delta_explanation=delta_explanation,
            reconciliation_confidence=recon_confidence,
        )

    @staticmethod
    def _extract_reference(text: str) -> str | None:
        """Extract a payout reference ID from text, if present.

        Currently returns None for all Etsy/CapOne data (no reference IDs).
        Ready for Etsy API payout data which includes payout_id.
        """
        import re
        # Etsy API format: "Payout #12345678"
        m = re.search(r'(?:Payout|payout)[# ]+(\d{6,})', text)
        if m:
            return f"payout_{m.group(1)}"
        return None
