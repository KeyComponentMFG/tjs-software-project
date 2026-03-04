"""
accounting/pipeline.py — Orchestrator: ingest → compute → validate → publish.

This is the main entry point. Call full_rebuild() with raw data to get an
immutable Ledger with validated metrics.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

import pandas as pd

from .agents.audit import AuditAgent
from .agents.computation import ComputationAgent
from .agents.expense_completeness import ExpenseCompletenessAgent
from .agents.ingestion import IngestionAgent
from .agents.reconciliation import ReconciliationAgent
from .agents.validation import ValidationAgent
from .journal import Journal
from .ledger import Ledger
from .models import Confidence


class AccountingPipeline:
    """Orchestrates the full accounting pipeline."""

    def __init__(self):
        self.journal = Journal()
        self.ingestion = IngestionAgent()
        self.computation = ComputationAgent()
        self.validation = ValidationAgent()
        self.reconciliation = ReconciliationAgent()
        self.audit = AuditAgent()
        self.expense_completeness = ExpenseCompletenessAgent()
        self._ledger: Optional[Ledger] = None
        self._previous_ledger: Optional[Ledger] = None
        self._expense_result = None
        self._recon_result = None
        self._strict_mode: bool = False

    @property
    def ledger(self) -> Optional[Ledger]:
        return self._ledger

    def full_rebuild(self, etsy_df: pd.DataFrame, bank_txns: list[dict],
                     config: dict, invoices: list[dict] | None = None,
                     strict_mode: bool = False) -> Ledger:
        """Run the complete pipeline: ingest → compute → validate → publish.

        Parameters:
            etsy_df: Etsy transactions DataFrame (with Net_Clean, Date_Parsed, etc.)
            bank_txns: Parsed bank transactions list
            config: Config dict (with pre_capone_detail, etc.)
            invoices: Invoice data for expense completeness (optional)
            strict_mode: If True, use accounting-grade rules (no tolerances, no estimates)

        Returns:
            Immutable Ledger with validated metrics.
        """
        self._strict_mode = strict_mode
        # Save previous ledger for diff
        self._previous_ledger = self._ledger

        # ── Step 1: Ingest ──
        self.journal.clear()

        etsy_count = self.ingestion.ingest_etsy_dataframe(etsy_df, self.journal)
        bank_count = self.ingestion.ingest_bank_transactions(bank_txns, self.journal)
        config_count = self.ingestion.ingest_pre_capone_config(config, self.journal)

        print(f"[Pipeline] Ingested: {etsy_count} Etsy, {bank_count} bank, {config_count} config entries")

        if self.ingestion.warnings:
            for w in self.ingestion.warnings:
                print(f"[Pipeline] WARNING: {w}")

        # ── Step 2: Compute pre-CSV balance ──
        etsy_balance, balance_conf, balance_explanation = (
            self.ingestion.compute_pre_csv_balance(self.journal))
        print(f"[Pipeline] Etsy balance: ${etsy_balance:.2f} ({balance_conf.value}) - {balance_explanation}")

        # Get pre-CapOne deposits total
        pre_capone_deposits = Decimal(str(config.get("etsy_pre_capone_deposits", 0)))

        # ── Step 3: Compute all metrics ──
        metrics = self.computation.compute_all(
            self.journal,
            etsy_balance=etsy_balance,
            etsy_balance_confidence=balance_conf,
            pre_capone_deposits=pre_capone_deposits,
            strict_mode=strict_mode,
        )

        print(f"[Pipeline] Computed {len(metrics)} metrics")

        # ── Step 4: Validate ──
        # Parity check: compare etsy_net_earned (formula breakdown) against
        # the journal's own total net (deduplicated, not the raw DataFrame which may have overlapping CSVs)
        from .models import TxnSource
        _etsy_journal_entries = [e for e in self.journal if e.source == TxnSource.ETSY_CSV]
        raw_net_sum = sum((e.amount for e in _etsy_journal_entries), Decimal("0"))
        raw_net_sum = Decimal(str(round(float(raw_net_sum), 2)))

        validations = self.validation.validate_all(metrics, raw_net_sum, strict_mode=strict_mode)

        for v in validations:
            status = "PASS" if v.passed else "FAIL"
            print(f"[Pipeline] {v.severity} {status}: {v.check_name} - {v.message}")

        # ── Step 5: Reconcile deposits ──
        self._recon_result = self.reconciliation.reconcile(self.journal, strict_mode=strict_mode)
        recon = self._recon_result
        nr_msg = f", {len(recon.needs_review)} needs-review" if recon.needs_review else ""
        print(f"[Pipeline] Reconciliation: {len(recon.matched)} matched, "
              f"{len(recon.etsy_unmatched)} Etsy-only, {len(recon.bank_unmatched)} bank-only{nr_msg}")

        # ── Step 5b: Expense completeness ──
        self._expense_result = None
        if invoices is not None:
            try:
                vendor_map = self._load_vendor_map(config)
                self.expense_completeness.vendor_map = vendor_map
                self._expense_result = self.expense_completeness.run(self.journal, invoices)
                print(f"[Pipeline] Expense completeness: {len(self._expense_result.receipt_matches)} matched, "
                      f"{len(self._expense_result.missing_receipts)} missing")
            except Exception as e:
                print(f"[Pipeline] WARNING: Expense completeness failed: {e}")

        # ── Step 6: Publish Ledger ──
        self._ledger = Ledger(metrics, validations, journal=self.journal,
                              strict_mode=strict_mode)
        print(f"[Pipeline] {self._ledger.summary()}")

        # Report diff if we have a previous snapshot
        if self._previous_ledger:
            changes = self.audit.diff_snapshots(
                self._previous_ledger.metrics, self._ledger.metrics)
            if changes:
                print(f"[Pipeline] {len(changes)} metrics changed since last build:")
                for c in changes[:10]:  # Cap output
                    print(f"  {c}")

        return self._ledger

    def get_bank_by_cat(self) -> dict[str, float]:
        """Get bank category breakdown as float dict for backward compat."""
        raw = getattr(self.computation, '_bank_by_cat', {})
        return {k: float(v) for k, v in raw.items()}

    def get_bank_monthly(self) -> dict[str, dict[str, float]]:
        """Get bank monthly breakdown as float dict for backward compat."""
        raw = getattr(self.computation, '_bank_monthly', {})
        return {k: {"deposits": float(v["deposits"]), "debits": float(v["debits"])}
                for k, v in raw.items()}

    def get_draw_owed_to(self) -> str:
        return getattr(self.computation, '_draw_owed_to', "TJ")

    def get_tulsa_draws_raw(self) -> list:
        """Return raw tulsa draw entries for backward compat."""
        entries = getattr(self.computation, '_tulsa_draws', [])
        return [{"date": _fmt_bank_date(e.txn_date), "desc": e.title,
                 "amount": float(abs(e.amount)), "type": "debit",
                 "category": e.category} for e in entries]

    def get_texas_draws_raw(self) -> list:
        """Return raw texas draw entries for backward compat."""
        entries = getattr(self.computation, '_texas_draws', [])
        return [{"date": _fmt_bank_date(e.txn_date), "desc": e.title,
                 "amount": float(abs(e.amount)), "type": "debit",
                 "category": e.category} for e in entries]

    def get_reconciliation_result(self):
        """Return the ReconciliationResult, or None if not run."""
        return self._recon_result

    def get_expense_completeness(self):
        """Return the ExpenseCompletenessResult, or None if not run."""
        return self._expense_result

    def get_missing_receipts(self) -> list[dict]:
        """Return missing receipts as list[dict] for backward compat."""
        if self._expense_result is None:
            return []
        return [
            {
                "transaction_id": m.transaction_id,
                "vendor": m.vendor,
                "raw_desc": m.raw_desc,
                "date": _fmt_bank_date(m.date),
                "amount": float(m.amount),
                "category": m.bank_category,
                "suggested_category": m.suggested_category,
                "tax_deductible": m.tax_deductible,
                "priority_score": m.priority_score,
                "rationale": m.rationale,
            }
            for m in self._expense_result.missing_receipts
        ]

    @staticmethod
    def _load_vendor_map(config: dict) -> dict[str, str]:
        """Load vendor normalization map from data/vendor_map.json."""
        import json
        import os
        vendor_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "vendor_map.json")
        try:
            with open(vendor_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"[Pipeline] WARNING: Could not load vendor_map.json: {e}")
            return {}


def _fmt_bank_date(d) -> str:
    """Format date as MM/DD/YYYY for backward compat."""
    return f"{d.month:02d}/{d.day:02d}/{d.year}"
