"""
Unit tests for accounting/agents/reconciliation.py

Tests deposit matching: exact match, date-shifted match, amount tolerance,
unmatched deposits on both sides.
"""

from datetime import date
from decimal import Decimal

import pytest

from accounting.agents.ingestion import IngestionAgent
from accounting.agents.reconciliation import ReconciliationAgent
from accounting.journal import Journal

from .conftest import load_golden


def _build_journal(ds):
    agent = IngestionAgent()
    journal = Journal()
    agent.ingest_etsy_dataframe(ds.etsy_df, journal)
    agent.ingest_bank_transactions(ds.bank_txns, journal)
    agent.ingest_pre_capone_config(ds.config, journal)
    return journal


class TestScenario1Reconciliation:
    """Simple: 1 Etsy deposit, 1 bank deposit, should match."""

    def test_one_match(self):
        ds = load_golden("scenario_1_simple_month.json")
        journal = _build_journal(ds)

        recon = ReconciliationAgent()
        result = recon.reconcile(journal)

        assert len(result.matched) == 1
        assert len(result.etsy_unmatched) == 0

    def test_matched_amounts(self):
        ds = load_golden("scenario_1_simple_month.json")
        journal = _build_journal(ds)

        recon = ReconciliationAgent()
        recon.reconcile(journal)

        match = recon.matched[0]
        assert match.etsy_amount == Decimal("72.74")
        assert match.bank_amount == Decimal("72.74")
        assert match.amount_diff == Decimal("0")


class TestScenario3SplitPayouts:
    """2 Etsy deposits, 2 bank deposits (one delayed 2 days)."""

    def test_both_matched(self):
        ds = load_golden("scenario_3_split_payouts.json")
        journal = _build_journal(ds)

        recon = ReconciliationAgent()
        result = recon.reconcile(journal)

        assert len(result.matched) == 2
        assert len(result.etsy_unmatched) == 0

    def test_delayed_deposit_within_tolerance(self):
        ds = load_golden("scenario_3_split_payouts.json")
        journal = _build_journal(ds)

        recon = ReconciliationAgent()
        recon.reconcile(journal)

        # Find the match for the delayed deposit ($30.64)
        delayed = [m for m in recon.matched if m.etsy_amount == Decimal("30.64")]
        assert len(delayed) == 1
        assert delayed[0].date_diff_days <= 3


class TestScenario2NoBank:
    """Only 1 bank deposit, no mismatches on bank side."""

    def test_single_match(self):
        ds = load_golden("scenario_2_refunds_after_payout.json")
        journal = _build_journal(ds)

        recon = ReconciliationAgent()
        result = recon.reconcile(journal)

        assert len(result.matched) == 1


class TestEdgeCases:
    def test_empty_journal(self):
        journal = Journal()
        recon = ReconciliationAgent()
        result = recon.reconcile(journal)
        assert len(result.matched) == 0
        assert len(result.etsy_unmatched) == 0
        assert len(result.bank_unmatched) == 0
