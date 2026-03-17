"""
Unit tests for accounting/agents/expense_completeness.py

Tests receipt matching, vendor normalization, skip categories, missing receipt
detection, priority scoring, and gap calculations.
"""

from datetime import date
from decimal import Decimal

import pytest

from accounting.agents.expense_completeness import (
    CATEGORY_TO_RECEIPT_SOURCES,
    SKIP_CATEGORIES,
    ExpenseCompletenessAgent,
)
from accounting.agents.ingestion import IngestionAgent
from accounting.journal import Journal
from accounting.models import Confidence

from .conftest import assert_money, load_golden


def _build_journal(ds):
    agent = IngestionAgent()
    journal = Journal()
    agent.ingest_etsy_dataframe(ds.etsy_df, journal)
    agent.ingest_bank_transactions(ds.bank_txns, journal)
    agent.ingest_pre_capone_config(ds.config, journal)
    return journal


# ── Vendor Normalization ──

class TestVendorNormalization:
    def test_amazon_match(self):
        agent = ExpenseCompletenessAgent({"AMAZON MKTPL": "Amazon"})
        result = agent._normalize_vendor("AMAZON MKTPL ABC123")
        assert result == "Amazon"

    def test_hobbylobby_match(self):
        agent = ExpenseCompletenessAgent({"HOBBYLOBBY": "Hobby Lobby"})
        result = agent._normalize_vendor("HOBBYLOBBY 452 TULSA OK")
        assert result == "Hobby Lobby"

    def test_case_insensitive(self):
        agent = ExpenseCompletenessAgent({"UPS STORE": "UPS Store"})
        result = agent._normalize_vendor("ups store 1234")
        assert result == "UPS Store"

    def test_fallback_truncates(self):
        agent = ExpenseCompletenessAgent({})
        result = agent._normalize_vendor("SOME VERY LONG DESCRIPTION THAT EXCEEDS FORTY CHARACTERS TOTAL")
        assert len(result) <= 40

    def test_empty_map(self):
        agent = ExpenseCompletenessAgent({})
        result = agent._normalize_vendor("RANDOM VENDOR")
        assert result == "RANDOM VENDOR"


# ── Skip Categories ──

class TestSkipCategories:
    def test_etsy_fees_skipped(self):
        assert "Etsy Fees" in SKIP_CATEGORIES

    def test_owner_draws_skipped(self):
        assert "Owner Draw - Tulsa" in SKIP_CATEGORIES
        assert "Owner Draw - Texas" in SKIP_CATEGORIES

    def test_etsy_payout_skipped(self):
        assert "Etsy Payout" in SKIP_CATEGORIES

    def test_amazon_not_skipped(self):
        assert "Amazon Inventory" not in SKIP_CATEGORIES

    def test_shipping_skipped(self):
        assert "Shipping" in SKIP_CATEGORIES

    def test_subscriptions_skipped(self):
        assert "Subscriptions" in SKIP_CATEGORIES

    def test_personal_skipped(self):
        assert "Personal" in SKIP_CATEGORIES


# ── Scenario 1: 1 Matched, 1 Missing ──

class TestScenario1ExpenseCompleteness:
    @pytest.fixture(autouse=True)
    def setup(self):
        ds = load_golden("scenario_1_simple_month.json")
        journal = _build_journal(ds)
        agent = ExpenseCompletenessAgent({"AMAZON MKTPL": "Amazon", "USPS": "USPS"})
        self.result = agent.run(journal, ds.invoices)

    def test_matched_count(self):
        assert len(self.result.receipt_matches) == 1

    def test_matched_is_amazon(self):
        match = self.result.receipt_matches[0]
        assert match.bank_entry.category == "Amazon Inventory"
        assert match.amount_diff <= Decimal("0.02")

    def test_missing_count(self):
        assert len(self.result.missing_receipts) == 0

    def test_shipping_is_skipped(self):
        skipped_cats = {e.category for e in self.result.skipped_transactions}
        assert "Shipping" in skipped_cats

    def test_gap(self):
        assert_money(float(self.result.gap_total), 0.00, "gap_total")

    def test_verified_total(self):
        assert_money(float(self.result.receipt_verified_total), 23.79, "verified")

    def test_bank_recorded_total(self):
        assert_money(float(self.result.bank_recorded_total), 23.79, "bank_recorded")

    def test_gap_equation(self):
        """gap = bank_recorded - verified."""
        gap = float(self.result.gap_total)
        diff = float(self.result.bank_recorded_total) - float(self.result.receipt_verified_total)
        assert abs(gap - diff) < 0.01


# ── Scenario 2: No Bank Debits ──

class TestScenario2ExpenseCompleteness:
    def test_all_zeros(self):
        ds = load_golden("scenario_2_refunds_after_payout.json")
        journal = _build_journal(ds)
        agent = ExpenseCompletenessAgent()
        result = agent.run(journal, ds.invoices)

        assert len(result.receipt_matches) == 0
        assert len(result.missing_receipts) == 0
        assert result.receipt_verified_total == Decimal("0")
        assert result.bank_recorded_total == Decimal("0")


# ── Scenario 5: Multiple Missing ──

class TestScenario5ExpenseCompleteness:
    @pytest.fixture(autouse=True)
    def setup(self):
        ds = load_golden("scenario_5_missing_receipts.json")
        journal = _build_journal(ds)
        agent = ExpenseCompletenessAgent({
            "AMAZON MKTPL": "Amazon", "HOBBYLOBBY": "Hobby Lobby",
            "PAYPAL THANGS": "Thangs 3D", "UPS STORE": "UPS Store",
        })
        self.result = agent.run(journal, ds.invoices)
        self.expected = ds.expected

    def test_matched_count(self):
        assert len(self.result.receipt_matches) == 2

    def test_missing_count(self):
        assert len(self.result.missing_receipts) == 1

    def test_verified_total(self):
        assert_money(float(self.result.receipt_verified_total), 47.97, "verified")

    def test_gap(self):
        # Only Amazon $18.99 is unmatched (Shipping/Subscriptions are skipped)
        assert_money(float(self.result.gap_total), 18.99, "gap")

    def test_missing_sorted_by_priority(self):
        """Missing receipts should be sorted by priority_score descending."""
        scores = [m.priority_score for m in self.result.missing_receipts]
        assert scores == sorted(scores, reverse=True)

    def test_tax_deductible_priority_higher(self):
        """Tax-deductible items should have 2x priority score."""
        for m in self.result.missing_receipts:
            if m.tax_deductible:
                assert m.priority_score == int(float(m.amount) * 2)
            else:
                assert m.priority_score == int(float(m.amount))

    def test_strict_tolerance_rejects_late_receipt(self):
        """Amazon receipt dated June 1 should NOT match May 7 debit (25 days > 14 limit)."""
        # The $18.99 Amazon debit should be in missing, not matched
        missing_amounts = [float(m.amount) for m in self.result.missing_receipts]
        assert 18.99 in missing_amounts

    def test_by_category_breakdown(self):
        by_cat = self.result.by_category
        assert "Amazon Inventory" in by_cat
        assert "Craft Supplies" in by_cat
        # Shipping and Subscriptions are now skipped — not in expense categories
        assert "Subscriptions" not in by_cat
        assert "Shipping" not in by_cat

    def test_category_gap_sums(self):
        """Sum of per-category gaps should equal total gap."""
        cat_gaps = sum(float(v["gap"]) for v in self.result.by_category.values())
        assert_money(cat_gaps, float(self.result.gap_total), "category_gap_sum")
