"""
Unit tests for accounting/agents/computation.py

Tests metric computation in isolation: gross sales, fees, refunds, net earned,
bank metrics, fee breakdowns, and draw settlement.
"""

from decimal import Decimal

import pytest

from accounting.agents.computation import ComputationAgent
from accounting.agents.ingestion import IngestionAgent
from accounting.journal import Journal
from accounting.models import Confidence

from .conftest import assert_money, load_golden


def _build_journal_and_compute(ds):
    """Ingest a golden dataset and compute metrics. Returns (metrics, journal)."""
    agent = IngestionAgent()
    journal = Journal()
    agent.ingest_etsy_dataframe(ds.etsy_df, journal)
    agent.ingest_bank_transactions(ds.bank_txns, journal)
    agent.ingest_pre_capone_config(ds.config, journal)

    etsy_balance, balance_conf, _ = agent.compute_pre_csv_balance(journal)
    pre_capone = Decimal(str(ds.config.get("etsy_pre_capone_deposits", 0)))

    comp = ComputationAgent()
    metrics = comp.compute_all(journal, etsy_balance, balance_conf, pre_capone)
    return metrics, journal


# ── Scenario 1: Simple Month ──

class TestScenario1Computation:
    @pytest.fixture(autouse=True)
    def setup(self):
        ds = load_golden("scenario_1_simple_month.json")
        self.metrics, self.journal = _build_journal_and_compute(ds)
        self.expected = ds.expected

    def test_gross_sales(self):
        assert_money(float(self.metrics["gross_sales"].value), 100.00, "gross_sales")

    def test_total_fees(self):
        assert_money(float(self.metrics["total_fees"].value), 11.51, "total_fees")

    def test_total_shipping(self):
        assert_money(float(self.metrics["total_shipping_cost"].value), 14.75, "total_shipping")

    def test_etsy_net_earned(self):
        assert_money(float(self.metrics["etsy_net_earned"].value), 73.74, "etsy_net_earned")

    def test_order_count(self):
        assert int(self.metrics["order_count"].value) == 3

    def test_bank_deposits(self):
        assert_money(float(self.metrics["bank_total_deposits"].value), 72.74, "bank_deposits")

    def test_bank_debits(self):
        assert_money(float(self.metrics["bank_total_debits"].value), 32.29, "bank_debits")

    def test_bank_net_cash(self):
        assert_money(float(self.metrics["bank_net_cash"].value), 40.45, "bank_net_cash")

    def test_etsy_balance(self):
        assert_money(float(self.metrics["etsy_balance"].value), 1.00, "etsy_balance")

    def test_no_refunds(self):
        assert_money(float(self.metrics["total_refunds"].value), 0.00, "total_refunds")


# ── Scenario 2: Refunds After Payout ──

class TestScenario2Computation:
    @pytest.fixture(autouse=True)
    def setup(self):
        ds = load_golden("scenario_2_refunds_after_payout.json")
        self.metrics, self.journal = _build_journal_and_compute(ds)
        self.expected = ds.expected

    def test_gross_sales(self):
        assert_money(float(self.metrics["gross_sales"].value), 80.00, "gross_sales")

    def test_total_refunds(self):
        assert_money(float(self.metrics["total_refunds"].value), 50.00, "total_refunds")

    def test_net_sales(self):
        assert_money(float(self.metrics["net_sales"].value), 30.00, "net_sales")

    def test_total_fees_net_of_credits(self):
        """total_fees = abs(sum of ALL fee entries including credits)."""
        assert_money(float(self.metrics["total_fees"].value), 3.67, "total_fees")

    def test_total_credits(self):
        assert_money(float(self.metrics["total_credits"].value), 5.45, "total_credits")

    def test_etsy_net_earned(self):
        assert_money(float(self.metrics["etsy_net_earned"].value), 12.08, "etsy_net_earned")

    def test_negative_balance(self):
        """Balance should be negative: earned less than deposited."""
        balance = float(self.metrics["etsy_balance"].value)
        assert balance < 0
        assert_money(balance, -49.60, "etsy_balance")


# ── Scenario 4: Chargeback with Tax ──

class TestScenario4Computation:
    @pytest.fixture(autouse=True)
    def setup(self):
        ds = load_golden("scenario_4_chargeback_tax.json")
        self.metrics, self.journal = _build_journal_and_compute(ds)

    def test_taxes(self):
        assert_money(float(self.metrics["total_taxes"].value), 6.40, "total_taxes")

    def test_refund_equals_sale(self):
        """Full chargeback: refund = original sale amount."""
        assert_money(float(self.metrics["total_refunds"].value), 80.00, "total_refunds")

    def test_negative_net(self):
        """Net earned should be negative (chargeback + fees + tax exceed sale)."""
        net = float(self.metrics["etsy_net_earned"].value)
        assert net < 0
        assert_money(net, -11.60, "etsy_net_earned")

    def test_deeply_negative_balance(self):
        balance = float(self.metrics["etsy_balance"].value)
        assert balance < -50  # Well into negative
        assert_money(balance, -71.28, "etsy_balance")

    def test_fees_net_almost_zero(self):
        """After credits, net fees = just listing fee ($0.20)."""
        assert_money(float(self.metrics["total_fees"].value), 0.20, "total_fees")


# ── Fee Breakdown (cross-scenario) ──

class TestFeeBreakdown:
    def test_listing_fees_scenario1(self):
        ds = load_golden("scenario_1_simple_month.json")
        metrics, _ = _build_journal_and_compute(ds)
        assert_money(float(metrics["listing_fees"].value), 0.60, "listing_fees")

    def test_processing_fees_scenario1(self):
        ds = load_golden("scenario_1_simple_month.json")
        metrics, _ = _build_journal_and_compute(ds)
        assert_money(float(metrics["processing_fees"].value), 4.40, "processing_fees")

    def test_total_fees_gross_equals_breakdown(self):
        """total_fees_gross = listing + transaction_product + transaction_shipping + processing."""
        ds = load_golden("scenario_1_simple_month.json")
        metrics, _ = _build_journal_and_compute(ds)
        listing = float(metrics["listing_fees"].value)
        txn_prod = float(metrics["transaction_fees_product"].value)
        txn_ship = float(metrics["transaction_fees_shipping"].value)
        processing = float(metrics["processing_fees"].value)
        gross = float(metrics["total_fees_gross"].value)
        assert_money(listing + txn_prod + txn_ship + processing, gross, "fee_breakdown_sum")


# ── Confidence Levels ──

class TestConfidence:
    def test_sales_are_verified(self):
        ds = load_golden("scenario_1_simple_month.json")
        metrics, _ = _build_journal_and_compute(ds)
        assert metrics["gross_sales"].confidence == Confidence.VERIFIED

    def test_net_is_derived(self):
        ds = load_golden("scenario_1_simple_month.json")
        metrics, _ = _build_journal_and_compute(ds)
        assert metrics["etsy_net_earned"].confidence == Confidence.DERIVED

    def test_buyer_paid_shipping_is_unknown(self):
        ds = load_golden("scenario_1_simple_month.json")
        metrics, _ = _build_journal_and_compute(ds)
        assert metrics["buyer_paid_shipping"].confidence == Confidence.UNKNOWN
