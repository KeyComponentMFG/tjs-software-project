"""
Integration tests for accounting/pipeline.py

Tests the FULL pipeline (ingest → compute → validate → reconcile → expense)
end-to-end against each golden dataset. Verifies that all expected outputs
match to the penny.
"""

from decimal import Decimal

import pytest

from .conftest import (
    GoldenDataset,
    PipelineResult,
    assert_money,
    load_golden,
    run_pipeline,
)


# ── Parametrized: Every golden dataset through the full pipeline ──

ALL_SCENARIOS = [
    "scenario_1_simple_month.json",
    "scenario_2_refunds_after_payout.json",
    "scenario_3_split_payouts.json",
    "scenario_4_chargeback_tax.json",
    "scenario_5_missing_receipts.json",
    "scenario_6_clean_reconciliation.json",
]


@pytest.fixture(params=ALL_SCENARIOS, ids=[s.replace(".json", "") for s in ALL_SCENARIOS])
def scenario(request):
    ds = load_golden(request.param)
    result = run_pipeline(ds)
    return ds, result


# ── Core Etsy Metrics ──

class TestEtsyMetrics:
    def test_gross_sales(self, scenario):
        ds, r = scenario
        assert_money(r.metric("gross_sales"), ds.expect("gross_sales"), f"{ds.name} gross_sales")

    def test_total_refunds(self, scenario):
        ds, r = scenario
        assert_money(r.metric("total_refunds"), ds.expect("total_refunds"), f"{ds.name} total_refunds")

    def test_total_fees(self, scenario):
        ds, r = scenario
        assert_money(r.metric("total_fees"), ds.expect("total_fees"), f"{ds.name} total_fees")

    def test_total_shipping(self, scenario):
        ds, r = scenario
        assert_money(r.metric("total_shipping_cost"), ds.expect("total_shipping_cost"),
                     f"{ds.name} total_shipping")

    def test_etsy_net_earned(self, scenario):
        ds, r = scenario
        assert_money(r.metric("etsy_net_earned"), ds.expect("etsy_net_earned"),
                     f"{ds.name} etsy_net_earned")

    def test_order_count(self, scenario):
        ds, r = scenario
        assert r.metric("order_count") == ds.expect("order_count"), f"{ds.name} order_count"


# ── Bank Metrics ──

class TestBankMetrics:
    def test_bank_deposits(self, scenario):
        ds, r = scenario
        assert_money(r.metric("bank_total_deposits"), ds.expect("bank_total_deposits"),
                     f"{ds.name} bank_deposits")

    def test_bank_debits(self, scenario):
        ds, r = scenario
        assert_money(r.metric("bank_total_debits"), ds.expect("bank_total_debits"),
                     f"{ds.name} bank_debits")

    def test_bank_net_cash(self, scenario):
        ds, r = scenario
        assert_money(r.metric("bank_net_cash"), ds.expect("bank_net_cash"),
                     f"{ds.name} bank_net_cash")


# ── Balance ──

class TestBalance:
    def test_etsy_balance(self, scenario):
        ds, r = scenario
        assert_money(r.metric("etsy_balance"), ds.expect("etsy_balance"),
                     f"{ds.name} etsy_balance")


# ── Expense Completeness ──

class TestExpenseCompleteness:
    def test_receipt_verified(self, scenario):
        ds, r = scenario
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")
        assert_money(float(r.expense_result.receipt_verified_total),
                     ds.expect("expense_receipt_verified"),
                     f"{ds.name} expense_verified")

    def test_bank_recorded(self, scenario):
        ds, r = scenario
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")
        assert_money(float(r.expense_result.bank_recorded_total),
                     ds.expect("expense_bank_recorded"),
                     f"{ds.name} expense_bank_recorded")

    def test_gap(self, scenario):
        ds, r = scenario
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")
        assert_money(float(r.expense_result.gap_total),
                     ds.expect("expense_gap"),
                     f"{ds.name} expense_gap")

    def test_matched_count(self, scenario):
        ds, r = scenario
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")
        assert len(r.expense_result.receipt_matches) == ds.expect("expense_matched_count"), \
            f"{ds.name} matched_count"

    def test_missing_count(self, scenario):
        ds, r = scenario
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")
        assert len(r.expense_result.missing_receipts) == ds.expect("expense_missing_count"), \
            f"{ds.name} missing_count"


# ── Validation Gate ──

class TestValidationGate:
    def test_parity_passes(self, scenario):
        """etsy_net_parity should PASS for all golden datasets."""
        ds, r = scenario
        parity = [v for v in r.ledger.validations if v.check_name == "etsy_net_parity"]
        assert len(parity) == 1, f"{ds.name}: parity check missing"
        assert parity[0].passed, f"{ds.name}: parity FAILED: {parity[0].message}"

    def test_profit_chain_passes(self, scenario):
        """profit_chain should PASS for all golden datasets."""
        ds, r = scenario
        chain = [v for v in r.ledger.validations if v.check_name == "profit_chain"]
        assert len(chain) == 1, f"{ds.name}: profit_chain check missing"
        assert chain[0].passed, f"{ds.name}: profit_chain FAILED: {chain[0].message}"

    def test_no_nan(self, scenario):
        """no_nan should PASS for all golden datasets."""
        ds, r = scenario
        nan_check = [v for v in r.ledger.validations if v.check_name == "no_nan"]
        assert len(nan_check) == 1
        assert nan_check[0].passed, f"{ds.name}: NaN found: {nan_check[0].message}"

    def test_no_critical_failures(self, scenario):
        """No CRITICAL validation should fail on golden data."""
        ds, r = scenario
        critical_fails = [v for v in r.ledger.validations
                          if v.severity == "CRITICAL" and not v.passed]
        assert len(critical_fails) == 0, \
            f"{ds.name}: {len(critical_fails)} critical failures: " + \
            ", ".join(v.check_name for v in critical_fails)


# ── Reconciliation ──

class TestReconciliation:
    def test_scenario3_both_matched(self):
        ds = load_golden("scenario_3_split_payouts.json")
        result = run_pipeline(ds)
        assert result.recon["matched_count"] == 2
        assert result.recon["etsy_unmatched_count"] == 0

    def test_scenario1_single_match(self):
        ds = load_golden("scenario_1_simple_month.json")
        result = run_pipeline(ds)
        assert result.recon["matched_count"] == 1


# ── Pipeline Stability ──

class TestPipelineStability:
    def test_double_rebuild_idempotent(self):
        """Running pipeline twice on same data should produce identical metrics."""
        ds = load_golden("scenario_1_simple_month.json")
        r1 = run_pipeline(ds)
        r2 = run_pipeline(ds)

        for name in ["gross_sales", "etsy_net_earned", "bank_net_cash", "etsy_balance"]:
            v1 = r1.metric(name)
            v2 = r2.metric(name)
            assert v1 == v2, f"{name}: {v1} != {v2} on second run"

    def test_empty_data(self):
        """Pipeline should handle empty DataFrame without crashing."""
        import pandas as pd
        from accounting.pipeline import AccountingPipeline

        pipeline = AccountingPipeline()
        df = pd.DataFrame(columns=["Date", "Type", "Title", "Info", "Currency",
                                    "Amount", "Fees & Taxes", "Net"])
        ledger = pipeline.full_rebuild(df, [], {"etsy_pre_capone_deposits": 0, "pre_capone_detail": []})
        assert ledger is not None
