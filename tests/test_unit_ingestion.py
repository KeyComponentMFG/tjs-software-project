"""
Unit tests for accounting/agents/ingestion.py

Tests the IngestionAgent in isolation: Etsy CSV parsing, bank transaction
parsing, deposit amount extraction, and pre-CSV balance computation.
"""

from decimal import Decimal

import pytest

from accounting.agents.ingestion import (
    IngestionAgent,
    _parse_decimal,
    _parse_deposit_amount,
)
from accounting.journal import Journal
from accounting.models import Confidence, TxnSource, TxnType

from .conftest import load_golden


# ── _parse_decimal ──

class TestParseDecimal:
    def test_normal_positive(self):
        val, conf = _parse_decimal("$40.00")
        assert val == Decimal("40.00")
        assert conf == Confidence.VERIFIED

    def test_normal_negative(self):
        val, conf = _parse_decimal("-$3.25")
        assert val == Decimal("-3.25")
        assert conf == Confidence.VERIFIED

    def test_dashes_are_zero(self):
        val, conf = _parse_decimal("--")
        assert val == Decimal("0")
        assert conf == Confidence.VERIFIED

    def test_empty_is_zero(self):
        val, conf = _parse_decimal("")
        assert val == Decimal("0")
        assert conf == Confidence.VERIFIED

    def test_none_is_zero(self):
        val, conf = _parse_decimal(None)
        assert val == Decimal("0")
        assert conf == Confidence.VERIFIED

    def test_garbage_is_unknown(self):
        val, conf = _parse_decimal("abc")
        assert val == Decimal("0")
        assert conf == Confidence.UNKNOWN

    def test_comma_thousands(self):
        val, conf = _parse_decimal("$1,234.56")
        assert val == Decimal("1234.56")


# ── _parse_deposit_amount ──

class TestParseDepositAmount:
    def test_normal_deposit_title(self):
        val, conf = _parse_deposit_amount("$651.33 sent to your bank account")
        assert val == Decimal("651.33")
        assert conf == Confidence.VERIFIED

    def test_comma_amount(self):
        val, conf = _parse_deposit_amount("$1,287.26 sent to your bank account")
        assert val == Decimal("1287.26")
        assert conf == Confidence.VERIFIED

    def test_no_amount(self):
        val, conf = _parse_deposit_amount("No amount here")
        assert val == Decimal("0")
        assert conf == Confidence.UNKNOWN

    def test_non_string(self):
        val, conf = _parse_deposit_amount(None)
        assert val == Decimal("0")
        assert conf == Confidence.UNKNOWN


# ── Etsy DataFrame Ingestion ──

class TestIngestEtsy:
    def test_scenario1_counts(self):
        ds = load_golden("scenario_1_simple_month.json")
        agent = IngestionAgent()
        journal = Journal()
        count = agent.ingest_etsy_dataframe(ds.etsy_df, journal)
        assert count == len(ds.etsy_df)  # All rows should be unique

    def test_scenario1_types(self):
        ds = load_golden("scenario_1_simple_month.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_etsy_dataframe(ds.etsy_df, journal)

        assert len(journal.by_type(TxnType.SALE)) == 3
        assert len(journal.by_type(TxnType.FEE)) == 9
        assert len(journal.by_type(TxnType.SHIPPING)) == 3
        assert len(journal.by_type(TxnType.DEPOSIT)) == 1

    def test_sale_amounts_positive(self):
        ds = load_golden("scenario_1_simple_month.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_etsy_dataframe(ds.etsy_df, journal)

        for sale in journal.by_type(TxnType.SALE):
            assert sale.amount > 0, f"Sale should be positive: {sale}"

    def test_fee_amounts_negative(self):
        """Standard fee entries (not credits) should have negative amounts."""
        ds = load_golden("scenario_1_simple_month.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_etsy_dataframe(ds.etsy_df, journal)

        for fee in journal.by_type(TxnType.FEE):
            assert fee.amount < 0, f"Fee should be negative: {fee}"

    def test_deposit_amount_is_zero(self):
        """Deposit entry amount should be 0 (Net='--'), with parsed amount in raw_row."""
        ds = load_golden("scenario_1_simple_month.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_etsy_dataframe(ds.etsy_df, journal)

        deposits = journal.by_type(TxnType.DEPOSIT)
        assert len(deposits) == 1
        assert deposits[0].amount == Decimal("0")
        assert deposits[0].raw_row["deposit_parsed_amount"] == "72.74"

    def test_refund_scenario2(self):
        ds = load_golden("scenario_2_refunds_after_payout.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_etsy_dataframe(ds.etsy_df, journal)

        refunds = journal.by_type(TxnType.REFUND)
        assert len(refunds) == 1
        assert refunds[0].amount == Decimal("-50.00")

    def test_fee_credits_positive(self):
        """Credit-for-fee entries should have positive Net amounts."""
        ds = load_golden("scenario_2_refunds_after_payout.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_etsy_dataframe(ds.etsy_df, journal)

        credits = [e for e in journal.by_type(TxnType.FEE) if "Credit for" in e.title]
        assert len(credits) == 2
        for c in credits:
            assert c.amount > 0, f"Fee credit should be positive: {c}"

    def test_dedup_prevents_double_ingest(self):
        ds = load_golden("scenario_1_simple_month.json")
        agent = IngestionAgent()
        journal = Journal()
        count1 = agent.ingest_etsy_dataframe(ds.etsy_df, journal)
        count2 = agent.ingest_etsy_dataframe(ds.etsy_df, journal)
        assert count1 > 0
        assert count2 == 0  # All duplicates


# ── Bank Transaction Ingestion ──

class TestIngestBank:
    def test_scenario1_counts(self):
        ds = load_golden("scenario_1_simple_month.json")
        agent = IngestionAgent()
        journal = Journal()
        count = agent.ingest_bank_transactions(ds.bank_txns, journal)
        assert count == 3  # 1 deposit + 2 debits

    def test_deposits_positive(self):
        ds = load_golden("scenario_1_simple_month.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_bank_transactions(ds.bank_txns, journal)

        for dep in journal.bank_deposits():
            assert dep.amount > 0, f"Bank deposit should be positive: {dep}"

    def test_debits_negative(self):
        ds = load_golden("scenario_1_simple_month.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_bank_transactions(ds.bank_txns, journal)

        for deb in journal.bank_debits():
            assert deb.amount < 0, f"Bank debit should be negative: {deb}"

    def test_categories_preserved(self):
        ds = load_golden("scenario_5_missing_receipts.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_bank_transactions(ds.bank_txns, journal)

        cats = {e.category for e in journal.bank_debits()}
        assert "Amazon Inventory" in cats
        assert "Craft Supplies" in cats
        assert "Subscriptions" in cats
        assert "Shipping" in cats


# ── Pre-CSV Balance ──

class TestPreCsvBalance:
    def test_scenario1_balance(self):
        ds = load_golden("scenario_1_simple_month.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_etsy_dataframe(ds.etsy_df, journal)
        agent.ingest_pre_capone_config(ds.config, journal)

        balance, conf, explanation = agent.compute_pre_csv_balance(journal)
        assert abs(float(balance) - 1.00) < 0.01

    def test_scenario2_negative_balance(self):
        ds = load_golden("scenario_2_refunds_after_payout.json")
        agent = IngestionAgent()
        journal = Journal()
        agent.ingest_etsy_dataframe(ds.etsy_df, journal)
        agent.ingest_pre_capone_config(ds.config, journal)

        balance, conf, explanation = agent.compute_pre_csv_balance(journal)
        assert float(balance) < 0, "Balance should be negative after refund post-payout"
        assert abs(float(balance) - (-49.60)) < 0.01
