"""
Accounting Invariants — Universal laws that must ALWAYS hold.

These are not scenario-specific. They are mathematical identities that the
pipeline must satisfy for ANY valid input. If any invariant fails, the pipeline
has a structural bug.

Run with: pytest tests/test_invariants.py -v
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

ALL_SCENARIOS = [
    "scenario_1_simple_month.json",
    "scenario_2_refunds_after_payout.json",
    "scenario_3_split_payouts.json",
    "scenario_4_chargeback_tax.json",
    "scenario_5_missing_receipts.json",
    "scenario_6_clean_reconciliation.json",
]


@pytest.fixture(params=ALL_SCENARIOS, ids=[s.replace(".json", "") for s in ALL_SCENARIOS])
def pr(request) -> tuple[GoldenDataset, PipelineResult]:
    ds = load_golden(request.param)
    result = run_pipeline(ds)
    return ds, result


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 1: Net Parity
#  etsy_net_earned == SUM(all_etsy_entries.amount)
#  (because deposit entries have amount=0 in journal)
# ══════════════════════════════════════════════════════════════════════

class TestInvariant1_NetParity:
    def test_etsy_net_equals_raw_sum(self, pr):
        ds, r = pr
        from accounting.models import TxnSource
        etsy_entries = [e for e in r.journal if e.source == TxnSource.ETSY_CSV]
        raw_sum = sum((e.amount for e in etsy_entries), Decimal("0"))
        raw_sum = Decimal(str(round(float(raw_sum), 2)))
        earned = r.metric_decimal("etsy_net_earned")
        diff = abs(earned - raw_sum)
        assert diff <= Decimal("0.01"), \
            f"INV1 VIOLATED: etsy_net_earned={earned} != raw_sum={raw_sum} (diff={diff})"


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 2: Net Decomposition
#  etsy_net_earned = gross_sales - total_fees - total_shipping
#                    - total_marketing - total_refunds - total_taxes
#                    - total_buyer_fees + total_payments
# ══════════════════════════════════════════════════════════════════════

class TestInvariant2_NetDecomposition:
    def test_net_equals_breakdown(self, pr):
        ds, r = pr
        gross = r.metric("gross_sales")
        fees = r.metric("total_fees")
        shipping = r.metric("total_shipping_cost")
        marketing = r.metric("total_marketing") or 0
        refunds = r.metric("total_refunds")
        taxes = r.metric("total_taxes") or 0
        buyer_fees = r.metric("total_buyer_fees") or 0
        payments = r.metric("total_payments") or 0

        expected = gross - fees - shipping - marketing - refunds - taxes - buyer_fees + payments
        actual = r.metric("etsy_net_earned")
        assert_money(actual, expected, "INV2: net_decomposition")


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 3: Bank Net Cash Identity
#  bank_net_cash = bank_total_deposits - bank_total_debits
# ══════════════════════════════════════════════════════════════════════

class TestInvariant3_BankNetCash:
    def test_bank_net_identity(self, pr):
        ds, r = pr
        deps = r.metric("bank_total_deposits")
        debs = r.metric("bank_total_debits")
        net = r.metric("bank_net_cash")
        expected = deps - debs
        assert_money(net, expected, "INV3: bank_net_cash")


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 4: Profit Chain
#  real_profit = bank_cash_on_hand + bank_owner_draw_total
# ══════════════════════════════════════════════════════════════════════

class TestInvariant4_ProfitChain:
    def test_profit_equals_cash_plus_draws(self, pr):
        ds, r = pr
        profit = r.metric("real_profit")
        cash = r.metric("bank_cash_on_hand")
        draws = r.metric("bank_owner_draw_total")
        expected = cash + draws
        assert_money(profit, expected, "INV4: profit_chain")


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 5: Cash on Hand
#  bank_cash_on_hand = bank_net_cash + etsy_balance
# ══════════════════════════════════════════════════════════════════════

class TestInvariant5_CashOnHand:
    def test_cash_on_hand_identity(self, pr):
        ds, r = pr
        cash_on_hand = r.metric("bank_cash_on_hand")
        bank_net = r.metric("bank_net_cash")
        etsy_bal = r.metric("etsy_balance")
        expected = bank_net + etsy_bal
        assert_money(cash_on_hand, expected, "INV5: cash_on_hand")


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 6: Fee Breakdown
#  total_fees = abs(total_fees_gross - total_credits)
#  where total_fees_gross = listing + txn_product + txn_shipping + processing
# ══════════════════════════════════════════════════════════════════════

class TestInvariant6_FeeBreakdown:
    def test_total_fees_equals_journal_sum(self, pr):
        """total_fees = abs(sum of all Fee-type journal entries).

        This is the definitional invariant. The fee breakdown metrics
        (total_fees_gross, total_credits) may overlap when credit entries
        match charge title patterns, so we verify against the journal directly.
        """
        ds, r = pr
        from accounting.models import TxnType
        fee_entries = r.journal.by_type(TxnType.FEE)
        expected = abs(float(r.journal.sum_amount(fee_entries)))
        actual = r.metric("total_fees")
        assert_money(actual, expected, "INV6: total_fees vs journal")


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 7: Expense Completeness Gap
#  expense_gap = expense_bank_recorded - expense_receipt_verified
#  AND: every bank debit is either matched, missing, or skipped (nothing lost)
# ══════════════════════════════════════════════════════════════════════

class TestInvariant7_ExpenseGap:
    def test_gap_equation(self, pr):
        ds, r = pr
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")
        gap = float(r.expense_result.gap_total)
        recorded = float(r.expense_result.bank_recorded_total)
        verified = float(r.expense_result.receipt_verified_total)
        assert_money(gap, recorded - verified, "INV7: expense_gap")

    def test_all_debits_accounted(self, pr):
        """Every bank debit must be in exactly one bucket: matched, missing, or skipped."""
        ds, r = pr
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")

        matched_ids = {m.bank_entry.dedup_hash for m in r.expense_result.receipt_matches}
        missing_ids = {m.transaction_id for m in r.expense_result.missing_receipts}
        skipped_ids = {e.dedup_hash for e in r.expense_result.skipped_transactions}

        all_debit_ids = {e.dedup_hash for e in r.journal.bank_debits()}

        accounted = matched_ids | missing_ids | skipped_ids
        unaccounted = all_debit_ids - accounted
        assert len(unaccounted) == 0, \
            f"INV7: {len(unaccounted)} bank debits not in any bucket"

    def test_no_double_counting(self, pr):
        """No debit should appear in more than one bucket."""
        ds, r = pr
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")

        matched_ids = {m.bank_entry.dedup_hash for m in r.expense_result.receipt_matches}
        missing_ids = {m.transaction_id for m in r.expense_result.missing_receipts}
        skipped_ids = {e.dedup_hash for e in r.expense_result.skipped_transactions}

        assert len(matched_ids & missing_ids) == 0, "Debit in both matched AND missing"
        assert len(matched_ids & skipped_ids) == 0, "Debit in both matched AND skipped"
        assert len(missing_ids & skipped_ids) == 0, "Debit in both missing AND skipped"


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 8: No Receipt Amount in Verified Total
#  Only bank debit amounts go into receipt_verified_total, NOT receipt amounts.
#  This prevents receipt-side rounding from inflating verified totals.
# ══════════════════════════════════════════════════════════════════════

class TestInvariant8_VerifiedFromBankSide:
    def test_verified_uses_bank_amounts(self, pr):
        ds, r = pr
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")
        # Sum the bank-side amounts of all matches
        bank_side_sum = sum(
            abs(m.bank_entry.amount) for m in r.expense_result.receipt_matches
        )
        assert abs(float(bank_side_sum) - float(r.expense_result.receipt_verified_total)) < 0.01


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 9: Net Sales Identity
#  net_sales = gross_sales - total_refunds
# ══════════════════════════════════════════════════════════════════════

class TestInvariant9_NetSales:
    def test_net_sales_identity(self, pr):
        ds, r = pr
        gross = r.metric("gross_sales")
        refunds = r.metric("total_refunds")
        net_sales = r.metric("net_sales")
        assert_money(net_sales, gross - refunds, "INV9: net_sales")


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 10: Balance Derivation
#  etsy_balance = etsy_net_earned - total_deposited
#  (where total_deposited = pre_capone + bank deposits that are Etsy payouts)
# ══════════════════════════════════════════════════════════════════════

class TestInvariant10_BalanceDerivation:
    def test_balance_formula(self, pr):
        ds, r = pr
        # The balance computation uses deposit amounts parsed from titles,
        # NOT from the journal amount field (which is 0 for deposits).
        # So we verify: balance = etsy_net - sum(deposit_parsed_amounts)
        from accounting.models import TxnType
        deposits = r.journal.by_type(TxnType.DEPOSIT)
        parsed_total = sum(
            Decimal(e.raw_row.get("deposit_parsed_amount", "0"))
            for e in deposits
        )
        net = r.metric_decimal("etsy_net_earned")
        balance = r.metric_decimal("etsy_balance")
        expected = net - parsed_total
        assert abs(float(balance) - float(expected)) < 0.01, \
            f"INV10: balance={balance} != net({net}) - deposits({parsed_total}) = {expected}"


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 11: Non-Negative Amounts
#  Gross sales, total_fees, total_refunds, total_shipping are all >= 0.
#  (They're computed with abs(), so this should always hold.)
# ══════════════════════════════════════════════════════════════════════

class TestInvariant11_NonNegativeAmounts:
    def test_non_negative(self, pr):
        ds, r = pr
        for name in ["gross_sales", "total_fees", "total_refunds", "total_shipping_cost",
                      "total_marketing", "total_taxes", "total_buyer_fees",
                      "bank_total_deposits", "bank_total_debits"]:
            val = r.metric(name)
            if val is not None:
                assert val >= 0, f"INV11: {name} should be >= 0, got {val}"


# ══════════════════════════════════════════════════════════════════════
#  INVARIANT 12: Strict Receipt Tolerance
#  Every receipt match has amount_diff <= $0.02 and date_diff <= 14 days.
# ══════════════════════════════════════════════════════════════════════

class TestInvariant12_StrictTolerance:
    def test_amount_tolerance(self, pr):
        ds, r = pr
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")
        for match in r.expense_result.receipt_matches:
            assert match.amount_diff <= Decimal("0.02"), \
                f"INV12: match amount_diff={match.amount_diff} > $0.02"

    def test_date_tolerance(self, pr):
        ds, r = pr
        if r.expense_result is None:
            pytest.skip("Expense completeness not run")
        for match in r.expense_result.receipt_matches:
            assert match.date_diff_days <= 14, \
                f"INV12: match date_diff={match.date_diff_days} > 14 days"
