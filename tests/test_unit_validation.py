"""
Unit tests for accounting/agents/validation.py

Tests each validation check independently: parity, profit chain, NaN detection,
balance sanity, and deposit reconciliation.
"""

from decimal import Decimal

import pytest

from accounting.agents.validation import ValidationAgent
from accounting.models import Confidence, MetricValue, Provenance


def _make_metric(name: str, value: float, conf=Confidence.VERIFIED, fmt="money"):
    return MetricValue(
        name=name, value=Decimal(str(value)), confidence=conf,
        provenance=Provenance(formula="test"), display_format=fmt,
    )


class TestEtsyNetParity:
    def test_passes_when_equal(self):
        metrics = {"etsy_net_earned": _make_metric("etsy_net_earned", 73.74)}
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("73.74"))
        parity = [r for r in results if r.check_name == "etsy_net_parity"]
        assert len(parity) == 1
        assert parity[0].passed is True

    def test_passes_within_penny(self):
        metrics = {"etsy_net_earned": _make_metric("etsy_net_earned", 73.74)}
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("73.745"))
        parity = [r for r in results if r.check_name == "etsy_net_parity"]
        assert parity[0].passed is True

    def test_fails_when_off(self):
        metrics = {"etsy_net_earned": _make_metric("etsy_net_earned", 73.74)}
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("70.00"))
        parity = [r for r in results if r.check_name == "etsy_net_parity"]
        assert parity[0].passed is False
        assert parity[0].severity == "CRITICAL"


class TestProfitChain:
    def test_passes_when_valid(self):
        metrics = {
            "etsy_net_earned": _make_metric("etsy_net_earned", 100),
            "real_profit": _make_metric("real_profit", 50),
            "bank_cash_on_hand": _make_metric("bank_cash_on_hand", 30),
            "bank_owner_draw_total": _make_metric("bank_owner_draw_total", 20),
        }
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("100"))
        chain = [r for r in results if r.check_name == "profit_chain"]
        assert chain[0].passed is True  # 50 = 30 + 20

    def test_fails_when_broken(self):
        metrics = {
            "etsy_net_earned": _make_metric("etsy_net_earned", 100),
            "real_profit": _make_metric("real_profit", 50),
            "bank_cash_on_hand": _make_metric("bank_cash_on_hand", 30),
            "bank_owner_draw_total": _make_metric("bank_owner_draw_total", 10),
        }
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("100"))
        chain = [r for r in results if r.check_name == "profit_chain"]
        assert chain[0].passed is False  # 50 != 30 + 10


class TestNoNaN:
    def test_passes_with_normal_values(self):
        metrics = {
            "etsy_net_earned": _make_metric("etsy_net_earned", 100),
            "gross_sales": _make_metric("gross_sales", 200),
        }
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("100"))
        nan_check = [r for r in results if r.check_name == "no_nan"]
        assert nan_check[0].passed is True

    def test_fails_with_nan(self):
        metrics = {
            "etsy_net_earned": _make_metric("etsy_net_earned", 100),
            "bad_metric": _make_metric("bad_metric", float("nan")),
        }
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("100"))
        nan_check = [r for r in results if r.check_name == "no_nan"]
        assert nan_check[0].passed is False

    def test_skips_unknown_confidence(self):
        """UNKNOWN-confidence metrics are allowed to be zero (intentionally unknown)."""
        metrics = {
            "etsy_net_earned": _make_metric("etsy_net_earned", 100),
            "buyer_paid_shipping": _make_metric("buyer_paid_shipping", 0, Confidence.UNKNOWN),
        }
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("100"))
        nan_check = [r for r in results if r.check_name == "no_nan"]
        assert nan_check[0].passed is True


class TestBalanceSanity:
    def test_passes_positive_balance(self):
        metrics = {
            "etsy_net_earned": _make_metric("etsy_net_earned", 100),
            "etsy_balance": _make_metric("etsy_balance", 50),
        }
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("100"))
        sanity = [r for r in results if r.check_name == "balance_sanity"]
        assert sanity[0].passed is True

    def test_passes_slightly_negative(self):
        metrics = {
            "etsy_net_earned": _make_metric("etsy_net_earned", 100),
            "etsy_balance": _make_metric("etsy_balance", -30),
        }
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("100"))
        sanity = [r for r in results if r.check_name == "balance_sanity"]
        assert sanity[0].passed is True  # >= -50

    def test_fails_deeply_negative(self):
        metrics = {
            "etsy_net_earned": _make_metric("etsy_net_earned", 100),
            "etsy_balance": _make_metric("etsy_balance", -100),
        }
        agent = ValidationAgent()
        results = agent.validate_all(metrics, Decimal("100"))
        sanity = [r for r in results if r.check_name == "balance_sanity"]
        assert sanity[0].passed is False
        assert sanity[0].severity == "HIGH"
