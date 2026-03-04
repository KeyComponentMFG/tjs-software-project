"""
accounting/agents/validation.py — Agent 3: Pre-display validation gate.

Runs BEFORE the Ledger is published. Catches errors before they reach the dashboard.

CRITICAL checks → block display if failed
HIGH checks → warn but display
"""

from __future__ import annotations

import math
from decimal import Decimal

from ..models import Confidence, MetricValue, ValidationResult


class ValidationAgent:
    """Validates computed metrics before they're published to the Ledger."""

    def __init__(self):
        self.results: list[ValidationResult] = []

    def validate_all(self, metrics: dict[str, MetricValue],
                     raw_net_sum: Decimal,
                     strict_mode: bool = False) -> list[ValidationResult]:
        """Run all validation checks.

        Parameters:
            metrics: Computed metrics from ComputationAgent
            raw_net_sum: Direct SUM(Net_Clean) from raw data for parity check
            strict_mode: If True, use tighter thresholds (no %-based tolerances)

        Returns:
            List of ValidationResult (check results)
        """
        self.results = []

        self._check_etsy_net_parity(metrics, raw_net_sum)
        self._check_profit_chain(metrics)
        self._check_no_nan(metrics)
        self._check_balance_sanity(metrics, strict_mode=strict_mode)
        self._check_deposit_reconciliation(metrics, strict_mode=strict_mode)

        return self.results

    @property
    def has_critical_failures(self) -> bool:
        return any(r.severity == "CRITICAL" and not r.passed for r in self.results)

    @property
    def critical_failures(self) -> list[ValidationResult]:
        return [r for r in self.results if r.severity == "CRITICAL" and not r.passed]

    @property
    def warnings(self) -> list[ValidationResult]:
        return [r for r in self.results if r.severity == "HIGH" and not r.passed]

    # ── CRITICAL Checks ──

    def _check_etsy_net_parity(self, metrics: dict[str, MetricValue],
                                raw_net_sum: Decimal):
        """etsy_net_earned must equal raw SUM(Net_Clean) within $0.01."""
        earned = metrics.get("etsy_net_earned")
        if not earned:
            self.results.append(ValidationResult(
                check_name="etsy_net_parity",
                passed=False,
                severity="CRITICAL",
                message="etsy_net_earned metric not found",
                affected_metrics=["etsy_net_earned"],
            ))
            return

        diff = abs(earned.value - raw_net_sum)
        passed = diff <= Decimal("0.01")
        self.results.append(ValidationResult(
            check_name="etsy_net_parity",
            passed=passed,
            severity="CRITICAL",
            message=(f"etsy_net_earned matches raw Net_Clean sum (diff=${diff:.2f})"
                     if passed else
                     f"MISMATCH: etsy_net_earned (${earned.value:.2f}) != raw Net_Clean sum (${raw_net_sum:.2f}), diff=${diff:.2f}"),
            expected=f"${raw_net_sum:.2f}",
            actual=f"${earned.value:.2f}",
            affected_metrics=["etsy_net_earned", "etsy_net", "real_profit"],
        ))

    def _check_profit_chain(self, metrics: dict[str, MetricValue]):
        """real_profit must equal bank_cash_on_hand + bank_owner_draw_total."""
        profit = metrics.get("real_profit")
        cash = metrics.get("bank_cash_on_hand")
        draws = metrics.get("bank_owner_draw_total")

        if not all([profit, cash, draws]):
            self.results.append(ValidationResult(
                check_name="profit_chain",
                passed=False,
                severity="CRITICAL",
                message="Missing metrics for profit chain check",
                affected_metrics=["real_profit"],
            ))
            return

        expected = cash.value + draws.value
        diff = abs(profit.value - expected)
        passed = diff <= Decimal("0.01")
        self.results.append(ValidationResult(
            check_name="profit_chain",
            passed=passed,
            severity="CRITICAL",
            message=(f"Profit chain valid: real_profit = cash + draws"
                     if passed else
                     f"BROKEN: real_profit (${profit.value:.2f}) != cash (${cash.value:.2f}) + draws (${draws.value:.2f})"),
            expected=f"${expected:.2f}",
            actual=f"${profit.value:.2f}",
            affected_metrics=["real_profit", "real_profit_margin"],
        ))

    def _check_no_nan(self, metrics: dict[str, MetricValue]):
        """No metric can be NaN or Infinity. UNKNOWN-confidence metrics are skipped."""
        nan_metrics = []
        for name, mv in metrics.items():
            if mv.confidence == Confidence.UNKNOWN:
                continue  # Intentionally set to 0/None — not a data error
            try:
                f = float(mv.value)
                if math.isnan(f) or math.isinf(f):
                    nan_metrics.append(name)
            except (ValueError, TypeError, OverflowError):
                nan_metrics.append(name)

        passed = len(nan_metrics) == 0
        self.results.append(ValidationResult(
            check_name="no_nan",
            passed=passed,
            severity="CRITICAL",
            message=("All metrics are finite numbers"
                     if passed else
                     f"NaN/Infinity found in: {', '.join(nan_metrics)}"),
            affected_metrics=nan_metrics,
        ))

    # ── HIGH Checks (warn but display) ──

    def _check_balance_sanity(self, metrics: dict[str, MetricValue],
                              strict_mode: bool = False):
        """Etsy balance shouldn't be negative by more than $50 (normal) or $0 (strict)."""
        balance = metrics.get("etsy_balance")
        if not balance:
            return

        threshold = Decimal("0") if strict_mode else Decimal("-50")
        passed = balance.value >= threshold
        severity = "CRITICAL" if strict_mode else "HIGH"
        self.results.append(ValidationResult(
            check_name="balance_sanity",
            passed=passed,
            severity=severity,
            message=(f"Etsy balance is reasonable (${balance.value:.2f})"
                     if passed else
                     f"Etsy balance is significantly negative (${balance.value:.2f}) - possible data gap"),
            actual=f"${balance.value:.2f}",
            affected_metrics=["etsy_balance", "bank_cash_on_hand", "real_profit"],
        ))

    def _check_deposit_reconciliation(self, metrics: dict[str, MetricValue],
                                      strict_mode: bool = False):
        """Etsy deposit total should match bank deposit total.

        Normal mode: 10% or $100 tolerance (accounts for pre-CapOne timing).
        Strict mode: Skip this check — etsy_total_deposited may be UNKNOWN
                     (depends on ESTIMATED pre_capone_deposits). The deterministic
                     reconciliation agent handles deposit matching instead.
        """
        etsy_deposited = metrics.get("etsy_total_deposited")
        bank_deposits = metrics.get("bank_total_deposits")

        if not etsy_deposited or not bank_deposits:
            return

        # In strict mode, if etsy_total_deposited is UNKNOWN, report honestly
        if strict_mode and etsy_deposited.confidence in (
                Confidence.UNKNOWN, Confidence.ESTIMATED):
            self.results.append(ValidationResult(
                check_name="deposit_reconciliation",
                passed=False,
                severity="HIGH",
                message=(f"Deposit reconciliation: UNKNOWN — etsy_total_deposited "
                         f"confidence is {etsy_deposited.confidence.value} "
                         f"(missing pre-CapOne source data). "
                         f"Bank deposits: ${bank_deposits.value:.2f} (VERIFIED). "
                         f"Etsy total deposited: UNKNOWN."),
                expected="etsy_total_deposited confidence = verified",
                actual=f"etsy_total_deposited confidence = {etsy_deposited.confidence.value}",
                affected_metrics=["etsy_total_deposited", "bank_total_deposits"],
            ))
            return

        diff = abs(etsy_deposited.value - bank_deposits.value)
        # Allow some tolerance for pre-CapOne deposits and timing
        threshold = max(Decimal("100"), bank_deposits.value * Decimal("0.1"))
        passed = diff <= threshold
        self.results.append(ValidationResult(
            check_name="deposit_reconciliation",
            passed=passed,
            severity="HIGH",
            message=(f"Deposit totals reconcile within tolerance (diff=${diff:.2f})"
                     if passed else
                     f"Deposit mismatch: Etsy says ${etsy_deposited.value:.2f}, bank says ${bank_deposits.value:.2f} (diff=${diff:.2f})"),
            expected=f"Etsy: ${etsy_deposited.value:.2f}",
            actual=f"Bank: ${bank_deposits.value:.2f}",
            affected_metrics=["etsy_total_deposited", "bank_total_deposits"],
        ))
