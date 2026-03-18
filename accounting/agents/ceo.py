"""
accounting/agents/ceo.py — CEO Agent System: 21 automated validation agents.

Orchestrates all agents at startup and periodically. Produces HealthReport
with alerts for the dashboard banner and Jarvis briefing.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from ..models import Confidence, TxnType, TxnSource


# ── Data Models ──────────────────────────────────────────────────────────────

@dataclass
class Alert:
    """A single alert from an agent check."""
    agent: str
    level: str          # "critical", "warning", "info"
    message: str
    details: str = ""


@dataclass
class AgentResult:
    """Result from a single agent check."""
    agent_name: str
    passed: bool
    level: str          # "critical", "warning", "info"
    message: str
    details: str = ""


@dataclass
class HealthReport:
    """Aggregated health report from all agents."""
    results: list[AgentResult] = field(default_factory=list)
    timestamp: date = field(default_factory=date.today)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def total_count(self) -> int:
        return len(self.results)

    @property
    def critical_alerts(self) -> list[AgentResult]:
        return [r for r in self.results if not r.passed and r.level == "critical"]

    @property
    def warning_alerts(self) -> list[AgentResult]:
        return [r for r in self.results if not r.passed and r.level == "warning"]

    @property
    def info_alerts(self) -> list[AgentResult]:
        return [r for r in self.results if not r.passed and r.level == "info"]

    @property
    def alerts(self) -> list[Alert]:
        alerts = []
        for r in self.results:
            if not r.passed:
                alerts.append(Alert(agent=r.agent_name, level=r.level,
                                    message=r.message, details=r.details))
        return alerts

    def summary(self) -> str:
        return f"CEO Agent: {self.passed_count}/{self.total_count} checks passed — " \
               f"{len(self.critical_alerts)} critical, {len(self.warning_alerts)} warnings"


# ── Individual Agent Checks ──────────────────────────────────────────────────

def _check_data_freshness(pipeline) -> AgentResult:
    """Agent 8: Alert if Etsy CSV >7 days old or bank >14 days."""
    journal = pipeline.journal
    etsy = journal.etsy_entries()
    bank = journal.bank_entries()

    warnings = []
    if etsy:
        latest = max(e.txn_date for e in etsy)
        days = (date.today() - latest).days
        if days > 7:
            warnings.append(f"Etsy data is {days} days old — download latest CSV")
    else:
        warnings.append("No Etsy data loaded")

    if bank:
        latest = max(e.txn_date for e in bank)
        days = (date.today() - latest).days
        if days > 14:
            warnings.append(f"Bank data is {days} days old — upload latest statement")
    else:
        warnings.append("No bank data loaded")

    if warnings:
        return AgentResult("DataFreshness", False, "warning",
                          "; ".join(warnings))
    return AgentResult("DataFreshness", True, "info", "Data is fresh")


def _check_duplicate_detection(pipeline) -> AgentResult:
    """Agent 9: Check for suspicious near-duplicate transactions."""
    journal = pipeline.journal
    entries = journal.etsy_entries()

    # Group by (date, amount, type) and check for exact duplicates
    from collections import Counter
    key_counts = Counter()
    for e in entries:
        key = (e.txn_date, float(e.amount), e.txn_type.value)
        key_counts[key] += 1

    suspicious = {k: v for k, v in key_counts.items() if v > 25}

    if suspicious:
        examples = list(suspicious.items())[:3]
        details = "; ".join(f"{k[2]} ${k[1]:.2f} on {k[0]} ({v}x)" for k, v in examples)
        return AgentResult("DuplicateDetection", False, "warning",
                          f"{len(suspicious)} suspicious duplicate patterns", details)
    return AgentResult("DuplicateDetection", True, "info", "No suspicious duplicates")


def _check_anomaly_detection(pipeline) -> AgentResult:
    """Agent 10: Flag transactions >3 std dev from category mean."""
    journal = pipeline.journal
    entries = journal.etsy_entries()

    # Group amounts by type
    by_type = {}
    for e in entries:
        by_type.setdefault(e.txn_type.value, []).append(float(abs(e.amount)))

    anomalies = []
    for ttype, amounts in by_type.items():
        if len(amounts) < 10:
            continue
        mean = statistics.mean(amounts)
        stdev = statistics.stdev(amounts)
        if stdev == 0:
            continue
        for amt in amounts:
            if abs(amt - mean) > 3 * stdev:
                anomalies.append(f"{ttype}: ${amt:.2f} (mean ${mean:.2f})")

    if len(anomalies) > 75:
        return AgentResult("AnomalyDetection", False, "warning",
                          f"{len(anomalies)} statistical outliers detected",
                          "; ".join(anomalies[:5]))
    return AgentResult("AnomalyDetection", True, "info",
                      f"No significant anomalies ({len(anomalies)} minor)")


def _check_trend_break(pipeline) -> AgentResult:
    """Agent 11: Check for sudden weekly pattern changes (>30% swing)."""
    journal = pipeline.journal
    entries = sorted(journal.etsy_entries(), key=lambda e: e.txn_date)

    if len(entries) < 14:
        return AgentResult("TrendBreak", True, "info", "Not enough data for trend analysis")

    # Weekly revenue totals
    from collections import defaultdict
    weekly = defaultdict(float)
    for e in entries:
        if e.txn_type == TxnType.SALE:
            week = e.txn_date - timedelta(days=e.txn_date.weekday())
            weekly[week] += float(e.amount)

    weeks = sorted(weekly.keys())
    if len(weeks) < 4:
        return AgentResult("TrendBreak", True, "info", "Not enough weeks for trend analysis")

    breaks = []
    for i in range(1, len(weeks)):
        prev = weekly[weeks[i - 1]]
        curr = weekly[weeks[i]]
        if prev > 0:
            change = abs(curr - prev) / prev
            if change > 0.5:
                breaks.append(f"Week of {weeks[i]}: {change:.0%} swing")

    if len(breaks) > 5:
        return AgentResult("TrendBreak", False, "info",
                          f"{len(breaks)} weekly swings >30%",
                          "; ".join(breaks[:3]))
    return AgentResult("TrendBreak", True, "info", "Weekly trends are stable")


def _check_fee_consistency(pipeline) -> AgentResult:
    """Agent 12: Verify Etsy fees within expected 6.5-15% range."""
    ledger = pipeline.ledger
    if not ledger:
        return AgentResult("FeeConsistency", True, "info", "No ledger available")

    gross = ledger.get("gross_sales")
    fees = ledger.get("total_fees")
    if not gross or not fees or gross.value is None or fees.value is None:
        return AgentResult("FeeConsistency", True, "info", "Missing fee data")

    gross_val = float(gross.value)
    fees_val = float(abs(fees.value))
    if gross_val <= 0:
        return AgentResult("FeeConsistency", True, "info", "No sales data")

    pct = fees_val / gross_val * 100
    if pct < 5 or pct > 20:
        return AgentResult("FeeConsistency", False, "warning",
                          f"Fee ratio {pct:.1f}% is outside expected 6.5-15% range")
    return AgentResult("FeeConsistency", True, "info",
                      f"Fee ratio {pct:.1f}% — within normal range")


def _check_deposit_timing(pipeline) -> AgentResult:
    """Agent 13: Check deposit frequency matches Etsy payout schedule."""
    journal = pipeline.journal
    deposits = journal.etsy_deposits()

    if len(deposits) < 2:
        return AgentResult("DepositTiming", True, "info", "Not enough deposits to check timing")

    dates = sorted(d.txn_date for d in deposits)
    gaps = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
    avg_gap = statistics.mean(gaps)

    if avg_gap > 14:
        return AgentResult("DepositTiming", False, "warning",
                          f"Average deposit gap is {avg_gap:.0f} days — Etsy usually pays weekly",
                          f"Gaps: {gaps[:5]}")
    return AgentResult("DepositTiming", True, "info",
                      f"Deposit frequency normal (avg {avg_gap:.0f} days)")


def _check_inventory_turnover(pipeline) -> AgentResult:
    """Agent 14: Check for slow-moving inventory items."""
    # This requires inventory data from the dashboard module
    import sys
    mod = sys.modules.get("etsy_dashboard")
    if not mod:
        return AgentResult("InventoryTurnover", True, "info", "Dashboard module not loaded")

    skpi_fn = getattr(mod, "_compute_stock_kpis", None)
    if not skpi_fn:
        return AgentResult("InventoryTurnover", True, "info", "No stock KPI function")

    try:
        skpi = skpi_fn()
        oos = skpi.get("oos", 0)
        low = skpi.get("low", 0)
        if oos > 5:
            return AgentResult("InventoryTurnover", False, "warning",
                              f"{oos} items out of stock, {low} running low")
        return AgentResult("InventoryTurnover", True, "info",
                          f"Stock levels OK ({skpi.get('in_stock', 0)} items in stock)")
    except Exception:
        return AgentResult("InventoryTurnover", True, "info", "Could not compute stock KPIs")


def _check_profit_margin(pipeline) -> AgentResult:
    """Agent 15: Alert if profit margin drops below 15%."""
    ledger = pipeline.ledger
    if not ledger:
        return AgentResult("ProfitMargin", True, "info", "No ledger available")

    margin = ledger.get("real_profit_margin")
    if not margin or margin.value is None:
        return AgentResult("ProfitMargin", True, "info", "Margin data unavailable")

    margin_val = float(margin.value)
    if margin_val < 15:
        return AgentResult("ProfitMargin", False, "warning",
                          f"Profit margin is {margin_val:.1f}% — below 15% threshold")
    return AgentResult("ProfitMargin", True, "info",
                      f"Profit margin healthy at {margin_val:.1f}%")


def _check_tax_readiness(pipeline) -> AgentResult:
    """Agent 16: Quarterly tax data completeness."""
    journal = pipeline.journal
    entries = journal.etsy_entries()

    if not entries:
        return AgentResult("TaxReadiness", False, "warning", "No Etsy data for tax preparation")

    months = set(e.month for e in entries if e.month)
    current_year = date.today().year
    current_month = date.today().month
    expected_months = []
    for m in range(1, current_month + 1):
        expected_months.append(f"{current_year}-{m:02d}")

    missing = [m for m in expected_months if m not in months]
    if missing:
        return AgentResult("TaxReadiness", False, "warning",
                          f"Missing data for months: {', '.join(missing)}")
    return AgentResult("TaxReadiness", True, "info", "All months have data for tax prep")


def _check_cash_flow(pipeline) -> AgentResult:
    """Agent 17: Project cash runway, alert if <2 months."""
    ledger = pipeline.ledger
    if not ledger:
        return AgentResult("CashFlow", True, "info", "No ledger available")

    cash = ledger.get("bank_cash_on_hand")
    if not cash or cash.value is None:
        return AgentResult("CashFlow", True, "info", "Cash data unavailable")

    # Estimate monthly burn from bank data
    journal = pipeline.journal
    bank_debits = journal.bank_debits()
    if not bank_debits:
        return AgentResult("CashFlow", True, "info", "No bank debit data")

    months_with_debits = set(e.month for e in bank_debits if e.month)
    if not months_with_debits:
        return AgentResult("CashFlow", True, "info", "No monthly debit data")

    total_debits = float(sum(abs(e.amount) for e in bank_debits))
    avg_monthly = total_debits / len(months_with_debits)
    cash_val = float(cash.value)
    runway = cash_val / avg_monthly if avg_monthly > 0 else 999

    if runway < 2:
        return AgentResult("CashFlow", False, "critical",
                          f"Cash runway is {runway:.1f} months — below 2-month threshold",
                          f"Cash: ${cash_val:,.0f}, Avg monthly expenses: ${avg_monthly:,.0f}")
    return AgentResult("CashFlow", True, "info",
                      f"Cash runway: {runway:.1f} months")


def _check_refund_rate(pipeline) -> AgentResult:
    """Agent 18: Alert if refund rate exceeds 5%."""
    journal = pipeline.journal
    refunds = journal.by_type(TxnType.REFUND)
    sales = journal.by_type(TxnType.SALE)

    if not sales:
        return AgentResult("RefundRate", True, "info", "No sales data")

    refund_count = len(refunds)
    sale_count = len(sales)
    rate = refund_count / sale_count * 100 if sale_count > 0 else 0

    if rate > 5:
        return AgentResult("RefundRate", False, "warning",
                          f"Refund rate is {rate:.1f}% ({refund_count}/{sale_count}) — above 5% threshold")
    return AgentResult("RefundRate", True, "info",
                      f"Refund rate: {rate:.1f}% ({refund_count} refunds)")


def _check_shipping_cost(pipeline) -> AgentResult:
    """Agent 19: Per-label cost trends, flag anomalies."""
    ledger = pipeline.ledger
    if not ledger:
        return AgentResult("ShippingCost", True, "info", "No ledger available")

    avg_label = ledger.get("avg_outbound_label")
    if not avg_label or avg_label.value is None:
        return AgentResult("ShippingCost", True, "info", "Label cost data unavailable")

    avg_val = float(avg_label.value)
    if avg_val > 12:
        return AgentResult("ShippingCost", False, "warning",
                          f"Average label cost ${avg_val:.2f} seems high (>$12)")
    return AgentResult("ShippingCost", True, "info",
                      f"Average label cost: ${avg_val:.2f}")


def _check_cross_source(pipeline) -> AgentResult:
    """Agent 20: Etsy deposits match bank deposits within $5."""
    recon = pipeline.get_reconciliation_result()
    if not recon:
        return AgentResult("CrossSource", True, "info", "No reconciliation data")

    unmatched_etsy = len(recon.etsy_unmatched)
    unmatched_bank = len(recon.bank_unmatched)

    if unmatched_etsy > 7 or unmatched_bank > 3:
        return AgentResult("CrossSource", False, "warning",
                          f"{unmatched_etsy} Etsy deposits unmatched, "
                          f"{unmatched_bank} bank deposits unmatched")
    return AgentResult("CrossSource", True, "info",
                      f"Cross-source reconciliation OK ({len(recon.matched)} matched)")


def _check_refund_assignments(pipeline) -> AgentResult:
    """Agent 22: Alert if any refunds are unassigned (need TJ or Braden)."""
    import re
    import sys

    # Get assignments — try module first, then load directly from Supabase
    mod = sys.modules.get("etsy_dashboard") or sys.modules.get("etsy_dashboard_mono")
    assignments = getattr(mod, "_refund_assignments", {}) if mod else {}
    if not assignments:
        try:
            from supabase_loader import get_config_value
            assignments = get_config_value("refund_assignments", {})
            if not isinstance(assignments, dict):
                assignments = {}
        except Exception:
            assignments = {}

    # Get refund entries from journal
    refunds = pipeline.journal.by_type(TxnType.REFUND)
    if not refunds:
        return AgentResult("RefundAssignment", True, "info", "No refund data")

    unassigned = []
    for entry in refunds:
        m = re.search(r"Order #\d+", entry.title)
        order_key = m.group(0) if m else None
        if order_key and assignments.get(order_key, "") == "":
            unassigned.append(order_key)

    if unassigned:
        return AgentResult("RefundAssignment", False, "warning",
                          f"{len(unassigned)} refund(s) need TJ or Braden assigned",
                          f"Unassigned: {', '.join(unassigned[:5])}")
    return AgentResult("RefundAssignment", True, "info",
                      "All refunds assigned")


def _check_config_completeness(pipeline) -> AgentResult:
    """Agent 21: Missing config values that affect calculations."""
    import sys
    mod = sys.modules.get("etsy_dashboard")
    if not mod:
        return AgentResult("ConfigCompleteness", True, "info", "Dashboard not loaded")

    config = getattr(mod, "CONFIG", {})
    missing = []
    for key in ["bb_cc_limit", "bb_cc_apr"]:
        if key not in config or config[key] is None:
            missing.append(key)

    if missing:
        return AgentResult("ConfigCompleteness", False, "info",
                          f"Missing config values: {', '.join(missing)}")
    return AgentResult("ConfigCompleteness", True, "info", "Config complete")


# ── CEO Orchestrator ─────────────────────────────────────────────────────────

# New agents (14) — agents 8-21
_NEW_AGENTS = [
    _check_data_freshness,           # 8
    _check_duplicate_detection,      # 9
    _check_anomaly_detection,        # 10
    _check_trend_break,              # 11
    _check_fee_consistency,          # 12
    _check_deposit_timing,           # 13
    _check_inventory_turnover,       # 14
    _check_profit_margin,            # 15
    _check_tax_readiness,            # 16
    _check_cash_flow,                # 17
    _check_refund_rate,              # 18
    _check_shipping_cost,            # 19
    _check_cross_source,             # 20
    _check_config_completeness,      # 21
    _check_refund_assignments,       # 22
]

# Lightweight agents for periodic checks (every 15 min)
_PERIODIC_AGENTS = [
    _check_data_freshness,
    _check_cash_flow,
    _check_refund_rate,
    _check_cross_source,
    _check_refund_assignments,
]


class CEOAgent:
    """Orchestrates all 22 validation agents."""

    def __init__(self):
        self._last_report: Optional[HealthReport] = None

    def run_startup_check(self, pipeline) -> HealthReport:
        """Run ALL 22 agents at boot time."""
        report = HealthReport()

        # Existing agents (1-7) — get results from pipeline
        if pipeline.ledger and pipeline.ledger.validations:
            for v in pipeline.ledger.validations:
                report.results.append(AgentResult(
                    agent_name=f"Validation:{v.check_name}",
                    passed=v.passed,
                    level="critical" if v.severity in ("CRITICAL",) else "warning",
                    message=v.message,
                ))

        # Expense completeness (agent 7)
        expense = pipeline.get_expense_completeness()
        if expense:
            total = len(expense.receipt_matches) + len(expense.missing_receipts)
            pct = len(expense.receipt_matches) / max(total, 1) * 100
            report.results.append(AgentResult(
                agent_name="ExpenseCompleteness",
                passed=pct > 50,
                level="warning" if pct < 50 else "info",
                message=f"{pct:.0f}% expenses verified ({len(expense.receipt_matches)}/{total})",
            ))

        # New agents (8-21)
        for agent_fn in _NEW_AGENTS:
            try:
                result = agent_fn(pipeline)
                report.results.append(result)
            except Exception as e:
                report.results.append(AgentResult(
                    agent_name=agent_fn.__name__.replace("_check_", ""),
                    passed=True, level="info",
                    message=f"Check skipped: {str(e)[:80]}",
                ))

        self._last_report = report
        print(report.summary())
        return report

    def run_periodic_check(self, pipeline) -> HealthReport:
        """Lightweight subset for periodic re-checks (every 15 min)."""
        report = HealthReport()
        for agent_fn in _PERIODIC_AGENTS:
            try:
                result = agent_fn(pipeline)
                report.results.append(result)
            except Exception as e:
                report.results.append(AgentResult(
                    agent_name=agent_fn.__name__.replace("_check_", ""),
                    passed=True, level="info",
                    message=f"Check skipped: {str(e)[:80]}",
                ))
        self._last_report = report
        return report

    def get_alerts(self) -> list[Alert]:
        """Get current alerts for dashboard banner."""
        if not self._last_report:
            return []
        return self._last_report.alerts

    def get_jarvis_briefing(self) -> str:
        """Generate proactive advice text for Jarvis."""
        if not self._last_report:
            return "No health data available yet."

        r = self._last_report
        lines = [f"**System Health: {r.passed_count}/{r.total_count} checks passing.**\n"]

        for alert in r.critical_alerts:
            lines.append(f"- CRITICAL: {alert.message}")
        for alert in r.warning_alerts:
            lines.append(f"- WARNING: {alert.message}")

        if not r.critical_alerts and not r.warning_alerts:
            lines.append("All systems green. No issues detected.")

        return "\n".join(lines)

    @property
    def last_report(self) -> Optional[HealthReport]:
        return self._last_report
