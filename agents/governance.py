"""
Multi-Agent Executive Governance System
========================================
6 agents that continuously validate dashboard accuracy, flag inconsistencies,
and produce a defensible confidence score — without touching the UI, database
schema, or deployment.

Architecture:
  Agent 1 — CEO Governance (final arbiter, runs last)
  Agent 2 — Financial Integrity (30 pts)
  Agent 3 — Data Consistency  (25 pts)
  Agent 4 — Tax & Valuation   (15 pts)
  Agent 5 — Self-Testing       (15 pts)
  Agent 6 — Deployment Guardian (15 pts)

All agents read dashboard globals in read-only mode.
"""

import json
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional


def _sanitize(obj):
    """Recursively convert numpy/pandas types to JSON-safe Python natives."""
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, tuple):
        return [_sanitize(v) for v in obj]
    # numpy int types
    try:
        import numpy as np
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.bool_):
            return bool(obj)
    except ImportError:
        pass
    # pandas types
    try:
        import pandas as pd
        if pd.isna(obj):
            return None
    except (ImportError, TypeError, ValueError):
        pass
    return obj


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str               # e.g. "etsy_net_parity"
    status: str             # PASS | FAIL | WARN | SKIP
    message: str            # Human-readable
    severity: str           # CRITICAL | HIGH | MEDIUM | LOW | INFO
    source_variable: str = ""
    source_value: float = 0.0
    expected_value: float = 0.0
    delta: float = 0.0
    tolerance: float = 0.0
    citation: str = ""


@dataclass
class GovernanceMessage:
    agent_name: str
    agent_id: int
    timestamp: str = ""
    checks: list = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    findings: list = field(default_factory=list)

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Metric source registry (anti-hallucination)
# ---------------------------------------------------------------------------

METRIC_SOURCES = {
    "etsy_net_earned": {
        "formula": "gross_sales - total_fees - total_shipping_cost - total_marketing - total_refunds - total_taxes - total_buyer_fees + total_payments",
        "line": 702,
    },
    "gross_sales": {"formula": "sales_df['Net_Clean'].sum()", "line": 382},
    "total_fees": {"formula": "abs(fee_df['Net_Clean'].sum())", "line": 386},
    "total_shipping_cost": {"formula": "abs(ship_df['Net_Clean'].sum())", "line": 387},
    "total_marketing": {"formula": "abs(mkt_df['Net_Clean'].sum())", "line": 388},
    "total_refunds": {"formula": "abs(refund_df['Net_Clean'].sum())", "line": 383},
    "total_taxes": {"formula": "abs(tax_df['Net_Clean'].sum())", "line": 389},
    "total_buyer_fees": {"formula": "abs(buyer_fee_df['Net_Clean'].sum())", "line": 701},
    "total_payments": {"formula": "payment_df['Net_Clean'].sum()", "line": 390},
    "bank_net_cash": {"formula": "bank_total_deposits - bank_total_debits", "line": 671},
    "bank_cash_on_hand": {"formula": "bank_net_cash + etsy_balance", "line": 722},
    "real_profit": {"formula": "bank_cash_on_hand + bank_owner_draw_total", "line": 724},
    "bank_owner_draw_total": {"formula": "sum(draws by category)", "line": 723},
    "tulsa_draw_total": {"formula": "sum(Owner Draw - Tulsa amounts)", "line": 735},
    "texas_draw_total": {"formula": "sum(Owner Draw - Texas amounts)", "line": 736},
    "etsy_balance": {"formula": "max(0, _etsy_balance_auto + _etsy_starting_balance)", "line": 281},
    "etsy_csv_gap": {"formula": "etsy_balance_calculated - etsy_balance", "line": 709},
    "total_inventory_cost": {"formula": "INV_DF['grand_total'].sum()", "line": 636},
    "monthly_net_revenue": {"formula": "per-month (sales - fees - ship - mkt - refunds - tax)", "line": 2025},
    "buyer_paid_shipping": {"formula": "None (reverse-engineering removed)", "line": 926},
    "shipping_profit": {"formula": "None (depends on buyer_paid_shipping)", "line": 927},
    "shipping_margin": {"formula": "None (depends on buyer_paid_shipping)", "line": 928},
    "est_label_cost_paid_orders": {"formula": "None (estimate removed)", "line": 1928},
    "est_label_cost_free_orders": {"formula": "None (estimate removed)", "line": 1930},
    "est_refund_label_cost": {"formula": "None (estimate removed)", "line": 1938},
}


def _cite(var_name, value, extra=""):
    """Build a citation string for anti-hallucination enforcement."""
    src = METRIC_SOURCES.get(var_name, {})
    line = src.get("line", "?")
    formula = src.get("formula", "")
    base = f"var:{var_name}={value}"
    if formula:
        base += f" (L{line}: {formula})"
    if extra:
        base += f" | {extra}"
    return base


# ---------------------------------------------------------------------------
# Helper: safely import a dashboard global
# ---------------------------------------------------------------------------

def _get_dashboard():
    """Import and return a reference to the etsy_dashboard module.
    Returns None if not yet loaded."""
    try:
        import etsy_dashboard as ed
        return ed
    except ImportError:
        return None


def _safe_getattr(mod, name, default=None):
    """Get an attribute from the dashboard module, returning *default* on failure."""
    try:
        return getattr(mod, name, default)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Agent 2: Financial Integrity (30 points)
# ---------------------------------------------------------------------------

class FinancialIntegrityAgent:
    agent_id = 2
    agent_name = "FinancialIntegrityAgent"

    def run(self) -> GovernanceMessage:
        ed = _get_dashboard()
        checks: list[CheckResult] = []
        if ed is None:
            checks.append(CheckResult(
                name="dashboard_import", status="SKIP", message="Dashboard not loaded",
                severity="CRITICAL", citation="import etsy_dashboard failed"))
            return self._wrap(checks)

        checks.append(self._etsy_net_parity(ed))
        checks.append(self._monthly_sum(ed))
        checks.append(self._bank_balance(ed))
        checks.append(self._draw_reconciliation(ed))
        checks.append(self._inventory_consistency(ed))
        checks.append(self._tax_year_completeness(ed))
        checks.append(self._dedup_integrity(ed))
        checks.append(self._reverse_engineering_gone(ed))
        checks.append(self._profit_chain(ed))
        checks.append(self._none_estimates_intact(ed))
        checks.append(self._inferred_revenue_flagged(ed))

        return self._wrap(checks)

    # ── individual checks ──

    def _etsy_net_parity(self, ed):
        net_earned = round(_safe_getattr(ed, "etsy_net_earned", 0), 2)
        data = _safe_getattr(ed, "DATA")
        raw_sum = round(data["Net_Clean"].sum(), 2) if data is not None else 0
        delta = round(abs(net_earned - raw_sum), 2)
        tol = 0.02
        status = "PASS" if delta <= tol else "FAIL"
        return CheckResult(
            name="etsy_net_parity", status=status,
            message=f"etsy_net_earned ({net_earned}) vs DATA.Net_Clean.sum() ({raw_sum}), delta={delta}",
            severity="CRITICAL", source_variable="etsy_net_earned",
            source_value=net_earned, expected_value=raw_sum,
            delta=delta, tolerance=tol,
            citation=_cite("etsy_net_earned", net_earned, f"raw_sum={raw_sum}"))

    def _monthly_sum(self, ed):
        monthly = _safe_getattr(ed, "monthly_net_revenue", {})
        monthly_total = round(sum(monthly.values()), 2)
        net_earned = round(_safe_getattr(ed, "etsy_net_earned", 0), 2)
        delta = round(abs(monthly_total - net_earned), 2)
        tol = 1.00
        status = "PASS" if delta <= tol else "FAIL"
        return CheckResult(
            name="monthly_sum", status=status,
            message=f"sum(monthly_net_revenue)={monthly_total} vs etsy_net_earned={net_earned}, delta={delta}",
            severity="HIGH", source_variable="monthly_net_revenue",
            source_value=monthly_total, expected_value=net_earned,
            delta=delta, tolerance=tol,
            citation=_cite("monthly_net_revenue", monthly_total, f"etsy_net_earned={net_earned}"))

    def _bank_balance(self, ed):
        deposits = round(_safe_getattr(ed, "bank_total_deposits", 0), 2)
        debits = round(_safe_getattr(ed, "bank_total_debits", 0), 2)
        net_cash = round(_safe_getattr(ed, "bank_net_cash", 0), 2)
        expected = round(deposits - debits, 2)
        delta = round(abs(net_cash - expected), 2)
        tol = 0.01
        status = "PASS" if delta <= tol else "FAIL"
        return CheckResult(
            name="bank_balance", status=status,
            message=f"bank_net_cash={net_cash} vs deposits-debits={expected}, delta={delta}",
            severity="CRITICAL", source_variable="bank_net_cash",
            source_value=net_cash, expected_value=expected,
            delta=delta, tolerance=tol,
            citation=_cite("bank_net_cash", net_cash, f"deposits={deposits}, debits={debits}"))

    def _draw_reconciliation(self, ed):
        tulsa = round(_safe_getattr(ed, "tulsa_draw_total", 0), 2)
        texas = round(_safe_getattr(ed, "texas_draw_total", 0), 2)
        total_draws = round(_safe_getattr(ed, "bank_owner_draw_total", 0), 2)
        expected = round(tulsa + texas, 2)
        delta = round(abs(total_draws - expected), 2)
        tol = 0.01
        status = "PASS" if delta <= tol else "FAIL"
        return CheckResult(
            name="draw_reconciliation", status=status,
            message=f"tulsa({tulsa})+texas({texas})={expected} vs bank_owner_draw_total={total_draws}, delta={delta}",
            severity="HIGH", source_variable="bank_owner_draw_total",
            source_value=total_draws, expected_value=expected,
            delta=delta, tolerance=tol,
            citation=_cite("bank_owner_draw_total", total_draws, f"tulsa={tulsa}, texas={texas}"))

    def _inventory_consistency(self, ed):
        inv_df = _safe_getattr(ed, "INV_DF")
        total_cost = round(_safe_getattr(ed, "total_inventory_cost", 0), 2)
        raw_sum = round(inv_df["grand_total"].sum(), 2) if inv_df is not None and len(inv_df) > 0 else 0
        delta = round(abs(total_cost - raw_sum), 2)
        tol = 0.01
        status = "PASS" if delta <= tol else "FAIL"
        return CheckResult(
            name="inventory_consistency", status=status,
            message=f"total_inventory_cost={total_cost} vs INV_DF.sum={raw_sum}, delta={delta}",
            severity="MEDIUM", source_variable="total_inventory_cost",
            source_value=total_cost, expected_value=raw_sum,
            delta=delta, tolerance=tol,
            citation=_cite("total_inventory_cost", total_cost, f"INV_DF.grand_total.sum()={raw_sum}"))

    def _tax_year_completeness(self, ed):
        tax_years = _safe_getattr(ed, "TAX_YEARS", {})
        if not tax_years:
            return CheckResult(
                name="tax_year_completeness", status="SKIP",
                message="TAX_YEARS not computed yet", severity="MEDIUM",
                citation="TAX_YEARS is empty")

        # Sum each year's etsy_net and compare to module etsy_net_earned
        yr_net_sum = round(sum(y.get("etsy_net", 0) for y in tax_years.values()), 2)
        net_earned = round(_safe_getattr(ed, "etsy_net_earned", 0), 2)
        delta = round(abs(yr_net_sum - net_earned), 2)
        tol = 0.02
        status = "PASS" if delta <= tol else "FAIL"
        return CheckResult(
            name="tax_year_completeness", status=status,
            message=f"sum(TAX_YEARS.etsy_net)={yr_net_sum} vs etsy_net_earned={net_earned}, delta={delta}",
            severity="MEDIUM", source_variable="TAX_YEARS",
            source_value=yr_net_sum, expected_value=net_earned,
            delta=delta, tolerance=tol,
            citation=_cite("etsy_net_earned", net_earned, f"yr_net_sum={yr_net_sum}"))

    def _dedup_integrity(self, ed):
        data = _safe_getattr(ed, "DATA")
        if data is None or len(data) == 0:
            return CheckResult(
                name="dedup_integrity", status="SKIP",
                message="DATA not available", severity="MEDIUM",
                citation="DATA is None/empty")

        # Check for exact duplicate rows — must include Info to distinguish
        # legitimate same-day transactions (listing fees, shipping labels, etc.)
        dup_cols = ["Date", "Type", "Title", "Info", "Net"]
        existing_cols = [c for c in dup_cols if c in data.columns]
        if len(existing_cols) < 4:
            return CheckResult(
                name="dedup_integrity", status="SKIP",
                message="Not enough columns for dedup check", severity="LOW",
                citation=f"columns available: {list(data.columns)[:10]}")

        dup_count = data.duplicated(subset=existing_cols, keep=False).sum()
        # Deposits can legitimately duplicate (same title pattern), so only flag non-deposits
        non_deposit = data[data["Type"] != "Deposit"]
        real_dups = non_deposit.duplicated(subset=existing_cols, keep=False).sum()
        status = "PASS" if real_dups == 0 else "WARN"
        return CheckResult(
            name="dedup_integrity", status=status,
            message=f"{real_dups} potential duplicate non-deposit rows (total dups incl deposits: {dup_count})",
            severity="HIGH" if real_dups > 0 else "LOW",
            source_variable="DATA", source_value=real_dups, expected_value=0,
            delta=real_dups, tolerance=0,
            citation=f"var:DATA rows={len(data)}, dup_check on {existing_cols}")

    def _reverse_engineering_gone(self, ed):
        removed_vars = ["buyer_paid_shipping", "shipping_profit", "shipping_margin"]
        failures = []
        for var in removed_vars:
            val = _safe_getattr(ed, var, "MISSING")
            if val is not None and val != "MISSING":
                failures.append(f"{var}={val}")
        status = "PASS" if not failures else "FAIL"
        msg = "All reverse-engineered vars are None" if status == "PASS" else f"Non-None: {', '.join(failures)}"
        return CheckResult(
            name="reverse_engineering_gone", status=status,
            message=msg, severity="HIGH",
            citation=_cite("buyer_paid_shipping", _safe_getattr(ed, "buyer_paid_shipping")))

    def _profit_chain(self, ed):
        cash_on_hand = round(_safe_getattr(ed, "bank_cash_on_hand", 0), 2)
        draws = round(_safe_getattr(ed, "bank_owner_draw_total", 0), 2)
        real_profit = round(_safe_getattr(ed, "real_profit", 0), 2)
        expected = round(cash_on_hand + draws, 2)
        delta = round(abs(real_profit - expected), 2)
        tol = 0.01
        status = "PASS" if delta <= tol else "FAIL"
        return CheckResult(
            name="profit_chain", status=status,
            message=f"real_profit={real_profit} vs cash_on_hand({cash_on_hand})+draws({draws})={expected}, delta={delta}",
            severity="CRITICAL", source_variable="real_profit",
            source_value=real_profit, expected_value=expected,
            delta=delta, tolerance=tol,
            citation=_cite("real_profit", real_profit, f"cash_on_hand={cash_on_hand}, draws={draws}"))

    def _none_estimates_intact(self, ed):
        none_vars = [
            "est_label_cost_paid_orders", "paid_shipping_profit",
            "est_label_cost_free_orders", "est_refund_label_cost",
        ]
        failures = []
        for var in none_vars:
            val = _safe_getattr(ed, var, "MISSING")
            if val is not None and val != "MISSING":
                failures.append(f"{var}={val}")
        status = "PASS" if not failures else "FAIL"
        msg = "All removed estimates are None" if status == "PASS" else f"Non-None: {', '.join(failures)}"
        return CheckResult(
            name="none_estimates_intact", status=status,
            message=msg, severity="MEDIUM",
            citation=_cite("est_label_cost_paid_orders", _safe_getattr(ed, "est_label_cost_paid_orders")))

    def _inferred_revenue_flagged(self, ed):
        """INFO-level check: flag that product revenue is inferred from CSV groupby."""
        data = _safe_getattr(ed, "DATA")
        if data is not None and "Title" in data.columns:
            sales = data[data["Type"] == "Sale"]
            with_title = sales["Title"].notna().sum()
            total = len(sales)
            return CheckResult(
                name="inferred_revenue_flagged", status="PASS",
                message=f"Product revenue sourced from CSV Title column ({with_title}/{total} sales have titles)",
                severity="INFO", source_variable="DATA.Title",
                source_value=with_title, expected_value=total,
                citation=f"var:sales_rows={total}, titled={with_title}")
        return CheckResult(
            name="inferred_revenue_flagged", status="SKIP",
            message="DATA not available for title check", severity="INFO",
            citation="DATA is None")

    def _wrap(self, checks):
        passed = sum(1 for c in checks if c.status == "PASS")
        failed = sum(1 for c in checks if c.status == "FAIL")
        warned = sum(1 for c in checks if c.status == "WARN")
        skipped = sum(1 for c in checks if c.status == "SKIP")
        findings = [f"[{c.status}] {c.name}: {c.message}" for c in checks if c.status in ("FAIL", "WARN")]
        return GovernanceMessage(
            agent_name=self.agent_name, agent_id=self.agent_id,
            checks=[asdict(c) for c in checks],
            summary={"total": len(checks), "passed": passed, "failed": failed,
                     "warned": warned, "skipped": skipped},
            findings=findings)


# ---------------------------------------------------------------------------
# Agent 3: Data Consistency (25 points)
# ---------------------------------------------------------------------------

class DataConsistencyAgent:
    agent_id = 3
    agent_name = "DataConsistencyAgent"

    def run(self) -> GovernanceMessage:
        ed = _get_dashboard()
        checks: list[CheckResult] = []
        if ed is None:
            checks.append(CheckResult(
                name="dashboard_import", status="SKIP", message="Dashboard not loaded",
                severity="CRITICAL", citation="import etsy_dashboard failed"))
            return self._wrap(checks)

        checks.append(self._etsy_row_count(ed))
        checks.append(self._bank_row_count(ed))
        checks.append(self._inventory_row_count(ed))
        checks.append(self._deposit_reconciliation(ed))
        checks.append(self._inventory_vs_bank(ed))
        checks.append(self._missing_months(ed))
        checks.append(self._orphaned_records(ed))
        checks.append(self._etsy_balance_gap(ed))
        checks.append(self._type_coverage(ed))
        checks.append(self._duplicate_bank_check(ed))

        return self._wrap(checks)

    def _etsy_row_count(self, ed):
        data = _safe_getattr(ed, "DATA")
        count = len(data) if data is not None else 0
        status = "PASS" if count > 0 else "FAIL"
        return CheckResult(
            name="etsy_row_count", status=status,
            message=f"Etsy DATA has {count} rows",
            severity="CRITICAL" if count == 0 else "INFO",
            source_variable="DATA", source_value=count,
            citation=f"var:DATA rows={count}")

    def _bank_row_count(self, ed):
        txns = _safe_getattr(ed, "BANK_TXNS", [])
        count = len(txns)
        status = "PASS" if count > 0 else "FAIL"
        return CheckResult(
            name="bank_row_count", status=status,
            message=f"BANK_TXNS has {count} transactions",
            severity="CRITICAL" if count == 0 else "INFO",
            source_variable="BANK_TXNS", source_value=count,
            citation=f"var:BANK_TXNS len={count}")

    def _inventory_row_count(self, ed):
        invoices = _safe_getattr(ed, "INVOICES", [])
        count = len(invoices)
        status = "PASS" if count > 0 else "WARN"
        return CheckResult(
            name="inventory_row_count", status=status,
            message=f"INVOICES has {count} orders",
            severity="MEDIUM" if count == 0 else "INFO",
            source_variable="INVOICES", source_value=count,
            citation=f"var:INVOICES len={count}")

    def _deposit_reconciliation(self, ed):
        """Etsy deposits should approximately match bank Etsy payouts + pre-capone."""
        etsy_deposited = round(_safe_getattr(ed, "etsy_total_deposited", 0), 2)
        bank_deposits = round(_safe_getattr(ed, "bank_total_deposits", 0), 2)
        pre_capone = round(_safe_getattr(ed, "etsy_pre_capone_deposits", 0), 2)
        # They won't match exactly because bank deposits include non-Etsy deposits
        # Just check that etsy_total_deposited >= bank_deposits (Etsy records all payouts)
        delta = round(abs(etsy_deposited - bank_deposits), 2)
        status = "PASS" if etsy_deposited >= bank_deposits * 0.8 else "WARN"
        return CheckResult(
            name="deposit_reconciliation", status=status,
            message=f"etsy_total_deposited={etsy_deposited}, bank_total_deposits={bank_deposits}, pre_capone={pre_capone}",
            severity="MEDIUM", source_variable="etsy_total_deposited",
            source_value=etsy_deposited, expected_value=bank_deposits,
            delta=delta, tolerance=0,
            citation=f"var:etsy_total_deposited={etsy_deposited}, bank_total_deposits={bank_deposits}")

    def _inventory_vs_bank(self, ed):
        """Check that inventory cost and bank Amazon category are in the same ballpark."""
        true_inv = round(_safe_getattr(ed, "true_inventory_cost", 0), 2)
        bank_amazon = round(_safe_getattr(ed, "bank_by_cat", {}).get("Amazon Inventory", 0), 2)
        delta = round(abs(true_inv - bank_amazon), 2)
        # These won't match exactly (different timing, personal orders excluded)
        ratio = true_inv / bank_amazon if bank_amazon > 0 else 0
        status = "PASS" if 0.5 <= ratio <= 2.0 or (true_inv == 0 and bank_amazon == 0) else "WARN"
        return CheckResult(
            name="inventory_vs_bank", status=status,
            message=f"true_inventory_cost={true_inv}, bank Amazon={bank_amazon}, ratio={ratio:.2f}",
            severity="MEDIUM", source_variable="true_inventory_cost",
            source_value=true_inv, expected_value=bank_amazon,
            delta=delta,
            citation=f"var:true_inventory_cost={true_inv}, bank_by_cat['Amazon Inventory']={bank_amazon}")

    def _missing_months(self, ed):
        months = _safe_getattr(ed, "months_sorted", [])
        if len(months) < 2:
            return CheckResult(
                name="missing_months", status="PASS",
                message=f"Only {len(months)} month(s), gap check not applicable",
                severity="INFO", source_variable="months_sorted",
                source_value=len(months),
                citation=f"var:months_sorted len={len(months)}")

        import pandas as pd
        gaps = []
        for i in range(1, len(months)):
            prev = pd.Period(months[i - 1], freq="M")
            curr = pd.Period(months[i], freq="M")
            diff = (curr.year - prev.year) * 12 + (curr.month - prev.month)
            if diff > 1:
                gaps.append(f"{months[i-1]}→{months[i]} ({diff-1} missing)")

        status = "PASS" if not gaps else "WARN"
        return CheckResult(
            name="missing_months", status=status,
            message=f"{'No gaps' if not gaps else 'Gaps: ' + '; '.join(gaps)} in {len(months)} months",
            severity="MEDIUM" if gaps else "INFO",
            source_variable="months_sorted", source_value=len(months),
            citation=f"var:months_sorted={months[:3]}...{months[-1:]}")

    def _orphaned_records(self, ed):
        """Check that sale records contain parseable order numbers.
        Etsy order numbers are in the Title field: 'Payment for Order #XXXXXXXXX'."""
        data = _safe_getattr(ed, "DATA")
        if data is None:
            return CheckResult(
                name="orphaned_records", status="SKIP",
                message="DATA not available", severity="LOW",
                citation="DATA is None")

        import re
        sale_orders = set()
        sales = data[data["Type"] == "Sale"]
        if "Title" in data.columns and len(sales) > 0:
            for title in sales["Title"].dropna():
                m = re.search(r"Order\s*#(\d{9,})", str(title))
                if m:
                    sale_orders.add(m.group(1))

        total_sales = len(sales)
        pct = (len(sale_orders) / total_sales * 100) if total_sales > 0 else 0
        status = "PASS" if pct >= 80 else ("WARN" if pct >= 50 else "FAIL")
        return CheckResult(
            name="orphaned_records", status=status,
            message=f"{len(sale_orders)}/{total_sales} sales have order numbers ({pct:.0f}%)",
            severity="LOW" if status == "PASS" else "MEDIUM",
            source_variable="DATA.Title", source_value=len(sale_orders),
            expected_value=total_sales,
            citation=f"var:sale_order_count={len(sale_orders)}, total_sales={total_sales}")

    def _etsy_balance_gap(self, ed):
        gap = round(abs(_safe_getattr(ed, "etsy_csv_gap", 0)), 2)
        tol = 5.0
        status = "PASS" if gap <= tol else "WARN"
        return CheckResult(
            name="etsy_balance_gap", status=status,
            message=f"etsy_csv_gap={gap}, tolerance={tol}",
            severity="MEDIUM" if gap > tol else "LOW",
            source_variable="etsy_csv_gap", source_value=gap,
            tolerance=tol,
            citation=_cite("etsy_csv_gap", _safe_getattr(ed, "etsy_csv_gap", 0)))

    def _type_coverage(self, ed):
        data = _safe_getattr(ed, "DATA")
        if data is None:
            return CheckResult(
                name="type_coverage", status="SKIP",
                message="DATA not available", severity="LOW",
                citation="DATA is None")

        expected_types = {"Sale", "Fee", "Shipping", "Marketing", "Refund", "Tax", "Deposit", "Buyer Fee", "Payment"}
        actual_types = set(data["Type"].unique()) if "Type" in data.columns else set()
        missing = expected_types - actual_types
        status = "PASS" if not missing else "WARN"
        return CheckResult(
            name="type_coverage", status=status,
            message=f"{'All 9 types present' if not missing else f'Missing types: {missing}'} (found: {len(actual_types)})",
            severity="MEDIUM" if missing else "INFO",
            source_variable="DATA.Type", source_value=len(actual_types),
            expected_value=len(expected_types),
            citation=f"var:actual_types={sorted(actual_types)}")

    def _duplicate_bank_check(self, ed):
        txns = _safe_getattr(ed, "BANK_TXNS", [])
        if not txns:
            return CheckResult(
                name="duplicate_bank_check", status="SKIP",
                message="No bank transactions to check", severity="LOW",
                citation="BANK_TXNS is empty")

        seen = {}
        dups = 0
        for t in txns:
            key = (t.get("date", ""), t.get("amount", 0), t.get("type", ""),
                   t.get("raw_desc", t.get("desc", "")))
            if key in seen:
                dups += 1
            seen[key] = True

        status = "PASS" if dups == 0 else "WARN"
        return CheckResult(
            name="duplicate_bank_check", status=status,
            message=f"{dups} potential duplicate bank transactions out of {len(txns)}",
            severity="HIGH" if dups > 5 else "MEDIUM" if dups > 0 else "INFO",
            source_variable="BANK_TXNS", source_value=dups,
            expected_value=0, delta=dups,
            citation=f"var:BANK_TXNS len={len(txns)}, dups={dups}")

    def _wrap(self, checks):
        passed = sum(1 for c in checks if c.status == "PASS")
        failed = sum(1 for c in checks if c.status == "FAIL")
        warned = sum(1 for c in checks if c.status == "WARN")
        skipped = sum(1 for c in checks if c.status == "SKIP")
        findings = [f"[{c.status}] {c.name}: {c.message}" for c in checks if c.status in ("FAIL", "WARN")]
        return GovernanceMessage(
            agent_name=self.agent_name, agent_id=self.agent_id,
            checks=[asdict(c) for c in checks],
            summary={"total": len(checks), "passed": passed, "failed": failed,
                     "warned": warned, "skipped": skipped},
            findings=findings)


# ---------------------------------------------------------------------------
# Agent 4: Tax & Valuation (15 points)
# ---------------------------------------------------------------------------

class TaxValuationAgent:
    agent_id = 4
    agent_name = "TaxValuationAgent"

    def run(self) -> GovernanceMessage:
        ed = _get_dashboard()
        checks: list[CheckResult] = []
        if ed is None:
            checks.append(CheckResult(
                name="dashboard_import", status="SKIP", message="Dashboard not loaded",
                severity="CRITICAL", citation="import etsy_dashboard failed"))
            return self._wrap(checks)

        checks.append(self._tax_year_splits(ed))
        checks.append(self._tax_year_net_income(ed))
        checks.append(self._income_tax_function(ed))
        checks.append(self._draw_parity_across_years(ed))
        checks.append(self._valuation_data_sufficiency(ed))
        checks.append(self._valuation_confidence_labels(ed))
        checks.append(self._tax_logic_chain(ed))

        return self._wrap(checks)

    def _tax_year_splits(self, ed):
        tax_years = _safe_getattr(ed, "TAX_YEARS", {})
        if not tax_years:
            return CheckResult(
                name="tax_year_splits", status="SKIP",
                message="TAX_YEARS not available", severity="MEDIUM",
                citation="TAX_YEARS is empty")

        # gross_sales split should sum to module gross_sales
        yr_gross_sum = round(sum(y.get("gross_sales", 0) for y in tax_years.values()), 2)
        gross_sales = round(_safe_getattr(ed, "gross_sales", 0), 2)
        delta = round(abs(yr_gross_sum - gross_sales), 2)
        tol = 0.02
        status = "PASS" if delta <= tol else "FAIL"
        return CheckResult(
            name="tax_year_splits", status=status,
            message=f"sum(TAX_YEARS.gross_sales)={yr_gross_sum} vs gross_sales={gross_sales}, delta={delta}",
            severity="HIGH", source_variable="TAX_YEARS",
            source_value=yr_gross_sum, expected_value=gross_sales,
            delta=delta, tolerance=tol,
            citation=f"var:gross_sales={gross_sales}, yr_sum={yr_gross_sum}")

    def _tax_year_net_income(self, ed):
        tax_years = _safe_getattr(ed, "TAX_YEARS", {})
        failures = []
        for yr, d in tax_years.items():
            etsy_net = d.get("etsy_net", 0)
            bank_add = d.get("bank_additional_expense", 0)
            inv_cost = d.get("inventory_cost", 0)
            expected_net = round(etsy_net - bank_add - inv_cost, 2)
            actual_net = round(d.get("net_income", 0), 2)
            if abs(expected_net - actual_net) > 0.02:
                failures.append(f"{yr}: expected={expected_net}, actual={actual_net}")
        status = "PASS" if not failures else "FAIL"
        msg = "All year net_income = etsy_net - bank_additional - inventory" if not failures else "; ".join(failures)
        return CheckResult(
            name="tax_year_net_income", status=status,
            message=msg, severity="HIGH",
            citation=f"TAX_YEARS net_income formula check, failures={len(failures)}")

    def _income_tax_function(self, ed):
        compute_tax = _safe_getattr(ed, "_compute_income_tax")
        if compute_tax is None:
            return CheckResult(
                name="income_tax_function", status="SKIP",
                message="_compute_income_tax function not accessible", severity="MEDIUM",
                citation="_compute_income_tax not found on module")

        failures = []
        # Zero income = zero tax
        if compute_tax(0) != 0:
            failures.append(f"tax(0)={compute_tax(0)}, expected 0")
        # Negative income = zero tax
        if compute_tax(-1000) != 0:
            failures.append(f"tax(-1000)={compute_tax(-1000)}, expected 0")
        # Known bracket: $10,000 at 10% = $1,000
        t10k = compute_tax(10000)
        if abs(t10k - 1000.0) > 1.0:
            failures.append(f"tax(10000)={t10k}, expected ~1000")
        # Must be monotonically increasing
        if compute_tax(50000) <= compute_tax(10000):
            failures.append("tax(50000) should be > tax(10000)")

        status = "PASS" if not failures else "FAIL"
        msg = "Income tax function validates correctly" if not failures else "; ".join(failures)
        return CheckResult(
            name="income_tax_function", status=status,
            message=msg, severity="MEDIUM",
            citation=f"_compute_income_tax tested at 0, -1000, 10000, 50000")

    def _draw_parity_across_years(self, ed):
        tax_years = _safe_getattr(ed, "TAX_YEARS", {})
        yr_draws_sum = round(sum(d.get("total_draws", 0) for d in tax_years.values()), 2)
        module_draws = round(_safe_getattr(ed, "bank_owner_draw_total", 0), 2)
        delta = round(abs(yr_draws_sum - module_draws), 2)
        tol = 0.01
        status = "PASS" if delta <= tol else "FAIL"
        return CheckResult(
            name="draw_parity_across_years", status=status,
            message=f"sum(TAX_YEARS.total_draws)={yr_draws_sum} vs bank_owner_draw_total={module_draws}, delta={delta}",
            severity="HIGH", source_variable="bank_owner_draw_total",
            source_value=yr_draws_sum, expected_value=module_draws,
            delta=delta, tolerance=tol,
            citation=_cite("bank_owner_draw_total", module_draws, f"yr_sum={yr_draws_sum}"))

    def _valuation_data_sufficiency(self, ed):
        months = _safe_getattr(ed, "months_sorted", [])
        count = len(months)
        status = "PASS" if count >= 6 else "WARN"
        label = "SUFFICIENT" if count >= 6 else "SPECULATIVE"
        return CheckResult(
            name="valuation_data_sufficiency", status=status,
            message=f"{count} months of data — valuation is {label} (need 6+ for confidence)",
            severity="MEDIUM" if count < 6 else "INFO",
            source_variable="months_sorted", source_value=count,
            expected_value=6,
            citation=f"var:months_sorted len={count}")

    def _valuation_confidence_labels(self, ed):
        """Classify valuation metrics by confidence level."""
        classifications = []
        # Revenue multiples: HIGH confidence (derived from actual sales)
        val_annual = _safe_getattr(ed, "val_annual_revenue", 0)
        months = len(_safe_getattr(ed, "months_sorted", []))
        if months >= 12:
            classifications.append(("val_annual_revenue", "HIGH"))
        elif months >= 6:
            classifications.append(("val_annual_revenue", "MEDIUM"))
        else:
            classifications.append(("val_annual_revenue", "LOW"))

        # SDE: MEDIUM (includes owner draws which are real)
        sde = _safe_getattr(ed, "val_annual_sde", 0)
        classifications.append(("val_annual_sde", "MEDIUM" if sde > 0 else "LOW"))

        # Health score: MEDIUM (composite of many factors)
        classifications.append(("val_health_score", "MEDIUM"))

        low_count = sum(1 for _, c in classifications if c == "LOW")
        status = "PASS" if low_count == 0 else "WARN"
        return CheckResult(
            name="valuation_confidence_labels", status=status,
            message=f"Confidence: {', '.join(f'{n}={c}' for n, c in classifications)}",
            severity="MEDIUM" if low_count > 0 else "INFO",
            source_variable="valuation_metrics",
            citation=f"val_annual_revenue={val_annual}, months={months}")

    def _tax_logic_chain(self, ed):
        """Document the provenance chain for tax metrics — always PASS (info only)."""
        chain = [
            "gross_sales → sales_df.Net_Clean.sum() (L382)",
            "etsy_net → gross - fees - ship - mkt - refunds - tax - buyer_fees + payments (L702)",
            "net_income → etsy_net - bank_additional_expense - inventory_cost (L3450)",
            "income_tax → _compute_income_tax(net_income) progressive brackets (L3601)",
        ]
        return CheckResult(
            name="tax_logic_chain", status="PASS",
            message=f"Tax provenance: {len(chain)} steps documented",
            severity="INFO", citation=" → ".join(chain))

    def _wrap(self, checks):
        passed = sum(1 for c in checks if c.status == "PASS")
        failed = sum(1 for c in checks if c.status == "FAIL")
        warned = sum(1 for c in checks if c.status == "WARN")
        skipped = sum(1 for c in checks if c.status == "SKIP")
        findings = [f"[{c.status}] {c.name}: {c.message}" for c in checks if c.status in ("FAIL", "WARN")]
        return GovernanceMessage(
            agent_name=self.agent_name, agent_id=self.agent_id,
            checks=[asdict(c) for c in checks],
            summary={"total": len(checks), "passed": passed, "failed": failed,
                     "warned": warned, "skipped": skipped},
            findings=findings)


# ---------------------------------------------------------------------------
# Agent 5: Self-Testing (15 points)
# ---------------------------------------------------------------------------

class SelfTestingAgent:
    agent_id = 5
    agent_name = "SelfTestingAgent"

    def run(self) -> GovernanceMessage:
        ed = _get_dashboard()
        checks: list[CheckResult] = []
        if ed is None:
            checks.append(CheckResult(
                name="dashboard_import", status="SKIP", message="Dashboard not loaded",
                severity="CRITICAL", citation="import etsy_dashboard failed"))
            return self._wrap(checks)

        checks.append(self._money_function(ed))
        checks.append(self._parse_money_function(ed))
        checks.append(self._none_propagation(ed))
        checks.append(self._negative_guards(ed))
        checks.append(self._zero_division_guards(ed))
        checks.append(self._reconciliation_structure(ed))
        checks.append(self._empty_df_safety(ed))
        checks.append(self._config_key_types(ed))

        return self._wrap(checks)

    def _money_function(self, ed):
        money = _safe_getattr(ed, "money")
        if money is None:
            return CheckResult(
                name="money_function", status="SKIP",
                message="money() function not accessible", severity="MEDIUM",
                citation="money not found on module")

        failures = []
        if money(None) != "UNKNOWN":
            failures.append(f"money(None)='{money(None)}', expected 'UNKNOWN'")
        if money(0) != "$0.00":
            failures.append(f"money(0)='{money(0)}', expected '$0.00'")
        if money(-1.5) != "-$1.50":
            failures.append(f"money(-1.5)='{money(-1.5)}', expected '-$1.50'")
        if money(1234567.89) != "$1,234,567.89":
            failures.append(f"money(1234567.89)='{money(1234567.89)}', expected '$1,234,567.89'")

        status = "PASS" if not failures else "FAIL"
        msg = "money() handles all edge cases" if not failures else "; ".join(failures)
        return CheckResult(
            name="money_function", status=status,
            message=msg, severity="MEDIUM",
            citation=f"money() tested with None, 0, -1.5, 1234567.89")

    def _parse_money_function(self, ed):
        pm = _safe_getattr(ed, "parse_money")
        if pm is None:
            return CheckResult(
                name="parse_money_function", status="SKIP",
                message="parse_money() not accessible", severity="MEDIUM",
                citation="parse_money not found on module")

        import math
        failures = []
        if pm("$1,234.56") != 1234.56:
            failures.append(f"parse_money('$1,234.56')={pm('$1,234.56')}")
        if pm("--") != 0.0:
            failures.append(f"parse_money('--')={pm('--')}")
        if pm("") != 0.0:
            failures.append(f"parse_money('')={pm('')}")
        nan_result = pm(float("nan"))
        if nan_result != 0.0:
            failures.append(f"parse_money(NaN)={nan_result}")

        status = "PASS" if not failures else "FAIL"
        msg = "parse_money() handles all edge cases" if not failures else "; ".join(failures)
        return CheckResult(
            name="parse_money_function", status=status,
            message=msg, severity="MEDIUM",
            citation=f"parse_money() tested with '$1,234.56', '--', '', NaN")

    def _none_propagation(self, ed):
        money = _safe_getattr(ed, "money")
        if money is None:
            return CheckResult(
                name="none_propagation", status="SKIP",
                message="money() not accessible", severity="MEDIUM",
                citation="money not found")

        none_metrics = [
            "buyer_paid_shipping", "shipping_profit", "shipping_margin",
            "est_label_cost_paid_orders", "est_label_cost_free_orders",
            "est_refund_label_cost",
        ]
        failures = []
        for var in none_metrics:
            val = _safe_getattr(ed, var, "MISSING")
            if val is None:
                result = money(val)
                if result != "UNKNOWN":
                    failures.append(f"money({var}=None)='{result}'")

        status = "PASS" if not failures else "FAIL"
        msg = "All None metrics pass through money() safely" if not failures else "; ".join(failures)
        return CheckResult(
            name="none_propagation", status=status,
            message=msg, severity="MEDIUM",
            citation=f"Tested {len(none_metrics)} None metrics through money()")

    def _negative_guards(self, ed):
        checks_list = [
            ("total_refunds", _safe_getattr(ed, "total_refunds", 0)),
            ("total_fees", _safe_getattr(ed, "total_fees", 0)),
            ("etsy_balance", _safe_getattr(ed, "etsy_balance", 0)),
        ]
        failures = []
        for name, val in checks_list:
            if val is not None and val < 0:
                failures.append(f"{name}={val} (negative)")

        status = "PASS" if not failures else "WARN"
        msg = "All guard values are non-negative" if not failures else "; ".join(failures)
        return CheckResult(
            name="negative_guards", status=status,
            message=msg, severity="MEDIUM" if failures else "LOW",
            citation=f"Checked: {', '.join(n for n, _ in checks_list)}")

    def _zero_division_guards(self, ed):
        failures = []
        # avg_order should handle 0 orders
        order_count = _safe_getattr(ed, "order_count", 0)
        avg_order = _safe_getattr(ed, "avg_order", 0)
        if order_count == 0 and avg_order != 0:
            failures.append(f"avg_order={avg_order} with 0 orders")

        # margins should handle 0 gross_sales
        gross = _safe_getattr(ed, "gross_sales", 0)
        margin = _safe_getattr(ed, "etsy_net_margin", 0)
        if gross == 0 and margin != 0:
            failures.append(f"etsy_net_margin={margin} with 0 gross_sales")

        status = "PASS" if not failures else "FAIL"
        msg = "Zero-division guards intact" if not failures else "; ".join(failures)
        return CheckResult(
            name="zero_division_guards", status=status,
            message=msg, severity="MEDIUM" if failures else "LOW",
            citation=f"order_count={order_count}, gross_sales={gross}")

    def _reconciliation_structure(self, ed):
        build_recon = _safe_getattr(ed, "_build_reconciliation_report")
        if build_recon is None:
            return CheckResult(
                name="reconciliation_structure", status="SKIP",
                message="_build_reconciliation_report not accessible", severity="LOW",
                citation="_build_reconciliation_report not found")

        try:
            rows = build_recon()
            if not isinstance(rows, list):
                return CheckResult(
                    name="reconciliation_structure", status="FAIL",
                    message=f"Expected list, got {type(rows).__name__}", severity="MEDIUM",
                    citation=f"type={type(rows).__name__}")
            if len(rows) == 0:
                return CheckResult(
                    name="reconciliation_structure", status="WARN",
                    message="Reconciliation report returned 0 rows", severity="MEDIUM",
                    citation="rows=0")
            # Check structure of first row
            first = rows[0]
            expected_keys = {"metric", "dashboard", "raw_sum", "delta", "status"}
            actual_keys = set(first.keys())
            missing = expected_keys - actual_keys
            status = "PASS" if not missing else "FAIL"
            return CheckResult(
                name="reconciliation_structure", status=status,
                message=f"Reconciliation report has {len(rows)} rows, keys={'valid' if not missing else f'missing {missing}'}",
                severity="MEDIUM" if missing else "LOW",
                citation=f"rows={len(rows)}, keys={sorted(actual_keys)}")
        except Exception as e:
            return CheckResult(
                name="reconciliation_structure", status="FAIL",
                message=f"Reconciliation report raised: {e}", severity="MEDIUM",
                citation=f"exception: {type(e).__name__}: {e}")

    def _empty_df_safety(self, ed):
        """Check that empty DataFrames produce 0 aggregates, not NaN."""
        import pandas as pd
        import numpy as np

        empty_df = pd.DataFrame(columns=["Net_Clean", "grand_total"])
        failures = []

        net_sum = empty_df["Net_Clean"].sum()
        if not (net_sum == 0 or (isinstance(net_sum, float) and net_sum == 0.0)):
            failures.append(f"empty Net_Clean.sum()={net_sum}")

        gt_sum = empty_df["grand_total"].sum()
        if not (gt_sum == 0 or (isinstance(gt_sum, float) and gt_sum == 0.0)):
            failures.append(f"empty grand_total.sum()={gt_sum}")

        status = "PASS" if not failures else "FAIL"
        msg = "Empty DataFrames produce 0 aggregates" if not failures else "; ".join(failures)
        return CheckResult(
            name="empty_df_safety", status=status,
            message=msg, severity="LOW",
            citation="Tested pd.DataFrame(columns=[...]).sum()")

    def _config_key_types(self, ed):
        config = _safe_getattr(ed, "CONFIG", {})
        if not config:
            return CheckResult(
                name="config_key_types", status="WARN",
                message="CONFIG is empty", severity="MEDIUM",
                citation="CONFIG is empty dict")

        failures = []
        # etsy_pre_capone_deposits should be numeric
        pcd = config.get("etsy_pre_capone_deposits")
        if pcd is not None and not isinstance(pcd, (int, float)):
            failures.append(f"etsy_pre_capone_deposits type={type(pcd).__name__}")

        # etsy_starting_balance should be numeric
        esb = config.get("etsy_starting_balance")
        if esb is not None and not isinstance(esb, (int, float, str)):
            failures.append(f"etsy_starting_balance type={type(esb).__name__}")

        # listing_aliases should be dict if present
        la = config.get("listing_aliases")
        if la is not None and not isinstance(la, dict):
            failures.append(f"listing_aliases type={type(la).__name__}")

        status = "PASS" if not failures else "WARN"
        msg = f"CONFIG has {len(config)} keys, types valid" if not failures else "; ".join(failures)
        return CheckResult(
            name="config_key_types", status=status,
            message=msg, severity="LOW" if not failures else "MEDIUM",
            citation=f"CONFIG keys checked: {len(config)}")

    def _wrap(self, checks):
        passed = sum(1 for c in checks if c.status == "PASS")
        failed = sum(1 for c in checks if c.status == "FAIL")
        warned = sum(1 for c in checks if c.status == "WARN")
        skipped = sum(1 for c in checks if c.status == "SKIP")
        findings = [f"[{c.status}] {c.name}: {c.message}" for c in checks if c.status in ("FAIL", "WARN")]
        return GovernanceMessage(
            agent_name=self.agent_name, agent_id=self.agent_id,
            checks=[asdict(c) for c in checks],
            summary={"total": len(checks), "passed": passed, "failed": failed,
                     "warned": warned, "skipped": skipped},
            findings=findings)


# ---------------------------------------------------------------------------
# Agent 6: Deployment Guardian (15 points)
# ---------------------------------------------------------------------------

class DeploymentGuardianAgent:
    agent_id = 6
    agent_name = "DeploymentGuardianAgent"

    # Canonical table list for Supabase
    EXPECTED_TABLES = [
        "etsy_transactions", "bank_transactions", "config",
        "inventory_orders", "inventory_items",
        "inventory_location_overrides", "inventory_item_details",
        "inventory_usage", "inventory_quick_add",
    ]

    def run(self) -> GovernanceMessage:
        checks: list[CheckResult] = []

        checks.append(self._supabase_connected())
        checks.append(self._tables_exist())
        checks.append(self._env_vars_present())
        checks.append(self._config_keys_valid())
        checks.append(self._schema_snapshot())
        checks.append(self._mutation_log_audit())

        return self._wrap(checks)

    def _supabase_connected(self):
        try:
            from supabase_loader import _get_supabase_client
            client = _get_supabase_client()
            status = "PASS" if client is not None else "FAIL"
            return CheckResult(
                name="supabase_connected", status=status,
                message=f"Supabase client {'connected' if client else 'unavailable'}",
                severity="CRITICAL" if client is None else "INFO",
                citation="supabase_loader._get_supabase_client()")
        except Exception as e:
            return CheckResult(
                name="supabase_connected", status="FAIL",
                message=f"Supabase import failed: {e}", severity="CRITICAL",
                citation=f"exception: {e}")

    def _tables_exist(self):
        try:
            from supabase_loader import _get_supabase_client
            client = _get_supabase_client()
            if client is None:
                return CheckResult(
                    name="tables_exist", status="SKIP",
                    message="No Supabase client", severity="HIGH",
                    citation="client is None")

            accessible = []
            missing = []
            for table in self.EXPECTED_TABLES:
                try:
                    # Use a thread with timeout to avoid hanging on slow Supabase queries
                    result = [None]
                    def _check(t=table):
                        try:
                            client.table(t).select("*").limit(0).execute()
                            result[0] = True
                        except Exception:
                            result[0] = False
                    th = threading.Thread(target=_check, daemon=True)
                    th.start()
                    th.join(timeout=5)
                    if result[0] is True:
                        accessible.append(table)
                    else:
                        missing.append(table)
                except Exception:
                    missing.append(table)

            status = "PASS" if not missing else "FAIL"
            return CheckResult(
                name="tables_exist", status=status,
                message=f"{len(accessible)}/{len(self.EXPECTED_TABLES)} tables accessible"
                        + (f", missing: {missing}" if missing else ""),
                severity="HIGH" if missing else "INFO",
                source_value=len(accessible), expected_value=len(self.EXPECTED_TABLES),
                citation=f"tables checked: {self.EXPECTED_TABLES}")
        except Exception as e:
            return CheckResult(
                name="tables_exist", status="FAIL",
                message=f"Table check error: {e}", severity="HIGH",
                citation=f"exception: {e}")

    def _env_vars_present(self):
        import os
        required = ["SUPABASE_URL", "SUPABASE_KEY"]
        # Also check for fallback defaults in supabase_loader
        present = []
        missing_env = []
        for var in required:
            if os.environ.get(var):
                present.append(var)
            else:
                missing_env.append(var)

        # If env vars missing but hardcoded defaults exist, that's OK
        if missing_env:
            try:
                from supabase_loader import _SUPABASE_URL_DEFAULT, _SUPABASE_KEY_DEFAULT
                if _SUPABASE_URL_DEFAULT and _SUPABASE_KEY_DEFAULT:
                    status = "WARN"
                    msg = f"Env vars {missing_env} missing but hardcoded defaults exist"
                else:
                    status = "FAIL"
                    msg = f"Missing env vars: {missing_env}"
            except ImportError:
                status = "FAIL"
                msg = f"Missing env vars: {missing_env}"
        else:
            status = "PASS"
            msg = f"All {len(required)} env vars present"

        return CheckResult(
            name="env_vars_present", status=status,
            message=msg, severity="HIGH" if status == "FAIL" else "LOW",
            source_value=len(present), expected_value=len(required),
            citation=f"env vars checked: {required} (values NOT logged)")

    def _config_keys_valid(self):
        ed = _get_dashboard()
        config = _safe_getattr(ed, "CONFIG", {}) if ed else {}
        expected_keys = [
            "etsy_pre_capone_deposits", "etsy_starting_balance",
            "draw_reasons", "manual_transactions",
        ]
        present = [k for k in expected_keys if k in config]
        missing = [k for k in expected_keys if k not in config]
        status = "PASS" if not missing else "WARN"
        return CheckResult(
            name="config_keys_valid", status=status,
            message=f"{len(present)}/{len(expected_keys)} expected config keys present"
                    + (f", missing: {missing}" if missing else ""),
            severity="MEDIUM" if missing else "INFO",
            source_value=len(present), expected_value=len(expected_keys),
            citation=f"CONFIG has {len(config)} total keys, checked {expected_keys}")

    def _schema_snapshot(self):
        """Check or create a schema baseline from current table columns."""
        try:
            from supabase_loader import _get_supabase_client, get_config_value, save_config_value
            client = _get_supabase_client()
            if client is None:
                return CheckResult(
                    name="schema_snapshot", status="SKIP",
                    message="No Supabase client for schema check", severity="LOW",
                    citation="client is None")

            # Try to get a single row from each table to discover columns
            current_schema = {}
            for table in self.EXPECTED_TABLES:
                try:
                    result = [None]
                    def _fetch(t=table):
                        try:
                            resp = client.table(t).select("*").limit(1).execute()
                            result[0] = sorted(resp.data[0].keys()) if resp.data else []
                        except Exception:
                            result[0] = None
                    th = threading.Thread(target=_fetch, daemon=True)
                    th.start()
                    th.join(timeout=5)
                    current_schema[table] = result[0]
                except Exception:
                    current_schema[table] = None

            # Compare to saved baseline
            saved = get_config_value("governance_schema_snapshot")
            if saved is None:
                # First run: save baseline
                save_config_value("governance_schema_snapshot", current_schema)
                return CheckResult(
                    name="schema_snapshot", status="PASS",
                    message=f"Schema baseline created for {len(current_schema)} tables",
                    severity="INFO",
                    citation=f"baseline saved to config.governance_schema_snapshot")

            # Compare
            diffs = []
            for table, cols in current_schema.items():
                if cols is None:
                    continue
                saved_cols = saved.get(table)
                if saved_cols is not None and sorted(cols) != sorted(saved_cols):
                    added = set(cols) - set(saved_cols)
                    removed = set(saved_cols) - set(cols)
                    diffs.append(f"{table}: +{added or '{}'} -{removed or '{}'}")

            if diffs:
                # Update baseline
                save_config_value("governance_schema_snapshot", current_schema)

            status = "PASS" if not diffs else "WARN"
            return CheckResult(
                name="schema_snapshot", status=status,
                message=f"Schema {'unchanged' if not diffs else 'changed: ' + '; '.join(diffs)}",
                severity="MEDIUM" if diffs else "INFO",
                citation=f"compared {len(current_schema)} tables against baseline")
        except Exception as e:
            return CheckResult(
                name="schema_snapshot", status="SKIP",
                message=f"Schema check error: {e}", severity="LOW",
                citation=f"exception: {e}")

    def _mutation_log_audit(self):
        """Report config writes since last governance run."""
        try:
            from supabase_loader import get_config_value
            log = get_config_value("governance_mutation_log", [])
            if not isinstance(log, list):
                log = []
            count = len(log)
            last_5 = log[-5:] if log else []
            return CheckResult(
                name="mutation_log_audit", status="PASS",
                message=f"{count} config mutations logged" + (f", recent: {[e.get('key','?') for e in last_5]}" if last_5 else ""),
                severity="INFO",
                source_value=count,
                citation=f"governance_mutation_log has {count} entries")
        except Exception as e:
            return CheckResult(
                name="mutation_log_audit", status="SKIP",
                message=f"Mutation log check error: {e}", severity="LOW",
                citation=f"exception: {e}")

    def _wrap(self, checks):
        passed = sum(1 for c in checks if c.status == "PASS")
        failed = sum(1 for c in checks if c.status == "FAIL")
        warned = sum(1 for c in checks if c.status == "WARN")
        skipped = sum(1 for c in checks if c.status == "SKIP")
        findings = [f"[{c.status}] {c.name}: {c.message}" for c in checks if c.status in ("FAIL", "WARN")]
        return GovernanceMessage(
            agent_name=self.agent_name, agent_id=self.agent_id,
            checks=[asdict(c) for c in checks],
            summary={"total": len(checks), "passed": passed, "failed": failed,
                     "warned": warned, "skipped": skipped},
            findings=findings)


# ---------------------------------------------------------------------------
# Agent 7: Silent Error Sentinel (10 points)
# ---------------------------------------------------------------------------

class SilentErrorSentinelAgent:
    """Detects data quality issues that silent exception handlers may have hidden."""
    agent_id = 7
    agent_name = "SilentErrorSentinelAgent"

    def run(self) -> GovernanceMessage:
        ed = _get_dashboard()
        checks: list[CheckResult] = []
        if ed is None:
            checks.append(CheckResult(
                name="dashboard_import", status="SKIP", message="Dashboard not loaded",
                severity="CRITICAL", citation="import etsy_dashboard failed"))
            return self._wrap(checks)

        checks.append(self._nan_in_financials(ed))
        checks.append(self._null_dates(ed))
        checks.append(self._bank_txn_integrity(ed))
        checks.append(self._invoice_field_completeness(ed))
        checks.append(self._config_type_safety(ed))
        checks.append(self._data_loading_coverage(ed))
        checks.append(self._amount_parse_quality(ed))

        return self._wrap(checks)

    def _nan_in_financials(self, ed):
        """Check that critical financial totals are never NaN or infinite."""
        import math
        critical_vars = [
            "gross_sales", "total_fees", "total_refunds", "total_shipping_cost",
            "total_marketing", "total_taxes", "etsy_net_earned", "bank_net_cash",
            "real_profit", "bank_total_deposits", "bank_total_debits",
            "total_inventory_cost", "bank_owner_draw_total",
        ]
        nan_vars = []
        inf_vars = []
        for var in critical_vars:
            val = _safe_getattr(ed, var, "MISSING")
            if val == "MISSING":
                continue
            if val is None:
                nan_vars.append(f"{var}=None")
            elif isinstance(val, float):
                if math.isnan(val):
                    nan_vars.append(f"{var}=NaN")
                elif math.isinf(val):
                    inf_vars.append(f"{var}=Inf")

        failures = nan_vars + inf_vars
        status = "PASS" if not failures else "FAIL"
        return CheckResult(
            name="nan_in_financials", status=status,
            message=f"{'All {0} financial vars are valid numbers'.format(len(critical_vars))}" if not failures
                    else f"Bad values: {', '.join(failures)}",
            severity="CRITICAL" if failures else "INFO",
            source_value=len(failures), expected_value=0,
            citation=f"checked {len(critical_vars)} financial globals for NaN/Inf")

    def _null_dates(self, ed):
        """Check for NaT dates in DATA that could cause silent grouping errors."""
        import pandas as pd
        data = _safe_getattr(ed, "DATA")
        if data is None or "Date_Parsed" not in data.columns:
            return CheckResult(
                name="null_dates", status="SKIP",
                message="DATA.Date_Parsed not available", severity="LOW",
                citation="DATA is None or missing Date_Parsed")

        nat_count = data["Date_Parsed"].isna().sum()
        total = len(data)
        pct = (nat_count / total * 100) if total > 0 else 0
        status = "PASS" if nat_count == 0 else ("WARN" if pct < 5 else "FAIL")
        return CheckResult(
            name="null_dates", status=status,
            message=f"{nat_count}/{total} rows have null dates ({pct:.1f}%)",
            severity="HIGH" if pct >= 5 else ("MEDIUM" if nat_count > 0 else "INFO"),
            source_variable="DATA.Date_Parsed", source_value=nat_count,
            expected_value=0, delta=nat_count,
            citation=f"var:DATA rows={total}, NaT dates={nat_count}")

    def _bank_txn_integrity(self, ed):
        """Verify every bank transaction has required fields and valid amounts."""
        txns = _safe_getattr(ed, "BANK_TXNS", [])
        if not txns:
            return CheckResult(
                name="bank_txn_integrity", status="SKIP",
                message="No bank transactions", severity="MEDIUM",
                citation="BANK_TXNS is empty")

        issues = []
        for i, t in enumerate(txns):
            if not t.get("date"):
                issues.append(f"txn[{i}] missing date")
            if not t.get("type"):
                issues.append(f"txn[{i}] missing type")
            amt = t.get("amount")
            if amt is None or (isinstance(amt, float) and (amt != amt)):  # NaN check
                issues.append(f"txn[{i}] bad amount={amt}")
            if not t.get("category"):
                issues.append(f"txn[{i}] missing category")

        status = "PASS" if not issues else ("WARN" if len(issues) < 5 else "FAIL")
        return CheckResult(
            name="bank_txn_integrity", status=status,
            message=f"{len(issues)} field issues in {len(txns)} bank transactions"
                    + (f": {issues[:3]}" if issues else ""),
            severity="HIGH" if len(issues) >= 5 else ("MEDIUM" if issues else "INFO"),
            source_variable="BANK_TXNS", source_value=len(issues),
            expected_value=0, delta=len(issues),
            citation=f"var:BANK_TXNS len={len(txns)}, field_issues={len(issues)}")

    def _invoice_field_completeness(self, ed):
        """Check that invoices have all required fields populated."""
        invoices = _safe_getattr(ed, "INVOICES", [])
        if not invoices:
            return CheckResult(
                name="invoice_field_completeness", status="SKIP",
                message="No invoices to check", severity="LOW",
                citation="INVOICES is empty")

        required = ["order_num", "date", "grand_total", "subtotal", "source"]
        incomplete = 0
        for inv in invoices:
            for field in required:
                if not inv.get(field) and inv.get(field) != 0:
                    incomplete += 1
                    break

        pct = (1 - incomplete / len(invoices)) * 100 if invoices else 0
        status = "PASS" if incomplete == 0 else ("WARN" if pct >= 90 else "FAIL")
        return CheckResult(
            name="invoice_field_completeness", status=status,
            message=f"{len(invoices) - incomplete}/{len(invoices)} invoices have all required fields ({pct:.0f}%)",
            severity="MEDIUM" if incomplete > 0 else "INFO",
            source_variable="INVOICES", source_value=incomplete,
            expected_value=0,
            citation=f"var:INVOICES len={len(invoices)}, checked fields={required}")

    def _config_type_safety(self, ed):
        """Check that numeric config values are actually numeric, not strings."""
        config = _safe_getattr(ed, "CONFIG", {})
        numeric_keys = ["etsy_pre_capone_deposits", "etsy_starting_balance"]
        type_errors = []
        for key in numeric_keys:
            val = config.get(key)
            if val is not None and isinstance(val, str):
                try:
                    float(val)
                    type_errors.append(f"{key} is string '{val}' (works but risky)")
                except ValueError:
                    type_errors.append(f"{key} is non-numeric string '{val}'")

        status = "PASS" if not type_errors else "WARN"
        return CheckResult(
            name="config_type_safety", status=status,
            message=f"{'Config numeric types OK' if not type_errors else '; '.join(type_errors)}",
            severity="MEDIUM" if type_errors else "INFO",
            citation=f"checked {numeric_keys} for type safety")

    def _data_loading_coverage(self, ed):
        """Verify that all three data sources loaded with non-zero data."""
        data = _safe_getattr(ed, "DATA")
        txns = _safe_getattr(ed, "BANK_TXNS", [])
        invoices = _safe_getattr(ed, "INVOICES", [])

        sources = {
            "Etsy (DATA)": len(data) if data is not None else 0,
            "Bank (BANK_TXNS)": len(txns),
            "Inventory (INVOICES)": len(invoices),
        }
        empty = [name for name, count in sources.items() if count == 0]
        status = "PASS" if not empty else "FAIL"
        return CheckResult(
            name="data_loading_coverage", status=status,
            message=f"{'All 3 sources loaded' if not empty else f'Empty sources: {empty}'}: "
                    + ", ".join(f"{n}={c}" for n, c in sources.items()),
            severity="CRITICAL" if empty else "INFO",
            citation=f"sources: {sources}")

    def _amount_parse_quality(self, ed):
        """Check for $0.00 amounts that might indicate silent parse failures."""
        data = _safe_getattr(ed, "DATA")
        if data is None or "Net_Clean" not in data.columns:
            return CheckResult(
                name="amount_parse_quality", status="SKIP",
                message="DATA.Net_Clean not available", severity="LOW",
                citation="DATA is None")

        # Sales and Fees should rarely have exactly $0.00
        sales = data[data["Type"] == "Sale"]
        zero_sales = (sales["Net_Clean"] == 0).sum() if len(sales) > 0 else 0
        fees = data[data["Type"] == "Fee"]
        zero_fees = (fees["Net_Clean"] == 0).sum() if len(fees) > 0 else 0

        pct_zero_sales = (zero_sales / len(sales) * 100) if len(sales) > 0 else 0
        status = "PASS" if pct_zero_sales < 5 else "WARN"
        return CheckResult(
            name="amount_parse_quality", status=status,
            message=f"{zero_sales} zero-amount sales ({pct_zero_sales:.1f}%), {zero_fees} zero-amount fees",
            severity="MEDIUM" if pct_zero_sales >= 5 else "INFO",
            source_variable="DATA.Net_Clean", source_value=zero_sales,
            citation=f"var:zero_sales={zero_sales}/{len(sales)}, zero_fees={zero_fees}/{len(fees)}")

    def _wrap(self, checks):
        passed = sum(1 for c in checks if c.status == "PASS")
        failed = sum(1 for c in checks if c.status == "FAIL")
        warned = sum(1 for c in checks if c.status == "WARN")
        skipped = sum(1 for c in checks if c.status == "SKIP")
        findings = [f"[{c.status}] {c.name}: {c.message}" for c in checks if c.status in ("FAIL", "WARN")]
        return GovernanceMessage(
            agent_name=self.agent_name, agent_id=self.agent_id,
            checks=[asdict(c) for c in checks],
            summary={"total": len(checks), "passed": passed, "failed": failed,
                     "warned": warned, "skipped": skipped},
            findings=findings)


# ---------------------------------------------------------------------------
# Agent 8: Trend Anomaly Detector (10 points)
# ---------------------------------------------------------------------------

class TrendAnomalyAgent:
    """Detects statistical anomalies in monthly financial data."""
    agent_id = 8
    agent_name = "TrendAnomalyAgent"

    def run(self) -> GovernanceMessage:
        ed = _get_dashboard()
        checks: list[CheckResult] = []
        if ed is None:
            checks.append(CheckResult(
                name="dashboard_import", status="SKIP", message="Dashboard not loaded",
                severity="CRITICAL", citation="import etsy_dashboard failed"))
            return self._wrap(checks)

        checks.append(self._revenue_anomalies(ed))
        checks.append(self._fee_ratio_check(ed))
        checks.append(self._refund_rate(ed))
        checks.append(self._margin_consistency(ed))
        checks.append(self._growth_trajectory(ed))
        checks.append(self._seasonal_pattern(ed))

        return self._wrap(checks)

    def _revenue_anomalies(self, ed):
        """Flag months with revenue >2 std deviations from mean."""
        import numpy as np
        monthly_sales = _safe_getattr(ed, "monthly_sales", {})
        # monthly_sales may be a pandas Series or dict — convert to dict
        if hasattr(monthly_sales, "to_dict"):
            monthly_sales = monthly_sales.to_dict()
        if len(monthly_sales) < 3:
            return CheckResult(
                name="revenue_anomalies", status="SKIP",
                message=f"Need 3+ months for anomaly detection (have {len(monthly_sales)})",
                severity="INFO", citation=f"monthly_sales has {len(monthly_sales)} entries")

        values = list(monthly_sales.values())
        mean = float(np.mean(values))
        std = float(np.std(values))
        anomalies = []
        if std > 0:
            for month, val in monthly_sales.items():
                z = (float(val) - mean) / std
                if abs(z) > 2.0:
                    direction = "spike" if z > 0 else "drop"
                    anomalies.append(f"{month}: ${val:,.0f} ({direction}, z={z:.1f})")

        status = "PASS" if not anomalies else "WARN"
        return CheckResult(
            name="revenue_anomalies", status=status,
            message=f"{'No revenue anomalies' if not anomalies else f'{len(anomalies)} anomalies: ' + '; '.join(anomalies)}",
            severity="MEDIUM" if anomalies else "INFO",
            source_variable="monthly_sales",
            citation=f"mean=${mean:,.0f}, std=${std:,.0f}, months={len(monthly_sales)}")

    @staticmethod
    def _to_dict(obj):
        """Convert pandas Series to dict; pass dicts through."""
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        return obj if obj else {}

    def _fee_ratio_check(self, ed):
        """Verify that fees are within expected Etsy range (typically 15-25% of sales)."""
        gross = _safe_getattr(ed, "gross_sales", 0)
        fees = _safe_getattr(ed, "total_fees", 0)
        if gross <= 0:
            return CheckResult(
                name="fee_ratio_check", status="SKIP",
                message="No gross sales", severity="LOW",
                citation="gross_sales=0")

        ratio = fees / gross * 100
        status = "PASS" if 10 <= ratio <= 30 else "WARN"
        return CheckResult(
            name="fee_ratio_check", status=status,
            message=f"Fee ratio: {ratio:.1f}% (fees=${fees:,.2f} / sales=${gross:,.2f})",
            severity="MEDIUM" if status == "WARN" else "INFO",
            source_variable="total_fees", source_value=round(ratio, 1),
            expected_value=20.0,
            citation=f"var:total_fees={fees}, gross_sales={gross}, ratio={ratio:.1f}%")

    def _refund_rate(self, ed):
        """Check that refund rate is within normal range (<10%)."""
        gross = _safe_getattr(ed, "gross_sales", 0)
        refunds = _safe_getattr(ed, "total_refunds", 0)
        if gross <= 0:
            return CheckResult(
                name="refund_rate", status="SKIP",
                message="No gross sales", severity="LOW",
                citation="gross_sales=0")

        rate = refunds / gross * 100
        status = "PASS" if rate < 10 else ("WARN" if rate < 20 else "FAIL")
        return CheckResult(
            name="refund_rate", status=status,
            message=f"Refund rate: {rate:.1f}% (${refunds:,.2f} of ${gross:,.2f})",
            severity="HIGH" if rate >= 20 else ("MEDIUM" if rate >= 10 else "INFO"),
            source_variable="total_refunds", source_value=round(rate, 1),
            citation=f"var:total_refunds={refunds}, gross_sales={gross}")

    def _margin_consistency(self, ed):
        """Check that monthly margins don't swing wildly."""
        monthly_sales = self._to_dict(_safe_getattr(ed, "monthly_sales", {}))
        monthly_net = self._to_dict(_safe_getattr(ed, "monthly_net_revenue", {}))
        months_sorted = _safe_getattr(ed, "months_sorted", [])

        if len(months_sorted) < 3:
            return CheckResult(
                name="margin_consistency", status="SKIP",
                message="Need 3+ months", severity="INFO",
                citation=f"months_sorted has {len(months_sorted)} entries")

        margins = []
        for m in months_sorted:
            s = monthly_sales.get(m, 0)
            n = monthly_net.get(m, 0)
            if s > 0:
                margins.append((m, n / s * 100))

        if len(margins) < 3:
            return CheckResult(
                name="margin_consistency", status="SKIP",
                message="Not enough months with sales for margin check",
                severity="INFO", citation=f"margins computed for {len(margins)} months")

        margin_vals = [m[1] for m in margins]
        import numpy as np
        std = np.std(margin_vals)
        mean = np.mean(margin_vals)
        # Wild swings = std > 15 percentage points
        status = "PASS" if std < 15 else "WARN"
        worst = max(margins, key=lambda x: abs(x[1] - mean))
        return CheckResult(
            name="margin_consistency", status=status,
            message=f"Margin mean={mean:.1f}%, std={std:.1f}%, most volatile month: {worst[0]} ({worst[1]:.1f}%)",
            severity="MEDIUM" if std >= 15 else "INFO",
            source_variable="monthly_net_revenue",
            citation=f"margins across {len(margins)} months, mean={mean:.1f}%, std={std:.1f}%")

    def _growth_trajectory(self, ed):
        """Check if business is growing, stable, or declining."""
        monthly_sales = self._to_dict(_safe_getattr(ed, "monthly_sales", {}))
        months_sorted = _safe_getattr(ed, "months_sorted", [])
        if len(months_sorted) < 3:
            return CheckResult(
                name="growth_trajectory", status="SKIP",
                message="Need 3+ months for growth analysis",
                severity="INFO", citation=f"{len(months_sorted)} months")

        values = [monthly_sales.get(m, 0) for m in months_sorted]
        # Compare first half to second half
        mid = len(values) // 2
        first_half = sum(values[:mid]) / max(mid, 1)
        second_half = sum(values[mid:]) / max(len(values) - mid, 1)

        if first_half > 0:
            growth = (second_half - first_half) / first_half * 100
        else:
            growth = 100 if second_half > 0 else 0

        if growth > 10:
            label = "GROWING"
        elif growth > -10:
            label = "STABLE"
        else:
            label = "DECLINING"

        status = "PASS" if growth >= -20 else "WARN"
        return CheckResult(
            name="growth_trajectory", status=status,
            message=f"Business is {label}: {growth:+.1f}% (first half avg ${first_half:,.0f} → second half avg ${second_half:,.0f})",
            severity="MEDIUM" if label == "DECLINING" else "INFO",
            source_variable="monthly_sales", source_value=round(growth, 1),
            citation=f"first_half_avg=${first_half:,.0f}, second_half_avg=${second_half:,.0f}, growth={growth:.1f}%")

    def _seasonal_pattern(self, ed):
        """Info-level: detect if there's a seasonal sales pattern."""
        monthly_sales = self._to_dict(_safe_getattr(ed, "monthly_sales", {}))
        if len(monthly_sales) < 4:
            return CheckResult(
                name="seasonal_pattern", status="SKIP",
                message="Need 4+ months for seasonal analysis",
                severity="INFO", citation=f"{len(monthly_sales)} months")

        best_month = max(monthly_sales, key=monthly_sales.get)
        worst_month = min(monthly_sales, key=monthly_sales.get)
        best_val = monthly_sales[best_month]
        worst_val = monthly_sales[worst_month]
        ratio = best_val / worst_val if worst_val > 0 else float("inf")

        return CheckResult(
            name="seasonal_pattern", status="PASS",
            message=f"Best: {best_month} (${best_val:,.0f}), Worst: {worst_month} (${worst_val:,.0f}), ratio={ratio:.1f}x",
            severity="INFO",
            source_variable="monthly_sales",
            citation=f"best={best_month}=${best_val:,.0f}, worst={worst_month}=${worst_val:,.0f}")

    def _wrap(self, checks):
        passed = sum(1 for c in checks if c.status == "PASS")
        failed = sum(1 for c in checks if c.status == "FAIL")
        warned = sum(1 for c in checks if c.status == "WARN")
        skipped = sum(1 for c in checks if c.status == "SKIP")
        findings = [f"[{c.status}] {c.name}: {c.message}" for c in checks if c.status in ("FAIL", "WARN")]
        return GovernanceMessage(
            agent_name=self.agent_name, agent_id=self.agent_id,
            checks=[asdict(c) for c in checks],
            summary={"total": len(checks), "passed": passed, "failed": failed,
                     "warned": warned, "skipped": skipped},
            findings=findings)


# ---------------------------------------------------------------------------
# Agent 9: Cross-Source Reconciliation (10 points)
# ---------------------------------------------------------------------------

class CrossSourceReconciliationAgent:
    """Cross-references data across Etsy, bank, and inventory sources to verify
    end-to-end pipeline coherence."""
    agent_id = 9
    agent_name = "CrossSourceReconciliationAgent"

    def run(self) -> GovernanceMessage:
        ed = _get_dashboard()
        checks: list[CheckResult] = []
        if ed is None:
            checks.append(CheckResult(
                name="dashboard_import", status="SKIP", message="Dashboard not loaded",
                severity="CRITICAL", citation="import etsy_dashboard failed"))
            return self._wrap(checks)

        checks.append(self._etsy_deposit_vs_bank(ed))
        checks.append(self._profit_waterfall(ed))
        checks.append(self._expense_coverage(ed))
        checks.append(self._draw_settlement_math(ed))
        checks.append(self._inventory_pipeline(ed))
        checks.append(self._cash_flow_identity(ed))

        return self._wrap(checks)

    def _etsy_deposit_vs_bank(self, ed):
        """Etsy total deposited should be close to bank total deposits
        (bank may have non-Etsy deposits, so bank >= etsy_deposited minus pre-capone)."""
        etsy_deposited = _safe_getattr(ed, "etsy_total_deposited", 0)
        bank_deposits = _safe_getattr(ed, "bank_total_deposits", 0)
        pre_capone = _safe_getattr(ed, "etsy_pre_capone_deposits", 0)

        # Bank deposits should be roughly etsy_total_deposited - pre_capone
        # (pre_capone was before this bank account existed)
        expected_bank = etsy_deposited - pre_capone
        delta = round(abs(bank_deposits - expected_bank), 2)
        # Allow some tolerance — timing differences, partial months
        tol_pct = 0.15  # 15%
        tol_abs = max(expected_bank * tol_pct, 100) if expected_bank > 0 else 100
        status = "PASS" if delta <= tol_abs else "WARN"
        return CheckResult(
            name="etsy_deposit_vs_bank", status=status,
            message=f"bank_deposits=${bank_deposits:,.2f} vs expected ${expected_bank:,.2f} "
                    f"(etsy_deposited=${etsy_deposited:,.2f} - pre_capone=${pre_capone:,.2f}), delta=${delta:,.2f}",
            severity="HIGH" if status == "WARN" else "INFO",
            source_variable="bank_total_deposits", source_value=bank_deposits,
            expected_value=round(expected_bank, 2), delta=delta,
            tolerance=round(tol_abs, 2),
            citation=f"var:etsy_total_deposited={etsy_deposited}, bank_total_deposits={bank_deposits}, pre_capone={pre_capone}")

    def _profit_waterfall(self, ed):
        """Verify the full profit waterfall: revenue → expenses → profit → cash + draws."""
        gross = _safe_getattr(ed, "gross_sales", 0)
        etsy_net = _safe_getattr(ed, "etsy_net_earned", 0)
        real_profit = _safe_getattr(ed, "real_profit", 0)
        cash = _safe_getattr(ed, "bank_cash_on_hand", 0)
        draws = _safe_getattr(ed, "bank_owner_draw_total", 0)

        # real_profit should be <= gross_sales (you can't profit more than you earned)
        profit_exceeds = real_profit > gross * 1.1  # 10% tolerance for timing
        # cash + draws should equal real_profit
        profit_check = abs(round(cash + draws, 2) - round(real_profit, 2)) <= 0.01

        failures = []
        if profit_exceeds and gross > 0:
            failures.append(f"real_profit (${real_profit:,.2f}) exceeds gross_sales (${gross:,.2f})")
        if not profit_check:
            failures.append(f"cash({cash:,.2f})+draws({draws:,.2f}) != real_profit({real_profit:,.2f})")

        status = "PASS" if not failures else "FAIL"
        return CheckResult(
            name="profit_waterfall", status=status,
            message=f"{'Profit waterfall valid' if not failures else '; '.join(failures)}: "
                    f"gross=${gross:,.2f} → net=${etsy_net:,.2f} → profit=${real_profit:,.2f} "
                    f"= cash(${cash:,.2f}) + draws(${draws:,.2f})",
            severity="CRITICAL" if failures else "INFO",
            source_variable="real_profit", source_value=round(real_profit, 2),
            citation=f"waterfall: gross={gross} → etsy_net={etsy_net} → profit={real_profit}")

    def _expense_coverage(self, ed):
        """Check that bank expenses + Etsy fees account for the gap between gross and profit."""
        gross = _safe_getattr(ed, "gross_sales", 0)
        real_profit = _safe_getattr(ed, "real_profit", 0)
        total_fees = _safe_getattr(ed, "total_fees", 0)
        total_ship = _safe_getattr(ed, "total_shipping_cost", 0)
        total_mkt = _safe_getattr(ed, "total_marketing", 0)
        total_refunds = _safe_getattr(ed, "total_refunds", 0)
        total_taxes = _safe_getattr(ed, "total_taxes", 0)
        bank_all_exp = _safe_getattr(ed, "bank_all_expenses", 0)
        inv_cost = _safe_getattr(ed, "total_inventory_cost", 0)

        known_expenses = total_fees + total_ship + total_mkt + total_refunds + total_taxes
        total_gap = gross - real_profit
        accounted = known_expenses + bank_all_exp
        unexplained = abs(total_gap - accounted)

        # Some gap is expected (Etsy balance, timing)
        pct_unexplained = (unexplained / gross * 100) if gross > 0 else 0
        status = "PASS" if pct_unexplained < 10 else ("WARN" if pct_unexplained < 25 else "FAIL")
        return CheckResult(
            name="expense_coverage", status=status,
            message=f"Gross→Profit gap: ${total_gap:,.2f}, accounted: ${accounted:,.2f}, "
                    f"unexplained: ${unexplained:,.2f} ({pct_unexplained:.1f}%)",
            severity="HIGH" if pct_unexplained >= 25 else ("MEDIUM" if pct_unexplained >= 10 else "INFO"),
            source_variable="gross_sales", source_value=round(total_gap, 2),
            expected_value=round(accounted, 2),
            delta=round(unexplained, 2),
            citation=f"gap={total_gap}, etsy_expenses={known_expenses}, bank_expenses={bank_all_exp}")

    def _draw_settlement_math(self, ed):
        """Verify draw settlement calculations between partners."""
        tulsa = round(_safe_getattr(ed, "tulsa_draw_total", 0), 2)
        texas = round(_safe_getattr(ed, "texas_draw_total", 0), 2)
        diff = round(_safe_getattr(ed, "draw_diff", 0), 2)
        owed_to = _safe_getattr(ed, "draw_owed_to", "")

        expected_diff = round(abs(tulsa - texas), 2)
        diff_match = abs(diff - expected_diff) <= 0.01

        expected_owed = "Braden" if tulsa > texas else "TJ"
        owed_match = owed_to == expected_owed or tulsa == texas

        failures = []
        if not diff_match:
            failures.append(f"draw_diff={diff} but |tulsa-texas|={expected_diff}")
        if not owed_match:
            failures.append(f"draw_owed_to='{owed_to}' but expected '{expected_owed}'")

        status = "PASS" if not failures else "FAIL"
        return CheckResult(
            name="draw_settlement_math", status=status,
            message=f"Tulsa=${tulsa:,.2f}, Texas=${texas:,.2f}, diff=${diff:,.2f}, owed_to={owed_to}"
                    + (f" ERRORS: {failures}" if failures else ""),
            severity="HIGH" if failures else "INFO",
            source_variable="draw_diff", source_value=diff,
            expected_value=expected_diff,
            citation=f"var:tulsa={tulsa}, texas={texas}, diff={diff}, owed_to={owed_to}")

    def _inventory_pipeline(self, ed):
        """Check inventory data pipeline: INVOICES → INV_DF → totals."""
        invoices = _safe_getattr(ed, "INVOICES", [])
        inv_df = _safe_getattr(ed, "INV_DF")
        total_cost = _safe_getattr(ed, "total_inventory_cost", 0)

        if not invoices:
            return CheckResult(
                name="inventory_pipeline", status="SKIP",
                message="No invoices", severity="LOW",
                citation="INVOICES is empty")

        # INVOICES count should match INV_DF rows
        inv_count = len(invoices)
        df_count = len(inv_df) if inv_df is not None else 0
        count_match = inv_count == df_count

        # INV_DF sum should match total_inventory_cost
        df_sum = round(inv_df["grand_total"].sum(), 2) if inv_df is not None and len(inv_df) > 0 else 0
        sum_match = abs(round(total_cost, 2) - df_sum) <= 0.01

        failures = []
        if not count_match:
            failures.append(f"INVOICES({inv_count}) != INV_DF({df_count})")
        if not sum_match:
            failures.append(f"total_inventory_cost({total_cost}) != INV_DF.sum({df_sum})")

        status = "PASS" if not failures else "FAIL"
        return CheckResult(
            name="inventory_pipeline", status=status,
            message=f"Pipeline: {inv_count} invoices → {df_count} rows → ${total_cost:,.2f}"
                    + (f" ERRORS: {failures}" if failures else ""),
            severity="HIGH" if failures else "INFO",
            source_variable="INVOICES",
            citation=f"INVOICES={inv_count}, INV_DF={df_count}, total_cost={total_cost}")

    def _cash_flow_identity(self, ed):
        """The fundamental accounting identity:
        bank_net_cash = bank_total_deposits - bank_total_debits
        bank_cash_on_hand = bank_net_cash + etsy_balance
        real_profit = bank_cash_on_hand + bank_owner_draw_total"""
        deposits = round(_safe_getattr(ed, "bank_total_deposits", 0), 2)
        debits = round(_safe_getattr(ed, "bank_total_debits", 0), 2)
        net_cash = round(_safe_getattr(ed, "bank_net_cash", 0), 2)
        etsy_bal = round(_safe_getattr(ed, "etsy_balance", 0), 2)
        cash_on_hand = round(_safe_getattr(ed, "bank_cash_on_hand", 0), 2)
        draws = round(_safe_getattr(ed, "bank_owner_draw_total", 0), 2)
        profit = round(_safe_getattr(ed, "real_profit", 0), 2)

        checks_passed = []
        failures = []

        # Identity 1
        expected_net = round(deposits - debits, 2)
        if abs(net_cash - expected_net) <= 0.01:
            checks_passed.append("deposits-debits=net_cash")
        else:
            failures.append(f"net_cash({net_cash}) != deposits({deposits})-debits({debits})={expected_net}")

        # Identity 2
        expected_coh = round(net_cash + etsy_bal, 2)
        if abs(cash_on_hand - expected_coh) <= 0.01:
            checks_passed.append("net_cash+etsy_bal=cash_on_hand")
        else:
            failures.append(f"cash_on_hand({cash_on_hand}) != net_cash({net_cash})+etsy_bal({etsy_bal})={expected_coh}")

        # Identity 3
        expected_profit = round(cash_on_hand + draws, 2)
        if abs(profit - expected_profit) <= 0.01:
            checks_passed.append("cash_on_hand+draws=profit")
        else:
            failures.append(f"profit({profit}) != cash_on_hand({cash_on_hand})+draws({draws})={expected_profit}")

        status = "PASS" if not failures else "FAIL"
        return CheckResult(
            name="cash_flow_identity", status=status,
            message=f"{'All 3 identities hold' if not failures else f'{len(failures)} broken: ' + '; '.join(failures)}",
            severity="CRITICAL" if failures else "INFO",
            source_variable="real_profit", source_value=profit,
            citation=f"3 identities: {' | '.join(checks_passed + failures)}")

    def _wrap(self, checks):
        passed = sum(1 for c in checks if c.status == "PASS")
        failed = sum(1 for c in checks if c.status == "FAIL")
        warned = sum(1 for c in checks if c.status == "WARN")
        skipped = sum(1 for c in checks if c.status == "SKIP")
        findings = [f"[{c.status}] {c.name}: {c.message}" for c in checks if c.status in ("FAIL", "WARN")]
        return GovernanceMessage(
            agent_name=self.agent_name, agent_id=self.agent_id,
            checks=[asdict(c) for c in checks],
            summary={"total": len(checks), "passed": passed, "failed": failed,
                     "warned": warned, "skipped": skipped},
            findings=findings)


# ---------------------------------------------------------------------------
# Agent 1: CEO Governance (runs last, reviews all)
# ---------------------------------------------------------------------------

# Weight tables for scoring
AGENT_WEIGHTS = {
    # Financial Integrity — 25 pts (most critical: actual dollar accuracy)
    2: {"total": 25, "breakdown": {
        "etsy_net_parity": 10, "reverse_engineering_gone": 3, "profit_chain": 5,
        "dedup_integrity": 4, "monthly_sum": 3, "bank_balance": 0,
        "draw_reconciliation": 0, "inventory_consistency": 0,
        "tax_year_completeness": 0, "none_estimates_intact": 0,
        "inferred_revenue_flagged": 0,
    }},
    # Data Consistency — 15 pts
    3: {"total": 15, "breakdown": {
        "etsy_row_count": 2, "bank_row_count": 2, "inventory_row_count": 1,
        "deposit_reconciliation": 3, "inventory_vs_bank": 0,
        "missing_months": 2, "orphaned_records": 0,
        "etsy_balance_gap": 3, "type_coverage": 2,
        "duplicate_bank_check": 0,
    }},
    # Tax & Valuation — 10 pts
    4: {"total": 10, "breakdown": {
        "tax_year_splits": 3, "income_tax_function": 2, "draw_parity_across_years": 2,
        "valuation_confidence_labels": 3, "tax_year_net_income": 0,
        "valuation_data_sufficiency": 0, "tax_logic_chain": 0,
    }},
    # Self-Testing — 10 pts
    5: {"total": 10, "breakdown": {
        "money_function": 2, "parse_money_function": 2,
        "none_propagation": 2, "negative_guards": 2,
        "zero_division_guards": 0, "reconciliation_structure": 2,
        "empty_df_safety": 0, "config_key_types": 0,
    }},
    # Deployment Guardian — 10 pts
    6: {"total": 10, "breakdown": {
        "supabase_connected": 2, "tables_exist": 3, "env_vars_present": 2,
        "config_keys_valid": 2, "schema_snapshot": 1, "mutation_log_audit": 0,
    }},
    # Silent Error Sentinel — 10 pts
    7: {"total": 10, "breakdown": {
        "nan_in_financials": 3, "null_dates": 2, "bank_txn_integrity": 2,
        "data_loading_coverage": 3, "invoice_field_completeness": 0,
        "config_type_safety": 0, "amount_parse_quality": 0,
    }},
    # Trend Anomaly Detector — 10 pts
    8: {"total": 10, "breakdown": {
        "revenue_anomalies": 3, "fee_ratio_check": 2, "refund_rate": 2,
        "margin_consistency": 3, "growth_trajectory": 0,
        "seasonal_pattern": 0,
    }},
    # Cross-Source Reconciliation — 10 pts
    9: {"total": 10, "breakdown": {
        "etsy_deposit_vs_bank": 2, "profit_waterfall": 3,
        "cash_flow_identity": 3, "draw_settlement_math": 2,
        "expense_coverage": 0, "inventory_pipeline": 0,
    }},
}


class CEOGovernanceAgent:
    agent_id = 1
    agent_name = "CEOGovernanceAgent"

    def run(self, agent_messages: list[GovernanceMessage]) -> dict:
        """Review all agent reports and produce final governance report."""
        t_start = time.time()

        # Index messages by agent_id
        by_id = {m.agent_id: m for m in agent_messages}

        # Compute score
        score = 0.0
        agent_scores = {}
        all_checks = []
        has_critical_fail = False

        for agent_id, weight_info in AGENT_WEIGHTS.items():
            msg = by_id.get(agent_id)
            if msg is None:
                agent_scores[agent_id] = {"earned": 0, "max": weight_info["total"], "pct": 0}
                continue

            agent_earned = 0.0
            for check in msg.checks:
                name = check["name"]
                max_pts = weight_info["breakdown"].get(name, 0)
                all_checks.append(check)

                if check["status"] == "PASS":
                    agent_earned += max_pts
                elif check["status"] == "WARN" or check["status"] == "SKIP":
                    agent_earned += max_pts * 0.5
                # FAIL = 0 points

                if check["status"] == "FAIL" and check["severity"] == "CRITICAL":
                    has_critical_fail = True

            agent_scores[agent_id] = {
                "earned": round(agent_earned, 1),
                "max": weight_info["total"],
                "pct": round(agent_earned / weight_info["total"] * 100, 1) if weight_info["total"] > 0 else 0,
            }
            score += agent_earned

        # VETO: any CRITICAL FAIL caps at 49
        if has_critical_fail and score > 49:
            score = 49.0

        score = round(score, 1)

        # Grade
        if score >= 90:
            grade = "A"
        elif score >= 75:
            grade = "B"
        elif score >= 50:
            grade = "C"
        elif score >= 25:
            grade = "D"
        else:
            grade = "F"

        # Citation validation: demote checks with empty citations
        uncited = []
        for check in all_checks:
            if not check.get("citation", "").strip():
                uncited.append(check["name"])
                # Demote to WARN if it was PASS
                if check["status"] == "PASS":
                    check["status"] = "WARN"
                    check["message"] += " [DEMOTED: missing citation]"

        # Cross-agent conflict detection
        conflicts = self._detect_conflicts(by_id)

        # Executive summary
        total_checks = len(all_checks)
        total_passed = sum(1 for c in all_checks if c["status"] == "PASS")
        total_failed = sum(1 for c in all_checks if c["status"] == "FAIL")
        total_warned = sum(1 for c in all_checks if c["status"] == "WARN")

        all_findings = []
        for msg in agent_messages:
            all_findings.extend(msg.findings)

        duration = round(time.time() - t_start, 3)

        report = {
            "governance_version": "1.0.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": duration,
            "score": score,
            "grade": grade,
            "veto_applied": has_critical_fail,
            "total_checks": total_checks,
            "passed": total_passed,
            "failed": total_failed,
            "warned": total_warned,
            "agent_scores": agent_scores,
            "uncited_claims": uncited,
            "cross_agent_conflicts": conflicts,
            "executive_summary": all_findings,
            "agents": {m.agent_id: {
                "name": m.agent_name,
                "timestamp": m.timestamp,
                "summary": m.summary,
                "checks": m.checks,
                "findings": m.findings,
            } for m in agent_messages},
            "metric_sources": METRIC_SOURCES,
        }

        return report

    def _detect_conflicts(self, by_id):
        """Look for cross-agent contradictions."""
        conflicts = []

        # Agent 2 and Agent 4 both check tax year sums
        a2 = by_id.get(2)
        a4 = by_id.get(4)
        if a2 and a4:
            a2_tax = next((c for c in a2.checks if c["name"] == "tax_year_completeness"), None)
            a4_tax = next((c for c in a4.checks if c["name"] == "tax_year_splits"), None)
            if a2_tax and a4_tax and a2_tax["status"] != a4_tax["status"]:
                conflicts.append({
                    "agents": [2, 4],
                    "check_names": ["tax_year_completeness", "tax_year_splits"],
                    "description": f"Agent 2 says {a2_tax['status']} but Agent 4 says {a4_tax['status']} for tax year validation",
                })

        # Agent 2 and Agent 3 both check dedup
        a3 = by_id.get(3)
        if a2 and a3:
            a2_dedup = next((c for c in a2.checks if c["name"] == "dedup_integrity"), None)
            a3_dedup = next((c for c in a3.checks if c["name"] == "duplicate_bank_check"), None)
            # If one says duplicates exist and the other says no, that's a conflict
            if a2_dedup and a3_dedup:
                a2_has_dups = a2_dedup["status"] in ("WARN", "FAIL")
                a3_has_dups = a3_dedup["status"] in ("WARN", "FAIL")
                if a2_has_dups != a3_has_dups:
                    conflicts.append({
                        "agents": [2, 3],
                        "check_names": ["dedup_integrity", "duplicate_bank_check"],
                        "description": "Conflicting dedup findings between Financial Integrity and Data Consistency agents",
                    })

        return conflicts


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

# Module-level cache for the latest report
_LATEST_REPORT: dict = {}
_REPORT_LOCK = threading.Lock()


def run_governance(trigger="manual") -> dict:
    """Execute all 6 agents and return the full governance report."""
    t_start = time.time()

    # Run agents 2-9
    agents = [
        FinancialIntegrityAgent(),
        DataConsistencyAgent(),
        TaxValuationAgent(),
        SelfTestingAgent(),
        DeploymentGuardianAgent(),
        SilentErrorSentinelAgent(),
        TrendAnomalyAgent(),
        CrossSourceReconciliationAgent(),
    ]

    messages = []
    for agent in agents:
        try:
            msg = agent.run()
            messages.append(msg)
        except Exception as e:
            messages.append(GovernanceMessage(
                agent_name=agent.agent_name,
                agent_id=agent.agent_id,
                checks=[asdict(CheckResult(
                    name="agent_crash", status="FAIL",
                    message=f"Agent crashed: {e}", severity="HIGH",
                    citation=f"exception: {type(e).__name__}: {e}"))],
                summary={"total": 1, "passed": 0, "failed": 1, "warned": 0, "skipped": 0},
                findings=[f"[FAIL] Agent {agent.agent_name} crashed: {e}"],
            ))

    # Run CEO agent (Agent 1) last
    ceo = CEOGovernanceAgent()
    report = ceo.run(messages)
    report["trigger"] = trigger
    report["total_duration_seconds"] = round(time.time() - t_start, 3)

    # Sanitize numpy/pandas types for JSON serialization
    report = _sanitize(report)

    # Cache the report
    with _REPORT_LOCK:
        global _LATEST_REPORT
        _LATEST_REPORT = report

    # Persist to Supabase config (background thread to avoid blocking)
    def _persist():
        try:
            from supabase_loader import save_config_value
            save_config_value("governance_last_report", report)
            save_config_value("governance_last_score", {
                "score": report["score"],
                "grade": report["grade"],
                "timestamp": report["timestamp"],
            })
            save_config_value("governance_last_run", {
                "timestamp": report["timestamp"],
                "duration_seconds": report["total_duration_seconds"],
                "trigger": trigger,
            })
        except Exception:
            pass
    threading.Thread(target=_persist, daemon=True).start()

    return report


def run_governance_async(trigger="reload"):
    """Run governance in a background thread (non-blocking)."""
    def _run():
        try:
            run_governance(trigger=trigger)
        except Exception:
            pass
    threading.Thread(target=_run, daemon=True).start()


# ---------------------------------------------------------------------------
# Mutation logging
# ---------------------------------------------------------------------------

_MUTATION_LOG_LOCK = threading.Lock()


def log_mutation(table: str, key: str, value=None):
    """Append a mutation event to the governance mutation log."""
    entry = {
        "table": table,
        "key": str(key),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "value_type": type(value).__name__ if value is not None else "None",
    }
    try:
        from supabase_loader import get_config_value, save_config_value
        with _MUTATION_LOG_LOCK:
            log = get_config_value("governance_mutation_log", [])
            if not isinstance(log, list):
                log = []
            log.append(entry)
            # Cap at 1000 entries
            if len(log) > 1000:
                log = log[-1000:]
            save_config_value("governance_mutation_log", log)
    except Exception:
        pass  # Best-effort logging


# ---------------------------------------------------------------------------
# Flask API routes
# ---------------------------------------------------------------------------

def register_governance_routes(server):
    """Register governance API endpoints on the Flask server."""
    import flask

    @server.route("/api/governance/run", methods=["POST"])
    def api_governance_run():
        try:
            report = run_governance(trigger="api")
            return flask.jsonify(report)
        except Exception as e:
            return flask.jsonify({"error": str(e)}), 500

    @server.route("/api/governance/report")
    def api_governance_report():
        with _REPORT_LOCK:
            if _LATEST_REPORT:
                return flask.jsonify(_LATEST_REPORT)
        # Try loading from Supabase cache
        try:
            from supabase_loader import get_config_value
            saved = get_config_value("governance_last_report")
            if saved:
                return flask.jsonify(saved)
        except Exception:
            pass
        return flask.jsonify({"error": "No governance report available. POST /api/governance/run first."}), 404

    @server.route("/api/governance/score")
    def api_governance_score():
        with _REPORT_LOCK:
            if _LATEST_REPORT:
                return flask.jsonify({
                    "score": _LATEST_REPORT["score"],
                    "grade": _LATEST_REPORT["grade"],
                    "timestamp": _LATEST_REPORT["timestamp"],
                    "checks": _LATEST_REPORT["total_checks"],
                    "failed": _LATEST_REPORT["failed"],
                })
        try:
            from supabase_loader import get_config_value
            saved = get_config_value("governance_last_score")
            if saved:
                return flask.jsonify(saved)
        except Exception:
            pass
        return flask.jsonify({"error": "No score available"}), 404

    @server.route("/api/governance/agent/<int:agent_id>")
    def api_governance_agent(agent_id):
        with _REPORT_LOCK:
            if _LATEST_REPORT and agent_id in _LATEST_REPORT.get("agents", {}):
                return flask.jsonify(_LATEST_REPORT["agents"][agent_id])
        try:
            from supabase_loader import get_config_value
            saved = get_config_value("governance_last_report")
            if saved and str(agent_id) in saved.get("agents", {}):
                return flask.jsonify(saved["agents"][str(agent_id)])
        except Exception:
            pass
        return flask.jsonify({"error": f"Agent {agent_id} report not found"}), 404

    @server.route("/api/governance/health")
    def api_governance_health():
        last_run = None
        try:
            from supabase_loader import get_config_value
            last_run = get_config_value("governance_last_run")
        except Exception:
            pass
        return flask.jsonify({
            "status": "ok",
            "agents": 9,
            "last_run": last_run,
            "has_cached_report": bool(_LATEST_REPORT),
        })

    @server.route("/api/governance/mutations")
    def api_governance_mutations():
        try:
            from supabase_loader import get_config_value
            log = get_config_value("governance_mutation_log", [])
            return flask.jsonify({"count": len(log), "mutations": log})
        except Exception as e:
            return flask.jsonify({"error": str(e)}), 500
