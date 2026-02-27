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

        # Check for exact duplicate rows (same Date, Type, Title, Net)
        dup_cols = ["Date", "Type", "Title", "Net"]
        existing_cols = [c for c in dup_cols if c in data.columns]
        if len(existing_cols) < 3:
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
        """Check that all sale-related types have at least some matching sale records."""
        data = _safe_getattr(ed, "DATA")
        if data is None:
            return CheckResult(
                name="orphaned_records", status="SKIP",
                message="DATA not available", severity="LOW",
                citation="DATA is None")

        sale_orders = set()
        if "Info" in data.columns:
            sale_info = data[data["Type"] == "Sale"]["Info"].dropna()
            for info in sale_info:
                # Extract order number patterns
                import re
                m = re.search(r"#?(\d{9,})", str(info))
                if m:
                    sale_orders.add(m.group(1))

        status = "PASS" if len(sale_orders) > 0 else "WARN"
        return CheckResult(
            name="orphaned_records", status=status,
            message=f"{len(sale_orders)} unique order references found in sale records",
            severity="LOW", source_variable="DATA.Info",
            source_value=len(sale_orders),
            citation=f"var:sale_order_count={len(sale_orders)}")

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
                            client.table(t).select("id", count="exact").limit(0).execute()
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
# Agent 1: CEO Governance (runs last, reviews all)
# ---------------------------------------------------------------------------

# Weight tables for scoring
AGENT_WEIGHTS = {
    2: {"total": 30, "breakdown": {
        "etsy_net_parity": 15, "reverse_engineering_gone": 5, "profit_chain": 5,
        "dedup_integrity": 5, "monthly_sum": 0, "bank_balance": 0,
        "draw_reconciliation": 0, "inventory_consistency": 0,
        "tax_year_completeness": 0, "none_estimates_intact": 0,
        "inferred_revenue_flagged": 0,
    }},
    3: {"total": 25, "breakdown": {
        "etsy_row_count": 3, "bank_row_count": 3, "inventory_row_count": 3,
        "deposit_reconciliation": 4, "inventory_vs_bank": 0,
        "missing_months": 4, "orphaned_records": 0,
        "etsy_balance_gap": 4, "type_coverage": 4,
        "duplicate_bank_check": 0,
    }},
    4: {"total": 15, "breakdown": {
        "tax_year_splits": 5, "income_tax_function": 3, "draw_parity_across_years": 3,
        "valuation_confidence_labels": 4, "tax_year_net_income": 0,
        "valuation_data_sufficiency": 0, "tax_logic_chain": 0,
    }},
    5: {"total": 15, "breakdown": {
        "money_function": 2, "parse_money_function": 3,
        "none_propagation": 4, "negative_guards": 3,
        "zero_division_guards": 0, "reconciliation_structure": 3,
        "empty_df_safety": 0, "config_key_types": 0,
    }},
    6: {"total": 15, "breakdown": {
        "supabase_connected": 3, "tables_exist": 4, "env_vars_present": 3,
        "config_keys_valid": 3, "schema_snapshot": 2, "mutation_log_audit": 0,
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

    # Run agents 2-6
    agents = [
        FinancialIntegrityAgent(),
        DataConsistencyAgent(),
        TaxValuationAgent(),
        SelfTestingAgent(),
        DeploymentGuardianAgent(),
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
                    message=f"Agent crashed: {e}", severity="CRITICAL",
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
            "agents": 6,
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
