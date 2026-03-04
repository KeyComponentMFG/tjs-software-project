"""
accounting/compat.py — Backward compatibility shim.

Publishes Ledger values → module globals in etsy_dashboard.py so existing
14K lines of charts, callbacks, and KPIs keep working unchanged.

Usage in etsy_dashboard.py:
    from accounting.compat import publish_to_globals
    publish_to_globals(pipeline, target_module)
"""

from __future__ import annotations

import sys
from decimal import Decimal
from typing import TYPE_CHECKING

from .models import Confidence

if TYPE_CHECKING:
    from .pipeline import AccountingPipeline


def publish_to_globals(pipeline: AccountingPipeline, target_module_name: str):
    """Write all Ledger metrics into the target module's global namespace.

    This is the backward-compat bridge: the pipeline computes metrics with
    Decimal precision and confidence tracking, then we publish float values
    to the module globals that existing dashboard code reads.

    Parameters:
        pipeline: The AccountingPipeline with a populated ledger
        target_module_name: Module name to write globals into (e.g., "__main__" or "etsy_dashboard")
    """
    ledger = pipeline.ledger
    if ledger is None:
        print("[compat] WARNING: No ledger available, skipping global publish")
        return

    mod = sys.modules.get(target_module_name)
    if mod is None:
        print(f"[compat] WARNING: Module {target_module_name!r} not found in sys.modules")
        return

    strict_mode = ledger.strict_mode

    # Publish pipeline references
    setattr(mod, "strict_mode", strict_mode)
    setattr(mod, "ledger_ref", ledger)

    # Confidence levels that become None in strict mode
    _STRICT_HIDDEN = {Confidence.ESTIMATED, Confidence.PROJECTION, Confidence.HEURISTIC}

    # ── Scalar metrics (float) ──
    float_metrics = [
        "gross_sales", "total_refunds", "net_sales",
        "total_fees", "total_shipping_cost", "total_marketing",
        "total_taxes", "total_payments", "total_buyer_fees",
        "avg_order",
        "etsy_net_earned", "etsy_net_margin",
        "etsy_balance", "etsy_pre_capone_deposits",
        "etsy_total_deposited", "etsy_balance_calculated", "etsy_csv_gap",
        "bank_total_deposits", "bank_total_debits", "bank_net_cash",
        "bank_tax_deductible", "bank_personal", "bank_pending",
        "bank_biz_expense_total", "bank_all_expenses",
        "bank_cash_on_hand", "bank_owner_draw_total",
        "real_profit", "real_profit_margin",
        "tulsa_draw_total", "texas_draw_total", "draw_diff",
        "listing_fees", "transaction_fees_product", "transaction_fees_shipping",
        "processing_fees",
        "credit_transaction", "credit_listing", "credit_processing",
        "share_save", "total_credits", "total_fees_gross",
        "etsy_ads", "offsite_ads_fees", "offsite_ads_credits",
        "usps_outbound", "usps_return",
        "asendia_labels", "ship_adjustments", "ship_credits", "ship_insurance",
        "avg_outbound_label",
        "buyer_paid_shipping", "shipping_profit", "shipping_margin",
    ]

    for name in float_metrics:
        mv = ledger.get(name)
        if mv is not None:
            # Publish None for UNKNOWN-confidence metrics (removed estimates)
            # In strict mode, also hide ESTIMATED/PROJECTION/HEURISTIC
            if mv.confidence == Confidence.UNKNOWN or mv.confidence == Confidence.QUARANTINED:
                setattr(mod, name, None)
            elif strict_mode and mv.confidence in _STRICT_HIDDEN:
                setattr(mod, name, None)
            else:
                setattr(mod, name, float(mv.value))

    # Integer metrics
    int_metrics = [
        "order_count",
        "usps_outbound_count", "usps_return_count",
        "asendia_count", "ship_adjust_count", "ship_credit_count",
        "ship_insurance_count", "paid_ship_count", "free_ship_count",
    ]
    for name in int_metrics:
        mv = ledger.get(name)
        if mv is not None:
            setattr(mod, name, int(mv.value))

    # etsy_net is an alias for etsy_net_earned
    mv = ledger.get("etsy_net_earned")
    if mv is not None:
        setattr(mod, "etsy_net", float(mv.value))

    # ── Complex data structures ──
    # bank_by_cat: dict[str, float]
    setattr(mod, "bank_by_cat", pipeline.get_bank_by_cat())

    # bank_monthly: dict[str, dict[str, float]]
    setattr(mod, "bank_monthly", pipeline.get_bank_monthly())

    # Draw settlement
    setattr(mod, "draw_owed_to", pipeline.get_draw_owed_to())
    setattr(mod, "tulsa_draws", pipeline.get_tulsa_draws_raw())
    setattr(mod, "texas_draws", pipeline.get_texas_draws_raw())

    # bank_unaccounted (legacy, replaced by reconciliation)
    setattr(mod, "bank_unaccounted", ledger.get_float("bank_unaccounted", 0.0))

    # ── Expense Completeness ──
    expense_result = pipeline.get_expense_completeness()
    if expense_result:
        setattr(mod, "expense_receipt_verified", float(expense_result.receipt_verified_total))
        setattr(mod, "expense_bank_recorded", float(expense_result.bank_recorded_total))
        setattr(mod, "expense_gap", float(expense_result.gap_total))
        setattr(mod, "expense_by_category", {
            k: {kk: float(vv) if isinstance(vv, Decimal) else vv for kk, vv in v.items()}
            for k, v in expense_result.by_category.items()
        })
        setattr(mod, "expense_missing_receipts", pipeline.get_missing_receipts())

    print(f"[compat] Published {len(float_metrics) + len(int_metrics)} metrics to {target_module_name}")
