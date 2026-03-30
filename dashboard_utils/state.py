"""Core state container for the Etsy dashboard.

Replaces 223 mutable global variables with a single immutable state object.
This is a BRIDGE — it produces the exact same values as the current globals.

The EtsyState object holds data that changes when the store filter changes
(Etsy metrics, charts). It does NOT hold things that are constant regardless
of store (bank data, inventory, config, pipeline).
"""
from __future__ import annotations

import re
import threading
from typing import Any, Optional

import pandas as pd

from dashboard_utils.logging_config import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helper functions — canonical definitions live in dashboard_utils.helpers
# ---------------------------------------------------------------------------
from dashboard_utils.helpers import _normalize_product_name, _merge_product_prefixes  # noqa: E402


# ---------------------------------------------------------------------------
# EtsyState — immutable snapshot of Etsy data for a specific store filter
# ---------------------------------------------------------------------------

class EtsyState:
    """Immutable snapshot of Etsy data for a specific store filter.

    All fields are set once at construction time and should not be mutated.
    """

    __slots__ = (
        "store",
        # Raw filtered data
        "data",
        # Filtered DataFrames
        "sales_df", "fee_df", "ship_df", "mkt_df", "refund_df", "tax_df",
        "deposit_df", "buyer_fee_df", "payment_df",
        # Scalar metrics
        "gross_sales", "total_refunds", "net_sales", "total_fees",
        "total_shipping_cost", "total_marketing", "total_taxes",
        "total_payments", "total_buyer_fees", "order_count", "avg_order",
        # Monthly aggregations
        "months_sorted",
        "monthly_sales", "monthly_fees", "monthly_shipping", "monthly_marketing",
        "monthly_refunds", "monthly_taxes",
        "monthly_raw_fees", "monthly_raw_shipping", "monthly_raw_marketing",
        "monthly_raw_refunds", "monthly_raw_taxes", "monthly_raw_buyer_fees",
        "monthly_raw_payments",
        "monthly_net_revenue", "monthly_order_counts",
        "monthly_aov", "monthly_profit_per_order",
        "days_active",
        # Daily aggregations
        "daily_sales", "daily_orders", "daily_df", "weekly_aov",
        # Fee breakdown
        "listing_fees", "transaction_fees_product", "transaction_fees_shipping",
        "processing_fees",
        "credit_transaction", "credit_listing", "credit_processing",
        "share_save", "total_credits", "total_fees_gross",
        # Marketing breakdown
        "etsy_ads", "offsite_ads_fees", "offsite_ads_credits",
        # Shipping breakdown
        "usps_outbound", "usps_outbound_count",
        "usps_return", "usps_return_count",
        "asendia_labels", "asendia_count",
        "ship_adjustments", "ship_adjust_count",
        "ship_credits", "ship_credit_count",
        "ship_insurance", "ship_insurance_count",
        "buyer_paid_shipping", "shipping_profit", "shipping_margin",
        "paid_ship_count", "free_ship_count", "avg_outbound_label",
        # Product performance
        "product_fee_totals", "product_revenue_est",
        # Deposit tracking
        "_etsy_deposit_total", "_deposit_rows",
    )

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            object.__setattr__(self, key, value)


# ---------------------------------------------------------------------------
# build_etsy_state — compute ALL derived values from filtered data
# ---------------------------------------------------------------------------

def build_etsy_state(data: pd.DataFrame, store: str, config: dict) -> EtsyState:
    """Build an EtsyState snapshot from the given data.

    Replicates the exact logic of _rebuild_etsy_derived(), _apply_store_filter(),
    and the module-level fee/shipping/monthly breakdowns in etsy_dashboard.py.

    Args:
        data: The full or store-filtered DATA DataFrame.
        store: Which store this is for ("all", "keycomponentmfg", "aurvio", "lunalinks").
        config: The CONFIG dict (needed for listing_aliases).

    Returns:
        An EtsyState snapshot with all derived values.
    """
    # -- Filtered DataFrames --------------------------------------------------
    sales_df = data[data["Type"] == "Sale"]
    fee_df = data[data["Type"] == "Fee"]
    ship_df = data[data["Type"] == "Shipping"]
    mkt_df = data[data["Type"] == "Marketing"]
    refund_df = data[data["Type"] == "Refund"]
    tax_df = data[data["Type"] == "Tax"]
    deposit_df = data[data["Type"] == "Deposit"]
    buyer_fee_df = data[data["Type"] == "Buyer Fee"]
    payment_df = data[data["Type"] == "Payment"]

    # -- Deposit totals (parsed from Title text) ------------------------------
    _deposit_rows = deposit_df
    _etsy_deposit_total = 0.0
    for _, _dr in _deposit_rows.iterrows():
        _m = re.search(r'([\d,]+\.\d+)', str(_dr.get("Title", "")))
        if _m:
            _etsy_deposit_total += float(_m.group(1).replace(",", ""))

    # -- Scalar metrics (from _apply_store_filter) ----------------------------
    gross_sales = sales_df["Net_Clean"].sum() if len(sales_df) else 0.0
    total_refunds = abs(refund_df["Net_Clean"].sum()) if len(refund_df) else 0.0
    net_sales = gross_sales - total_refunds
    total_fees = abs(fee_df["Net_Clean"].sum()) if len(fee_df) else 0.0
    total_shipping_cost = abs(ship_df["Net_Clean"].sum()) if len(ship_df) else 0.0
    total_marketing = abs(mkt_df["Net_Clean"].sum()) if len(mkt_df) else 0.0
    total_taxes = abs(tax_df["Net_Clean"].sum()) if len(tax_df) else 0.0
    total_payments = payment_df["Net_Clean"].sum() if len(payment_df) else 0.0
    total_buyer_fees = abs(buyer_fee_df["Net_Clean"].sum()) if len(buyer_fee_df) else 0.0
    order_count = len(sales_df)
    avg_order = gross_sales / order_count if order_count else 0.0

    # -- Product performance --------------------------------------------------
    _listing_aliases = config.get("listing_aliases", {})
    prod_fees = fee_df[
        fee_df["Title"].str.startswith("Transaction fee:", na=False)
        & ~fee_df["Title"].str.contains("Shipping", na=False)
    ].copy()
    prod_fees["Product"] = prod_fees["Title"].str.replace(
        "Transaction fee: ", "", regex=False
    ).apply(lambda n: _normalize_product_name(n, aliases=_listing_aliases))
    product_fee_totals = (
        prod_fees.groupby("Product")["Net_Clean"].sum().abs().sort_values(ascending=False)
    )

    _order_to_product = (
        prod_fees.dropna(subset=["Info"])
        .drop_duplicates(subset=["Info"])
        .set_index("Info")["Product"]
    )
    _sales_with_product = sales_df.copy()
    _sales_with_product["Product"] = (
        _sales_with_product["Title"]
        .str.extract(r"(Order #\d+)", expand=False)
        .map(_order_to_product)
    )
    _sales_with_product = _sales_with_product.dropna(subset=["Product"])
    _sales_with_product["Product"] = _merge_product_prefixes(
        _sales_with_product["Product"], aliases=_listing_aliases
    )
    if len(_sales_with_product) > 0:
        product_revenue_est = (
            _sales_with_product.groupby("Product")["Net_Clean"]
            .sum()
            .sort_values(ascending=False)
            .round(2)
        )
    else:
        product_revenue_est = pd.Series(dtype=float)

    # -- Monthly breakdown ----------------------------------------------------
    months_sorted = sorted(data["Month"].dropna().unique()) if len(data) else []

    def monthly_sum(type_name: str) -> pd.Series:
        return data[data["Type"] == type_name].groupby("Month")["Net_Clean"].sum()

    monthly_sales = monthly_sum("Sale")
    monthly_fees = monthly_sum("Fee").abs()
    monthly_shipping = monthly_sum("Shipping").abs()
    monthly_marketing = monthly_sum("Marketing").abs()
    monthly_refunds = monthly_sum("Refund").abs()
    monthly_taxes = monthly_sum("Tax").abs()

    monthly_raw_fees = data[data["Type"] == "Fee"].groupby("Month")["Net_Clean"].sum() if len(data) else pd.Series(dtype=float)
    monthly_raw_shipping = data[data["Type"] == "Shipping"].groupby("Month")["Net_Clean"].sum() if len(data) else pd.Series(dtype=float)
    monthly_raw_marketing = data[data["Type"] == "Marketing"].groupby("Month")["Net_Clean"].sum() if len(data) else pd.Series(dtype=float)
    monthly_raw_refunds = data[data["Type"] == "Refund"].groupby("Month")["Net_Clean"].sum() if len(data) else pd.Series(dtype=float)
    monthly_raw_taxes = data[data["Type"] == "Tax"].groupby("Month")["Net_Clean"].sum() if len(data) else pd.Series(dtype=float)
    monthly_raw_buyer_fees = data[data["Type"] == "Buyer Fee"].groupby("Month")["Net_Clean"].sum() if len(data) else pd.Series(dtype=float)
    monthly_raw_payments = data[data["Type"] == "Payment"].groupby("Month")["Net_Clean"].sum() if len(data) else pd.Series(dtype=float)

    monthly_net_revenue: dict = {}
    for m in months_sorted:
        monthly_net_revenue[m] = (
            monthly_sales.get(m, 0)
            + monthly_raw_fees.get(m, 0)
            + monthly_raw_shipping.get(m, 0)
            + monthly_raw_marketing.get(m, 0)
            + monthly_raw_refunds.get(m, 0)
            + monthly_raw_taxes.get(m, 0)
            + monthly_raw_buyer_fees.get(m, 0)
            + monthly_raw_payments.get(m, 0)
        )

    # -- Daily aggregations ---------------------------------------------------
    if len(sales_df) and sales_df["Date_Parsed"].notna().any():
        daily_sales = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
        daily_orders = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].count()
    else:
        daily_sales = pd.Series(dtype=float)
        daily_orders = pd.Series(dtype=float)

    daily_fee_cost = fee_df.groupby(fee_df["Date_Parsed"].dt.date)["Net_Clean"].sum() if len(fee_df) else pd.Series(dtype=float)
    daily_ship_cost = ship_df.groupby(ship_df["Date_Parsed"].dt.date)["Net_Clean"].sum() if len(ship_df) else pd.Series(dtype=float)
    daily_mkt_cost = mkt_df.groupby(mkt_df["Date_Parsed"].dt.date)["Net_Clean"].sum() if len(mkt_df) else pd.Series(dtype=float)
    daily_refund_cost = refund_df.groupby(refund_df["Date_Parsed"].dt.date)["Net_Clean"].sum() if len(refund_df) else pd.Series(dtype=float)
    daily_buyer_fee = buyer_fee_df.groupby(buyer_fee_df["Date_Parsed"].dt.date)["Net_Clean"].sum() if len(buyer_fee_df) else pd.Series(dtype=float)
    daily_tax = tax_df.groupby(tax_df["Date_Parsed"].dt.date)["Net_Clean"].sum() if len(tax_df) else pd.Series(dtype=float)
    daily_payment = payment_df.groupby(payment_df["Date_Parsed"].dt.date)["Net_Clean"].sum() if len(payment_df) else pd.Series(dtype=float)

    all_dates = sorted(set(daily_sales.index) | set(daily_fee_cost.index) | set(daily_ship_cost.index))
    daily_df = pd.DataFrame(index=all_dates)
    daily_df["revenue"] = pd.Series(daily_sales)
    daily_df["fees"] = pd.Series(daily_fee_cost)
    daily_df["shipping"] = pd.Series(daily_ship_cost)
    daily_df["marketing"] = pd.Series(daily_mkt_cost)
    daily_df["refunds"] = pd.Series(daily_refund_cost)
    daily_df["buyer_fees"] = pd.Series(daily_buyer_fee)
    daily_df["taxes"] = pd.Series(daily_tax)
    daily_df["payments"] = pd.Series(daily_payment)
    daily_df["orders"] = pd.Series(daily_orders)
    daily_df = daily_df.fillna(0)
    daily_df["profit"] = (
        daily_df["revenue"] + daily_df["fees"] + daily_df["shipping"]
        + daily_df["marketing"] + daily_df["refunds"]
        + daily_df["buyer_fees"] + daily_df["taxes"] + daily_df["payments"]
    )
    daily_df["cum_revenue"] = daily_df["revenue"].cumsum()
    daily_df["cum_profit"] = daily_df["profit"].cumsum()

    # -- Weekly AOV -----------------------------------------------------------
    if len(sales_df) and sales_df["Date_Parsed"].notna().any():
        weekly_sales_df = sales_df.copy()
        weekly_sales_df["WeekStart"] = (
            weekly_sales_df["Date_Parsed"]
            .dt.to_period("W")
            .apply(lambda p: p.start_time)
        )
        weekly_aov = weekly_sales_df.groupby("WeekStart").agg(
            total=("Net_Clean", "sum"),
            count=("Net_Clean", "count"),
        )
        weekly_aov["aov"] = weekly_aov["total"] / weekly_aov["count"]
    else:
        weekly_aov = pd.DataFrame(columns=["total", "count", "aov"])

    # -- Monthly order counts and AOV -----------------------------------------
    monthly_order_counts = sales_df.groupby("Month")["Net_Clean"].count() if len(sales_df) else pd.Series(dtype=int)
    monthly_aov: dict = {}
    monthly_profit_per_order: dict = {}
    for m in months_sorted:
        oc = monthly_order_counts.get(m, 0)
        if oc > 0:
            monthly_aov[m] = monthly_sales.get(m, 0) / oc
            monthly_profit_per_order[m] = monthly_net_revenue.get(m, 0) / oc
        else:
            monthly_aov[m] = 0
            monthly_profit_per_order[m] = 0

    # -- Days active ----------------------------------------------------------
    if len(data) > 0 and data["Date_Parsed"].notna().any():
        days_active = max(
            (data["Date_Parsed"].max() - data["Date_Parsed"].min()).days + 1, 1
        )
    else:
        days_active = 1

    # -- Fee breakdown --------------------------------------------------------
    listing_fees = abs(
        fee_df[fee_df["Title"].str.contains("Listing fee", na=False)]["Net_Clean"].sum()
    )
    transaction_fees_product = abs(
        fee_df[
            fee_df["Title"].str.startswith("Transaction fee:", na=False)
            & ~fee_df["Title"].str.contains("Shipping", na=False)
        ]["Net_Clean"].sum()
    )
    transaction_fees_shipping = abs(
        fee_df[fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)]["Net_Clean"].sum()
    )
    processing_fees = abs(
        fee_df[fee_df["Title"].str.contains("Processing fee", na=False)]["Net_Clean"].sum()
    )
    credit_transaction = fee_df[
        fee_df["Title"].str.startswith("Credit for transaction fee", na=False)
    ]["Net_Clean"].sum()
    credit_listing = fee_df[
        fee_df["Title"].str.startswith("Credit for listing fee", na=False)
    ]["Net_Clean"].sum()
    credit_processing = fee_df[
        fee_df["Title"].str.startswith("Credit for processing fee", na=False)
    ]["Net_Clean"].sum()
    share_save = fee_df[
        fee_df["Title"].str.contains("Share & Save", na=False)
    ]["Net_Clean"].sum()
    total_credits = credit_transaction + credit_listing + credit_processing + share_save
    total_fees_gross = (
        listing_fees + transaction_fees_product
        + transaction_fees_shipping + processing_fees
    )

    # -- Marketing breakdown --------------------------------------------------
    etsy_ads = abs(
        mkt_df[mkt_df["Title"].str.contains("Etsy Ads", na=False)]["Net_Clean"].sum()
    )
    offsite_ads_fees = abs(
        mkt_df[
            mkt_df["Title"].str.contains("Offsite Ads", na=False)
            & ~mkt_df["Title"].str.contains("Credit", na=False)
        ]["Net_Clean"].sum()
    )
    offsite_ads_credits = mkt_df[
        mkt_df["Title"].str.contains("Credit for Offsite", na=False)
    ]["Net_Clean"].sum()

    # -- Shipping breakdown ---------------------------------------------------
    usps_outbound = abs(
        ship_df[ship_df["Title"] == "USPS shipping label"]["Net_Clean"].sum()
    )
    usps_outbound_count = len(ship_df[ship_df["Title"] == "USPS shipping label"])
    usps_return = abs(
        ship_df[ship_df["Title"] == "USPS return shipping label"]["Net_Clean"].sum()
    )
    usps_return_count = len(ship_df[ship_df["Title"] == "USPS return shipping label"])
    asendia_labels = abs(
        ship_df[ship_df["Title"].str.contains("Asendia", na=False)]["Net_Clean"].sum()
    )
    asendia_count = len(ship_df[ship_df["Title"].str.contains("Asendia", na=False)])
    ship_adjustments = abs(
        ship_df[ship_df["Title"].str.contains("Adjustment", na=False)]["Net_Clean"].sum()
    )
    ship_adjust_count = len(
        ship_df[ship_df["Title"].str.contains("Adjustment", na=False)]
    )
    ship_credits = ship_df[
        ship_df["Title"].str.contains("Credit for", na=False)
    ]["Net_Clean"].sum()
    ship_credit_count = len(
        ship_df[ship_df["Title"].str.contains("Credit for", na=False)]
    )
    ship_insurance = abs(
        ship_df[ship_df["Title"].str.contains("insurance", case=False, na=False)]["Net_Clean"].sum()
    )
    ship_insurance_count = len(
        ship_df[ship_df["Title"].str.contains("insurance", case=False, na=False)]
    )

    # Buyer paid shipping: UNKNOWN — /0.065 back-solve REMOVED
    buyer_paid_shipping = None
    shipping_profit = None
    shipping_margin = None

    # Paid vs free shipping orders (counts are still real)
    ship_fee_rows = fee_df[
        fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)
    ].copy()
    orders_with_paid_shipping = set(ship_fee_rows["Info"].dropna())
    all_order_ids = set(
        sales_df["Title"].str.extract(r"(Order #\d+)", expand=False).dropna()
    )
    orders_free_shipping = all_order_ids - orders_with_paid_shipping
    paid_ship_count = len(orders_with_paid_shipping & all_order_ids)
    free_ship_count = len(orders_free_shipping)
    avg_outbound_label = usps_outbound / usps_outbound_count if usps_outbound_count else 0.0

    # -- Build the state object -----------------------------------------------
    return EtsyState(
        store=store,
        data=data,
        # Filtered DataFrames
        sales_df=sales_df,
        fee_df=fee_df,
        ship_df=ship_df,
        mkt_df=mkt_df,
        refund_df=refund_df,
        tax_df=tax_df,
        deposit_df=deposit_df,
        buyer_fee_df=buyer_fee_df,
        payment_df=payment_df,
        # Scalar metrics
        gross_sales=gross_sales,
        total_refunds=total_refunds,
        net_sales=net_sales,
        total_fees=total_fees,
        total_shipping_cost=total_shipping_cost,
        total_marketing=total_marketing,
        total_taxes=total_taxes,
        total_payments=total_payments,
        total_buyer_fees=total_buyer_fees,
        order_count=order_count,
        avg_order=avg_order,
        # Monthly aggregations
        months_sorted=months_sorted,
        monthly_sales=monthly_sales,
        monthly_fees=monthly_fees,
        monthly_shipping=monthly_shipping,
        monthly_marketing=monthly_marketing,
        monthly_refunds=monthly_refunds,
        monthly_taxes=monthly_taxes,
        monthly_raw_fees=monthly_raw_fees,
        monthly_raw_shipping=monthly_raw_shipping,
        monthly_raw_marketing=monthly_raw_marketing,
        monthly_raw_refunds=monthly_raw_refunds,
        monthly_raw_taxes=monthly_raw_taxes,
        monthly_raw_buyer_fees=monthly_raw_buyer_fees,
        monthly_raw_payments=monthly_raw_payments,
        monthly_net_revenue=monthly_net_revenue,
        monthly_order_counts=monthly_order_counts,
        monthly_aov=monthly_aov,
        monthly_profit_per_order=monthly_profit_per_order,
        days_active=days_active,
        # Daily aggregations
        daily_sales=daily_sales,
        daily_orders=daily_orders,
        daily_df=daily_df,
        weekly_aov=weekly_aov,
        # Fee breakdown
        listing_fees=listing_fees,
        transaction_fees_product=transaction_fees_product,
        transaction_fees_shipping=transaction_fees_shipping,
        processing_fees=processing_fees,
        credit_transaction=credit_transaction,
        credit_listing=credit_listing,
        credit_processing=credit_processing,
        share_save=share_save,
        total_credits=total_credits,
        total_fees_gross=total_fees_gross,
        # Marketing breakdown
        etsy_ads=etsy_ads,
        offsite_ads_fees=offsite_ads_fees,
        offsite_ads_credits=offsite_ads_credits,
        # Shipping breakdown
        usps_outbound=usps_outbound,
        usps_outbound_count=usps_outbound_count,
        usps_return=usps_return,
        usps_return_count=usps_return_count,
        asendia_labels=asendia_labels,
        asendia_count=asendia_count,
        ship_adjustments=ship_adjustments,
        ship_adjust_count=ship_adjust_count,
        ship_credits=ship_credits,
        ship_credit_count=ship_credit_count,
        ship_insurance=ship_insurance,
        ship_insurance_count=ship_insurance_count,
        buyer_paid_shipping=buyer_paid_shipping,
        shipping_profit=shipping_profit,
        shipping_margin=shipping_margin,
        paid_ship_count=paid_ship_count,
        free_ship_count=free_ship_count,
        avg_outbound_label=avg_outbound_label,
        # Product performance
        product_fee_totals=product_fee_totals,
        product_revenue_est=product_revenue_est,
        # Deposit tracking
        _etsy_deposit_total=_etsy_deposit_total,
        _deposit_rows=_deposit_rows,
    )


# ---------------------------------------------------------------------------
# build_etsy_state_from_api_ledger — compute state from API-verified orders
# ---------------------------------------------------------------------------

def build_etsy_state_from_api_ledger(
    ledger_orders: list,
    config: dict,
    statement_data: "pd.DataFrame | None" = None,
    labels_data: list | None = None,
) -> EtsyState:
    """Build an EtsyState from API-verified per-order data.

    Produces the exact same shape as build_etsy_state() so all downstream
    code (charts, tabs, KPIs) works unchanged. Uses the verified per-order
    data as the source of truth instead of statement CSVs.

    Args:
        ledger_orders: list of order dicts from order_profit_ledger_keycomponentmfg
        config: CONFIG dict
        statement_data: optional statement DataFrame for deposit rows only
        labels_data: optional list of label dicts for shipping breakdown
    """
    from datetime import datetime as _dt

    # Include ALL orders — canceled ones may have real costs (user confirmed some
    # "canceled" orders were actually refunds with payments and sunk label costs)
    active = list(ledger_orders)

    # --- Build a synthetic DataFrame for compatibility ---
    rows = []
    for o in active:
        sale_date_str = o.get("Sale Date", "")
        try:
            dt = _dt.strptime(sale_date_str, "%m/%d/%Y")
        except (ValueError, TypeError):
            dt = None

        month = dt.strftime("%Y-%m") if dt else None
        sale_price = o.get("Sale Price", 0) or 0
        buyer_ship = o.get("Buyer Shipping", 0) or 0
        revenue = sale_price + buyer_ship

        rows.append({
            "Date_Parsed": dt,
            "Month": month,
            "Type": "Sale",
            "Title": f"Payment for Order #{o.get('Order ID', '')}",
            "Info": f"Order #{o.get('Order ID', '')}",
            "Net_Clean": revenue,
            "Store": o.get("_store", "keycomponentmfg"),
            "Amount_Clean": revenue,
            "Fees_Clean": 0,
            # Extra fields for product performance
            "_item_names": o.get("Item Names", ""),
            "_sale_price": sale_price,
            "_buyer_ship": buyer_ship,
            "_txn_fee": o.get("Transaction Fee", 0) or 0,
            "_proc_fee": o.get("Processing Fee", 0) or 0,
            "_listing_fee": o.get("Listing Fee", 0) or 0,
            "_ads": o.get("Offsite Ads", 0) or 0,
            "_label": o.get("Shipping Label", 0) or 0,
            "_refund": o.get("Refund", 0) or 0,
            "_true_net": o.get("True Net", 0) or 0,
            "_ship_pl": o.get("Ship P/L", 0) or 0,
            "_ship_country": o.get("Ship Country", "") or "",
        })

    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if len(df) == 0:
        df["Date_Parsed"] = pd.Series(dtype="datetime64[ns]")

    # --- Scalar metrics ---
    gross_sales = sum(r["Net_Clean"] for r in rows)
    total_refunds = sum(o.get("Refund", 0) or 0 for o in active)
    net_sales = gross_sales - total_refunds

    _txn_fees = sum(o.get("Transaction Fee", 0) or 0 for o in active)
    _proc_fees = sum(o.get("Processing Fee", 0) or 0 for o in active)
    _listing_fees_total = sum(o.get("Listing Fee", 0) or 0 for o in active)
    _ads_total = sum(o.get("Offsite Ads", 0) or 0 for o in active)
    # total_fees = txn + proc (NOT ads — ads go in total_marketing to avoid double-counting)
    total_fees = _txn_fees + _proc_fees
    total_shipping_cost = sum(o.get("Shipping Label", 0) or 0 for o in active)
    total_marketing = _ads_total
    # Sales tax is pass-through (collected from buyer, remitted by Etsy) — NOT a cost
    total_taxes = 0.0
    total_payments = 0.0
    total_buyer_fees = 0.0
    order_count = len(ledger_orders)
    avg_order = gross_sales / order_count if order_count else 0.0

    # --- Monthly aggregations ---
    months_sorted = sorted(set(r["Month"] for r in rows if r["Month"])) if rows else []

    def _monthly_sum(field):
        """Sum a field by month from the rows list."""
        result = {}
        for r in rows:
            m = r.get("Month")
            if m:
                result[m] = result.get(m, 0) + (r.get(field, 0) or 0)
        return pd.Series(result)

    monthly_sales = _monthly_sum("Net_Clean")

    # Build monthly fee/shipping/marketing/refund from per-order data
    _monthly_fees_dict = {}
    _monthly_shipping_dict = {}
    _monthly_marketing_dict = {}
    _monthly_refunds_dict = {}
    _monthly_taxes_dict = {}
    _monthly_order_counts_dict = {}

    for r in rows:
        m = r.get("Month")
        if not m:
            continue
        _monthly_fees_dict[m] = _monthly_fees_dict.get(m, 0) + r["_txn_fee"] + r["_proc_fee"]  # ads in marketing, not fees
        _monthly_shipping_dict[m] = _monthly_shipping_dict.get(m, 0) + r["_label"]
        _monthly_marketing_dict[m] = _monthly_marketing_dict.get(m, 0) + r["_ads"]
        _monthly_refunds_dict[m] = _monthly_refunds_dict.get(m, 0) + r["_refund"]
        _monthly_taxes_dict[m] = _monthly_taxes_dict.get(m, 0) + 0  # taxes are pass-through
        _monthly_order_counts_dict[m] = _monthly_order_counts_dict.get(m, 0) + 1

    monthly_fees = pd.Series(_monthly_fees_dict)
    monthly_shipping = pd.Series(_monthly_shipping_dict)
    monthly_marketing = pd.Series(_monthly_marketing_dict)
    monthly_refunds = pd.Series(_monthly_refunds_dict)
    monthly_taxes = pd.Series(_monthly_taxes_dict) if _monthly_taxes_dict else pd.Series(dtype=float)

    # Raw monthly (negative values for fees/shipping/refunds — used for net revenue calc)
    monthly_raw_fees = -monthly_fees
    monthly_raw_shipping = -monthly_shipping
    monthly_raw_marketing = -monthly_marketing
    monthly_raw_refunds = -monthly_refunds
    monthly_raw_taxes = pd.Series({m: 0 for m in months_sorted}, dtype=float)
    monthly_raw_buyer_fees = pd.Series({m: 0 for m in months_sorted}, dtype=float)
    monthly_raw_payments = pd.Series({m: 0 for m in months_sorted}, dtype=float)

    monthly_order_counts = pd.Series(_monthly_order_counts_dict)

    monthly_net_revenue = {}
    for m in months_sorted:
        monthly_net_revenue[m] = (
            monthly_sales.get(m, 0)
            + monthly_raw_fees.get(m, 0)
            + monthly_raw_shipping.get(m, 0)
            + monthly_raw_marketing.get(m, 0)
            + monthly_raw_refunds.get(m, 0)
        )

    monthly_aov = {}
    monthly_profit_per_order = {}
    for m in months_sorted:
        oc = monthly_order_counts.get(m, 0)
        if oc > 0:
            monthly_aov[m] = monthly_sales.get(m, 0) / oc
            monthly_profit_per_order[m] = monthly_net_revenue.get(m, 0) / oc
        else:
            monthly_aov[m] = 0
            monthly_profit_per_order[m] = 0

    # --- Daily aggregations ---
    if len(df) and df["Date_Parsed"].notna().any():
        daily_sales = df.groupby(df["Date_Parsed"].dt.date)["Net_Clean"].sum()
        daily_orders = df.groupby(df["Date_Parsed"].dt.date)["Net_Clean"].count()
    else:
        daily_sales = pd.Series(dtype=float)
        daily_orders = pd.Series(dtype=float)

    # Build daily cost series from per-order data
    _daily_fees = {}
    _daily_shipping = {}
    _daily_marketing = {}
    _daily_refunds = {}
    for r in rows:
        dt = r.get("Date_Parsed")
        if dt is None:
            continue
        d = dt.date()
        _daily_fees[d] = _daily_fees.get(d, 0) - (r["_txn_fee"] + r["_proc_fee"])  # ads in marketing, not fees
        _daily_shipping[d] = _daily_shipping.get(d, 0) - r["_label"]
        _daily_marketing[d] = _daily_marketing.get(d, 0) - r["_ads"]
        _daily_refunds[d] = _daily_refunds.get(d, 0) - r["_refund"]

    all_dates = sorted(set(daily_sales.index) | set(_daily_fees.keys()))
    daily_df = pd.DataFrame(index=all_dates)
    daily_df["revenue"] = pd.Series(daily_sales)
    daily_df["fees"] = pd.Series(_daily_fees)
    daily_df["shipping"] = pd.Series(_daily_shipping)
    daily_df["marketing"] = pd.Series(_daily_marketing)
    daily_df["refunds"] = pd.Series(_daily_refunds)
    daily_df["buyer_fees"] = 0
    daily_df["taxes"] = 0
    daily_df["payments"] = 0
    daily_df["orders"] = pd.Series(daily_orders)
    daily_df = daily_df.fillna(0)
    daily_df["profit"] = (
        daily_df["revenue"] + daily_df["fees"] + daily_df["shipping"]
        + daily_df["marketing"] + daily_df["refunds"]
    )
    daily_df["cum_revenue"] = daily_df["revenue"].cumsum()
    daily_df["cum_profit"] = daily_df["profit"].cumsum()

    # Weekly AOV
    if len(df) and df["Date_Parsed"].notna().any():
        _wdf = df.copy()
        _wdf["WeekStart"] = _wdf["Date_Parsed"].dt.to_period("W").apply(lambda p: p.start_time)
        weekly_aov = _wdf.groupby("WeekStart").agg(
            total=("Net_Clean", "sum"), count=("Net_Clean", "count"),
        )
        weekly_aov["aov"] = weekly_aov["total"] / weekly_aov["count"]
    else:
        weekly_aov = pd.DataFrame(columns=["total", "count", "aov"])

    # Days active
    if len(df) and df["Date_Parsed"].notna().any():
        days_active = max((df["Date_Parsed"].max() - df["Date_Parsed"].min()).days + 1, 1)
    else:
        days_active = 1

    # --- Fee breakdown ---
    listing_fees = _listing_fees_total
    transaction_fees_product = _txn_fees
    transaction_fees_shipping = 0  # included in transaction_fees_product for API
    processing_fees = _proc_fees
    credit_transaction = 0  # API fees are net of credits
    credit_listing = 0
    credit_processing = 0
    share_save = 0
    total_credits = 0
    total_fees_gross = _txn_fees + _proc_fees + _listing_fees_total

    # --- Marketing breakdown ---
    etsy_ads = 0  # Etsy Ads (CPC) not in API ledger, only in statement
    offsite_ads_fees = _ads_total
    offsite_ads_credits = 0

    # --- Shipping breakdown (from labels data) ---
    _label_list = labels_data or []
    _assigned_labels = [lb for lb in _label_list if lb.get("assigned_to")]

    # Count by type
    _outbound_labels = [lb for lb in _assigned_labels if lb.get("type") == "shipping_labels"]
    _return_labels = [lb for lb in _assigned_labels if lb.get("type") == "shipping_labels_usps_return"]
    _adj_labels = [lb for lb in _assigned_labels if "adjustment" in (lb.get("type", "") or "") and "credit" not in (lb.get("type", "") or "")]
    _credit_labels = [lb for lb in _assigned_labels if "credit" in (lb.get("type", "") or "") or "refund" in (lb.get("type", "") or "")]
    _insurance_labels = [lb for lb in _assigned_labels if "insurance" in (lb.get("type", "") or "")]

    # Count international by ship country
    _intl_orders = [o for o in active if o.get("Ship Country", "") not in ("US", "", None)]
    _intl_label_cost = sum(o.get("Shipping Label", 0) or 0 for o in _intl_orders)

    # Outbound = total label cost minus returns/adjustments/credits/insurance
    _all_label_cost = sum(o.get("Shipping Label", 0) or 0 for o in active)
    _return_cost = sum(lb.get("amount", 0) for lb in _return_labels)
    _adj_cost = sum(lb.get("amount", 0) for lb in _adj_labels)
    _credit_amt = sum(lb.get("amount", 0) for lb in _credit_labels)
    _insurance_cost = sum(lb.get("amount", 0) for lb in _insurance_labels)

    usps_outbound = _all_label_cost - _return_cost - _adj_cost + _credit_amt - _insurance_cost
    usps_outbound_count = len([o for o in active if o.get("Label ID")])
    usps_return = _return_cost
    usps_return_count = len(_return_labels)
    asendia_labels = _intl_label_cost
    asendia_count = len(_intl_orders)
    ship_adjustments = _adj_cost
    ship_adjust_count = len(_adj_labels)
    ship_credits_val = _credit_amt
    ship_credit_count = len(_credit_labels)
    ship_insurance = _insurance_cost
    ship_insurance_count = len(_insurance_labels)

    # Buyer paid shipping (API has this!)
    _buyer_ship_total = sum(o.get("Buyer Shipping", 0) or 0 for o in active)
    _ship_pl_total = sum(o.get("Ship P/L", 0) or 0 for o in active)
    buyer_paid_shipping = _buyer_ship_total
    shipping_profit = _ship_pl_total
    shipping_margin = round(_ship_pl_total / _all_label_cost * 100, 1) if _all_label_cost > 0 else 0

    paid_ship_count = len([o for o in active if (o.get("Buyer Shipping", 0) or 0) > 0])
    free_ship_count = len([o for o in active if (o.get("Buyer Shipping", 0) or 0) == 0])
    avg_outbound_label = round(_all_label_cost / usps_outbound_count, 2) if usps_outbound_count else 0

    # --- Product performance ---
    _listing_aliases = config.get("listing_aliases", {})
    _product_rev = {}
    _product_fees = {}
    for o in active:
        item = o.get("Item Names", "") or "Unknown"
        # Use first item name, normalize
        first_item = item.split(" | ")[0].strip()
        name = _normalize_product_name(first_item, aliases=_listing_aliases)
        sale = (o.get("Sale Price", 0) or 0) + (o.get("Buyer Shipping", 0) or 0)
        fee = (o.get("Transaction Fee", 0) or 0)
        _product_rev[name] = _product_rev.get(name, 0) + sale
        _product_fees[name] = _product_fees.get(name, 0) + fee

    product_revenue_est = pd.Series(_product_rev).sort_values(ascending=False).round(2) if _product_rev else pd.Series(dtype=float)
    product_fee_totals = pd.Series(_product_fees).abs().sort_values(ascending=False) if _product_fees else pd.Series(dtype=float)

    # --- Deposits (from statement data — not per-order) ---
    import re as _re
    _etsy_deposit_total = 0.0
    _deposit_rows_df = pd.DataFrame()
    if statement_data is not None and len(statement_data) > 0:
        _kc_stmt = statement_data[statement_data["Store"] == "keycomponentmfg"] if "Store" in statement_data.columns else statement_data
        _deposit_rows_df = _kc_stmt[_kc_stmt["Type"] == "Deposit"] if "Type" in _kc_stmt.columns else pd.DataFrame()
        for _, _dr in _deposit_rows_df.iterrows():
            _m = _re.search(r'([\d,]+\.\d+)', str(_dr.get("Title", "")))
            if _m:
                _etsy_deposit_total += float(_m.group(1).replace(",", ""))

    # --- Build synthetic DataFrames for downstream compat ---
    # These satisfy code that iterates refund_df, ship_df, etc.
    sales_df = df[["Date_Parsed", "Month", "Type", "Title", "Info", "Net_Clean", "Store"]].copy() if len(df) else pd.DataFrame(columns=["Date_Parsed", "Month", "Type", "Title", "Info", "Net_Clean", "Store"])

    # Fee rows: one per order for each fee type
    _fee_rows = []
    for o in active:
        dt_str = o.get("Sale Date", "")
        try:
            dt = _dt.strptime(dt_str, "%m/%d/%Y")
        except (ValueError, TypeError):
            dt = None
        m = dt.strftime("%Y-%m") if dt else None
        oid = o.get("Order ID", "")
        if o.get("Transaction Fee", 0):
            _fee_rows.append({"Date_Parsed": dt, "Month": m, "Type": "Fee", "Title": f"Transaction fee: Order #{oid}", "Info": f"Order #{oid}", "Net_Clean": -(o.get("Transaction Fee", 0) or 0), "Store": "keycomponentmfg"})
        if o.get("Processing Fee", 0):
            _fee_rows.append({"Date_Parsed": dt, "Month": m, "Type": "Fee", "Title": f"Processing fee", "Info": f"Order #{oid}", "Net_Clean": -(o.get("Processing Fee", 0) or 0), "Store": "keycomponentmfg"})
    fee_df = pd.DataFrame(_fee_rows) if _fee_rows else pd.DataFrame(columns=["Date_Parsed", "Month", "Type", "Title", "Info", "Net_Clean", "Store"])

    # Ship rows
    _ship_rows = []
    for o in active:
        if not o.get("Shipping Label"):
            continue
        dt_str = o.get("Sale Date", "")
        try:
            dt = _dt.strptime(dt_str, "%m/%d/%Y")
        except (ValueError, TypeError):
            dt = None
        _is_intl = o.get("Ship Country", "") not in ("US", "", None)
        _title = "Asendia shipping label" if _is_intl else "USPS shipping label"
        _ship_rows.append({"Date_Parsed": dt, "Month": dt.strftime("%Y-%m") if dt else None, "Type": "Shipping", "Title": _title, "Info": f"Label #{o.get('Label ID', '')}", "Net_Clean": -(o.get("Shipping Label", 0) or 0), "Store": "keycomponentmfg"})
    ship_df = pd.DataFrame(_ship_rows) if _ship_rows else pd.DataFrame(columns=["Date_Parsed", "Month", "Type", "Title", "Info", "Net_Clean", "Store"])

    # Refund rows
    _ref_rows = []
    for o in active:
        if not o.get("Refund"):
            continue
        dt_str = o.get("Sale Date", "")
        try:
            dt = _dt.strptime(dt_str, "%m/%d/%Y")
        except (ValueError, TypeError):
            dt = None
        _ref_rows.append({"Date_Parsed": dt, "Month": dt.strftime("%Y-%m") if dt else None, "Type": "Refund", "Title": f"Refund Order #{o.get('Order ID', '')}", "Info": f"Order #{o.get('Order ID', '')}", "Net_Clean": -(o.get("Refund", 0) or 0), "Store": "keycomponentmfg"})
    refund_df = pd.DataFrame(_ref_rows) if _ref_rows else pd.DataFrame(columns=["Date_Parsed", "Month", "Type", "Title", "Info", "Net_Clean", "Store"])

    # Marketing rows
    _mkt_rows = []
    for o in active:
        if not o.get("Offsite Ads"):
            continue
        dt_str = o.get("Sale Date", "")
        try:
            dt = _dt.strptime(dt_str, "%m/%d/%Y")
        except (ValueError, TypeError):
            dt = None
        _mkt_rows.append({"Date_Parsed": dt, "Month": dt.strftime("%Y-%m") if dt else None, "Type": "Marketing", "Title": f"Offsite Ads fee", "Info": f"Order #{o.get('Order ID', '')}", "Net_Clean": -(o.get("Offsite Ads", 0) or 0), "Store": "keycomponentmfg"})
    mkt_df = pd.DataFrame(_mkt_rows) if _mkt_rows else pd.DataFrame(columns=["Date_Parsed", "Month", "Type", "Title", "Info", "Net_Clean", "Store"])

    # Empty DFs for types not in API
    _empty_df = pd.DataFrame(columns=["Date_Parsed", "Month", "Type", "Title", "Info", "Net_Clean", "Store"])
    tax_df = _empty_df.copy()
    buyer_fee_df = _empty_df.copy()
    payment_df = _empty_df.copy()

    return EtsyState(
        store="keycomponentmfg",
        data=df,
        sales_df=sales_df, fee_df=fee_df, ship_df=ship_df, mkt_df=mkt_df,
        refund_df=refund_df, tax_df=tax_df, deposit_df=_deposit_rows_df,
        buyer_fee_df=buyer_fee_df, payment_df=payment_df,
        gross_sales=gross_sales, total_refunds=total_refunds, net_sales=net_sales,
        total_fees=total_fees, total_shipping_cost=total_shipping_cost,
        total_marketing=total_marketing, total_taxes=total_taxes,
        total_payments=total_payments, total_buyer_fees=total_buyer_fees,
        order_count=order_count, avg_order=avg_order,
        months_sorted=months_sorted,
        monthly_sales=monthly_sales, monthly_fees=monthly_fees,
        monthly_shipping=monthly_shipping, monthly_marketing=monthly_marketing,
        monthly_refunds=monthly_refunds, monthly_taxes=monthly_taxes,
        monthly_raw_fees=monthly_raw_fees, monthly_raw_shipping=monthly_raw_shipping,
        monthly_raw_marketing=monthly_raw_marketing, monthly_raw_refunds=monthly_raw_refunds,
        monthly_raw_taxes=monthly_raw_taxes, monthly_raw_buyer_fees=monthly_raw_buyer_fees,
        monthly_raw_payments=monthly_raw_payments,
        monthly_net_revenue=monthly_net_revenue, monthly_order_counts=monthly_order_counts,
        monthly_aov=monthly_aov, monthly_profit_per_order=monthly_profit_per_order,
        days_active=days_active,
        daily_sales=daily_sales, daily_orders=daily_orders,
        daily_df=daily_df, weekly_aov=weekly_aov,
        listing_fees=listing_fees, transaction_fees_product=transaction_fees_product,
        transaction_fees_shipping=transaction_fees_shipping, processing_fees=processing_fees,
        credit_transaction=credit_transaction, credit_listing=credit_listing,
        credit_processing=credit_processing, share_save=share_save,
        total_credits=total_credits, total_fees_gross=total_fees_gross,
        etsy_ads=etsy_ads, offsite_ads_fees=offsite_ads_fees, offsite_ads_credits=offsite_ads_credits,
        usps_outbound=usps_outbound, usps_outbound_count=usps_outbound_count,
        usps_return=usps_return, usps_return_count=usps_return_count,
        asendia_labels=asendia_labels, asendia_count=asendia_count,
        ship_adjustments=ship_adjustments, ship_adjust_count=ship_adjust_count,
        ship_credits=ship_credits_val, ship_credit_count=ship_credit_count,
        ship_insurance=ship_insurance, ship_insurance_count=ship_insurance_count,
        buyer_paid_shipping=buyer_paid_shipping, shipping_profit=shipping_profit,
        shipping_margin=shipping_margin,
        paid_ship_count=paid_ship_count, free_ship_count=free_ship_count,
        avg_outbound_label=avg_outbound_label,
        product_fee_totals=product_fee_totals, product_revenue_est=product_revenue_est,
        _etsy_deposit_total=_etsy_deposit_total, _deposit_rows=_deposit_rows_df,
    )


# ---------------------------------------------------------------------------
# StateManager — thread-safe manager for dashboard state
# ---------------------------------------------------------------------------

class StateManager:
    """Thread-safe manager for dashboard state.

    Holds the full unfiltered dataset and produces EtsyState snapshots
    for any store filter. The current snapshot is cached and swapped
    atomically when the store filter changes.
    """

    def __init__(self) -> None:
        self._full_data: Optional[pd.DataFrame] = None
        self._config: dict = {}
        self._current_state: Optional[EtsyState] = None
        self._lock = threading.Lock()

    def initialize(self, data: pd.DataFrame, config: dict) -> None:
        """Set the full dataset. Called on startup and after uploads."""
        with self._lock:
            self._full_data = data.copy()
            self._config = config
            self._current_state = build_etsy_state(data, "all", config)
        logger.info("StateManager initialized with %d rows", len(data))

    def set_store_filter(self, store: str) -> EtsyState:
        """Build a new state for the given store filter."""
        with self._lock:
            if store == "all" or not store:
                filtered = self._full_data
            elif "Store" not in self._full_data.columns:
                # Store column not present (single-store data or Supabase-only load)
                filtered = self._full_data
            else:
                filtered = self._full_data[self._full_data["Store"] == store].copy()
            self._current_state = build_etsy_state(filtered, store or "all", self._config)
        logger.debug("Store filter set to '%s' (%d rows)", store, len(filtered))
        return self._current_state

    def get_state(self) -> Optional[EtsyState]:
        """Get the current state snapshot."""
        return self._current_state

    def get_full_data(self) -> Optional[pd.DataFrame]:
        """Get the unfiltered dataset (for Data Hub, etc.)."""
        return self._full_data

    def update_data(self, data: pd.DataFrame, config: dict | None = None) -> None:
        """Update the full dataset after an upload."""
        with self._lock:
            self._full_data = data.copy()
            if config is not None:
                self._config = config
            # Rebuild current state with the same store filter
            store = self._current_state.store if self._current_state else "all"
        # Release lock before calling set_store_filter (which acquires it)
        self.set_store_filter(store)
        logger.info("StateManager updated with %d rows", len(data))


# Module-level singleton
state_manager = StateManager()
