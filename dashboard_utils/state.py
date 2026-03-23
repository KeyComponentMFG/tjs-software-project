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
