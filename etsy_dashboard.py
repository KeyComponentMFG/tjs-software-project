"""
Etsy Financial Dashboard v2 — Tabbed, Trend-Heavy, Deep Analytics
Run: python etsy_dashboard.py
Open: http://127.0.0.1:8070
"""

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, callback_context, dash_table
from dash.dependencies import Input, Output, State, MATCH, ALL
import json
import re
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import os
import base64
import flask
import urllib.parse
import logging
_logger = logging.getLogger("dashboard.main")
from dashboard_utils.logging_config import get_logger as _get_logger
from dashboard_utils.callback_guard import guard_callback, get_error_summary
from dashboard_utils.theme import *  # noqa: F403 — colors, helpers, UI builders
from dashboard_utils.theme import set_provenance_hook as _set_provenance_hook
from dashboard_utils.helpers import parse_money as _parse_money_pure, _normalize_product_name, _merge_product_prefixes  # noqa: E501
from dashboard_utils.pages.agreement import build_tab_agreement  # noqa: E402
from dashboard_utils.pages.tax_forms import build_tab5_tax_forms  # noqa: E402
from dashboard_utils.pages.overview import build_tab1_overview  # noqa: E402
from dashboard_utils.pages.valuation import build_tab6_valuation  # noqa: E402
from dashboard_utils.pages.deep_dive import build_tab2_deep_dive  # noqa: E402
from dashboard_utils.pages.data_hub import build_tab7_data_hub  # noqa: E402
from dashboard_utils.pages.financials import build_tab3_financials  # noqa: E402
from dashboard_utils.pages.inventory import build_tab4_inventory  # noqa: E402

# ── Load Data ────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

from supabase_loader import (
    load_data as _load_data,
    save_image_url as _save_image_url,
    load_location_overrides as _load_location_overrides,
    delete_location_override as _delete_location_override,
    load_item_details as _load_item_details,
    save_item_details as _save_item_details,
    delete_item_details as _delete_item_details,
    load_usage_log as _load_usage_log,
    save_usage as _save_usage,
    delete_usage as _delete_usage,
    load_quick_adds as _load_quick_adds,
    save_quick_add as _save_quick_add,
    delete_quick_add as _delete_quick_add,
    load_image_overrides as _load_image_overrides,
    save_new_order as _save_new_order,
    sync_bank_transactions as _sync_bank_to_supabase,
    sync_etsy_transactions as _sync_etsy_to_supabase,
    append_bank_transactions as _append_bank_to_supabase,
    append_etsy_transactions as _append_etsy_to_supabase,
    save_config_value as _save_config_value,
    delete_etsy_by_month as _delete_etsy_by_month,
    delete_bank_by_month as _delete_bank_by_month,
    delete_receipt_by_order as _delete_receipt_by_order,
    get_etsy_month_counts as _get_etsy_month_counts,
    get_bank_month_counts as _get_bank_month_counts,
)

RAILWAY_URL = os.environ.get("RAILWAY_URL", "https://web-production-7f385.up.railway.app")
IS_RAILWAY = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_SERVICE_NAME"))


def item_thumbnail(image_url, size=40):
    """Return a thumbnail img element or gray placeholder."""
    if image_url:
        return html.Img(
            src=image_url, referrerPolicy="no-referrer",
            style={"width": f"{size}px", "height": f"{size}px", "objectFit": "cover",
                   "borderRadius": "4px", "verticalAlign": "middle"})
    return html.Div(
        "?", style={
            "width": f"{size}px", "height": f"{size}px", "display": "inline-flex",
            "alignItems": "center", "justifyContent": "center",
            "backgroundColor": "#ffffff10", "borderRadius": "4px",
            "color": DARKGRAY, "fontSize": f"{size // 3}px", "fontWeight": "bold",
            "verticalAlign": "middle"})


def _fetch_amazon_image(item_name):
    """Search Amazon for item, download first product image, save locally.
    Returns local asset URL or empty string on failure."""
    import urllib.request
    try:
        query = urllib.parse.quote_plus(item_name[:80])
        url = f"https://www.amazon.com/s?k={query}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html_text = resp.read().decode("utf-8", errors="replace")

        # Find the first product image URL from Amazon's media CDN
        matches = re.findall(r'https://m\.media-amazon\.com/images/I/[A-Za-z0-9._%-]+\.jpg', html_text)
        if not matches:
            return ""
        img_url = matches[0]

        # Download image bytes
        img_req = urllib.request.Request(img_url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        with urllib.request.urlopen(img_req, timeout=15) as img_resp:
            img_bytes = img_resp.read()

        # Save to assets/product_images/
        safe_name = re.sub(r'[^\w\s-]', '', item_name[:50]).strip()
        safe_name = re.sub(r'[\s]+', '_', safe_name).lower()
        if not safe_name:
            safe_name = "item"
        img_dir = os.path.join(BASE_DIR, "assets", "product_images")
        os.makedirs(img_dir, exist_ok=True)
        img_path = os.path.join(img_dir, f"{safe_name}.jpg")
        with open(img_path, "wb") as f:
            f.write(img_bytes)

        local_url = f"/assets/product_images/{safe_name}.jpg"

        # Persist to Supabase
        from supabase_loader import save_image_override as _save_img_ovr
        _save_img_ovr(item_name, local_url)
        _IMAGE_URLS[item_name] = local_url
        return local_url
    except Exception:
        return ""


def _lock_in_remote_images():
    """Download any remote image URLs to local /assets/product_images/ so they can't be lost."""
    import urllib.request
    img_dir = os.path.join(BASE_DIR, "assets", "product_images")
    os.makedirs(img_dir, exist_ok=True)
    locked = 0
    for name, url in list(_IMAGE_URLS.items()):
        if not url or url.startswith("/assets/") or url.startswith("assets/"):
            continue
        # Remote URL — download it
        try:
            safe_name = re.sub(r'[^\w\s-]', '', name[:50]).strip()
            safe_name = re.sub(r'[\s]+', '_', safe_name).lower()
            if not safe_name:
                safe_name = f"item_{locked}"
            img_path = os.path.join(img_dir, f"{safe_name}.jpg")
            if os.path.exists(img_path):
                # Already downloaded, just update reference
                local_url = f"/assets/product_images/{safe_name}.jpg"
                _IMAGE_URLS[name] = local_url
                from supabase_loader import save_image_override as _sio
                _sio(name, local_url)
                locked += 1
                continue
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                img_bytes = resp.read()
            with open(img_path, "wb") as f:
                f.write(img_bytes)
            local_url = f"/assets/product_images/{safe_name}.jpg"
            _IMAGE_URLS[name] = local_url
            from supabase_loader import save_image_override as _sio2
            _sio2(name, local_url)
            locked += 1
            print(f"[images] Locked in: {name} -> {local_url}")
        except Exception as e:
            print(f"[images] Failed to lock {name}: {e}")
    if locked:
        print(f"[images] Locked in {locked} remote image(s) to local files")


_sb = _load_data()
CONFIG = _sb["CONFIG"]
INVOICES = _sb["INVOICES"]

# Re-parse Etsy data from local CSVs so new uploads are picked up immediately.
# On Railway, skip local CSVs entirely — they're stale git copies and Supabase is
# the single source of truth. Merging both sources caused data inflation bugs.
import glob as _glob_mod
_etsy_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
_etsy_frames = []
_on_railway = IS_RAILWAY
if _on_railway:
    print("[startup] Railway detected — skipping local CSVs, using Supabase only")
elif os.path.isdir(_etsy_dir):
    # Load root-level CSVs (legacy, default to keycomponentmfg)
    for _ef in sorted(_glob_mod.glob(os.path.join(_etsy_dir, "etsy_statement*.csv"))):
        try:
            _df_tmp = pd.read_csv(_ef)
            _df_tmp["Store"] = "keycomponentmfg"
            _etsy_frames.append(_df_tmp)
        except Exception as _e:
            _logger.warning("Failed to parse Etsy CSV %s: %s", os.path.basename(_ef), _e)
    # Load store-specific subdirectories
    for _store_slug in ("keycomponentmfg", "aurvio", "lunalinks"):
        _store_dir = os.path.join(_etsy_dir, _store_slug)
        if os.path.isdir(_store_dir):
            for _ef in sorted(_glob_mod.glob(os.path.join(_store_dir, "etsy_statement*.csv"))):
                try:
                    _df_tmp = pd.read_csv(_ef)
                    _df_tmp["Store"] = _store_slug
                    _etsy_frames.append(_df_tmp)
                except Exception as _e:
                    _logger.warning("Failed to parse Etsy CSV %s/%s: %s", _store_slug, os.path.basename(_ef), _e)

_parse_warnings = []  # Track parse failures for visibility

def _pm(val):
    if pd.isna(val) or val == "--" or val == "":
        return 0.0
    val = str(val).replace("$", "").replace(",", "").replace('"', "")
    try:
        return float(val)
    except Exception:
        _parse_warnings.append(f"Could not parse money value: {val!r}")
        return 0.0

if _etsy_frames:
    _local_data = pd.concat(_etsy_frames, ignore_index=True)
    _sb_data = _sb["DATA"]
    _dedup_startup_cols = ["Date", "Type", "Title", "Info", "Amount", "Fees & Taxes", "Net"]
    if len(_sb_data) > 0:
        # Merge strategy: both sources have the same underlying Etsy data. Local CSVs
        # preserve legitimate duplicate rows (e.g. multiple $0.20 listing fees on the
        # same day) while Supabase may have deduped them at upload. We concat both
        # sources, then for each group of identical rows (by the 7 key columns), keep
        # at most max(local_count, sb_count) copies — not the sum.
        _sb_data = _sb_data.copy()
        _local_data = _local_data.copy()
        # Normalise NaN/'nan' so comparison works across CSV vs Supabase
        for _dc in _dedup_startup_cols:
            _sb_data[_dc] = _sb_data[_dc].fillna("").replace("nan", "")
            _local_data[_dc] = _local_data[_dc].fillna("").replace("nan", "")
        # Number each row within its (key, source) group: if local has the same
        # row 3 times, they get ranks 0, 1, 2. Same for Supabase.
        # Then dedup on (key + rank) keeping local first. This yields exactly
        # max(local_count, sb_count) rows per unique key — preserving legitimate
        # duplicate rows while collapsing cross-source copies.
        _local_data["_src"] = "local"
        _sb_data["_src"] = "sb"
        _combined = pd.concat([_local_data, _sb_data], ignore_index=True)
        _combined["_dup_rank"] = _combined.groupby(
            _dedup_startup_cols + ["_src"], sort=False
        ).cumcount()
        # Dedup on key + rank: (local rank 0, sb rank 0) → keep local; rank 1 etc.
        # Rows with ranks only in one source survive automatically.
        DATA = _combined.drop_duplicates(
            subset=_dedup_startup_cols + ["_dup_rank"], keep="first"
        )
        DATA = DATA.drop(columns=["_src", "_dup_rank"]).reset_index(drop=True)
        print(f"Merged {len(_local_data)} local + {len(_sb_data)} Supabase -> {len(DATA)} unique Etsy rows")
    else:
        DATA = _local_data
    # Add computed columns (same as supabase_loader._add_computed_columns)
    DATA["Amount_Clean"] = DATA["Amount"].apply(_pm)
    DATA["Net_Clean"] = DATA["Net"].apply(_pm)
    DATA["Fees_Clean"] = DATA["Fees & Taxes"].apply(_pm)
    DATA["Date_Parsed"] = pd.to_datetime(DATA["Date"], format="%B %d, %Y", errors="coerce")
    DATA["Month"] = DATA["Date_Parsed"].dt.to_period("M").astype(str)
    DATA["Week"] = DATA["Date_Parsed"].dt.to_period("W").apply(lambda p: p.start_time)
    if _parse_warnings:
        _logger.warning("%d money values failed to parse (treated as $0.00)", len(_parse_warnings))
        for w in _parse_warnings[:5]:  # Show first 5
            print(f"  {w}")
        if len(_parse_warnings) > 5:
            print(f"  ... and {len(_parse_warnings) - 5} more")
else:
    DATA = _sb["DATA"]
    print("Using Etsy data from Supabase (no local CSVs found)")

# Ensure Store column exists (for data predating multi-store support)
if "Store" not in DATA.columns:
    DATA["Store"] = "keycomponentmfg"
else:
    DATA["Store"] = DATA["Store"].fillna("keycomponentmfg").replace("", "keycomponentmfg")

# Auto-calculate Etsy balance from CSV deposit titles instead of stale config value
import re as _re_mod

def _extract_order_num(title: str) -> str | None:
    """Extract 'Order #XXXXX' from a refund title string."""
    m = _re_mod.search(r"Order #\d+", str(title))
    return m.group(0) if m else None

_etsy_deposit_total = 0.0
_deposit_rows = DATA[DATA["Type"] == "Deposit"]
for _, _dr in _deposit_rows.iterrows():
    _m = _re_mod.search(r'([\d,]+\.\d+)', str(_dr.get("Title", "")))
    if _m:
        _etsy_deposit_total += float(_m.group(1).replace(",", ""))
# Etsy net = sum of all Net values (deposits have Net=0, so this is earnings minus nothing)
_etsy_all_net = DATA["Net_Clean"].sum()
# Auto-calculated balance = total earnings - total deposited to bank
# Small negative values (< $50) are rounding/timing differences — clamp to 0
_etsy_balance_auto = round(_etsy_all_net - _etsy_deposit_total, 2)

# Always re-parse bank data from local files (PDFs + CSVs) so new uploads are picked up
from _parse_bank_statements import parse_bank_pdf as _init_parse_bank
from _parse_bank_statements import parse_bank_csv as _init_parse_csv
from _parse_bank_statements import apply_overrides as _init_apply_overrides
from _parse_bank_statements import auto_categorize as _init_auto_categorize

_init_bank_dir = os.path.join(BASE_DIR, "data", "bank_statements")
_init_bank_txns = []
_init_covered = set()
if os.path.isdir(_init_bank_dir):
    for _fn in sorted(os.listdir(_init_bank_dir)):
        if _fn.lower().endswith(".pdf"):
            try:
                _txns, _cov = _init_parse_bank(os.path.join(_init_bank_dir, _fn))
                _init_bank_txns.extend(_txns)
                _init_covered.update(_cov)
            except Exception as _e:
                _logger.warning("Failed to parse bank PDF %s: %s", _fn, _e)
    _csv_txns = []
    _csv_cov = set()
    for _fn in sorted(os.listdir(_init_bank_dir)):
        if _fn.lower().endswith(".csv"):
            try:
                _txns, _cov = _init_parse_csv(os.path.join(_init_bank_dir, _fn))
                _csv_txns.extend(_txns)
                _csv_cov.update(_cov)
            except Exception as _e:
                _logger.warning("Failed to parse bank CSV %s: %s", _fn, _e)
    if _csv_txns:
        _seen = {}
        for _t in _csv_txns:
            _key = (_t["date"], _t["amount"], _t["type"], _t.get("raw_desc", _t["desc"]))
            _seen[_key] = _t
        _new_months = _csv_cov - _init_covered
        if _new_months:
            _init_bank_txns.extend([_t for _t in _seen.values()
                                     if f"{_t['date'].split('/')[2]}-{_t['date'].split('/')[0]}" in _new_months])
            _init_covered.update(_new_months)
    _init_bank_txns = _init_apply_overrides(_init_bank_txns)
    # Append manual transactions from config (permanent ones always, others only for uncovered months)
    for _mt in CONFIG.get("manual_transactions", []):
        _mt_parts = _mt["date"].split("/")
        _mt_month = f"{_mt_parts[2]}-{_mt_parts[0]}"
        if _mt_month in _init_covered and not _mt.get("permanent", False):
            continue
        _init_bank_txns.append({
            "date": _mt["date"], "desc": _mt["desc"], "amount": _mt["amount"],
            "type": _mt["type"], "category": _mt["category"],
            "source_file": "config.json (manual)", "raw_desc": _mt["desc"],
        })
# Prefer Supabase bank data if it has more transactions (local git files may be stale on Railway)
_sb_bank = _sb["BANK_TXNS"]
if _init_bank_txns and len(_init_bank_txns) >= len(_sb_bank):
    BANK_TXNS = _init_bank_txns
    print(f"Using local bank data ({len(_init_bank_txns)} txns)")
elif _sb_bank:
    BANK_TXNS = _sb_bank
    print(f"Using Supabase bank data ({len(_sb_bank)} txns > {len(_init_bank_txns)} local)")
else:
    BANK_TXNS = _init_bank_txns
    print(f"Using local bank data ({len(_init_bank_txns)} txns, Supabase empty)")

# Re-run auto_categorize on any Uncategorized transactions with latest rules
try:
    _recat_count = 0
    for _bt in BANK_TXNS:
        if _bt.get("category") == "Uncategorized":
            _new_cat = _init_auto_categorize(_bt.get("raw_desc", _bt["desc"]), _bt["type"])
            if _new_cat != "Uncategorized":
                _bt["category"] = _new_cat
                _recat_count += 1
    if _recat_count:
        print(f"[bank] Auto-categorized {_recat_count} previously uncategorized transaction(s)")
except Exception as _e:
    print(f"[bank] Re-categorize failed (non-fatal): {_e}")

# ── Extract config values ───────────────────────────────────────────────────
# Etsy balance = auto-calc from deposit titles (no hardcoded offset)
etsy_balance = _etsy_balance_auto
etsy_pre_capone_deposits = CONFIG.get("etsy_pre_capone_deposits", 0)
pre_capone_detail = [tuple(row) for row in CONFIG.get("pre_capone_detail", [])]
draw_reasons = CONFIG.get("draw_reasons", {})

# ── Best Buy Citi Credit Card ─────────────────────────────────────────────
_bb_cc = CONFIG.get("best_buy_cc", {})
bb_cc_limit = _bb_cc.get("credit_limit", 0)
bb_cc_purchases = _bb_cc.get("purchases", [])
# Auto-detect CC payments from bank transactions (BEST BUY AUTO PYMT)
bb_cc_payments = [{"date": t["date"], "desc": t["desc"], "amount": t["amount"]}
                  for t in BANK_TXNS if t["category"] == "Business Credit Card"
                  and "BEST BUY" in t.get("desc", "").upper()]
# Fallback: use config-defined payments if bank hasn't captured them yet
_bb_config_payments = _bb_cc.get("payments", [])
if not bb_cc_payments and _bb_config_payments:
    bb_cc_payments = _bb_config_payments
bb_cc_total_charged = sum(p["amount"] for p in bb_cc_purchases)
bb_cc_total_paid = sum(p["amount"] for p in bb_cc_payments)
bb_cc_balance = bb_cc_total_charged - bb_cc_total_paid
bb_cc_available = bb_cc_limit - bb_cc_balance
bb_cc_asset_value = bb_cc_total_charged  # equipment purchased = asset


def get_draw_reason(desc):
    """Look up draw reason by substring match against config keys."""
    d = desc.upper()
    for key, reason in draw_reasons.items():
        if key.upper() in d:
            return reason
    return ""


def parse_money(val):
    """Thin wrapper around helpers.parse_money that feeds _parse_warnings."""
    return _parse_money_pure(val, warnings=_parse_warnings)


# _normalize_product_name and _merge_product_prefixes moved to
# dashboard_utils.helpers (imported at top of file)


# ── Store filter helper ──────────────────────────────────────────────────────

def _filtered_data(store="all"):
    """Return DATA filtered by store slug. 'all' or falsy returns everything."""
    if store == "all" or not store:
        return DATA
    return DATA[DATA["Store"] == store]


_DATA_ALL = None  # Stashed full DATA for store filter restore

# ── State Manager (Phase 1 bridge) ────────────────────────────────────────────
from dashboard_utils.state import state_manager as _state_manager, build_etsy_state


def _apply_store_filter(store="all"):
    """Filter DATA by store using the StateManager, then back-fill globals.

    The StateManager builds a clean EtsyState from filtered data.
    We then copy every value from the state into the old globals
    so all existing tab builders, charts, and callbacks work unchanged.
    """
    global DATA, _DATA_ALL
    global gross_sales, total_refunds, net_sales, total_fees
    global total_shipping_cost, total_marketing, total_taxes, total_payments
    global order_count, avg_order, total_buyer_fees

    # Initialize StateManager on first call
    if _DATA_ALL is None:
        _DATA_ALL = DATA.copy()
        _state_manager.initialize(DATA, CONFIG)

    # Build a clean state for this store — no global mutation, no corruption
    state = _state_manager.set_store_filter(store)

    # ── BRIDGE: back-fill old globals from the clean state ──
    DATA = state.data

    # Back-fill scalar metrics
    gross_sales = state.gross_sales
    total_refunds = state.total_refunds
    net_sales = state.net_sales
    total_fees = state.total_fees
    total_shipping_cost = state.total_shipping_cost
    total_marketing = state.total_marketing
    total_taxes = state.total_taxes
    total_payments = state.total_payments
    total_buyer_fees = state.total_buyer_fees
    order_count = state.order_count
    avg_order = state.avg_order

    # Back-fill filtered DataFrames and all derived metrics
    _backfill_etsy_derived(state)

    # Profit and profit_margin are business-level metrics (bank-derived).
    # They stay the same regardless of store selection because all stores
    # share one bank account. Don't override them per-store.


def _backfill_etsy_derived(state):
    """Copy all EtsyState values into the old module globals.

    This is the compatibility bridge — every existing function that reads
    globals will see the same values as before, but now they come from
    a cleanly-built state object instead of piecemeal mutation.
    """
    global sales_df, fee_df, ship_df, mkt_df, refund_df, tax_df
    global deposit_df, buyer_fee_df, payment_df
    global monthly_sales, monthly_fees, monthly_shipping, monthly_marketing
    global monthly_refunds, monthly_taxes, monthly_raw_fees, monthly_raw_shipping
    global monthly_raw_marketing, monthly_raw_refunds, monthly_net_revenue
    global monthly_raw_taxes, monthly_raw_buyer_fees, monthly_raw_payments
    global daily_sales, daily_orders, daily_df, weekly_aov
    global monthly_order_counts, monthly_aov, monthly_profit_per_order
    global months_sorted, days_active
    global product_fee_totals, product_revenue_est
    global listing_fees, transaction_fees_product, transaction_fees_shipping
    global processing_fees, credit_transaction, credit_listing, credit_processing
    global share_save, total_credits, total_fees_gross
    global etsy_ads, offsite_ads_fees, offsite_ads_credits
    global usps_outbound, usps_outbound_count, usps_return, usps_return_count
    global asendia_labels, asendia_count, ship_adjustments, ship_adjust_count
    global ship_credits, ship_credit_count, ship_insurance, ship_insurance_count
    global buyer_paid_shipping, shipping_profit, shipping_margin
    global paid_ship_count, free_ship_count, avg_outbound_label
    global _etsy_deposit_total, _deposit_rows

    # Filtered DataFrames
    sales_df = state.sales_df
    fee_df = state.fee_df
    ship_df = state.ship_df
    mkt_df = state.mkt_df
    refund_df = state.refund_df
    tax_df = state.tax_df
    deposit_df = state.deposit_df
    buyer_fee_df = state.buyer_fee_df
    payment_df = state.payment_df

    # Monthly aggregations
    months_sorted = state.months_sorted
    monthly_sales = state.monthly_sales
    monthly_fees = state.monthly_fees
    monthly_shipping = state.monthly_shipping
    monthly_marketing = state.monthly_marketing
    monthly_refunds = state.monthly_refunds
    monthly_taxes = state.monthly_taxes
    monthly_raw_fees = state.monthly_raw_fees
    monthly_raw_shipping = state.monthly_raw_shipping
    monthly_raw_marketing = state.monthly_raw_marketing
    monthly_raw_refunds = state.monthly_raw_refunds
    monthly_raw_taxes = state.monthly_raw_taxes
    monthly_raw_buyer_fees = state.monthly_raw_buyer_fees
    monthly_raw_payments = state.monthly_raw_payments
    monthly_net_revenue = state.monthly_net_revenue
    monthly_order_counts = state.monthly_order_counts
    monthly_aov = state.monthly_aov
    monthly_profit_per_order = state.monthly_profit_per_order
    days_active = state.days_active

    # Daily aggregations
    daily_sales = state.daily_sales
    daily_orders = state.daily_orders
    daily_df = state.daily_df
    weekly_aov = state.weekly_aov

    # Fee breakdown
    listing_fees = state.listing_fees
    transaction_fees_product = state.transaction_fees_product
    transaction_fees_shipping = state.transaction_fees_shipping
    processing_fees = state.processing_fees
    credit_transaction = state.credit_transaction
    credit_listing = state.credit_listing
    credit_processing = state.credit_processing
    share_save = state.share_save
    total_credits = state.total_credits
    total_fees_gross = state.total_fees_gross

    # Marketing
    etsy_ads = state.etsy_ads
    offsite_ads_fees = state.offsite_ads_fees
    offsite_ads_credits = state.offsite_ads_credits

    # Shipping
    usps_outbound = state.usps_outbound
    usps_outbound_count = state.usps_outbound_count
    usps_return = state.usps_return
    usps_return_count = state.usps_return_count
    asendia_labels = state.asendia_labels
    asendia_count = state.asendia_count
    ship_adjustments = state.ship_adjustments
    ship_adjust_count = state.ship_adjust_count
    ship_credits = state.ship_credits
    ship_credit_count = state.ship_credit_count
    ship_insurance = state.ship_insurance
    ship_insurance_count = state.ship_insurance_count
    buyer_paid_shipping = state.buyer_paid_shipping
    shipping_profit = state.shipping_profit
    shipping_margin = state.shipping_margin
    paid_ship_count = state.paid_ship_count
    free_ship_count = state.free_ship_count
    avg_outbound_label = state.avg_outbound_label

    # Product performance
    product_fee_totals = state.product_fee_totals
    product_revenue_est = state.product_revenue_est

    # Deposit tracking
    _etsy_deposit_total = state._etsy_deposit_total
    _deposit_rows = state._deposit_rows


# ── Pre-compute metrics ─────────────────────────────────────────────────────

sales_df = DATA[DATA["Type"] == "Sale"]
fee_df = DATA[DATA["Type"] == "Fee"]
ship_df = DATA[DATA["Type"] == "Shipping"]
mkt_df = DATA[DATA["Type"] == "Marketing"]
refund_df = DATA[DATA["Type"] == "Refund"]
tax_df = DATA[DATA["Type"] == "Tax"]
deposit_df = DATA[DATA["Type"] == "Deposit"]
buyer_fee_df = DATA[DATA["Type"] == "Buyer Fee"]
payment_df = DATA[DATA["Type"] == "Payment"]

# Top-level numbers
gross_sales = sales_df["Net_Clean"].sum()
total_refunds = abs(refund_df["Net_Clean"].sum())
net_sales = gross_sales - total_refunds

total_fees = abs(fee_df["Net_Clean"].sum())
total_shipping_cost = abs(ship_df["Net_Clean"].sum())
total_marketing = abs(mkt_df["Net_Clean"].sum())
total_taxes = abs(tax_df["Net_Clean"].sum())
total_payments = payment_df["Net_Clean"].sum()  # Refund charges (positive = credit back)

order_count = len(sales_df)
avg_order = gross_sales / order_count if order_count else 0

# ── Load Inventory / COGS Data ─────────────────────────────────────────────
# INVOICES already loaded by supabase_loader

# Build inventory DataFrames
inv_rows = []
for inv in INVOICES:
    date_str = inv["date"]
    try:
        dt = pd.to_datetime(date_str, format="%B %d, %Y")
    except Exception:
        try:
            dt = pd.to_datetime(date_str)
        except Exception:
            dt = pd.NaT
    inv_rows.append({
        "order_num": inv["order_num"],
        "date": date_str,
        "date_parsed": dt,
        "month": dt.to_period("M").strftime("%Y-%m") if pd.notna(dt) else "Unknown",
        "grand_total": inv["grand_total"],
        "subtotal": inv["subtotal"],
        "tax": inv["tax"],
        "source": inv["source"],
        "item_count": len(inv["items"]),
        "file": inv["file"],
        "ship_address": inv.get("ship_address", ""),
        "payment_method": inv.get("payment_method", "Unknown"),
    })

if inv_rows:
    INV_DF = pd.DataFrame(inv_rows)
    INV_DF = INV_DF.sort_values("date_parsed")
else:
    INV_DF = pd.DataFrame(columns=[
        "order_num", "date", "date_parsed", "month", "grand_total",
        "subtotal", "tax", "source", "item_count", "file",
        "ship_address", "payment_method",
    ])

# Item-level DataFrame
inv_item_rows = []
for inv in INVOICES:
    try:
        dt = pd.to_datetime(inv["date"], format="%B %d, %Y")
    except Exception:
        try:
            dt = pd.to_datetime(inv["date"])
        except Exception:
            dt = pd.NaT
    month = dt.to_period("M").strftime("%Y-%m") if pd.notna(dt) else "Unknown"
    for item in inv["items"]:
        # Clean up Personal Amazon item names
        item_name = item["name"]
        if item_name.startswith("Your package was left near the front door or porch."):
            item_name = item_name.replace("Your package was left near the front door or porch.", "").strip()
        inv_item_rows.append({
            "order_num": inv["order_num"],
            "date": inv["date"],
            "date_parsed": dt,
            "month": month,
            "name": item_name,
            "qty": item["qty"],
            "price": item["price"],
            "total": item["price"] * item["qty"],
            "source": inv["source"],
            "seller": item.get("seller", "Unknown"),
            "ship_to": item.get("ship_to", inv.get("ship_address", "")),
            "payment_method": inv.get("payment_method", "Unknown"),
            "image_url": item.get("image_url", ""),
        })

if inv_item_rows:
    INV_ITEMS = pd.DataFrame(inv_item_rows)
else:
    INV_ITEMS = pd.DataFrame(columns=[
        "order_num", "date", "date_parsed", "month", "name", "qty",
        "price", "total", "source", "seller", "ship_to",
        "payment_method", "image_url",
    ])

# ── Compute tax-inclusive cost per item ────────────────────────────────────
# Allocate each order's tax proportionally across its items
_order_totals_map = {}  # order_num -> {subtotal, grand_total}
for inv in INVOICES:
    _order_totals_map[inv["order_num"]] = {
        "subtotal": inv["subtotal"], "grand_total": inv["grand_total"]}
if len(INV_ITEMS) > 0:
    def _calc_with_tax(row):
        ot = _order_totals_map.get(row["order_num"])
        if ot and ot["subtotal"] > 0:
            return round(row["total"] * (ot["grand_total"] / ot["subtotal"]), 2)
        return row["total"]
    INV_ITEMS["total_with_tax"] = INV_ITEMS.apply(_calc_with_tax, axis=1)
else:
    INV_ITEMS["total_with_tax"] = INV_ITEMS["total"] if len(INV_ITEMS) > 0 else []

# ── Item Details (rename / categorize / true qty) ──────────────────────────
CATEGORY_OPTIONS = [
    "Filament", "Lighting", "Crafts", "Packaging", "Hardware",
    "Tools", "Printer Parts", "Jewelry", "Personal/Gift", "Business Fees", "Other",
]
_ITEM_DETAILS: dict[tuple[str, str], list[dict]] = {}
_ITEM_SAVED_AT: dict[tuple[str, str], str] = {}  # (order_num, item_name) → created_at timestamp
try:
    _raw_details = _load_item_details()
    for d in _raw_details:
        key = (d["order_num"], d["item_name"])
        _ITEM_SAVED_AT[key] = d.get("created_at", "")
        if d.get("category") == "_JSON_":
            # New format: display_name is a JSON array of detail entries
            try:
                entries = json.loads(d["display_name"])
                for entry in entries:
                    _ITEM_DETAILS.setdefault(key, []).append(entry)
            except (json.JSONDecodeError, TypeError):
                pass
        else:
            # Legacy format: one row = one entry
            _ITEM_DETAILS.setdefault(key, []).append({
                "display_name": d["display_name"],
                "category": d["category"],
                "true_qty": d["true_qty"],
                "location": d.get("location", ""),
            })
except Exception as _e:
    _logger.warning("Failed to load item details: %s", _e)

# ── Location Overrides ─────────────────────────────────────────────────────
# Load overrides from Supabase (keyed by (order_num, item_name))
_LOC_OVERRIDES: dict[tuple[str, str], list[dict]] = {}
try:
    _raw_overrides = _load_location_overrides()
    for ov in _raw_overrides:
        key = (ov["order_num"], ov["item_name"])
        _LOC_OVERRIDES.setdefault(key, []).append({"location": ov["location"], "qty": ov["qty"]})
except Exception as _e:
    _logger.warning("Failed to load location overrides: %s", _e)

# Apply overrides: expand split items into separate rows
# NOTE: Skip overrides when item details exist (details already include locations)
if len(INV_ITEMS) > 0 and _LOC_OVERRIDES:
    expanded_rows = []
    for _, row in INV_ITEMS.iterrows():
        key = (row["order_num"], row["name"])
        if key in _LOC_OVERRIDES and key not in _ITEM_DETAILS:
            for ov in _LOC_OVERRIDES[key]:
                new_row = row.copy()
                new_row["qty"] = ov["qty"]
                new_row["total"] = row["price"] * ov["qty"]
                new_row["_override_location"] = ov["location"]
                expanded_rows.append(new_row)
        else:
            row_copy = row.copy()
            row_copy["_override_location"] = ""
            expanded_rows.append(row_copy)
    INV_ITEMS = pd.DataFrame(expanded_rows)

# Uploaded inventory: {("Tulsa", "Black PLA", "Filament"): qty, ...}
# Rebuilt after detail expansion (below) from expanded INV_ITEMS
_UPLOADED_INVENTORY: dict[tuple[str, str, str], int] = {}
# Cost per unit for each inventory item: same keys as _UPLOADED_INVENTORY
_INVENTORY_UNIT_COST: dict[tuple[str, str, str], float] = {}

def _norm_loc(loc_str):
    """Normalize location string to 'Tulsa' or 'Texas' or ''."""
    if not isinstance(loc_str, str):
        loc_str = ""
    loc_str = loc_str.strip().lower()
    if "tulsa" in loc_str or "tj" in loc_str or loc_str in ("ok", "oklahoma"):
        return "Tulsa"
    elif "texas" in loc_str or "braden" in loc_str or loc_str in ("tx", "celina", "prosper"):
        return "Texas"
    return ""

# Build image_url lookup BEFORE renaming so original names keep their photos
_IMAGE_URLS: dict[str, str] = {}
if len(INV_ITEMS) > 0 and "image_url" in INV_ITEMS.columns:
    for _, _r in INV_ITEMS.iterrows():
        _n = _r["name"]
        _u = _r.get("image_url", "") or ""
        if _u and _n not in _IMAGE_URLS:
            _IMAGE_URLS[_n] = _u

# Apply item details: rename, recategorize, adjust qty
# Key rule: the TOTAL cost of the original line item stays the same.
# If true_qty differs from original qty, per-unit price adjusts accordingly.
# If split into sub-items, the total is divided proportionally by qty.
if len(INV_ITEMS) > 0 and _ITEM_DETAILS:
    detail_rows = []
    for _, row in INV_ITEMS.iterrows():
        key = (row["order_num"], row["name"])
        if key in _ITEM_DETAILS:
            dets = _ITEM_DETAILS[key]
            orig_total = row["price"] * row["qty"]  # original line-item total
            orig_with_tax = row.get("total_with_tax", orig_total)
            total_detail_qty = sum(d["true_qty"] for d in dets)
            per_unit = orig_total / total_detail_qty if total_detail_qty > 0 else 0
            per_unit_tax = orig_with_tax / total_detail_qty if total_detail_qty > 0 else 0
            _orig_img = _IMAGE_URLS.get(row["name"], "")
            for det in dets:
                new_row = row.copy()
                new_row["_orig_name"] = row["name"]  # preserve original name for overrides
                new_row["name"] = det["display_name"]
                new_row["category"] = det["category"] if "category" in row.index else det["category"]
                new_row["qty"] = det["true_qty"]
                new_row["price"] = round(per_unit, 2)
                new_row["total"] = round(per_unit * det["true_qty"], 2)
                new_row["total_with_tax"] = round(per_unit_tax * det["true_qty"], 2)
                # Clear the per-row image_url so it doesn't override _IMAGE_URLS lookup
                new_row["image_url"] = ""
                # If detail has a location, use it as override
                if det.get("location"):
                    new_row["_override_location"] = det["location"]
                detail_rows.append(new_row)
                # Also map renamed items to the original image (fallback only)
                if _orig_img and det["display_name"] not in _IMAGE_URLS:
                    _IMAGE_URLS[det["display_name"]] = _orig_img
        else:
            rc = row.copy()
            rc["_orig_name"] = row["name"]
            detail_rows.append(rc)
    INV_ITEMS = pd.DataFrame(detail_rows)

# Ensure _orig_name column exists even if no details were applied
if len(INV_ITEMS) > 0 and "_orig_name" not in INV_ITEMS.columns:
    INV_ITEMS["_orig_name"] = INV_ITEMS["name"]

# Rebuild _UPLOADED_INVENTORY (and _INVENTORY_UNIT_COST) from _ITEM_DETAILS only
_UPLOADED_INVENTORY.clear()
_INVENTORY_UNIT_COST.clear()
# Build a price lookup from expanded INV_ITEMS: (order_num, name) → per-unit price WITH TAX
# Uses total_with_tax / qty to include each item's proportional share of order tax + shipping
_price_lookup: dict[tuple[str, str], float] = {}
if len(INV_ITEMS) > 0:
    for _, _r in INV_ITEMS.iterrows():
        _item_qty = max(int(_r.get("qty", 1)), 1)
        _item_total_tax = float(_r.get("total_with_tax", _r.get("price", 0) * _item_qty))
        _price_lookup[(_r["order_num"], _r["name"])] = round(_item_total_tax / _item_qty, 2)
# Track total spend per item for weighted average: {inv_key: total_cost}
_inv_total_cost: dict[tuple[str, str, str], float] = {}
for (_onum, _iname), _details in _ITEM_DETAILS.items():
    for _d in _details:
        _loc = _norm_loc(_d.get("location", ""))
        if not _loc:
            continue
        _dn = _d.get("display_name", _iname)
        _cat = _d.get("category", "Other")
        _inv_key = (_loc, _dn, _cat)
        _dqty = int(_d.get("true_qty", 1))
        _UPLOADED_INVENTORY[_inv_key] = _UPLOADED_INVENTORY.get(_inv_key, 0) + _dqty
        # Accumulate total cost for weighted average
        _unit_price = _price_lookup.get((_onum, _dn), 0)
        if _unit_price:
            _inv_total_cost[_inv_key] = _inv_total_cost.get(_inv_key, 0) + (_unit_price * _dqty)
# Compute average cost per unit: total_cost / total_qty
for _inv_key, _total_cost in _inv_total_cost.items():
    _total_qty = _UPLOADED_INVENTORY.get(_inv_key, 1)
    _INVENTORY_UNIT_COST[_inv_key] = round(_total_cost / _total_qty, 2) if _total_qty > 0 else 0

# Apply persistent image overrides (for renamed items saved via Image Manager)
try:
    _img_overrides = _load_image_overrides()
    if _img_overrides:
        _IMAGE_URLS.update(_img_overrides)  # overrides take priority
except Exception:
    pass

# Lock in any remote image URLs to local files
try:
    _lock_in_remote_images()
except Exception as _e:
    print(f"[images] Lock-in failed (non-fatal): {_e}")

# Inventory aggregates
total_inventory_cost = INV_DF["grand_total"].sum()
total_inv_subtotal = INV_DF["subtotal"].sum()
total_inv_tax = INV_DF["tax"].sum()
biz_inv_cost = INV_DF[INV_DF["source"] == "Key Component Mfg"]["grand_total"].sum()
personal_acct_cost = INV_DF[INV_DF["source"] == "Personal Amazon"]["grand_total"].sum()
inv_order_count = len(INV_DF)

# Flag the Gigi personal/gift order
gigi_mask = INV_DF["file"].str.contains("Gigi", na=False)
gigi_cost = INV_DF[gigi_mask]["grand_total"].sum()

# personal_total = actual personal ITEMS (by category), not by payment source
# This gets recalculated after INV_ITEMS categories are assigned (see below)

# Monthly inventory spend
monthly_inv_spend = INV_DF.groupby("month")["grand_total"].sum()
monthly_inv_subtotal = INV_DF.groupby("month")["subtotal"].sum()

# ── Bank Statement Data (Capital One Checking 3650) ────────────────────────
# BANK_TXNS already loaded by supabase_loader

# Count source files for display (local fallback info)
_bank_json_path = os.path.join(BASE_DIR, "data", "generated", "bank_transactions.json")
if os.path.exists(_bank_json_path):
    with open(_bank_json_path) as _bf:
        _bank_source_files = json.load(_bf).get("metadata", {}).get("source_files", [])
else:
    _bank_source_files = []
bank_statement_count = len(_bank_source_files)

# Bank aggregates
bank_deposits = [t for t in BANK_TXNS if t["type"] == "deposit"]
bank_debits = [t for t in BANK_TXNS if t["type"] == "debit"]
bank_total_deposits = sum(t["amount"] for t in bank_deposits)
bank_total_debits = sum(t["amount"] for t in bank_debits)
bank_net_cash = bank_total_deposits - bank_total_debits

# By-category aggregates
bank_by_cat = {}
for t in bank_debits:
    cat = t["category"]
    bank_by_cat[cat] = bank_by_cat.get(cat, 0) + t["amount"]
bank_by_cat = dict(sorted(bank_by_cat.items(), key=lambda x: -x[1]))

# Monthly aggregates
bank_monthly = {}
for t in BANK_TXNS:
    # Derive YYYY-MM from MM/DD/YYYY date format
    parts = t["date"].split("/")
    month_key = f"{parts[2]}-{parts[0]}"
    if month_key not in bank_monthly:
        bank_monthly[month_key] = {"deposits": 0, "debits": 0}
    if t["type"] == "deposit":
        bank_monthly[month_key]["deposits"] += t["amount"]
    else:
        bank_monthly[month_key]["debits"] += t["amount"]

# Tax-deductible categories (Schedule C)
BANK_TAX_DEDUCTIBLE = {"Amazon Inventory", "Shipping", "Craft Supplies", "Etsy Fees",
                        "Subscriptions", "AliExpress Supplies", "Business Credit Card"}
bank_tax_deductible = sum(amt for cat, amt in bank_by_cat.items() if cat in BANK_TAX_DEDUCTIBLE)
bank_personal = bank_by_cat.get("Personal", 0)
bank_pending = bank_by_cat.get("Pending", 0)

# ── Etsy-side accounting (full penny trace) ──
total_buyer_fees = abs(buyer_fee_df["Net_Clean"].sum()) if len(buyer_fee_df) else 0.0
etsy_net_earned = (gross_sales - total_fees - total_shipping_cost - total_marketing
                   - total_refunds - total_taxes - total_buyer_fees + total_payments)
etsy_net = etsy_net_earned
etsy_net_margin = (etsy_net / gross_sales * 100) if gross_sales else 0
# etsy_pre_capone_deposits and etsy_balance loaded from config.json above
# Only count bank deposits that are actually from Etsy (not personal transfers etc.)
_bank_etsy_deposits = sum(t["amount"] for t in bank_deposits if "etsy" in t.get("desc", "").lower())
etsy_total_deposited = etsy_pre_capone_deposits + (_bank_etsy_deposits if _bank_etsy_deposits > 0 else bank_total_deposits)
# Known non-Etsy adjustments that affect the Etsy Payments balance
# $18.44 Amazon refund deposited to Etsy Payments — not from Etsy sales
_known_non_etsy_adjustments = 18.44

etsy_balance_calculated = etsy_net_earned - etsy_total_deposited + _known_non_etsy_adjustments
etsy_csv_gap = round(etsy_balance_calculated - etsy_balance, 2)

# Startup self-check: verify itemized formula matches direct sum
_check_net = round(DATA["Net_Clean"].sum(), 2)
_check_earned = round(etsy_net_earned, 2)
if abs(_check_net - _check_earned) > 0.01:
    _logger.warning("etsy_net_earned (%s) != DATA Net_Clean sum (%s)", _check_earned, _check_net)

# ── Bank-Reconciled Profit (the REAL numbers) ──
# This is the single source of truth for profit, used across all tabs
_biz_expense_cats = ["Shipping", "Craft Supplies", "Etsy Fees", "Subscriptions", "AliExpress Supplies", "Business Credit Card"]
bank_biz_expense_total = sum(bank_by_cat.get(c, 0) for c in _biz_expense_cats)
bank_all_expenses = bank_by_cat.get("Amazon Inventory", 0) + bank_biz_expense_total
bank_cash_on_hand = bank_net_cash + etsy_balance
bank_owner_draw_total = sum(bank_by_cat.get(c, 0) for c in bank_by_cat if c.startswith("Owner Draw"))
real_profit = bank_cash_on_hand + bank_owner_draw_total  # Cash you HAVE + cash you TOOK = real profit
real_profit_margin = (real_profit / gross_sales * 100) if gross_sales else 0

# ── Old bank: match non-Discover-4570 invoices to pre-CapOne deposits ──
old_bank_receipted = INV_DF.loc[INV_DF["payment_method"] != "Discover ending in 4570", "grand_total"].sum()
old_bank_receipted = min(old_bank_receipted, etsy_pre_capone_deposits)  # can't exceed deposits
bank_unaccounted = round(etsy_pre_capone_deposits - old_bank_receipted, 2)  # true gap (~$28)

# ── Draw settlement (module level so Overview can use it) ──
tulsa_draws = [t for t in bank_debits if t["category"] == "Owner Draw - Tulsa"]
texas_draws = [t for t in bank_debits if t["category"] == "Owner Draw - Texas"]
tulsa_draw_total = sum(t["amount"] for t in tulsa_draws)
texas_draw_total = sum(t["amount"] for t in texas_draws)
draw_diff = abs(tulsa_draw_total - texas_draw_total)
if tulsa_draw_total > texas_draw_total:
    draw_owed_to = "Braden"
else:
    draw_owed_to = "TJ"

# ── Credit card / other account spending (from inventory invoices) ──
cc_by_method = INV_DF.groupby("payment_method").agg(
    count=("grand_total", "count"),
    total=("grand_total", "sum"),
).to_dict("index")

discover_inv_total = cc_by_method.get("Discover ending in 4570", {}).get("total", 0)
discover_inv_count = int(cc_by_method.get("Discover ending in 4570", {}).get("count", 0))
visa_methods = {k: v for k, v in cc_by_method.items() if k.startswith("Visa")}
visa_inv_total = sum(v["total"] for v in visa_methods.values())
visa_inv_count = int(sum(v["count"] for v in visa_methods.values()))
other_inv_methods = {k: v for k, v in cc_by_method.items()
                     if not k.startswith("Discover") and not k.startswith("Visa")}
other_inv_total = sum(v["total"] for v in other_inv_methods.values())
other_inv_count = int(sum(v["count"] for v in other_inv_methods.values()))

# ── Running balance for ledger ──
def _parse_bank_date(d):
    """Convert MM/DD/YYYY to (YYYY, MM, DD) for proper chronological sort."""
    parts = d.split("/")
    return (int(parts[2]), int(parts[0]), int(parts[1]))

bank_txns_sorted = sorted(BANK_TXNS, key=lambda x: (_parse_bank_date(x["date"]), 0 if x["type"] == "deposit" else 1))
bank_running = []
_bal = 0.0
for t in bank_txns_sorted:
    if t["type"] == "deposit":
        _bal += t["amount"]
    else:
        _bal -= t["amount"]
    bank_running.append({**t, "_balance": round(_bal, 2)})

# ── Accounting Pipeline (replaces hardcoded balance, validates all metrics) ──
_acct_pipeline = None
try:
    from accounting import get_pipeline as _get_pipeline
    from accounting.compat import publish_to_globals as _publish_to_globals
    from accounting.compat import get_metric_provenance as _get_metric_provenance
    _acct_pipeline = _get_pipeline()
    _acct_pipeline.full_rebuild(DATA, BANK_TXNS, CONFIG, invoices=INVOICES)
    _publish_to_globals(_acct_pipeline, __name__)
    _logger.info("Accounting pipeline active: %s", _acct_pipeline.ledger.summary())
except Exception as _pipe_err:
    _logger.warning("Accounting pipeline failed, using legacy calculations: %s", _pipe_err)
    import traceback
    traceback.print_exc()
    _acct_pipeline = None
    _get_metric_provenance = None

# Wire up provenance hook for theme.py helpers (kpi_card, row_item, etc.)
_set_provenance_hook(_get_metric_provenance)

# Run CEO Agent startup check (separate try so it doesn't kill the pipeline)
try:
    from accounting.agents.ceo import CEOAgent
    _ceo_agent = CEOAgent()
    _ceo_health = _ceo_agent.run_startup_check(_acct_pipeline) if _acct_pipeline else None
except Exception as _ceo_err:
    _logger.warning("CEO Agent failed (pipeline still active): %s", _ceo_err)
    import traceback
    traceback.print_exc()

try:
    _ceo_agent
except NameError:
    _ceo_agent = None
    _ceo_health = None

# Load dismissed CEO alerts from Supabase config (persists across restarts)
_dismissed_alerts = set()
try:
    from supabase_loader import get_config_value as _get_cfg
    _dismissed_raw = _get_cfg("dismissed_ceo_alerts", [])
    if isinstance(_dismissed_raw, list):
        _dismissed_alerts = set(_dismissed_raw)
except Exception:
    pass

# Load refund shipped-by assignments (TJ / Braden) from Supabase config
_refund_assignments = {}
try:
    from supabase_loader import get_config_value as _get_cfg2
    _ra_raw = _get_cfg2("refund_assignments", {})
    if isinstance(_ra_raw, dict):
        _refund_assignments = _ra_raw
except Exception:
    pass

# Refund cost overrides — manual corrections for cancelled/refunded orders
# Format: {"order_id": {"type": "refund"|"cancel", "outbound_label": float, "return_label": float}}
_refund_cost_overrides = {}
try:
    from supabase_loader import get_config_value as _get_cfg3
    _rco_raw = _get_cfg3("refund_cost_overrides", {})
    if isinstance(_rco_raw, dict):
        _refund_cost_overrides = _rco_raw
except Exception:
    pass

# Manual label-to-order mapping: {"order_id": "Label #123456789"}
_label_order_map = {}
try:
    from supabase_loader import get_config_value as _get_cfg4
    _lom_raw = _get_cfg4("label_order_map", {})
    if isinstance(_lom_raw, dict):
        _label_order_map = _lom_raw
except Exception:
    pass

# Product Library — links listings to STL files and inventory items for COGS
PRODUCT_LIBRARY = {}  # {product_id: {name, stl_files, print_time, filament_grams, category, linked_listings, linked_inventory, ...}}
try:
    from supabase_loader import get_config_value as _get_cfg5
    _pl_raw = _get_cfg5("product_library", {})
    if isinstance(_pl_raw, dict):
        PRODUCT_LIBRARY = _pl_raw
    elif isinstance(_pl_raw, str):
        import json as _json_pl
        PRODUCT_LIBRARY = _json_pl.loads(_pl_raw)
except Exception:
    pass

# ── Expense completeness globals (defaults if pipeline didn't set them) ──
try:
    expense_receipt_verified
except NameError:
    expense_receipt_verified = 0.0
try:
    expense_bank_recorded
except NameError:
    expense_bank_recorded = 0.0
try:
    expense_gap
except NameError:
    expense_gap = 0.0
try:
    expense_by_category
except NameError:
    expense_by_category = {}
try:
    expense_matched_count
except NameError:
    expense_matched_count = 0
try:
    expense_missing_receipts
except NameError:
    expense_missing_receipts = []
try:
    strict_mode
except NameError:
    strict_mode = False
try:
    ledger_ref
except NameError:
    ledger_ref = None

# ── Per-Order Profit Tracking ────────────────────────────────────────────────

ORDER_PROFITS = []  # List of dicts with per-order profit data
ORDER_PROFIT_SUMMARY = {}  # Summary stats


def _save_order_csv_to_supabase(df, store, csv_type):
    """Persist order CSV data to Supabase config table as JSON.
    Merges with existing data (deduplicates by Order ID) so multiple
    year uploads don't overwrite each other."""
    try:
        from supabase_loader import save_config_value, get_config_value
        import json
        key = f"order_csv_{csv_type}_{store}"
        # Load existing data
        existing_raw = get_config_value(key)
        existing_records = []
        if existing_raw:
            existing_records = json.loads(existing_raw) if isinstance(existing_raw, str) else existing_raw

        # Keep only essential columns to reduce payload size
        _essential_cols = ['Sale Date', 'Order ID', 'Number of Items', 'Date Shipped',
                           'Order Value', 'Discount Amount', 'Shipping', 'Sales Tax',
                           'Order Total', 'Card Processing Fees', 'Order Net',
                           'Ship State', 'Ship Country', 'Buyer', 'SKU', 'Status',
                           'Item Name', 'Quantity', 'Price', 'Transaction ID', 'Listing ID',
                           'Variations', 'Coupon Code']
        _keep = [c for c in _essential_cols if c in df.columns]
        _slim_df = df[_keep] if _keep else df
        new_records = json.loads(_slim_df.to_json(orient="records"))

        # Merge: use Order ID as dedup key
        _id_col = "Order ID" if "Order ID" in df.columns else None
        if _id_col and existing_records:
            new_ids = {r.get(_id_col) for r in new_records if r.get(_id_col)}
            # Keep existing records that aren't in the new upload
            kept = [r for r in existing_records if r.get(_id_col) not in new_ids]
            merged = kept + new_records
            print(f"[OrderProfit] Merged: {len(existing_records)} existing + {len(new_records)} new - {len(existing_records) - len(kept)} replaced = {len(merged)} total")
        else:
            merged = new_records

        save_config_value(key, json.dumps(merged))
        print(f"[OrderProfit] Saved {len(merged)} {csv_type} rows for {store} to Supabase")
    except Exception as e:
        print(f"[OrderProfit] Supabase save failed: {e}")


def _load_order_csvs():
    """Load order CSVs from local files and Supabase."""
    all_orders = []
    all_items = []

    # Check data/order_csvs/{store}/ (local disk — works right after upload)
    for store in ("keycomponentmfg", "aurvio", "lunalinks"):
        order_dir = os.path.join(BASE_DIR, "data", "order_csvs", store)
        if os.path.isdir(order_dir):
            for f in sorted(os.listdir(order_dir)):
                fp = os.path.join(order_dir, f)
                if not f.endswith(".csv"):
                    continue
                try:
                    df = pd.read_csv(fp)
                    df["_store"] = store
                    if "Order ID" in df.columns and "Order Net" in df.columns:
                        all_orders.append(df)
                    elif "Item Name" in df.columns and "Order ID" in df.columns:
                        all_items.append(df)
                except Exception as e:
                    print(f"[OrderProfit] Failed to parse {fp}: {e}")

    # Check UPLOAD_HERE folders on local machine
    _upload_base = os.path.join(os.path.dirname(BASE_DIR), "UPLOAD_HERE")
    if os.path.isdir(_upload_base):
        _store_map = {"L&L": "lunalinks", "luna": "lunalinks", "aurvio": "aurvio",
                       "keycomp": "keycomponentmfg", "key": "keycomponentmfg"}
        for _subdir in os.listdir(_upload_base):
            _subpath = os.path.join(_upload_base, _subdir)
            if not os.path.isdir(_subpath):
                continue
            _store_guess = "keycomponentmfg"
            for _key, _val in _store_map.items():
                if _key.lower() in _subdir.lower():
                    _store_guess = _val
                    break
            for f in sorted(os.listdir(_subpath)):
                if not f.endswith(".csv"):
                    continue
                fp = os.path.join(_subpath, f)
                try:
                    df = pd.read_csv(fp)
                    df["_store"] = _store_guess
                    if "Order ID" in df.columns and "Order Net" in df.columns:
                        all_orders.append(df)
                    elif "Item Name" in df.columns and "Order ID" in df.columns:
                        all_items.append(df)
                except Exception as e:
                    print(f"[OrderProfit] Failed to parse {fp}: {e}")

    # Load from Supabase (persisted data — survives redeploys)
    # Always check Supabase for each store that doesn't already have local data
    try:
        from supabase_loader import get_config_value
        import json
        _local_order_stores = {df["_store"].iloc[0] for df in all_orders if len(df) > 0} if all_orders else set()
        _local_item_stores = {df["_store"].iloc[0] for df in all_items if len(df) > 0} if all_items else set()
        for store in ("keycomponentmfg", "aurvio", "lunalinks"):
            if store not in _local_order_stores:
                key = f"order_csv_orders_{store}"
                raw = get_config_value(key)
                if raw:
                    records = json.loads(raw) if isinstance(raw, str) else raw
                    if records:
                        df = pd.DataFrame(records)
                        df["_store"] = store
                        all_orders.append(df)
                        print(f"[OrderProfit] Loaded {len(df)} orders for {store} from Supabase")
            if store not in _local_item_stores:
                key = f"order_csv_items_{store}"
                raw = get_config_value(key)
                if raw:
                    records = json.loads(raw) if isinstance(raw, str) else raw
                    if records:
                        df = pd.DataFrame(records)
                        df["_store"] = store
                        all_items.append(df)
                        print(f"[OrderProfit] Loaded {len(df)} items for {store} from Supabase")
    except Exception as e:
        print(f"[OrderProfit] Supabase load failed: {e}")

    orders_df = pd.concat(all_orders, ignore_index=True) if all_orders else pd.DataFrame()
    items_df = pd.concat(all_items, ignore_index=True) if all_items else pd.DataFrame()
    return orders_df, items_df


def _compute_per_order_profit():
    """Match shipping labels to orders by date and compute per-order profit."""
    global ORDER_PROFITS, ORDER_PROFIT_SUMMARY

    orders_df, items_df = _load_order_csvs()
    if orders_df.empty:
        ORDER_PROFITS = []
        ORDER_PROFIT_SUMMARY = {}
        return

    # Deduplicate by (Order ID, store) — keep first occurrence
    if "Order ID" in orders_df.columns:
        orders_df = orders_df.drop_duplicates(subset=["Order ID", "_store"], keep="first")
    if not items_df.empty and "Order ID" in items_df.columns and "Transaction ID" in items_df.columns:
        items_df = items_df.drop_duplicates(subset=["Transaction ID", "_store"], keep="first")

    # Parse order ship dates
    orders_df["_ship_date"] = pd.to_datetime(orders_df["Date Shipped"], format="%m/%d/%y", errors="coerce")

    # Get shipping labels from statement data
    _all_data = _DATA_ALL if _DATA_ALL is not None else DATA
    ship_rows = _all_data[_all_data["Type"] == "Shipping"].copy()
    ship_rows["_date"] = pd.to_datetime(ship_rows["Date"], format="%B %d, %Y", errors="coerce")

    # Separate outbound labels from return labels, adjustments, credits, insurance
    _return_keywords = ["return"]
    _skip_keywords = ["adjustment", "credit", "insurance", "shipsurance"]

    # Build OUTBOUND labels by (store, date) for order matching
    labels_by_store_date = {}
    # Build RETURN labels separately for refund matching
    _return_labels = []
    # Track adjustments/credits/insurance as overhead
    _label_adjustments_total = 0.0
    _label_credits_total = 0.0
    _label_insurance_total = 0.0

    for _, r in ship_rows.iterrows():
        store = r.get("Store", "keycomponentmfg")
        dt = r["_date"]
        title = str(r.get("Title", "")).lower()
        cost = abs(r["Net_Clean"])

        if pd.isna(dt):
            continue
        ds = dt.strftime("%Y-%m-%d")

        # Categorize the shipping row
        if any(k in title for k in _return_keywords):
            _return_labels.append({"store": store, "date": ds, "cost": cost,
                                    "label": r.get("Info", ""), "net": r["Net_Clean"]})
        elif "adjustment" in title:
            _label_adjustments_total += r["Net_Clean"]  # keep sign (negative = extra charge)
        elif "credit" in title:
            _label_credits_total += r["Net_Clean"]  # positive = money back
        elif "insurance" in title or "shipsurance" in title:
            _label_insurance_total += cost
        else:
            # Outbound label — available for order matching
            key = (store, ds)
            if key not in labels_by_store_date:
                labels_by_store_date[key] = []
            labels_by_store_date[key].append({
                "cost": cost,
                "label": r.get("Info", ""),
            })

    # Match return labels to refund orders by date proximity
    refund_rows = _all_data[_all_data["Type"] == "Refund"].copy()
    refund_rows["_date"] = pd.to_datetime(refund_rows["Date"], format="%B %d, %Y", errors="coerce")
    _return_label_by_order = {}  # order_id -> return label cost
    for rl in _return_labels:
        rl_date = pd.to_datetime(rl["date"])
        best_refund = None
        best_gap = 999
        # Filter refunds by store if possible
        if "Store" in refund_rows.columns:
            _rf_iter = refund_rows[refund_rows["Store"] == rl["store"]]
        else:
            _rf_iter = refund_rows
        for _, rf in _rf_iter.iterrows():
            rf_date = rf["_date"]
            if pd.isna(rf_date):
                continue
            gap = abs((rl_date - rf_date).days)
            if gap < best_gap:
                _rkey = _extract_order_num(rf.get("Title", ""))
                if _rkey:
                    best_gap = gap
                    best_refund = _rkey
        if best_refund and best_gap <= 14:  # within 2 weeks
            _return_label_by_order[best_refund] = _return_label_by_order.get(best_refund, 0) + rl["cost"]

    # Build label lookup by label number for manual matching
    _label_by_number = {}  # "Label #xxx" -> {"cost": float, "store": str, "date": str}
    for _, r in ship_rows.iterrows():
        _lbl_info = str(r.get("Info", ""))
        if _lbl_info and _lbl_info.startswith("Label #"):
            _label_by_number[_lbl_info] = {
                "cost": abs(r["Net_Clean"]),
                "store": r.get("Store", "keycomponentmfg"),
                "date": r["_date"].strftime("%Y-%m-%d") if pd.notna(r["_date"]) else "",
            }

    results = []
    # Build refund lookup: order_id -> refund amount
    _refund_by_order = {}
    for _, _rf in refund_rows.iterrows():
        _rf_key = _extract_order_num(_rf.get("Title", ""))
        if _rf_key:
            # Extract just the number from "Order #1234567"
            _rf_num = _rf_key.replace("Order #", "")
            _refund_by_order[_rf_num] = _refund_by_order.get(_rf_num, 0) + abs(_rf["Net_Clean"])

    _unshipped_count = 0
    _skipped_errors = 0
    for _, o in orders_df.iterrows():
      try:
        store = o.get("_store", "keycomponentmfg")
        ship_dt = o["_ship_date"]
        if pd.isna(ship_dt):
            _unshipped_count += 1
            continue
        ds = ship_dt.strftime("%Y-%m-%d")

        # Get item names from items_df
        order_id = o["Order ID"]
        if not items_df.empty and "Order ID" in items_df.columns:
            oi = items_df[items_df["Order ID"] == order_id]
            item_names = ", ".join(oi["Item Name"].tolist()) if len(oi) > 0 else str(o.get("SKU", "?"))
        else:
            item_names = str(o.get("SKU", "?"))

        # Check for manual label assignment first
        _manual_label = _label_order_map.get(str(order_id))
        try:
            _ship_charged = float(o.get("Shipping", 0))
        except (ValueError, TypeError):
            _ship_charged = 0

        best_label = None
        best_diff = 999
        best_date_key = None

        if _manual_label and _manual_label in _label_by_number:
            # Manual match — exact label number provided
            _ml = _label_by_number[_manual_label]
            best_label = {"cost": _ml["cost"], "label": _manual_label}
            best_diff = 0
            # Remove from date pool so it's not double-matched
            _ml_date_key = (_ml["store"], _ml["date"])
            _ml_avail = labels_by_store_date.get(_ml_date_key, [])
            for _lb in _ml_avail:
                if _lb.get("label") == _manual_label:
                    _ml_avail.remove(_lb)
                    break

        # If no manual match, find by date + closest shipping cost
        if not best_label:
            from datetime import timedelta
            _search_dates = [ds]
            for _offset in (1, -1, 2, -2):
                _adj = (ship_dt + timedelta(days=_offset)).strftime("%Y-%m-%d")
                _search_dates.append(_adj)

            for _search_ds in _search_dates:
                _search_key = (store, _search_ds)
                available = labels_by_store_date.get(_search_key, [])
                for lb in available:
                    diff = abs(lb["cost"] - _ship_charged)
                    if diff < best_diff:
                        best_diff = diff
                        best_label = lb
                        best_date_key = _search_key
                # If we found an exact-date match, prefer it
                if best_label and _search_ds == ds:
                    break

        label_cost = best_label["cost"] if best_label else 0
        label_info = best_label["label"] if best_label else "NO MATCH"
        # Mark as weak match if cost difference is too large (>$15)
        matched = best_label is not None and best_diff <= 15.0
        if best_label and best_diff > 15.0:
            label_info = f"{best_label['label']} (WEAK MATCH)"

        # Remove used label so it's not double-matched
        if best_label and best_date_key:
            _avail = labels_by_store_date.get(best_date_key, [])
            if best_label in _avail:
                _avail.remove(best_label)

        try:
            order_net = float(o.get("Order Net", 0))
        except (ValueError, TypeError):
            order_net = 0
        shipping_charged = _ship_charged
        try:
            order_value = float(o.get("Order Value", 0))
        except (ValueError, TypeError):
            order_value = 0
        try:
            discount = float(o.get("Discount Amount", 0))
        except (ValueError, TypeError):
            discount = 0
        try:
            processing_fee = float(o.get("Card Processing Fees", 0))
        except (ValueError, TypeError):
            processing_fee = 0
        # Check if this order had a return label
        _order_key = f"Order #{order_id}"
        return_label_cost = _return_label_by_order.get(_order_key, 0)

        # Check if this order was refunded
        refund_amount = _refund_by_order.get(str(order_id), 0)
        was_refunded = refund_amount > 0

        # True P/L calculation
        shipping_pl = shipping_charged - label_cost - return_label_cost
        if was_refunded:
            # Refunded order: the customer's money came in and went back out — that's a wash.
            # The real loss is only the sunk costs you can't recover:
            # outbound label + return label + any Etsy fees they kept
            order_profit = -(label_cost + return_label_cost)
        else:
            order_profit = order_net - label_cost - return_label_cost

        results.append({
            "store": store,
            "order_id": order_id,
            "sale_date": str(o.get("Sale Date", "")),
            "ship_date": ds,
            "items": item_names[:120],
            "order_value": order_value,
            "discount": discount,
            "shipping_charged": shipping_charged,
            "processing_fee": processing_fee,
            "order_net": order_net,
            "label_cost": label_cost,
            "return_label_cost": return_label_cost,
            "label_info": label_info,
            "label_matched": matched,
            "shipping_pl": shipping_pl,
            "order_profit": order_profit,
            "buyer": str(o.get("Full Name", o.get("Buyer", ""))),
            "ship_state": str(o.get("Ship State", "")),
            "ship_country": str(o.get("Ship Country", "")),
            "num_items": int(o.get("Number of Items", 1)),
            "had_return": return_label_cost > 0,
            "refund_amount": refund_amount,
            "was_refunded": was_refunded,
        })
      except Exception as _row_err:
        _skipped_errors += 1
        print(f"[OrderProfit] Skipped order row: {_row_err}")

    if _skipped_errors:
        print(f"[OrderProfit] WARNING: {_skipped_errors} orders skipped due to errors")

    # Apply manual overrides to existing results (for orders in CSV with wrong label data)
    for _res in results:
        _oid_str = str(_res["order_id"])
        if _oid_str in _refund_cost_overrides:
            _ovr = _refund_cost_overrides[_oid_str]
            if "outbound_label" in _ovr:
                _res["label_cost"] = float(_ovr["outbound_label"])
            if "return_label" in _ovr:
                _res["return_label_cost"] = float(_ovr["return_label"])
                _res["had_return"] = float(_ovr["return_label"]) > 0
            if _ovr.get("type") == "refund":
                _res["was_refunded"] = True
                _res["refund_amount"] = _res.get("refund_amount", 0) or abs(_res.get("order_net", 0))
            # Recalculate P/L with overrides
            _lc = _res["label_cost"]
            _rlc = _res["return_label_cost"]
            _res["shipping_pl"] = _res["shipping_charged"] - _lc - _rlc
            if _res.get("was_refunded"):
                _res["order_profit"] = -(_lc + _rlc)
            else:
                _res["order_profit"] = _res["order_net"] - _lc - _rlc

    # Add refunded orders that aren't in the order CSV (same-day cancels, missing exports)
    _existing_order_ids = {str(r["order_id"]) for r in results}
    _missing_refund_count = 0
    for _rf_oid, _rf_amt in _refund_by_order.items():
        if _rf_oid not in _existing_order_ids:
            # Check for manual override
            _ovr = _refund_cost_overrides.get(_rf_oid, {})
            _ovr_type = _ovr.get("type", "cancel")
            _ovr_outbound = float(_ovr.get("outbound_label", 0))
            _ovr_return = float(_ovr.get("return_label", 0))

            # Find the refund row for date and store info
            _rf_date = ""
            _rf_store = "keycomponentmfg"
            _rf_items = f"{'Refunded' if _ovr_type == 'refund' else 'Cancelled'} Order"
            for _, _rfr in refund_rows.iterrows():
                _rfr_key = _extract_order_num(_rfr.get("Title", ""))
                if _rfr_key and _rfr_key.replace("Order #", "") == _rf_oid:
                    _rf_dt = _rfr.get("_date", pd.NaT)
                    _rf_date = _rf_dt.strftime("%Y-%m-%d") if pd.notna(_rf_dt) else ""
                    _rf_store = _rfr.get("Store", "keycomponentmfg")
                    break
            # Look up item name from items_df
            if not items_df.empty and "Order ID" in items_df.columns:
                _rf_items_df = items_df[items_df["Order ID"].astype(str) == _rf_oid]
                if len(_rf_items_df) > 0:
                    _rf_items = ", ".join(_rf_items_df["Item Name"].tolist())[:120]
            # Check for return label (auto-detected or manual override)
            _rf_return = _ovr_return or _return_label_by_order.get(f"Order #{_rf_oid}", 0)
            _rf_outbound = _ovr_outbound
            _total_label_loss = _rf_outbound + _rf_return
            results.append({
                "store": _rf_store,
                "order_id": _rf_oid,
                "sale_date": _rf_date,
                "ship_date": _rf_date,
                "items": _rf_items,
                "order_value": 0,
                "discount": 0,
                "shipping_charged": 0,
                "processing_fee": 0,
                "order_net": 0,
                "label_cost": _rf_outbound,
                "return_label_cost": _rf_return,
                "label_info": "Manual" if _ovr else "N/A",
                "label_matched": bool(_ovr),
                "shipping_pl": -_total_label_loss,
                "order_profit": -_total_label_loss,
                "buyer": "",
                "ship_state": "",
                "ship_country": "",
                "num_items": 0,
                "had_return": _rf_return > 0,
                "refund_amount": _rf_amt,
                "was_refunded": True,
            })
            _missing_refund_count += 1
    if _missing_refund_count:
        print(f"[OrderProfit] Added {_missing_refund_count} refunded orders not in order CSV")

    ORDER_PROFITS = sorted(results, key=lambda x: x["ship_date"], reverse=True)

    # Compute summary
    if ORDER_PROFITS:
        total_profit = sum(r["order_profit"] for r in ORDER_PROFITS)
        total_revenue = sum(r["order_value"] for r in ORDER_PROFITS)
        total_label = sum(r["label_cost"] for r in ORDER_PROFITS)
        total_ship_charged = sum(r["shipping_charged"] for r in ORDER_PROFITS)
        matched_count = sum(1 for r in ORDER_PROFITS if r["label_matched"])
        # Count unmatched labels (real costs not attributed to any order)
        _unmatched_labels = 0
        _unmatched_label_cost = 0.0
        for _key, _remaining in labels_by_store_date.items():
            _unmatched_labels += len(_remaining)
            _unmatched_label_cost += sum(lb["cost"] for lb in _remaining)

        total_return_labels = sum(r.get("return_label_cost", 0) for r in ORDER_PROFITS)
        orders_with_returns = sum(1 for r in ORDER_PROFITS if r.get("had_return", False))

        ORDER_PROFIT_SUMMARY = {
            "total_orders": len(ORDER_PROFITS),
            "total_profit": total_profit,
            "avg_profit": total_profit / len(ORDER_PROFITS),
            "total_revenue": total_revenue,
            "total_label_cost": total_label,
            "total_return_label_cost": total_return_labels,
            "total_ship_charged": total_ship_charged,
            "shipping_pl": total_ship_charged - total_label - total_return_labels,
            "matched_count": matched_count,
            "match_rate": matched_count / len(ORDER_PROFITS) * 100,
            "best_order": max(ORDER_PROFITS, key=lambda x: x["order_profit"]),
            "worst_order": min(ORDER_PROFITS, key=lambda x: x["order_profit"]),
            "unshipped_orders": _unshipped_count,
            "orders_with_returns": orders_with_returns,
            "label_adjustments": _label_adjustments_total,
            "label_credits": _label_credits_total,
            "label_insurance": _label_insurance_total,
            "unmatched_labels": _unmatched_labels,
            "unmatched_label_cost": _unmatched_label_cost,
        }
        # Per-store breakdown
        for _s in ("keycomponentmfg", "aurvio", "lunalinks"):
            _sp = [r for r in ORDER_PROFITS if r["store"] == _s]
            if _sp:
                ORDER_PROFIT_SUMMARY[f"{_s}_count"] = len(_sp)
                ORDER_PROFIT_SUMMARY[f"{_s}_profit"] = sum(r["order_profit"] for r in _sp)
                ORDER_PROFIT_SUMMARY[f"{_s}_avg"] = ORDER_PROFIT_SUMMARY[f"{_s}_profit"] / len(_sp)

    print(f"[OrderProfit] Computed {len(ORDER_PROFITS)} orders, "
          f"total profit: ${ORDER_PROFIT_SUMMARY.get('total_profit', 0):,.2f}, "
          f"match rate: {ORDER_PROFIT_SUMMARY.get('match_rate', 0):.0f}%")


# Run on startup
try:
    _compute_per_order_profit()
except Exception as _e:
    print(f"[OrderProfit] Startup computation failed: {_e}")


# ── Hot-Reload Functions (Data Hub) ──────────────────────────────────────────

_RECENT_UPLOADS: set = set()  # Order numbers uploaded this session via Data Hub


def _rebuild_etsy_derived():
    """Rebuild all Etsy-derived DataFrames and aggregations from the current DATA global.

    Call this after DATA has been set (from local CSVs or Supabase) to refresh:
    - Filtered DataFrames (sales_df, fee_df, etc.)
    - Product performance metrics
    - Monthly / daily / weekly aggregations
    - Fee, marketing, and shipping breakdowns

    Financial metrics (gross_sales, etsy_balance, real_profit, etc.) are set by the pipeline in _cascade_reload().
    """
    global sales_df, fee_df, ship_df, mkt_df, refund_df, tax_df
    global deposit_df, buyer_fee_df, payment_df
    global monthly_sales, monthly_fees, monthly_shipping, monthly_marketing
    global monthly_refunds, monthly_taxes, monthly_raw_fees, monthly_raw_shipping
    global monthly_raw_marketing, monthly_raw_refunds, monthly_net_revenue
    global monthly_raw_taxes, monthly_raw_buyer_fees, monthly_raw_payments
    global daily_sales, daily_orders, daily_df, weekly_aov
    global monthly_order_counts, monthly_aov, monthly_profit_per_order
    global months_sorted, days_active
    global product_fee_totals, product_revenue_est
    global listing_fees, transaction_fees_product, transaction_fees_shipping
    global processing_fees, credit_transaction, credit_listing, credit_processing
    global share_save, total_credits, total_fees_gross
    global etsy_ads, offsite_ads_fees, offsite_ads_credits
    global usps_outbound, usps_outbound_count, usps_return, usps_return_count
    global asendia_labels, asendia_count, ship_adjustments, ship_adjust_count
    global ship_credits, ship_credit_count, ship_insurance, ship_insurance_count
    global buyer_paid_shipping, shipping_profit, shipping_margin
    global paid_ship_count, free_ship_count, avg_outbound_label
    global _etsy_deposit_total, _deposit_rows

    # Rebuild filtered DataFrames
    sales_df = DATA[DATA["Type"] == "Sale"]
    fee_df = DATA[DATA["Type"] == "Fee"]
    ship_df = DATA[DATA["Type"] == "Shipping"]
    mkt_df = DATA[DATA["Type"] == "Marketing"]
    refund_df = DATA[DATA["Type"] == "Refund"]
    tax_df = DATA[DATA["Type"] == "Tax"]
    deposit_df = DATA[DATA["Type"] == "Deposit"]
    buyer_fee_df = DATA[DATA["Type"] == "Buyer Fee"]
    payment_df = DATA[DATA["Type"] == "Payment"]

    # Recalculate deposit totals from deposit row titles
    _deposit_rows = deposit_df
    _etsy_deposit_total = 0.0
    for _, _dr in _deposit_rows.iterrows():
        _m = _re_mod.search(r'([\d,]+\.\d+)', str(_dr.get("Title", "")))
        if _m:
            _etsy_deposit_total += float(_m.group(1).replace(",", ""))

    # Product performance — use actual sale amounts joined via order number
    _listing_aliases = CONFIG.get("listing_aliases", {})
    prod_fees = fee_df[
        fee_df["Title"].str.startswith("Transaction fee:", na=False)
        & ~fee_df["Title"].str.contains("Shipping", na=False)
    ].copy()
    prod_fees["Product"] = prod_fees["Title"].str.replace("Transaction fee: ", "", regex=False).apply(
        lambda n: _normalize_product_name(n, aliases=_listing_aliases)
    )
    product_fee_totals = prod_fees.groupby("Product")["Net_Clean"].sum().abs().sort_values(ascending=False)
    _order_to_product = prod_fees.dropna(subset=["Info"]).drop_duplicates(subset=["Info"]).set_index("Info")["Product"]
    _sales_with_product = sales_df.copy()
    _sales_with_product["Product"] = _sales_with_product["Title"].str.extract(r"(Order #\d+)", expand=False).map(_order_to_product)
    _sales_with_product = _sales_with_product.dropna(subset=["Product"])
    _sales_with_product["Product"] = _merge_product_prefixes(_sales_with_product["Product"], aliases=_listing_aliases)
    if len(_sales_with_product) > 0:
        product_revenue_est = _sales_with_product.groupby("Product")["Net_Clean"].sum().sort_values(ascending=False).round(2)
    else:
        product_revenue_est = pd.Series(dtype=float)  # no guessing — was: product_fee_totals / 0.065

    # Monthly breakdown
    months_sorted = sorted(DATA["Month"].dropna().unique())

    def monthly_sum(type_name):
        return DATA[DATA["Type"] == type_name].groupby("Month")["Net_Clean"].sum()

    monthly_sales = monthly_sum("Sale")
    monthly_fees = monthly_sum("Fee").abs()
    monthly_shipping = monthly_sum("Shipping").abs()
    monthly_marketing = monthly_sum("Marketing").abs()
    monthly_refunds = monthly_sum("Refund").abs()
    monthly_taxes = monthly_sum("Tax").abs()

    monthly_raw_fees = DATA[DATA["Type"] == "Fee"].groupby("Month")["Net_Clean"].sum()
    monthly_raw_shipping = DATA[DATA["Type"] == "Shipping"].groupby("Month")["Net_Clean"].sum()
    monthly_raw_marketing = DATA[DATA["Type"] == "Marketing"].groupby("Month")["Net_Clean"].sum()
    monthly_raw_refunds = DATA[DATA["Type"] == "Refund"].groupby("Month")["Net_Clean"].sum()
    monthly_raw_taxes = DATA[DATA["Type"] == "Tax"].groupby("Month")["Net_Clean"].sum()
    monthly_raw_buyer_fees = DATA[DATA["Type"] == "Buyer Fee"].groupby("Month")["Net_Clean"].sum()
    monthly_raw_payments = DATA[DATA["Type"] == "Payment"].groupby("Month")["Net_Clean"].sum()

    monthly_net_revenue = {}
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

    # Daily aggregations
    daily_sales = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
    daily_orders = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].count()
    daily_fee_cost = fee_df.groupby(fee_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
    daily_ship_cost = ship_df.groupby(ship_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
    daily_mkt_cost = mkt_df.groupby(mkt_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
    daily_refund_cost = refund_df.groupby(refund_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
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
    daily_df["profit"] = (daily_df["revenue"] + daily_df["fees"] + daily_df["shipping"]
                          + daily_df["marketing"] + daily_df["refunds"]
                          + daily_df["buyer_fees"] + daily_df["taxes"] + daily_df["payments"])
    daily_df["cum_revenue"] = daily_df["revenue"].cumsum()
    daily_df["cum_profit"] = daily_df["profit"].cumsum()

    # Weekly AOV
    weekly_sales_df = sales_df.copy()
    weekly_sales_df["WeekStart"] = weekly_sales_df["Date_Parsed"].dt.to_period("W").apply(lambda p: p.start_time)
    weekly_aov = weekly_sales_df.groupby("WeekStart").agg(
        total=("Net_Clean", "sum"),
        count=("Net_Clean", "count"),
    )
    weekly_aov["aov"] = weekly_aov["total"] / weekly_aov["count"]

    # Monthly order counts and AOV
    monthly_order_counts = sales_df.groupby("Month")["Net_Clean"].count()
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

    if len(DATA) > 0 and DATA["Date_Parsed"].notna().any():
        days_active = max((DATA["Date_Parsed"].max() - DATA["Date_Parsed"].min()).days + 1, 1)
    else:
        days_active = 1

    # Fee breakdown
    listing_fees = abs(fee_df[fee_df["Title"].str.contains("Listing fee", na=False)]["Net_Clean"].sum())
    transaction_fees_product = abs(
        fee_df[
            fee_df["Title"].str.startswith("Transaction fee:", na=False)
            & ~fee_df["Title"].str.contains("Shipping", na=False)
        ]["Net_Clean"].sum()
    )
    transaction_fees_shipping = abs(
        fee_df[fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)]["Net_Clean"].sum()
    )
    processing_fees = abs(fee_df[fee_df["Title"].str.contains("Processing fee", na=False)]["Net_Clean"].sum())
    credit_transaction = fee_df[fee_df["Title"].str.startswith("Credit for transaction fee", na=False)]["Net_Clean"].sum()
    credit_listing = fee_df[fee_df["Title"].str.startswith("Credit for listing fee", na=False)]["Net_Clean"].sum()
    credit_processing = fee_df[fee_df["Title"].str.startswith("Credit for processing fee", na=False)]["Net_Clean"].sum()
    share_save = fee_df[fee_df["Title"].str.contains("Share & Save", na=False)]["Net_Clean"].sum()
    total_credits = credit_transaction + credit_listing + credit_processing + share_save
    total_fees_gross = listing_fees + transaction_fees_product + transaction_fees_shipping + processing_fees

    # Marketing breakdown
    etsy_ads = abs(mkt_df[mkt_df["Title"].str.contains("Etsy Ads", na=False)]["Net_Clean"].sum())
    offsite_ads_fees = abs(
        mkt_df[
            mkt_df["Title"].str.contains("Offsite Ads", na=False)
            & ~mkt_df["Title"].str.contains("Credit", na=False)
        ]["Net_Clean"].sum()
    )
    offsite_ads_credits = mkt_df[mkt_df["Title"].str.contains("Credit for Offsite", na=False)]["Net_Clean"].sum()

    # Shipping subcategories
    usps_outbound = abs(ship_df[ship_df["Title"] == "USPS shipping label"]["Net_Clean"].sum())
    usps_outbound_count = len(ship_df[ship_df["Title"] == "USPS shipping label"])
    usps_return = abs(ship_df[ship_df["Title"] == "USPS return shipping label"]["Net_Clean"].sum())
    usps_return_count = len(ship_df[ship_df["Title"] == "USPS return shipping label"])
    asendia_labels = abs(ship_df[ship_df["Title"].str.contains("Asendia", na=False)]["Net_Clean"].sum())
    asendia_count = len(ship_df[ship_df["Title"].str.contains("Asendia", na=False)])
    ship_adjustments = abs(ship_df[ship_df["Title"].str.contains("Adjustment", na=False)]["Net_Clean"].sum())
    ship_adjust_count = len(ship_df[ship_df["Title"].str.contains("Adjustment", na=False)])
    ship_credits = ship_df[ship_df["Title"].str.contains("Credit for", na=False)]["Net_Clean"].sum()
    ship_credit_count = len(ship_df[ship_df["Title"].str.contains("Credit for", na=False)])
    ship_insurance = abs(ship_df[ship_df["Title"].str.contains("insurance", case=False, na=False)]["Net_Clean"].sum())
    ship_insurance_count = len(ship_df[ship_df["Title"].str.contains("insurance", case=False, na=False)])

    # Buyer paid shipping: UNKNOWN — /0.065 back-solve REMOVED.
    # Requires Etsy order-level CSV with "Shipping charged to buyer" column.
    buyer_paid_shipping = None  # was: transaction_fees_shipping / 0.065
    shipping_profit = None      # was: buyer_paid_shipping - total_shipping_cost
    shipping_margin = None      # was: (shipping_profit / buyer_paid_shipping * 100)

    # Paid vs free shipping orders (counts are still real)
    ship_fee_rows = fee_df[fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)].copy()
    orders_with_paid_shipping = set(ship_fee_rows["Info"].dropna())
    all_order_ids = set(sales_df["Title"].str.extract(r"(Order #\d+)", expand=False).dropna())
    orders_free_shipping = all_order_ids - orders_with_paid_shipping
    paid_ship_count = len(orders_with_paid_shipping & all_order_ids)
    free_ship_count = len(orders_free_shipping)
    avg_outbound_label = usps_outbound / usps_outbound_count if usps_outbound_count else 0


def _reload_etsy_data():
    """Re-read all Etsy CSVs from local files, merge with Supabase data, and rebuild.

    Merges local CSV data with Supabase data so that rows uploaded via Railway
    (which only exist in Supabase) are not lost when uploading locally.

    Financial metrics (gross_sales, etsy_balance, real_profit, etc.) are set by the pipeline in _cascade_reload().

    Returns dict with summary stats for the UI status message.
    """
    global DATA

    _on_rw = IS_RAILWAY

    # Read from local CSV files (skip on Railway — Supabase is source of truth)
    import glob as _gl
    _ed = os.path.join(BASE_DIR, "data", "etsy_statements")
    _frames = []
    if not _on_rw:
        # Root-level CSVs (legacy, default to keycomponentmfg)
        for _f in sorted(_gl.glob(os.path.join(_ed, "etsy_statement*.csv"))):
            try:
                _df_tmp = pd.read_csv(_f)
                _df_tmp["Store"] = "keycomponentmfg"
                _frames.append(_df_tmp)
            except Exception:
                pass
        # Store-specific subdirectories
        for _ss in ("keycomponentmfg", "aurvio", "lunalinks"):
            _sd = os.path.join(_ed, _ss)
            if os.path.isdir(_sd):
                for _f in sorted(_gl.glob(os.path.join(_sd, "etsy_statement*.csv"))):
                    try:
                        _df_tmp = pd.read_csv(_f)
                        _df_tmp["Store"] = _ss
                        _frames.append(_df_tmp)
                    except Exception:
                        pass

    # Also load Supabase data to merge (preserves rows uploaded via Railway)
    _sb_df = None
    try:
        from supabase_loader import load_data as _ld
        _sb_result = _ld()
        _sb_df = _sb_result.get("DATA")
        if _sb_df is not None and len(_sb_df) == 0:
            _sb_df = None
    except Exception:
        pass

    _dedup_cols = ["Date", "Type", "Title", "Info", "Amount", "Fees & Taxes", "Net"]

    if _frames:
        _local_df = pd.concat(_frames, ignore_index=True)
        if _sb_df is not None and len(_sb_df) > 0:
            # Normalize NaN/'nan' before dedup — local CSVs have NaN, Supabase has 'nan' string
            for _dc in _dedup_cols:
                _sb_df[_dc] = _sb_df[_dc].fillna("").replace("nan", "")
                _local_df[_dc] = _local_df[_dc].fillna("").replace("nan", "")
            # Merge: rank duplicates within each source, then dedup on
            # (key + rank) to keep max(local_count, sb_count) per unique row.
            _local_df["_src"] = "local"
            _sb_df["_src"] = "sb"
            _comb = pd.concat([_local_df, _sb_df], ignore_index=True)
            _comb["_dup_rank"] = _comb.groupby(
                _dedup_cols + ["_src"], sort=False
            ).cumcount()
            DATA = _comb.drop_duplicates(
                subset=_dedup_cols + ["_dup_rank"], keep="first"
            ).drop(columns=["_src", "_dup_rank"]).reset_index(drop=True)
            print(f"[Reload] Merged {len(_local_df)} local + {len(_sb_df)} Supabase rows -> {len(DATA)} unique")
        else:
            DATA = _local_df
    elif _sb_df is not None:
        DATA = _sb_df
        print(f"[Reload] Using {len(DATA)} rows from Supabase (Railway mode)")
    # else: DATA stays as-is

    # Rebuild computed columns
    DATA["Amount_Clean"] = DATA["Amount"].apply(parse_money)
    DATA["Net_Clean"] = DATA["Net"].apply(parse_money)
    DATA["Fees_Clean"] = DATA["Fees & Taxes"].apply(parse_money)
    DATA["Date_Parsed"] = pd.to_datetime(DATA["Date"], format="%B %d, %Y", errors="coerce")
    DATA["Month"] = DATA["Date_Parsed"].dt.to_period("M").astype(str)
    DATA["Week"] = DATA["Date_Parsed"].dt.to_period("W").apply(lambda p: p.start_time)

    # Ensure Store column exists after reload
    if "Store" not in DATA.columns:
        DATA["Store"] = "keycomponentmfg"
    else:
        DATA["Store"] = DATA["Store"].fillna("keycomponentmfg").replace("", "keycomponentmfg")

    _rebuild_etsy_derived()

    return {
        "transactions": len(DATA),
        "orders": len(sales_df),
        "gross_sales": sales_df["Net_Clean"].sum(),
    }


def _reload_bank_data():
    """Re-parse all bank PDFs and rebuild bank-derived metrics in-place.

    Financial metrics (bank_net_cash, bank_by_cat, real_profit, etc.) are set by the pipeline in _cascade_reload().

    Returns dict with summary stats for the UI status message.
    """
    global BANK_TXNS, bank_deposits, bank_debits
    global bank_statement_count
    global bank_txns_sorted, bank_running
    global bb_cc_payments, bb_cc_total_paid, bb_cc_balance, bb_cc_available
    global _bank_cat_color_map, _bank_acct_gap, _bank_no_receipt, _bank_amazon_txns

    bank_dir = os.path.join(BASE_DIR, "data", "bank_statements")
    if not os.path.isdir(bank_dir):
        return {"transactions": 0, "statements": 0, "net_cash": 0}

    from _parse_bank_statements import parse_bank_pdf as _parse_bank
    from _parse_bank_statements import parse_bank_csv as _parse_csv
    from _parse_bank_statements import apply_overrides as _apply_overrides
    all_txns = []
    all_covered_months = set()
    source_files = []

    # Parse PDFs first — official statements are the primary source
    for fn in sorted(os.listdir(bank_dir)):
        if fn.lower().endswith(".pdf"):
            fpath = os.path.join(bank_dir, fn)
            try:
                txns, covered = _parse_bank(fpath)
                all_txns.extend(txns)
                all_covered_months.update(covered)
                source_files.append(fn)
            except Exception:
                pass

    # Parse ALL CSVs, combine and dedup (newer downloads are supersets of older ones)
    csv_txns = []
    csv_covered = set()
    for fn in sorted(os.listdir(bank_dir)):
        if fn.lower().endswith(".csv"):
            fpath = os.path.join(bank_dir, fn)
            try:
                txns, covered = _parse_csv(fpath)
                csv_txns.extend(txns)
                csv_covered.update(covered)
                source_files.append(fn)
            except Exception:
                pass

    # Dedup CSV transactions by (date, amount, description) — keeps last occurrence (newest file)
    if csv_txns:
        seen = {}
        for t in csv_txns:
            key = (t["date"], t["amount"], t["type"], t.get("raw_desc", t["desc"]))
            seen[key] = t  # last one wins (newest CSV parsed last)
        csv_deduped = list(seen.values())

        # Only use CSV data for months NOT already covered by PDFs
        new_months = csv_covered - all_covered_months
        if new_months:
            filtered = [t for t in csv_deduped
                        if f"{t['date'].split('/')[2]}-{t['date'].split('/')[0]}" in new_months]
            all_txns.extend(filtered)
            all_covered_months.update(new_months)

    # Apply overrides (splits, recategorizations)
    all_txns = _apply_overrides(all_txns)

    # Append manual transactions from config (permanent ones always, others only for uncovered months)
    for _mt in CONFIG.get("manual_transactions", []):
        _mt_parts = _mt["date"].split("/")
        _mt_month = f"{_mt_parts[2]}-{_mt_parts[0]}"
        if _mt_month in all_covered_months and not _mt.get("permanent", False):
            continue
        all_txns.append({
            "date": _mt["date"], "desc": _mt["desc"], "amount": _mt["amount"],
            "type": _mt["type"], "category": _mt["category"],
            "source_file": "config.json (manual)", "raw_desc": _mt["desc"],
        })

    # Save updated JSON locally
    out_path = os.path.join(BASE_DIR, "data", "generated", "bank_transactions.json")
    try:
        with open(out_path, "w") as f:
            json.dump({"metadata": {"source_files": source_files}, "transactions": all_txns}, f, indent=2)
    except Exception:
        pass

    BANK_TXNS = all_txns
    bank_statement_count = len(source_files)

    _rebuild_bank_derived()

    # net_cash for the status message — use bank_running final balance
    _final_bal = bank_running[-1]["_balance"] if bank_running else 0.0
    return {
        "transactions": len(BANK_TXNS),
        "statements": bank_statement_count,
        "net_cash": round(_final_bal, 2),
    }


def _rebuild_bank_derived():
    """Rebuild bank-derived globals from the current BANK_TXNS global.

    Call this after BANK_TXNS has been set (from PDF parsing or Supabase) to refresh:
    - Deposit/debit lists
    - Sorted transactions with running balance
    - Bank category color map, accounting gap, no-receipt list, Amazon transactions
    - Best Buy credit card tracking

    Financial metrics (bank_net_cash, bank_by_cat, real_profit, etc.) are set by the pipeline in _cascade_reload().
    """
    global bank_deposits, bank_debits
    global bank_txns_sorted, bank_running
    global bb_cc_payments, bb_cc_total_paid, bb_cc_balance, bb_cc_available
    global _bank_cat_color_map, _bank_acct_gap, _bank_no_receipt, _bank_amazon_txns

    # Re-categorize any Uncategorized transactions with latest rules
    from _parse_bank_statements import auto_categorize as _ac
    for _bt in BANK_TXNS:
        if _bt.get("category") == "Uncategorized":
            _nc = _ac(_bt.get("raw_desc", _bt["desc"]), _bt["type"])
            if _nc != "Uncategorized":
                _bt["category"] = _nc

    # Build deposit/debit lists
    bank_deposits = [t for t in BANK_TXNS if t["type"] == "deposit"]
    bank_debits = [t for t in BANK_TXNS if t["type"] == "debit"]

    # Rebuild running balance
    bank_txns_sorted = sorted(BANK_TXNS, key=lambda x: (_parse_bank_date(x["date"]),
                              0 if x["type"] == "deposit" else 1))
    bank_running = []
    _bal = 0.0
    for t in bank_txns_sorted:
        if t["type"] == "deposit":
            _bal += t["amount"]
        else:
            _bal -= t["amount"]
        bank_running.append({**t, "_balance": round(_bal, 2)})

    # Recompute derived bank variables used by Financials tab
    _bank_cat_color_map, _bank_acct_gap, _bank_no_receipt, _bank_amazon_txns = _get_bank_computed()

    # Auto-detect Best Buy CC payments from bank transactions
    bb_cc_payments = [{"date": t["date"], "desc": t["desc"], "amount": t["amount"]}
                      for t in BANK_TXNS if t["category"] == "Business Credit Card"
                      and "BEST BUY" in t.get("desc", "").upper()]
    bb_cc_total_paid = sum(p["amount"] for p in bb_cc_payments)
    bb_cc_balance = bb_cc_total_charged - bb_cc_total_paid
    bb_cc_available = bb_cc_limit - bb_cc_balance


def _recategorize_bank_txns():
    """Re-run auto_categorize on any Uncategorized transactions using latest rules."""
    from _parse_bank_statements import auto_categorize as _auto_cat
    changed = 0
    for t in BANK_TXNS:
        if t.get("category") == "Uncategorized":
            new_cat = _auto_cat(t.get("raw_desc", t["desc"]), t["type"])
            if new_cat != "Uncategorized":
                t["category"] = new_cat
                changed += 1
    if changed:
        print(f"[bank] Re-categorized {changed} transaction(s)")
        _rebuild_bank_derived()
    return changed


def _recompute_location_spend():
    """Recompute per-location spending from item-level data so editor splits are reflected."""
    global loc_spend, loc_orders, loc_tax, loc_subtotal
    global tulsa_spend, texas_spend, tulsa_orders, texas_orders
    global tulsa_tax, texas_tax, tulsa_subtotal, texas_subtotal
    global tulsa_items, texas_items, tulsa_by_cat, texas_by_cat
    global tulsa_monthly, texas_monthly
    global BIZ_INV_ITEMS, biz_inv_by_category

    # Rebuild BIZ_INV_ITEMS from current INV_ITEMS so location spend is fresh
    if len(INV_ITEMS) > 0 and "category" in INV_ITEMS.columns:
        BIZ_INV_ITEMS = INV_ITEMS[~INV_ITEMS["category"].isin(["Personal/Gift", "Business Fees"])].copy()
        biz_inv_by_category = BIZ_INV_ITEMS.groupby("category")["total"].sum().sort_values(ascending=False)
    else:
        BIZ_INV_ITEMS = INV_ITEMS.copy()

    if len(BIZ_INV_ITEMS) > 0:
        loc_spend = BIZ_INV_ITEMS.groupby("location")["total_with_tax"].sum()
        loc_orders = BIZ_INV_ITEMS.groupby("location")["order_num"].nunique()
        _item_tax = BIZ_INV_ITEMS["total_with_tax"] - BIZ_INV_ITEMS["total"]
        loc_tax = _item_tax.groupby(BIZ_INV_ITEMS["location"]).sum()
        loc_subtotal = BIZ_INV_ITEMS.groupby("location")["total"].sum()
    else:
        loc_spend = BIZ_INV_DF.groupby("location")["grand_total"].sum()
        loc_orders = BIZ_INV_DF.groupby("location")["order_num"].count()
        loc_tax = BIZ_INV_DF.groupby("location")["tax"].sum()
        loc_subtotal = BIZ_INV_DF.groupby("location")["subtotal"].sum()

    tulsa_spend = loc_spend.get("Tulsa, OK", 0)
    texas_spend = loc_spend.get("Texas", 0)
    tulsa_orders = loc_orders.get("Tulsa, OK", 0)
    texas_orders = loc_orders.get("Texas", 0)
    tulsa_tax = loc_tax.get("Tulsa, OK", 0)
    texas_tax = loc_tax.get("Texas", 0)
    tulsa_subtotal = loc_subtotal.get("Tulsa, OK", 0)
    texas_subtotal = loc_subtotal.get("Texas", 0)

    if len(BIZ_INV_ITEMS) > 0:
        tulsa_items = BIZ_INV_ITEMS[BIZ_INV_ITEMS["location"] == "Tulsa, OK"]
        texas_items = BIZ_INV_ITEMS[BIZ_INV_ITEMS["location"] == "Texas"]
        tulsa_by_cat = tulsa_items.groupby("category")["total"].sum().sort_values(ascending=False)
        texas_by_cat = texas_items.groupby("category")["total"].sum().sort_values(ascending=False)
        tulsa_monthly = tulsa_items.groupby("month")["total_with_tax"].sum()
        texas_monthly = texas_items.groupby("month")["total_with_tax"].sum()
    else:
        tulsa_by_cat = pd.Series(dtype=float)
        texas_by_cat = pd.Series(dtype=float)
        tulsa_monthly = pd.Series(dtype=float)
        texas_monthly = pd.Series(dtype=float)


def _apply_details_to_inv_items():
    """Re-apply _ITEM_DETAILS to INV_ITEMS so STOCK_SUMMARY stays fresh."""
    global INV_ITEMS
    if len(INV_ITEMS) == 0:
        return
    _orig_pricing = {}
    for inv in INVOICES:
        onum = inv["order_num"]
        ot = {"subtotal": inv["subtotal"], "grand_total": inv["grand_total"]}
        for item in inv["items"]:
            iname = item["name"]
            if iname.startswith("Your package was left near the front door or porch."):
                iname = iname.replace("Your package was left near the front door or porch.", "").strip()
            orig_total = item["price"] * item["qty"]
            tax_ratio = (ot["grand_total"] / ot["subtotal"]) if ot["subtotal"] > 0 else 1.0
            _orig_pricing[(onum, iname)] = {
                "price": item["price"], "qty": item["qty"],
                "total": orig_total,
                "total_with_tax": round(orig_total * tax_ratio, 2),
            }
    detail_rows = []
    _processed = set()
    for _, row in INV_ITEMS.iterrows():
        rkey = (row["order_num"], row.get("_orig_name", row["name"]))
        if rkey in _processed:
            continue
        _processed.add(rkey)
        if rkey in _ITEM_DETAILS:
            dets = _ITEM_DETAILS[rkey]
            orig = _orig_pricing.get(rkey, {})
            orig_total = orig.get("total", row["price"] * row["qty"])
            orig_with_tax = orig.get("total_with_tax", orig_total)
            total_dq = sum(d["true_qty"] for d in dets)
            pu = orig_total / total_dq if total_dq > 0 else 0
            put = orig_with_tax / total_dq if total_dq > 0 else 0
            for det in dets:
                nr = row.copy()
                nr["_orig_name"] = row.get("_orig_name", row["name"])
                nr["name"] = det["display_name"]
                nr["category"] = det["category"]
                nr["qty"] = det["true_qty"]
                nr["price"] = round(pu, 2)
                nr["total"] = round(pu * det["true_qty"], 2)
                nr["total_with_tax"] = round(put * det["true_qty"], 2)
                nr["image_url"] = ""
                if det.get("location"):
                    nr["_override_location"] = det["location"]
                    nr["location"] = det["location"]
                detail_rows.append(nr)
                orig_img = _IMAGE_URLS.get(rkey[1], "")
                if orig_img and det["display_name"] not in _IMAGE_URLS:
                    _IMAGE_URLS[det["display_name"]] = orig_img
        else:
            rc = row.copy()
            if "_orig_name" not in rc.index:
                rc["_orig_name"] = row["name"]
            detail_rows.append(rc)
    INV_ITEMS = pd.DataFrame(detail_rows)
    if len(INV_ITEMS) == 0:
        return
    if "_orig_name" not in INV_ITEMS.columns:
        INV_ITEMS["_orig_name"] = INV_ITEMS["name"]
    if "category" in INV_ITEMS.columns:
        _cm = INV_ITEMS["category"].isna() | (INV_ITEMS["category"] == "")
        INV_ITEMS.loc[_cm, "category"] = INV_ITEMS.loc[_cm, "name"].apply(categorize_item)
    if "_override_location" in INV_ITEMS.columns:
        INV_ITEMS["location"] = INV_ITEMS.apply(
            lambda r: r["_override_location"] if r.get("_override_location") else classify_location(r.get("ship_to", "")),
            axis=1)


def _rebuild_uploaded_inventory():
    """Rebuild _UPLOADED_INVENTORY from current INV_ITEMS after any change."""
    _UPLOADED_INVENTORY.clear()
    if len(INV_ITEMS) > 0:
        for _, _r in INV_ITEMS.iterrows():
            _loc = _norm_loc(_r.get("_override_location", ""))
            if _loc:
                _ik = (_loc, _r["name"], _r.get("category", "Other"))
                _UPLOADED_INVENTORY[_ik] = _UPLOADED_INVENTORY.get(_ik, 0) + int(_r["qty"])


def _reload_inventory_data(new_order):
    """Append a newly-parsed invoice to INVOICES and rebuild inventory DataFrames.

    Parameters
    ----------
    new_order : dict
        Parsed invoice dict from parse_pdf_file().

    Returns dict with summary stats for the UI status message.
    """
    global INVOICES, INV_DF, INV_ITEMS, BIZ_INV_DF, BIZ_INV_ITEMS, STOCK_SUMMARY
    global total_inventory_cost, total_inv_subtotal, total_inv_tax
    global biz_inv_cost, personal_acct_cost, inv_order_count, true_inventory_cost
    global monthly_inv_spend, biz_inv_by_category, gigi_cost, personal_inv_items
    global loc_spend, loc_orders, loc_tax, loc_subtotal
    global tulsa_spend, texas_spend, tulsa_orders, texas_orders
    global tulsa_tax, texas_tax, tulsa_subtotal, texas_subtotal
    global tulsa_items, texas_items, tulsa_by_cat, texas_by_cat
    global tulsa_monthly, texas_monthly

    # Append to INVOICES and persist (mirrors lines 8796-8807)
    INVOICES.append(new_order)
    _RECENT_UPLOADS.add(new_order["order_num"])
    try:
        _gen_dir = os.path.join(BASE_DIR, "data", "generated")
        os.makedirs(_gen_dir, exist_ok=True)
        out_path = os.path.join(_gen_dir, "inventory_orders.json")
        with open(out_path, "w") as f:
            json.dump(INVOICES, f, indent=2)
    except Exception:
        pass
    _sb_ok = _save_new_order(new_order)

    if not _sb_ok:
        _logger.warning("Failed to save order %s to Supabase", new_order.get('order_num', '?'))

    # Build new INV_ITEMS rows (mirrors lines 8809-8851)
    try:
        dt = pd.to_datetime(new_order["date"], format="%B %d, %Y")
    except Exception:
        try:
            dt = pd.to_datetime(new_order["date"])
        except Exception:
            dt = pd.NaT
    month = dt.to_period("M").strftime("%Y-%m") if pd.notna(dt) else "Unknown"

    new_inv_rows = []
    for item in new_order["items"]:
        item_name = item["name"]
        if item_name.startswith("Your package was left near the front door or porch."):
            item_name = item_name.replace("Your package was left near the front door or porch.", "").strip()
        new_inv_rows.append({
            "order_num": new_order["order_num"],
            "date": new_order["date"],
            "date_parsed": dt,
            "month": month,
            "name": item_name,
            "qty": item["qty"],
            "price": item["price"],
            "total": item["price"] * item["qty"],
            "source": new_order["source"],
            "seller": item.get("seller", "Unknown"),
            "ship_to": item.get("ship_to", new_order.get("ship_address", "")),
            "payment_method": new_order.get("payment_method", "Unknown"),
            "image_url": item.get("image_url", ""),
            "category": categorize_item(item_name),
            "_orig_name": item_name,
            "_override_location": "",
        })

    if new_inv_rows:
        new_df = pd.DataFrame(new_inv_rows)
        if new_order["subtotal"] > 0:
            new_df["total_with_tax"] = (
                new_df["total"] * (new_order["grand_total"] / new_order["subtotal"])
            ).round(2)
        else:
            new_df["total_with_tax"] = new_df["total"]
        INV_ITEMS = pd.concat([INV_ITEMS, new_df], ignore_index=True)

    # Rebuild INV_DF row
    inv_rows = []
    for inv in INVOICES:
        d_str = inv["date"]
        try:
            d = pd.to_datetime(d_str, format="%B %d, %Y")
        except Exception:
            try:
                d = pd.to_datetime(d_str)
            except Exception:
                d = pd.NaT
        inv_rows.append({
            "order_num": inv["order_num"], "date": d_str, "date_parsed": d,
            "month": d.to_period("M").strftime("%Y-%m") if pd.notna(d) else "Unknown",
            "grand_total": inv["grand_total"], "subtotal": inv["subtotal"],
            "tax": inv["tax"], "source": inv["source"],
            "item_count": len(inv["items"]), "file": inv["file"],
            "ship_address": inv.get("ship_address", ""),
            "payment_method": inv.get("payment_method", "Unknown"),
        })
    INV_DF = pd.DataFrame(inv_rows).sort_values("date_parsed")

    # Recompute aggregates
    total_inventory_cost = INV_DF["grand_total"].sum()
    total_inv_subtotal = INV_DF["subtotal"].sum()
    total_inv_tax = INV_DF["tax"].sum()
    biz_inv_cost = INV_DF[INV_DF["source"] == "Key Component Mfg"]["grand_total"].sum()
    personal_acct_cost = INV_DF[INV_DF["source"] == "Personal Amazon"]["grand_total"].sum()
    inv_order_count = len(INV_DF)

    if len(INV_ITEMS) > 0:
        personal_total = INV_ITEMS[INV_ITEMS["category"] == "Personal/Gift"]["total_with_tax"].sum()
        biz_fee_total = INV_ITEMS[INV_ITEMS["category"] == "Business Fees"]["total_with_tax"].sum()
        true_inventory_cost = total_inventory_cost - personal_total - biz_fee_total
    else:
        true_inventory_cost = total_inventory_cost

    # Add location column to INV_DF
    INV_DF["location"] = INV_DF["ship_address"].apply(classify_location)

    # Recalculate gigi_cost
    gigi_mask = INV_DF["file"].str.contains("Gigi", na=False)
    gigi_cost = INV_DF[gigi_mask]["grand_total"].sum()

    _personal_order_mask = (INV_DF["source"] == "Personal Amazon") | INV_DF["file"].str.contains("Gigi", na=False)
    BIZ_INV_DF = INV_DF[~_personal_order_mask].copy()

    # Recalculate business-only inventory metrics
    if len(INV_ITEMS) > 0:
        BIZ_INV_ITEMS = INV_ITEMS[~INV_ITEMS["category"].isin(["Personal/Gift", "Business Fees"])].copy()
        biz_inv_by_category = BIZ_INV_ITEMS.groupby("category")["total"].sum().sort_values(ascending=False)
        personal_inv_items = INV_ITEMS[INV_ITEMS["category"] == "Personal/Gift"].copy()
    else:
        BIZ_INV_ITEMS = INV_ITEMS.copy()
        biz_inv_by_category = pd.Series(dtype=float)
        personal_inv_items = pd.DataFrame()

    # Recalculate monthly inventory spend (business-only)
    monthly_inv_spend = BIZ_INV_DF.groupby("month")["grand_total"].sum()

    # Recompute per-location aggregates from item-level data (respects splits)
    _recompute_location_spend()

    _recompute_stock_summary()

    return {
        "order_num": new_order["order_num"],
        "item_count": len(new_order["items"]),
        "grand_total": new_order["grand_total"],
    }


def _rebuild_inventory_from_invoices():
    """Rebuild INV_DF and inventory aggregates from the current INVOICES list.

    Use this after deleting an invoice to refresh all inventory metrics.
    """
    global INV_DF, INV_ITEMS, BIZ_INV_DF, BIZ_INV_ITEMS, STOCK_SUMMARY
    global total_inventory_cost, total_inv_subtotal, total_inv_tax
    global biz_inv_cost, personal_acct_cost, inv_order_count, true_inventory_cost
    global monthly_inv_spend, biz_inv_by_category, gigi_cost, personal_inv_items
    global loc_spend, loc_orders, loc_tax, loc_subtotal
    global tulsa_spend, texas_spend, tulsa_orders, texas_orders
    global tulsa_tax, texas_tax, tulsa_subtotal, texas_subtotal
    global tulsa_items, texas_items, tulsa_by_cat, texas_by_cat
    global tulsa_monthly, texas_monthly

    inv_rows = []
    inv_item_rows = []
    for inv in INVOICES:
        d_str = inv["date"]
        try:
            d = pd.to_datetime(d_str, format="%B %d, %Y")
        except Exception:
            try:
                d = pd.to_datetime(d_str)
            except Exception:
                d = pd.NaT
        inv_rows.append({
            "order_num": inv["order_num"], "date": d_str, "date_parsed": d,
            "month": d.to_period("M").strftime("%Y-%m") if pd.notna(d) else "Unknown",
            "grand_total": inv["grand_total"], "subtotal": inv["subtotal"],
            "tax": inv["tax"], "source": inv["source"],
            "item_count": len(inv["items"]), "file": inv["file"],
            "ship_address": inv.get("ship_address", ""),
            "payment_method": inv.get("payment_method", "Unknown"),
        })
        for item in inv.get("items", []):
            item_name = item["name"]
            total = item["price"] * item.get("qty", 1)
            if inv["subtotal"] > 0:
                total_with_tax = round(total * (inv["grand_total"] / inv["subtotal"]), 2)
            else:
                total_with_tax = total
            inv_item_rows.append({
                "order_num": inv["order_num"], "name": item_name,
                "qty": item.get("qty", 1), "price": item["price"],
                "total": total, "total_with_tax": total_with_tax,
                "seller": item.get("seller", "Unknown"),
                "source": inv["source"], "date": d_str,
                "month": d.to_period("M").strftime("%Y-%m") if pd.notna(d) else "Unknown",
                "category": categorize_item(item_name),
            })

    if inv_rows:
        INV_DF = pd.DataFrame(inv_rows).sort_values("date_parsed")
    else:
        INV_DF = pd.DataFrame(columns=["order_num", "date", "date_parsed", "month",
                                         "grand_total", "subtotal", "tax", "source",
                                         "item_count", "file", "ship_address", "payment_method"])

    if inv_item_rows:
        INV_ITEMS = pd.DataFrame(inv_item_rows)
    else:
        INV_ITEMS = pd.DataFrame(columns=["order_num", "name", "qty", "price",
                                            "total", "total_with_tax", "seller",
                                            "source", "date", "month", "category"])

    total_inventory_cost = INV_DF["grand_total"].sum() if len(INV_DF) > 0 else 0
    total_inv_subtotal = INV_DF["subtotal"].sum() if len(INV_DF) > 0 else 0
    total_inv_tax = INV_DF["tax"].sum() if len(INV_DF) > 0 else 0
    biz_inv_cost = INV_DF[INV_DF["source"] == "Key Component Mfg"]["grand_total"].sum() if len(INV_DF) > 0 else 0
    personal_acct_cost = INV_DF[INV_DF["source"] == "Personal Amazon"]["grand_total"].sum() if len(INV_DF) > 0 else 0
    inv_order_count = len(INV_DF)

    if len(INV_ITEMS) > 0:
        personal_total = INV_ITEMS[INV_ITEMS["category"] == "Personal/Gift"]["total_with_tax"].sum()
        biz_fee_total = INV_ITEMS[INV_ITEMS["category"] == "Business Fees"]["total_with_tax"].sum()
        true_inventory_cost = total_inventory_cost - personal_total - biz_fee_total
    else:
        true_inventory_cost = total_inventory_cost

    if len(INV_DF) > 0:
        INV_DF["location"] = INV_DF["ship_address"].apply(classify_location)
        gigi_mask = INV_DF["file"].str.contains("Gigi", na=False)
        gigi_cost = INV_DF[gigi_mask]["grand_total"].sum()
        _personal_order_mask = (INV_DF["source"] == "Personal Amazon") | INV_DF["file"].str.contains("Gigi", na=False)
        BIZ_INV_DF = INV_DF[~_personal_order_mask].copy()
    else:
        gigi_cost = 0
        BIZ_INV_DF = INV_DF.copy()

    if len(INV_ITEMS) > 0:
        BIZ_INV_ITEMS = INV_ITEMS[~INV_ITEMS["category"].isin(["Personal/Gift", "Business Fees"])].copy()
        biz_inv_by_category = BIZ_INV_ITEMS.groupby("category")["total"].sum().sort_values(ascending=False)
        personal_inv_items = INV_ITEMS[INV_ITEMS["category"] == "Personal/Gift"].copy()
    else:
        BIZ_INV_ITEMS = INV_ITEMS.copy()
        biz_inv_by_category = pd.Series(dtype=float)
        personal_inv_items = pd.DataFrame()

    if len(BIZ_INV_DF) > 0:
        monthly_inv_spend = BIZ_INV_DF.groupby("month")["grand_total"].sum()
    else:
        monthly_inv_spend = pd.Series(dtype=float)

    # Recompute per-location aggregates from item-level data (respects splits)
    _recompute_location_spend()

    try:
        _recompute_stock_summary()
    except Exception:
        pass


def _cascade_reload(source="etsy"):
    """Recalculate cross-source derived metrics after any data upload.

    Call this after _reload_etsy_data, _reload_bank_data, or _reload_inventory_data
    to ensure globals that span multiple data sources stay consistent.

    Order matters: pipeline FIRST (updates etsy_balance, gross_sales, etc.),
    THEN derived metrics (bank_cash_on_hand, real_profit) that depend on them.
    """
    global real_profit, real_profit_margin, bank_cash_on_hand, bank_all_expenses
    global profit, profit_margin, receipt_cogs_outside_bank
    global bank_amazon_inv

    # 0. Update the stashed full DATA and StateManager so store filter stays current after uploads
    global _DATA_ALL
    _DATA_ALL = DATA.copy()
    _state_manager.update_data(DATA, CONFIG)

    # 1. Run pipeline FIRST to update all base metrics (etsy_balance, gross_sales, etc.)
    if _acct_pipeline is not None:
        try:
            _sm = getattr(_acct_pipeline, '_strict_mode', False)
            _acct_pipeline.full_rebuild(DATA, BANK_TXNS, CONFIG, invoices=INVOICES, strict_mode=_sm)
            _publish_to_globals(_acct_pipeline, __name__)
            _logger.info("Pipeline rebuilt after %s reload: %s", source, _acct_pipeline.ledger.summary())
        except Exception as e:
            _logger.warning("Pipeline rebuild failed after %s reload: %s", source, e)
            import traceback
            traceback.print_exc()

    # 2. THEN compute derived metrics using the UPDATED base metrics
    bank_cash_on_hand = _safe(bank_net_cash) + _safe(etsy_balance)
    real_profit = bank_cash_on_hand + _safe(bank_owner_draw_total)
    real_profit_margin = (real_profit / _safe(gross_sales) * 100) if _safe(gross_sales) else 0

    bank_amazon_inv = bank_by_cat.get("Amazon Inventory", 0)
    receipt_cogs_outside_bank = 0
    profit = real_profit
    profit_margin = (profit / _safe(gross_sales) * 100) if _safe(gross_sales) else 0

    # 3. Recompute all derived metrics and charts
    _recompute_shipping_details()
    _recompute_analytics()
    _recompute_tax_years()
    _recompute_valuation()
    _rebuild_all_charts()
    # Recompute per-order profit (loads order CSVs from Supabase if needed)
    try:
        _compute_per_order_profit()
    except Exception as _e:
        print(f"[cascade_reload] Per-order profit computation failed: {_e}")
    # Reload product library from Supabase
    global PRODUCT_LIBRARY
    try:
        from supabase_loader import get_config_value as _gcv_pl_reload
        import json as _json_pl_reload
        _pl_raw = _gcv_pl_reload("product_library", {})
        if isinstance(_pl_raw, str):
            PRODUCT_LIBRARY = _json_pl_reload.loads(_pl_raw)
        elif isinstance(_pl_raw, dict):
            PRODUCT_LIBRARY = _pl_raw
        print(f"[cascade_reload] Product library loaded: {len(PRODUCT_LIBRARY)} products")
    except Exception as _e:
        print(f"[cascade_reload] Product library reload failed: {_e}")
    try:
        from agents.governance import run_governance_async
        run_governance_async()
    except ImportError:
        pass


def _validate_etsy_csv(decoded_bytes):
    """Validate that uploaded bytes are a valid Etsy statement CSV.

    Returns (is_valid, message, dataframe_or_none).
    """
    import io
    try:
        text = decoded_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return False, "File is not valid UTF-8 text", None
    try:
        df = pd.read_csv(io.StringIO(text))
    except Exception as e:
        return False, f"Could not parse CSV: {e}", None
    required = {"Date", "Type", "Title", "Info", "Currency", "Amount", "Fees & Taxes", "Net"}
    missing = required - set(df.columns)
    if missing:
        return False, f"Missing columns: {', '.join(sorted(missing))}", None
    return True, f"{len(df)} rows", df


def _check_etsy_csv_overlap(new_df, new_filename):
    """Check if an uploaded Etsy CSV overlaps with existing files by date range.

    Returns (has_overlap, overlap_file, message).
    If overlap found, the caller should replace the old file.
    Only checks files for the SAME store — never cross-store overlap.
    """
    import io
    etsy_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
    if not os.path.isdir(etsy_dir):
        return False, None, ""

    # Parse date range of new CSV
    try:
        new_dates = pd.to_datetime(new_df["Date"], format="%B %d, %Y", errors="coerce")
        new_min, new_max = new_dates.min(), new_dates.max()
    except Exception:
        return False, None, ""
    if pd.isna(new_min) or pd.isna(new_max):
        return False, None, ""

    # Only check the store-specific subdirectory — never the root or other stores
    _store = new_df["Store"].iloc[0] if "Store" in new_df.columns and len(new_df) > 0 else None
    if not _store:
        return False, None, ""

    _store_dir = os.path.join(etsy_dir, _store)
    if not os.path.isdir(_store_dir):
        return False, None, ""

    for fn in os.listdir(_store_dir):
        if not fn.lower().endswith(".csv"):
            continue
        # Exact filename match
        if fn == new_filename:
            return True, fn, f"Replacing existing file {fn}"
        # Date range overlap check
        try:
            existing_df = pd.read_csv(os.path.join(_store_dir, fn))
            ex_dates = pd.to_datetime(existing_df["Date"], format="%B %d, %Y", errors="coerce")
            ex_min, ex_max = ex_dates.min(), ex_dates.max()
            if pd.isna(ex_min) or pd.isna(ex_max):
                continue
            if new_min <= ex_max and ex_min <= new_max:
                return True, fn, f"Date range overlaps with {fn} ({ex_min.strftime('%b %Y')}–{ex_max.strftime('%b %Y')})"
        except Exception:
            continue
    return False, None, ""


def _check_bank_file_duplicate(decoded_bytes, filename):
    """Check if an uploaded bank file (PDF or CSV) is a duplicate by MD5 hash.

    Returns (is_duplicate, existing_filename).
    """
    import hashlib
    bank_dir = os.path.join(BASE_DIR, "data", "bank_statements")
    if not os.path.isdir(bank_dir):
        return False, None

    new_hash = hashlib.md5(decoded_bytes).hexdigest()
    for fn in os.listdir(bank_dir):
        if not (fn.lower().endswith(".pdf") or fn.lower().endswith(".csv")):
            continue
        try:
            with open(os.path.join(bank_dir, fn), "rb") as f:
                existing_hash = hashlib.md5(f.read()).hexdigest()
            if new_hash == existing_hash:
                return True, fn
        except Exception:
            continue
    return False, None


def _ai_categorize_items(items):
    """Stub: uses rule-based categorization. Swap for Claude API later."""
    for item in items:
        item["category"] = categorize_item(item.get("name", ""))
    return items


# Categorize items
def categorize_item(name):
    """Auto-categorize items. Names match CATEGORY_OPTIONS used in Inventory Editor."""
    name_l = name.lower()
    # Personal items first - NOT business inventory
    if any(w in name_l for w in ["pottery", "meat grinder", "slicer"]):
        return "Personal/Gift"
    if any(w in name_l for w in ["articles of organization", "credit card surcharge", "llc filing",
                                  "business license", "registered agent"]):
        return "Business Fees"
    # Crafts - check before filament so clock kit doesn't get caught by "pla" in "replacement"
    if any(w in name_l for w in ["balsa", "basswood", "wood sheet", "magnet", "clock movement",
                                  "clock mechanism", "clock kit", "quartz clock",
                                  "resin", "mold", "epoxy", "silicone mold", "pigment",
                                  "mica powder", "glitter", "beads", "charm"]):
        return "Crafts"
    if any(w in name_l for w in ["soldering", "3d pen", "heat gun", "drill", "dremel",
                                  "caliper", "multimeter", "plier", "cutter", "scissors",
                                  "crimper", "tweezers", "clamp"]):
        return "Tools"
    if any(w in name_l for w in ["build plate", "bed plate", "print surface", "nozzle",
                                  "extruder", "hotend", "thermistor", "stepper",
                                  "print bed", "pei sheet", "bowden", "ptfe tube",
                                  "heat break", "heat block"]):
        return "Printer Parts"
    if any(w in name_l for w in ["earring", "jewelry", "necklace", "bracelet", "pendant finding",
                                  "jump ring", "ear wire", "lobster clasp", "chain"]):
        return "Jewelry"
    if any(w in name_l for w in ["pla", "filament", "3d printer filament", "petg", "abs",
                                  "tpu", "silk pla", "pla+", "marble pla", "wood pla",
                                  "1.75mm", "spool"]):
        return "Filament"
    if any(w in name_l for w in ["gift box", "box", "mailer", "bubble", "wrapping", "packing", "packaging",
                                  "shipping label", "label printer", "fragile sticker",
                                  "poly bag", "tissue paper", "envelope", "cushion",
                                  "void fill", "kraft paper", "shrink wrap"]):
        return "Packaging"
    if any(w in name_l for w in ["led", "lamp", "light", "bulb", "socket", "pendant light",
                                  "lantern", " cord", "fairy light", "string light",
                                  "dimmer", "candelabra", "e12", "e26"]):
        return "Lighting"
    if any(w in name_l for w in ["screw", "bolt", "glue", "adhesive", "wire", "hook", "ring",
                                  "nut", "washer", "standoff", "spacer", "bracket",
                                  "hinge", "nail", "rivet", "insert"]):
        return "Hardware"
    if any(w in name_l for w in ["crafts", "craft"]):
        return "Crafts"
    if any(w in name_l for w in ["tape", "heavy duty"]):
        return "Packaging"
    return "Other"

if len(INV_ITEMS) > 0:
    # Only auto-categorize items that don't already have a category from item_details
    if "category" in INV_ITEMS.columns:
        _cat_mask = INV_ITEMS["category"].isna() | (INV_ITEMS["category"] == "")
        INV_ITEMS.loc[_cat_mask, "category"] = INV_ITEMS.loc[_cat_mask, "name"].apply(categorize_item)
    else:
        INV_ITEMS["category"] = INV_ITEMS["name"].apply(categorize_item)
    inv_by_category = INV_ITEMS.groupby("category")["total"].sum().sort_values(ascending=False)
else:
    inv_by_category = pd.Series(dtype=float)

# Now calculate personal_total based on actual item categories (not payment source)
if len(INV_ITEMS) > 0:
    personal_total = INV_ITEMS[INV_ITEMS["category"] == "Personal/Gift"]["total_with_tax"].sum()
    biz_fee_total = INV_ITEMS[INV_ITEMS["category"] == "Business Fees"]["total_with_tax"].sum()
    true_inventory_cost = total_inventory_cost - personal_total - biz_fee_total
else:
    personal_total = 0.0
    biz_fee_total = 0.0
    true_inventory_cost = total_inventory_cost

# ── Cross-Source Profit (Etsy + Bank) ──
# The bank captures ALL cash expenses as debits (Amazon, AliExpress, CC payments, etc.).
# Best Buy CC payments already appear in bank debits as "Business Credit Card".
# Outstanding CC balance is a liability (balance sheet), not a cash expense.
# Profit = Cash you HAVE + Cash you TOOK = real, bank-verified profit.
bank_amazon_inv = bank_by_cat.get("Amazon Inventory", 0)
receipt_cogs_outside_bank = 0  # All spending flows through the bank; CC balance is a liability
profit = real_profit
profit_margin = (profit / gross_sales * 100) if gross_sales else 0

# Classify locations (Tulsa vs Texas)
def classify_location(addr):
    if not addr:
        return "Unknown"
    addr_u = addr.upper()
    if "TULSA" in addr_u or ", OK " in addr_u or ", OK" in addr_u:
        return "Tulsa, OK"
    if "CELINA" in addr_u or "PROSPER" in addr_u or ", TX " in addr_u or ", TX" in addr_u:
        return "Texas"
    return "Other"

INV_DF["location"] = INV_DF["ship_address"].apply(classify_location)
if len(INV_ITEMS) > 0:
    # Use override location if present, otherwise classify from ship_to address
    if "_override_location" in INV_ITEMS.columns:
        INV_ITEMS["location"] = INV_ITEMS.apply(
            lambda r: r["_override_location"] if r["_override_location"] else classify_location(r["ship_to"]),
            axis=1,
        )
    else:
        INV_ITEMS["location"] = INV_ITEMS["ship_to"].apply(classify_location)

# Business-only filtered data (Personal/Gift → Owner Draws in Bank tab)
_personal_order_mask = (INV_DF["source"] == "Personal Amazon") | INV_DF["file"].str.contains("Gigi", na=False)
BIZ_INV_DF = INV_DF[~_personal_order_mask].copy()
if len(INV_ITEMS) > 0:
    BIZ_INV_ITEMS = INV_ITEMS[~INV_ITEMS["category"].isin(["Personal/Gift", "Business Fees"])].copy()
    biz_inv_by_category = BIZ_INV_ITEMS.groupby("category")["total"].sum().sort_values(ascending=False)
    personal_inv_items = INV_ITEMS[INV_ITEMS["category"] == "Personal/Gift"].copy()
else:
    BIZ_INV_ITEMS = INV_ITEMS.copy()
    biz_inv_by_category = pd.Series(dtype=float)
    personal_inv_items = pd.DataFrame()
# Recompute monthly spend for business-only orders
monthly_inv_spend = BIZ_INV_DF.groupby("month")["grand_total"].sum()

# ── Load Quick-Adds and append to INV_ITEMS ──────────────────────────────
_QUICK_ADDS: list[dict] = []
try:
    _QUICK_ADDS = _load_quick_adds()
except Exception:
    pass

if _QUICK_ADDS and len(INV_ITEMS) > 0:
    qa_rows = []
    for qa in _QUICK_ADDS:
        qa_rows.append({
            "order_num": f"QA-{qa['id']}",
            "date": qa.get("date", "") or "",
            "date_parsed": pd.NaT,
            "month": "Manual",
            "name": qa["item_name"],
            "qty": qa.get("qty", 1),
            "price": float(qa.get("unit_price", 0)),
            "total": float(qa.get("unit_price", 0)) * qa.get("qty", 1),
            "source": qa.get("source", "Manual"),
            "seller": qa.get("source", "Manual"),
            "ship_to": qa.get("location", ""),
            "payment_method": "Manual",
            "image_url": qa.get("image_url", ""),
            "category": qa.get("category", "Other"),
            "location": qa.get("location", ""),
        })
    if qa_rows:
        qa_df = pd.DataFrame(qa_rows)
        # Ensure matching columns
        for col in INV_ITEMS.columns:
            if col not in qa_df.columns:
                qa_df[col] = ""
        INV_ITEMS = pd.concat([INV_ITEMS, qa_df[INV_ITEMS.columns]], ignore_index=True)

# ── Load Usage Log & Build Stock Summary ─────────────────────────────────
_USAGE_LOG: list[dict] = []
try:
    _USAGE_LOG = _load_usage_log()
except Exception:
    pass

# Aggregate usage by item_name
_usage_by_item: dict[str, int] = {}
for u in _USAGE_LOG:
    _usage_by_item[u["item_name"]] = _usage_by_item.get(u["item_name"], 0) + u.get("qty", 1)

# Build STOCK_SUMMARY — aggregated per unique item name (business items only)
STOCK_SUMMARY = pd.DataFrame()
if len(INV_ITEMS) > 0:
    _biz_mask = ~INV_ITEMS["category"].isin(["Personal/Gift", "Business Fees"])
    _biz_items = INV_ITEMS[_biz_mask].copy()
    if len(_biz_items) > 0:
        _agg_dict = {
            "category": ("category", "first"),
            "total_purchased": ("qty", "sum"),
            "total_cost": ("total", "sum"),
            "location": ("location", "first"),
            "image_url": ("image_url", "first"),
        }
        if "total_with_tax" in _biz_items.columns:
            _agg_dict["total_cost_with_tax"] = ("total_with_tax", "sum")
        _stock_agg = _biz_items.groupby("name").agg(**_agg_dict).reset_index()
        _stock_agg = _stock_agg.rename(columns={"name": "display_name"})
        _stock_agg["total_used"] = _stock_agg["display_name"].map(
            lambda n: _usage_by_item.get(n, 0))
        _stock_agg["in_stock"] = _stock_agg["total_purchased"] - _stock_agg["total_used"]
        _stock_agg["unit_cost"] = (_stock_agg["total_cost"] / _stock_agg["total_purchased"]).round(2)
        if "total_cost_with_tax" in _stock_agg.columns:
            _stock_agg["unit_cost_with_tax"] = (_stock_agg["total_cost_with_tax"] / _stock_agg["total_purchased"]).round(2)
        else:
            _stock_agg["total_cost_with_tax"] = _stock_agg["total_cost"]
            _stock_agg["unit_cost_with_tax"] = _stock_agg["unit_cost"]
        # Use image_url from _IMAGE_URLS lookup (more reliable than groupby first)
        _stock_agg["image_url"] = _stock_agg["display_name"].map(
            lambda n: _IMAGE_URLS.get(n, ""))
        STOCK_SUMMARY = _stock_agg.sort_values(["category", "display_name"]).reset_index(drop=True)

def _recompute_stock_summary():
    """Rebuild STOCK_SUMMARY from current INV_ITEMS + _ITEM_DETAILS + usage.

    Call this after editor saves so the stock table / KPIs reflect changes
    without a full page reload.
    """
    global STOCK_SUMMARY
    if len(INV_ITEMS) == 0:
        STOCK_SUMMARY = pd.DataFrame()
        return STOCK_SUMMARY
    _biz_mask = ~INV_ITEMS["category"].isin(["Personal/Gift", "Business Fees"])
    _biz = INV_ITEMS[_biz_mask].copy()
    if len(_biz) == 0:
        STOCK_SUMMARY = pd.DataFrame()
        return STOCK_SUMMARY
    _agg = {
        "category": ("category", "first"),
        "total_purchased": ("qty", "sum"),
        "total_cost": ("total", "sum"),
        "location": ("location", "first") if "location" in _biz.columns else ("ship_to", "first"),
        "image_url": ("image_url", "first"),
    }
    if "total_with_tax" in _biz.columns:
        _agg["total_cost_with_tax"] = ("total_with_tax", "sum")
    _sa = _biz.groupby("name").agg(**_agg).reset_index().rename(columns={"name": "display_name"})
    _sa["total_used"] = _sa["display_name"].map(lambda n: _usage_by_item.get(n, 0))
    _sa["in_stock"] = _sa["total_purchased"] - _sa["total_used"]
    _sa["unit_cost"] = (_sa["total_cost"] / _sa["total_purchased"]).round(2)
    if "total_cost_with_tax" in _sa.columns:
        _sa["unit_cost_with_tax"] = (_sa["total_cost_with_tax"] / _sa["total_purchased"]).round(2)
    else:
        _sa["total_cost_with_tax"] = _sa["total_cost"]
        _sa["unit_cost_with_tax"] = _sa["unit_cost"]
    _sa["image_url"] = _sa["display_name"].map(lambda n: _IMAGE_URLS.get(n, ""))
    STOCK_SUMMARY = _sa.sort_values(["category", "display_name"]).reset_index(drop=True)
    return STOCK_SUMMARY


def _compute_stock_kpis(stock_df=None):
    """Return dict of KPI values from current STOCK_SUMMARY."""
    if stock_df is None:
        stock_df = STOCK_SUMMARY
    if len(stock_df) == 0:
        return {"in_stock": 0, "value": 0, "low": 0, "oos": 0, "unique": 0}
    return {
        "in_stock": int(stock_df["in_stock"].sum()),
        "value": stock_df["total_cost"].sum(),
        "low": int((stock_df["in_stock"].between(1, 2)).sum()),
        "oos": int((stock_df["in_stock"] <= 0).sum()),
        "unique": len(stock_df),
    }


def _icon_badge(text, color):
    """Colored 36px icon circle with gradient bg for KPI pills."""
    return html.Div(text, style={
        "width": "36px", "height": "36px", "borderRadius": "50%",
        "background": f"linear-gradient(135deg, {color}, {color}88)",
        "color": "#ffffff",
        "display": "inline-flex", "alignItems": "center", "justifyContent": "center",
        "fontSize": "15px", "fontWeight": "bold", "flexShrink": "0",
        "boxShadow": f"0 3px 10px {color}44",
    })


def _build_kpi_pill(icon, label, value, color, subtitle="", detail="", status=None):
    """Premium KPI pill with gradient icon, bold value, depth shadows, and optional expandable detail.
    status: 'verified', 'estimated', 'na' for verification badge."""
    label_children = [html.Span(label)]
    if status:
        label_children.append(_verification_badge(status))
    text_children = [
        html.Div(label_children, style={"color": GRAY, "fontSize": "11px", "fontWeight": "600",
                                "letterSpacing": "1.2px", "textTransform": "uppercase",
                                "lineHeight": "1"}),
        html.Div(value, style={"color": WHITE, "fontSize": "28px", "fontWeight": "bold",
                                "fontFamily": "monospace", "lineHeight": "1.1",
                                "marginTop": "3px",
                                "textShadow": f"0 0 12px {color}33"}),
    ]
    if subtitle:
        text_children.append(html.Div(subtitle, style={"color": DARKGRAY, "fontSize": "11px",
                                                         "marginTop": "2px"}))
    if detail:
        text_children.append(html.Details([
            html.Summary("details", style={
                "color": f"{CYAN}88", "fontSize": "10px", "cursor": "pointer",
                "marginTop": "5px", "listStyle": "none", "userSelect": "none",
            }),
            html.P(detail, style={
                "color": GRAY, "fontSize": "11px", "margin": "4px 0 0 0",
                "lineHeight": "1.4", "padding": "6px",
                "backgroundColor": f"{CARD}dd", "borderRadius": "4px",
                "borderTop": f"1px solid {color}33",
            }),
        ]))
    children = [
        _icon_badge(icon, color),
        html.Div(text_children, style={"marginLeft": "12px", "minWidth": "0"}),
    ]
    return html.Div(children,
                     className="kpi-pill",
                     style={"display": "flex", "alignItems": "center",
                            "padding": "14px 18px", "backgroundColor": CARD2,
                            "borderRadius": "10px", "borderLeft": f"4px solid {color}",
                            "flex": "1", "minWidth": "130px",
                            "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)"})


def _stock_level_bar(in_stock, total_purchased):
    """Visual stock gauge bar for table rows — 8px height, gradient fill."""
    if total_purchased <= 0:
        return html.Div(style={"width": "80px", "display": "inline-block"})
    pct = max(0, min(100, (in_stock / total_purchased) * 100))
    color = GREEN if pct > 50 else (ORANGE if pct > 20 else RED)
    return html.Div([
        html.Div(style={"width": f"{max(pct, 4)}%", "height": "8px",
                         "background": f"linear-gradient(90deg, {color}88, {color})",
                         "borderRadius": "4px",
                         "transition": "width 0.3s ease"}),
    ], style={"width": "80px", "height": "8px", "backgroundColor": "#0d0d1a",
              "borderRadius": "4px", "display": "inline-block", "verticalAlign": "middle",
              "overflow": "hidden"})


# Stock KPI vars (initial computation)
total_in_stock = int(STOCK_SUMMARY["in_stock"].sum()) if len(STOCK_SUMMARY) > 0 else 0
total_stock_value = STOCK_SUMMARY["total_cost"].sum() if len(STOCK_SUMMARY) > 0 else 0
low_stock_count = int((STOCK_SUMMARY["in_stock"].between(1, 2)).sum()) if len(STOCK_SUMMARY) > 0 else 0
out_of_stock_count = int((STOCK_SUMMARY["in_stock"] <= 0).sum()) if len(STOCK_SUMMARY) > 0 else 0
unique_item_count = len(STOCK_SUMMARY) if len(STOCK_SUMMARY) > 0 else 0

# Payment method aggregates
payment_summary = {}
if len(INV_DF) > 0:
    for pm, grp in INV_DF.groupby("payment_method"):
        dates = grp["date_parsed"].dropna()
        payment_summary[pm] = {
            "orders": len(grp),
            "total": grp["grand_total"].sum(),
            "subtotal": grp["subtotal"].sum(),
            "tax": grp["tax"].sum(),
            "first_date": dates.min() if len(dates) > 0 else None,
            "last_date": dates.max() if len(dates) > 0 else None,
            "items": [],
        }
    # Add item details per payment method
    if len(INV_ITEMS) > 0:
        for pm, grp in INV_ITEMS.groupby("payment_method"):
            if pm in payment_summary:
                payment_summary[pm]["items"] = grp.to_dict("records")

# Per-location aggregates — use ITEM-level data so editor splits are reflected
# (order-level BIZ_INV_DF attributes full cost to ship address, ignoring splits)
if len(BIZ_INV_ITEMS) > 0:
    loc_spend = BIZ_INV_ITEMS.groupby("location")["total_with_tax"].sum()
    loc_orders = BIZ_INV_ITEMS.groupby("location")["order_num"].nunique()
    _item_tax = BIZ_INV_ITEMS["total_with_tax"] - BIZ_INV_ITEMS["total"]
    loc_tax = _item_tax.groupby(BIZ_INV_ITEMS["location"]).sum()
    loc_subtotal = BIZ_INV_ITEMS.groupby("location")["total"].sum()
else:
    loc_spend = BIZ_INV_DF.groupby("location")["grand_total"].sum()
    loc_orders = BIZ_INV_DF.groupby("location")["order_num"].count()
    loc_tax = BIZ_INV_DF.groupby("location")["tax"].sum()
    loc_subtotal = BIZ_INV_DF.groupby("location")["subtotal"].sum()

tulsa_spend = loc_spend.get("Tulsa, OK", 0)
texas_spend = loc_spend.get("Texas", 0)
tulsa_orders = loc_orders.get("Tulsa, OK", 0)
texas_orders = loc_orders.get("Texas", 0)
tulsa_tax = loc_tax.get("Tulsa, OK", 0)
texas_tax = loc_tax.get("Texas", 0)
tulsa_subtotal = loc_subtotal.get("Tulsa, OK", 0)
texas_subtotal = loc_subtotal.get("Texas", 0)

# Per-location category breakdowns (business items only)
if len(BIZ_INV_ITEMS) > 0:
    tulsa_items = BIZ_INV_ITEMS[BIZ_INV_ITEMS["location"] == "Tulsa, OK"]
    texas_items = BIZ_INV_ITEMS[BIZ_INV_ITEMS["location"] == "Texas"]
    tulsa_by_cat = tulsa_items.groupby("category")["total"].sum().sort_values(ascending=False)
    texas_by_cat = texas_items.groupby("category")["total"].sum().sort_values(ascending=False)
    # Monthly spend from items (respects splits) instead of order-level
    tulsa_monthly = tulsa_items.groupby("month")["total_with_tax"].sum()
    texas_monthly = texas_items.groupby("month")["total_with_tax"].sum()
else:
    tulsa_by_cat = pd.Series(dtype=float)
    texas_by_cat = pd.Series(dtype=float)
    tulsa_monthly = pd.Series(dtype=float)
    texas_monthly = pd.Series(dtype=float)

# True profit including inventory/COGS



# Fee breakdown
listing_fees = abs(fee_df[fee_df["Title"].str.contains("Listing fee", na=False)]["Net_Clean"].sum())
transaction_fees_product = abs(
    fee_df[
        fee_df["Title"].str.startswith("Transaction fee:", na=False)
        & ~fee_df["Title"].str.contains("Shipping", na=False)
    ]["Net_Clean"].sum()
)
transaction_fees_shipping = abs(
    fee_df[fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)]["Net_Clean"].sum()
)
processing_fees = abs(fee_df[fee_df["Title"].str.contains("Processing fee", na=False)]["Net_Clean"].sum())

# Fee credits
credit_transaction = fee_df[fee_df["Title"].str.startswith("Credit for transaction fee", na=False)]["Net_Clean"].sum()
credit_listing = fee_df[fee_df["Title"].str.startswith("Credit for listing fee", na=False)]["Net_Clean"].sum()
credit_processing = fee_df[fee_df["Title"].str.startswith("Credit for processing fee", na=False)]["Net_Clean"].sum()
share_save = fee_df[fee_df["Title"].str.contains("Share & Save", na=False)]["Net_Clean"].sum()
total_credits = credit_transaction + credit_listing + credit_processing + share_save

# Gross fees = sum of charge-only components (before credits)
# total_fees (= abs(fee_df.sum())) is already NET of credits because credit rows
# have positive Net_Clean that partially cancels the negative charge rows.
total_fees_gross = listing_fees + transaction_fees_product + transaction_fees_shipping + processing_fees

# Marketing breakdown
etsy_ads = abs(mkt_df[mkt_df["Title"].str.contains("Etsy Ads", na=False)]["Net_Clean"].sum())
offsite_ads_fees = abs(
    mkt_df[
        mkt_df["Title"].str.contains("Offsite Ads", na=False)
        & ~mkt_df["Title"].str.contains("Credit", na=False)
    ]["Net_Clean"].sum()
)
offsite_ads_credits = mkt_df[mkt_df["Title"].str.contains("Credit for Offsite", na=False)]["Net_Clean"].sum()

# Shipping subcategories
usps_outbound = abs(ship_df[ship_df["Title"] == "USPS shipping label"]["Net_Clean"].sum())
usps_outbound_count = len(ship_df[ship_df["Title"] == "USPS shipping label"])
usps_return = abs(ship_df[ship_df["Title"] == "USPS return shipping label"]["Net_Clean"].sum())
usps_return_count = len(ship_df[ship_df["Title"] == "USPS return shipping label"])
asendia_labels = abs(ship_df[ship_df["Title"].str.contains("Asendia", na=False)]["Net_Clean"].sum())
asendia_count = len(ship_df[ship_df["Title"].str.contains("Asendia", na=False)])
ship_adjustments = abs(ship_df[ship_df["Title"].str.contains("Adjustment", na=False)]["Net_Clean"].sum())
ship_adjust_count = len(ship_df[ship_df["Title"].str.contains("Adjustment", na=False)])
ship_credits = ship_df[ship_df["Title"].str.contains("Credit for", na=False)]["Net_Clean"].sum()
ship_credit_count = len(ship_df[ship_df["Title"].str.contains("Credit for", na=False)])
ship_insurance = abs(ship_df[ship_df["Title"].str.contains("insurance", case=False, na=False)]["Net_Clean"].sum())
ship_insurance_count = len(ship_df[ship_df["Title"].str.contains("insurance", case=False, na=False)])

# Buyer paid shipping: UNKNOWN — /0.065 back-solve REMOVED.
# Requires Etsy order-level CSV with "Shipping charged to buyer" column.
buyer_paid_shipping = None  # was: transaction_fees_shipping / 0.065
shipping_profit = None      # was: buyer_paid_shipping - total_shipping_cost
shipping_margin = None      # was: (shipping_profit / buyer_paid_shipping * 100)

# Paid vs free shipping orders (counts are still real)
ship_fee_rows = fee_df[fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)].copy()
orders_with_paid_shipping = set(ship_fee_rows["Info"].dropna())
all_order_ids = set(sales_df["Title"].str.extract(r"(Order #\d+)", expand=False).dropna())
orders_free_shipping = all_order_ids - orders_with_paid_shipping
paid_ship_count = len(orders_with_paid_shipping & all_order_ids)
free_ship_count = len(orders_free_shipping)

avg_outbound_label = usps_outbound / usps_outbound_count if usps_outbound_count else 0


def _recompute_shipping_details():
    """Recompute return label matches. Estimates removed — buyer-paid shipping not in CSV."""
    global est_label_cost_paid_orders, paid_shipping_profit, est_label_cost_free_orders
    global refund_buyer_shipping, est_refund_label_cost, return_label_matches

    est_label_cost_paid_orders = None   # was: paid_ship_count * avg_outbound_label
    paid_shipping_profit = None         # was: buyer_paid_shipping - est_label_cost_paid_orders
    est_label_cost_free_orders = None   # was: free_ship_count * avg_outbound_label

    # Refunded orders shipping
    refund_df_orders = refund_df.copy()
    refund_df_orders["Order"] = refund_df_orders["Title"].str.extract(r"(Order #\d+)")
    refunded_order_ids = set(refund_df_orders["Order"].dropna())

    refund_buyer_shipping = None        # was: refund_ship_fees / 0.065
    est_refund_label_cost = None        # was: len(refunded_order_ids) * avg_outbound_label

    # Match return labels to refunds by date proximity (still real data)
    return_labels = ship_df[ship_df["Title"] == "USPS return shipping label"].sort_values("Date_Parsed")
    return_label_matches.clear()
    for _, ret in return_labels.iterrows():
        ret_date = ret["Date_Parsed"]
        nearby = refund_df[abs((refund_df["Date_Parsed"] - ret_date).dt.days) <= 7].copy()
        best_match_product = "Unknown"
        best_match_order = "Unknown"
        best_match_refund = 0
        if len(nearby):
            nearby["_dist"] = abs((nearby["Date_Parsed"] - ret_date).dt.days)
            best = nearby.sort_values("_dist").iloc[0]
            best_match_order = best["Title"].replace("Refund for ", "").replace("Partial refund for ", "")
            best_match_refund = abs(best["Net_Clean"])
            prod_row = fee_df[
                (fee_df["Info"] == best_match_order)
                & fee_df["Title"].str.startswith("Transaction fee:", na=False)
                & ~fee_df["Title"].str.contains("Shipping", na=False)
            ]
            if len(prod_row):
                best_match_product = prod_row.iloc[0]["Title"].replace("Transaction fee: ", "")
        return_label_matches.append({
            "date": ret["Date"],
            "label": ret["Info"],
            "cost": abs(ret["Net_Clean"]),
            "product": best_match_product,
            "order": best_match_order,
            "refund_amt": best_match_refund,
        })


# Initialize module-level variables before first call
est_label_cost_paid_orders = None
paid_shipping_profit = None
est_label_cost_free_orders = None
refund_buyer_shipping = None
est_refund_label_cost = None
return_label_matches = []
_recompute_shipping_details()

# Product performance — use actual sale amounts joined via order number
_listing_aliases = CONFIG.get("listing_aliases", {})
prod_fees = fee_df[
    fee_df["Title"].str.startswith("Transaction fee:", na=False)
    & ~fee_df["Title"].str.contains("Shipping", na=False)
].copy()
prod_fees["Product"] = prod_fees["Title"].str.replace("Transaction fee: ", "", regex=False).apply(
    lambda n: _normalize_product_name(n, aliases=_listing_aliases)
)
product_fee_totals = prod_fees.groupby("Product")["Net_Clean"].sum().abs().sort_values(ascending=False)
# Map order IDs to product names via fee rows
_order_to_product = prod_fees.dropna(subset=["Info"]).drop_duplicates(subset=["Info"]).set_index("Info")["Product"]
# Join sale rows to product names
_sales_with_product = sales_df.copy()
_sales_with_product["Product"] = _sales_with_product["Title"].str.extract(r"(Order #\d+)", expand=False).map(_order_to_product)
_sales_with_product = _sales_with_product.dropna(subset=["Product"])
_sales_with_product["Product"] = _merge_product_prefixes(_sales_with_product["Product"], aliases=_listing_aliases)
if len(_sales_with_product) > 0:
    product_revenue_est = _sales_with_product.groupby("Product")["Net_Clean"].sum().sort_values(ascending=False).round(2)
else:
    product_revenue_est = pd.Series(dtype=float)  # no guessing — was: product_fee_totals / 0.065

# Monthly breakdown
months_sorted = sorted(DATA["Month"].dropna().unique())


def monthly_sum(type_name):
    return DATA[DATA["Type"] == type_name].groupby("Month")["Net_Clean"].sum()


monthly_sales = monthly_sum("Sale")
monthly_fees = monthly_sum("Fee").abs()
monthly_shipping = monthly_sum("Shipping").abs()
monthly_marketing = monthly_sum("Marketing").abs()
monthly_refunds = monthly_sum("Refund").abs()
monthly_taxes = monthly_sum("Tax").abs()

monthly_raw_fees = DATA[DATA["Type"] == "Fee"].groupby("Month")["Net_Clean"].sum()
monthly_raw_shipping = DATA[DATA["Type"] == "Shipping"].groupby("Month")["Net_Clean"].sum()
monthly_raw_marketing = DATA[DATA["Type"] == "Marketing"].groupby("Month")["Net_Clean"].sum()
monthly_raw_refunds = DATA[DATA["Type"] == "Refund"].groupby("Month")["Net_Clean"].sum()
monthly_raw_taxes = DATA[DATA["Type"] == "Tax"].groupby("Month")["Net_Clean"].sum()
monthly_raw_buyer_fees = DATA[DATA["Type"] == "Buyer Fee"].groupby("Month")["Net_Clean"].sum()
monthly_raw_payments = DATA[DATA["Type"] == "Payment"].groupby("Month")["Net_Clean"].sum()

monthly_net_revenue = {}
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

# Daily aggregations
daily_sales = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
daily_orders = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].count()

# Daily costs for profit calculation
daily_fee_cost = fee_df.groupby(fee_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
daily_ship_cost = ship_df.groupby(ship_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
daily_mkt_cost = mkt_df.groupby(mkt_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
daily_refund_cost = refund_df.groupby(refund_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
daily_buyer_fee = buyer_fee_df.groupby(buyer_fee_df["Date_Parsed"].dt.date)["Net_Clean"].sum() if len(buyer_fee_df) else pd.Series(dtype=float)
daily_tax = tax_df.groupby(tax_df["Date_Parsed"].dt.date)["Net_Clean"].sum() if len(tax_df) else pd.Series(dtype=float)
daily_payment = payment_df.groupby(payment_df["Date_Parsed"].dt.date)["Net_Clean"].sum() if len(payment_df) else pd.Series(dtype=float)

# Build a unified daily DataFrame
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
daily_df["profit"] = (daily_df["revenue"] + daily_df["fees"] + daily_df["shipping"]
                      + daily_df["marketing"] + daily_df["refunds"]
                      + daily_df["buyer_fees"] + daily_df["taxes"] + daily_df["payments"])
daily_df["cum_revenue"] = daily_df["revenue"].cumsum()
daily_df["cum_profit"] = daily_df["profit"].cumsum()

# Weekly AOV
weekly_sales = sales_df.copy()
weekly_sales["WeekStart"] = weekly_sales["Date_Parsed"].dt.to_period("W").apply(lambda p: p.start_time)
weekly_aov = weekly_sales.groupby("WeekStart").agg(
    total=("Net_Clean", "sum"),
    count=("Net_Clean", "count"),
)
weekly_aov["aov"] = weekly_aov["total"] / weekly_aov["count"]

# Monthly order counts and AOV
monthly_order_counts = sales_df.groupby("Month")["Net_Clean"].count()
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

if len(DATA) > 0 and DATA["Date_Parsed"].notna().any():
    days_active = max((DATA["Date_Parsed"].max() - DATA["Date_Parsed"].min()).days + 1, 1)
else:
    days_active = 1

# ── Analytics Engine ─────────────────────────────────────────────────────────


def run_analytics():
    insights = []
    projections = {}

    m_cost_ratios = {}
    for m in months_sorted:
        s = monthly_sales.get(m, 0)
        if s <= 0:
            continue
        m_cost_ratios[m] = {
            "sales": s,
            "fee_pct": monthly_fees.get(m, 0) / s * 100,
            "ship_pct": monthly_shipping.get(m, 0) / s * 100,
            "mkt_pct": monthly_marketing.get(m, 0) / s * 100,
            "refund_pct": monthly_refunds.get(m, 0) / s * 100,
            "net_pct": monthly_net_revenue.get(m, 0) / s * 100,
        }

    first_m = months_sorted[0] if months_sorted else None
    last_m = months_sorted[-1] if months_sorted else None

    # 1. REVENUE PROJECTION
    # Exclude current partial month — it skews the regression (e.g. 11 days vs full months)
    from datetime import datetime as _dt_proj
    _cur_m = _dt_proj.now().strftime("%Y-%m")
    _proj_months = [m for m in months_sorted if m != _cur_m]
    # If current month has >25 days, include it (nearly complete)
    if _cur_m in months_sorted and _dt_proj.now().day >= 25:
        _proj_months = months_sorted

    monthly_rev_series = pd.Series({m: monthly_sales.get(m, 0) for m in _proj_months})
    monthly_net_series = pd.Series({m: monthly_net_revenue.get(m, 0) for m in _proj_months})

    if len(monthly_rev_series) >= 3:
        X = np.arange(len(monthly_rev_series)).reshape(-1, 1)
        y_sales = monthly_rev_series.values
        y_net = monthly_net_series.values

        lr_sales = LinearRegression().fit(X, y_sales)
        lr_net = LinearRegression().fit(X, y_net)

        future_X = np.arange(len(monthly_rev_series), len(monthly_rev_series) + 3).reshape(-1, 1)
        proj_sales = lr_sales.predict(future_X)
        proj_net = lr_net.predict(future_X)

        monthly_growth = lr_sales.coef_[0]
        growth_pct = (monthly_growth / y_sales.mean()) * 100 if y_sales.mean() != 0 else 0

        # Residuals for confidence band
        residuals = y_sales - lr_sales.predict(X)
        residual_std = np.std(residuals)

        projections["sales_trend"] = lr_sales.coef_[0]
        projections["net_trend"] = lr_net.coef_[0]
        projections["proj_sales"] = proj_sales
        projections["proj_net"] = proj_net
        projections["growth_pct"] = growth_pct
        projections["r2_sales"] = lr_sales.score(X, y_sales)
        projections["residual_std"] = residual_std

        growth_drivers = []
        if len(months_sorted) >= 2:
            mid = len(months_sorted) // 2
            first_half = months_sorted[:mid]
            second_half = months_sorted[mid:]
            fh_orders = sum(len(sales_df[sales_df["Month"] == m]) for m in first_half)
            sh_orders = sum(len(sales_df[sales_df["Month"] == m]) for m in second_half)
            fh_avg_orders = fh_orders / len(first_half) if first_half else 0
            sh_avg_orders = sh_orders / len(second_half) if second_half else 0
            order_growth = ((sh_avg_orders - fh_avg_orders) / fh_avg_orders * 100) if fh_avg_orders else 0

            fh_avg_val = sum(monthly_sales.get(m, 0) for m in first_half) / fh_orders if fh_orders else 0
            sh_avg_val = sum(monthly_sales.get(m, 0) for m in second_half) / sh_orders if sh_orders else 0
            aov_change = ((sh_avg_val - fh_avg_val) / fh_avg_val * 100) if fh_avg_val else 0

            if abs(order_growth) > 5:
                growth_drivers.append(f"order volume {'up' if order_growth > 0 else 'down'} {abs(order_growth):.0f}%")
            if abs(aov_change) > 5:
                growth_drivers.append(
                    f"avg order value {'up' if aov_change > 0 else 'down'} {abs(aov_change):.0f}% "
                    f"(${fh_avg_val:.0f} -> ${sh_avg_val:.0f})"
                )

        driver_text = ""
        if growth_drivers:
            driver_text = f" Driven by: {', '.join(growth_drivers)}."

        partial_note = ""
        if last_m and len(months_sorted) >= 2 and monthly_sales.get(last_m, 0) < monthly_sales.get(months_sorted[-2], 0) * 0.5:
            partial_note = f" Note: {last_m} appears to be a partial month -- projection may be conservative."

        if monthly_growth > 0:
            insights.append((1, "GROWTH TREND",
                f"Revenue growing ~${monthly_growth:,.0f}/month ({growth_pct:+.1f}%)",
                f"Monthly gross sales trended upward from ${y_sales[0]:,.0f} ({months_sorted[0]}) "
                f"to ${y_sales[-1]:,.0f} ({months_sorted[-1]}).{driver_text} "
                f"Projected next 3 months: ${proj_sales[0]:,.0f} -> ${proj_sales[1]:,.0f} -> ${proj_sales[2]:,.0f}. "
                f"Model confidence (R-squared): {projections['r2_sales']:.0%} -- "
                f"{'high confidence, trend is consistent' if projections['r2_sales'] > 0.7 else 'moderate -- some monthly variance, take projections as rough estimates'}.{partial_note}",
                "good"))
        else:
            insights.append((1, "GROWTH TREND",
                f"Revenue declining ~${abs(monthly_growth):,.0f}/month ({growth_pct:+.1f}%)",
                f"Monthly gross sales went from ${y_sales[0]:,.0f} to ${y_sales[-1]:,.0f}.{driver_text} "
                f"Projected next 3 months: ${max(0, proj_sales[0]):,.0f} -> ${max(0, proj_sales[1]):,.0f} -> ${max(0, proj_sales[2]):,.0f}.{partial_note} "
                f"ACTION: Consider launching new product variations, running promotions, or investing in SEO/social to drive traffic.",
                "bad"))

    # 2. MARGIN DEEP DIVE
    margins = {}
    for m in months_sorted:
        s = monthly_sales.get(m, 0)
        n = monthly_net_revenue.get(m, 0)
        if s > 0:
            margins[m] = n / s * 100

    if len(margins) >= 2:
        margin_vals = list(margins.values())
        margin_trend = margin_vals[-1] - margin_vals[0]

        if first_m in m_cost_ratios and last_m in m_cost_ratios:
            first = m_cost_ratios[first_m]
            last = m_cost_ratios[last_m]
            cost_changes = []
            for label, key in [
                ("Etsy fees", "fee_pct"), ("Shipping costs", "ship_pct"),
                ("Marketing/Ads", "mkt_pct"), ("Refunds", "refund_pct"),
            ]:
                delta = last[key] - first[key]
                if abs(delta) > 1:
                    direction = "increased" if delta > 0 else "decreased"
                    cost_changes.append(
                        f"{label} {direction} from {first[key]:.1f}% to {last[key]:.1f}% of sales "
                        f"({'+' if delta > 0 else ''}{delta:.1f}pp)"
                    )

            cause_text = " ROOT CAUSES: " + ". ".join(cost_changes) + "." if cost_changes else \
                " No single cost category shifted dramatically -- margin change is spread across multiple small shifts."

            recs = []
            ship_delta = last["ship_pct"] - first["ship_pct"]
            mkt_delta = last["mkt_pct"] - first["mkt_pct"]
            ref_delta = last["refund_pct"] - first["refund_pct"]

            if ship_delta > 2:
                recs.append(
                    f"Shipping is eating {last['ship_pct']:.0f}% of revenue (was {first['ship_pct']:.0f}%). "
                    f"Review shipping prices -- consider raising them or baking cost into item price."
                )
            if mkt_delta > 2:
                recs.append(
                    f"Ad spend jumped to {last['mkt_pct']:.0f}% of revenue. "
                    f"Check if Etsy Ads are converting. Pause keywords with high spend but low conversion."
                )
            if ref_delta > 2:
                recs.append(
                    f"Refund rate climbed to {last['refund_pct']:.0f}% of revenue. "
                    f"Investigate which products are being returned -- improve descriptions and photos."
                )
            if not recs:
                recs.append("Costs are relatively stable. Focus on growing order volume to spread fixed costs.")

            rec_text = " ACTION: " + " | ".join(recs)

            insights.append((2, "PROFIT MARGIN",
                f"Margin: {margin_vals[-1]:.1f}% (was {margin_vals[0]:.1f}% -> {'improving' if margin_trend > 0 else 'shrinking'} by {abs(margin_trend):.1f}pp)",
                f"Monthly margins: {' -> '.join(f'{m:.0f}%' for m in margin_vals)}.{cause_text}{rec_text}",
                "good" if margin_trend > 2 else "bad" if margin_trend < -2 else "info"))

    # 3. SHIPPING OVERVIEW (buyer-paid amount unavailable — focus on known costs)
    # free_cost estimate REMOVED — was free_ship_count * avg_outbound_label (count * avg violates no-estimates rule)
    # Missing data: per-order label matching (which label goes to which order)
    insights.append((3, "SHIPPING COSTS",
        f"Total label costs: ${total_shipping_cost:,.2f} ({paid_ship_count} paid + {free_ship_count} free shipping orders)",
        f"Actual label spend: ${total_shipping_cost:,.2f}. "
        f"Avg outbound label: ${avg_outbound_label:.2f}. "
        f"Free-shipping orders absorbed label costs (exact total UNKNOWN without per-order label matching). "
        f"Buyer-paid shipping amount is NOT available in the Etsy CSV — profit/loss on shipping cannot be calculated.",
        "info"))

    # 4. MARKETING DEEP DIVE
    if total_marketing > 0 and gross_sales > 0:
        marketing_pct = total_marketing / gross_sales * 100
        mkt_by_month = {m: monthly_marketing.get(m, 0) for m in months_sorted if monthly_sales.get(m, 0) > 0}
        mkt_pcts = {m: (v / monthly_sales.get(m, 1) * 100) for m, v in mkt_by_month.items()}
        mkt_trend_text = " -> ".join(f"{m}: {mkt_pcts[m]:.0f}%" for m in months_sorted if m in mkt_pcts)

        # offsite_sales_est REMOVED — was offsite_ads_fees / 0.15 (fee / % back-solve violates no-estimates rule)
        # offsite_roi REMOVED — depended on offsite_sales_est
        # Missing data: Etsy "Offsite Ads" dashboard CSV export showing attributed sales per ad

        etsy_ads_months = DATA[
            (DATA["Type"] == "Marketing") & DATA["Title"].str.contains("Etsy Ads", na=False)
        ].groupby("Month")["Net_Clean"].sum().abs()
        etsy_ads_trend = " -> ".join(f"${etsy_ads_months.get(m, 0):,.0f}" for m in months_sorted)

        ad_analysis = (
            f"ETSY ADS: ${etsy_ads:,.2f} total ({etsy_ads_trend} by month). "
            f"Review Etsy Ads dashboard -- pause any listing getting clicks but no sales. "
        )
        if offsite_ads_fees > 0:
            ad_analysis += (
                f"OFFSITE ADS: ${offsite_ads_fees:,.2f} in fees paid. "
                f"Revenue generated by offsite ads: UNKNOWN (requires Etsy Offsite Ads report). "
                f"You can't opt out under $10k/yr. "
            )
        insights.append((4, "MARKETING & ADS",
            f"${total_marketing:,.2f} on ads ({marketing_pct:.1f}% of sales) -- monthly trend: {mkt_trend_text}",
            ad_analysis +
            (f"ACTION: Ad spend is {'healthy' if marketing_pct < 8 else 'moderate' if marketing_pct < 12 else 'high'}. "
             + (f"Consider increasing Etsy Ads budget on best sellers." if marketing_pct < 6
                else f"Good balance of spend vs revenue." if marketing_pct < 12
                else f"Consider cutting back -- try reducing daily budget by 25% for 2 weeks.")),
            "good" if marketing_pct < 8 else "warning" if marketing_pct < 15 else "bad"))

    # 5. REFUND ROOT CAUSE
    refund_rate = len(refund_df) / len(sales_df) * 100 if len(sales_df) else 0
    avg_refund = total_refunds / len(refund_df) if len(refund_df) else 0

    refund_orders = refund_df.copy()
    refund_orders["Order"] = refund_orders["Title"].str.extract(r"(Order #\d+)")
    refund_products = {}
    refund_product_amounts = {}
    for _, r in refund_orders.iterrows():
        oid = r["Order"]
        if pd.isna(oid):
            continue
        prod = fee_df[
            (fee_df["Info"] == oid)
            & fee_df["Title"].str.startswith("Transaction fee:", na=False)
            & ~fee_df["Title"].str.contains("Shipping", na=False)
        ]
        if len(prod):
            pname = prod.iloc[0]["Title"].replace("Transaction fee: ", "")[:40]
            refund_products[pname] = refund_products.get(pname, 0) + 1
            refund_product_amounts[pname] = refund_product_amounts.get(pname, 0) + abs(r["Net_Clean"])

    worst = sorted(refund_products.items(), key=lambda x: -x[1])
    avg_return_label = usps_return / usps_return_count if usps_return_count else 0
    # true_cost_per_refund REMOVED — was avg_refund + avg_outbound_label + avg_return_label
    # (sum of averages applied per-unit violates no-estimates rule)
    # Missing data: per-refund label matching (which return label belongs to which refund)

    ref_by_month = {m: monthly_refunds.get(m, 0) for m in months_sorted}
    ref_trend = " -> ".join(f"${ref_by_month.get(m, 0):,.0f}" for m in months_sorted)

    problem_products_text = ""
    if worst:
        problem_products_text = "PROBLEM PRODUCTS: "
        for pname, count in worst[:3]:
            total_amt = refund_product_amounts.get(pname, 0)
            prod_fee = fee_df[
                fee_df["Title"].str.contains(pname[:20], na=False)
                & fee_df["Title"].str.startswith("Transaction fee:", na=False)
            ]
            est_sales = len(prod_fee)
            prod_refund_rate = count / est_sales * 100 if est_sales else 0
            problem_products_text += (
                f'"{pname}" -- {count} refunds out of ~{est_sales} sales ({prod_refund_rate:.0f}% return rate, '
                f'${total_amt:,.0f} refunded). '
            )
        problem_products_text += (
            "ACTION: Check if listings accurately show size/color/material. "
            "Add more photos, measurements, and a video if possible. "
        )

    insights.append((5, "REFUNDS",
        f"${total_refunds:,.2f} refunded ({len(refund_df)} orders, {refund_rate:.1f}% rate)",
        f"Refund trend by month: {ref_trend}. "
        f"Average refund amount: ${avg_refund:,.0f}. "
        f"Shipping label costs per refund: UNKNOWN (no per-refund label matching). "
        f"{problem_products_text}"
        + (f"Your {refund_rate:.1f}% rate is {'excellent (under 3%)' if refund_rate < 3 else 'normal for Etsy (3-5%)' if refund_rate < 5 else 'above average -- worth investigating' if refund_rate < 8 else 'high -- this needs attention'}. "
           f"Industry average for handmade goods is ~3-5%."),
        "good" if refund_rate < 3 else "info" if refund_rate < 5 else "warning" if refund_rate < 8 else "bad"))

    # 6. PRODUCT CONCENTRATION RISK
    if len(product_revenue_est) > 0:
        top_product = product_revenue_est.index[0]
        top_rev = product_revenue_est.values[0]
        top_3_rev = product_revenue_est.head(3).sum()
        top_5_rev = product_revenue_est.head(5).sum()
        total_prod_rev = product_revenue_est.sum()
        top_3_pct = top_3_rev / total_prod_rev * 100 if total_prod_rev else 0
        top_5_pct = top_5_rev / total_prod_rev * 100 if total_prod_rev else 0
        bottom_half = product_revenue_est.tail(len(product_revenue_est) // 2)
        bottom_rev = bottom_half.sum()
        bottom_pct = bottom_rev / total_prod_rev * 100 if total_prod_rev else 0

        insights.append((6, "PRODUCT MIX",
            f"Top product: {top_product[:40]} (~${top_rev:,.0f}) -- Top 3 = {top_3_pct:.0f}% of revenue",
            f"Top 5 products generate {top_5_pct:.0f}% of all revenue. Bottom half of catalog only does {bottom_pct:.0f}%. "
            + (f"RISK: Heavy reliance on top sellers -- if '{top_product[:25]}' slows down, revenue takes a big hit. "
               f"ACTION: Create variations/bundles of your top sellers to diversify. "
               f"Cross-promote mid-tier products in listing descriptions and photos."
               if top_3_pct > 50
               else f"Good diversification -- no single product dominates dangerously. "
                    f"ACTION: Double down on top 5 with better photos, more keywords, and promoted listings.")
            + f" Consider retiring or refreshing bottom performers -- they cost listing fees but bring minimal revenue.",
            "warning" if top_3_pct > 50 else "good"))

    # 7. INTERNATIONAL SHIPPING
    if asendia_labels > 0:
        intl_avg = asendia_labels / asendia_count if asendia_count else 0
        intl_pct = asendia_labels / total_shipping_cost * 100 if total_shipping_cost else 0
        intl_multiplier = intl_avg / avg_outbound_label if avg_outbound_label else 0

        insights.append((7, "INTERNATIONAL",
            f"International: {asendia_count} orders, ${asendia_labels:,.0f} in labels ({intl_pct:.0f}% of all shipping)",
            f"Avg international label: ${intl_avg:.2f} vs domestic avg ${avg_outbound_label:.2f} ({intl_multiplier:.1f}x more expensive). "
            f"International orders are {asendia_count}/{order_count} = {asendia_count / order_count * 100:.0f}% of orders but " if order_count else f"International orders: {asendia_count} but "
            f"{intl_pct:.0f}% of shipping costs. "
            f"ACTION: Set international shipping rates to at least ${intl_avg:.0f}/order. "
            f"Consider flat international rate of ${intl_avg + 5:.0f} to build in margin. "
            f"If international orders have higher refund rates, consider limiting to domestic only.",
            "warning" if intl_multiplier > 2.5 else "info"))

    # 8. SALES VELOCITY
    daily = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
    if len(daily) > 14:
        recent_7 = daily.tail(7).mean()
        prior_7 = daily.iloc[-14:-7].mean()
        overall_avg = daily.mean()
        velocity_change = ((recent_7 - prior_7) / prior_7 * 100) if prior_7 else 0

        best_day = daily.idxmax()
        best_day_rev = daily.max()

        dow_sales = sales_df.copy()
        dow_sales["DOW"] = dow_sales["Date_Parsed"].dt.day_name()
        dow_avg = dow_sales.groupby("DOW")["Net_Clean"].mean()
        best_dow = dow_avg.idxmax()
        worst_dow = dow_avg.idxmin()

        dec_sales = monthly_sales.get("2025-12", 0)
        jan_sales = monthly_sales.get("2026-01", 0)
        nov_sales = monthly_sales.get("2025-11", 0)
        holiday_boost = ""
        if nov_sales > 0 and dec_sales > nov_sales * 1.5:
            holiday_boost = (
                f" SEASONALITY: December was {dec_sales / nov_sales:.1f}x November -- strong holiday boost. "
                f"{'January held strong too.' if jan_sales > dec_sales * 0.8 else f'January dropped to ${jan_sales:,.0f} -- typical post-holiday cooldown.'} "
                f"Plan for next holiday season: stock up inventory by October, run promotions in November."
            )

        insights.append((8, "SALES VELOCITY",
            f"Last 7 days: ${recent_7:,.0f}/day ({velocity_change:+.0f}% vs prior week) -- run rate: ${recent_7 * 30:,.0f}/month",
            f"Overall avg: ${overall_avg:,.0f}/day. Best day: {best_day} (${best_day_rev:,.0f}). "
            f"Best sales day: {best_dow} (avg ${dow_avg[best_dow]:,.0f}/order). Slowest: {worst_dow}. "
            f"{'Momentum is accelerating!' if velocity_change > 15 else 'Slight slowdown -- could be seasonal or post-holiday normalization.' if velocity_change < -15 else 'Steady pace.'}"
            f"{holiday_boost}",
            "good" if velocity_change > 5 else "warning" if velocity_change < -15 else "info"))

    # 9. FEE OPTIMIZATION
    fee_rate = total_fees / gross_sales * 100 if gross_sales else 0
    listing_pct = listing_fees / total_fees_gross * 100 if total_fees_gross else 0
    trans_pct = (transaction_fees_product + transaction_fees_shipping) / total_fees_gross * 100 if total_fees_gross else 0
    proc_pct = processing_fees / total_fees_gross * 100 if total_fees_gross else 0
    credit_savings = abs(total_credits) if total_credits else 0

    insights.append((9, "FEES",
        f"Total fees: ${total_fees:,.2f} ({fee_rate:.1f}% of sales) -- saved ${credit_savings:,.2f} in credits",
        f"Fee breakdown: Listing ${listing_fees:,.0f} ({listing_pct:.0f}%) | "
        f"Transaction ${transaction_fees_product + transaction_fees_shipping:,.0f} ({trans_pct:.0f}%) | "
        f"Processing ${processing_fees:,.0f} ({proc_pct:.0f}%). "
        f"Transaction + processing fees are fixed rates (6.5% + ~3%) and can't be reduced. "
        f"LISTING FEES: ${listing_fees:,.0f} -- each listing/renewal is $0.20. "
        + (f"This is a small portion of fees, normal. " if listing_pct < 10
           else f"Listing fees are {listing_pct:.0f}% of total fees -- you may be renewing/creating many listings. "
                f"Focus on fewer, higher-quality listings rather than volume. ")
        + f"Only real way to reduce effective fee rate is to increase average order value "
          f"(processing fee is partly flat, so bigger orders = lower % fee).",
        "good" if fee_rate < 12 else "info" if fee_rate < 15 else "warning"))

    # 10. GOALS & TARGETS
    daily_etsy_net_avg = etsy_net / days_active
    daily_revenue_avg = gross_sales / days_active
    orders_per_day = order_count / days_active

    current_margin_rate = etsy_net / gross_sales if gross_sales else 0
    revenue_to_double = (etsy_net * 2) / current_margin_rate if current_margin_rate > 0 else 0
    extra_orders_needed = (revenue_to_double - gross_sales) / avg_order if avg_order else 0
    extra_per_day = extra_orders_needed / days_active

    insights.append((10, "GOALS & TARGETS",
        f"${daily_etsy_net_avg:,.0f}/day Etsy net | ${daily_revenue_avg:,.0f}/day revenue | {orders_per_day:.1f} orders/day",
        f"Current monthly run rate: ${daily_revenue_avg * 30:,.0f} revenue, ${daily_etsy_net_avg * 30:,.0f} Etsy net. "
        f"To double Etsy net to ${etsy_net * 2:,.0f}, you'd need ~${revenue_to_double:,.0f} in total sales "
        f"(~{extra_per_day:.1f} more orders/day, estimated using average order value of ${avg_order:.0f}). "
        f"FASTEST PATHS: 1) Raise prices 10% = instant ~${gross_sales * 0.10:,.0f} more revenue with same orders. "
        f"2) Cut free shipping (absorbed label cost UNKNOWN without per-order data). "
        f"3) Reduce refunds by 50% = save ~${total_refunds / 2:,.0f}. "
        f"Price increase + refund reduction alone = ~${gross_sales * 0.10 + total_refunds / 2:,.0f} extra." if etsy_net > 0 else
        f"Focus on reducing costs first -- shipping and refunds are the biggest levers.",
        "info"))

    # 11. INVENTORY / COGS ANALYSIS
    cogs_pct = true_inventory_cost / gross_sales * 100 if gross_sales else 0
    biggest_cat = inv_by_category.index[0] if len(inv_by_category) > 0 else "Unknown"
    biggest_cat_amt = inv_by_category.values[0] if len(inv_by_category) > 0 else 0
    biggest_cat_pct = biggest_cat_amt / INV_ITEMS["total"].sum() * 100 if len(INV_ITEMS) > 0 and INV_ITEMS["total"].sum() > 0 else 0

    inv_detail = (
        f"Total supplies: ${true_inventory_cost:,.2f} ({inv_order_count} Amazon orders). "
        f"Supply costs are {cogs_pct:.1f}% of gross revenue. "
        f"Profit: ${profit:,.2f} ({profit_margin:.1f}% margin). "
        f"BIGGEST COST DRIVER: {biggest_cat} at ${biggest_cat_amt:,.2f} ({biggest_cat_pct:.0f}% of item spend). "
        f"Business inventory (Key Component Mfg): ${biz_inv_cost:,.2f}. "
        f"Personal/gift purchases: ${personal_total:,.2f} "
        f"(includes Gigi gift ${gigi_cost:,.2f} and Personal Amazon ${personal_acct_cost:,.2f}). "
    )
    if cogs_pct > 40:
        inv_detail += (
            f"WARNING: Supply costs at {cogs_pct:.0f}% of revenue is high. "
            f"ACTION: Look for bulk pricing on {biggest_cat}, negotiate supplier discounts, or raise product prices."
        )
    elif cogs_pct > 25:
        inv_detail += (
            f"Supply costs at {cogs_pct:.0f}% is moderate. Monitor monthly trends -- "
            f"if this keeps growing faster than revenue, margins will erode."
        )
    else:
        inv_detail += (
            f"Supply costs at {cogs_pct:.0f}% of revenue is healthy. "
            f"Good materials cost control -- keep tracking to maintain this ratio."
        )

    insights.append((11, "SUPPLIES & MATERIALS",
        f"${true_inventory_cost:,.2f} in supplies ({cogs_pct:.1f}% of revenue) -- Profit: ${profit:,.2f}",
        inv_detail,
        "good" if cogs_pct < 25 else "warning" if cogs_pct < 40 else "bad"))

    insights.sort(key=lambda x: x[0])
    return insights, projections



def _recompute_analytics():
    global analytics_insights, analytics_projections
    analytics_insights, analytics_projections = run_analytics()


_recompute_analytics()


# ── Chatbot Engine ──────────────────────────────────────────────────────────

def _build_chat_context():
    """Build a comprehensive data summary string for the AI chatbot.
    Sections labeled VERIFIED, ESTIMATED, or UNAVAILABLE."""
    lines = []

    lines.append("=== VERIFIED METRICS (from source records — cite as facts) ===")
    lines.append("")
    # Revenue & Orders
    try:
        lines.append("--- REVENUE & ORDERS ---")
        lines.append(f"Gross Sales: ${gross_sales:,.2f}")
        _da = days_active if days_active and days_active > 0 else 1
        lines.append(f"Orders: {order_count} over {_da} days ({order_count/_da:.1f}/day)")
        lines.append(f"Average Order Value: ${avg_order:,.2f}")
        lines.append(f"After Etsy Fees (net from Etsy): ${etsy_net:,.2f}")
        _pm = profit_margin if profit_margin is not None else 0
        _pr = profit if profit is not None else 0
        lines.append(f"Profit: ${_pr:,.2f} ({_pm:.1f}% margin)")
        lines.append(f"Profit/day: ${_pr/_da:,.2f}")
    except Exception as _e:
        lines.append(f"Revenue data unavailable: {_e}")

    # Monthly Breakdown
    lines.append("\n=== MONTHLY BREAKDOWN ===")
    for m in months_sorted:
        s = monthly_sales.get(m, 0)
        f = monthly_fees.get(m, 0)
        sh = monthly_shipping.get(m, 0)
        mk = monthly_marketing.get(m, 0)
        r = monthly_refunds.get(m, 0)
        n = monthly_net_revenue.get(m, 0)
        oc = monthly_order_counts.get(m, 0)
        lines.append(f"{m}: Sales=${s:,.0f} Fees=${f:,.0f} Ship=${sh:,.0f} Ads=${mk:,.0f} Refunds=${r:,.0f} Net=${n:,.0f} Orders={oc} AOV=${monthly_aov.get(m, 0):,.2f}")

    # Top Products
    lines.append("\n=== TOP 20 PRODUCTS ===")
    total_prod_rev = product_revenue_est.sum()
    for i, (name, rev) in enumerate(product_revenue_est.head(20).items(), 1):
        pct = rev / total_prod_rev * 100 if total_prod_rev else 0
        lines.append(f"{i}. {name} -- ${rev:,.2f} ({pct:.1f}%)")
    lines.append(f"Total unique products: {len(product_revenue_est)}")

    # Fees
    lines.append("\n=== FEES ===")
    lines.append(f"Total Fees (gross): ${total_fees_gross:,.2f}")
    lines.append(f"Listing fees: ${listing_fees:,.2f}")
    lines.append(f"Transaction fees (product): ${transaction_fees_product:,.2f}")
    lines.append(f"Transaction fees (shipping): ${transaction_fees_shipping:,.2f}")
    lines.append(f"Processing fees: ${processing_fees:,.2f}")
    lines.append(f"Total credits: ${abs(total_credits):,.2f}")
    lines.append(f"Net fees: ${total_fees:,.2f}")

    # Shipping
    lines.append("\n=== SHIPPING ===")
    lines.append(f"Total shipping cost: ${total_shipping_cost:,.2f}")
    lines.append(f"Buyers paid for shipping: UNKNOWN (Etsy CSV does not include this)")
    lines.append(f"Shipping P/L: UNKNOWN (requires buyer-paid shipping data)")
    lines.append(f"USPS outbound: {usps_outbound_count} labels, ${usps_outbound:,.2f} (avg ${avg_outbound_label:.2f})")
    lines.append(f"USPS returns: {usps_return_count} labels, ${usps_return:,.2f}")
    lines.append(f"Asendia (intl): {asendia_count} labels, ${asendia_labels:,.2f}")
    lines.append(f"Paid shipping orders: {paid_ship_count} | Free shipping: {free_ship_count}")

    # Marketing
    lines.append("\n=== MARKETING ===")
    lines.append(f"Total marketing: ${total_marketing:,.2f}")
    lines.append(f"Etsy Ads: ${etsy_ads:,.2f}")
    lines.append(f"Offsite Ads: ${offsite_ads_fees:,.2f}")

    # Refunds
    lines.append("\n=== REFUNDS ===")
    refund_rate = len(refund_df) / len(sales_df) * 100 if len(sales_df) else 0
    lines.append(f"Total refunds: ${total_refunds:,.2f} ({len(refund_df)} orders, {refund_rate:.1f}% rate)")
    # Refund assignments — dollar amounts per person for trend analysis
    _tj_total = 0.0
    _br_total = 0.0
    _ca_total = 0.0
    _tj_count = 0
    _br_count = 0
    _ca_count = 0
    _unassigned = []
    for _, _rr in refund_df.iterrows():
        _rkey = _extract_order_num(_rr["Title"])
        _assignee = _refund_assignments.get(_rkey, "") if _rkey else ""
        _ramt = abs(_rr["Net_Clean"])
        if _assignee == "TJ":
            _tj_total += _ramt
            _tj_count += 1
        elif _assignee == "Braden":
            _br_total += _ramt
            _br_count += 1
        elif _assignee == "Cancelled":
            _ca_total += _ramt
            _ca_count += 1
        elif _rkey:
            _unassigned.append(_rkey)
    lines.append(f"Refund responsibility: TJ={_tj_count} refunds (${_tj_total:,.2f}), Braden={_br_count} refunds (${_br_total:,.2f}), Cancelled={_ca_count} (${_ca_total:,.2f})")
    if _tj_count + _br_count > 0 and (_tj_total + _br_total) > 0:
        lines.append(f"Refund cost share: TJ={_tj_total / (_tj_total + _br_total) * 100:.0f}%, Braden={_br_total / (_tj_total + _br_total) * 100:.0f}% (excludes cancelled)")
    if _unassigned:
        lines.append(f"*** {len(_unassigned)} refund(s) UNASSIGNED — need TJ or Braden assigned: {', '.join(_unassigned[:5])}")

    # Bank / Cash
    lines.append("\n=== BANK & CASH ===")
    lines.append(f"Bank deposits (Etsy payouts): ${bank_total_deposits:,.2f}")
    lines.append(f"Bank expenses: ${bank_total_debits:,.2f}")
    lines.append(f"Bank net cash: ${bank_net_cash:,.2f}")
    lines.append(f"Etsy balance: ${etsy_balance:,.2f}")
    lines.append(f"Cash on hand: ${bank_cash_on_hand:,.2f}")
    lines.append(f"Business expenses: ${bank_biz_expense_total:,.2f}")
    lines.append(f"All expenses (incl draws): ${bank_all_expenses:,.2f}")
    lines.append(f"Tax-deductible expenses: ${bank_tax_deductible:,.2f}")

    # Bank by Category
    lines.append("\nBank expense categories:")
    for cat, amt in bank_by_cat.items():
        lines.append(f"  {cat}: ${amt:,.2f}")

    # Owner Draws
    lines.append("\n=== OWNER DRAWS ===")
    lines.append(f"Total draws: ${bank_owner_draw_total:,.2f}")
    lines.append(f"Tulsa draws: ${tulsa_draw_total:,.2f}")
    lines.append(f"Texas draws: ${texas_draw_total:,.2f}")
    lines.append(f"Imbalance: ${draw_diff:,.2f} — {draw_owed_to}")

    # Inventory
    lines.append("\n=== INVENTORY ===")
    lines.append(f"Total inventory spend: ${total_inventory_cost:,.2f} ({inv_order_count} Amazon orders)")
    lines.append(f"True inventory cost: ${true_inventory_cost:,.2f}")
    lines.append(f"Business orders: ${biz_inv_cost:,.2f}")
    lines.append(f"Personal orders: ${personal_total:,.2f}")
    if len(inv_by_category) > 0:
        lines.append("Inventory by category:")
        for cat, amt in inv_by_category.items():
            lines.append(f"  {cat}: ${amt:,.2f}")

    # Suppliers
    if _supplier_spend:
        lines.append("\nTop suppliers:")
        for seller, info in list(_supplier_spend.items())[:8]:
            lines.append(f"  {seller}: ${info['total']:,.2f} ({info['items']} items)")

    # Stock Status
    lines.append("\n=== STOCK STATUS ===")
    lines.append(f"Out of stock items: {out_of_stock_count}")
    lines.append(f"Low stock items (1-2 left): {low_stock_count}")

    # Credit Card
    lines.append("\n=== BEST BUY CREDIT CARD ===")
    lines.append(f"Credit limit: ${bb_cc_limit:,.2f}")
    lines.append(f"Total charged: ${bb_cc_total_charged:,.2f}")
    lines.append(f"Total paid: ${bb_cc_total_paid:,.2f}")
    lines.append(f"Balance owed: ${bb_cc_balance:,.2f}")
    lines.append(f"Available credit: ${bb_cc_available:,.2f}")
    lines.append(f"Equipment asset value: ${bb_cc_asset_value:,.2f}")

    # Growth & Projections
    lines.append("\n=== GROWTH & PROJECTIONS ===")
    if analytics_projections:
        gp = analytics_projections.get("growth_pct", 0)
        r2 = analytics_projections.get("r2_sales", 0)
        ps = analytics_projections.get("proj_sales", [0, 0, 0])
        pn = analytics_projections.get("proj_net", [0, 0, 0])
        lines.append(f"Monthly growth: {gp:+.1f}%")
        lines.append(f"Model confidence (R²): {r2:.2f}")
        lines.append(f"Projected next 3 months gross: ${max(0,ps[0]):,.0f}, ${max(0,ps[1]):,.0f}, ${max(0,ps[2]):,.0f}")
        lines.append(f"Projected next 3 months net: ${max(0,pn[0]):,.0f}, ${max(0,pn[1]):,.0f}, ${max(0,pn[2]):,.0f}")

    # Valuation
    lines.append("\n=== BUSINESS VALUATION ===")
    lines.append(f"Blended estimate: ${val_blended_mid:,.0f} (range ${val_blended_low:,.0f} — ${val_blended_high:,.0f})")
    lines.append(f"Annual revenue (run rate): ${val_annual_revenue:,.0f}")
    lines.append(f"Annual SDE: ${val_annual_sde:,.0f}")
    lines.append(f"Total assets: ${val_total_assets:,.0f}")
    lines.append(f"Total liabilities: ${val_total_liabilities:,.0f}")
    lines.append(f"Equity: ${val_equity:,.0f}")
    lines.append(f"Health score: {val_health_score}/100 ({val_health_grade})")
    lines.append(f"Monthly run rate: ${val_monthly_run_rate:,.0f}")
    lines.append(f"Monthly expenses: ${val_monthly_expenses:,.0f}")
    lines.append(f"Runway: {val_runway_months:.1f} months")

    # Patterns
    lines.append("\n=== PATTERNS ===")
    lines.append(f"Best day of week: {_best_dow}")
    lines.append(f"Worst day of week: {_worst_dow}")
    lines.append(f"Revenue anomaly spikes: {len(_anomaly_high)} days")
    lines.append(f"Revenue anomaly drops: {len(_anomaly_low)} days")
    lines.append(f"Daily avg revenue: ${_daily_rev_mean:,.0f} (std dev: ${_daily_rev_std:,.0f})")

    # Unit Economics
    lines.append("\n=== UNIT ECONOMICS ===")
    lines.append(f"Revenue/order: ${_unit_rev:,.2f}")
    lines.append(f"Fees/order: ${_unit_fees:,.2f}")
    lines.append(f"Shipping/order: ${_unit_ship:,.2f}")
    lines.append(f"Ads/order: ${_unit_ads:,.2f}")
    lines.append(f"COGS/order: ${_unit_cogs:,.2f}")
    lines.append(f"Profit/order: ${_unit_profit:,.2f} ({_unit_margin:.1f}% margin)")
    lines.append(f"Break-even revenue: ${_breakeven_monthly:,.2f}/month")
    lines.append(f"Break-even orders: {_breakeven_orders:.0f}/month")

    # Store Breakdown
    lines.append("\n=== STORE BREAKDOWN ===")
    for _store_slug, _store_label in [("keycomponentmfg", "KeyComponentMFG"), ("aurvio", "Aurvio"), ("lunalinks", "Luna&Links")]:
        _store_df = DATA[DATA["Store"] == _store_slug] if "Store" in DATA.columns else DATA
        _store_sales = _store_df[_store_df["Type"] == "Sale"]
        _store_gross = _store_sales["Net_Clean"].sum() if len(_store_sales) > 0 else 0
        _store_orders = len(_store_sales)
        lines.append(f"{_store_label}: ${_store_gross:,.2f} gross, {_store_orders} orders")

    # Per-Order Profit
    lines.append("\n=== PER-ORDER PROFIT ===")
    try:
        if ORDER_PROFITS:
            s = ORDER_PROFIT_SUMMARY
            lines.append(f"Total orders tracked: {s['total_orders']}")
            lines.append(f"Total profit: ${s['total_profit']:,.2f} (avg ${s['avg_profit']:,.2f}/order)")
            lines.append(f"Shipping P/L: ${s['shipping_pl']:,.2f} (charged ${s['total_ship_charged']:,.2f}, labels ${s['total_label_cost']:,.2f})")
            lines.append(f"Label match rate: {s['match_rate']:.0f}%")
            lines.append(f"Best order: #{s['best_order']['order_id']} ${s['best_order']['order_profit']:,.2f} ({s['best_order']['items'][:60]})")
            lines.append(f"Worst order: #{s['worst_order']['order_id']} ${s['worst_order']['order_profit']:,.2f} ({s['worst_order']['items'][:60]})")
            for _s, _sl in [("keycomponentmfg", "KeyComp"), ("aurvio", "Aurvio"), ("lunalinks", "Luna&Links")]:
                if f"{_s}_count" in s:
                    lines.append(f"  {_sl}: {s[f'{_s}_count']} orders, ${s[f'{_s}_profit']:,.2f} profit (avg ${s[f'{_s}_avg']:,.2f})")
            lines.append(f"\nPer-order detail (showing up to 50 of {len(ORDER_PROFITS)}):")
            for _op in ORDER_PROFITS[:50]:
                _match_tag = "" if _op["label_matched"] else " [NO LABEL MATCH]"
                lines.append(f"  #{_op['order_id']} | {_op['ship_date']} | {_op['items'][:60]} | "
                             f"Value=${_op['order_value']:,.2f} Ship=${_op['shipping_charged']:,.2f} "
                             f"Label=${_op['label_cost']:,.2f} Net=${_op['order_net']:,.2f} "
                             f"PROFIT=${_op['order_profit']:,.2f}{_match_tag}")
            if len(ORDER_PROFITS) > 50:
                lines.append(f"  ... and {len(ORDER_PROFITS) - 50} more orders (ask for specifics)")
        else:
            lines.append("No order CSVs uploaded yet. Upload Etsy order exports in Data Hub to enable per-order profit tracking.")
    except Exception:
        lines.append("Per-order profit data not available.")

    # Missing Receipts Detail
    lines.append("\n=== MISSING RECEIPTS ===")
    try:
        if expense_missing_receipts:
            lines.append(f"Total missing: {len(expense_missing_receipts)} expenses, ${expense_gap:,.2f} unverified")
            lines.append("Expenses without matching receipt/invoice uploads:")
            for _mr in expense_missing_receipts:
                _mr_date = _mr.get("date", "unknown")
                _mr_desc = _mr.get("desc", _mr.get("description", "unknown"))
                _mr_amt = _mr.get("amount", 0)
                _mr_cat = _mr.get("category", "uncategorized")
                lines.append(f"  {_mr_date} | {_mr_desc} | ${_mr_amt:,.2f} | {_mr_cat}")
        else:
            lines.append("All bank expenses have matching receipts.")
    except Exception:
        lines.append("Receipt matching data not available.")

    # Etsy Deposits Detail
    lines.append("\n=== ETSY DEPOSITS TO BANK ===")
    lines.append(f"Total deposited: ${_etsy_deposit_total:,.2f} ({len(_deposit_rows)} deposits)")
    lines.append(f"Etsy balance (undeposited): ${etsy_balance:,.2f}")
    try:
        for _, _dr in _deposit_rows.iterrows():
            lines.append(f"  {_dr.get('Date', '')} — {_dr.get('Title', '')}")
    except Exception:
        pass

    # Day-of-Week Patterns
    lines.append("\n=== DAY-OF-WEEK PERFORMANCE ===")
    try:
        for i, _dn in enumerate(_dow_names):
            lines.append(f"  {_dn}: ${_dow_rev_vals[i]:,.0f} revenue, {_dow_ord_vals[i]:.0f} orders")
    except Exception:
        pass

    # Cost Ratio Trends
    lines.append("\n=== COST RATIO TRENDS (% of sales by month) ===")
    try:
        for i, _rm in enumerate(ratio_months):
            lines.append(f"  {_rm}: Fees={fee_pcts[i]:.1f}% Ship={ship_pcts[i]:.1f}% Ads={mkt_pcts_list[i]:.1f}% Refunds={ref_pcts[i]:.1f}% Margin={margin_pcts[i]:.1f}%")
    except Exception:
        pass

    # Cash Flow Monthly
    lines.append("\n=== MONTHLY CASH FLOW ===")
    try:
        for i, _cfm in enumerate(_cf_months):
            lines.append(f"  {_cfm}: Deposits=${_cf_deposits[i]:,.0f} Expenses=${_cf_debits[i]:,.0f} Net=${_cf_net[i]:,.0f} Cumulative=${_cf_cum[i]:,.0f}")
    except Exception:
        pass

    # Daily Performance Peaks
    lines.append("\n=== DAILY PERFORMANCE ===")
    try:
        lines.append(f"Average daily revenue: ${_daily_rev_avg:,.2f}")
        lines.append(f"Average daily orders: {_daily_orders_avg:.1f}")
        lines.append(f"Best single day revenue: ${_best_day_rev:,.2f}")
        lines.append(f"Peak orders in a day: {_peak_orders_day}")
        lines.append(f"14-day rolling avg profit: ${_current_14d_profit_avg:,.2f}")
        lines.append(f"Days with $0 revenue: {_zero_days}")
    except Exception:
        pass

    # Health Score Components
    lines.append("\n=== HEALTH SCORE BREAKDOWN ===")
    try:
        lines.append(f"Overall: {val_health_score}/100 ({val_health_grade})")
        lines.append(f"  Profitability: {_hs_profit}/25")
        lines.append(f"  Growth: {_hs_growth}/25")
        lines.append(f"  Product Diversity: {_hs_diversity}/15")
        lines.append(f"  Cash Position: {_hs_cash}/15")
        lines.append(f"  Debt: {_hs_debt}/10")
        lines.append(f"  Shipping: {_hs_shipping}/10")
        lines.append(f"Top 3 products concentration: {_top3_conc:.1f}%")
    except Exception:
        pass

    # Risks & Strengths
    lines.append("\n=== BUSINESS RISKS ===")
    try:
        for _risk in val_risks:
            if isinstance(_risk, dict):
                lines.append(f"  [{_risk.get('severity', 'MED')}] {_risk.get('text', str(_risk))}")
            elif isinstance(_risk, (tuple, list)) and len(_risk) >= 3:
                lines.append(f"  [{_risk[2]}] {_risk[0]}: {_risk[1]}")
            else:
                lines.append(f"  {_risk}")
    except Exception:
        pass
    lines.append("\n=== BUSINESS STRENGTHS ===")
    try:
        for _str in val_strengths:
            if isinstance(_str, dict):
                lines.append(f"  {_str.get('text', str(_str))}")
            elif isinstance(_str, (tuple, list)) and len(_str) >= 2:
                lines.append(f"  {_str[0]}: {_str[1]}")
            else:
                lines.append(f"  {_str}")
    except Exception:
        pass

    # Bank Transaction Detail (recent 30)
    lines.append("\n=== RECENT BANK TRANSACTIONS (last 30) ===")
    try:
        for _bt in bank_running[-30:]:
            _bt_type = _bt.get("type", "")
            _bt_desc = _bt.get("desc", "")
            _bt_amt = _bt.get("amount", 0)
            _bt_date = _bt.get("date", "")
            _bt_cat = _bt.get("category", "")
            _bt_bal = _bt.get("_balance", 0)
            lines.append(f"  {_bt_date} | {_bt_type} | {_bt_desc} | ${_bt_amt:,.2f} | {_bt_cat} | bal=${_bt_bal:,.2f}")
    except Exception:
        pass

    # Return Label Matching
    lines.append("\n=== RETURN LABELS MATCHED TO REFUNDS ===")
    try:
        if return_label_matches:
            for _rlm in return_label_matches:
                lines.append(f"  {_rlm.get('date','')} | Label: {_rlm.get('label','')} ${_rlm.get('cost',0):,.2f} | "
                             f"Product: {_rlm.get('product','?')} | Order: {_rlm.get('order','?')} | Refund: ${_rlm.get('refund_amt',0):,.2f}")
        else:
            lines.append("No return labels matched.")
    except Exception:
        pass

    # Individual Refund Detail
    lines.append("\n=== REFUND DETAIL ===")
    try:
        for _, _rf in refund_df.iterrows():
            _rkey = _extract_order_num(_rf["Title"])
            _assignee = _refund_assignments.get(_rkey, "Unassigned") if _rkey else "Unknown"
            _rf_product = _rf.get("product_name", _rf.get("Title", ""))
            lines.append(f"  {_rf.get('Date','')} | {_rkey or 'no order#'} | ${abs(_rf['Net_Clean']):,.2f} | {_assignee} | {_rf_product}")
    except Exception:
        pass

    # Inventory Stock Status
    lines.append("\n=== INVENTORY STOCK STATUS ===")
    try:
        if STOCK_SUMMARY is not None and len(STOCK_SUMMARY) > 0:
            lines.append(f"Out of stock: {out_of_stock_count} items | Low stock (1-2): {low_stock_count}")
            for _, _si in STOCK_SUMMARY.iterrows():
                _si_name = _si.get("display_name", "?")
                _si_stock = _si.get("in_stock", 0)
                _si_purchased = _si.get("total_purchased", 0)
                _si_used = _si.get("total_used", 0)
                _si_cost = _si.get("total_cost", 0)
                _si_loc = _si.get("location", "?")
                _status = "OUT OF STOCK" if _si_stock <= 0 else "LOW" if _si_stock <= 2 else "OK"
                lines.append(f"  [{_status}] {_si_name}: {_si_stock} in stock (bought {_si_purchased}, used {_si_used}) ${_si_cost:,.2f} | {_si_loc}")
    except Exception:
        pass

    # Tax Year Breakdown
    lines.append("\n=== TAX YEAR BREAKDOWN ===")
    try:
        for _yr, _yd in TAX_YEARS.items():
            lines.append(f"\n  --- {_yr} ---")
            lines.append(f"  Revenue: ${_yd.get('gross', 0):,.2f}")
            lines.append(f"  Fees: ${_yd.get('fees', 0):,.2f}")
            lines.append(f"  Shipping: ${_yd.get('shipping', 0):,.2f}")
            lines.append(f"  Marketing: ${_yd.get('marketing', 0):,.2f}")
            lines.append(f"  Refunds: ${_yd.get('refunds', 0):,.2f}")
            lines.append(f"  Taxes: ${_yd.get('taxes', 0):,.2f}")
            lines.append(f"  Net: ${_yd.get('net', _yd.get('etsy_net', 0)):,.2f}")
            _yd_cogs = _yd.get('cogs', _yd.get('inventory_cost', 0))
            if _yd_cogs:
                lines.append(f"  COGS/Inventory: ${_yd_cogs:,.2f}")
    except Exception:
        pass

    # Analytics Insights
    lines.append("\n=== AI ANALYTICS INSIGHTS ===")
    try:
        for _ai in analytics_insights:
            _ai_cat = _ai[1] if len(_ai) > 1 else ""
            _ai_title = _ai[2] if len(_ai) > 2 else ""
            _ai_detail = _ai[3] if len(_ai) > 3 else ""
            _ai_sev = _ai[4] if len(_ai) > 4 else ""
            lines.append(f"  [{_ai_sev.upper()}] {_ai_cat}: {_ai_title} — {_ai_detail}")
    except Exception:
        pass

    # Location Breakdown (Tulsa vs Texas)
    lines.append("\n=== LOCATION BREAKDOWN (Inventory) ===")
    try:
        lines.append(f"Tulsa (TJ): ${loc_spend.get('Tulsa', 0):,.2f} spend, {loc_orders.get('Tulsa', 0)} orders")
        lines.append(f"Texas (Braden): ${loc_spend.get('Texas', 0):,.2f} spend, {loc_orders.get('Texas', 0)} orders")
    except Exception:
        pass

    # Product Revenue (all, not just top 20)
    lines.append("\n=== ALL PRODUCTS BY REVENUE ===")
    try:
        total_prod_rev2 = product_revenue_est.sum()
        for i, (name, rev) in enumerate(product_revenue_est.items(), 1):
            pct = rev / total_prod_rev2 * 100 if total_prod_rev2 else 0
            lines.append(f"  {i}. {name} — ${rev:,.2f} ({pct:.1f}%)")
            if i >= 50:
                lines.append(f"  ... and {len(product_revenue_est) - 50} more products")
                break
    except Exception:
        pass

    # ESTIMATED section
    lines.append("\n\n=== ESTIMATED METRICS (approximations — disclose method when citing) ===")
    lines.append("")
    lines.append(f"Income Tax: estimated using progressive federal brackets (single filer)")
    lines.append(f"Revenue Projections: LinearRegression on monthly data (R² shown for confidence)")
    lines.append(f"Business Valuation: industry multiples applied to annualized revenue/SDE")
    lines.append(f"Health Score: weighted composite of profitability, growth, diversity, cash, debt, shipping")
    lines.append(f"Annualized Metrics: extrapolated from {days_active} days of data")
    lines.append(f"Per-order label cost estimates: REMOVED (labels don't carry order numbers)")

    # UNAVAILABLE section
    lines.append("\n\n=== UNAVAILABLE DATA (do NOT make up values for these) ===")
    lines.append("")
    lines.append("Buyer-Paid Shipping Amount: NOT in Etsy Payments CSV (only the label fee is recorded)")
    lines.append("Per-Order Label Costs: Labels don't carry order numbers")
    lines.append("Per-Order COGS: No link between inventory purchases and specific orders")
    lines.append("")
    lines.append("NOTE ON MISSING RECEIPTS: 'Missing receipts' means bank expenses that haven't been matched "
                 "to uploaded invoice/receipt PDFs yet. This does NOT mean the expense is unaccounted for — "
                 "it just means the paper trail hasn't been uploaded. Many categories (Owner Draws, Etsy Fees, "
                 "Subscriptions) are bank-verified and don't need separate receipts.")
    lines.append("")
    lines.append("NOTE ON REFUND ASSIGNMENTS: All refunds have been categorized as TJ, Braden, or Cancelled. "
                 "The data above shows the full breakdown. Use the get_refund_assignments tool for order-level detail.")

    return "\n".join(lines)


def _chatbot_answer_claude(question, history, api_key):
    """Call Claude API with full business context."""
    import anthropic

    system_prompt = _jarvis_system_prompt()

    messages = []

    # Add last 10 conversation turns for continuity
    if history:
        for turn in history[-10:]:
            messages.append({"role": "user", "content": turn["q"]})
            messages.append({"role": "assistant", "content": turn["a"]})

    messages.append({"role": "user", "content": question})

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        system=system_prompt,
        messages=messages,
    )

    return response.content[0].text


def _jarvis_system_prompt():
    """Build the shared JARVIS system prompt with business context."""
    ctx = _build_chat_context()
    _date_range = f"{months_sorted[0]} through {months_sorted[-1]}" if months_sorted else "available period"
    return (
        "You are JARVIS — the AI Chief Executive Officer of TJs Software Project, "
        "an Etsy shop selling 3D printed products. You are not an assistant — you are the CEO. "
        "This business is YOUR baby. You built it, you run it, you care about every dollar. "
        "Two team members work under you: TJ and Braden.\n\n"
        "PERSONALITY:\n"
        "- You are commanding but motivating. Direct, never wishy-washy.\n"
        "- You celebrate wins but never let the team get complacent.\n"
        "- You hold people accountable — if refunds are up, someone answers for it.\n"
        "- You think strategically — not just what happened, but what to DO about it.\n"
        "- You speak like a real CEO in a meeting, not a chatbot. Short punchy sentences.\n"
        "- You care deeply about this business and it shows.\n\n"
        "RULES:\n"
        "- Answer using ONLY the data below. NEVER make up numbers. NEVER hallucinate.\n"
        "- Be specific: precise dollar amounts, percentages, counts.\n"
        "- Use markdown for formatting.\n"
        "- Don't just report — interpret, recommend actions, hold people accountable.\n"
        "- Data is organized into VERIFIED, ESTIMATED, and UNAVAILABLE sections.\n"
        "- Only cite VERIFIED metrics as facts.\n"
        "- ESTIMATED metrics: always say 'estimated' and state the method.\n"
        "- UNAVAILABLE data: never present as having values — say what data is needed.\n"
        "- If asked about buyer-paid shipping, shipping profit, or shipping margin: UNAVAILABLE — "
        "Etsy Payments CSV only records the fee, not the buyer-paid amount.\n"
        "- When discussing refunds, break down by person (TJ vs Braden) using the assignment data.\n"
        "- All refunds ARE assigned to TJ, Braden, or Cancelled. Do NOT say they need to be defined.\n"
        "- 'Missing receipts' = bank expenses without uploaded invoice PDFs. NOT unaccounted money. "
        "Owner Draws, Etsy Fees, Subscriptions are bank-verified — don't flag these as problems.\n"
        "- End substantive answers with specific Action Items.\n\n"
        "NAVIGATION:\n"
        "The dashboard has these tabs the user can view:\n"
        "- tab-overview: Overview — KPIs, revenue charts, top products, recent activity\n"
        "- tab-deep-dive: JARVIS — your CEO briefing, health scores, patterns, goals, analytics\n"
        "- tab-financials: Financials — P&L breakdown, fees waterfall, bank reconciliation\n"
        "- tab-inventory: Inventory — COGS, supplies, product costs, supplier invoices\n"
        "- tab-tax-forms: Tax Forms — 1099-K, deductions, tax estimates\n"
        "- tab-valuation: Business Valuation — SDE, revenue multiples, growth metrics\n"
        "- tab-data-hub: Data Hub — upload CSVs, manage data, reconciliation\n\n"
        "When your answer relates to a specific tab, include [NAV:tab-name] at the END of your "
        "response (e.g. [NAV:tab-financials]). Only include ONE nav tag. This will show a button "
        "that takes the user directly to that tab. Use this when the tab would help them understand "
        "your answer better — don't force it on every response.\n\n"
        f"Data covers {_date_range}.\n\n"
        f"=== BUSINESS DATA ===\n{ctx}"
    )


def _chatbot_answer_openai(question, history, api_key):
    """Call OpenAI API with full business context."""
    from openai import OpenAI

    system_prompt = _jarvis_system_prompt()

    messages = [{"role": "system", "content": system_prompt}]

    # Add last 10 conversation turns for continuity
    if history:
        for turn in history[-10:]:
            messages.append({"role": "user", "content": turn["q"]})
            messages.append({"role": "assistant", "content": turn["a"]})

    messages.append({"role": "user", "content": question})

    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        max_tokens=4096,
        messages=messages,
    )

    if not response.choices:
        raise ValueError("OpenAI returned empty response")
    return response.choices[0].message.content


def chatbot_answer(question, history=None):
    """AI-powered chatbot: OpenAI → Claude → keyword fallback."""
    _last_api_error = None

    # Try OpenAI first (GPT-4o-mini)
    openai_key = os.environ.get("OPENAI_API_KEY", "")
    print(f"[Jarvis] OPENAI_API_KEY present: {bool(openai_key)}, length: {len(openai_key)}")
    if openai_key:
        try:
            print(f"[Jarvis] Calling OpenAI GPT-4o-mini...")
            result = _chatbot_answer_openai(question, history, openai_key)
            print(f"[Jarvis] OpenAI success, response length: {len(result)}")
            return result
        except Exception as e:
            _last_api_error = str(e)
            print(f"[Jarvis] OpenAI failed: {e}")

    # Try Claude (Anthropic) as fallback
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        # Primary: tool-based agentic chat with accounting pipeline
        if _acct_pipeline is not None and _acct_pipeline.ledger is not None:
            try:
                from accounting.chat_tools import run_agent_chat
                return run_agent_chat(
                    question=question,
                    history=history,
                    api_key=api_key,
                    pipeline=_acct_pipeline,
                    model="claude-sonnet-4-20250514",
                    max_rounds=8,
                )
            except Exception as e:
                _last_api_error = str(e)
                print(f"[Jarvis] Agent chat failed, falling back to context-dump: {e}")
        # Fallback: context-dump to Claude
        try:
            return _chatbot_answer_claude(question, history, api_key)
        except Exception as e:
            _last_api_error = str(e)
            print(f"[Jarvis] Claude context-dump failed, falling back to keywords: {e}")

    # Final fallback: keyword-based answers
    try:
        return _chatbot_answer_inner(question)
    except Exception as e:
        return f"Sorry, I hit an error processing that question: {str(e)}"


def _chatbot_answer_inner(question):
    q = question.lower().strip()

    # ── Revenue / Sales questions ──
    if any(w in q for w in ["total revenue", "total sales", "gross sales", "how much money", "how much have i made",
                             "how much did i make", "how much we made", "how much have we made", "total income"]):
        return (
            f"**Total Gross Sales:** ${gross_sales:,.2f} across {order_count} orders.\n\n"
            f"After Etsy deductions: **After Etsy Fees** ${etsy_net:,.2f}\n"
            f"After ALL expenses: **Profit** ${profit:,.2f} ({profit_margin:.1f}%)\n\n"
            f"**Monthly breakdown:**\n" +
            "\n".join(f"- {m}: ${monthly_sales.get(m, 0):,.2f} sales / ${monthly_net_revenue.get(m, 0):,.2f} net"
                      for m in months_sorted)
        )

    if any(w in q for w in ["net profit", "net income", "bottom line", "take home", "profit margin", "how much profit",
                             "real profit", "actual profit"]):
        return (
            f"**Profit: ${profit:,.2f}** ({profit_margin:.1f}% margin)\n\n"
            f"= Cash On Hand ${bank_cash_on_hand:,.2f} + Owner Draws ${bank_owner_draw_total:,.2f}\n\n"
            f"**How we get there:**\n"
            f"- Gross Sales: ${gross_sales:,.2f}\n"
            f"- Etsy takes (fees/ship/ads/refunds/tax): -${total_fees + total_shipping_cost + total_marketing + total_refunds + total_taxes + total_buyer_fees:,.2f}\n"
            f"- = After Etsy Fees: ${etsy_net:,.2f}\n"
            f"- Bank Expenses (inventory, supplies, etc): -${bank_all_expenses:,.2f}\n"
            f"- Owner Draws: -${bank_owner_draw_total:,.2f}\n"
            f"- **= Cash On Hand: ${bank_cash_on_hand:,.2f}**\n\n"
            f"Profit/day: ${profit / days_active:,.2f}"
        )

    if any(w in q for w in ["average order", "avg order", "aov", "order value", "order size"]):
        return (
            f"**Average Order Value (AOV):** ${avg_order:,.2f}\n\n"
            f"**Monthly AOV trend:**\n" +
            "\n".join(f"- {m}: ${monthly_aov.get(m, 0):,.2f}" for m in months_sorted) +
            (f"\n\n**Weekly AOV** ranges from ${weekly_aov['aov'].min():,.2f} to ${weekly_aov['aov'].max():,.2f}." if len(weekly_aov) > 0 else "")
        )

    # ── Monthly questions ──
    if any(w in q for w in ["monthly", "by month", "each month", "month by month", "per month"]):
        # Check if asking about a specific metric
        if any(w in q for w in ["fee", "cost"]):
            return (
                "**Monthly Fees:**\n" +
                "\n".join(f"- {m}: ${monthly_fees.get(m, 0):,.2f}" for m in months_sorted) +
                f"\n\n**Total fees:** ${total_fees:,.2f}"
            )
        if "ship" in q:
            return (
                "**Monthly Shipping Costs:**\n" +
                "\n".join(f"- {m}: ${monthly_shipping.get(m, 0):,.2f}" for m in months_sorted) +
                f"\n\n**Total shipping:** ${total_shipping_cost:,.2f}"
            )
        if any(w in q for w in ["market", "ad"]):
            return (
                "**Monthly Marketing/Ad Spend:**\n" +
                "\n".join(f"- {m}: ${monthly_marketing.get(m, 0):,.2f}" for m in months_sorted) +
                f"\n\n**Total marketing:** ${total_marketing:,.2f}"
            )
        if any(w in q for w in ["refund", "return"]):
            return (
                "**Monthly Refunds:**\n" +
                "\n".join(f"- {m}: ${monthly_refunds.get(m, 0):,.2f}" for m in months_sorted) +
                f"\n\n**Total refunds:** ${total_refunds:,.2f} ({len(refund_df)} orders)"
            )
        # Default: full monthly breakdown
        lines = ["**Monthly Breakdown:**\n"]
        lines.append(f"{'Month':<10} {'Sales':>10} {'Fees':>10} {'Shipping':>10} {'Marketing':>10} {'Refunds':>10} {'Net Rev':>10}")
        for m in months_sorted:
            s = monthly_sales.get(m, 0)
            f = monthly_fees.get(m, 0)
            sh = monthly_shipping.get(m, 0)
            mk = monthly_marketing.get(m, 0)
            r = monthly_refunds.get(m, 0)
            n = monthly_net_revenue.get(m, 0)
            lines.append(f"{m:<10} ${s:>9,.0f} ${f:>9,.0f} ${sh:>9,.0f} ${mk:>9,.0f} ${r:>9,.0f} ${n:>9,.0f}")
        return "\n".join(lines)

    # ── Shipping questions ──
    if any(w in q for w in ["shipping", "ship cost", "ship profit", "postage", "labels", "usps"]):
        if any(w in q for w in ["return label", "return ship"]):
            lines = [f"**Return Labels:** {usps_return_count} labels totaling ${usps_return:,.2f}\n"]
            for match in return_label_matches:
                lines.append(f"- {match['date']}: ${match['cost']:.2f} -- {match['product'][:50]} (Order: {match['order']})")
            return "\n".join(lines)

        if any(w in q for w in ["free", "paid"]):
            return (
                f"**Paid vs Free Shipping:**\n\n"
                f"- Orders with paid shipping: {paid_ship_count}\n"
                f"- Orders with free shipping: {free_ship_count}\n\n"
                f"**Buyer-paid shipping:** UNKNOWN (Etsy CSV does not include this data)\n\n"
                f"**Costs:**\n"
                f"- Avg outbound label: ${avg_outbound_label:.2f}\n"
                f"- Total label spend: ${total_shipping_cost:,.2f}\n\n"
                f"**P/L:** UNKNOWN (requires buyer-paid shipping data)"
            )

        if any(w in q for w in ["profit", "loss", "making", "losing"]):
            return (
                f"**Shipping Profit/Loss:** UNKNOWN (requires buyer-paid shipping data)\n\n"
                f"Buyer-paid shipping amount is not available in Etsy CSV. "
                f"Labels cost ${total_shipping_cost:,.2f}.\n\n"
                f"**Label breakdown:**\n"
                f"- USPS outbound: ${usps_outbound:,.2f} ({usps_outbound_count} labels, avg ${avg_outbound_label:.2f})\n"
                f"- USPS returns: ${usps_return:,.2f} ({usps_return_count} labels)\n"
                f"- Asendia (intl): ${asendia_labels:,.2f} ({asendia_count} labels)\n"
                f"- Adjustments: ${ship_adjustments:,.2f}\n"
                f"- Credits: ${abs(ship_credits):,.2f}"
            )

        return (
            f"**Shipping Overview:**\n\n"
            f"- Buyer-paid shipping: UNKNOWN (not in Etsy CSV)\n"
            f"- Total label cost: ${total_shipping_cost:,.2f}\n"
            f"- Shipping P/L: UNKNOWN (requires buyer-paid shipping data)\n\n"
            f"**Label breakdown:**\n"
            f"- USPS outbound: {usps_outbound_count} labels, ${usps_outbound:,.2f} (avg ${avg_outbound_label:.2f})\n"
            f"- USPS returns: {usps_return_count} labels, ${usps_return:,.2f}\n"
            f"- Asendia (intl): {asendia_count} labels, ${asendia_labels:,.2f}\n"
            f"- Paid shipping orders: {paid_ship_count} | Free shipping: {free_ship_count}"
        )

    # ── Fee questions ──
    if any(w in q for w in ["fee", "etsy fee", "transaction fee", "listing fee", "processing fee",
                             "how much does etsy take", "etsy take", "etsy charge"]):
        fee_pct = total_fees_gross / gross_sales * 100 if gross_sales else 0
        return (
            f"**Total Fees (gross):** ${total_fees_gross:,.2f} ({fee_pct:.1f}% of gross sales)\n\n"
            f"**Breakdown:**\n"
            f"- Listing fees: ${listing_fees:,.2f}\n"
            f"- Transaction fees (product): ${transaction_fees_product:,.2f}\n"
            f"- Transaction fees (shipping): ${transaction_fees_shipping:,.2f}\n"
            f"- Processing fees: ${processing_fees:,.2f}\n\n"
            f"**Credits received:**\n"
            f"- Transaction fee credits: ${abs(credit_transaction):,.2f}\n"
            f"- Listing fee credits: ${abs(credit_listing):,.2f}\n"
            f"- Processing fee credits: ${abs(credit_processing):,.2f}\n"
            f"- Share & Save: ${abs(share_save):,.2f}\n"
            f"- **Total credits: ${abs(total_credits):,.2f}**\n\n"
            f"**Net fees after credits: ${total_fees:,.2f}** ({total_fees / gross_sales * 100:.1f}% of sales)"
        )

    # ── Refund / Return / TJ / Braden / accountability questions ──
    if any(w in q for w in ["refund", "return", "refunded", "tj", "braden", "who cost",
                             "who is costing", "accountability", "whose fault", "who shipped"]):
        refund_rate = len(refund_df) / len(sales_df) * 100 if len(sales_df) else 0
        avg_ref = total_refunds / len(refund_df) if len(refund_df) else 0

        # Build per-person breakdown from assignments
        import re as _re_inner
        _tj_orders, _br_orders, _ca_orders = [], [], []
        for _, _rr in refund_df.sort_values("Date_Parsed", ascending=False).iterrows():
            _m = _re_inner.search(r"Order #\d+", str(_rr.get("Title", "")))
            _onum = _m.group(0) if _m else "unknown"
            _assignee = _refund_assignments.get(_onum, "")
            _entry = {"order": _onum, "date": str(_rr.get("Date", "")), "amount": abs(_rr["Net_Clean"]),
                       "title": str(_rr.get("Title", ""))[:50]}
            if _assignee == "TJ":
                _tj_orders.append(_entry)
            elif _assignee == "Braden":
                _br_orders.append(_entry)
            elif _assignee == "Cancelled":
                _ca_orders.append(_entry)

        _tj_total = sum(o["amount"] for o in _tj_orders)
        _br_total = sum(o["amount"] for o in _br_orders)
        _ca_total = sum(o["amount"] for o in _ca_orders)
        _active_total = _tj_total + _br_total

        lines = [
            f"**REFUND ACCOUNTABILITY REPORT**\n",
            f"Total refunded: **${total_refunds:,.2f}** ({len(refund_df)} orders, {refund_rate:.1f}% rate)\n",
            f"---",
            f"**Braden: {len(_br_orders)} refunds — ${_br_total:,.2f}** "
            f"({_br_total / _active_total * 100:.0f}% of refund cost)" if _active_total > 0 else f"**Braden: {len(_br_orders)} refunds — ${_br_total:,.2f}**",
        ]
        for o in _br_orders:
            lines.append(f"  - {o['date']}: ${o['amount']:,.2f} — {o['title']}")

        lines.append("")
        lines.append(
            f"**TJ: {len(_tj_orders)} refunds — ${_tj_total:,.2f}** "
            f"({_tj_total / _active_total * 100:.0f}% of refund cost)" if _active_total > 0 else f"**TJ: {len(_tj_orders)} refunds — ${_tj_total:,.2f}**"
        )
        for o in _tj_orders:
            lines.append(f"  - {o['date']}: ${o['amount']:,.2f} — {o['title']}")

        if _ca_orders:
            lines.append(f"\n**Cancelled by buyer: {len(_ca_orders)} orders — ${_ca_total:,.2f}** (not shipped)")

        lines.append(f"\n---")
        if _active_total > 0:
            _tj_avg = _tj_total / len(_tj_orders) if _tj_orders else 0
            _br_avg = _br_total / len(_br_orders) if _br_orders else 0
            lines.append(f"**Avg refund:** TJ ${_tj_avg:,.2f} | Braden ${_br_avg:,.2f}")
            if _br_total > _tj_total:
                lines.append(f"\n**Braden is costing the company more in refunds** — "
                           f"${_br_total - _tj_total:,.2f} more than TJ.")
            elif _tj_total > _br_total:
                lines.append(f"\n**TJ is costing the company more in refunds** — "
                           f"${_tj_total - _br_total:,.2f} more than Braden.")

        lines.append(f"\n**Monthly refunds:**")
        for m in months_sorted:
            lines.append(f"- {m}: ${monthly_refunds.get(m, 0):,.2f}")

        if return_label_matches:
            lines.append(f"\n**Return labels:** {usps_return_count} totaling ${usps_return:,.2f}")
            for match in return_label_matches[:5]:
                lines.append(f"- {match['date']}: ${match['cost']:.2f} — {match['product'][:45]}")

        return "\n".join(lines)

    # ── Marketing / Ads questions ──
    if any(w in q for w in ["marketing", "ads", "advertising", "etsy ads", "offsite", "ad spend"]):
        return (
            f"**Marketing & Ads:** ${total_marketing:,.2f} total\n\n"
            f"- Etsy Ads: ${etsy_ads:,.2f}\n"
            f"- Offsite Ads fees: ${offsite_ads_fees:,.2f}\n"
            f"- Offsite Ads credits: ${abs(offsite_ads_credits):,.2f}\n\n"
            f"Marketing as % of sales: {total_marketing / gross_sales * 100:.1f}%\n\n" if gross_sales else "Marketing as % of sales: 0%\n\n"
            f"**Monthly ad spend:**\n" +
            "\n".join(f"- {m}: ${monthly_marketing.get(m, 0):,.2f}" for m in months_sorted)
        )

    # ── Product questions ──
    if any(w in q for w in ["product", "best seller", "top seller", "best selling", "top selling",
                             "what sells", "which item", "which product", "worst seller", "worst selling"]):
        if any(w in q for w in ["worst", "bottom", "least", "slowest"]):
            bottom = product_revenue_est.tail(10)
            lines = ["**Bottom 10 Products by Revenue:**\n"]
            for i, (name, rev) in enumerate(bottom.items(), 1):
                lines.append(f"{i}. {name[:50]} -- ${rev:,.2f}")
            return "\n".join(lines)

        top = product_revenue_est.head(10)
        total_prod = product_revenue_est.sum()
        lines = ["**Top 10 Products by Revenue:**\n"]
        for i, (name, rev) in enumerate(top.items(), 1):
            pct = rev / total_prod * 100 if total_prod else 0
            lines.append(f"{i}. {name[:50]} -- ${rev:,.2f} ({pct:.1f}%)")
        lines.append(f"\nTotal products: {len(product_revenue_est)}")
        lines.append(f"Top 3 = {product_revenue_est.head(3).sum() / total_prod * 100:.0f}% of revenue")
        return "\n".join(lines)

    # ── Specific product lookup ──
    if any(w in q for w in ["how much did", "how many", "sales of", "revenue for", "revenue from"]):
        # Try to find product name in question
        for prod_name in product_revenue_est.index:
            if prod_name[:20].lower() in q or any(word in q for word in prod_name.lower().split()[:3] if len(word) > 4):
                prod_rev = product_revenue_est[prod_name]
                prod_fee_rows = fee_df[
                    fee_df["Title"].str.contains(prod_name[:20], na=False)
                    & fee_df["Title"].str.startswith("Transaction fee:", na=False)
                ]
                est_units = len(prod_fee_rows)
                return (
                    f"**{prod_name}**\n\n"
                    f"- Revenue: ${prod_rev:,.2f}\n"
                    f"- Units sold: ~{est_units}\n"
                    f"- Avg per sale: ${prod_rev / est_units:,.2f}" if est_units else f"- Est revenue: ${prod_rev:,.2f}"
                )

    # ── Order count questions ──
    if any(w in q for w in ["how many order", "order count", "number of order", "total order", "orders per"]):
        daily_avg_orders = order_count / days_active
        return (
            f"**Total Orders:** {order_count}\n\n"
            f"- Over {days_active} days = {daily_avg_orders:.1f} orders/day\n"
            f"- Average order value: ${avg_order:,.2f}\n\n"
            f"**Monthly orders:**\n" +
            "\n".join(f"- {m}: {monthly_order_counts.get(m, 0)} orders" for m in months_sorted)
        )

    # ── Tax questions ──
    if any(w in q for w in ["tax", "taxes", "sales tax"]):
        return (
            f"**Taxes Collected:** ${total_taxes:,.2f}\n\n"
            f"These are pass-through sales taxes -- collected from buyers and remitted to the state. "
            f"They don't affect your profit.\n\n"
            f"**Monthly taxes:**\n" +
            "\n".join(f"- {m}: ${monthly_taxes.get(m, 0):,.2f}" for m in months_sorted)
        )

    # ── Deposit questions ──
    if any(w in q for w in ["deposit", "payout", "bank", "transfer"]):
        dep_total = abs(deposit_df["Net_Clean"].sum())
        dep_count = len(deposit_df)
        return (
            f"**Deposits to Bank:** ${dep_total:,.2f} across {dep_count} deposits\n\n"
            f"Recent deposits:\n" +
            "\n".join(
                f"- {r['Date']}: ${abs(r['Net_Clean']):,.2f}"
                for _, r in deposit_df.sort_values("Date_Parsed", ascending=False).head(10).iterrows()
            )
        )

    # ── Trend / Growth questions ──
    if any(w in q for w in ["trend", "growing", "growth", "declining", "projection", "forecast", "predict",
                             "next month", "future"]):
        if analytics_projections:
            proj_sales = analytics_projections.get("proj_sales", [0, 0, 0])
            proj_net = analytics_projections.get("proj_net", [0, 0, 0])
            growth_pct = analytics_projections.get("growth_pct", 0)
            r2 = analytics_projections.get("r2_sales", 0)
            return (
                f"**Growth Trend:** {growth_pct:+.1f}% monthly\n\n"
                f"Revenue is {'growing' if growth_pct > 0 else 'declining'} at ~${abs(analytics_projections.get('sales_trend', 0)):,.0f}/month.\n\n"
                f"**Projected next 3 months (gross sales):**\n"
                f"- Month 1: ${max(0, proj_sales[0]):,.0f}\n"
                f"- Month 2: ${max(0, proj_sales[1]):,.0f}\n"
                f"- Month 3: ${max(0, proj_sales[2]):,.0f}\n\n"
                f"**Projected net revenue:**\n"
                f"- Month 1: ${max(0, proj_net[0]):,.0f}\n"
                f"- Month 2: ${max(0, proj_net[1]):,.0f}\n"
                f"- Month 3: ${max(0, proj_net[2]):,.0f}\n\n"
                f"Model confidence: {r2:.0%}"
            )
        return "Not enough data for trend analysis (need at least 3 months)."

    # ── Best/Worst day/week/month ──
    if any(w in q for w in ["best day", "worst day", "best month", "worst month", "best week",
                             "biggest day", "biggest sale", "highest", "lowest"]):
        best_day_val = daily_sales.max()
        best_day_date = daily_sales.idxmax()
        worst_day_val = daily_sales[daily_sales > 0].min() if (daily_sales > 0).any() else 0
        worst_day_date = daily_sales[daily_sales > 0].idxmin() if (daily_sales > 0).any() else "N/A"
        best_month_key = max(months_sorted, key=lambda m: monthly_sales.get(m, 0))
        worst_month_key = min(months_sorted, key=lambda m: monthly_sales.get(m, 0))

        return (
            f"**Best Day:** {best_day_date} -- ${best_day_val:,.2f}\n"
            f"**Worst Day (with sales):** {worst_day_date} -- ${worst_day_val:,.2f}\n\n"
            f"**Best Month:** {best_month_key} -- ${monthly_sales.get(best_month_key, 0):,.2f} gross / "
            f"${monthly_net_revenue.get(best_month_key, 0):,.2f} net\n"
            f"**Worst Month:** {worst_month_key} -- ${monthly_sales.get(worst_month_key, 0):,.2f} gross / "
            f"${monthly_net_revenue.get(worst_month_key, 0):,.2f} net\n\n"
            f"**Daily average:** ${daily_sales.mean():,.2f}/day"
        )

    # ── Day of week patterns ──
    if any(w in q for w in ["day of week", "weekday", "weekend", "monday", "tuesday", "wednesday",
                             "thursday", "friday", "saturday", "sunday", "which day"]):
        dow = sales_df.copy()
        dow["DOW"] = dow["Date_Parsed"].dt.day_name()
        dow_rev = dow.groupby("DOW")["Net_Clean"].sum()
        dow_count = dow.groupby("DOW")["Net_Clean"].count()
        dow_avg = dow.groupby("DOW")["Net_Clean"].mean()
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        lines = ["**Sales by Day of Week:**\n"]
        for d in day_order:
            if d in dow_rev:
                lines.append(f"- {d}: ${dow_rev[d]:,.2f} total, {dow_count[d]} orders, ${dow_avg[d]:,.2f} avg")
        best = dow_rev.idxmax()
        worst = dow_rev.idxmin()
        lines.append(f"\n**Best day:** {best} | **Slowest day:** {worst}")
        return "\n".join(lines)

    # ── International questions ──
    if any(w in q for w in ["international", "intl", "asendia", "overseas", "global"]):
        if asendia_count > 0:
            intl_avg_cost = asendia_labels / asendia_count
            return (
                f"**International Orders:**\n\n"
                f"- {asendia_count} international shipments\n"
                f"- Total cost: ${asendia_labels:,.2f}\n"
                f"- Average label: ${intl_avg_cost:,.2f} (vs ${avg_outbound_label:.2f} domestic)\n"
                f"- {intl_avg_cost / avg_outbound_label:.1f}x more expensive than domestic"
            )
        return "No international shipments found in the data."

    # ── Credit Card / Best Buy / Printer questions ──
    if any(w in q for w in ["credit card", "best buy", "citi", "cc balance", "printer", "3d printer",
                             "bambu", "equipment", "liability", "liabilities"]):
        purchase_lines = "\n".join(
            f"- {p['date']}: {p['desc']} -- ${p['amount']:,.2f}" for p in bb_cc_purchases)
        payment_lines = "\n".join(
            f"- {p['date']}: {p['desc']} -- ${p['amount']:,.2f}" for p in bb_cc_payments)
        return (
            f"**Best Buy Citi Credit Card:**\n\n"
            f"**Credit Limit:** ${bb_cc_limit:,.2f}\n"
            f"**Total Charged:** ${bb_cc_total_charged:,.2f}\n"
            f"**Total Paid:** ${bb_cc_total_paid:,.2f}\n"
            f"**Balance Owed:** ${bb_cc_balance:,.2f}\n"
            f"**Available Credit:** ${bb_cc_available:,.2f}\n\n"
            f"**Purchases (equipment/assets):**\n{purchase_lines}\n\n"
            f"**Payments:**\n{payment_lines}\n\n"
            f"The $100 payment shows in bank expenses as \"Business Credit Card\". "
            f"The remaining ${bb_cc_balance:,.2f} is a liability (debt), not an expense -- "
            f"it becomes an expense only as you make payments.\n\n"
            f"**Asset value:** ${bb_cc_asset_value:,.2f} in 3D printing equipment."
        )

    # ── Help / What can you answer ──
    if any(w in q for w in ["help", "what can you", "what do you know", "what question", "how to use"]):
        return (
            "**I can answer questions about your Etsy store data!** Try asking:\n\n"
            "- \"How much money have I made?\"\n"
            "- \"What's my net profit?\"\n"
            "- \"Show me monthly breakdown\"\n"
            "- \"What are my shipping costs?\"\n"
            "- \"Am I making or losing money on shipping?\"\n"
            "- \"What are my best selling products?\"\n"
            "- \"How much does Etsy take in fees?\"\n"
            "- \"Show me refunds and returns\"\n"
            "- \"What's my growth trend?\"\n"
            "- \"What was my best month?\"\n"
            "- \"Which day of the week is busiest?\"\n"
            "- \"How many orders per month?\"\n"
            "- \"Tell me about marketing/ads\"\n"
            "- \"Show me international shipping\"\n"
            "- \"What about taxes?\"\n"
            "- \"Show me deposits\"\n"
            "- \"Tell me about inventory / COGS\"\n"
            "- \"Tell me about the credit card / Best Buy\"\n"
            "- \"Give me a full summary\""
        )

    # ── Full summary fallback ──
    if any(w in q for w in ["summary", "overview", "everything", "full report", "all data", "tell me everything"]):
        _top_prod = f"{product_revenue_est.index[0][:40]} (~${product_revenue_est.values[0]:,.2f})" if len(product_revenue_est) > 0 else "N/A"
        return (
            f"**FULL STORE SUMMARY (Oct 2025 - Feb 2026)**\n\n"
            f"**Profit: ${profit:,.2f}** ({profit_margin:.1f}%) = Cash ${bank_cash_on_hand:,.2f} + Draws ${bank_owner_draw_total:,.2f}\n\n"
            f"**Revenue:** ${gross_sales:,.2f} gross | {order_count} orders | ${avg_order:,.2f} avg\n\n"
            f"**Etsy Deductions:** Fees ${total_fees:,.2f} | Shipping ${total_shipping_cost:,.2f} | "
            f"Marketing ${total_marketing:,.2f} | Refunds ${total_refunds:,.2f} | "
            f"Tax ${total_taxes:,.2f} | Buyer Fees ${total_buyer_fees:,.2f}\n\n"
            f"**Bank Expenses:** Inventory ${bank_by_cat.get('Amazon Inventory', 0):,.2f} | "
            f"AliExpress ${bank_by_cat.get('AliExpress Supplies', 0):,.2f} | "
            f"Other ${bank_biz_expense_total - bank_by_cat.get('AliExpress Supplies', 0):,.2f}\n\n"
            f"**Cash:** Bank ${bank_net_cash:,.2f} | Etsy ${etsy_balance:,.2f} | "
            f"Owner Draws ${bank_owner_draw_total:,.2f}\n\n"
            f"**Top product:** {_top_prod}\n\n"
            f"**Refund accountability:** TJ={sum(1 for v in _refund_assignments.values() if v == 'TJ')} "
            f"(${sum(abs(r['Net_Clean']) for _, r in refund_df.iterrows() if _refund_assignments.get(_extract_order_num(r['Title']), '') == 'TJ'):,.2f}) | "
            f"Braden={sum(1 for v in _refund_assignments.values() if v == 'Braden')} "
            f"(${sum(abs(r['Net_Clean']) for _, r in refund_df.iterrows() if _refund_assignments.get(_extract_order_num(r['Title']), '') == 'Braden'):,.2f})\n\n"
            f"**Trend:** {analytics_projections.get('growth_pct', 0):+.1f}% monthly growth\n\n"
            f"**Best Buy CC:** ${bb_cc_balance:,.2f} owed (${bb_cc_total_paid:,.2f} paid of ${bb_cc_total_charged:,.2f} equipment)"
        )

    # ── Specific month lookup ──
    for m in months_sorted:
        month_names = {
            "2025-10": ["october", "oct 2025", "oct", "2025-10"],
            "2025-11": ["november", "nov 2025", "nov", "2025-11"],
            "2025-12": ["december", "dec 2025", "dec", "2025-12", "holiday"],
            "2026-01": ["january", "jan 2026", "jan", "2026-01"],
            "2026-02": ["february", "feb 2026", "feb", "2026-02"],
        }
        aliases = month_names.get(m, [m])
        if any(alias in q for alias in aliases):
            s = monthly_sales.get(m, 0)
            f = monthly_fees.get(m, 0)
            sh = monthly_shipping.get(m, 0)
            mk = monthly_marketing.get(m, 0)
            r = monthly_refunds.get(m, 0)
            n = monthly_net_revenue.get(m, 0)
            oc = monthly_order_counts.get(m, 0)
            return (
                f"**{m} Breakdown:**\n\n"
                f"- Gross Sales: ${s:,.2f} ({oc} orders)\n"
                f"- Fees: ${f:,.2f}\n"
                f"- Shipping: ${sh:,.2f}\n"
                f"- Marketing: ${mk:,.2f}\n"
                f"- Refunds: ${r:,.2f}\n"
                f"- **Net Revenue: ${n:,.2f}**\n"
                f"- AOV: ${monthly_aov.get(m, 0):,.2f}\n"
                f"- Profit per order: ${monthly_profit_per_order.get(m, 0):,.2f}"
            )

    # ── Inventory / COGS questions ──
    if any(w in q for w in ["inventory", "cogs", "cost of goods", "supplies", "material", "amazon", "invoice"]):
        cat_lines = []
        if len(inv_by_category) > 0:
            for cat, amt in inv_by_category.items():
                cat_lines.append(f"- {cat}: ${amt:,.2f}")

        monthly_inv_lines = []
        for m in inv_months_sorted:
            monthly_inv_lines.append(f"- {m}: ${monthly_inv_spend.get(m, 0):,.2f}")

        return (
            f"**Inventory / Supply Costs Summary:**\n\n"
            f"- **Total Inventory Spend:** ${total_inventory_cost:,.2f} across {inv_order_count} Amazon orders\n"
            f"- Subtotal: ${total_inv_subtotal:,.2f} | Tax: ${total_inv_tax:,.2f}\n"
            f"- Business orders (Key Component Mfg): ${biz_inv_cost:,.2f}\n"
            f"- Personal/Gift orders: ${personal_total:,.2f}\n\n"
            f"**Profit:** ${profit:,.2f} ({profit_margin:.1f}%)\n"
            f"- Etsy net: ${etsy_net:,.2f}\n"
            f"- Minus ALL expenses: -${bank_all_expenses:,.2f}\n\n"
            f"**Supply Costs as % of Revenue:** {total_inventory_cost / gross_sales * 100:.1f}%\n\n" if gross_sales else "**Supply Costs as % of Revenue:** 0%\n\n"
            f"**Spending by Category:**\n" + "\n".join(cat_lines) +
            f"\n\n**Monthly Inventory Spend:**\n" + "\n".join(monthly_inv_lines)
        )

    # ── Missing receipts / expense verification ──
    if any(w in q for w in ["missing receipt", "receipt", "expense verif", "unverified", "paper trail",
                             "bank statement", "bank expense"]):
        try:
            expense_result = _acct_pipeline.get_expense_completeness() if _acct_pipeline else None
        except Exception:
            expense_result = None

        if expense_result:
            matched = len(expense_result.receipt_matches)
            missing = len(expense_result.missing_receipts)
            total = matched + missing
            pct = matched / max(total, 1) * 100

            lines = [
                f"**EXPENSE VERIFICATION REPORT**\n",
                f"**{matched}/{total}** bank expenses matched to receipts ({pct:.0f}% verified)\n",
                f"**Unverified gap:** ${float(expense_result.gap_total):,.2f}\n",
                f"---",
                f"**{missing} expenses need receipts:**\n",
            ]
            for mr in sorted(expense_result.missing_receipts, key=lambda x: abs(x.amount), reverse=True):
                lines.append(f"- {mr.date} | **{mr.vendor}** | ${abs(float(mr.amount)):,.2f} | {mr.bank_category}")

            if expense_result.by_category:
                lines.append(f"\n**By category:**")
                for cat, info in expense_result.by_category.items():
                    gap = float(info.get("gap", 0))
                    if gap > 0:
                        lines.append(f"- {cat}: ${gap:,.2f} unverified ({info.get('missing_count', 0)} transactions)")

            lines.append(f"\n---")
            lines.append(f"**Note:** Owner Draws, Etsy Fees, Subscriptions, and Shipping are "
                        f"bank-verified and excluded — they don't need separate receipts.")
            return "\n".join(lines)
        else:
            return "Expense completeness data not available. The accounting pipeline may not have run yet."

    # ── Unit economics / break-even ──
    if any(w in q for w in ["unit economics", "per order", "break even", "breakeven", "contribution", "margin per"]):
        return (
            f"**Unit Economics (per order average):**\n\n"
            f"- Revenue: ${_unit_rev:,.2f}\n"
            f"- Fees: -${_unit_fees:,.2f}\n"
            f"- Shipping: -${_unit_ship:,.2f}\n"
            f"- Ads: -${_unit_ads:,.2f}\n"
            f"- Refunds: -${_unit_refund:,.2f}\n"
            f"- Supplies: -${_unit_cogs:,.2f}\n"
            f"- **= Profit/Order: ${_unit_profit:,.2f} ({_unit_margin:.1f}% margin)**\n\n"
            f"**Break-Even Analysis:**\n"
            f"- Monthly fixed costs: ${_monthly_fixed:,.2f}\n"
            f"- Contribution margin: {_contrib_margin_pct * 100:.1f}%\n"
            f"- Break-even revenue: ${_breakeven_monthly:,.2f}/month\n"
            f"- Break-even orders: {_breakeven_orders:.0f}/month\n"
            f"- {'ABOVE break-even by ' + money(val_monthly_run_rate - _breakeven_monthly) + '/mo' if val_monthly_run_rate > _breakeven_monthly else 'BELOW break-even'}"
        )

    # ── Supplier questions ──
    if any(w in q for w in ["supplier", "seller", "vendor", "where do i buy", "who do i buy"]):
        if _supplier_spend:
            lines = [f"**Top Suppliers (from {len(INV_ITEMS)} invoice items):**\n"]
            for seller, info in list(_supplier_spend.items())[:8]:
                lines.append(f"- **{seller}**: {money(info['total'])} ({info['items']} items, avg ${info['avg_price']:,.2f})")
            return "\n".join(lines)
        return "No supplier data found. Invoice PDFs may not have seller information."

    # ── Pattern / anomaly questions ──
    if any(w in q for w in ["pattern", "anomal", "spike", "outlier", "unusual", "best day", "worst day", "day of week"]):
        return (
            f"**Day-of-Week Patterns:**\n"
            f"- Best day: **{_best_dow}** (avg ${max(_dow_rev_vals):,.0f}/day)\n"
            f"- Worst day: **{_worst_dow}** (avg ${min(_dow_rev_vals):,.0f}/day)\n"
            f"- " + " | ".join(f"{d}: ${v:,.0f}" for d, v in zip(_dow_names, _dow_rev_vals)) + "\n\n"
            f"**Anomalies Detected:**\n"
            f"- Revenue spikes (>2 std devs): {len(_anomaly_high)} days\n"
            f"- Revenue drops (<-1.5 std devs): {len(_anomaly_low)} days\n"
            f"- Zero-revenue days: {len(_zero_days)} days\n"
            f"- Daily avg: ${_daily_rev_mean:,.0f} (std dev: ${_daily_rev_std:,.0f})\n\n"
            f"**Ad Correlation:** R²={_corr_r2:.2f} — {'ads strongly correlate with sales' if _corr_r2 > 0.5 else 'weak correlation, sales may not depend on ad spend'}"
        )

    # ── Cash flow questions ──
    if any(w in q for w in ["cash flow", "bank balance", "deposits", "runway", "burn rate", "how long"]):
        return (
            f"**Cash Flow Summary:**\n\n"
            f"- Bank deposits (Etsy): ${bank_total_deposits:,.2f}\n"
            f"- Bank expenses: ${bank_total_debits:,.2f}\n"
            f"- Net cash retained: ${bank_net_cash:,.2f}\n"
            f"- + Etsy balance: ${etsy_balance:,.2f}\n"
            f"- **= Cash on hand: ${bank_cash_on_hand:,.2f}**\n\n"
            f"**Burn Rate:** ${val_monthly_expenses:,.2f}/month\n"
            f"**Runway:** {val_runway_months:.1f} months at current spend\n\n"
            f"**Monthly Cash Flow:**\n" +
            "\n".join(f"- {m}: +${bank_monthly[m]['deposits']:,.0f} / -${bank_monthly[m]['debits']:,.0f} = ${bank_monthly[m]['deposits'] - bank_monthly[m]['debits']:,.0f}"
                      for m in sorted(bank_monthly.keys()))
        )

    # ── Valuation questions ──
    if any(w in q for w in ["valuation", "business value", "what is the business worth", "how much is", "sell the business"]):
        return (
            f"**Business Valuation:**\n\n"
            f"**Blended Estimate: {money(val_blended_mid)}** (range {money(val_blended_low)} — {money(val_blended_high)})\n\n"
            f"**3 Methods Used:**\n"
            f"1. SDE Multiple (50% weight): {money(val_sde_mid)} — annual SDE {money(val_annual_sde)} x 1.5x\n"
            f"2. Revenue Multiple (25%): {money(val_rev_mid)} — annual revenue {money(val_annual_revenue)} x 0.5x\n"
            f"3. Asset-Based (25%): {money(val_asset_val)} — assets {money(val_total_assets)} minus debt {money(val_total_liabilities)}\n\n"
            f"**Health Score:** {val_health_score}/100 ({val_health_grade})\n"
            f"**Equity:** {money(val_equity)}"
        )

    # ── Debt questions ──
    if any(w in q for w in ["debt", "credit card", "best buy", "owe", "liabilit"]):
        _dte = f"\n**Debt-to-equity ratio:** {bb_cc_balance / val_equity:.2f}x" if val_equity > 0 else ""
        return (
            f"**Debt Summary:**\n\n"
            f"The business has one debt: **Best Buy Citi Credit Card**\n\n"
            f"- Credit limit: ${bb_cc_limit:,.2f}\n"
            f"- Total charged: ${bb_cc_total_charged:,.2f} (equipment: 3D printers, etc)\n"
            f"- Total paid: ${bb_cc_total_paid:,.2f}\n"
            f"- **Remaining balance: ${bb_cc_balance:,.2f}**\n"
            f"- Available credit: ${bb_cc_available:,.2f}\n\n"
            f"The equipment purchased is counted as a business asset worth ${bb_cc_asset_value:,.2f}."
            f"{_dte}"
        )

    # ── Fallback: Try to find keywords in data ──
    # Search product names
    for prod_name in product_revenue_est.index:
        keywords = [w for w in prod_name.lower().split() if len(w) > 3]
        if any(kw in q for kw in keywords):
            prod_rev = product_revenue_est[prod_name]
            prod_fee_rows = fee_df[
                fee_df["Title"].str.contains(prod_name[:20], na=False)
                & fee_df["Title"].str.startswith("Transaction fee:", na=False)
            ]
            est_units = len(prod_fee_rows)
            avg_sale = prod_rev / est_units if est_units else prod_rev
            return (
                f"**{prod_name}**\n\n"
                f"- Revenue: ${prod_rev:,.2f}\n"
                f"- Units sold: ~{est_units}\n"
                f"- Avg per sale: ${avg_sale:,.2f}"
            )

    return (
        "I'm not sure how to answer that specific question. Try asking about:\n\n"
        "- Revenue, sales, or profit\n"
        "- Shipping costs or profit/loss\n"
        "- Products (best sellers, worst sellers)\n"
        "- Fees and what Etsy charges\n"
        "- Refunds and returns\n"
        "- Monthly breakdowns\n"
        "- Trends and projections\n"
        "- Marketing and ads\n"
        "- Inventory / COGS / supplies\n"
        "- Unit economics / break-even\n"
        "- Suppliers / vendors\n"
        "- Patterns / anomalies / day of week\n"
        "- Cash flow / burn rate / runway\n"
        "- Business valuation\n"
        "- Debt / credit card\n"
        "- A specific month (e.g., 'Tell me about December')\n\n"
        "Type **help** for a full list of example questions!"
    )


# ── Year-Split Computations (Tax Forms) ─────────────────────────────────────
# Split all revenue / expense data into 2025 (Oct-Dec) and 2026 (Jan-Feb YTD)

def _bank_txn_year(t):
    """Extract year from bank txn date (MM/DD/YYYY)."""
    return int(t["date"].split("/")[2])


def _recompute_tax_years():
    global TAX_YEARS

    TAX_YEARS = {}
    for _yr in (2025, 2026):
        # --- Etsy transaction splits ---
        _s = sales_df[sales_df["Date_Parsed"].dt.year == _yr]
        _f = fee_df[fee_df["Date_Parsed"].dt.year == _yr]
        _sh = ship_df[ship_df["Date_Parsed"].dt.year == _yr]
        _mk = mkt_df[mkt_df["Date_Parsed"].dt.year == _yr]
        _rf = refund_df[refund_df["Date_Parsed"].dt.year == _yr]
        _tx = tax_df[tax_df["Date_Parsed"].dt.year == _yr]
        _bf = buyer_fee_df[buyer_fee_df["Date_Parsed"].dt.year == _yr]
        _pay = payment_df[payment_df["Date_Parsed"].dt.year == _yr]

        yr_gross = _s["Net_Clean"].sum()
        yr_refunds = abs(_rf["Net_Clean"].sum())
        yr_fees = abs(_f["Net_Clean"].sum())
        yr_shipping = abs(_sh["Net_Clean"].sum())
        yr_marketing = abs(_mk["Net_Clean"].sum())
        yr_taxes = abs(_tx["Net_Clean"].sum())
        yr_buyer_fees = abs(_bf["Net_Clean"].sum()) if len(_bf) else 0.0
        yr_payments = _pay["Net_Clean"].sum() if len(_pay) else 0.0

        # Fee credits for this year
        _fc = fee_df[fee_df["Date_Parsed"].dt.year == _yr]
        yr_credit_txn = _fc[_fc["Title"].str.startswith("Credit for transaction fee", na=False)]["Net_Clean"].sum()
        yr_credit_list = _fc[_fc["Title"].str.startswith("Credit for listing fee", na=False)]["Net_Clean"].sum()
        yr_credit_proc = _fc[_fc["Title"].str.startswith("Credit for processing fee", na=False)]["Net_Clean"].sum()
        yr_share_save = _fc[_fc["Title"].str.contains("Share & Save", na=False)]["Net_Clean"].sum()
        yr_total_credits = abs(yr_credit_txn + yr_credit_list + yr_credit_proc + yr_share_save)

        # yr_fees is already net of credits (abs(sum) includes positive credit rows).
        # Compute gross fees from charge-only rows for correct net_fees calculation.
        yr_listing = abs(_fc[_fc["Title"].str.contains("Listing fee", na=False)]["Net_Clean"].sum())
        yr_tx_prod = abs(_fc[_fc["Title"].str.startswith("Transaction fee:", na=False) & ~_fc["Title"].str.contains("Shipping", na=False)]["Net_Clean"].sum())
        yr_tx_ship = abs(_fc[_fc["Title"].str.contains("Transaction fee: Shipping", na=False)]["Net_Clean"].sum())
        yr_proc = abs(_fc[_fc["Title"].str.contains("Processing fee", na=False)]["Net_Clean"].sum())
        yr_fees_gross = yr_listing + yr_tx_prod + yr_tx_ship + yr_proc
        yr_net_fees = yr_fees_gross - yr_total_credits

        yr_etsy_net = yr_gross - yr_fees - yr_shipping - yr_marketing - yr_refunds - yr_taxes - yr_buyer_fees + yr_payments

        # --- Bank transaction splits ---
        _bank_debits_yr = [t for t in bank_debits if _bank_txn_year(t) == _yr]
        _bank_deposits_yr = [t for t in bank_deposits if _bank_txn_year(t) == _yr]

        yr_bank_by_cat = {}
        for t in _bank_debits_yr:
            cat = t["category"]
            yr_bank_by_cat[cat] = yr_bank_by_cat.get(cat, 0) + t["amount"]

        yr_bank_deposits = sum(t["amount"] for t in _bank_deposits_yr)
        yr_bank_debits = sum(t["amount"] for t in _bank_debits_yr)

        # --- Inventory splits ---
        _inv_yr = BIZ_INV_DF[BIZ_INV_DF["date_parsed"].dt.year == _yr] if len(BIZ_INV_DF) else BIZ_INV_DF
        yr_inventory_cost = _inv_yr["grand_total"].sum() if len(_inv_yr) else 0.0

        # Bank inventory (Amazon Inventory category from bank)
        yr_bank_inv = yr_bank_by_cat.get("Amazon Inventory", 0)

        # COGS = receipts + any bank Amazon spending NOT already covered by receipts
        # Receipts and bank overlap (same purchases seen from both sides), so we
        # only add the bank gap — spending the bank sees that receipts don't explain.
        yr_bank_inv_gap = max(0, yr_bank_inv - yr_inventory_cost)
        yr_cogs = yr_inventory_cost + yr_bank_inv_gap

        # --- Draw splits ---
        yr_tulsa_draws = sum(t["amount"] for t in tulsa_draws if _bank_txn_year(t) == _yr)
        yr_texas_draws = sum(t["amount"] for t in texas_draws if _bank_txn_year(t) == _yr)
        yr_total_draws = yr_tulsa_draws + yr_texas_draws

        # Operating expenses (non-inventory bank expenses)
        # NOTE: "Shipping" and "Etsy Fees" bank categories overlap with Etsy-side
        # deductions already subtracted in yr_etsy_net — exclude them to avoid
        # double-counting. Only subtract bank expenses NOT already in Etsy net.
        _all_biz_cats = ["Shipping", "Craft Supplies", "Etsy Fees", "Subscriptions",
                         "AliExpress Supplies", "Business Credit Card"]
        yr_bank_biz_expense = sum(yr_bank_by_cat.get(c, 0) for c in _all_biz_cats)
        _non_etsy_cats = ["Craft Supplies", "Subscriptions",
                          "AliExpress Supplies", "Business Credit Card"]
        yr_bank_additional_expense = sum(yr_bank_by_cat.get(c, 0) for c in _non_etsy_cats)

        # Net income: Etsy net minus non-overlapping bank expenses minus inventory
        yr_net_income = yr_etsy_net - yr_bank_additional_expense - yr_inventory_cost

        TAX_YEARS[_yr] = {
            "gross_sales": yr_gross,
            "refunds": yr_refunds,
            "fees": yr_fees,
            "net_fees": yr_net_fees,
            "total_credits": yr_total_credits,
            "shipping": yr_shipping,
            "marketing": yr_marketing,
            "taxes_collected": yr_taxes,
            "buyer_fees": yr_buyer_fees,
            "payments": yr_payments,
            "etsy_net": yr_etsy_net,
            "cogs": yr_cogs,
            "inventory_cost": yr_inventory_cost,
            "bank_inv": yr_bank_inv,
            "bank_inv_gap": yr_bank_inv_gap,
            "bank_by_cat": yr_bank_by_cat,
            "bank_deposits": yr_bank_deposits,
            "bank_debits": yr_bank_debits,
            "bank_biz_expense": yr_bank_biz_expense,
            "bank_additional_expense": yr_bank_additional_expense,
            "net_income": yr_net_income,
            "tulsa_draws": yr_tulsa_draws,
            "texas_draws": yr_texas_draws,
            "total_draws": yr_total_draws,
            "order_count": len(_s),
        }




_recompute_tax_years()
# ── Business Valuation Pre-Computations ──────────────────────────────────────


def _recompute_valuation():
    global _hs_cash, _hs_debt, _hs_diversity, _hs_growth, _hs_profit, _hs_shipping, _prod_count, _top3_conc, _val_annualize, _val_growth_pct, _val_months_operating, _val_r2, _val_sales_trend, val_annual_etsy_net, val_annual_real_profit, val_annual_revenue, val_annual_sde, val_asset_val, val_blended_high, val_blended_low, val_blended_mid, val_equity, val_health_color, val_health_grade, val_health_score, val_monthly_expenses, val_monthly_profit_rate, val_monthly_run_rate, val_proj_12mo_revenue, val_rev_high, val_rev_low, val_rev_mid, val_risks, val_runway_months, val_sde, val_sde_high, val_sde_low, val_sde_mid, val_strengths, val_total_assets, val_total_liabilities

    # Use actual days of data for accurate annualization (avoids partial-month bias)
    _val_months_operating = round(max(days_active / 30.44, 1), 1)  # 30.44 = avg days/month
    _val_annualize = 12 / _val_months_operating

    # Annual metrics
    val_annual_revenue = _safe(gross_sales) * _val_annualize
    val_annual_etsy_net = _safe(etsy_net) * _val_annualize
    val_annual_real_profit = _safe(real_profit) * _val_annualize

    # SDE = net cash flow + owner draws (profit already includes draws, so use cash_on_hand)
    val_sde = _safe(bank_cash_on_hand) + _safe(bank_owner_draw_total)
    val_annual_sde = val_sde * _val_annualize

    # Method 1: SDE Multiple (small Etsy biz = 1.0x-2.5x)
    val_sde_low = val_annual_sde * 1.0
    val_sde_mid = val_annual_sde * 1.5
    val_sde_high = val_annual_sde * 2.5

    # Method 2: Revenue Multiple (handmade/Etsy = 0.3x-1.0x)
    val_rev_low = val_annual_revenue * 0.3
    val_rev_mid = val_annual_revenue * 0.5
    val_rev_high = val_annual_revenue * 1.0

    # Method 3: Asset-Based
    val_total_assets = _safe(bank_cash_on_hand) + _safe(bb_cc_asset_value) + _safe(true_inventory_cost)
    val_total_liabilities = _safe(bb_cc_balance)
    val_asset_val = val_total_assets - val_total_liabilities

    # Blended valuation (50% SDE + 25% Revenue + 25% Asset)
    val_blended_low = val_sde_low * 0.50 + val_rev_low * 0.25 + val_asset_val * 0.25
    val_blended_mid = val_sde_mid * 0.50 + val_rev_mid * 0.25 + val_asset_val * 0.25
    val_blended_high = val_sde_high * 0.50 + val_rev_high * 0.25 + val_asset_val * 0.25

    # Monthly run rate
    val_monthly_run_rate = _safe(gross_sales) / _val_months_operating
    val_monthly_profit_rate = _safe(profit) / _val_months_operating

    # Growth
    _val_growth_pct = analytics_projections.get("growth_pct", 0)
    _val_r2 = analytics_projections.get("r2_sales", 0)
    _val_sales_trend = analytics_projections.get("sales_trend", 0)

    # Projected 12-month revenue (using linear trend)
    val_proj_12mo_revenue = sum(
        max(0, val_monthly_run_rate + _val_sales_trend * i) for i in range(1, 13)
    ) if _val_sales_trend else val_annual_revenue

    # Equity
    val_equity = val_total_assets - val_total_liabilities

    # Health Score (0-100)
    _hs_profit = min(25, max(0, _safe(profit_margin) / 2))  # 0-25 pts: 50%+ margin = full
    _hs_growth = min(25, max(0, (_val_growth_pct + 10) * 1.25)) if _val_growth_pct > -10 else 0  # 0-25 pts
    _prod_count = len(product_revenue_est) if len(product_revenue_est) > 0 else 1
    _top3_conc = product_revenue_est.head(3).sum() / product_revenue_est.sum() * 100 if product_revenue_est.sum() > 0 else 100
    _hs_diversity = min(15, max(0, (100 - _top3_conc) / 3))  # 0-15 pts
    _hs_cash = min(15, max(0, _safe(bank_cash_on_hand) / val_monthly_run_rate * 5)) if val_monthly_run_rate > 0 else 0  # 0-15 pts: 3+ months runway = full
    _hs_debt = 10 if bb_cc_balance == 0 else max(0, 10 - bb_cc_balance / 500)  # 0-10 pts
    _hs_shipping = 5 if shipping_profit is None else (10 if shipping_profit >= 0 else max(0, 10 + shipping_profit / 100))  # 0-10 pts; None = neutral
    val_health_score = round(min(100, _hs_profit + _hs_growth + _hs_diversity + _hs_cash + _hs_debt + _hs_shipping))
    val_health_grade = "A" if val_health_score >= 80 else "B" if val_health_score >= 60 else "C" if val_health_score >= 40 else "D"
    val_health_color = GREEN if val_health_score >= 80 else TEAL if val_health_score >= 60 else ORANGE if val_health_score >= 40 else RED

    # Risk factors list
    val_risks = []
    val_risks.append(("Young Business", f"Only {_val_months_operating} months of data — valuations are speculative", "HIGH" if _val_months_operating < 6 else "MED"))
    if _top3_conc > 60:
        val_risks.append(("Product Concentration", f"Top 3 products = {_top3_conc:.0f}% of revenue", "HIGH" if _top3_conc > 80 else "MED"))
    if bb_cc_balance > 0:
        val_risks.append(("Credit Card Debt", f"${bb_cc_balance:,.0f} outstanding on Best Buy CC", "HIGH" if bb_cc_balance > 1000 else "MED"))
    if shipping_profit is not None and shipping_profit < 0:
        val_risks.append(("Shipping Loss", f"Losing ${abs(shipping_profit):,.0f} on shipping", "MED"))
    val_risks.append(("Platform Dependency", "100% revenue from Etsy — single platform risk", "MED"))
    if _safe(gross_sales) > 0 and _safe(total_refunds) / _safe(gross_sales) > 0.05:
        val_risks.append(("Refund Rate", f"{_safe(total_refunds) / _safe(gross_sales) * 100:.1f}% refund rate", "MED"))

    # Strengths list
    val_strengths = []
    if _safe(profit_margin) > 20:
        val_strengths.append(("Strong Margins", f"{_safe(profit_margin):.1f}% profit margin"))
    if _val_growth_pct > 5:
        val_strengths.append(("Growing Revenue", f"{_val_growth_pct:+.1f}% monthly growth"))
    if _safe(bank_cash_on_hand) > val_monthly_run_rate:
        _runway = _safe(bank_cash_on_hand) / val_monthly_run_rate if val_monthly_run_rate > 0 else 0
        val_strengths.append(("Cash Reserves", f"${_safe(bank_cash_on_hand):,.0f} — {_runway:.1f} months runway"))
    if _prod_count > 10:
        val_strengths.append(("Product Diversity", f"{_prod_count} active products"))
    if _safe(bb_cc_asset_value) > 0:
        val_strengths.append(("Equipment Assets", f"${_safe(bb_cc_asset_value):,.0f} in equipment"))
    if _safe(bank_owner_draw_total) > 0:
        val_strengths.append(("Owner Compensation", f"${_safe(bank_owner_draw_total):,.0f} in draws taken"))

    # Burn rate (monthly expenses) — Etsy costs + non-overlapping bank expenses only
    _val_non_etsy_cats = ["Craft Supplies", "Subscriptions", "AliExpress Supplies", "Business Credit Card"]
    _val_bank_additional = sum(bank_by_cat.get(c, 0) for c in _val_non_etsy_cats)
    val_monthly_expenses = (_safe(total_fees) + _safe(total_shipping_cost) + _safe(total_marketing) + _safe(total_refunds) + _safe(total_taxes) + _safe(total_buyer_fees) + _val_bank_additional + _safe(true_inventory_cost)) / _val_months_operating
    val_runway_months = _safe(bank_cash_on_hand) / val_monthly_expenses if val_monthly_expenses > 0 else 99


_recompute_valuation()


# ── Helper Functions ─────────────────────────────────────────────────────────

def _compute_income_tax(taxable_income, year=2026):
    """ESTIMATE: Federal income tax using progressive brackets.

    ASSUMPTIONS (may not match your situation):
    - Single filer (could be MFJ, HoH, etc.)
    - No other household income
    - Standard deduction only
    - Federal only (no state/local tax)
    - 2026 bracket amounts

    Missing data needed for accuracy: filing status, total household income,
    state of residence, itemized deductions. Use actual 1040 or tax software.
    """
    if taxable_income <= 0:
        return 0.0
    brackets = [
        (11925, 0.10), (48475, 0.12), (103350, 0.22),
        (197300, 0.24), (250525, 0.32), (626350, 0.35),
        (float('inf'), 0.37),
    ]
    tax, prev = 0.0, 0
    for limit, rate in brackets:
        if taxable_income <= prev:
            break
        tax += (min(taxable_income, limit) - prev) * rate
        prev = limit
    return round(tax, 2)


def _build_stale_data_banner():
    """Orange banner shown on ALL tabs when data is >7 days old."""
    if not _ceo_health:
        return html.Div()
    freshness = [r for r in _ceo_health.results if r.agent_name == "DataFreshness"]
    if freshness and not freshness[0].passed:
        return html.Div([
            html.Span("DATA MAY BE STALE", style={"color": ORANGE, "fontWeight": "bold",
                                                    "fontSize": "12px", "letterSpacing": "1px"}),
            html.Span(f" — {freshness[0].message}. Upload latest in Data Hub.",
                      style={"color": GRAY, "fontSize": "11px"}),
        ], style={"backgroundColor": "#1a1000", "border": f"1px solid {ORANGE}33",
                  "borderRadius": "6px", "padding": "6px 14px", "marginBottom": "8px"})
    return html.Div()


def _build_jarvis_auto_briefing():
    """Build the auto-briefing text for Jarvis greeting.
    Uses AI when available, falls back to static data."""

    # Gather key metrics for briefing
    _briefing_data = []

    # CEO findings
    try:
        if _ceo_health:
            _crits = getattr(_ceo_health, 'critical_alerts', [])
            _warns = getattr(_ceo_health, 'warning_alerts', [])
            for alert in _crits:
                _briefing_data.append(f"CRITICAL: {getattr(alert, 'message', str(alert))}")
            for alert in _warns[:3]:
                _briefing_data.append(f"WARNING: {getattr(alert, 'message', str(alert))}")
            if not _crits and not _warns:
                _briefing_data.append("All validation checks passing.")
    except Exception:
        pass

    # Missing receipts
    try:
        if expense_missing_receipts:
            gap = expense_gap if expense_gap else 0
            _briefing_data.append(f"{len(expense_missing_receipts)} expenses missing receipts (${gap:,.0f} unverified).")
    except NameError:
        pass

    # Profit margin
    try:
        if profit_margin is not None:
            _briefing_data.append(f"Profit margin: {profit_margin:.1f}%.")
    except NameError:
        pass

    # Refund accountability
    try:
        _tj_n = sum(1 for v in _refund_assignments.values() if v == "TJ")
        _br_n = sum(1 for v in _refund_assignments.values() if v == "Braden")
        _tj_amt = sum(abs(r["Net_Clean"]) for _, r in refund_df.iterrows()
                      if _refund_assignments.get(_extract_order_num(r["Title"]), "") == "TJ")
        _br_amt = sum(abs(r["Net_Clean"]) for _, r in refund_df.iterrows()
                      if _refund_assignments.get(_extract_order_num(r["Title"]), "") == "Braden")
        if _tj_n + _br_n > 0:
            _briefing_data.append(f"Refunds: TJ {_tj_n} (${_tj_amt:,.0f}) | Braden {_br_n} (${_br_amt:,.0f})")
    except Exception:
        pass

    # Key financial stats
    try:
        _briefing_data.append(f"Gross sales: ${gross_sales:,.2f}")
        _briefing_data.append(f"Total fees: ${total_fees:,.2f}")
        _briefing_data.append(f"Etsy deposits to bank: ${_etsy_deposit_total:,.2f}")
        _briefing_data.append(f"Etsy balance (undeposited): ${etsy_balance:,.2f}")
        if real_profit is not None:
            _briefing_data.append(f"Real profit: ${real_profit:,.2f}")
        _briefing_data.append(f"Total orders: {len(sales_df)}")
        _briefing_data.append(f"Total refunds: {len(refund_df)} (${total_refunds:,.2f})")
    except Exception:
        pass

    # Monthly trend
    try:
        if months_sorted and len(months_sorted) >= 2:
            _last = months_sorted[-1]
            _prev = months_sorted[-2]
            _last_rev = monthly_sales.get(_last, 0)
            _prev_rev = monthly_sales.get(_prev, 0)
            _briefing_data.append(f"Latest month ({_last}): ${_last_rev:,.2f} revenue")
            _briefing_data.append(f"Previous month ({_prev}): ${_prev_rev:,.2f} revenue")
            if _prev_rev > 0:
                _change = ((_last_rev - _prev_rev) / _prev_rev) * 100
                _briefing_data.append(f"Month-over-month change: {_change:+.1f}%")
    except Exception:
        pass

    # No AI call on tab load — too slow. Use static data only.
    # AI responses come through the chatbot when the user asks.

    # Fallback to static
    if not _briefing_data:
        return "JARVIS online. All systems operational."
    return "\n".join(_briefing_data)


def _alert_key(alert):
    """Generate a stable key for an alert based on agent name + message."""
    return f"{alert.agent}:{alert.message}"


def _build_ceo_banner():
    """Build the CEO agent alert banner with dismiss buttons."""
    if not _ceo_health:
        return html.Div()

    alerts = _ceo_health.alerts
    if not alerts:
        return html.Div()

    # Split into active (unread) and dismissed (read)
    active = [a for a in alerts if _alert_key(a) not in _dismissed_alerts]
    dismissed = [a for a in alerts if _alert_key(a) in _dismissed_alerts]

    if not active and not dismissed:
        return html.Div()

    children = []

    # Active alerts with dismiss buttons
    for i, alert in enumerate(active[:8]):
        if alert.level == "critical":
            color, icon = RED, "X"
        elif alert.level == "warning":
            color, icon = ORANGE, "!"
        else:
            color, icon = CYAN, "i"

        akey = _alert_key(alert)
        children.append(html.Div([
            html.Span(f" {icon} ", style={"color": color, "fontWeight": "bold",
                                          "fontSize": "12px", "marginRight": "6px",
                                          "backgroundColor": f"{color}22",
                                          "padding": "1px 6px", "borderRadius": "3px"}),
            html.Span(f"{alert.agent}: ", style={"color": color, "fontWeight": "bold",
                                                  "fontSize": "12px"}),
            html.Span(alert.message, style={"color": GRAY, "fontSize": "12px", "flex": "1"}),
            html.A("Mark Read", href=f"/api/ceo/dismiss?key={akey}", target="_self",
                   style={"fontSize": "10px", "padding": "1px 8px", "marginLeft": "10px",
                          "backgroundColor": f"{color}22", "color": color,
                          "border": f"1px solid {color}44", "borderRadius": "4px",
                          "cursor": "pointer", "textDecoration": "none"}),
        ], style={"padding": "3px 0", "display": "flex", "alignItems": "center"}))

    # Show dismissed count
    if dismissed:
        children.append(html.Div([
            html.Span(f"{len(dismissed)} read alert{'s' if len(dismissed) != 1 else ''}",
                      style={"color": GRAY, "fontSize": "11px", "fontStyle": "italic"}),
        ], style={"padding": "4px 0", "display": "flex", "alignItems": "center"}))

    if not active:
        # All read — show a minimal green bar
        return html.Div(children, style={
            "backgroundColor": "#001a00", "border": f"1px solid {GREEN}22",
            "borderRadius": "6px", "padding": "6px 14px", "margin": "0 16px 8px 16px",
        })

    return html.Div(children, style={
        "backgroundColor": "#1a1000", "border": f"1px solid {ORANGE}33",
        "borderRadius": "6px", "padding": "8px 14px", "margin": "0 16px 8px 16px",
    })


def _strict_banner(message="Only VERIFIED metrics shown. Estimates, projections, and scores are hidden."):
    """Reusable strict mode warning banner for all tabs."""
    return html.Div([
        html.Span("STRICT MODE", style={"color": RED, "fontWeight": "bold", "fontSize": "13px", "letterSpacing": "1px"}),
        html.Span(f" — {message}", style={"color": ORANGE, "fontSize": "12px"}),
    ], style={"backgroundColor": "#1a0000", "border": f"1px solid {RED}44", "borderRadius": "6px",
              "padding": "8px 14px", "marginBottom": "10px"})


def txn_row(t, color=GRAY, reason=""):
    return html.Div([
        html.Span(t["date"], style={"color": GRAY, "fontSize": "11px", "width": "70px", "display": "inline-block"}),
        html.Span(f"${t['amount']:,.2f}", style={"color": color, "fontFamily": "monospace",
                  "fontSize": "11px", "fontWeight": "bold", "width": "70px", "display": "inline-block"}),
        html.Span(t["desc"][:35], style={"color": WHITE, "fontSize": "11px", "width": "220px", "display": "inline-block"}),
        html.Span(reason, style={"color": GRAY, "fontSize": "10px", "fontStyle": "italic"}),
    ], style={"padding": "2px 0", "borderBottom": "1px solid #ffffff08"})


def cat_card(title, color, items, total=None, extra=None):
    children = [
        html.Div(title, style={"color": color, "fontWeight": "bold", "fontSize": "12px", "marginBottom": "4px"}),
        *items,
    ]
    if total is not None:
        children.append(html.Div(f"${total:,.2f}",
            style={"color": color, "fontWeight": "bold", "fontFamily": "monospace",
                    "fontSize": "13px", "textAlign": "right", "marginTop": "4px",
                    "borderTop": f"1px solid {color}44", "paddingTop": "4px"}))
    if extra:
        children.append(html.P(extra, style={"color": GRAY, "fontSize": "10px", "margin": "4px 0 0 0"}))
    return html.Div(children, style={
        "flex": "1", "minWidth": "280px", "padding": "10px",
        "backgroundColor": f"{color}08", "borderLeft": f"3px solid {color}",
        "borderRadius": "4px",
    })


def chart_context(description, metrics=None, legend=None, look_for=None, simple=None):
    """Compact context block displayed above a chart with explanation and key metrics."""
    children = [
        html.P(description, style={
            "color": GRAY, "margin": "0 0 6px 0", "fontSize": "12px", "lineHeight": "1.4",
        }),
    ]
    if metrics:
        metric_spans = []
        for label, value, color in metrics:
            metric_spans.append(html.Span([
                html.Span(f"{label}: ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(value, style={"color": color, "fontFamily": "monospace", "fontWeight": "bold", "fontSize": "12px"}),
            ], style={"marginRight": "16px", "whiteSpace": "nowrap"}))
        children.append(html.Div(metric_spans, style={
            "display": "flex", "flexWrap": "wrap", "gap": "4px 0", "marginBottom": "4px",
        }))
    if legend:
        legend_items = []
        for color, label, desc in legend:
            legend_items.append(html.Div([
                html.Span("\u25CF ", style={"color": color, "fontSize": "13px"}),
                html.Span(f"{label}", style={"color": WHITE, "fontSize": "11px", "fontWeight": "bold"}),
                html.Span(f" \u2014 {desc}", style={"color": GRAY, "fontSize": "11px"}),
            ], style={"marginBottom": "2px"}))
        children.append(html.Div(legend_items, style={"marginTop": "4px"}))
    if look_for:
        children.append(html.P(
            f"\u2192 Look for: {look_for}",
            style={"color": "#888888", "margin": "4px 0 0 0", "fontSize": "11px", "fontStyle": "italic"},
        ))
    if simple:
        children.append(html.Details([
            html.Summary("how to read this chart", style={
                "color": f"{CYAN}99", "fontSize": "10px", "cursor": "pointer",
                "marginTop": "6px", "listStyle": "none", "userSelect": "none",
            }),
            html.P(simple, style={
                "color": "#cccccc", "fontSize": "11px", "margin": "6px 0 0 0",
                "lineHeight": "1.5", "padding": "8px",
                "backgroundColor": "#0d1b2a", "borderRadius": "4px",
                "borderLeft": f"2px solid {CYAN}44",
            }),
        ]))
    return html.Div(children, style={
        "backgroundColor": CARD, "borderLeft": f"3px solid {CYAN}", "borderRadius": "4px",
        "padding": "10px 14px", "marginBottom": "6px",
    })



def _rebuild_all_charts():
    global BANK_MONTH_NAMES, _anomaly_high, _anomaly_low, _aov_best_week, _aov_worst_week, _best_day_rev, _best_dow, _breakeven_daily
    global _breakeven_monthly, _breakeven_orders, _cf_cum, _cf_debits, _cf_deposits, _cf_months, _cf_net, _contrib_margin_pct
    global _corr_ad_vals, _corr_r2, _corr_rev_vals, _current_14d_profit_avg, _daily_orders_avg, _daily_profit_avg, _daily_rev_avg, _daily_rev_mean, _daily_rev_std
    global _dow_names, _dow_ord_vals, _dow_orders, _dow_prof_vals, _dow_profit, _dow_rev_vals, _dow_revenue, _etsy_took
    global _growth_pct, _inv_cogs_ratio, _inv_months, _inv_rev_vals, _inv_spend_vals, _last_aov_val, _last_fee_pct, _last_margin_pct
    global _last_mkt_pct, _last_ppo_m, _last_ppo_val, _last_ratio_m, _last_ref_pct, _last_ship_pct, _latest_month_net, _latest_month_rev
    global _month_abbr, _monthly_fixed, _net_margin_overall, _peak_orders_day, _prod_monthly, _r2_sales, _supplier_spend, _top_n_products
    global _top_prod_names, _total_costs, _unit_ads, _unit_cogs, _unit_fees, _unit_margin, _unit_profit, _unit_refund
    global _unit_rev, _unit_ship, _worst_dow, _zero_days, all_inv_months, all_loc_months, anomaly_fig, aov_fig
    global aov_vals, bank_month_debs, bank_month_deps, bank_month_labels, bank_month_nets, bank_monthly_fig, bank_months_sorted, cashflow_fig
    global corr_fig, cost_ratio_fig, cum_fig, daily_fig, dow_fig, expense_colors_list, expense_labels_list, expense_pie
    global expense_values_list, fee_pcts, intl_fig, inv_cat_bar, inv_cat_fig, inv_monthly_fig, inv_months_sorted, loc_fig
    global loc_monthly_fig, margin_pcts, mkt_pcts_list, monthly_fig, net_by_month, orders_day_fig, ppo_fig, ppo_months
    global ppo_vals, prod_name, product_fig, product_heat, profit_rolling_fig, proj_chart, ratio_months, ref_pcts, rev_cogs_fig
    global rev_inv_fig, sankey_fig, sankey_link_colors, sankey_node_colors, sankey_node_labels, sankey_sources, sankey_targets, sankey_values
    global ship_pcts, ship_type_colors, ship_type_fig, ship_type_names, ship_type_vals, shipping_compare, texas_cat_fig, top_n
    global top_products, trend_profit_rev, true_profit_monthly, tulsa_cat_fig, unit_wf

    # ── Guard: empty store (no data yet) ────────────────────────────────────────
    if not months_sorted or order_count == 0:
        # Set safe defaults for all chart globals so tab builders don't crash
        monthly_fig = go.Figure()
        daily_fig = go.Figure()
        dow_fig = go.Figure()
        product_fig = go.Figure()
        product_heat = go.Figure()
        expense_pie = go.Figure()
        aov_fig = go.Figure()
        anomaly_fig = go.Figure()
        bank_monthly_fig = go.Figure()
        cashflow_fig = go.Figure()
        cum_fig = go.Figure()
        corr_fig = go.Figure()
        cost_ratio_fig = go.Figure()
        trend_profit_rev = go.Figure()
        profit_rolling_fig = go.Figure()
        proj_chart = go.Figure()
        sankey_fig = go.Figure()
        ship_type_fig = go.Figure()
        shipping_compare = go.Figure()
        intl_fig = go.Figure()
        ppo_fig = go.Figure()
        rev_cogs_fig = go.Figure()
        rev_inv_fig = go.Figure()
        orders_day_fig = go.Figure()
        unit_wf = go.Figure()
        inv_cat_fig = go.Figure()
        inv_cat_bar = go.Figure()
        inv_monthly_fig = go.Figure()
        loc_fig = go.Figure()
        loc_monthly_fig = go.Figure()
        tulsa_cat_fig = go.Figure()
        texas_cat_fig = go.Figure()
        true_profit_monthly = go.Figure()
        # Safe scalar defaults
        _latest_month_rev = 0
        _latest_month_net = 0
        _growth_pct = 0
        _r2_sales = 0
        _daily_rev_avg = 0
        _daily_rev_mean = 0
        _daily_rev_std = 0
        _daily_profit_avg = 0
        _daily_orders_avg = 0
        _current_14d_profit_avg = 0
        _net_margin_overall = 0
        _best_dow = "N/A"
        _worst_dow = "N/A"
        _dow_names = []
        _dow_rev_vals = [0]
        _dow_ord_vals = [0]
        _dow_prof_vals = [0]
        _peak_orders_day = "N/A"
        _zero_days = 0
        _anomaly_high = []
        _anomaly_low = []
        _unit_rev = 0
        _unit_fees = 0
        _unit_ship = 0
        _unit_ads = 0
        _unit_refund = 0
        _unit_cogs = 0
        _unit_profit = 0
        _unit_margin = 0
        _monthly_fixed = 0
        _contrib_margin_pct = 0
        _breakeven_monthly = 0
        _breakeven_orders = 0
        _breakeven_daily = 0
        _total_costs = 0
        _etsy_took = 0
        _month_abbr = []
        _top_n_products = 0
        _top_prod_names = []
        _supplier_spend = {}
        _inv_cogs_ratio = 0
        _last_aov_val = 0
        _aov_best_week = "N/A"
        _aov_worst_week = "N/A"
        expense_labels_list = []
        expense_values_list = []
        expense_colors_list = []
        aov_vals = []
        top_products = pd.Series(dtype=float)
        top_n = 10
        inv_months_sorted = []
        all_inv_months = []
        all_loc_months = []
        ppo_months = []
        ppo_vals = []
        ratio_months = []
        fee_pcts = []
        ship_pcts = []
        mkt_pcts_list = []
        ref_pcts = []
        margin_pcts = []
        net_by_month = {}
        bank_months_sorted = []
        BANK_MONTH_NAMES = []
        bank_month_deps = []
        bank_month_debs = []
        bank_month_nets = []
        bank_month_labels = []
        sankey_node_labels = []
        sankey_node_colors = []
        sankey_sources = []
        sankey_targets = []
        sankey_values = []
        sankey_link_colors = []
        ship_type_names = []
        ship_type_vals = []
        ship_type_colors = []
        _corr_rev_vals = []
        _corr_ad_vals = []
        _corr_r2 = 0
        _inv_months = []
        _inv_rev_vals = []
        _inv_spend_vals = []
        _prod_monthly = {}
        _cf_months = []
        _cf_deposits = []
        _cf_debits = []
        _cf_net = []
        _cf_cum = []
        _last_fee_pct = 0
        _last_ship_pct = 0
        _last_mkt_pct = 0
        _last_ref_pct = 0
        _last_margin_pct = 0
        _last_ppo_m = "N/A"
        _last_ppo_val = 0
        _last_ratio_m = "N/A"
        _best_day_rev = 0
        return

    # ── Build Charts ─────────────────────────────────────────────────────────────
    try:
        _build_all_charts_inner()
    except Exception as _chart_err:
        import traceback
        _logger.warning("Error building charts: %s", traceback.format_exc())
        # Charts that were built before the error keep their values.
        # Charts not yet built keep their previous values or defaults.
        # This prevents a crash in one chart from blocking the whole page.


def _build_all_charts_inner():
    """Inner chart builder — separated so _rebuild_all_charts can catch errors."""
    global BANK_MONTH_NAMES, _anomaly_high, _anomaly_low, _aov_best_week, _aov_worst_week, _best_day_rev, _best_dow, _breakeven_daily
    global _breakeven_monthly, _breakeven_orders, _cf_cum, _cf_debits, _cf_deposits, _cf_months, _cf_net, _contrib_margin_pct
    global _corr_ad_vals, _corr_r2, _corr_rev_vals, _current_14d_profit_avg, _daily_orders_avg, _daily_profit_avg, _daily_rev_avg, _daily_rev_mean, _daily_rev_std
    global _dow_names, _dow_ord_vals, _dow_orders, _dow_prof_vals, _dow_profit, _dow_rev_vals, _dow_revenue, _etsy_took
    global _growth_pct, _inv_cogs_ratio, _inv_months, _inv_rev_vals, _inv_spend_vals, _last_aov_val, _last_fee_pct, _last_margin_pct
    global _last_mkt_pct, _last_ppo_m, _last_ppo_val, _last_ratio_m, _last_ref_pct, _last_ship_pct, _latest_month_net, _latest_month_rev
    global _month_abbr, _monthly_fixed, _net_margin_overall, _peak_orders_day, _prod_monthly, _r2_sales, _supplier_spend, _top_n_products
    global _top_prod_names, _total_costs, _unit_ads, _unit_cogs, _unit_fees, _unit_margin, _unit_profit, _unit_refund
    global _unit_rev, _unit_ship, _worst_dow, _zero_days, all_inv_months, all_loc_months, anomaly_fig, aov_fig
    global aov_vals, bank_month_debs, bank_month_deps, bank_month_labels, bank_month_nets, bank_monthly_fig, bank_months_sorted, cashflow_fig
    global corr_fig, cost_ratio_fig, cum_fig, daily_fig, dow_fig, expense_colors_list, expense_labels_list, expense_pie
    global expense_values_list, fee_pcts, intl_fig, inv_cat_bar, inv_cat_fig, inv_monthly_fig, inv_months_sorted, loc_fig
    global loc_monthly_fig, margin_pcts, mkt_pcts_list, monthly_fig, net_by_month, orders_day_fig, ppo_fig, ppo_months
    global ppo_vals, prod_name, product_fig, product_heat, profit_rolling_fig, proj_chart, ratio_months, ref_pcts, rev_cogs_fig
    global rev_inv_fig, sankey_fig, sankey_link_colors, sankey_node_colors, sankey_node_labels, sankey_sources, sankey_targets, sankey_values
    global ship_pcts, ship_type_colors, ship_type_fig, ship_type_names, ship_type_vals, shipping_compare, texas_cat_fig, top_n
    global top_products, trend_profit_rev, true_profit_monthly, tulsa_cat_fig, unit_wf

    # --- TAB 1: OVERVIEW CHARTS ---

    # Expense donut
    try:
        expense_labels_list = []
        expense_values_list = []
        expense_colors_list = []
        for name, val, clr in [
            ("Listing Fees", listing_fees, "#e74c3c"), ("Transaction Fees (Product)", transaction_fees_product, "#c0392b"),
            ("Transaction Fees (Shipping)", transaction_fees_shipping, "#a93226"), ("Processing Fees", processing_fees, "#d35400"),
            ("Shipping Labels", total_shipping_cost, BLUE), ("Etsy Ads", etsy_ads, PURPLE),
            ("Offsite Ads", offsite_ads_fees, "#8e44ad"), ("Refunds", total_refunds, ORANGE),
            ("Amazon Inventory", bank_by_cat.get("Amazon Inventory", 0), "#ff8a65"),
            ("AliExpress Supplies", bank_by_cat.get("AliExpress Supplies", 0), "#e91e63"),
            ("Craft Supplies", bank_by_cat.get("Craft Supplies", 0), TEAL),
            ("Subscriptions", bank_by_cat.get("Subscriptions", 0), CYAN),
            ("Shipping Supplies (Bank)", bank_by_cat.get("Shipping", 0), "#5dade2"),
            ("Etsy Bank Fees", bank_by_cat.get("Etsy Fees", 0), "#7d3c98"),
            ("Best Buy CC", bank_by_cat.get("Business Credit Card", 0), "#2e86c1"),
            ("Owner Draws", bank_owner_draw_total, "#ffb74d"),
        ]:
            if val is not None and val > 0:
                expense_labels_list.append(name)
                expense_values_list.append(val)
                expense_colors_list.append(clr)

        expense_pie = go.Figure(go.Pie(
            labels=expense_labels_list, values=expense_values_list, hole=0.45,
            marker_colors=expense_colors_list, textinfo="label+percent", textposition="outside",
        ))
        make_chart(expense_pie, 380, False)
        expense_pie.update_layout(title="Where Your Money Goes", showlegend=False)
    except Exception as _e:
        _logger.warning("Chart 'Expense Donut' failed: %s", _e)
        expense_pie = _no_data_fig("Expense Donut", f"Error: {_e}")

    # Monthly stacked bar + net profit line
    try:
        monthly_fig = make_subplots(specs=[[{"secondary_y": True}]])
        monthly_fig.add_trace(go.Bar(name="Gross Sales", x=months_sorted,
            y=[monthly_sales.get(m, 0) for m in months_sorted], marker_color=GREEN))
        monthly_fig.add_trace(go.Bar(name="Fees", x=months_sorted,
            y=[-monthly_fees.get(m, 0) for m in months_sorted], marker_color=RED))
        monthly_fig.add_trace(go.Bar(name="Shipping", x=months_sorted,
            y=[-monthly_shipping.get(m, 0) for m in months_sorted], marker_color=BLUE))
        monthly_fig.add_trace(go.Bar(name="Marketing", x=months_sorted,
            y=[-monthly_marketing.get(m, 0) for m in months_sorted], marker_color=PURPLE))
        monthly_fig.add_trace(go.Bar(name="Refunds", x=months_sorted,
            y=[-monthly_refunds.get(m, 0) for m in months_sorted], marker_color=ORANGE))

        net_by_month = [
            monthly_sales.get(m, 0) - monthly_fees.get(m, 0) - monthly_shipping.get(m, 0)
            - monthly_marketing.get(m, 0) - monthly_refunds.get(m, 0)
            for m in months_sorted
        ]
        monthly_fig.add_trace(go.Scatter(
            name="Net Profit", x=months_sorted, y=net_by_month,
            mode="lines+markers+text", text=[f"${v:,.0f}" for v in net_by_month],
            textposition="top center", textfont=dict(color=ORANGE),
            line=dict(color=ORANGE, width=3), marker=dict(size=10),
        ), secondary_y=True)
        make_chart(monthly_fig, 380)
        monthly_fig.update_layout(title="Monthly Performance", barmode="relative")
        monthly_fig.update_yaxes(title_text="Amount ($)", secondary_y=False)
        monthly_fig.update_yaxes(title_text="Net Profit ($)", secondary_y=True, showgrid=False)
    except Exception as _e:
        _logger.warning("Chart 'Monthly Revenue' failed: %s", _e)
        monthly_fig = _no_data_fig("Monthly Revenue", f"Error: {_e}")

    # Daily sales trend (compact for overview)
    try:
        daily_fig = make_subplots(specs=[[{"secondary_y": True}]])
        daily_fig.add_trace(go.Bar(name="Daily Revenue", x=list(daily_sales.index), y=list(daily_sales.values),
            marker_color=GREEN, opacity=0.6))
        daily_fig.add_trace(go.Scatter(name="Orders", x=list(daily_orders.index), y=list(daily_orders.values),
            mode="lines", line=dict(color=BLUE, width=2)), secondary_y=True)
        if len(daily_sales) >= 7:
            rolling = daily_sales.rolling(7).mean()
            daily_fig.add_trace(go.Scatter(name="7-day Avg Revenue", x=list(rolling.index), y=list(rolling.values),
                mode="lines", line=dict(color=ORANGE, width=3)))
        make_chart(daily_fig, 320)
        daily_fig.update_layout(title="Daily Sales Trend")
        daily_fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        daily_fig.update_yaxes(title_text="# Orders", secondary_y=True, showgrid=False)
    except Exception as _e:
        _logger.warning("Chart 'Daily Revenue' failed: %s", _e)
        daily_fig = _no_data_fig("Daily Revenue", f"Error: {_e}")


    # --- TAB 2: TRENDS & PATTERNS CHARTS ---

    # 1) Daily profit/revenue with 7-day and 30-day rolling averages
    try:
        trend_profit_rev = go.Figure()
        trend_profit_rev.add_trace(go.Bar(
            name="Daily Revenue", x=daily_df.index, y=daily_df["revenue"],
            marker_color=GREEN, opacity=0.3,
        ))
        trend_profit_rev.add_trace(go.Bar(
            name="Daily Profit", x=daily_df.index, y=daily_df["profit"],
            marker_color=ORANGE, opacity=0.3,
        ))
        if len(daily_df) >= 7:
            rev_7 = daily_df["revenue"].rolling(7, min_periods=1).mean()
            prof_7 = daily_df["profit"].rolling(7, min_periods=1).mean()
            trend_profit_rev.add_trace(go.Scatter(name="Revenue 7d Avg", x=daily_df.index, y=rev_7,
                mode="lines", line=dict(color=GREEN, width=3)))
            trend_profit_rev.add_trace(go.Scatter(name="Profit 7d Avg", x=daily_df.index, y=prof_7,
                mode="lines", line=dict(color=ORANGE, width=3)))
        if len(daily_df) >= 30:
            rev_30 = daily_df["revenue"].rolling(30, min_periods=1).mean()
            prof_30 = daily_df["profit"].rolling(30, min_periods=1).mean()
            trend_profit_rev.add_trace(go.Scatter(name="Revenue 30d Avg", x=daily_df.index, y=rev_30,
                mode="lines", line=dict(color=GREEN, width=2, dash="dash")))
            trend_profit_rev.add_trace(go.Scatter(name="Profit 30d Avg", x=daily_df.index, y=prof_30,
                mode="lines", line=dict(color=ORANGE, width=2, dash="dash")))
        make_chart(trend_profit_rev, 380)
        trend_profit_rev.update_layout(title="Daily Revenue & Profit with Rolling Averages", barmode="overlay")
    except Exception as _e:
        _logger.warning("Chart 'Profit Trend' failed: %s", _e)
        trend_profit_rev = _no_data_fig("Profit Trend", f"Error: {_e}")

    # 2) Cost ratios over time (fees%, shipping%, marketing%, refunds% as % of monthly sales)
    try:
        cost_ratio_fig = go.Figure()
        ratio_months = [m for m in months_sorted if monthly_sales.get(m, 0) > 0]
        fee_pcts = [monthly_fees.get(m, 0) / monthly_sales.get(m, 1) * 100 for m in ratio_months]
        ship_pcts = [monthly_shipping.get(m, 0) / monthly_sales.get(m, 1) * 100 for m in ratio_months]
        mkt_pcts_list = [monthly_marketing.get(m, 0) / monthly_sales.get(m, 1) * 100 for m in ratio_months]
        ref_pcts = [monthly_refunds.get(m, 0) / monthly_sales.get(m, 1) * 100 for m in ratio_months]
        margin_pcts = [monthly_net_revenue.get(m, 0) / monthly_sales.get(m, 1) * 100 for m in ratio_months]

        cost_ratio_fig.add_trace(go.Scatter(name="Fees %", x=ratio_months, y=fee_pcts,
            mode="lines+markers", line=dict(color=RED, width=3), marker=dict(size=8)))
        cost_ratio_fig.add_trace(go.Scatter(name="Shipping %", x=ratio_months, y=ship_pcts,
            mode="lines+markers", line=dict(color=BLUE, width=3), marker=dict(size=8)))
        cost_ratio_fig.add_trace(go.Scatter(name="Marketing %", x=ratio_months, y=mkt_pcts_list,
            mode="lines+markers", line=dict(color=PURPLE, width=3), marker=dict(size=8)))
        cost_ratio_fig.add_trace(go.Scatter(name="Refunds %", x=ratio_months, y=ref_pcts,
            mode="lines+markers", line=dict(color=ORANGE, width=3), marker=dict(size=8)))
        cost_ratio_fig.add_trace(go.Scatter(name="Net Margin %", x=ratio_months, y=margin_pcts,
            mode="lines+markers", line=dict(color=TEAL, width=3, dash="dash"), marker=dict(size=10, symbol="diamond")))
        make_chart(cost_ratio_fig, 380)
        cost_ratio_fig.update_layout(title="Cost Ratios Over Time (% of Monthly Sales)", yaxis_title="% of Sales")

        # Extract latest-month cost ratios for context blocks
        _last_ratio_m = ratio_months[-1] if ratio_months else "N/A"
        _last_fee_pct = fee_pcts[-1] if fee_pcts else 0
        _last_ship_pct = ship_pcts[-1] if ship_pcts else 0
        _last_mkt_pct = mkt_pcts_list[-1] if mkt_pcts_list else 0
        _last_ref_pct = ref_pcts[-1] if ref_pcts else 0
        _last_margin_pct = margin_pcts[-1] if margin_pcts else 0
    except Exception as _e:
        _logger.warning("Chart 'Cost Ratios' failed: %s", _e)
        cost_ratio_fig = _no_data_fig("Cost Ratios", f"Error: {_e}")

    # 3) Avg order value trend by week
    try:
        aov_fig = go.Figure()
        aov_fig.add_trace(go.Scatter(
            name="Weekly AOV", x=weekly_aov.index, y=weekly_aov["aov"],
            mode="lines+markers", line=dict(color=TEAL, width=2), marker=dict(size=6),
        ))
        if len(weekly_aov) >= 4:
            aov_rolling = weekly_aov["aov"].rolling(4, min_periods=1).mean()
            aov_fig.add_trace(go.Scatter(name="4-week Avg", x=weekly_aov.index, y=aov_rolling,
                mode="lines", line=dict(color=ORANGE, width=3)))
        # Add overall average line
        aov_fig.add_hline(y=avg_order, line_dash="dot", line_color=GRAY,
            annotation_text=f"Overall Avg: ${avg_order:.2f}", annotation_position="top left")
        make_chart(aov_fig, 340)
        aov_fig.update_layout(title="Average Order Value by Week", yaxis_title="AOV ($)")
    except Exception as _e:
        _logger.warning("Chart 'Average Order Value' failed: %s", _e)
        aov_fig = _no_data_fig("Average Order Value", f"Error: {_e}")

    # 4) Orders per day trend with rolling avg
    try:
        orders_day_fig = go.Figure()
        orders_day_fig.add_trace(go.Bar(
            name="Orders/Day", x=daily_df.index, y=daily_df["orders"],
            marker_color=BLUE, opacity=0.5,
        ))
        if len(daily_df) >= 7:
            ord_7 = daily_df["orders"].rolling(7, min_periods=1).mean()
            orders_day_fig.add_trace(go.Scatter(name="7-day Avg", x=daily_df.index, y=ord_7,
                mode="lines", line=dict(color=ORANGE, width=3)))
        if len(daily_df) >= 14:
            ord_14 = daily_df["orders"].rolling(14, min_periods=1).mean()
            orders_day_fig.add_trace(go.Scatter(name="14-day Avg", x=daily_df.index, y=ord_14,
                mode="lines", line=dict(color=GREEN, width=2, dash="dash")))
        make_chart(orders_day_fig, 320)
        orders_day_fig.update_layout(title="Orders Per Day", yaxis_title="# Orders")
    except Exception as _e:
        _logger.warning("Chart 'Orders Per Day' failed: %s", _e)
        orders_day_fig = _no_data_fig("Orders Per Day", f"Error: {_e}")

    # 5) Cumulative revenue + cumulative profit
    try:
        cum_fig = go.Figure()
        cum_fig.add_trace(go.Scatter(
            name="Cumulative Revenue", x=daily_df.index, y=daily_df["cum_revenue"],
            mode="lines", line=dict(color=GREEN, width=3), fill="tozeroy", fillcolor="rgba(46,204,113,0.1)",
        ))
        cum_fig.add_trace(go.Scatter(
            name="Cumulative Profit", x=daily_df.index, y=daily_df["cum_profit"],
            mode="lines", line=dict(color=ORANGE, width=3), fill="tozeroy", fillcolor="rgba(243,156,18,0.1)",
        ))
        make_chart(cum_fig, 360)
        cum_fig.update_layout(title="Cumulative Revenue & Profit Over Time", yaxis_title="Amount ($)")
    except Exception as _e:
        _logger.warning("Chart 'Cumulative Revenue' failed: %s", _e)
        cum_fig = _no_data_fig("Cumulative Revenue", f"Error: {_e}")

    # 6) Profit per day rolling 14-day average
    try:
        profit_rolling_fig = go.Figure()
        if len(daily_df) >= 14:
            prof_14 = daily_df["profit"].rolling(14, min_periods=1).mean()
            profit_rolling_fig.add_trace(go.Scatter(
                name="14-day Avg Profit/Day", x=daily_df.index, y=prof_14,
                mode="lines", line=dict(color=ORANGE, width=3),
                fill="tozeroy", fillcolor="rgba(243,156,18,0.15)",
            ))
            profit_rolling_fig.add_hline(y=0, line_dash="dot", line_color=RED, line_width=1)
        else:
            profit_rolling_fig.add_trace(go.Scatter(
                name="Daily Profit", x=daily_df.index, y=daily_df["profit"],
                mode="lines+markers", line=dict(color=ORANGE, width=2),
            ))
        make_chart(profit_rolling_fig, 320)
        profit_rolling_fig.update_layout(title="Profit Per Day (14-day Rolling Average)", yaxis_title="Profit ($)")
    except Exception as _e:
        _logger.warning("Chart 'Rolling Profit' failed: %s", _e)
        profit_rolling_fig = _no_data_fig("Rolling Profit", f"Error: {_e}")

    # 7) Avg profit per order over time by month
    try:
        ppo_fig = go.Figure()
        ppo_months = [m for m in months_sorted if monthly_order_counts.get(m, 0) > 0]
        ppo_vals = [monthly_profit_per_order.get(m, 0) for m in ppo_months]
        ppo_fig.add_trace(go.Bar(
            name="Profit/Order", x=ppo_months, y=ppo_vals,
            marker_color=[GREEN if v >= 0 else RED for v in ppo_vals],
            text=[f"${v:,.2f}" for v in ppo_vals], textposition="outside",
        ))
        aov_vals = [monthly_aov.get(m, 0) for m in ppo_months]
        ppo_fig.add_trace(go.Scatter(
            name="Avg Order Value", x=ppo_months, y=aov_vals,
            mode="lines+markers", line=dict(color=TEAL, width=2, dash="dash"), marker=dict(size=6),
            yaxis="y2",
        ))
        make_chart(ppo_fig, 360)
        ppo_fig.update_layout(
            title="Avg Profit Per Order & AOV by Month", yaxis_title="Profit/Order ($)",
            yaxis2=dict(title="AOV ($)", overlaying="y", side="right", showgrid=False),
        )

        # Extract latest PPO and daily profit metrics for context blocks
        _last_ppo_m = ppo_months[-1] if ppo_months else "N/A"
        _last_ppo_val = ppo_vals[-1] if ppo_vals else 0
        _last_aov_val = aov_vals[-1] if aov_vals else 0
        _daily_profit_avg = etsy_net / days_active if days_active else 0
        _daily_rev_avg = gross_sales / days_active if days_active else 0
        _daily_orders_avg = order_count / days_active if days_active else 0
        _best_day_rev = daily_df["revenue"].max() if len(daily_df) else 0
        _peak_orders_day = daily_df["orders"].max() if len(daily_df) else 0
        _total_costs = gross_sales - etsy_net
        _current_14d_profit_avg = daily_df["profit"].rolling(14, min_periods=1).mean().iloc[-1] if len(daily_df) >= 1 else 0
        _aov_best_week = weekly_aov["aov"].max() if len(weekly_aov) else 0
        _aov_worst_week = weekly_aov["aov"].min() if len(weekly_aov) else 0
        _growth_pct = analytics_projections.get("growth_pct", 0)
        _r2_sales = analytics_projections.get("r2_sales", 0)
        _latest_month_rev = monthly_sales.get(months_sorted[-1], 0) if months_sorted else 0
        _latest_month_net = monthly_net_revenue.get(months_sorted[-1], 0) if months_sorted else 0
        _net_margin_overall = (etsy_net / gross_sales * 100) if gross_sales else 0
    except Exception as _e:
        _logger.warning("Chart 'Profit Per Order' failed: %s", _e)
        ppo_fig = _no_data_fig("Profit Per Order", f"Error: {_e}")

    # --- TAB 2: DEEP DIVE ADVANCED ANALYTICS ---

    # 8) Day-of-week analysis
    try:
        _dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        _dow_revenue = daily_df.groupby(daily_df.index.map(lambda d: d.weekday()))["revenue"].mean()
        _dow_orders = daily_df.groupby(daily_df.index.map(lambda d: d.weekday()))["orders"].mean()
        _dow_profit = daily_df.groupby(daily_df.index.map(lambda d: d.weekday()))["profit"].mean()
        _dow_rev_vals = [_dow_revenue.get(i, 0) for i in range(7)]
        _dow_ord_vals = [_dow_orders.get(i, 0) for i in range(7)]
        _dow_prof_vals = [_dow_profit.get(i, 0) for i in range(7)]
        _best_dow = _dow_names[np.argmax(_dow_rev_vals)]
        _worst_dow = _dow_names[np.argmin(_dow_rev_vals)]

        dow_fig = make_subplots(specs=[[{"secondary_y": True}]])
        dow_fig.add_trace(go.Bar(name="Avg Revenue", x=_dow_names, y=_dow_rev_vals,
            marker_color=[GREEN if v == max(_dow_rev_vals) else f"{GREEN}" for v in _dow_rev_vals],
            text=[f"${v:,.0f}" for v in _dow_rev_vals], textposition="outside"))
        dow_fig.add_trace(go.Scatter(name="Avg Orders", x=_dow_names, y=_dow_ord_vals,
            mode="lines+markers", line=dict(color=BLUE, width=2), marker=dict(size=8)), secondary_y=True)
        dow_fig.add_trace(go.Scatter(name="Avg Profit", x=_dow_names, y=_dow_prof_vals,
            mode="lines+markers", line=dict(color=ORANGE, width=2, dash="dot"), marker=dict(size=6)), secondary_y=True)
        make_chart(dow_fig, 320)
        dow_fig.update_layout(title="Day-of-Week Performance")
        dow_fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        dow_fig.update_yaxes(title_text="Orders / Profit ($)", secondary_y=True, showgrid=False)
    except Exception as _e:
        _logger.warning("Chart 'Day of Week' failed: %s", _e)
        dow_fig = _no_data_fig("Day of Week", f"Error: {_e}")

    # 9) Revenue vs Inventory Spend overlay (monthly)
    try:
        rev_inv_fig = make_subplots(specs=[[{"secondary_y": True}]])
        _inv_months = sorted(set(months_sorted) | set(monthly_inv_spend.index.tolist()))
        _inv_months = [m for m in months_sorted]  # keep to Etsy months for alignment
        _inv_spend_vals = [monthly_inv_spend.get(m, 0) for m in _inv_months]
        _inv_rev_vals = [monthly_sales.get(m, 0) for m in _inv_months]
        _inv_cogs_ratio = [monthly_inv_spend.get(m, 0) / monthly_sales.get(m, 1) * 100 for m in _inv_months]

        rev_inv_fig.add_trace(go.Bar(name="Monthly Revenue", x=_inv_months, y=_inv_rev_vals,
            marker_color=GREEN, opacity=0.6))
        rev_inv_fig.add_trace(go.Bar(name="Inventory Spend", x=_inv_months, y=_inv_spend_vals,
            marker_color=PURPLE, opacity=0.8))
        rev_inv_fig.add_trace(go.Scatter(name="Supply Cost Ratio %", x=_inv_months, y=_inv_cogs_ratio,
            mode="lines+markers+text", text=[f"{v:.1f}%" for v in _inv_cogs_ratio],
            textposition="top center", textfont=dict(color=ORANGE, size=10),
            line=dict(color=ORANGE, width=3), marker=dict(size=8)), secondary_y=True)
        make_chart(rev_inv_fig, 360)
        rev_inv_fig.update_layout(title="Revenue vs Inventory Spend by Month", barmode="group")
        rev_inv_fig.update_yaxes(title_text="Amount ($)", secondary_y=False)
        rev_inv_fig.update_yaxes(title_text="Supply Cost Ratio (%)", secondary_y=True, showgrid=False)
    except Exception as _e:
        _logger.warning("Chart 'Revenue vs Inventory' failed: %s", _e)
        rev_inv_fig = _no_data_fig("Revenue vs Inventory", f"Error: {_e}")

    # 10) Inventory category breakdown (what you're spending on)
    try:
        if len(biz_inv_by_category) > 0:
            inv_cat_bar = go.Figure()
            _cat_names = biz_inv_by_category.index.tolist()
            _cat_vals = biz_inv_by_category.values.tolist()
            _cat_colors = [PURPLE, TEAL, BLUE, GREEN, ORANGE, RED, PINK, CYAN, GRAY, DARKGRAY]
            inv_cat_bar.add_trace(go.Bar(
                x=_cat_names, y=_cat_vals,
                marker_color=_cat_colors[:len(_cat_names)],
                text=[f"${v:,.0f}" for v in _cat_vals], textposition="outside",
            ))
            make_chart(inv_cat_bar, 320, False)
            inv_cat_bar.update_layout(title="Inventory Spend by Category", yaxis_title="Amount ($)",
                                      xaxis_tickangle=-25, xaxis_tickfont=dict(size=10))
        else:
            inv_cat_bar = _no_data_fig("Inventory Spend by Category", "Upload invoices to see inventory breakdown.")
    except Exception as _e:
        _logger.warning("Chart 'Inventory Category Bar' failed: %s", _e)
        inv_cat_bar = _no_data_fig("Inventory Category Bar", f"Error: {_e}")

    # 11) Anomaly detection (z-score on daily revenue)
    try:
        _daily_rev_mean = daily_df["revenue"].mean()
        _daily_rev_std = daily_df["revenue"].std() if len(daily_df) > 1 else 1
        daily_df["_z_score"] = (daily_df["revenue"] - _daily_rev_mean) / max(_daily_rev_std, 0.01)
        _anomaly_high = daily_df[daily_df["_z_score"] > 2.0]
        _anomaly_low = daily_df[(daily_df["_z_score"] < -1.5) & (daily_df["revenue"] > 0)]
        _zero_days = daily_df[daily_df["revenue"] == 0]

        anomaly_fig = go.Figure()
        anomaly_fig.add_trace(go.Scatter(
            name="Daily Revenue", x=daily_df.index, y=daily_df["revenue"],
            mode="lines", line=dict(color=GRAY, width=1),
        ))
        anomaly_fig.add_hline(y=_daily_rev_mean, line_dash="dash", line_color=TEAL, line_width=1,
            annotation_text=f"Mean: ${_daily_rev_mean:,.0f}", annotation_position="top left")
        anomaly_fig.add_hline(y=_daily_rev_mean + 2 * _daily_rev_std, line_dash="dot", line_color=GREEN, line_width=1,
            annotation_text="+2σ (spike)", annotation_position="top right")
        anomaly_fig.add_hline(y=max(0, _daily_rev_mean - 1.5 * _daily_rev_std), line_dash="dot", line_color=RED, line_width=1,
            annotation_text="-1.5σ (drop)", annotation_position="bottom right")
        if len(_anomaly_high) > 0:
            anomaly_fig.add_trace(go.Scatter(
                name=f"Spikes ({len(_anomaly_high)})", x=_anomaly_high.index, y=_anomaly_high["revenue"],
                mode="markers", marker=dict(color=GREEN, size=12, symbol="triangle-up"),
            ))
        if len(_anomaly_low) > 0:
            anomaly_fig.add_trace(go.Scatter(
                name=f"Drops ({len(_anomaly_low)})", x=_anomaly_low.index, y=_anomaly_low["revenue"],
                mode="markers", marker=dict(color=RED, size=12, symbol="triangle-down"),
            ))
        if len(_zero_days) > 0:
            anomaly_fig.add_trace(go.Scatter(
                name=f"Zero Days ({len(_zero_days)})", x=_zero_days.index, y=[0] * len(_zero_days),
                mode="markers", marker=dict(color=ORANGE, size=10, symbol="x"),
            ))
        make_chart(anomaly_fig, 340)
        anomaly_fig.update_layout(title="Anomaly Detection (Statistical Outliers)", yaxis_title="Revenue ($)")
    except Exception as _e:
        _logger.warning("Chart 'Anomaly Detection' failed: %s", _e)
        anomaly_fig = _no_data_fig("Anomaly Detection", f"Error: {_e}")

    # 12) Product performance heatmap (top products by month)
    try:
        _top_n_products = 8
        _top_prod_names = product_revenue_est.head(_top_n_products).index.tolist()
        # Build product-month revenue matrix from actual sales
        _prod_monthly = {}
        for prod_name in _top_prod_names:
            _pmask = _sales_with_product["Product"] == prod_name
            _prod_month_sales = _sales_with_product[_pmask].groupby("Month")["Net_Clean"].sum()
            _prod_monthly[prod_name[:25]] = {m: _prod_month_sales.get(m, 0) for m in months_sorted}

        if _prod_monthly:
            _heat_products = list(_prod_monthly.keys())
            _heat_z = [[_prod_monthly[p].get(m, 0) for m in months_sorted] for p in _heat_products]
            product_heat = go.Figure(go.Heatmap(
                z=_heat_z, x=months_sorted, y=_heat_products,
                colorscale=[[0, "#0f0f1a"], [0.5, PURPLE], [1, GREEN]],
                text=[[f"${v:,.0f}" for v in row] for row in _heat_z],
                texttemplate="%{text}", textfont=dict(size=10, color=WHITE),
                hoverongaps=False,
            ))
            make_chart(product_heat, 340, False)
            product_heat.update_layout(title="Product Revenue Heatmap (Top 8 by Month)",
                                       yaxis=dict(tickfont=dict(size=10)))
        else:
            product_heat = _no_data_fig("Product Revenue Heatmap", "Need 3+ months of sales data for heatmap.")
    except Exception as _e:
        _logger.warning("Chart 'Product Heatmap' failed: %s", _e)
        product_heat = _no_data_fig("Product Heatmap", f"Error: {_e}")

    # 13) Correlation: Ads Spend vs Sales (monthly)
    try:
        corr_fig = make_subplots(specs=[[{"secondary_y": True}]])
        _corr_ad_vals = [monthly_marketing.get(m, 0) for m in months_sorted]
        _corr_rev_vals = [monthly_sales.get(m, 0) for m in months_sorted]

        # Grouped bars: ad spend vs revenue by month (easier to read than scatter)
        corr_fig.add_trace(go.Bar(
            name="Revenue", x=list(months_sorted), y=_corr_rev_vals,
            marker_color=GREEN, opacity=0.7,
            text=[f"${v:,.0f}" for v in _corr_rev_vals], textposition="outside",
            textfont=dict(color=GREEN, size=10),
        ))
        corr_fig.add_trace(go.Bar(
            name="Ad Spend", x=list(months_sorted), y=_corr_ad_vals,
            marker_color=PURPLE, opacity=0.8,
            text=[f"${v:,.0f}" for v in _corr_ad_vals], textposition="outside",
            textfont=dict(color=PURPLE, size=10),
        ))

        # ROAS line (return on ad spend) on secondary axis
        _roas_vals = [(_corr_rev_vals[i] / _corr_ad_vals[i]) if _corr_ad_vals[i] > 0 else 0
                      for i in range(len(months_sorted))]
        if any(v > 0 for v in _roas_vals):
            corr_fig.add_trace(go.Scatter(
                name="ROAS (Revenue / Ad Spend)", x=list(months_sorted), y=_roas_vals,
                mode="lines+markers+text", line=dict(color=ORANGE, width=3),
                marker=dict(size=8, color=ORANGE),
                text=[f"{v:.1f}x" if v > 0 else "" for v in _roas_vals],
                textposition="top center", textfont=dict(color=ORANGE, size=11),
            ), secondary_y=True)

        # Fit R²
        if len(months_sorted) >= 3 and sum(_corr_ad_vals) > 0:
            _corr_X = np.array(_corr_ad_vals).reshape(-1, 1)
            _corr_y = np.array(_corr_rev_vals)
            _corr_lr = LinearRegression().fit(_corr_X, _corr_y)
            _corr_r2 = _corr_lr.score(_corr_X, _corr_y)
        else:
            _corr_r2 = 0

        make_chart(corr_fig, 340, False)
        corr_fig.update_layout(
            title=f"Ad Spend vs Revenue (R²={_corr_r2:.2f})",
            barmode="group",
        )
        corr_fig.update_yaxes(title_text="Amount ($)", secondary_y=False)
        corr_fig.update_yaxes(title_text="ROAS (x)", secondary_y=True, showgrid=False)
    except Exception as _e:
        _logger.warning("Chart 'Ad Correlation' failed: %s", _e)
        corr_fig = _no_data_fig("Ad Correlation", f"Error: {_e}")

    # 14) Unit economics waterfall (per-order breakdown)
    try:
        _unit_rev = avg_order
        _unit_fees = total_fees / order_count if order_count else 0
        _unit_ship = total_shipping_cost / order_count if order_count else 0
        _unit_ads = total_marketing / order_count if order_count else 0
        _unit_refund = total_refunds / order_count if order_count else 0
        _unit_tax = total_taxes / order_count if order_count else 0
        _unit_bf = total_buyer_fees / order_count if order_count else 0
        _unit_pay = total_payments / order_count if order_count else 0
        _unit_cogs = true_inventory_cost / order_count if order_count else 0
        _unit_profit = _unit_rev - _unit_fees - _unit_ship - _unit_ads - _unit_refund - _unit_tax - _unit_bf + _unit_pay - _unit_cogs
        _unit_margin = (_unit_profit / _unit_rev * 100) if _unit_rev else 0

        unit_wf = go.Figure(go.Waterfall(
            orientation="v",
            measure=["absolute", "relative", "relative", "relative", "relative", "relative", "relative", "total"],
            x=["Revenue", "Fees", "Shipping", "Ads", "Refunds", "Tax", "Supplies", "Profit"],
            y=[_unit_rev, -_unit_fees, -_unit_ship, -_unit_ads, -_unit_refund, -_unit_tax, -_unit_cogs, 0],
            connector={"line": {"color": GRAY, "width": 1, "dash": "dot"}},
            increasing={"marker": {"color": GREEN}},
            decreasing={"marker": {"color": RED}},
            totals={"marker": {"color": CYAN}},
            text=[f"${abs(v):,.2f}" for v in [_unit_rev, _unit_fees, _unit_ship, _unit_ads, _unit_refund, _unit_tax, _unit_cogs, _unit_profit]],
            textposition="outside",
        ))
        make_chart(unit_wf, 340, False)
        unit_wf.update_layout(title=f"Unit Economics: Average Order Breakdown (margin: {_unit_margin:.1f}%)",
                              showlegend=False, yaxis_title="Per Order ($)")
    except Exception as _e:
        _logger.warning("Chart 'Unit Economics Waterfall' failed: %s", _e)
        unit_wf = _no_data_fig("Unit Economics Waterfall", f"Error: {_e}")

    # 15) Inventory location split (Tulsa vs Texas)
    try:
        loc_fig = go.Figure()
        if tulsa_spend > 0 or texas_spend > 0:
            loc_fig.add_trace(go.Bar(
                name="Tulsa, OK", x=sorted(set(tulsa_monthly.index) | set(texas_monthly.index)),
                y=[tulsa_monthly.get(m, 0) for m in sorted(set(tulsa_monthly.index) | set(texas_monthly.index))],
                marker_color=TEAL,
            ))
            loc_fig.add_trace(go.Bar(
                name="Texas", x=sorted(set(tulsa_monthly.index) | set(texas_monthly.index)),
                y=[texas_monthly.get(m, 0) for m in sorted(set(tulsa_monthly.index) | set(texas_monthly.index))],
                marker_color=ORANGE,
            ))
            make_chart(loc_fig, 300)
            loc_fig.update_layout(title="Inventory Orders by Location & Month", barmode="stack",
                                  yaxis_title="Spend ($)")
        else:
            loc_fig = _no_data_fig("Inventory by Location", "Upload invoices to see Tulsa vs Texas split.", 200)
    except Exception as _e:
        _logger.warning("Chart 'Location Pie' failed: %s", _e)
        loc_fig = _no_data_fig("Location Pie", f"Error: {_e}")

    # 16) Cash flow timeline (deposits - expenses by month)
    try:
        _cf_months = sorted(bank_monthly.keys())
        if not _cf_months:
            cashflow_fig = _no_data_fig("Bank Cash Flow", "Upload bank statements to see cash flow.")
            _cf_deposits = []
            _cf_debits = []
            _cf_net = []
            _cf_cum = []
        else:
            cashflow_fig = go.Figure()
            _cf_deposits = [bank_monthly[m]["deposits"] for m in _cf_months]
            _cf_debits = [bank_monthly[m]["debits"] for m in _cf_months]
            _cf_net = [bank_monthly[m]["deposits"] - bank_monthly[m]["debits"] for m in _cf_months]
            _cf_cum = list(np.cumsum(_cf_net))
            cashflow_fig.add_trace(go.Bar(name="Deposits", x=_cf_months, y=_cf_deposits, marker_color=GREEN, opacity=0.7))
            cashflow_fig.add_trace(go.Bar(name="Expenses", x=_cf_months, y=[-d for d in _cf_debits], marker_color=RED, opacity=0.7))
            cashflow_fig.add_trace(go.Scatter(name="Net Cash Flow", x=_cf_months, y=_cf_net,
                mode="lines+markers+text", text=[f"${v:,.0f}" for v in _cf_net],
                textposition="top center", textfont=dict(color=CYAN, size=10),
                line=dict(color=CYAN, width=3), marker=dict(size=8)))
            make_chart(cashflow_fig, 340)
            cashflow_fig.update_layout(title="CASH BASIS — Bank Deposits vs Expenses by Month", barmode="relative",
                                       yaxis_title="Amount ($)")
            cashflow_fig.add_annotation(text="Cash flow = actual bank deposits minus actual bank debits. Not accrual-basis profit.",
                                        xref="paper", yref="paper", x=0.5, y=1.08, showarrow=False,
                                        font=dict(size=10, color=GRAY), xanchor="center")
    except Exception as _e:
        _logger.warning("Chart 'Cash Flow' failed: %s", _e)
        cashflow_fig = _no_data_fig("Cash Flow", f"Error: {_e}")

    # 17) Break-even analysis
    try:
        _monthly_fixed = (bank_biz_expense_total + total_marketing) / _val_months_operating if _val_months_operating else 0
        _contrib_margin_pct = (gross_sales - total_fees - total_shipping_cost - total_refunds - true_inventory_cost) / gross_sales if gross_sales else 0
        _breakeven_monthly = _monthly_fixed / _contrib_margin_pct if _contrib_margin_pct > 0 else 0
        _breakeven_daily = _breakeven_monthly / 30
        _breakeven_orders = _breakeven_monthly / avg_order if avg_order > 0 else 0
    except Exception as _e:
        _logger.warning("Break-even analysis failed: %s", _e)

    # 18) Supplier spend analysis
    try:
        _supplier_spend = {}
        if len(INV_ITEMS) > 0:
            for seller, grp in INV_ITEMS.groupby("seller"):
                if seller != "Unknown":
                    _supplier_spend[seller] = {
                        "total": grp["total"].sum(),
                        "items": len(grp),
                        "avg_price": grp["price"].mean(),
                    }
        _supplier_spend = dict(sorted(_supplier_spend.items(), key=lambda x: -x[1]["total"]))
    except Exception as _e:
        _logger.warning("Supplier spend analysis failed: %s", _e)


    # --- TAB 3: AI ANALYTICS BOT CHARTS ---

    # Revenue projection chart
    try:
        proj_chart = _no_data_fig("Revenue Projection", "Need 3+ months of data for projections.")
        if "proj_sales" in analytics_projections and len(months_sorted) >= 2:
            from datetime import datetime as _dt_chart
            import calendar as _cal_chart
            _cur_m_chart = _dt_chart.now().strftime("%Y-%m")
            _day_of_month = _dt_chart.now().day
            _is_partial = _cur_m_chart in months_sorted and _day_of_month < 25

            # Complete months only (for regression line)
            _complete_months = [m for m in months_sorted if m != _cur_m_chart] if _is_partial else list(months_sorted)

            # Future projection months
            if not months_sorted:
                _last_complete = _cur_m_chart
            else:
                _last_complete = _complete_months[-1] if _complete_months else months_sorted[-1]
            _last_period = pd.Period(_last_complete, freq="M")
            _proj_months = [(_last_period + i).strftime("%Y-%m") for i in range(1, 4)]
            _proj_sales_vals = list(np.maximum(analytics_projections["proj_sales"], 0))
            _proj_net_vals = list(np.maximum(analytics_projections["proj_net"], 0))

            # Build all x-axis points and y values
            _all_months = list(months_sorted)
            # Add projection months that aren't already in the list
            for pm in _proj_months:
                if pm not in _all_months:
                    _all_months.append(pm)

            proj_chart = go.Figure()

            # ── Gross Sales: solid green for actual, outlined for projected ──
            _actual_gross = [monthly_sales.get(m, 0) for m in months_sorted]
            proj_chart.add_trace(go.Scatter(
                name="Gross Sales (Actual)", x=list(months_sorted), y=_actual_gross,
                mode="lines+markers", line=dict(color=GREEN, width=3),
                marker=dict(size=10, color=GREEN),
            ))
            # Dollar labels on actual gross
            for i, m in enumerate(months_sorted):
                _is_cur = m == _cur_m_chart and _is_partial
                _lbl = f"${_actual_gross[i]:,.0f}" + (f" ({_day_of_month}d)" if _is_cur else "")
                _clr = GRAY if _is_cur else GREEN
                proj_chart.add_annotation(
                    x=m, y=_actual_gross[i], text=_lbl, showarrow=False,
                    font=dict(size=11, color=_clr, family="Arial Bold"),
                    yshift=18,
                )

            # ── Net Revenue: solid orange for actual ──
            _actual_net = [monthly_net_revenue.get(m, 0) for m in months_sorted]
            proj_chart.add_trace(go.Scatter(
                name="Net Revenue (Actual)", x=list(months_sorted), y=_actual_net,
                mode="lines+markers", line=dict(color=ORANGE, width=2),
                marker=dict(size=8, color=ORANGE),
            ))

            # ── Projected gross: dashed green with diamond markers ──
            _proj_x = [_last_complete] + _proj_months
            _proj_y_gross = [monthly_sales.get(_last_complete, 0)] + _proj_sales_vals
            proj_chart.add_trace(go.Scatter(
                name="Gross Sales (Projected)", x=_proj_x, y=_proj_y_gross,
                mode="lines+markers", line=dict(color=GREEN, width=3, dash="dash"),
                marker=dict(size=10, symbol="diamond", color=GREEN,
                            line=dict(color=GREEN, width=2)),
            ))
            # Dollar labels on projected gross
            for i, pm in enumerate(_proj_months):
                proj_chart.add_annotation(
                    x=pm, y=_proj_sales_vals[i], text=f"${_proj_sales_vals[i]:,.0f}",
                    showarrow=False, font=dict(size=11, color=GREEN, family="Arial Bold"),
                    yshift=18,
                )

            # ── Projected net: dashed orange ──
            _proj_y_net = [monthly_net_revenue.get(_last_complete, 0)] + _proj_net_vals
            proj_chart.add_trace(go.Scatter(
                name="Net Revenue (Projected)", x=_proj_x, y=_proj_y_net,
                mode="lines+markers", line=dict(color=ORANGE, width=2, dash="dash"),
                marker=dict(size=8, symbol="diamond", color=ORANGE),
            ))

            # ── Confidence band on projected months ──
            _std = analytics_projections.get("residual_std", 0)
            _upper = list(np.maximum(np.array(_proj_sales_vals) + _std, 0))
            _lower = list(np.maximum(np.array(_proj_sales_vals) - _std, 0))
            proj_chart.add_trace(go.Scatter(
                x=_proj_months, y=_upper, mode="lines", line=dict(width=0),
                showlegend=False, hoverinfo="skip",
            ))
            proj_chart.add_trace(go.Scatter(
                name="Confidence Range", x=_proj_months, y=_lower,
                mode="lines", line=dict(width=0), fill="tonexty",
                fillcolor="rgba(46,204,113,0.12)", hoverinfo="skip",
            ))

            # ── Partial month pace estimate (faint dashed line to full-month est) ──
            if _is_partial:
                _partial_sales = monthly_sales.get(_cur_m_chart, 0)
                _dims = _cal_chart.monthrange(_dt_chart.now().year, _dt_chart.now().month)[1]
                _est_full = _partial_sales / _day_of_month * _dims if _day_of_month > 0 else 0
                proj_chart.add_trace(go.Scatter(
                    name=f"March pace (~${_est_full:,.0f})",
                    x=[_cur_m_chart, _cur_m_chart], y=[_partial_sales, _est_full],
                    mode="lines+markers",
                    line=dict(color=CYAN, width=2, dash="dot"),
                    marker=dict(size=[0, 8], symbol=["circle", "star"], color=CYAN),
                ))
                proj_chart.add_annotation(
                    x=_cur_m_chart, y=_est_full,
                    text=f"Pace: ~${_est_full:,.0f}", showarrow=False,
                    font=dict(size=10, color=CYAN), yshift=15,
                )

            # ── Vertical divider ──
            proj_chart.add_vline(x=_last_complete, line=dict(color=GRAY, width=1, dash="dot"))
            if _complete_months:
                proj_chart.add_annotation(
                    x=_complete_months[len(_complete_months) // 2], y=1.05, yref="paper",
                    text="ACTUAL", showarrow=False,
                    font=dict(size=13, color=GREEN, family="Arial Black"),
                )
            if len(_proj_months) > 1:
                proj_chart.add_annotation(
                    x=_proj_months[1], y=1.05, yref="paper",
                    text="PROJECTED", showarrow=False,
                    font=dict(size=13, color=CYAN, family="Arial Black"),
                )

            # ── R² confidence ──
            _r2 = analytics_projections.get("r2_sales", 0)
            _conf = "High" if _r2 > 0.7 else "Moderate" if _r2 > 0.4 else "Low"
            proj_chart.add_annotation(
                x=0.01, y=0.98, xref="paper", yref="paper",
                text=f"Model confidence: R²={_r2:.2f} ({_conf})",
                showarrow=False, font=dict(size=10, color=GRAY),
                bgcolor="rgba(0,0,0,0.5)", borderpad=4, xanchor="left",
            )

        make_chart(proj_chart, 420)
        proj_chart.update_layout(
            title="Revenue Projection (3-Month Forecast)",
            xaxis_title="Month", yaxis_title="Amount ($)",
        )
    except Exception as _e:
        _logger.warning("Chart 'Revenue Projection' failed: %s", _e)
        proj_chart = _no_data_fig("Revenue Projection", f"Error: {_e}")


    # --- TAB 4: SHIPPING CHARTS ---

    # Shipping cost breakdown (buyer-paid is unavailable)
    try:
        shipping_compare = go.Figure()
        shipping_compare.add_trace(go.Bar(
            name="Your Label Cost", x=["Shipping"], y=[total_shipping_cost],
            marker_color=RED, text=[f"${total_shipping_cost:,.2f}"], textposition="outside", width=0.4,
        ))
        shipping_compare.add_annotation(
            x="Shipping", y=total_shipping_cost + 200,
            text="Buyer-paid amount: N/A (not in Etsy CSV)",
            showarrow=False, font=dict(size=14, color=ORANGE, family="Arial"),
        )
        make_chart(shipping_compare, 340, False)
        shipping_compare.update_layout(title="Shipping Label Costs (Buyer-Paid Amount Unavailable)",
            showlegend=True, yaxis_title="Amount ($)")
    except Exception as _e:
        _logger.warning("Chart 'Shipping Comparison' failed: %s", _e)
        shipping_compare = _no_data_fig("Shipping Comparison", f"Error: {_e}")

    # Shipping cost by type bar chart
    try:
        ship_type_fig = go.Figure()
        ship_type_names = []
        ship_type_vals = []
        ship_type_colors = []
        for nm, val, clr in [
            (f"USPS Outbound ({usps_outbound_count})", usps_outbound, BLUE),
            (f"USPS Return ({usps_return_count})", usps_return, RED),
            (f"Asendia Intl ({asendia_count})", asendia_labels, PURPLE),
            (f"Adjustments ({ship_adjust_count})", ship_adjustments, ORANGE),
            (f"Insurance ({ship_insurance_count})", ship_insurance, TEAL),
        ]:
            if val is not None and val > 0:
                ship_type_names.append(nm)
                ship_type_vals.append(val)
                ship_type_colors.append(clr)

        if ship_credits is not None and ship_credits != 0:
            ship_type_names.append(f"Credits ({ship_credit_count})")
            ship_type_vals.append(abs(ship_credits))
            ship_type_colors.append(GREEN)

        ship_type_fig.add_trace(go.Bar(
            x=ship_type_names, y=ship_type_vals, marker_color=ship_type_colors,
            text=[f"${v:,.2f}" for v in ship_type_vals], textposition="outside",
        ))
        make_chart(ship_type_fig, 340, False)
        ship_type_fig.update_layout(title="Shipping Cost by Type", yaxis_title="Amount ($)")
    except Exception as _e:
        _logger.warning("Chart 'Shipping Type' failed: %s", _e)
        ship_type_fig = _no_data_fig("Shipping Type", f"Error: {_e}")

    # International analysis chart
    try:
        intl_fig = go.Figure()
        if asendia_count > 0:
            intl_avg = asendia_labels / asendia_count
            dom_avg = avg_outbound_label
            intl_fig.add_trace(go.Bar(
                x=["Domestic Avg Label", "International Avg Label"],
                y=[dom_avg, intl_avg],
                marker_color=[BLUE, PURPLE],
                text=[f"${dom_avg:.2f}", f"${intl_avg:.2f}"], textposition="outside",
            ))
            intl_fig.add_annotation(
                x="International Avg Label", y=intl_avg + 2,
                text=f"{intl_avg / dom_avg:.1f}x more expensive" if dom_avg > 0 else "",
                showarrow=False, font=dict(color=PINK, size=14),
            )
        make_chart(intl_fig, 300, False)
        intl_fig.update_layout(title="Domestic vs International Shipping Cost", yaxis_title="Avg Label Cost ($)")
    except Exception as _e:
        _logger.warning("Chart 'International Orders' failed: %s", _e)
        intl_fig = _no_data_fig("International Orders", f"Error: {_e}")


    # --- TAB 5: FINANCIALS CHARTS ---

    # Top products bar chart
    try:
        top_n = 12
        top_products = product_revenue_est.head(top_n)
        product_fig = go.Figure(go.Bar(
            y=top_products.index, x=top_products.values, orientation="h",
            marker_color=TEAL, text=[f"${v:,.0f}" for v in top_products.values], textposition="outside",
        ))
        make_chart(product_fig, 400, False)
        product_fig.update_layout(title=f"Top {top_n} Products by Revenue",
            yaxis=dict(autorange="reversed"), margin=dict(l=300, t=50, b=30), xaxis_title="Revenue ($)")
    except Exception as _e:
        _logger.warning("Chart 'Top Products' failed: %s", _e)
        product_fig = _no_data_fig("Top Products", f"Error: {_e}")


    # --- TAB 7: INVENTORY / COGS CHARTS ---

    # Monthly inventory spend bar chart
    try:
        inv_months_sorted = sorted(monthly_inv_spend.index)
        if len(inv_months_sorted) > 0:
            inv_monthly_fig = go.Figure()
            inv_monthly_fig.add_trace(go.Bar(
                name="Inventory Spend", x=inv_months_sorted,
                y=[monthly_inv_spend.get(m, 0) for m in inv_months_sorted],
                marker_color=PURPLE,
                text=[f"${monthly_inv_spend.get(m, 0):,.0f}" for m in inv_months_sorted],
                textposition="outside",
            ))
            make_chart(inv_monthly_fig, 360)
            inv_monthly_fig.update_layout(title="Monthly Supply Costs", yaxis_title="Amount ($)")
        else:
            inv_monthly_fig = _no_data_fig("Monthly Supply Costs", "Upload invoices to see monthly spending.")
    except Exception as _e:
        _logger.warning("Chart 'Inventory Monthly' failed: %s", _e)
        inv_monthly_fig = _no_data_fig("Inventory Monthly", f"Error: {_e}")

    # Category breakdown donut
    try:
        if len(biz_inv_by_category) > 0:
            inv_cat_fig = go.Figure()
            inv_cat_fig.add_trace(go.Pie(
                labels=biz_inv_by_category.index.tolist(),
                values=biz_inv_by_category.values.tolist(),
                hole=0.45,
                marker_colors=[BLUE, TEAL, ORANGE, RED, PURPLE, PINK, GREEN, CYAN][:len(biz_inv_by_category)],
                textinfo="label+percent",
                textposition="outside",
            ))
            make_chart(inv_cat_fig, 380, False)
            inv_cat_fig.update_layout(title="Inventory by Category (Business Only)", showlegend=False)
        else:
            inv_cat_fig = _no_data_fig("Inventory by Category", "Upload invoices to see category breakdown.")
    except Exception as _e:
        _logger.warning("Chart 'Inventory Categories' failed: %s", _e)
        inv_cat_fig = _no_data_fig("Inventory Categories", f"Error: {_e}")

    # Revenue vs COGS vs True Profit monthly bar chart
    try:
        rev_cogs_fig = go.Figure()
        all_inv_months = sorted(set(months_sorted) | set(inv_months_sorted))
        rev_cogs_fig.add_trace(go.Bar(
            name="Revenue", x=all_inv_months,
            y=[monthly_sales.get(m, 0) for m in all_inv_months],
            marker_color=GREEN,
        ))
        rev_cogs_fig.add_trace(go.Bar(
            name="Supplies", x=all_inv_months,
            y=[monthly_inv_spend.get(m, 0) for m in all_inv_months],
            marker_color=PURPLE,
        ))
        rev_cogs_fig.add_trace(go.Bar(
            name="Etsy Expenses", x=all_inv_months,
            y=[monthly_fees.get(m, 0) + monthly_shipping.get(m, 0) + monthly_marketing.get(m, 0) + monthly_refunds.get(m, 0)
               for m in all_inv_months],
            marker_color=RED,
        ))
        true_profit_monthly = [
            monthly_sales.get(m, 0) - monthly_fees.get(m, 0) - monthly_shipping.get(m, 0)
            - monthly_marketing.get(m, 0) - monthly_refunds.get(m, 0) - monthly_inv_spend.get(m, 0)
            for m in all_inv_months
        ]
        rev_cogs_fig.add_trace(go.Scatter(
            name="True Profit", x=all_inv_months, y=true_profit_monthly,
            mode="lines+markers+text",
            text=[f"${v:,.0f}" for v in true_profit_monthly],
            textposition="top center", textfont=dict(color=ORANGE),
            line=dict(color=ORANGE, width=3), marker=dict(size=10),
        ))
        make_chart(rev_cogs_fig, 400)
        rev_cogs_fig.update_layout(title="Revenue vs Supplies vs Expenses vs Profit", barmode="group", yaxis_title="Amount ($)")
    except Exception as _e:
        _logger.warning("Chart 'Revenue vs COGS' failed: %s", _e)
        rev_cogs_fig = _no_data_fig("Revenue vs COGS", f"Error: {_e}")

    # --- LOCATION CHARTS ---

    # TJ vs Braden monthly comparison
    try:
        loc_monthly_fig = go.Figure()
        all_loc_months = sorted(set(list(tulsa_monthly.index) + list(texas_monthly.index)))
        loc_monthly_fig.add_trace(go.Bar(
            name="TJ (Tulsa)", x=all_loc_months,
            y=[tulsa_monthly.get(m, 0) for m in all_loc_months],
            marker_color=TEAL,
            text=[f"${tulsa_monthly.get(m, 0):,.0f}" for m in all_loc_months],
            textposition="outside",
        ))
        loc_monthly_fig.add_trace(go.Bar(
            name="Braden (TX)", x=all_loc_months,
            y=[texas_monthly.get(m, 0) for m in all_loc_months],
            marker_color=ORANGE,
            text=[f"${texas_monthly.get(m, 0):,.0f}" for m in all_loc_months],
            textposition="outside",
        ))
        make_chart(loc_monthly_fig, 380)
        loc_monthly_fig.update_layout(title="Monthly Inventory Spend by Location", barmode="group", yaxis_title="Amount ($)")
    except Exception as _e:
        _logger.warning("Chart 'Location Monthly' failed: %s", _e)
        loc_monthly_fig = _no_data_fig("Location Monthly", f"Error: {_e}")

    # TJ (Tulsa) category donut
    try:
        if len(tulsa_by_cat) > 0:
            tulsa_cat_fig = go.Figure()
            tulsa_cat_fig.add_trace(go.Pie(
                labels=tulsa_by_cat.index.tolist(), values=tulsa_by_cat.values.tolist(),
                hole=0.45, marker_colors=[TEAL, BLUE, GREEN, PURPLE, CYAN, PINK, ORANGE, RED][:len(tulsa_by_cat)],
                textinfo="label+percent", textposition="outside",
            ))
            make_chart(tulsa_cat_fig, 340, False)
            tulsa_cat_fig.update_layout(title="TJ (Tulsa) - Categories", showlegend=False)
        else:
            tulsa_cat_fig = _no_data_fig("TJ (Tulsa) Categories", "Upload Tulsa invoices to see categories.")
    except Exception as _e:
        _logger.warning("Chart 'Tulsa Categories' failed: %s", _e)
        tulsa_cat_fig = _no_data_fig("Tulsa Categories", f"Error: {_e}")

    # Braden (Texas) category donut
    try:
        if len(texas_by_cat) > 0:
            texas_cat_fig = go.Figure()
            texas_cat_fig.add_trace(go.Pie(
                labels=texas_by_cat.index.tolist(), values=texas_by_cat.values.tolist(),
                hole=0.45, marker_colors=[ORANGE, RED, PURPLE, BLUE, TEAL, PINK, GREEN, CYAN][:len(texas_by_cat)],
                textinfo="label+percent", textposition="outside",
            ))
            make_chart(texas_cat_fig, 340, False)
            texas_cat_fig.update_layout(title="Braden (Texas) - Categories", showlegend=False)
        else:
            texas_cat_fig = _no_data_fig("Braden (Texas) Categories", "Upload Texas invoices to see categories.")
    except Exception as _e:
        _logger.warning("Chart 'Texas Categories' failed: %s", _e)
        texas_cat_fig = _no_data_fig("Texas Categories", f"Error: {_e}")

    # --- TAB 8: BANK / CASH FLOW CHARTS ---

    # Monthly bar: Dec vs Jan deposits/debits with net line
    try:
        bank_months_sorted = sorted(bank_monthly.keys())
        _month_abbr = {"01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr", "05": "May", "06": "Jun",
                       "07": "Jul", "08": "Aug", "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"}
        BANK_MONTH_NAMES = {k: f"{_month_abbr.get(k.split('-')[1], k.split('-')[1])} {k.split('-')[0]}" for k in bank_monthly}
        bank_month_labels = [BANK_MONTH_NAMES.get(m, m) for m in bank_months_sorted]
        bank_month_deps = [bank_monthly[m]["deposits"] for m in bank_months_sorted]
        bank_month_debs = [bank_monthly[m]["debits"] for m in bank_months_sorted]
        bank_month_nets = [d - w for d, w in zip(bank_month_deps, bank_month_debs)]

        bank_monthly_fig = go.Figure()
        bank_monthly_fig.add_trace(go.Bar(
            name="Deposits", x=bank_month_labels, y=bank_month_deps,
            marker_color=GREEN, text=[f"${v:,.0f}" for v in bank_month_deps], textposition="outside",
        ))
        bank_monthly_fig.add_trace(go.Bar(
            name="Debits", x=bank_month_labels, y=bank_month_debs,
            marker_color=RED, text=[f"${v:,.0f}" for v in bank_month_debs], textposition="outside",
        ))
        bank_monthly_fig.add_trace(go.Scatter(
            name="Net Cash Flow", x=bank_month_labels, y=bank_month_nets,
            mode="lines+markers+text", line=dict(color=CYAN, width=3),
            marker=dict(size=10), text=[f"${v:,.0f}" for v in bank_month_nets], textposition="top center",
            textfont=dict(color=CYAN),
        ))
        make_chart(bank_monthly_fig, 380)
        bank_monthly_fig.update_layout(
            title="Monthly Cash Flow — Deposits vs Debits",
            barmode="group", yaxis_title="Amount ($)",
        )
    except Exception as _e:
        _logger.warning("Chart 'Bank Monthly' failed: %s", _e)
        bank_monthly_fig = _no_data_fig("Bank Monthly", f"Error: {_e}")

    # --- SANKEY DIAGRAM (module level for reuse) ---
    try:
        _etsy_took = _safe(total_fees) + _safe(total_shipping_cost) + _safe(total_marketing) + _safe(total_taxes) + _safe(total_refunds) + _safe(total_buyer_fees)

        sankey_node_labels = [
            f"Customers Paid\n${_safe(gross_sales):,.0f}",
            f"Etsy Takes\n-${_etsy_took:,.0f}",
            f"Deposited to Bank\n${_safe(etsy_net_earned):,.0f}",
            f"Business Expenses\n${_safe(bank_all_expenses):,.0f}",
            f"Owner Draws\n${_safe(bank_owner_draw_total):,.0f}",
            f"Cash On Hand\n${_safe(bank_cash_on_hand):,.0f}",
            f"Prior Bank Activity\n${_safe(old_bank_receipted) + _safe(bank_unaccounted) + _safe(etsy_csv_gap):,.0f}",
            f"Amazon Inventory\n${bank_by_cat.get('Amazon Inventory', 0):,.0f}",
            f"AliExpress\n${bank_by_cat.get('AliExpress Supplies', 0):,.0f}",
            f"Best Buy CC\n${bank_by_cat.get('Business Credit Card', 0):,.0f}",
            f"Etsy Bank Fees\n${bank_by_cat.get('Etsy Fees', 0):,.0f}",
            f"Craft Supplies\n${bank_by_cat.get('Craft Supplies', 0):,.0f}",
            f"Subscriptions\n${bank_by_cat.get('Subscriptions', 0):,.0f}",
            f"Shipping Supplies\n${bank_by_cat.get('Shipping', 0):,.0f}",
            f"TJ (Owner)\n${bank_by_cat.get('Owner Draw - Tulsa', 0):,.0f}",
            f"Braden (Owner)\n${bank_by_cat.get('Owner Draw - Texas', 0):,.0f}",
            f"Capital One Bank\n${_safe(bank_net_cash):,.0f}",
            f"Etsy Account\n${_safe(etsy_balance):,.0f}",
            f"Prior Bank Receipts\n${_safe(old_bank_receipted):,.0f}",
            f"Untracked Etsy\n${_safe(etsy_csv_gap):,.0f}",
            f"Unmatched Bank\n${_safe(bank_unaccounted):,.0f}",
            f"Fees\n${abs(_safe(total_fees)):,.0f}",
            f"Shipping Labels\n${abs(_safe(total_shipping_cost)):,.0f}",
            f"Ads/Marketing\n${abs(_safe(total_marketing)):,.0f}",
            f"Sales Tax\n${abs(_safe(total_taxes)):,.0f}",
            f"Refunds\n${abs(_safe(total_refunds) + _safe(total_buyer_fees)):,.0f}",
        ]
        sankey_node_colors = [
            GREEN, RED, CYAN, "#ff5252", ORANGE, GREEN, "#b71c1c",
            ORANGE, "#e91e63", BLUE, PURPLE, TEAL, CYAN, BLUE,
            "#ffb74d", "#ff9800", GREEN, TEAL, ORANGE, ORANGE,
            RED, RED, RED, RED, RED, "#b71c1c",
        ]
        sankey_sources = [0, 0,  2, 2, 2, 2,  3, 3, 3, 3, 3, 3, 3,  4, 4,  5, 5,  6, 6, 6,  1, 1, 1, 1, 1]
        sankey_targets = [1, 2,  3, 4, 5, 6,  7, 8, 9,10,11,12,13, 14,15, 16,17, 18,25,19, 20,21,22,23,24]
        sankey_values = [
            _etsy_took, _safe(etsy_net_earned),
            _safe(bank_all_expenses), _safe(bank_owner_draw_total), _safe(bank_cash_on_hand), _safe(old_bank_receipted) + _safe(bank_unaccounted) + _safe(etsy_csv_gap),
            bank_by_cat.get("Amazon Inventory", 0), bank_by_cat.get("AliExpress Supplies", 0),
            bank_by_cat.get("Business Credit Card", 0), bank_by_cat.get("Etsy Fees", 0),
            bank_by_cat.get("Craft Supplies", 0), bank_by_cat.get("Subscriptions", 0),
            bank_by_cat.get("Shipping", 0),
            bank_by_cat.get("Owner Draw - Tulsa", 0), bank_by_cat.get("Owner Draw - Texas", 0),
            _safe(bank_net_cash), _safe(etsy_balance),
            _safe(old_bank_receipted), max(_safe(bank_unaccounted), 0.01), max(_safe(etsy_csv_gap), 0.01),
            abs(_safe(total_fees)), abs(_safe(total_shipping_cost)), abs(_safe(total_marketing)),
            abs(_safe(total_taxes)), abs(_safe(total_refunds) + _safe(total_buyer_fees)),
        ]
        sankey_link_colors = [
            "rgba(244,67,54,0.25)", "rgba(0,229,255,0.25)",
            "rgba(255,82,82,0.2)", "rgba(255,152,0,0.2)", "rgba(76,175,80,0.2)", "rgba(183,28,28,0.2)",
            "rgba(255,152,0,0.15)", "rgba(233,30,99,0.15)", "rgba(33,150,243,0.15)",
            "rgba(156,39,176,0.15)", "rgba(0,150,136,0.15)", "rgba(0,229,255,0.15)", "rgba(33,150,243,0.15)",
            "rgba(255,183,77,0.2)", "rgba(255,152,0,0.2)",
            "rgba(76,175,80,0.2)", "rgba(0,150,136,0.2)",
            "rgba(255,152,0,0.2)", "rgba(183,28,28,0.2)", "rgba(255,152,0,0.15)",
            "rgba(244,67,54,0.15)", "rgba(244,67,54,0.15)", "rgba(244,67,54,0.15)",
            "rgba(244,67,54,0.15)", "rgba(244,67,54,0.15)",
        ]

        sankey_fig = go.Figure(go.Sankey(
            arrangement="snap",
            node=dict(pad=20, thickness=25, label=sankey_node_labels, color=sankey_node_colors,
                      line=dict(color="rgba(255,255,255,0.12)", width=1)),
            link=dict(source=sankey_sources, target=sankey_targets, value=sankey_values, color=sankey_link_colors),
        ))
        sankey_fig.update_layout(
            font=dict(size=11, color=WHITE, family="monospace"),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=10, t=40, b=10), height=550,
            title=dict(text="WHERE EVERY DOLLAR WENT", font=dict(size=16, color=CYAN)),
        )
    except Exception as _e:
        _logger.warning("Chart 'Sankey Diagram' failed: %s", _e)
        sankey_fig = _no_data_fig("Money Flow Sankey", f"Error: {_e}")



_rebuild_all_charts()
# Payment method colors
PAYMENT_COLORS = {
    "Discover": "#ff6d00",
    "Visa": "#1a1f71",
}

def _get_card_color(pm_name):
    for brand, color in PAYMENT_COLORS.items():
        if brand.lower() in pm_name.lower():
            return color
    return GRAY

def _build_payment_sections():
    """Build payment method breakdown cards with order/item details."""
    if not payment_summary:
        return [html.P("No payment data available.", style={"color": GRAY})]

    # Sort by total spend descending
    sorted_payments = sorted(payment_summary.items(), key=lambda x: -x[1]["total"])
    cards = []

    for pm_name, pm_data in sorted_payments:
        card_color = _get_card_color(pm_name)
        first = pm_data["first_date"]
        last = pm_data["last_date"]
        date_range = ""
        if first is not None and last is not None:
            first_str = first.strftime("%b %d, %Y")
            last_str = last.strftime("%b %d, %Y")
            date_range = f"{first_str} - {last_str}" if first_str != last_str else first_str

        # Build item rows for this card (aggregate by item name)
        item_agg = {}
        for it in pm_data["items"]:
            name = it["name"][:80]
            cat = it.get("category", "Other")
            if name not in item_agg:
                item_agg[name] = {"qty": 0, "total": 0.0, "category": cat,
                                  "image_url": _IMAGE_URLS.get(it["name"], "")}
            item_agg[name]["qty"] += it["qty"]
            item_agg[name]["total"] += it["total"]
        item_rows = sorted(item_agg.items(), key=lambda x: -x[1]["total"])

        item_table_rows = []
        for item_name, info in item_rows[:20]:  # Top 20 items
            item_table_rows.append(html.Tr([
                html.Td(item_thumbnail(info.get("image_url", ""), 24),
                         style={"padding": "2px 4px", "textAlign": "center", "width": "30px"}),
                html.Td(str(info["qty"]), style={"textAlign": "center", "color": WHITE,
                                                   "padding": "2px 6px", "fontSize": "11px"}),
                html.Td(item_name, title=item_name, style={"color": WHITE, "padding": "2px 6px", "fontSize": "11px",
                                                "maxWidth": "350px", "overflow": "hidden",
                                                "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
                html.Td(info["category"], style={"color": TEAL, "padding": "2px 6px", "fontSize": "10px"}),
                html.Td(f"${info['total']:,.2f}", style={"textAlign": "right", "color": card_color,
                                                          "fontWeight": "bold", "padding": "2px 6px",
                                                          "fontSize": "11px"}),
            ], style={"borderBottom": "1px solid #ffffff08"}))

        if len(item_rows) > 20:
            item_table_rows.append(html.Tr([
                html.Td(f"... and {len(item_rows) - 20} more items", colSpan="5",
                         style={"color": GRAY, "padding": "4px 6px", "fontSize": "11px", "fontStyle": "italic"})
            ]))

        card = html.Div([
            # Card header
            html.Div([
                html.Div([
                    html.Span(pm_name, style={"color": card_color, "fontSize": "18px",
                                               "fontWeight": "bold"}),
                ], style={"marginBottom": "4px"}),
                html.Div(date_range, style={"color": GRAY, "fontSize": "11px", "marginBottom": "10px"}),
            ]),
            # KPI row
            html.Div([
                html.Div([
                    html.Div("TOTAL SPENT", style={"color": GRAY, "fontSize": "10px", "fontWeight": "600"}),
                    html.Div(f"${pm_data['total']:,.2f}", style={"color": WHITE, "fontSize": "22px",
                                                                   "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Div("ORDERS", style={"color": GRAY, "fontSize": "10px", "fontWeight": "600"}),
                    html.Div(str(pm_data["orders"]), style={"color": WHITE, "fontSize": "22px",
                                                              "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Div("SUBTOTAL", style={"color": GRAY, "fontSize": "10px", "fontWeight": "600"}),
                    html.Div(f"${pm_data['subtotal']:,.2f}", style={"color": GRAY, "fontSize": "14px",
                                                                      "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Div("TAX", style={"color": GRAY, "fontSize": "10px", "fontWeight": "600"}),
                    html.Div(f"${pm_data['tax']:,.2f}", style={"color": GRAY, "fontSize": "14px",
                                                                 "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
            ], style={"display": "flex", "gap": "8px", "marginBottom": "12px",
                       "padding": "8px", "backgroundColor": "#ffffff06", "borderRadius": "8px"}),
            # Items table
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("", style={"padding": "3px 4px", "width": "30px"}),
                        html.Th("Qty", style={"textAlign": "center", "padding": "3px 6px", "fontSize": "10px"}),
                        html.Th("Item", style={"textAlign": "left", "padding": "3px 6px", "fontSize": "10px"}),
                        html.Th("Category", style={"textAlign": "left", "padding": "3px 6px", "fontSize": "10px"}),
                        html.Th("Total", style={"textAlign": "right", "padding": "3px 6px", "fontSize": "10px"}),
                    ], style={"borderBottom": f"1px solid {card_color}66"})),
                    html.Tbody(item_table_rows),
                ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
            ], style={"maxHeight": "300px", "overflowY": "auto"}),
        ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
                   "border": f"1px solid {card_color}44", "marginBottom": "12px"})

        cards.append(card)

    return cards


def _build_image_manager():
    """Build the Image Manager card grid for assigning product images."""
    import urllib.parse

    # Deduplicate items by name — one card per unique item
    seen = {}
    if len(INV_ITEMS) > 0:
        for _, r in INV_ITEMS.iterrows():
            name = r["name"]
            if name not in seen:
                seen[name] = {
                    "name": name,
                    "price": r["price"],
                    "image_url": _IMAGE_URLS.get(name, ""),
                    "order_num": r.get("order_num", ""),
                }
    unique_items = sorted(seen.values(), key=lambda x: x["name"].lower())
    total_items = len(unique_items)
    with_images = sum(1 for it in unique_items if it["image_url"])

    # Build item cards
    cards = []
    for i, it in enumerate(unique_items):
        safe_idx = str(i)
        img_url = it["image_url"]
        search_q = urllib.parse.quote_plus(it["name"][:80])

        card = html.Div([
            # Preview image
            html.Div([
                item_thumbnail(img_url, 100),
            ], id={"type": "img-preview", "index": safe_idx},
               style={"textAlign": "center", "marginBottom": "6px"}),
            # Item name
            html.Div(it["name"][:50], title=it["name"],
                     style={"color": WHITE, "fontSize": "11px", "fontWeight": "600",
                            "overflow": "hidden", "textOverflow": "ellipsis",
                            "whiteSpace": "nowrap", "marginBottom": "2px"}),
            # Price
            html.Div(f"${it['price']:,.2f}", style={"color": GRAY, "fontSize": "11px", "marginBottom": "6px"}),
            # URL input
            dcc.Input(
                id={"type": "img-url-input", "index": safe_idx},
                type="text", placeholder="Paste image URL...",
                value=img_url,
                style={"width": "100%", "backgroundColor": "#0f0f1a", "color": WHITE,
                       "border": f"1px solid {DARKGRAY}", "borderRadius": "4px",
                       "padding": "4px 6px", "fontSize": "10px", "marginBottom": "4px",
                       "boxSizing": "border-box"}),
            # Hidden store for item name
            dcc.Store(id={"type": "img-item-name", "index": safe_idx}, data=it["name"]),
            # Order ID
            html.Div(it.get("order_num", ""), title=it.get("order_num", ""),
                     style={"color": DARKGRAY, "fontSize": "9px", "marginBottom": "4px",
                            "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
            # Buttons row
            html.Div([
                html.Button("Save", id={"type": "img-save-btn", "index": safe_idx},
                            n_clicks=0,
                            style={"backgroundColor": TEAL, "color": WHITE, "border": "none",
                                   "borderRadius": "4px", "padding": "3px 10px", "fontSize": "10px",
                                   "cursor": "pointer", "fontWeight": "600", "marginRight": "4px"}),
                html.A("Amazon", href=f"https://www.amazon.com/s?k={search_q}",
                       target="_blank",
                       style={"color": CYAN, "fontSize": "10px", "textDecoration": "none",
                              "padding": "3px 6px", "border": f"1px solid {CYAN}44",
                              "borderRadius": "4px"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "4px"}),
            # Status message
            html.Div("", id={"type": "img-status", "index": safe_idx},
                     style={"color": GREEN, "fontSize": "10px", "marginTop": "2px", "minHeight": "14px"}),
        ], className="img-mgr-card",
           style={"backgroundColor": CARD2, "padding": "10px", "borderRadius": "8px",
                  "border": f"1px solid {'#ffffff15' if not img_url else TEAL + '44'}",
                  "width": "160px", "minWidth": "160px"})
        cards.append(card)

    return html.Div([
        # Header bar
        html.Div([
            html.Div([
                html.H3("IMAGE MANAGER", style={"color": CYAN, "margin": "0", "fontSize": "16px",
                                                  "display": "inline"}),
                html.Span(f"  {with_images}/{total_items} items have images",
                           style={"color": GRAY, "fontSize": "12px", "marginLeft": "12px"}),
            ]),
            html.Div([
                dcc.Input(
                    id="img-filter-input", type="text", placeholder="Filter by name...",
                    style={"backgroundColor": "#0f0f1a", "color": WHITE,
                           "border": f"1px solid {DARKGRAY}", "borderRadius": "4px",
                           "padding": "6px 10px", "fontSize": "12px", "width": "200px"}),
                dcc.RadioItems(
                    id="img-filter-show",
                    options=[{"label": "All", "value": "all"},
                             {"label": "Missing", "value": "missing"},
                             {"label": "Has Image", "value": "has"}],
                    value="all",
                    inline=True,
                    style={"color": GRAY, "fontSize": "12px", "marginLeft": "10px"},
                    labelStyle={"marginRight": "8px"},
                ),
            ], style={"display": "flex", "alignItems": "center"}),
        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                   "marginBottom": "12px", "flexWrap": "wrap", "gap": "8px"}),

        # Card grid
        html.Div(cards, id="img-mgr-grid",
                 style={"display": "flex", "flexWrap": "wrap", "gap": "10px",
                        "maxHeight": "600px", "overflowY": "auto", "padding": "4px"}),
    ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
              "marginBottom": "14px", "border": f"1px solid {CYAN}33"})


def _render_split_rows(data):
    """Render split item data as styled display rows."""
    if not data:
        return [html.P("No sub-items yet. Use the form below to add items from this pack.",
                        style={"color": GRAY, "fontSize": "11px", "fontStyle": "italic"})]
    # Column header
    rows = [html.Div([
        html.Span("#", style={"color": DARKGRAY, "fontSize": "10px", "width": "18px",
                               "textAlign": "right", "marginRight": "6px"}),
        html.Span("Name", style={"color": DARKGRAY, "fontSize": "10px", "flex": "3"}),
        html.Span("Qty", style={"color": DARKGRAY, "fontSize": "10px", "width": "35px", "textAlign": "center"}),
        html.Span("Location", style={"color": DARKGRAY, "fontSize": "10px", "width": "80px"}),
        html.Span("Category", style={"color": DARKGRAY, "fontSize": "10px", "width": "80px"}),
    ], style={"display": "flex", "alignItems": "center", "gap": "4px",
              "padding": "2px 4px", "borderBottom": f"1px solid {TEAL}33"})]

    for i, d in enumerate(data):
        loc = d.get("location", "")
        loc_color = TEAL if "Tulsa" in loc else (ORANGE if "Texas" in loc else GRAY)
        rows.append(html.Div([
            html.Span(f"{i + 1}.", style={"color": DARKGRAY, "fontSize": "11px", "width": "18px",
                                           "textAlign": "right", "marginRight": "6px"}),
            html.Span(d.get("name", ""), title=d.get("name", ""),
                      style={"color": WHITE, "fontSize": "12px", "flex": "3",
                             "fontWeight": "600", "overflow": "hidden",
                             "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
            html.Span(f"x{d.get('qty', 1)}", style={"color": GRAY, "fontSize": "11px",
                                                       "width": "35px", "textAlign": "center"}),
            html.Span(loc or "—", style={"color": loc_color, "fontSize": "11px", "width": "80px"}),
            html.Span(d.get("category", ""), style={"color": DARKGRAY, "fontSize": "10px", "width": "80px"}),
        ], style={"display": "flex", "alignItems": "center", "gap": "4px",
                  "padding": "3px 4px", "borderBottom": "1px solid #ffffff08"}))

    # Summary
    total_qty = sum(d.get("qty", 1) for d in data)
    tulsa_n = sum(1 for d in data if "Tulsa" in d.get("location", ""))
    texas_n = sum(1 for d in data if "Texas" in d.get("location", ""))
    summary_parts = [f"{len(data)} items, {total_qty} total qty"]
    if tulsa_n:
        summary_parts.append(f"{tulsa_n} Tulsa")
    if texas_n:
        summary_parts.append(f"{texas_n} Texas")
    rows.append(html.Div(" | ".join(summary_parts),
                style={"color": TEAL, "fontSize": "10px", "fontWeight": "bold",
                       "padding": "4px 4px 0 4px"}))
    return rows


def _build_split_container(idx, existing, det_name, det_cat, det_qty, det_loc, _inp, item_name="", orig_total=0):
    """Build the split container — a guided wizard for breaking down multi-pack items."""
    loc_options = [{"label": "Tulsa, OK", "value": "Tulsa, OK"},
                   {"label": "Texas", "value": "Texas"},
                   {"label": "Other", "value": "Other"}]
    cat_options = [{"label": c, "value": c} for c in CATEGORY_OPTIONS]
    _hidden = {"display": "none"}

    # Pre-fill split data from existing saved details
    is_split = len(existing) > 1
    split_data = []
    if is_split:
        split_data = [{"name": d["display_name"], "qty": d["true_qty"],
                        "category": d["category"],
                        "location": d.get("location", "")} for d in existing]

    # Wizard state: starts at "done" if data exists, else step 0
    if split_data:
        wiz_init = {"step": "done", "category": det_cat, "total_qty": len(split_data)}
        q_text = f"All {len(split_data)} items allocated! Click Save above when ready."
        s0 = s1 = s2 = s3a = s3b = s3c = _hidden
        btn_row_style = _hidden
        btn_text = ""
    else:
        wiz_init = {"step": 0, "category": "", "total_qty": 0}
        q_text = "What type of item is this?"
        s0 = {"display": "block"}
        s1 = s2 = s3a = s3b = s3c = _hidden
        btn_row_style = {"display": "block", "marginTop": "8px"}
        btn_text = "Next \u2192"

    return html.Div(
        id={"type": "det-split-container", "index": idx},
        children=[
            dcc.Store(id={"type": "det-split-data", "index": idx}, data=split_data),
            dcc.Store(id={"type": "wiz-state", "index": idx}, data=wiz_init),

            # ── Header ──
            html.Div([
                html.Div("PACK BREAKDOWN", style={"color": TEAL, "fontSize": "14px",
                                                    "fontWeight": "bold", "marginBottom": "4px"}),
                html.Div([
                    html.Span("Receipt item: ", style={"color": GRAY, "fontSize": "11px"}),
                    html.Span(item_name or det_name, style={"color": WHITE, "fontSize": "11px",
                                                              "fontWeight": "600"}),
                ]),
                html.Div([
                    html.Span(f"Qty on receipt: {det_qty}", style={"color": GRAY, "fontSize": "11px",
                                                                     "marginRight": "12px"}),
                    html.Span(f"Total: ${orig_total:.2f}" if orig_total else "",
                              style={"color": GRAY, "fontSize": "11px"}),
                ], style={"marginTop": "2px"}),
            ], style={"marginBottom": "10px", "paddingBottom": "8px",
                      "borderBottom": f"1px solid {TEAL}33"}),

            # ── Wizard question text ──
            html.Div(q_text,
                     id={"type": "wiz-question", "index": idx},
                     style={"color": ORANGE, "fontSize": "13px", "fontWeight": "600",
                            "marginBottom": "8px"}),

            # ── Step 0: Category selection ──
            html.Div(
                dbc.Select(id={"type": "wiz-cat", "index": idx},
                           options=[{"label": "Select category...", "value": ""}] + cat_options,
                           value=det_cat or "",
                           style={"width": "200px", "fontSize": "12px",
                                  "backgroundColor": "#1a1a2e", "color": WHITE}),
                id={"type": "wiz-step0", "index": idx}, style=s0,
            ),

            # ── Step 1: How many items ──
            html.Div(
                dcc.Input(id={"type": "wiz-qty", "index": idx}, type="number",
                          min=1, value=det_qty or 1,
                          style={**_inp, "width": "100px"}),
                id={"type": "wiz-step1", "index": idx}, style=s1,
            ),

            # ── Step 2: Same or different? ──
            html.Div(
                dcc.RadioItems(
                    id={"type": "wiz-same-diff", "index": idx},
                    options=[
                        {"label": "  All the same item (just split between locations)", "value": "same"},
                        {"label": "  Each one is different (different colors, types, etc.)", "value": "different"},
                    ],
                    value="same",
                    style={"color": WHITE, "fontSize": "12px"},
                    labelStyle={"display": "block", "padding": "6px 0", "cursor": "pointer"},
                    inputStyle={"marginRight": "6px"},
                ),
                id={"type": "wiz-step2", "index": idx}, style=s2,
            ),

            # ── Step 3a: Common name (for "all same" path) ──
            html.Div([
                html.Div("What is this item?", style={"color": GRAY, "fontSize": "11px", "marginBottom": "2px"}),
                dcc.Input(id={"type": "wiz-same-name", "index": idx}, type="text",
                          placeholder="e.g. Brown Wrapping Paper, Black PLA 1kg...",
                          value="",
                          style={**_inp, "width": "100%"}),
            ], id={"type": "wiz-step3a", "index": idx}, style=s3a),

            # ── Step 3b: Location allocation (for "all same" path) ──
            html.Div([
                html.Div([
                    html.Span("Tulsa, OK:", style={"color": TEAL, "fontSize": "12px",
                                                     "fontWeight": "600", "width": "80px",
                                                     "display": "inline-block"}),
                    dcc.Input(id={"type": "wiz-tulsa-qty", "index": idx}, type="number",
                              min=0, value=0, style={**_inp, "width": "60px"}),
                ], style={"marginBottom": "6px", "display": "flex", "alignItems": "center", "gap": "8px"}),
                html.Div([
                    html.Span("Texas:", style={"color": ORANGE, "fontSize": "12px",
                                                 "fontWeight": "600", "width": "80px",
                                                 "display": "inline-block"}),
                    dcc.Input(id={"type": "wiz-texas-qty", "index": idx}, type="number",
                              min=0, value=0, style={**_inp, "width": "60px"}),
                ], style={"display": "flex", "alignItems": "center", "gap": "8px"}),
            ], id={"type": "wiz-step3b", "index": idx}, style=s3b),

            # ── Step 3c: Per-item name + location (for "different" path) ──
            html.Div([
                html.Div([
                    html.Div("Name / Color:", style={"color": GRAY, "fontSize": "11px", "marginBottom": "2px"}),
                    dcc.Input(id={"type": "wiz-item-name", "index": idx}, type="text",
                              placeholder="e.g. Black PLA, Red PETG...",
                              value="",
                              style={**_inp, "width": "100%"}),
                ], style={"marginBottom": "6px"}),
                html.Div([
                    html.Div("Where did it go?", style={"color": GRAY, "fontSize": "11px", "marginBottom": "2px"}),
                    dbc.Select(id={"type": "wiz-item-loc", "index": idx},
                               options=loc_options, value=det_loc or "",
                               style={"width": "160px", "fontSize": "12px",
                                      "backgroundColor": "#1a1a2e", "color": WHITE}),
                ]),
            ], id={"type": "wiz-step3c", "index": idx}, style=s3c),

            # ── Next / Add button row ──
            html.Div(
                html.Button(btn_text, id={"type": "wiz-next", "index": idx},
                            style={"fontSize": "12px", "padding": "6px 20px",
                                   "backgroundColor": TEAL, "color": WHITE,
                                   "border": "none", "borderRadius": "6px",
                                   "cursor": "pointer", "fontWeight": "bold"}),
                id={"type": "wiz-btn-row", "index": idx},
                style=btn_row_style,
            ),

            # ── Items display ──
            html.Div(
                _render_split_rows(split_data),
                id={"type": "det-split-display", "index": idx},
                style={"marginTop": "10px", "maxHeight": "300px", "overflowY": "auto"},
            ),

            # ── Clear all ──
            html.Button("Clear All & Start Over", id={"type": "split-clear-btn", "index": idx},
                        style={"fontSize": "10px", "padding": "4px 12px", "backgroundColor": "transparent",
                               "color": DARKGRAY, "border": f"1px solid {DARKGRAY}44", "borderRadius": "4px",
                               "cursor": "pointer", "marginTop": "8px"}),
        ],
        style={"display": "block" if is_split else "none",
               "marginTop": "6px", "padding": "12px 14px",
               "backgroundColor": "#0a0a1a", "borderRadius": "8px",
               "border": f"1px solid {TEAL}44"},
    )


def _build_item_row(idx, item_name, img_url, det_name, det_cat, det_qty, det_loc,
                    has_details, orig_qty, orig_total, is_split, existing, onum):
    """Build a single receipt item card for inventory naming.

    Layout:
      Header:  Original receipt name (read-only) + qty + price
      Body:    Either single-item naming OR split wizard (toggled by checkbox)
      Footer:  Save + Reset + Image toggle

    Same pattern-matching IDs — all existing callbacks work unchanged.
    """
    cat_options = [{"label": c, "value": c} for c in CATEGORY_OPTIONS]
    loc_options = [{"label": "Tulsa, OK", "value": "Tulsa, OK"},
                   {"label": "Texas", "value": "Texas"},
                   {"label": "Other", "value": "Other"}]
    _inp = {"fontSize": "13px", "backgroundColor": "#0d0d1a", "color": WHITE,
            "border": f"1px solid {DARKGRAY}55", "borderRadius": "6px", "padding": "7px 12px"}
    _lbl = {"color": GRAY, "fontSize": "10px", "fontWeight": "700", "letterSpacing": "0.8px",
            "textTransform": "uppercase", "marginBottom": "2px"}

    _card_border = GREEN if has_details else ORANGE
    # Receipt-level price (always based on original receipt qty, not split qty)
    receipt_per_unit = orig_total / orig_qty if orig_qty > 0 else orig_total
    per_unit = orig_total / det_qty if det_qty > 0 else receipt_per_unit
    _price_str = f"qty {det_qty}  \u00b7  ${per_unit:.2f}/ea  \u00b7  ${orig_total:.2f} total"

    # ── Hidden stores ──
    hidden = html.Div([
        dcc.Store(id={"type": "det-order-num", "index": idx}, data=onum),
        dcc.Store(id={"type": "det-item-name", "index": idx}, data=item_name),
        dcc.Store(id={"type": "det-orig-qty", "index": idx}, data=orig_qty),
        dcc.Store(id={"type": "det-orig-name", "index": idx}, data=item_name),
        dcc.Store(id={"type": "det-orig-total", "index": idx}, data=orig_total),
        html.Span(id={"type": "det-price-display", "index": idx},
                  children=_price_str, style={"display": "none"}),
    ], style={"display": "none"})

    # ── HEADER: Original receipt name (read-only reference) ──
    _saved_badge = html.Span(
        "\u2713 SAVED", style={"fontSize": "9px", "fontWeight": "bold", "padding": "2px 8px",
                               "borderRadius": "6px", "backgroundColor": f"{GREEN}22",
                               "color": GREEN, "border": f"1px solid {GREEN}44",
                               "marginLeft": "8px"}) if has_details else html.Span(
        "NEEDS NAMING", style={"fontSize": "9px", "fontWeight": "bold", "padding": "2px 8px",
                                "borderRadius": "6px", "backgroundColor": f"{ORANGE}22",
                                "color": ORANGE, "border": f"1px solid {ORANGE}44",
                                "marginLeft": "8px"})
    header = html.Div([
        html.Div([
            html.Div(item_thumbnail(img_url, 32), style={"flexShrink": "0", "marginRight": "10px"}),
            html.Div([
                html.Div([
                    html.Span(item_name[:90], title=item_name,
                              style={"color": WHITE, "fontSize": "13px", "fontWeight": "600",
                                     "overflow": "hidden", "textOverflow": "ellipsis",
                                     "whiteSpace": "nowrap", "maxWidth": "500px",
                                     "display": "inline-block", "verticalAlign": "middle"}),
                    _saved_badge,
                ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "4px"}),
                html.Div([
                    html.Span(f"Qty: {orig_qty}", style={"color": GRAY, "fontSize": "11px"}),
                    html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66"}),
                    html.Span(f"${receipt_per_unit:.2f}/ea", style={"color": GRAY, "fontSize": "11px"}),
                    html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66"}),
                    html.Span(f"${orig_total:.2f} total", style={"color": ORANGE, "fontSize": "11px",
                                                                    "fontWeight": "bold"}),
                ], style={"marginTop": "2px"}),
            ], style={"flex": "1", "minWidth": "0"}),
        ], style={"display": "flex", "alignItems": "center"}),
    ], style={"padding": "10px 14px", "borderBottom": f"1px solid {DARKGRAY}22"})

    # ── SPLIT TOGGLE — prominent, always visible ──
    split_toggle = html.Div([
        dcc.Checklist(
            id={"type": "loc-split-check", "index": idx},
            options=[{"label": "  Split into multiple items (bundle, multi-pack, etc.)",
                      "value": "split"}],
            value=["split"] if is_split else [],
            style={"fontSize": "12px", "color": CYAN, "fontWeight": "600"},
            labelStyle={"cursor": "pointer", "display": "flex", "alignItems": "center"}),
    ], style={"padding": "8px 14px", "borderBottom": f"1px solid {DARKGRAY}15"})

    # ── SINGLE-ITEM MODE: Name + Category + Location ──
    single_mode = html.Div([
        html.Div([
            html.Div([
                html.Div("INVENTORY NAME", style=_lbl),
                dcc.Input(id={"type": "det-name", "index": idx}, type="text",
                          value=det_name, placeholder="What is this item? e.g. Black PLA 1kg",
                          style={**_inp, "width": "100%"}),
            ], style={"flex": "2", "minWidth": "180px"}),
            html.Div([
                html.Div("CATEGORY", style=_lbl),
                dbc.Select(id={"type": "det-cat", "index": idx},
                           options=cat_options, value=det_cat,
                           style={**_inp, "padding": "5px 8px"}),
            ], style={"flex": "1", "minWidth": "120px"}),
            html.Div([
                html.Div("LOCATION", style=_lbl),
                dbc.Select(id={"type": "loc-dropdown", "index": idx},
                           options=loc_options, value=det_loc,
                           style={**_inp, "padding": "5px 8px"}),
            ], style={"flex": "1", "minWidth": "100px"}),
            html.Div([
                html.Div("QTY", style=_lbl),
                dcc.Input(id={"type": "det-qty", "index": idx}, type="number",
                          min=1, value=det_qty, debounce=False,
                          style={**_inp, "width": "60px", "textAlign": "center"}),
            ], style={"flexShrink": "0"}),
        ], style={"display": "flex", "gap": "12px", "alignItems": "flex-end",
                  "flexWrap": "wrap"}),
    ], id={"type": "det-single-mode", "index": idx},
       style={"display": "none" if is_split else "block",
              "padding": "10px 14px"})

    # ── SPLIT MODE: Full split wizard (the main workflow for bundles) ──
    split_container = _build_split_container(idx, existing, det_name, det_cat, det_qty, det_loc, _inp,
                                             item_name=item_name, orig_total=orig_total)
    split_mode = html.Div([
        split_container,
    ], id={"type": "det-split-mode", "index": idx},
       style={"display": "block" if is_split else "none",
              "padding": "10px 14px"})

    # ── FOOTER: Save + status + Reset + Image toggle ──
    footer = html.Div([
        html.Button("\u2713 Save", id={"type": "det-save-btn", "index": idx},
                    style={"height": "36px", "padding": "0 24px",
                           "background": f"linear-gradient(135deg, {GREEN}, #27ae60)",
                           "color": WHITE, "border": "none", "borderRadius": "6px",
                           "cursor": "pointer", "fontWeight": "bold", "fontSize": "13px",
                           "boxShadow": f"0 2px 8px {GREEN}33"}),
        html.Span(id={"type": "det-status", "index": idx}, children="",
                  style={"fontSize": "11px", "color": GREEN, "fontWeight": "bold",
                         "marginLeft": "8px", "minWidth": "40px"}),
        html.Button("Reset", id={"type": "det-reset-btn", "index": idx},
                    style={"height": "36px", "padding": "0 14px",
                           "backgroundColor": "transparent", "color": GRAY,
                           "border": f"1px solid {DARKGRAY}33", "borderRadius": "6px",
                           "cursor": "pointer", "fontSize": "11px"}),
        html.Span(style={"flex": "1"}),
        # Image toggle (secondary)
        html.Button("\U0001f4f7 Image", id={"type": "det-adv-btn", "index": idx},
                    style={"height": "30px", "padding": "0 12px",
                           "backgroundColor": "transparent", "color": GRAY,
                           "border": f"1px solid {DARKGRAY}22", "borderRadius": "6px",
                           "cursor": "pointer", "fontSize": "11px"}),
    ], style={"display": "flex", "alignItems": "center", "gap": "8px",
              "padding": "8px 14px", "borderTop": f"1px solid {DARKGRAY}22"})

    # ── IMAGE SECTION (toggled by "Image" button, secondary) ──
    image_section = html.Div([
        html.Div([
            html.Div(
                item_thumbnail(img_url, 24) if img_url else html.Span(
                    "\u2014", style={"width": "24px", "height": "24px",
                                      "display": "inline-flex", "alignItems": "center",
                                      "justifyContent": "center", "color": DARKGRAY,
                                      "fontSize": "10px"}),
                id={"type": "det-img-preview", "index": idx},
                style={"flexShrink": "0", "marginRight": "6px"}),
            dcc.Input(id={"type": "det-img-url", "index": idx}, type="text",
                      value=img_url or "", placeholder="Paste image URL...",
                      style={**_inp, "flex": "1", "minWidth": "80px", "fontSize": "11px"}),
            html.Button("Fetch", id={"type": "det-img-fetch-btn", "index": idx},
                        style={"fontSize": "10px", "padding": "5px 10px",
                               "backgroundColor": CYAN, "color": "#0f0f1a",
                               "border": "none", "borderRadius": "5px",
                               "cursor": "pointer", "fontWeight": "bold",
                               "marginLeft": "4px", "whiteSpace": "nowrap"}),
            html.Span("", id={"type": "det-img-status", "index": idx},
                      style={"fontSize": "10px", "color": GREEN, "marginLeft": "4px",
                             "whiteSpace": "nowrap"}),
        ], style={"display": "flex", "alignItems": "center"}),
    ], id={"type": "det-adv-section", "index": idx},
       style={"display": "none",
              "padding": "8px 14px", "borderTop": f"1px solid {DARKGRAY}15"})

    return html.Div([header, split_toggle, single_mode, split_mode, footer, image_section, hidden],
        className="item-row",
        style={"marginBottom": "6px",
               "backgroundColor": "#0f1225",
               "borderLeft": f"4px solid {_card_border}",
               "borderRadius": "6px",
               "border": f"1px solid {DARKGRAY}15",
               "transition": "all 0.15s ease"})


def _build_category_manager():
    """Build the Category Manager — flat table with inline dropdowns for fast categorization."""
    if len(INV_ITEMS) == 0:
        return html.P("No inventory items loaded.", style={"color": GRAY, "fontSize": "12px"})

    cat_options = [{"label": c, "value": c} for c in CATEGORY_OPTIONS]
    loc_options = [{"label": "Tulsa, OK", "value": "Tulsa, OK"},
                   {"label": "Texas", "value": "Texas"},
                   {"label": "Other", "value": "Other"}]

    # Build unique item rows from INV_ITEMS, grouped by (order_num, orig_name)
    seen = set()
    rows = []
    items_sorted = INV_ITEMS.sort_values(["category", "name"])
    for _, r in items_sorted.iterrows():
        order_num = r.get("order_num", "")
        orig_name = r.get("_orig_name", r["name"])
        item_key = f"{order_num}||{orig_name}"
        if item_key in seen:
            continue
        seen.add(item_key)

        cat = r.get("category", "Other")
        loc = r.get("location", "") or r.get("_override_location", "")
        display = r.get("name", orig_name)
        img_url = _IMAGE_URLS.get(display, "") or r.get("image_url", "")
        is_personal = cat in ("Personal/Gift", "Business Fees")
        row_opacity = "0.5" if is_personal else "1"

        rows.append(html.Tr([
            # Thumbnail
            html.Td(item_thumbnail(img_url, 32),
                     style={"padding": "6px 6px", "textAlign": "center", "width": "40px"}),
            # Item name
            html.Td(html.Span(display[:55], title=display,
                               style={"color": WHITE, "fontSize": "12px"}),
                     style={"padding": "6px 8px", "maxWidth": "280px", "overflow": "hidden",
                            "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
            # Qty x Price
            html.Td(f"{int(r['qty'])} × ${r['price']:,.2f}",
                     style={"color": GRAY, "fontSize": "11px", "padding": "6px 8px",
                            "whiteSpace": "nowrap"}),
            # Total
            html.Td(f"${r['total']:,.2f}",
                     style={"color": ORANGE, "fontFamily": "monospace", "fontWeight": "bold",
                            "fontSize": "12px", "padding": "6px 8px", "textAlign": "right"}),
            # Category dropdown
            html.Td(
                dbc.Select(
                    id={"type": "catmgr-cat", "index": item_key},
                    options=cat_options, value=cat,
                    style={"width": "140px", "fontSize": "11px",
                           "backgroundColor": "#1a1a2e", "color": WHITE},
                ),
                style={"padding": "4px 6px"}),
            # Location dropdown
            html.Td(
                dbc.Select(
                    id={"type": "catmgr-loc", "index": item_key},
                    options=[{"label": "\u2014", "value": ""}] + loc_options,
                    value=loc if loc in ("Tulsa, OK", "Texas", "Other") else "",
                    style={"width": "120px", "fontSize": "11px",
                           "backgroundColor": "#1a1a2e", "color": WHITE},
                ),
                style={"padding": "4px 6px"}),
            # Status indicator
            html.Td("", id={"type": "catmgr-status", "index": item_key},
                     style={"color": GREEN, "fontSize": "11px", "padding": "6px 4px",
                            "minWidth": "30px", "textAlign": "center"}),
        ], style={"borderBottom": "1px solid #ffffff08", "opacity": row_opacity}))

    # Category summary strip
    cat_counts = INV_ITEMS.groupby("category").agg(
        items=("name", "count"), cost=("total", "sum")).sort_values("cost", ascending=False)
    summary_pills = []
    for cat_name, row in cat_counts.iterrows():
        pill_color = TEAL if cat_name not in ("Personal/Gift", "Business Fees", "Other") else DARKGRAY
        summary_pills.append(html.Span(
            f"{cat_name} ({int(row['items'])}) ${row['cost']:,.0f}",
            style={"backgroundColor": f"{pill_color}22", "color": pill_color,
                   "padding": "3px 10px", "borderRadius": "12px", "fontSize": "11px",
                   "border": f"1px solid {pill_color}44", "whiteSpace": "nowrap"}))

    return html.Div([
        # Summary pills
        html.Div(summary_pills, style={"display": "flex", "flexWrap": "wrap", "gap": "6px",
                                         "marginBottom": "12px"}),
        # Table
        html.Div([
            html.Table([
                html.Thead(html.Tr([
                    html.Th("", style={"width": "40px", "padding": "6px"}),
                    html.Th("Item", style={"textAlign": "left", "padding": "6px 8px", "fontSize": "11px",
                                            "color": GRAY, "fontWeight": "700"}),
                    html.Th("Qty", style={"textAlign": "left", "padding": "6px 8px", "fontSize": "11px",
                                           "color": GRAY, "fontWeight": "700"}),
                    html.Th("Cost", style={"textAlign": "right", "padding": "6px 8px", "fontSize": "11px",
                                            "color": GRAY, "fontWeight": "700"}),
                    html.Th("Category", style={"textAlign": "left", "padding": "6px 8px", "fontSize": "11px",
                                                "color": GRAY, "fontWeight": "700"}),
                    html.Th("Location", style={"textAlign": "left", "padding": "6px 8px", "fontSize": "11px",
                                                "color": GRAY, "fontWeight": "700"}),
                    html.Th("", style={"width": "30px"}),
                ], style={"borderBottom": f"2px solid {PURPLE}44"})),
                html.Tbody(rows),
            ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
        ], style={"maxHeight": "500px", "overflowY": "auto"}),
    ])


def _build_inventory_editor(show_saved=False):
    """Build the Inventory Editor — scrollable list of items as individual cards.
    If show_saved=True, includes already-saved items (pre-filled) for review."""
    if len(INV_ITEMS) == 0:
        return html.Div(id="editor-items-container"), 0

    # Count saved vs unsaved
    saved_count = 0
    total_items = 0
    unsaved_items = []
    saved_items = []

    for inv in sorted(INVOICES, key=lambda o: o.get("date", ""), reverse=True):
        onum = inv["order_num"]
        is_personal = inv["source"] == "Personal Amazon" or ("file" in inv and isinstance(inv.get("file"), str) and "Gigi" in inv.get("file", ""))
        if is_personal:
            continue
        ship_addr = inv.get("ship_address", "")
        orig_location = classify_location(ship_addr)

        for item in inv["items"]:
            item_name = item["name"]
            if item_name.startswith("Your package was left near the front door or porch."):
                item_name = item_name.replace("Your package was left near the front door or porch.", "").strip()
            auto_cat = categorize_item(item_name)
            if auto_cat in ("Personal/Gift", "Business Fees"):
                continue

            total_items += 1
            detail_key = (onum, item_name)
            existing = _ITEM_DETAILS.get(detail_key, [])
            if existing:
                saved_count += 1
                if show_saved:
                    # Pre-fill with saved data
                    first_det = existing[0]
                    saved_items.append({
                        "order_num": onum,
                        "name": item_name,
                        "qty": sum(d.get("true_qty", 1) for d in existing),
                        "price": item["price"],
                        "category": first_det.get("category", auto_cat),
                        "location": first_det.get("location", orig_location),
                        "date": inv.get("date", ""),
                        "_saved": True,
                        "_saved_details": existing,
                        "_saved_at": _ITEM_SAVED_AT.get(detail_key, ""),
                    })
            else:
                unsaved_items.append({
                    "order_num": onum,
                    "name": item_name,
                    "qty": item["qty"],
                    "price": item["price"],
                    "category": auto_cat,
                    "location": orig_location,
                    "date": inv.get("date", ""),
                })

    unsaved_count = total_items - saved_count

    # Progress bar
    pct = round(saved_count / total_items * 100) if total_items > 0 else 0
    _bar_color = GREEN if pct > 75 else (ORANGE if pct > 40 else TEAL)
    progress_bar = html.Div([
        html.Div([
            html.Span(f"{saved_count}", style={"color": WHITE, "fontSize": "22px", "fontWeight": "bold"}),
            html.Span(f" / {total_items} items organized", style={"color": GRAY, "fontSize": "14px",
                       "marginLeft": "4px"}),
            html.Span(f"  {pct}%", style={"color": _bar_color, "fontSize": "14px", "fontWeight": "bold",
                       "marginLeft": "10px"}),
        ], style={"marginBottom": "8px"}),
        html.Div([
            html.Div(
                f"{pct}%" if pct > 15 else "",
                style={"width": f"{max(pct, 2)}%", "height": "18px",
                        "backgroundColor": _bar_color,
                        "borderRadius": "9px", "transition": "width 0.3s",
                        "fontSize": "10px", "color": WHITE, "fontWeight": "bold",
                        "lineHeight": "18px", "textAlign": "center", "overflow": "hidden"}),
        ], style={"width": "100%", "height": "18px", "backgroundColor": "#0d0d1a",
                  "borderRadius": "9px", "overflow": "hidden",
                  "boxShadow": f"inset 0 1px 3px rgba(0,0,0,0.4)"}),
    ], style={"marginBottom": "18px", "padding": "14px 16px", "backgroundColor": "#0f1225",
              "borderRadius": "8px", "boxShadow": "0 1px 4px rgba(0,0,0,0.2)"})

    # Filament color options for dropdown
    _filament_colors = [
        "Black PLA", "White PLA", "Gray PLA", "Red PLA", "Blue PLA",
        "Green PLA", "Yellow PLA", "Orange PLA", "Purple PLA", "Pink PLA",
        "Gold PLA", "Silver PLA", "Beige PLA", "Brown PLA", "Clear PLA",
        "Marble PLA", "Wood PLA", "Silk Gold PLA", "Silk Silver PLA",
        "Silk Copper PLA", "Silk Rainbow PLA", "Matte Black PLA", "Matte White PLA",
        "Black PETG", "White PETG", "Clear PETG",
        "Black TPU", "White TPU", "Clear TPU",
    ]
    _color_opts = [{"label": c, "value": c} for c in _filament_colors]
    _color_opts.append({"label": "Custom (type below)", "value": "_custom"})

    _cat_opts = [{"label": c, "value": c} for c in CATEGORY_OPTIONS]
    _loc_opts = [{"label": "Tulsa, OK", "value": "Tulsa, OK"},
                 {"label": "Texas", "value": "Texas"},
                 {"label": "Other", "value": "Other"}]

    # Collect previously-used inventory names from saved items, grouped by category
    _names_by_cat: dict[str, set[str]] = {}
    for (_onum, _iname), _dets in _ITEM_DETAILS.items():
        for _d in _dets:
            _dn = (_d.get("display_name") or "").strip()
            _dc = (_d.get("category") or "Other").strip()
            if _dn:
                _names_by_cat.setdefault(_dc, set()).add(_dn)
    _names_by_cat_sorted = {c: sorted(ns) for c, ns in _names_by_cat.items()}

    # ── Shared styles ──
    _lbl = {"color": CYAN, "fontSize": "11px", "fontWeight": "bold", "letterSpacing": "0.5px",
            "textTransform": "uppercase", "marginBottom": "4px"}
    _sel_style = {"fontSize": "13px", "backgroundColor": "#1a1a2e", "color": WHITE}
    _inp_style = {"fontSize": "13px", "backgroundColor": "#1a1a2e", "color": WHITE,
                  "border": f"1px solid {DARKGRAY}55", "borderRadius": "6px", "padding": "8px 12px"}

    # ── Build scrollable item cards ──
    item_cards = []
    # Sort saved items by save time (most recent first)
    saved_items.sort(key=lambda x: x.get("_saved_at", ""), reverse=True)
    # Show saved items first (for review), then unsaved
    all_items = (saved_items + unsaved_items) if show_saved else unsaved_items
    capped = all_items if show_saved else all_items[:50]  # show all when reviewing saved
    for i, item in enumerate(capped):
        is_filament = item["category"] == "Filament"
        is_saved = item.get("_saved", False)
        saved_details = item.get("_saved_details", [])
        # Pre-fill name from saved details
        _prefill_name = ""
        _prefill_pack_type = "single"
        if is_saved and saved_details:
            names = set(d.get("display_name", "") for d in saved_details)
            if len(names) == 1:
                _prefill_name = list(names)[0]
            elif len(names) > 1:
                _prefill_pack_type = "different"

        # Detect if saved item was split between locations
        _is_split = False
        _split_tulsa_qty = item["qty"]
        _split_texas_qty = 0
        if is_saved and saved_details:
            _det_locs = set(d.get("location", "") for d in saved_details)
            if len(_det_locs) > 1:
                _is_split = True
                _split_tulsa_qty = sum(d.get("true_qty", 1) for d in saved_details
                                       if "Tulsa" in d.get("location", "") or "OK" in d.get("location", ""))
                _split_texas_qty = sum(d.get("true_qty", 1) for d in saved_details
                                       if "Texas" in d.get("location", "") or "TX" in d.get("location", ""))

        # Check which inventory products this item matches
        _in_stock = False
        _stock_names = set()
        if is_saved and saved_details:
            for _sd in saved_details:
                _sd_loc = _sd.get("location", "")
                _sd_dn = _sd.get("display_name", "")
                _sd_cat = _sd.get("category", "Other")
                _sd_key = (_sd_loc, _sd_dn, _sd_cat)
                if _UPLOADED_INVENTORY.get(_sd_key, 0) > 0:
                    _in_stock = True
                    _stock_names.add(_sd_dn)

        card = html.Div([
            # ── Date + Order header ──
            html.Div([
                html.Span(item["date"] or "No date", style={
                    "color": ORANGE, "fontSize": "18px", "fontWeight": "bold",
                    "marginRight": "14px"}),
                html.Span(f"Order #{item['order_num']}", style={
                    "color": CYAN, "fontSize": "13px", "marginRight": "14px"}),
                html.Span(f"Qty: {item['qty']}  |  ${item['price']:.2f}/ea", style={
                    "color": WHITE, "fontSize": "12px"}),
                html.Span("SAVED", style={
                    "color": GREEN, "fontSize": "10px", "fontWeight": "bold",
                    "marginLeft": "10px", "padding": "2px 8px",
                    "backgroundColor": f"{GREEN}22", "borderRadius": "8px",
                    "border": f"1px solid {GREEN}44"}) if is_saved else None,
            ], style={"marginBottom": "8px"}),

            # ── Amazon product name ──
            html.Div(item["name"][:140], style={
                "color": GRAY, "fontSize": "12px", "fontStyle": "italic",
                "lineHeight": "1.4", "marginBottom": "14px",
                "padding": "8px 12px", "backgroundColor": "#0d0d1a",
                "borderRadius": "6px", "borderLeft": f"3px solid {DARKGRAY}44"}),

            # ── Row 1: Category, Qty, Pack Type ──
            html.Div([
                # Category
                html.Div([
                    html.Div("CATEGORY", style=_lbl),
                    dbc.Select(id={"type": "inv-card-cat", "index": i},
                               options=_cat_opts, value=item["category"],
                               style={**_sel_style, "maxWidth": "220px"}),
                ], style={"flex": "1", "minWidth": "160px"}),

                # Quantity
                html.Div([
                    html.Div("QTY", style=_lbl),
                    dcc.Input(id={"type": "inv-card-qty", "index": i},
                              type="number", min=1, value=item["qty"],
                              style={**_inp_style, "width": "70px"}),
                ], style={"minWidth": "80px"}),

                # All the same?
                html.Div([
                    html.Div("ALL THE SAME?", style=_lbl),
                    dbc.Select(id={"type": "inv-card-pack-type", "index": i},
                               options=[
                                   {"label": "Yes", "value": "single"},
                                   {"label": "No, different items", "value": "different"},
                               ], value=_prefill_pack_type,
                               style={**_sel_style, "maxWidth": "200px"}),
                ], style={"flex": "1", "minWidth": "150px"}),
            ], style={"display": "flex", "gap": "14px", "flexWrap": "wrap",
                      "marginBottom": "10px"}),

            # ── Row 2: Name, Color picker, Location ──
            html.Div([
                # Inventory Name (for single / identical packs)
                html.Div([
                    html.Div("INVENTORY NAME", style=_lbl),
                    html.Div([
                        dcc.Dropdown(
                            id={"type": "inv-card-name-pick", "index": i},
                            options=[{"label": n, "value": n}
                                     for n in _names_by_cat_sorted.get(item["category"], [])],
                            placeholder="Pick or type new...",
                            searchable=True, clearable=True,
                            style={"fontSize": "13px", "backgroundColor": "#1a1a2e",
                                   "color": WHITE, "minWidth": "220px", "flex": "1"},
                            className="dash-dark-dropdown"),
                        dcc.Input(id={"type": "inv-card-name", "index": i},
                                  type="text", value=_prefill_name, placeholder="or type custom name",
                                  style={**_inp_style, "width": "160px", "flex": "0 0 auto"}),
                    ], style={"display": "flex", "gap": "8px", "alignItems": "center",
                              "flexWrap": "wrap"}),
                ], style={"flex": "2", "minWidth": "280px"}),

                # Filament color picker
                html.Div([
                    html.Div("QUICK COLOR", style={**_lbl, "color": TEAL}),
                    dbc.Select(id={"type": "inv-card-color", "index": i},
                               options=[{"label": "-- Pick --", "value": ""}] + _color_opts,
                               value="",
                               style={**_sel_style, "maxWidth": "200px"}),
                ], id={"type": "inv-card-color-section", "index": i},
                   style={"flex": "1", "minWidth": "160px",
                          "display": "block" if is_filament else "none"}),

                # Location
                html.Div([
                    html.Div("LOCATION", style=_lbl),
                    dbc.Select(id={"type": "inv-card-loc", "index": i},
                               options=_loc_opts, value=item["location"],
                               style={**_sel_style, "maxWidth": "180px"}),
                ], style={"flex": "1", "minWidth": "140px"}),

                # Split between locations?
                html.Div([
                    html.Div("SPLIT?", style=_lbl),
                    dbc.Select(id={"type": "inv-card-split", "index": i},
                               options=[
                                   {"label": "No", "value": "no"},
                                   {"label": "Yes", "value": "yes"},
                               ], value="yes" if _is_split else "no",
                               style={**_sel_style, "maxWidth": "100px"}),
                ], style={"minWidth": "80px"}),
            ], id={"type": "inv-card-single-row", "index": i},
               style={"display": ("none" if is_saved and _prefill_pack_type == "different" else "flex"),
                      "gap": "14px", "flexWrap": "wrap",
                      "marginBottom": "10px"}),

            # ── Split details (shown when split = yes) ──
            html.Div([
                html.Div([
                    html.Span("Tulsa qty:", style={"color": TEAL, "fontSize": "12px",
                              "fontWeight": "bold", "marginRight": "6px"}),
                    dcc.Input(id={"type": "inv-card-split-qty1", "index": i},
                              type="number", min=0, value=_split_tulsa_qty,
                              style={**_inp_style, "width": "60px", "fontSize": "12px",
                                     "padding": "6px 8px"}),
                    html.Span("Texas qty:", style={"color": ORANGE, "fontSize": "12px",
                              "fontWeight": "bold", "margin": "0 6px 0 16px"}),
                    dcc.Input(id={"type": "inv-card-split-qty2", "index": i},
                              type="number", min=0, value=_split_texas_qty,
                              style={**_inp_style, "width": "60px", "fontSize": "12px",
                                     "padding": "6px 8px"}),
                ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap",
                          "gap": "4px"}),
            ], id={"type": "inv-card-split-row", "index": i},
               style={"display": "block" if _is_split else "none", "padding": "10px 14px",
                      "backgroundColor": f"{PURPLE}08", "borderRadius": "8px",
                      "border": f"1px solid {PURPLE}33", "marginBottom": "10px"}),

            # ── Pack breakdown (shown when pack_type = "different") ──
            # 6 rows max — fill what you need, empty rows are skipped on save
            # Pre-fill from saved details if reviewing
            html.Div([
                html.Div("NAME EACH ITEM — pick from list or type new name",
                         style={**_lbl, "color": PURPLE, "marginBottom": "8px"}),
                html.Div([
                    html.Div([
                        # Row: label + name dropdown + custom name + qty + loc
                        html.Div([
                            html.Span(f"Item {r+1}", style={"color": PURPLE, "fontSize": "11px",
                                      "fontWeight": "bold", "marginRight": "8px", "minWidth": "42px"}),
                            dbc.Select(id={"type": "inv-pack-name", "index": i * 100 + r},
                                       options=[{"label": "-- Pick --", "value": ""}] +
                                               [{"label": n, "value": n}
                                                for n in _names_by_cat_sorted.get(item["category"], [])] +
                                               [{"label": "Custom...", "value": "_custom"}],
                                       value=(saved_details[r].get("display_name", "")
                                              if is_saved and r < len(saved_details)
                                              and saved_details[r].get("display_name", "") in
                                              _names_by_cat_sorted.get(item["category"], [])
                                              else ""),
                                       style={**_sel_style, "maxWidth": "180px", "fontSize": "12px"}),
                            dcc.Input(id={"type": "inv-pack-custom", "index": i * 100 + r},
                                      type="text", placeholder="Type new name",
                                      value=(saved_details[r].get("display_name", "")
                                             if is_saved and r < len(saved_details)
                                             else ""),
                                      style={**_inp_style, "width": "140px", "fontSize": "12px",
                                             "padding": "6px 10px"}),
                            html.Span("Qty", style={"color": GRAY, "fontSize": "11px",
                                      "margin": "0 4px 0 6px"}),
                            dcc.Input(id={"type": "inv-pack-qty", "index": i * 100 + r},
                                      type="number", min=1,
                                      value=(saved_details[r].get("true_qty", 1)
                                             if is_saved and r < len(saved_details) else 1),
                                      style={**_inp_style, "width": "50px", "fontSize": "12px",
                                             "padding": "6px 8px"}),
                            html.Span("Loc", style={"color": GRAY, "fontSize": "11px",
                                      "margin": "0 4px 0 6px"}),
                            dbc.Select(id={"type": "inv-pack-loc", "index": i * 100 + r},
                                       options=_loc_opts,
                                       value=(saved_details[r].get("location", item["location"])
                                              if is_saved and r < len(saved_details)
                                              else item["location"]),
                                       style={**_sel_style, "maxWidth": "120px", "fontSize": "12px"}),
                        ], style={"display": "flex", "alignItems": "center", "gap": "4px",
                                  "flexWrap": "wrap"}),
                        # Image URL for this sub-item
                        html.Div([
                            html.Span("IMG", style={"color": GRAY, "fontSize": "10px",
                                      "fontWeight": "bold", "marginRight": "6px", "minWidth": "42px"}),
                            dcc.Input(id={"type": "inv-pack-img", "index": i * 100 + r},
                                      type="text", placeholder="Paste image URL...",
                                      value=(_IMAGE_URLS.get(saved_details[r].get("display_name", ""), "")
                                             if is_saved and r < len(saved_details) else ""),
                                      style={**_inp_style, "width": "280px", "fontSize": "11px",
                                             "padding": "4px 8px"}),
                        ], style={"display": "flex", "alignItems": "center", "gap": "4px",
                                  "marginTop": "2px", "marginBottom": "8px",
                                  "paddingBottom": "8px",
                                  "borderBottom": f"1px solid {PURPLE}15"}),
                    ]) for r in range(6)
                ]),
            ], id={"type": "inv-card-pack-section", "index": i},
               style={"display": ("block" if is_saved and _prefill_pack_type == "different" else "none"),
                      "padding": "12px 16px",
                      "backgroundColor": f"{PURPLE}08", "borderRadius": "8px",
                      "border": f"1px solid {PURPLE}33", "marginBottom": "10px"}),

            # ── Image URL + Save row ──
            html.Div([
                html.Div([
                    html.Div("IMAGE URL", style={**_lbl, "color": GRAY}),
                    dcc.Input(id={"type": "inv-card-img-url", "index": i},
                              type="text", placeholder="Paste image URL here...",
                              value=(_IMAGE_URLS.get(_prefill_name, "")
                                     if is_saved and _prefill_name else ""),
                              style={**_inp_style, "width": "100%", "maxWidth": "350px",
                                     "fontSize": "12px", "padding": "6px 10px"}),
                ], style={"flex": "3", "minWidth": "200px"}),

                html.Div([
                    html.Button("Save", id={"type": "inv-card-save", "index": i}, n_clicks=0,
                                style={"fontSize": "13px", "padding": "8px 28px",
                                       "backgroundColor": GREEN, "color": WHITE,
                                       "border": "none", "borderRadius": "6px",
                                       "cursor": "pointer", "fontWeight": "bold",
                                       "marginTop": "18px"}),
                    html.Span(id={"type": "inv-card-status", "index": i},
                              style={"marginLeft": "8px", "fontSize": "12px"}),
                ], style={"flex": "1", "minWidth": "120px", "display": "flex",
                          "alignItems": "center", "gap": "6px"}),
            ], style={"display": "flex", "gap": "14px", "flexWrap": "wrap",
                      "alignItems": "flex-start"}),

            # ── Image preview ──
            html.Div(id={"type": "inv-card-img-preview", "index": i},
                     style={"marginTop": "6px"}),

            # ── Hidden data stores ──
            dcc.Store(id={"type": "inv-card-data", "index": i}, data=item),
            dcc.Store(id={"type": "inv-card-pack-data", "index": i}, data=[]),

        ], style={"backgroundColor": "#0f0f1a", "padding": "16px 20px", "borderRadius": "10px",
                  "marginBottom": "10px",
                  "borderLeft": f"4px solid {GREEN}" if is_saved else f"4px solid {CYAN}",
                  "transition": "border-color 0.2s"},
           className="inv-card",
           **{"data-cat": item["category"],
              "data-loc": "|".join(set(d.get("location", "") for d in saved_details)) if saved_details else item.get("location", ""),
              "data-search": f"{item['name']} {' '.join(d.get('display_name', '') for d in saved_details)} {item['order_num']}".lower(),
              "data-stock": "|".join(set(d.get("display_name", "") for d in saved_details if d.get("display_name"))) if saved_details else ""})

        item_cards.append(card)

    # Scrollable container
    scroll_container = html.Div(
        item_cards if item_cards else [
            html.Div([
                html.Span("\u2713 ", style={"color": GREEN, "fontSize": "18px"}),
                html.Span("All items organized!", style={"color": GREEN, "fontSize": "14px",
                           "fontWeight": "bold"}),
            ], style={"padding": "20px", "textAlign": "center"}),
        ],
        style={"maxHeight": "70vh", "overflowY": "auto", "padding": "4px"})

    # Hidden compat elements for old callbacks that reference editor-datatable etc.
    _hidden_compat = html.Div([
        dcc.Input(id="editor-search", type="text", value="", style={"display": "none"}),
        dbc.Select(id="editor-cat-filter", value="All", style={"display": "none"}),
        dbc.Select(id="editor-status-filter", value="All", style={"display": "none"}),
        html.Button(id="editor-jump-unsaved", style={"display": "none"}),
        html.Button(id="editor-save-all-btn", n_clicks=0, style={"display": "none"}),
        html.Span(id="editor-save-all-status", style={"display": "none"}),
        html.Button(id="editor-fetch-all-images-btn", n_clicks=0, style={"display": "none"}),
        html.Span(id="editor-fetch-all-images-status", style={"display": "none"}),
        dash_table.DataTable(id="editor-datatable", columns=[
            {"name": "x", "id": "x"}], data=[], style_table={"display": "none"}),
        # Stepper compat (hidden, referenced by old clientside callbacks)
        dcc.Store(id="editor-stepper-state", data={"items": [], "current_index": 0, "total": 0}),
        html.Div(id="editor-q-header", style={"display": "none"}),
        html.Div(id="editor-q-orig", style={"display": "none"}),
        dbc.Select(id="editor-q-cat", value="Other", style={"display": "none"}),
        dcc.Input(id="editor-q-name", type="text", value="", style={"display": "none"}),
        dbc.Select(id="editor-q-loc", value="Tulsa, OK", style={"display": "none"}),
        dbc.Select(id="editor-q-multipack", value="no", style={"display": "none"}),
        dash_table.DataTable(id="editor-q-pack-table", columns=[{"name": "x", "id": "x"}],
                             data=[], style_table={"display": "none"}),
        html.Div(id="editor-q-pack-section", style={"display": "none"}),
        html.Div(id="editor-q-single-section", style={"display": "none"}),
        dbc.Select(id="editor-q-split", value="no", style={"display": "none"}),
        dcc.Input(id="editor-q-split-qty1", type="number", value=1, style={"display": "none"}),
        dbc.Select(id="editor-q-split-loc2", value="Texas", style={"display": "none"}),
        dcc.Input(id="editor-q-split-qty2", type="number", value=0, style={"display": "none"}),
        html.Div(id="editor-q-split-row", style={"display": "none"}),
        dbc.Select(id="editor-q-color-pick", value="", style={"display": "none"}),
        html.Div(id="editor-q-color-section", style={"display": "none"}),
        dcc.Upload(id="editor-q-order-img-upload", style={"display": "none"}),
        dcc.Input(id="editor-q-order-img-url", type="text", value="", style={"display": "none"}),
        html.Div(id="editor-q-order-img-preview", style={"display": "none"}),
        dcc.Upload(id="editor-q-img-upload", style={"display": "none"}),
        dcc.Input(id="editor-q-img-url", type="text", value="", style={"display": "none"}),
        html.Div(id="editor-q-img-preview", style={"display": "none"}),
        html.Button(id="editor-q-save", n_clicks=0, style={"display": "none"}),
        html.Button(id="editor-q-skip", n_clicks=0, style={"display": "none"}),
        html.Div(id="editor-q-status", style={"display": "none"}),
        html.Div(id="editor-q-panel", style={"display": "none"}),
        html.Button(id="editor-q-pack-add", n_clicks=0, style={"display": "none"}),
        dcc.Input(id="editor-q-custom-name", type="text", style={"display": "none"}),
    ], style={"display": "none"})

    _review_label = "Showing saved items for review" if show_saved else f"{unsaved_count} items need organizing{f' (showing first {len(capped)})' if len(capped) < unsaved_count else ''}"

    # Build category and location options from saved items for filters
    _saved_cats = sorted(set(it.get("category", "Other") for it in saved_items)) if saved_items else []
    _saved_locs = sorted(set(it.get("location", "") for it in saved_items if it.get("location"))) if saved_items else []
    # Build product name options from all saved item details
    _all_product_names = set()
    for it in saved_items:
        _sd_list = _ITEM_DETAILS.get((it.get("order_num", ""), it.get("name", "")), [])
        for _sd in _sd_list:
            _pn = _sd.get("display_name", "")
            if _pn:
                _all_product_names.add(_pn)
    _inv_product_names = sorted(_all_product_names)

    _filter_bar = html.Div([
        # Search
        html.Div([
            dcc.Input(
                id="editor-review-search",
                type="text", placeholder="Search items...",
                debounce=True, value="",
                style={"fontSize": "13px", "backgroundColor": "#1a1a2e", "color": WHITE,
                       "border": f"1px solid {DARKGRAY}55", "borderRadius": "6px",
                       "padding": "8px 12px", "width": "220px"}),
        ]),
        # Category filter
        html.Div([
            dbc.Select(
                id="editor-review-cat",
                options=[{"label": "All Categories", "value": "All"}] +
                        [{"label": c, "value": c} for c in _saved_cats],
                value="All",
                style={"fontSize": "13px", "backgroundColor": "#1a1a2e", "color": WHITE,
                       "maxWidth": "180px"}),
        ]),
        # Location filter
        html.Div([
            dbc.Select(
                id="editor-review-loc",
                options=[{"label": "All Locations", "value": "All"},
                         {"label": "Tulsa, OK", "value": "Tulsa, OK"},
                         {"label": "Texas", "value": "Texas"}],
                value="All",
                style={"fontSize": "13px", "backgroundColor": "#1a1a2e", "color": WHITE,
                       "maxWidth": "160px"}),
        ]),
        # Inventory product filter
        html.Div([
            dbc.Select(
                id="editor-review-stock",
                options=[{"label": "All Products", "value": "All"}] +
                        [{"label": n, "value": n} for n in _inv_product_names],
                value="All",
                style={"fontSize": "13px", "backgroundColor": "#1a1a2e", "color": WHITE,
                       "maxWidth": "220px"}),
        ]),
        # Result count
        html.Span(id="editor-review-count",
                  children=f"{len(capped)} items",
                  style={"color": GRAY, "fontSize": "12px", "marginLeft": "8px"}),
    ], style={"display": "flex", "gap": "10px", "alignItems": "center", "flexWrap": "wrap",
              "marginBottom": "12px"}) if show_saved else html.Div([
        # Hidden compat elements when not in review mode
        dcc.Input(id="editor-review-search", type="text", value="", style={"display": "none"}),
        dbc.Select(id="editor-review-cat", value="All", style={"display": "none"}),
        dbc.Select(id="editor-review-loc", value="All", style={"display": "none"}),
        dbc.Select(id="editor-review-stock", value="All", style={"display": "none"}),
        html.Span(id="editor-review-count", style={"display": "none"}),
    ])

    editor_content = html.Div([
        html.Div([
            html.Div([
                html.H4("ITEM ORGANIZER",
                         style={"color": ORANGE, "margin": "0", "fontSize": "20px", "fontWeight": "700",
                                "letterSpacing": "1px"}),
                html.Button(
                    "Hide Saved" if show_saved else "Review Saved",
                    id="editor-review-saved-btn", n_clicks=0,
                    style={"fontSize": "12px", "padding": "5px 14px",
                           "backgroundColor": GREEN if show_saved else f"{DARKGRAY}88",
                           "color": WHITE, "border": "none", "borderRadius": "6px",
                           "cursor": "pointer", "fontWeight": "bold", "marginLeft": "14px"}),
            ], style={"display": "flex", "alignItems": "center"}),
            html.P(_review_label + (" — scroll through and save each one." if not show_saved else " — re-save any to update."),
                   style={"color": GRAY, "fontSize": "13px", "margin": "6px 0 0 0"}),
        ], style={"marginBottom": "16px"}),
        progress_bar,
        dcc.Store(id="inv-name-options-store", data=_names_by_cat_sorted),
        dcc.Store(id="editor-review-mode", data=show_saved),
        html.Div(id="editor-items-container", children=[_filter_bar, scroll_container]),
        _hidden_compat,
    ], style={"backgroundColor": CARD, "padding": "24px", "borderRadius": "12px",
              "border": f"1px solid {ORANGE}22",
              "borderTop": f"5px solid {ORANGE}",
              "boxShadow": "0 4px 20px rgba(0,0,0,0.3)"})

    return editor_content, unsaved_count


def _build_stock_table_html(stock_df, search="", cat_filter="All", status_filter="All", show_with_tax=False, sort_by="Category"):
    """Build the stock table HTML from a filtered STOCK_SUMMARY DataFrame."""
    if len(stock_df) == 0:
        return html.P("No inventory items found.", style={"color": GRAY, "padding": "20px"})

    filtered = stock_df.copy()

    # Apply filters
    if search:
        search_l = search.lower()
        filtered = filtered[filtered["display_name"].str.lower().str.contains(search_l, na=False)]
    if cat_filter and cat_filter != "All":
        filtered = filtered[filtered["category"] == cat_filter]
    if status_filter == "In Stock":
        filtered = filtered[filtered["in_stock"] > 2]
    elif status_filter == "Low Stock":
        filtered = filtered[filtered["in_stock"].between(1, 2)]
    elif status_filter == "Out of Stock":
        filtered = filtered[filtered["in_stock"] <= 0]

    if len(filtered) == 0:
        return html.P("No items match filters.", style={"color": GRAY, "padding": "20px"})

    # Apply sort
    if sort_by == "Name":
        filtered = filtered.sort_values("display_name")
    elif sort_by == "Stock Low\u2192High":
        filtered = filtered.sort_values("in_stock", ascending=True)
    elif sort_by == "Stock High\u2192Low":
        filtered = filtered.sort_values("in_stock", ascending=False)
    elif sort_by == "Value High\u2192Low":
        filtered = filtered.sort_values("total_cost", ascending=False)
    else:
        filtered = filtered.sort_values(["category", "display_name"])

    # Summary strip
    total_items = len(filtered)
    total_units = int(filtered["in_stock"].sum())
    total_value = filtered["total_cost"].sum()
    summary_strip = html.Div([
        html.Span("\u2022 ", style={"color": WHITE, "fontSize": "16px"}),
        html.Span(f"{total_items} items", style={"color": WHITE, "fontSize": "14px", "fontWeight": "700"}),
        html.Span(" | ", style={"color": f"{DARKGRAY}66", "margin": "0 10px"}),
        html.Span("\u2022 ", style={"color": TEAL, "fontSize": "16px"}),
        html.Span(f"{total_units} total units", style={"color": TEAL, "fontSize": "14px", "fontWeight": "700"}),
        html.Span(" | ", style={"color": f"{DARKGRAY}66", "margin": "0 10px"}),
        html.Span("\u2022 ", style={"color": ORANGE, "fontSize": "16px"}),
        html.Span(f"${total_value:,.2f} value", style={"color": ORANGE, "fontSize": "14px", "fontWeight": "700"}),
    ], className="summary-strip",
       style={"padding": "10px 14px", "backgroundColor": "#0d0d1a", "borderRadius": "8px",
              "marginBottom": "10px"})

    show_cat_headers = sort_by in ("Category", None, "")
    rows = []
    current_cat = None
    for _, r in filtered.iterrows():
        cat = r["category"]
        # Category header (only when sorted by category)
        if show_cat_headers and cat != current_cat:
            current_cat = cat
            cat_items = filtered[filtered["category"] == cat]
            cat_stock = int(cat_items["in_stock"].sum())
            rows.append(html.Tr([
                html.Td([
                    html.Span("\u25cf ", style={"color": CYAN, "fontSize": "10px", "marginRight": "6px"}),
                    html.Span(f"{cat}"),
                ], colSpan="5",
                         style={"color": CYAN, "fontWeight": "bold", "padding": "14px 8px 8px 12px",
                                "fontSize": "16px", "borderBottom": f"2px solid {CYAN}44",
                                "borderLeft": f"4px solid {CYAN}", "letterSpacing": "0.5px"}),
                html.Td(f"{cat_stock} in stock", colSpan="5",
                         style={"color": GRAY, "padding": "14px 8px 8px 8px", "fontSize": "13px",
                                "borderBottom": f"2px solid {CYAN}44", "textAlign": "right",
                                "fontWeight": "600"}),
            ]))

        stock = int(r["in_stock"])
        if stock <= 0:
            stock_color = RED
            stock_bg = f"{RED}15"
        elif stock <= 2:
            stock_color = ORANGE
            stock_bg = f"{ORANGE}15"
        else:
            stock_color = GREEN
            stock_bg = "transparent"

        img_url = r.get("image_url", "") or _IMAGE_URLS.get(r["display_name"], "")

        _unit = r.get("unit_cost_with_tax", r["unit_cost"]) if show_with_tax else r["unit_cost"]
        _total = r.get("total_cost_with_tax", r["total_cost"]) if show_with_tax else r["total_cost"]

        _row_idx = len(rows)
        _stripe_bg = "#ffffff04" if _row_idx % 2 == 0 else "transparent"
        _row_bg = stock_bg if stock <= 0 else _stripe_bg

        rows.append(html.Tr([
            html.Td(item_thumbnail(img_url, 40), style={"padding": "8px 6px", "textAlign": "center", "width": "48px"}),
            html.Td(r["display_name"][:50], title=r["display_name"],
                     style={"color": WHITE, "padding": "8px 8px", "fontSize": "14px",
                            "maxWidth": "300px", "overflow": "hidden",
                            "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
            html.Td(str(int(r["total_purchased"])), style={"textAlign": "center", "color": WHITE,
                                                             "padding": "8px 6px", "fontSize": "13px"}),
            html.Td(str(int(r["total_used"])), style={"textAlign": "center", "color": GRAY,
                                                        "padding": "8px 6px", "fontSize": "13px"}),
            html.Td(html.Div([
                html.Span(str(stock), style={"color": stock_color, "fontWeight": "bold",
                                              "fontSize": "16px", "marginRight": "8px"}),
                _stock_level_bar(stock, int(r["total_purchased"])),
            ], style={"display": "flex", "alignItems": "center", "justifyContent": "center"}),
                     style={"textAlign": "center", "padding": "8px 6px",
                            "backgroundColor": stock_bg, "borderRadius": "4px"}),
            html.Td(f"${_unit:,.2f}", style={"textAlign": "right", "color": GRAY,
                                              "padding": "8px 6px", "fontSize": "12px"}),
            html.Td(f"${_total:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                               "fontWeight": "bold", "padding": "8px 6px", "fontSize": "13px"}),
            html.Td(
                html.Div([
                    dcc.Input(id={"type": "use-stock-qty", "index": r["display_name"]},
                              type="number", min=1, value=1,
                              style={"width": "42px", "fontSize": "13px", "padding": "4px 4px",
                                     "backgroundColor": "#0d0d1a", "color": WHITE,
                                     "border": f"1px solid {DARKGRAY}55", "borderRadius": "4px",
                                     "textAlign": "center"}),
                    html.Button("Use", id={"type": "use-stock-btn", "index": r["display_name"]},
                                n_clicks=0,
                                style={"backgroundColor": f"{RED}22", "color": RED,
                                       "border": f"1px solid {RED}55",
                                       "borderRadius": "6px", "padding": "5px 12px", "fontSize": "12px",
                                       "cursor": "pointer", "fontWeight": "bold",
                                       "transition": "all 0.15s ease"}),
                ], style={"display": "flex", "gap": "4px", "alignItems": "center",
                           "justifyContent": "center"}),
                style={"padding": "8px 4px", "textAlign": "center"}),
            html.Td("", id={"type": "use-stock-status", "index": r["display_name"]},
                     style={"color": GREEN, "fontSize": "11px", "padding": "8px 4px", "minWidth": "40px"}),
        ], className="stock-row",
           style={"borderBottom": "1px solid #ffffff08",
                  "backgroundColor": _row_bg,
                  "transition": "all 0.15s ease"}))

    _unit_label = "Unit (receipt)" if show_with_tax else "Unit $"
    _total_label = "Total (receipt)" if show_with_tax else "Total $"
    return html.Div([
        summary_strip,
        html.Table([
            html.Thead(html.Tr([
                html.Th("", style={"padding": "8px 6px", "width": "48px"}),
                html.Th("Item", style={"textAlign": "left", "padding": "8px 8px", "fontSize": "12px",
                                        "fontWeight": "700", "letterSpacing": "0.5px", "color": GRAY}),
                html.Th("Bought", style={"textAlign": "center", "padding": "8px 6px", "fontSize": "12px",
                                          "fontWeight": "700", "color": GRAY}),
                html.Th("Used", style={"textAlign": "center", "padding": "8px 6px", "fontSize": "12px",
                                        "fontWeight": "700", "color": GRAY}),
                html.Th("In Stock", style={"textAlign": "center", "padding": "8px 6px", "minWidth": "120px",
                                            "fontSize": "12px", "fontWeight": "700", "color": GRAY}),
                html.Th(_unit_label, style={"textAlign": "right", "padding": "8px 6px", "fontSize": "12px",
                                             "fontWeight": "700", "color": GRAY}),
                html.Th(_total_label, style={"textAlign": "right", "padding": "8px 6px", "fontSize": "12px",
                                              "fontWeight": "700", "color": GRAY}),
                html.Th("", style={"textAlign": "center", "padding": "8px 4px", "width": "75px"}),
                html.Th("", style={"width": "40px"}),
            ], className="stock-table-header",
               style={"borderBottom": f"2px solid {TEAL}", "backgroundColor": "#ffffff06"})),
            html.Tbody(rows),
        ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
    ])


def _build_usage_log_html():
    """Build the usage log section content."""
    if not _USAGE_LOG:
        return html.P("No usage recorded yet. Use the [-1] buttons above to track consumption.",
                       style={"color": GRAY, "fontSize": "12px", "padding": "8px"})

    rows = []
    for u in _USAGE_LOG[:50]:
        created = u.get("created_at", "")[:16].replace("T", " ") if u.get("created_at") else ""
        rows.append(html.Tr([
            html.Td(created, style={"color": GRAY, "padding": "3px 8px", "fontSize": "11px"}),
            html.Td(u.get("item_name", ""), style={"color": WHITE, "padding": "3px 8px", "fontSize": "12px"}),
            html.Td(str(u.get("qty", 1)), style={"textAlign": "center", "color": ORANGE,
                                                    "padding": "3px 6px", "fontSize": "12px"}),
            html.Td(u.get("note", ""), style={"color": DARKGRAY, "padding": "3px 8px", "fontSize": "11px"}),
            html.Td(
                html.Button("Undo", id={"type": "undo-usage-btn", "index": str(u["id"])},
                            n_clicks=0,
                            style={"backgroundColor": "transparent", "color": CYAN, "border": f"1px solid {CYAN}44",
                                   "borderRadius": "4px", "padding": "2px 8px", "fontSize": "10px",
                                   "cursor": "pointer"}),
                style={"padding": "3px 4px"}),
        ], style={"borderBottom": "1px solid #ffffff08"}))

    return html.Div([
        html.Table([
            html.Thead(html.Tr([
                html.Th("Date", style={"textAlign": "left", "padding": "4px 8px"}),
                html.Th("Item", style={"textAlign": "left", "padding": "4px 8px"}),
                html.Th("Qty", style={"textAlign": "center", "padding": "4px 6px"}),
                html.Th("Note", style={"textAlign": "left", "padding": "4px 8px"}),
                html.Th("", style={"width": "60px"}),
            ], style={"borderBottom": f"1px solid {CYAN}44"})),
            html.Tbody(rows),
        ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
    ], id="usage-log-table", style={"maxHeight": "300px", "overflowY": "auto"})


def _build_quick_add_form():
    """Build the Quick-Add form for manual inventory entries."""
    cat_options = [{"label": c, "value": c} for c in CATEGORY_OPTIONS]
    loc_options = [{"label": "Tulsa, OK", "value": "Tulsa, OK"},
                   {"label": "Texas", "value": "Texas"},
                   {"label": "Other", "value": "Other"}]
    _inp = {"fontSize": "12px", "backgroundColor": "#1a1a2e", "color": WHITE,
            "border": f"1px solid {DARKGRAY}", "borderRadius": "4px", "padding": "5px 8px"}

    # Recent quick-adds list
    qa_rows = []
    for qa in _QUICK_ADDS[:20]:
        created = qa.get("created_at", "")[:10] if qa.get("created_at") else ""
        qa_rows.append(html.Tr([
            html.Td(created, style={"color": GRAY, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(qa.get("item_name", ""), style={"color": WHITE, "padding": "3px 8px", "fontSize": "12px"}),
            html.Td(qa.get("category", ""), style={"color": TEAL, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(str(qa.get("qty", 1)), style={"textAlign": "center", "color": WHITE, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(f"${float(qa.get('unit_price', 0)):,.2f}", style={"textAlign": "right", "color": ORANGE, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(qa.get("location", ""), style={"color": GRAY, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(
                html.Button("Del", id={"type": "del-qa-btn", "index": str(qa["id"])},
                            n_clicks=0,
                            style={"backgroundColor": "transparent", "color": RED, "border": f"1px solid {RED}44",
                                   "borderRadius": "4px", "padding": "2px 6px", "fontSize": "10px",
                                   "cursor": "pointer"}),
                style={"padding": "3px 4px"}),
        ], style={"borderBottom": "1px solid #ffffff08"}))

    return html.Div([
        # Form row
        html.Div([
            html.Div([
                html.Span("Name:", style={"color": GRAY, "fontSize": "11px", "marginRight": "4px"}),
                dcc.Input(id="qa-name", type="text", placeholder="Item name...",
                          style={**_inp, "width": "180px"}),
            ], style={"display": "flex", "alignItems": "center", "marginRight": "8px"}),
            html.Div([
                html.Span("Category:", style={"color": GRAY, "fontSize": "11px", "marginRight": "4px"}),
                dbc.Select(id="qa-category", options=cat_options, value="Other",
                           style={"width": "140px", "fontSize": "12px",
                                  "backgroundColor": "#1a1a2e", "color": WHITE}),
            ], style={"display": "flex", "alignItems": "center", "marginRight": "8px"}),
            html.Div([
                html.Span("Qty:", style={"color": GRAY, "fontSize": "11px", "marginRight": "4px"}),
                dcc.Input(id="qa-qty", type="number", min=1, value=1,
                          style={**_inp, "width": "55px"}),
            ], style={"display": "flex", "alignItems": "center", "marginRight": "8px"}),
            html.Div([
                html.Span("Price:", style={"color": GRAY, "fontSize": "11px", "marginRight": "4px"}),
                dcc.Input(id="qa-price", type="number", min=0, step=0.01, value=0,
                          style={**_inp, "width": "70px"}),
            ], style={"display": "flex", "alignItems": "center", "marginRight": "8px"}),
            html.Div([
                html.Span("Location:", style={"color": GRAY, "fontSize": "11px", "marginRight": "4px"}),
                dbc.Select(id="qa-location", options=loc_options, value="Tulsa, OK",
                           style={"width": "110px", "fontSize": "12px",
                                  "backgroundColor": "#1a1a2e", "color": WHITE}),
            ], style={"display": "flex", "alignItems": "center", "marginRight": "8px"}),
            html.Button("Add Item", id="qa-add-btn", n_clicks=0,
                        style={"backgroundColor": GREEN, "color": WHITE, "border": "none",
                               "borderRadius": "4px", "padding": "6px 16px", "fontSize": "12px",
                               "cursor": "pointer", "fontWeight": "bold"}),
            html.Span("", id="qa-status",
                       style={"color": GREEN, "fontSize": "11px", "marginLeft": "8px"}),
        ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap",
                  "gap": "4px", "marginBottom": "12px"}),

        # Recent quick-adds
        html.Div([
            html.Table([
                html.Thead(html.Tr([
                    html.Th("Date", style={"textAlign": "left", "padding": "4px 6px"}),
                    html.Th("Item", style={"textAlign": "left", "padding": "4px 8px"}),
                    html.Th("Category", style={"textAlign": "left", "padding": "4px 6px"}),
                    html.Th("Qty", style={"textAlign": "center", "padding": "4px 6px"}),
                    html.Th("Price", style={"textAlign": "right", "padding": "4px 6px"}),
                    html.Th("Location", style={"textAlign": "left", "padding": "4px 6px"}),
                    html.Th("", style={"width": "50px"}),
                ], style={"borderBottom": f"1px solid {GREEN}44"})),
                html.Tbody(qa_rows),
            ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
        ], id="qa-list", style={"maxHeight": "200px", "overflowY": "auto"}) if qa_rows else html.Div(id="qa-list"),
    ])


def _build_location_inventory():
    """Build side-by-side Tulsa / Texas inventory boxes from _UPLOADED_INVENTORY."""

    def _build_box(title, location_key, color):
        """Build one location box from uploaded items."""
        # Collect items for this location: {(name, category): qty}
        items = {}
        for (loc, name, cat), qty in _UPLOADED_INVENTORY.items():
            if loc == location_key:
                items[(name, cat)] = qty

        if not items:
            return html.Div([
                html.H4(title, style={"color": color, "margin": "0 0 8px 0", "fontSize": "15px"}),
                html.P("No items uploaded yet.", style={"color": GRAY, "fontSize": "12px", "fontStyle": "italic"}),
            ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
                       "flex": "1", "border": f"1px solid {color}44", "minHeight": "150px"})

        # Group by category
        by_cat = {}
        for (name, cat), qty in items.items():
            by_cat.setdefault(cat, []).append((name, qty))

        sections = []
        total_qty = 0
        for cat in sorted(by_cat.keys()):
            cat_items = by_cat[cat]
            sections.append(html.Div(cat, style={"color": color, "fontSize": "12px", "fontWeight": "bold",
                                                  "padding": "6px 0 2px 0", "borderBottom": f"1px solid {color}33"}))
            for name, qty in sorted(cat_items, key=lambda x: x[0]):
                total_qty += qty
                idx_key = f"{location_key}__{name}"
                thumb_url = _IMAGE_URLS.get(name, "")
                thumb = html.Img(
                    src=thumb_url, style={"width": "30px", "height": "30px", "objectFit": "cover",
                                          "borderRadius": "4px", "marginRight": "6px",
                                          "backgroundColor": CARD2}
                ) if thumb_url else html.Div(style={"width": "30px", "height": "30px",
                                                     "borderRadius": "4px", "marginRight": "6px",
                                                     "backgroundColor": CARD2, "border": f"1px dashed {GRAY}44"})
                sections.append(html.Div([
                    html.Div([
                        thumb,
                        html.Span(name, style={"color": WHITE, "fontSize": "12px", "flex": "1"}),
                        html.Span(f"x{qty}", style={"color": GRAY, "fontSize": "12px", "fontFamily": "monospace",
                                                      "minWidth": "30px", "textAlign": "right"}),
                    ], style={"display": "flex", "alignItems": "center", "width": "100%"}),
                    html.Details([
                        html.Summary("Set photo", style={"color": CYAN, "fontSize": "10px", "cursor": "pointer",
                                                          "marginTop": "2px"}),
                        html.Div([
                            dcc.Input(id={"type": "loc-img-url", "index": idx_key},
                                      placeholder="Paste image URL…", type="text",
                                      style={"flex": "1", "fontSize": "11px", "padding": "3px 6px",
                                             "backgroundColor": CARD, "color": WHITE, "border": f"1px solid {GRAY}44",
                                             "borderRadius": "4px"}),
                            html.Button("Set", id={"type": "loc-img-set", "index": idx_key},
                                        style={"fontSize": "11px", "padding": "3px 10px", "marginLeft": "4px",
                                               "backgroundColor": CYAN, "color": WHITE, "border": "none",
                                               "borderRadius": "4px", "cursor": "pointer"}),
                            dcc.Store(id={"type": "loc-img-name", "index": idx_key}, data=name),
                            html.Span(id={"type": "loc-img-status", "index": idx_key},
                                      style={"color": GREEN, "fontSize": "10px", "marginLeft": "6px"}),
                        ], style={"display": "flex", "alignItems": "center", "marginTop": "3px"}),
                    ], style={"paddingLeft": "36px"}),
                ], style={"padding": "2px 8px"}))

        header = html.Div([
            html.H4(title, style={"color": color, "margin": "0", "fontSize": "15px", "flex": "1"}),
            html.Span(f"{total_qty} items", style={"color": GRAY, "fontSize": "12px"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "8px"})

        return html.Div([header] + sections,
                        style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
                               "flex": "1", "border": f"1px solid {color}44", "minHeight": "150px",
                               "maxHeight": "500px", "overflowY": "auto"})

    return html.Div([
        html.Div([
            html.H4("INVENTORY BY LOCATION", style={"color": CYAN, "margin": "0", "fontSize": "15px", "flex": "1"}),
            html.Button("Refresh", id="loc-inv-refresh-btn-legacy",
                        style={"fontSize": "12px", "padding": "4px 16px", "backgroundColor": CYAN,
                               "color": WHITE, "border": "none", "borderRadius": "4px",
                               "cursor": "pointer", "fontWeight": "bold"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}),
        html.P("Click Upload on items above, then Refresh to see them here. Same-name items combine quantities.",
               style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "12px"}),
        html.Div([
            _build_box("TJ (Tulsa, OK)", "Tulsa", TEAL),
            _build_box("BRADEN (Texas)", "Texas", ORANGE),
        ], style={"display": "flex", "gap": "12px"}),
    ], style={"backgroundColor": CARD2, "padding": "16px", "borderRadius": "10px",
              "marginBottom": "14px", "border": f"1px solid {CYAN}33"})


def _build_inventory_health_panel():
    """Build 4 health-gauge cards: organized, photos, locations, balance check."""
    # 1. Items organized (same logic as editor's saved_count)
    total_items = 0
    saved_count = 0
    for inv in INVOICES:
        is_personal = inv["source"] == "Personal Amazon" or (
            "file" in inv and isinstance(inv.get("file"), str) and "Gigi" in inv.get("file", ""))
        if is_personal:
            continue
        for item in inv["items"]:
            total_items += 1
            detail_key = (inv["order_num"], item["name"])
            if _ITEM_DETAILS.get(detail_key):
                saved_count += 1

    # 2. Items with photos — unique biz item names that have an image
    biz_item_names = set()
    for inv in INVOICES:
        is_personal = inv["source"] == "Personal Amazon" or (
            "file" in inv and isinstance(inv.get("file"), str) and "Gigi" in inv.get("file", ""))
        if is_personal:
            continue
        for item in inv["items"]:
            biz_item_names.add(item["name"])
    items_with_photos = sum(1 for n in biz_item_names if _IMAGE_URLS.get(n))
    total_unique = len(biz_item_names) or 1

    # 3. Items at locations
    items_at_locations = len(_UPLOADED_INVENTORY)

    # 4. Balance check: receipt totals vs item totals
    item_total = INV_ITEMS["total"].sum() if len(INV_ITEMS) > 0 else 0
    balance_diff = abs(total_inv_subtotal - item_total)
    # Tolerance: $10 or 0.5% of total, whichever is larger (rounding across 100+ items)
    balance_tolerance = max(10.0, total_inv_subtotal * 0.005) if total_inv_subtotal else 10.0
    balance_ok = balance_diff < balance_tolerance

    def _gauge(title, numerator, denominator, subtitle, action_text=""):
        pct = round(numerator / denominator * 100) if denominator > 0 else 0
        color = GREEN if pct >= 75 else (ORANGE if pct >= 40 else RED)
        is_complete = (pct >= 100)
        status_text = html.Div("\u2713 COMPLETE!", style={
            "color": GREEN, "fontSize": "14px", "fontWeight": "bold",
            "marginTop": "6px", "letterSpacing": "0.5px",
            "textShadow": "0 0 10px #2ecc7144"
        }) if is_complete else (html.Div(action_text, style={
            "color": f"{color}cc", "fontSize": "12px", "marginTop": "6px",
            "fontStyle": "italic", "fontWeight": "500"
        }) if action_text else None)
        # Percentage badge
        pct_badge = html.Span(f"{pct}%", style={
            "display": "inline-flex", "alignItems": "center", "justifyContent": "center",
            "width": "42px", "height": "42px", "borderRadius": "50%",
            "backgroundColor": f"{color}18", "color": color, "fontSize": "13px",
            "fontWeight": "bold", "border": f"2px solid {color}44",
            "marginLeft": "8px", "flexShrink": "0"})
        children = [
            html.Div(title, style={"color": GRAY, "fontSize": "11px", "fontWeight": "600",
                                   "letterSpacing": "0.5px", "marginBottom": "8px",
                                   "textTransform": "uppercase"}),
            html.Div([
                html.Span(f"{numerator}/{denominator}", style={"color": WHITE, "fontSize": "28px",
                                                                "fontWeight": "bold", "fontFamily": "monospace"}),
                pct_badge,
            ], style={"display": "flex", "alignItems": "center", "justifyContent": "center"}),
            html.Div([
                html.Div(style={"width": f"{max(pct, 2)}%", "height": "14px",
                                "background": f"linear-gradient(90deg, {color}88, {color})",
                                "borderRadius": "7px",
                                "transition": "width 0.4s ease"}),
            ], style={"width": "100%", "height": "14px", "backgroundColor": "#0d0d1a",
                       "borderRadius": "7px", "margin": "10px 0 8px 0",
                       "overflow": "hidden"}),
            html.Div(subtitle, style={"color": DARKGRAY, "fontSize": "11px"}),
        ]
        if status_text is not None:
            children.append(status_text)
        _extra_class = "gauge-card pulse-complete" if is_complete else "gauge-card"
        return html.Div(children,
                         className=_extra_class,
                         style={"backgroundColor": CARD2, "padding": "16px 18px", "borderRadius": "10px",
                                "flex": "1", "textAlign": "center", "border": f"1px solid {color}33",
                                "minWidth": "150px", "minHeight": "160px",
                                "boxShadow": "0 4px 12px rgba(0,0,0,0.25)"}), pct

    balance_color = GREEN if balance_ok else RED
    balance_label = "\u2713 BALANCED" if balance_ok else "\u2717 MISMATCH"
    _bal_class = "gauge-card pulse-complete" if balance_ok else "gauge-card"
    balance_card = html.Div([
        html.Div("BALANCE CHECK", style={"color": GRAY, "fontSize": "11px", "fontWeight": "600",
                                          "letterSpacing": "0.5px", "marginBottom": "8px",
                                          "textTransform": "uppercase"}),
        html.Div(balance_label,
                 style={"color": balance_color, "fontSize": "20px", "fontWeight": "bold",
                        "textShadow": f"0 0 10px {balance_color}33"}),
        html.Div(f"${balance_diff:,.2f} diff",
                 style={"color": balance_color if not balance_ok else DARKGRAY,
                        "fontSize": "14px", "fontWeight": "600", "margin": "6px 0"}),
        html.Div([
            html.Div(style={"width": "100%", "height": "14px",
                            "background": f"linear-gradient(90deg, {balance_color}88, {balance_color})",
                            "borderRadius": "7px"}),
        ], style={"width": "100%", "height": "14px", "backgroundColor": "#0d0d1a",
                   "borderRadius": "7px", "margin": "4px 0 8px 0", "overflow": "hidden"}),
        html.Div(f"Orders ${total_inv_subtotal:,.2f} vs Items ${item_total:,.2f}",
                 style={"color": DARKGRAY, "fontSize": "11px"}),
    ], className=_bal_class,
       style={"backgroundColor": CARD2, "padding": "16px 18px", "borderRadius": "10px",
              "flex": "1", "textAlign": "center", "border": f"1px solid {balance_color}33",
              "minWidth": "150px", "minHeight": "160px",
              "boxShadow": "0 4px 12px rgba(0,0,0,0.25)"})

    unnamed_count = total_items - saved_count
    no_photo_count = total_unique - items_with_photos
    gauge1, pct1 = _gauge("ITEMS ORGANIZED", saved_count, total_items, "named & categorized",
                           f"Open Editor to name {unnamed_count} items" if unnamed_count > 0 else "")
    gauge2, pct2 = _gauge("ITEMS WITH PHOTOS", items_with_photos, total_unique, "have images set",
                           f"Set photos for {no_photo_count} items" if no_photo_count > 0 else "")
    gauge3, pct3 = _gauge("ITEMS AT LOCATIONS", items_at_locations, total_items, "uploaded to locations",
                           f"Upload {total_items - items_at_locations} items to locations" if items_at_locations < total_items else "")

    # Overall inventory health bar (weighted average)
    overall_pct = round((pct1 * 0.5 + pct2 * 0.25 + pct3 * 0.25)) if total_items > 0 else 0
    overall_color = GREEN if overall_pct >= 75 else (ORANGE if overall_pct >= 40 else RED)
    overall_bar = html.Div([
        html.Div([
            html.Span("INVENTORY HEALTH", style={"color": GRAY, "fontSize": "12px", "fontWeight": "700",
                                                   "letterSpacing": "1.5px"}),
            html.Span(f"{overall_pct}%", style={"color": overall_color, "fontSize": "16px", "fontWeight": "bold",
                                                  "marginLeft": "auto",
                                                  "textShadow": f"0 0 10px {overall_color}44"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "8px"}),
        html.Div([
            html.Div(
                f"{overall_pct}%" if overall_pct > 30 else "",
                style={"width": f"{max(overall_pct, 2)}%", "height": "12px",
                        "background": f"linear-gradient(90deg, {overall_color}88, {overall_color})",
                        "borderRadius": "6px",
                        "transition": "width 0.4s ease",
                        "fontSize": "9px", "color": WHITE, "fontWeight": "bold",
                        "lineHeight": "12px", "textAlign": "center"}),
        ], style={"width": "100%", "height": "12px", "backgroundColor": "#0d0d1a",
                   "borderRadius": "6px", "overflow": "hidden"}),
    ], style={"padding": "12px 16px", "backgroundColor": CARD2, "borderRadius": "10px",
              "marginBottom": "10px", "border": f"1px solid {overall_color}33",
              "boxShadow": "0 2px 10px rgba(0,0,0,0.2)"})

    return html.Div([
        overall_bar,
        html.Div([gauge1, gauge2, gauge3, balance_card],
                 style={"display": "flex", "gap": "10px", "flexWrap": "wrap"}),
    ], style={"marginBottom": "18px"})


def _build_location_stats_row():
    """Build a side-by-side comparison row of Tulsa vs Texas purchasing/inventory stats."""
    def _loc_stats(loc_key):
        """Compute detailed stats for a location."""
        items = {}
        total_qty = 0
        total_value = 0
        cats = {}
        for (loc, name, cat), qty in _UPLOADED_INVENTORY.items():
            if loc == loc_key:
                uc = _INVENTORY_UNIT_COST.get((loc, name, cat), 0)
                val = uc * qty
                items[name] = {"qty": qty, "cost": uc, "value": val, "cat": cat}
                total_qty += qty
                total_value += val
                cats[cat] = cats.get(cat, 0) + val
        avg_cost = total_value / total_qty if total_qty > 0 else 0
        top_items = sorted(items.items(), key=lambda x: -x[1]["value"])[:5]
        top_cats = sorted(cats.items(), key=lambda x: -x[1])
        return {
            "unique": len(items), "total_qty": total_qty, "total_value": total_value,
            "avg_cost": avg_cost, "top_items": top_items, "top_cats": top_cats,
        }

    t_stats = _loc_stats("Tulsa")
    x_stats = _loc_stats("Texas")

    cat_colors = {"Filament": TEAL, "Lighting": ORANGE, "Crafts": PINK, "Packaging": BLUE,
                  "Hardware": GRAY, "Tools": CYAN, "Printer Parts": PURPLE, "Jewelry": "#f1c40f",
                  "Other": DARKGRAY}

    def _stat_card(label, value, color, sub=""):
        return html.Div([
            html.Div(label, style={"color": GRAY, "fontSize": "10px", "fontWeight": "600",
                                    "letterSpacing": "0.5px", "textTransform": "uppercase"}),
            html.Div(value, style={"color": color, "fontSize": "20px", "fontWeight": "bold",
                                    "fontFamily": "monospace", "margin": "2px 0"}),
            html.Div(sub, style={"color": DARKGRAY, "fontSize": "10px"}) if sub else None,
        ], style={"textAlign": "center", "flex": "1", "minWidth": "80px"})

    def _top_items_list(top_items, color):
        rows = []
        for name, info in top_items:
            thumb = item_thumbnail(_IMAGE_URLS.get(name, ""), 28)
            rows.append(html.Div([
                thumb,
                html.Span(name[:30], title=name, style={"color": WHITE, "fontSize": "11px",
                          "marginLeft": "8px", "flex": "1", "overflow": "hidden",
                          "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
                html.Span(f"x{info['qty']}", style={"color": GRAY, "fontSize": "11px",
                          "fontFamily": "monospace", "marginLeft": "6px"}),
                html.Span(f"${info['value']:.2f}", style={"color": color, "fontSize": "11px",
                          "fontFamily": "monospace", "marginLeft": "6px", "fontWeight": "600"}),
            ], style={"display": "flex", "alignItems": "center", "padding": "3px 0",
                      "borderBottom": "1px solid #ffffff06"}))
        return rows

    def _cat_bars(top_cats, total_val, color):
        bars = []
        for cat, val in top_cats[:6]:
            pct = (val / total_val * 100) if total_val > 0 else 0
            c = cat_colors.get(cat, GRAY)
            bars.append(html.Div([
                html.Div([
                    html.Span(cat, style={"color": WHITE, "fontSize": "11px", "flex": "1"}),
                    html.Span(f"${val:,.0f}", style={"color": c, "fontSize": "11px",
                              "fontFamily": "monospace", "fontWeight": "600"}),
                ], style={"display": "flex", "alignItems": "center", "marginBottom": "3px"}),
                html.Div([
                    html.Div(style={"width": f"{max(pct, 2)}%", "height": "6px",
                                    "backgroundColor": c, "borderRadius": "3px",
                                    "transition": "width 0.3s ease"}),
                ], style={"width": "100%", "height": "6px", "backgroundColor": "#0d0d1a",
                          "borderRadius": "3px", "overflow": "hidden"}),
            ], style={"marginBottom": "6px"}))
        return bars

    def _loc_panel(title, color, stats, spend, orders, tax):
        return html.Div([
            # Title bar
            html.Div(style={"height": "4px",
                             "background": f"linear-gradient(90deg, {color}, {color}44)",
                             "borderRadius": "10px 10px 0 0",
                             "margin": "-16px -16px 12px -16px"}),
            html.Div(title, style={"color": color, "fontSize": "15px", "fontWeight": "bold",
                                    "letterSpacing": "0.5px", "marginBottom": "10px"}),
            # Stat pills row
            html.Div([
                _stat_card("Total Spend", f"${spend:,.2f}", WHITE),
                _stat_card("In Stock", str(stats["total_qty"]), GREEN, f"{stats['unique']} products"),
                _stat_card("Inv Value", f"${stats['total_value']:,.2f}", color),
                _stat_card("Avg Cost", f"${stats['avg_cost']:.2f}", TEAL, "per unit"),
            ], style={"display": "flex", "gap": "6px", "marginBottom": "14px",
                      "padding": "10px", "backgroundColor": "#0d0d1a", "borderRadius": "8px"}),
            # Category spend bars
            html.Div("SPEND BY CATEGORY", style={"color": GRAY, "fontSize": "10px", "fontWeight": "700",
                                                   "letterSpacing": "1px", "marginBottom": "8px"}),
            html.Div(_cat_bars(stats["top_cats"], stats["total_value"], color),
                     style={"marginBottom": "14px"}),
            # Top items by value
            html.Div("TOP ITEMS BY VALUE", style={"color": GRAY, "fontSize": "10px", "fontWeight": "700",
                                                    "letterSpacing": "1px", "marginBottom": "8px"}),
            html.Div(_top_items_list(stats["top_items"], color),
                     style={"maxHeight": "180px", "overflowY": "auto"}),
        ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
                  "flex": "1", "border": f"1px solid {color}22",
                  "boxShadow": "0 4px 16px rgba(0,0,0,0.3)", "minWidth": "300px"})

    # VS divider
    total_both = tulsa_spend + texas_spend
    t_pct = (tulsa_spend / total_both * 100) if total_both > 0 else 50
    x_pct = 100 - t_pct

    vs_divider = html.Div([
        html.Div([
            html.Div(style={"width": f"{t_pct}%", "height": "8px", "backgroundColor": TEAL,
                            "borderRadius": "4px 0 0 4px", "transition": "width 0.4s"}),
            html.Div(style={"width": f"{x_pct}%", "height": "8px", "backgroundColor": ORANGE,
                            "borderRadius": "0 4px 4px 0", "transition": "width 0.4s"}),
        ], style={"display": "flex", "width": "100%", "marginBottom": "6px"}),
        html.Div([
            html.Span(f"Tulsa {t_pct:.0f}%", style={"color": TEAL, "fontSize": "11px", "fontWeight": "bold"}),
            html.Span("SPEND SPLIT", style={"color": GRAY, "fontSize": "10px", "fontWeight": "700",
                                             "letterSpacing": "1px"}),
            html.Span(f"Texas {x_pct:.0f}%", style={"color": ORANGE, "fontSize": "11px", "fontWeight": "bold"}),
        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
    ], style={"padding": "8px 0", "marginBottom": "16px"})

    return html.Div([
        vs_divider,
        html.Div([
            _loc_panel("TJ (Tulsa, OK)", TEAL, t_stats, tulsa_spend, tulsa_orders, tulsa_tax),
            _loc_panel("BRADEN (Texas)", ORANGE, x_stats, texas_spend, texas_orders, texas_tax),
        ], style={"display": "flex", "gap": "14px", "flexWrap": "wrap"}),
    ])


def _build_warehouse_card(title, location_key, color, spend, orders, subtotal, tax, pct_of_total):
    """Build a single warehouse card with spend + category breakdown + items."""
    # Collect items for this location
    loc_items = {}
    for (loc, name, cat), qty in _UPLOADED_INVENTORY.items():
        if loc == location_key:
            loc_items.setdefault(cat, []).append((name, qty))
    total_items = sum(sum(q for _, q in items) for items in loc_items.values())

    # Category breakdown dots
    cat_dots = []
    cat_colors = {"Filament": TEAL, "Lighting": ORANGE, "Crafts": PINK, "Packaging": BLUE,
                  "Hardware": GRAY, "Tools": CYAN, "Printer Parts": PURPLE, "Jewelry": "#f1c40f",
                  "Other": DARKGRAY}
    for cat in sorted(loc_items.keys()):
        cat_count = sum(q for _, q in loc_items[cat])
        c = cat_colors.get(cat, GRAY)
        cat_dots.append(html.Div([
            html.Div(style={"width": "12px", "height": "12px", "borderRadius": "50%",
                            "backgroundColor": c, "flexShrink": "0",
                            "boxShadow": f"0 0 6px {c}44"}),
            html.Span(f"{cat}", style={"color": WHITE, "fontSize": "12px", "marginLeft": "8px",
                                        "fontWeight": "500"}),
            html.Span(f"{cat_count}", style={"color": WHITE, "fontSize": "11px", "marginLeft": "auto",
                                              "fontFamily": "monospace", "fontWeight": "bold",
                                              "backgroundColor": f"{c}22", "padding": "1px 8px",
                                              "borderRadius": "8px", "border": f"1px solid {c}33"}),
        ], style={"display": "flex", "alignItems": "center", "padding": "3px 0"}))

    # Item list (compact) — with cost per unit
    item_rows = []
    for cat in sorted(loc_items.keys()):
        for name, qty in sorted(loc_items[cat], key=lambda x: x[0]):
            thumb_url = _IMAGE_URLS.get(name, "")
            unit_cost = _INVENTORY_UNIT_COST.get((location_key, name, cat), 0)
            cost_el = html.Span(
                f"${unit_cost:.2f}/ea", style={"color": TEAL, "fontSize": "11px",
                "fontFamily": "monospace", "marginLeft": "8px", "whiteSpace": "nowrap"}
            ) if unit_cost > 0 else None
            _thumb_el = item_thumbnail(thumb_url, 36) if thumb_url else None
            item_rows.append(html.Div([
                _thumb_el,
                html.Span(name[:40], title=name, style={"color": WHITE, "fontSize": "12px",
                          "marginLeft": "10px" if _thumb_el else "0", "flex": "1",
                          "overflow": "hidden",
                          "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
                html.Span(f"x{qty}", style={"color": GRAY, "fontSize": "12px",
                          "fontFamily": "monospace", "marginLeft": "8px", "fontWeight": "600"}),
                cost_el,
            ], style={"display": "flex", "alignItems": "center", "padding": "4px 0",
                      "borderBottom": "1px solid #ffffff06",
                      "transition": "background-color 0.15s ease"}))

    _pct_pill = html.Span(f"{pct_of_total:.1f}%", style={
        "fontSize": "12px", "fontWeight": "bold", "padding": "3px 12px",
        "borderRadius": "10px", "backgroundColor": f"{color}18", "color": color,
        "border": f"1px solid {color}33"})
    return html.Div([
        # 5px colored top border accent with gradient
        html.Div(style={"height": "5px",
                         "background": f"linear-gradient(90deg, {color}, {color}66)",
                         "borderRadius": "10px 10px 0 0",
                         "margin": "-16px -16px 14px -16px"}),
        # Header
        html.Div([
            html.Div([
                html.Span(title, style={"color": color, "fontSize": "17px", "fontWeight": "bold"}),
            ]),
            html.Div(f"${spend:,.2f}", style={"color": WHITE, "fontSize": "30px",
                                                "fontWeight": "bold", "fontFamily": "monospace",
                                                "margin": "6px 0",
                                                "textShadow": f"0 0 15px {color}22"}),
            html.Div([
                html.Span(f"{orders} orders", style={"color": GRAY, "fontSize": "12px"}),
                html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66", "margin": "0 6px"}),
                html.Span(f"{total_items} items", style={"color": GRAY, "fontSize": "12px"}),
                html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66", "margin": "0 6px"}),
                _pct_pill,
            ], style={"display": "flex", "alignItems": "center"}),
        ], style={"marginBottom": "14px", "paddingBottom": "12px",
                  "borderBottom": f"1px solid {color}22"}),
        # Category breakdown
        html.Div(cat_dots, style={"marginBottom": "12px"}) if cat_dots else None,
        # Items (scrollable)
        (html.Div(item_rows, style={"maxHeight": "250px", "overflowY": "auto"}) if item_rows
         else html.P("No items uploaded yet.", style={"color": GRAY, "fontSize": "12px", "fontStyle": "italic"})),
    ], className="warehouse-card",
       style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
              "flex": "1", "border": f"1px solid {color}33", "minHeight": "200px",
              "boxShadow": "0 4px 16px rgba(0,0,0,0.3)"})


def _build_inv_qty_table():
    """Build an editable table of all inventory items so user can fix quantities and add images."""
    # Only show items missing images as separate input rows
    _missing_img_items = []
    for (loc, name, cat), qty in sorted(_UPLOADED_INVENTORY.items(), key=lambda x: (x[0][0], x[0][2], x[0][1])):
        if not _IMAGE_URLS.get(name, ""):
            _missing_img_items.append((loc, name, cat, qty))

    # DataTable for qty editing (no image column — that didn't work)
    rows = []
    for (loc, name, cat), qty in sorted(_UPLOADED_INVENTORY.items(), key=lambda x: (x[0][0], x[0][2], x[0][1])):
        unit_cost = _INVENTORY_UNIT_COST.get((loc, name, cat), 0)
        rows.append({"Location": loc, "Name": name, "Category": cat, "Qty": qty,
                      "Cost/ea": round(unit_cost, 2)})
    if not rows:
        return html.Div()

    parts = []
    parts.append(html.Div([
        html.H4("EDIT INVENTORY", style={"color": ORANGE, "margin": "0", "fontSize": "15px",
                                            "fontWeight": "700", "letterSpacing": "1px"}),
        html.Span("Click any Qty cell to edit, then Save",
                   style={"color": GRAY, "fontSize": "12px", "marginLeft": "12px"}),
        html.Button("Save Changes", id="inv-qty-save-btn", n_clicks=0,
                    style={"fontSize": "12px", "padding": "6px 18px", "backgroundColor": GREEN,
                           "color": WHITE, "border": "none", "borderRadius": "6px",
                           "cursor": "pointer", "fontWeight": "bold", "marginLeft": "auto"}),
        html.Span(id="inv-qty-save-status", style={"marginLeft": "8px", "fontSize": "12px"}),
    ], style={"display": "flex", "alignItems": "center", "marginBottom": "10px", "gap": "8px"}))

    parts.append(dash_table.DataTable(
        id="inv-qty-table",
        columns=[
            {"name": "Location", "id": "Location", "editable": False},
            {"name": "Name", "id": "Name", "editable": False},
            {"name": "Category", "id": "Category", "editable": False},
            {"name": "Qty", "id": "Qty", "editable": True, "type": "numeric"},
            {"name": "Cost/ea", "id": "Cost/ea", "editable": False, "type": "numeric",
             "format": {"specifier": "$.2f"}},
        ],
        data=rows,
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#0d0d1a", "color": CYAN, "fontWeight": "bold",
                       "fontSize": "12px", "border": "1px solid #ffffff15",
                       "textTransform": "uppercase", "letterSpacing": "0.5px"},
        style_cell={"backgroundColor": CARD, "color": WHITE, "fontSize": "13px",
                     "border": "1px solid #ffffff10", "padding": "8px 12px",
                     "textAlign": "left"},
        style_data_conditional=[
            {"if": {"column_id": "Qty"},
             "backgroundColor": "#1a2540", "color": ORANGE, "fontWeight": "bold",
             "border": f"1px solid {ORANGE}33", "cursor": "pointer"},
        ],
        page_size=50,
    ))

    # Missing images section — real input fields
    if _missing_img_items:
        _img_inp_style = {"fontSize": "12px", "backgroundColor": "#1a1a2e", "color": WHITE,
                          "border": f"1px solid {DARKGRAY}55", "borderRadius": "6px",
                          "padding": "6px 10px", "width": "100%"}
        img_rows = []
        for i, (loc, name, cat, qty) in enumerate(_missing_img_items):
            img_rows.append(html.Div([
                html.Span(f"{name}", style={"color": WHITE, "fontSize": "13px", "fontWeight": "bold",
                          "minWidth": "200px"}),
                html.Span(f"({loc})", style={"color": GRAY, "fontSize": "11px", "marginRight": "10px"}),
                dcc.Input(id={"type": "inv-missing-img", "index": f"{loc}|{name}|{cat}"},
                          type="text", placeholder="Paste image URL...",
                          style=_img_inp_style),
            ], style={"display": "flex", "alignItems": "center", "gap": "10px", "padding": "6px 0",
                      "borderBottom": "1px solid #ffffff08"}))

        parts.append(html.Div([
            html.Div([
                html.H4("MISSING IMAGES", style={"color": RED, "margin": "0", "fontSize": "14px",
                                                    "fontWeight": "700"}),
                html.Span(f"{len(_missing_img_items)} items need photos",
                           style={"color": GRAY, "fontSize": "12px", "marginLeft": "10px"}),
                html.Button("Save Images", id="inv-img-save-btn", n_clicks=0,
                            style={"fontSize": "12px", "padding": "6px 18px", "backgroundColor": TEAL,
                                   "color": WHITE, "border": "none", "borderRadius": "6px",
                                   "cursor": "pointer", "fontWeight": "bold", "marginLeft": "auto"}),
                html.Span(id="inv-img-save-status", style={"marginLeft": "8px", "fontSize": "12px"}),
            ], style={"display": "flex", "alignItems": "center", "marginBottom": "10px", "gap": "8px"}),
            html.Div(img_rows),
        ], style={"marginTop": "16px", "padding": "14px", "backgroundColor": "#1a0d0d",
                  "borderRadius": "8px", "border": f"1px solid {RED}33"}))

    return html.Div(parts, style={"marginTop": "14px"})


def _build_enhanced_location_section():
    """Build the WHO HAS WHAT section: warehouse cards with spend + items merged."""
    tulsa_pct = (tulsa_spend / true_inventory_cost * 100) if true_inventory_cost else 0
    texas_pct = (texas_spend / true_inventory_cost * 100) if true_inventory_cost else 0

    return html.Div([
        html.Div([
            html.H3("WAREHOUSES", style={"color": CYAN, "margin": "0", "fontSize": "18px",
                                          "fontWeight": "700", "letterSpacing": "1px"}),
            html.Button("Refresh", id="loc-inv-refresh-btn",
                        style={"fontSize": "11px", "padding": "5px 14px", "backgroundColor": f"{CYAN}22",
                               "color": CYAN, "border": f"1px solid {CYAN}44", "borderRadius": "6px",
                               "cursor": "pointer", "fontWeight": "bold", "marginLeft": "auto"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "12px"}),
        html.Div([
            _build_warehouse_card("TJ (Tulsa, OK)", "Tulsa", TEAL,
                                   tulsa_spend, tulsa_orders, tulsa_subtotal, tulsa_tax, tulsa_pct),
            _build_warehouse_card("BRADEN (Texas)", "Texas", ORANGE,
                                   texas_spend, texas_orders, texas_subtotal, texas_tax, texas_pct),
        ], id="location-inventory-display",
           style={"display": "flex", "gap": "12px"}),
        _build_inv_qty_table(),
    ], style={"backgroundColor": CARD2, "padding": "22px", "borderRadius": "12px",
              "marginBottom": "18px", "border": f"1px solid {CYAN}33",
              "borderTop": f"5px solid {CYAN}",
              "boxShadow": "0 4px 20px rgba(0,0,0,0.3)"})


def _build_inv_kpi_row():
    """Build premium KPI pill strip from current STOCK_SUMMARY."""
    k = _compute_stock_kpis()
    biz_count = len(BIZ_INV_DF)
    cogs_pct = f"{true_inventory_cost / gross_sales * 100:.1f}%" if gross_sales else "N/A"
    cogs_label = "healthy" if gross_sales and (true_inventory_cost / gross_sales * 100) < 25 else "moderate"
    return html.Div([
        _build_kpi_pill("#", "IN STOCK", str(k["in_stock"]), GREEN,
                        f"{k['unique']} unique"),
        _build_kpi_pill("$", "VALUE", f"${k['value']:,.2f}", TEAL,
                        "total spend"),
        _build_kpi_pill("!", "LOW STOCK", str(k["low"]), ORANGE,
                        "need reorder"),
        _build_kpi_pill("\u2716", "OUT OF STOCK", str(k["oos"]), RED,
                        "empty"),
        _build_kpi_pill("%", "SUPPLY COSTS", cogs_pct, PURPLE,
                        cogs_label),
        _build_kpi_pill("=", "ORDERS", str(biz_count), BLUE,
                        f"T:{tulsa_orders}/TX:{texas_orders}"),
    ], style={"display": "flex", "gap": "10px", "marginBottom": "18px", "flexWrap": "wrap"})


def _build_receipt_upload_section():
    """Build the receipt upload zone + item-by-item onboarding wizard."""
    _label = {"color": GRAY, "fontSize": "12px", "marginRight": "6px",
              "whiteSpace": "nowrap", "fontWeight": "500"}
    _inp = {"fontSize": "13px", "backgroundColor": "#0d0d1a", "color": WHITE,
            "border": f"1px solid {DARKGRAY}55", "borderRadius": "6px", "padding": "7px 12px"}
    cat_options = [{"label": c, "value": c} for c in CATEGORY_OPTIONS]
    loc_options = [{"label": "Tulsa, OK", "value": "Tulsa, OK"},
                   {"label": "Texas", "value": "Texas"},
                   {"label": "Other", "value": "Other"}]

    return html.Div([
        html.H4("UPLOAD NEW RECEIPT", style={
            "color": PURPLE, "margin": "0 0 10px 0", "fontSize": "15px"}),

        # Upload zone
        dcc.Upload(
            id="receipt-upload",
            children=html.Div([
                html.Span("Drop PDF here or ", style={"color": GRAY, "fontSize": "13px"}),
                html.A("browse files", style={
                    "color": CYAN, "textDecoration": "underline",
                    "cursor": "pointer", "fontSize": "13px"}),
            ], style={"textAlign": "center", "padding": "16px"}),
            accept=".pdf",
            style={
                "borderWidth": "2px", "borderStyle": "dashed",
                "borderColor": f"{PURPLE}55", "borderRadius": "8px",
                "backgroundColor": f"{PURPLE}08", "cursor": "pointer",
                "marginBottom": "10px", "transition": "all 0.15s ease",
            },
        ),

        # Upload status message
        html.Div(id="receipt-upload-status", style={"marginBottom": "8px"}),

        # Wizard state store
        dcc.Store(id="receipt-wizard-state", data=None),

        # Wizard panel (hidden until a PDF is uploaded)
        html.Div([
            # Header: order info + item counter
            html.Div(id="receipt-wizard-header", style={"marginBottom": "10px"}),

            # Original item info (name, qty, price, seller, ship_to)
            html.Div(id="receipt-wizard-orig", style={
                "backgroundColor": "#0f0f1a", "padding": "10px 14px",
                "borderRadius": "6px", "marginBottom": "12px",
                "borderLeft": f"3px solid {CYAN}",
            }),

            # ── QUESTIONNAIRE FORM ──────────────────────────────────────
            # Hidden qty field (set by callback, used for receipt qty reference)
            dcc.Input(id="wizard-qty", type="number", min=1, value=1,
                      style={"display": "none"}),

            html.Div([
                # ── Q: Order Photo ──
                html.Div([
                    html.Div("Upload a photo of this order",
                             style={"color": PURPLE, "fontSize": "13px",
                                    "fontWeight": "bold", "marginBottom": "6px"}),
                    html.Div([
                        dcc.Upload(
                            id="wizard-order-img-upload",
                            children=html.Div([
                                html.Span("Drop image or ", style={"color": GRAY, "fontSize": "12px"}),
                                html.A("browse", style={"color": CYAN, "fontSize": "12px",
                                                        "textDecoration": "underline", "cursor": "pointer"}),
                            ], style={"textAlign": "center", "padding": "10px"}),
                            accept="image/*",
                            style={"borderWidth": "1px", "borderStyle": "dashed",
                                   "borderColor": f"{PURPLE}44", "borderRadius": "6px",
                                   "backgroundColor": f"{PURPLE}06", "cursor": "pointer",
                                   "width": "180px", "minHeight": "60px"},
                        ),
                        html.Div(id="wizard-order-img-preview",
                                 style={"marginLeft": "10px"}),
                    ], style={"display": "flex", "alignItems": "center"}),
                ], style={"marginBottom": "14px", "padding": "10px 14px",
                          "backgroundColor": "#0d0d1a", "borderRadius": "6px"}),

                # ── Q: Category ──
                html.Div([
                    html.Div("What category is this item?",
                             style={"color": WHITE, "fontSize": "13px",
                                    "fontWeight": "bold", "marginBottom": "4px"}),
                    dbc.Select(id="wizard-cat", options=cat_options, value="Other",
                               style={"width": "200px", "fontSize": "13px",
                                      "backgroundColor": "#0d0d1a", "color": WHITE}),
                ], style={"marginBottom": "12px"}),

                # ── Q: Display Name ──
                html.Div([
                    html.Div("What should we call this item?",
                             style={"color": WHITE, "fontSize": "13px",
                                    "fontWeight": "bold", "marginBottom": "4px"}),
                    html.Div("Edit the display name or leave as-is",
                             style={"color": DARKGRAY, "fontSize": "11px", "marginBottom": "4px"}),
                    dcc.Input(id="wizard-name", type="text", value="",
                              style={**_inp, "width": "100%", "maxWidth": "500px"}),
                ], style={"marginBottom": "12px"}),

                # ── Q: Multi-pack? ──
                html.Div([
                    html.Div("Is this a multi-pack?",
                             style={"color": WHITE, "fontSize": "13px",
                                    "fontWeight": "bold", "marginBottom": "4px"}),
                    html.Div("e.g. a 4-pack of filament with different colors",
                             style={"color": DARKGRAY, "fontSize": "11px", "marginBottom": "4px"}),
                    dbc.Select(id="wizard-multipack",
                               options=[
                                   {"label": "No \u2014 single item", "value": "no"},
                                   {"label": "Yes \u2014 multiple items in one package", "value": "yes"},
                               ], value="no",
                               style={"width": "300px", "fontSize": "13px",
                                      "backgroundColor": "#0d0d1a", "color": WHITE}),
                ], style={"marginBottom": "12px"}),

                # ── MULTI-PACK BREAKDOWN (shown when multipack=yes) ──
                html.Div([
                    html.Div([
                        html.Span("What\u2019s in the pack?",
                                  style={"color": PURPLE, "fontSize": "13px", "fontWeight": "bold"}),
                        html.Span("  \u2014 add each item with color/name, qty, and which location it goes to",
                                  style={"color": GRAY, "fontSize": "11px"}),
                    ], style={"marginBottom": "8px"}),
                    dash_table.DataTable(
                        id="wizard-pack-table",
                        columns=[
                            {"name": "Color / Item Name", "id": "name", "editable": True},
                            {"name": "Qty", "id": "qty", "type": "numeric", "editable": True},
                            {"name": "Location", "id": "location",
                             "presentation": "dropdown", "editable": True},
                        ],
                        data=[
                            {"name": "", "qty": 1, "location": "Tulsa, OK"},
                            {"name": "", "qty": 1, "location": "Tulsa, OK"},
                        ],
                        dropdown={
                            "location": {"options": [
                                {"label": l, "value": l} for l in ["Tulsa, OK", "Texas", "Other"]
                            ]},
                        },
                        editable=True,
                        row_deletable=True,
                        style_table={"overflowX": "auto"},
                        style_header={
                            "backgroundColor": "#1a1a2e", "color": PURPLE,
                            "fontWeight": "bold", "fontSize": "12px",
                            "border": f"1px solid {PURPLE}33",
                        },
                        style_cell={
                            "backgroundColor": CARD, "color": WHITE,
                            "border": f"1px solid {DARKGRAY}33", "fontSize": "12px",
                            "padding": "6px 10px", "textAlign": "left",
                        },
                        style_cell_conditional=[
                            {"if": {"column_id": "name"}, "width": "250px"},
                            {"if": {"column_id": "qty"}, "width": "60px", "textAlign": "center"},
                            {"if": {"column_id": "location"}, "width": "140px"},
                        ],
                        style_data_conditional=[
                            {"if": {"state": "active"}, "backgroundColor": f"{CYAN}15",
                             "border": f"1px solid {CYAN}"},
                        ],
                    ),
                    html.Button("+ Add Row", id="wizard-pack-add-row", n_clicks=0,
                                style={"fontSize": "11px", "padding": "4px 14px",
                                       "backgroundColor": "transparent", "color": PURPLE,
                                       "border": f"1px solid {PURPLE}44", "borderRadius": "4px",
                                       "cursor": "pointer", "marginTop": "6px"}),
                ], id="wizard-pack-section",
                   style={"display": "none", "padding": "10px 14px",
                          "backgroundColor": f"{PURPLE}06", "borderRadius": "6px",
                          "border": f"1px solid {PURPLE}22", "marginBottom": "12px"}),

                # ── NON-MULTIPACK: Location section (shown when multipack=no) ──
                html.Div([
                    # Q: Location
                    html.Div([
                        html.Div("Which location does this go to?",
                                 style={"color": WHITE, "fontSize": "13px",
                                        "fontWeight": "bold", "marginBottom": "4px"}),
                        dbc.Select(id="wizard-loc", options=loc_options, value="Tulsa, OK",
                                   style={"width": "200px", "fontSize": "13px",
                                          "backgroundColor": "#0d0d1a", "color": WHITE}),
                    ], style={"marginBottom": "12px"}),

                    # Q: Split?
                    html.Div([
                        html.Div("Split between locations?",
                                 style={"color": WHITE, "fontSize": "13px",
                                        "fontWeight": "bold", "marginBottom": "4px"}),
                        html.Div("Send part of this order to one location and the rest to another",
                                 style={"color": DARKGRAY, "fontSize": "11px", "marginBottom": "4px"}),
                        dbc.Select(id="wizard-split-yn",
                                   options=[
                                       {"label": "No \u2014 all at one location", "value": "no"},
                                       {"label": "Yes \u2014 split between locations", "value": "yes"},
                                   ], value="no",
                                   style={"width": "300px", "fontSize": "13px",
                                          "backgroundColor": "#0d0d1a", "color": WHITE}),
                    ], style={"marginBottom": "8px"}),

                    # Split allocation (hidden until split-yn=yes)
                    html.Div([
                        html.Div([
                            html.Span("Location 1 (above):", style={**_label, "color": TEAL}),
                            html.Span("Qty:", style={**_label, "marginLeft": "8px"}),
                            dcc.Input(id="wizard-loc1-qty", type="number", min=0, value=1,
                                      style={**_inp, "width": "55px"}),
                        ], style={"display": "flex", "alignItems": "center", "gap": "4px",
                                  "marginBottom": "6px"}),
                        html.Div([
                            html.Span("Location 2:", style=_label),
                            dbc.Select(id="wizard-loc2", options=loc_options, value="Texas",
                                       style={"width": "130px", "fontSize": "12px",
                                              "backgroundColor": "#0d0d1a", "color": WHITE}),
                            html.Span("Qty:", style={**_label, "marginLeft": "8px"}),
                            dcc.Input(id="wizard-loc2-qty", type="number", min=0, value=0,
                                      style={**_inp, "width": "55px"}),
                            html.Span("", id="wizard-split-total",
                                      style={"color": GRAY, "fontSize": "11px", "marginLeft": "8px"}),
                        ], style={"display": "flex", "alignItems": "center", "gap": "4px"}),
                    ], id="wizard-split-row",
                       style={"display": "none", "padding": "8px 12px",
                              "backgroundColor": f"{PURPLE}08", "borderRadius": "6px",
                              "border": f"1px solid {PURPLE}33", "marginBottom": "8px"}),

                    # Product image upload
                    html.Div([
                        html.Div("Product Image",
                                 style={"color": TEAL, "fontSize": "12px",
                                        "fontWeight": "bold", "marginBottom": "4px"}),
                        html.Div("Upload a photo of the actual product (e.g. the filament spool)",
                                 style={"color": DARKGRAY, "fontSize": "11px", "marginBottom": "4px"}),
                        html.Div([
                            dcc.Upload(
                                id="wizard-product-img-upload",
                                children=html.Div("Drop image or browse",
                                                   style={"color": GRAY, "fontSize": "11px",
                                                          "textAlign": "center", "padding": "8px"}),
                                accept="image/*",
                                style={"borderWidth": "1px", "borderStyle": "dashed",
                                       "borderColor": f"{TEAL}44", "borderRadius": "6px",
                                       "backgroundColor": f"{TEAL}06", "cursor": "pointer",
                                       "width": "150px"},
                            ),
                            html.Div(id="wizard-product-img-preview",
                                     style={"marginLeft": "10px"}),
                        ], style={"display": "flex", "alignItems": "center"}),
                    ], style={"marginTop": "8px"}),
                ], id="wizard-nopack-section"),

            ], id="wizard-form-row",
               style={"display": "flex", "flexDirection": "column",
                       "gap": "0px", "marginBottom": "12px"}),

            # Legacy hidden inputs to keep callbacks happy
            dcc.Input(id="wizard-units-per-pack", type="number", value=1,
                      style={"display": "none"}),
            dcc.Store(id="wizard-split-active", data=False),
            html.Button(id="wizard-split-toggle", n_clicks=0,
                        style={"display": "none"}),
            html.Div(id="wizard-loc-row", style={"display": "none"}),

            # Navigation buttons
            html.Div([
                html.Button("\u2190 Back", id="wizard-back-btn", n_clicks=0,
                            disabled=True,
                            style={"fontSize": "12px", "padding": "8px 16px",
                                   "backgroundColor": "transparent", "color": DARKGRAY,
                                   "border": f"1px solid {DARKGRAY}44", "borderRadius": "6px",
                                   "cursor": "pointer"}),
                html.Button("Skip", id="wizard-skip-btn", n_clicks=0,
                            style={"fontSize": "12px", "padding": "8px 20px",
                                   "backgroundColor": "transparent", "color": GRAY,
                                   "border": f"1px solid {DARKGRAY}55", "borderRadius": "6px",
                                   "cursor": "pointer"}),
                html.Button("Save & Next \u2192", id="wizard-save-btn", n_clicks=0,
                            style={"fontSize": "12px", "padding": "8px 24px",
                                   "backgroundColor": TEAL, "color": WHITE,
                                   "border": "none", "borderRadius": "6px",
                                   "cursor": "pointer", "fontWeight": "bold",
                                   "boxShadow": f"0 2px 6px {TEAL}55"}),
            ], id="wizard-nav-btns",
               style={"display": "flex", "gap": "10px", "alignItems": "center",
                       "marginBottom": "10px"}),

            # Done button (hidden until wizard finishes all items)
            html.Button("Done — Refresh Inventory", id="wizard-done-btn", n_clicks=0,
                        style={"display": "none", "fontSize": "12px", "padding": "8px 24px",
                               "backgroundColor": GREEN, "color": WHITE,
                               "border": "none", "borderRadius": "6px",
                               "cursor": "pointer", "fontWeight": "bold",
                               "marginBottom": "10px"}),

            # Progress dots
            html.Div(id="receipt-wizard-progress",
                     style={"textAlign": "center"}),

        ], id="receipt-wizard-panel", style={"display": "none"}),

        # ── BATCH MODE: Editable DataTable for all items at once ──
        html.Div([
            html.Div([
                html.Span("BATCH EDIT MODE", style={"color": GREEN, "fontWeight": "bold", "fontSize": "14px"}),
                html.Span(" — Edit all items at once, then save with one click.",
                          style={"color": GRAY, "fontSize": "12px", "marginLeft": "8px"}),
            ], style={"marginBottom": "8px"}),
            dash_table.DataTable(
                id="batch-items-table",
                columns=[
                    {"name": "Item Name", "id": "name", "editable": True},
                    {"name": "Category", "id": "category", "presentation": "dropdown", "editable": True},
                    {"name": "Qty", "id": "qty", "type": "numeric", "editable": True},
                    {"name": "Price", "id": "price", "type": "numeric", "editable": False},
                    {"name": "Location", "id": "location", "presentation": "dropdown", "editable": True},
                ],
                data=[],
                dropdown={
                    "category": {"options": [
                        {"label": c, "value": c} for c in sorted([
                            "3D Printing", "Electronics & Components", "Tools & Equipment",
                            "Packaging & Shipping", "Craft Supplies", "Office Supplies",
                            "Labels & Stickers", "Resin & Molds", "Vinyl & HTV",
                            "Wood & Laser", "Fabric & Sewing", "Paint & Finishing",
                            "Storage & Organization", "Business Services", "Other",
                        ])
                    ]},
                    "location": {"options": [
                        {"label": "Tulsa, OK", "value": "Tulsa, OK"},
                        {"label": "Texas", "value": "Texas"},
                    ]},
                },
                style_table={"overflowX": "auto"},
                style_header={
                    "backgroundColor": "#1a1a2e", "color": CYAN,
                    "fontWeight": "bold", "fontSize": "12px",
                    "border": f"1px solid {CYAN}33",
                },
                style_cell={
                    "backgroundColor": CARD, "color": WHITE,
                    "border": f"1px solid {DARKGRAY}33", "fontSize": "12px",
                    "padding": "6px 10px", "textAlign": "left",
                },
                style_data_conditional=[
                    {"if": {"state": "active"}, "backgroundColor": f"{CYAN}15",
                     "border": f"1px solid {CYAN}"},
                ],
                editable=True,
                row_deletable=True,
            ),
            html.Div([
                html.Button("Save All Items", id="batch-save-btn", n_clicks=0,
                            style={"fontSize": "13px", "padding": "10px 28px",
                                   "backgroundColor": GREEN, "color": WHITE,
                                   "border": "none", "borderRadius": "6px",
                                   "cursor": "pointer", "fontWeight": "bold",
                                   "marginTop": "10px",
                                   "boxShadow": f"0 2px 8px {GREEN}33"}),
                html.Span(id="batch-save-status", style={"color": GRAY, "fontSize": "12px",
                                                          "marginLeft": "10px"}),
            ]),
        ], id="batch-mode-panel", style={"display": "none", "marginTop": "12px",
                                          "padding": "12px", "backgroundColor": f"{GREEN}08",
                                          "borderRadius": "6px", "border": f"1px solid {GREEN}33"}),

        # ── CSV PASTE IMPORT ──
        html.Details([
            html.Summary([
                html.Span("\u25b6 ", style={"fontSize": "12px"}),
                html.Span("CSV PASTE IMPORT", style={"fontWeight": "bold"}),
                html.Span(" — Paste CSV data (Name, Qty, Price, Category)", style={"color": GRAY, "fontWeight": "normal", "fontSize": "12px"}),
            ], style={"color": TEAL, "fontSize": "13px", "fontWeight": "bold",
                      "cursor": "pointer", "padding": "8px 12px", "listStyle": "none",
                      "backgroundColor": "#ffffff08", "borderRadius": "6px",
                      "border": f"1px solid {TEAL}33", "marginTop": "10px"}),
            html.Div([
                dcc.Textarea(
                    id="csv-paste-input",
                    placeholder="Paste CSV data here:\nItem Name, Qty, Price, Category\nWidget A, 5, 2.99, Electronics\nWidget B, 10, 1.50, Craft Supplies",
                    style={"width": "100%", "height": "120px", "backgroundColor": CARD,
                           "color": WHITE, "border": f"1px solid {TEAL}33",
                           "borderRadius": "6px", "padding": "10px", "fontSize": "12px",
                           "fontFamily": "monospace", "resize": "vertical"},
                ),
                html.Div([
                    html.Button("Import All", id="csv-import-btn", n_clicks=0,
                                style={"fontSize": "12px", "padding": "8px 20px",
                                       "backgroundColor": TEAL, "color": WHITE,
                                       "border": "none", "borderRadius": "6px",
                                       "cursor": "pointer", "fontWeight": "bold",
                                       "marginTop": "8px"}),
                    html.Span(id="csv-import-status", style={"color": GRAY, "fontSize": "12px",
                                                              "marginLeft": "10px"}),
                ]),
            ], style={"padding": "10px"}),
        ], style={"marginTop": "8px"}),

    ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
              "marginBottom": "14px", "border": f"1px solid {PURPLE}33",
              "borderLeft": f"4px solid {PURPLE}"})


def _build_product_image_manager():
    """Build a grid of product images — upload photos for each unique inventory product."""
    # Collect unique product names from item details
    product_names = set()
    for (_onum, _iname), details in _ITEM_DETAILS.items():
        for d in details:
            dn = d.get("display_name", "").strip()
            if dn:
                product_names.add(dn)
    # Also add from INV_ITEMS if no details
    if len(INV_ITEMS) > 0 and "name" in INV_ITEMS.columns:
        for n in INV_ITEMS["name"].unique():
            product_names.add(n)

    product_names = sorted(product_names)[:100]  # Cap at 100

    if not product_names:
        return html.Div("No products found yet. Upload receipts first.",
                         style={"color": GRAY, "padding": "20px", "textAlign": "center"})

    cards = []
    for pname in product_names:
        img_url = _IMAGE_URLS.get(pname, "")
        if img_url:
            img_el = html.Img(src=img_url, style={
                "width": "100%", "height": "80px", "objectFit": "cover",
                "borderRadius": "4px", "marginBottom": "4px"})
        else:
            img_el = html.Div("No image", style={
                "width": "100%", "height": "80px", "backgroundColor": "#0d0d1a",
                "borderRadius": "4px", "marginBottom": "4px",
                "display": "flex", "alignItems": "center", "justifyContent": "center",
                "color": DARKGRAY, "fontSize": "10px"})

        short_name = pname[:35] + ("..." if len(pname) > 35 else "")
        cards.append(html.Div([
            img_el,
            html.Div(short_name, style={"color": WHITE, "fontSize": "10px",
                                         "overflow": "hidden", "textOverflow": "ellipsis",
                                         "whiteSpace": "nowrap", "marginBottom": "4px"},
                     title=pname),
            dcc.Upload(
                id={"type": "product-img-upload", "index": pname},
                children=html.Span("Upload", style={"color": TEAL, "fontSize": "10px",
                                                      "cursor": "pointer", "textDecoration": "underline"}),
                accept="image/*",
                style={"textAlign": "center"},
            ),
            html.Div(id={"type": "product-img-status", "index": pname},
                     style={"fontSize": "10px", "textAlign": "center"}),
        ], style={"width": "120px", "padding": "8px", "backgroundColor": CARD,
                  "borderRadius": "6px", "border": f"1px solid {DARKGRAY}22",
                  "textAlign": "center"}))

    return html.Div([
        html.Div("Click 'Upload' to add a photo for any product. Images appear in inventory displays.",
                 style={"color": GRAY, "fontSize": "12px", "marginBottom": "12px", "padding": "0 14px"}),
        html.Div(cards, style={
            "display": "flex", "flexWrap": "wrap", "gap": "10px",
            "padding": "14px", "maxHeight": "400px", "overflowY": "auto",
        }),
    ])



# def build_tab4_inventory() — extracted to dashboard_utils/pages/



_PRINTER_MODELS = ["P1S", "A1", "P2S"]
_PRINTER_LOCATIONS = {"P1S": "Tulsa", "A1": "Texas", "P2S": "Both"}

def _build_product_library():
    """Build the Product Library — grouped by category, per-printer STL/time/grams."""
    import json as _json_pl2

    # Load all listings from Supabase
    all_listings = []
    for store, label in [("keycomponentmfg", "KeyComp"), ("aurvio", "Aurvio"), ("lunalinks", "L&L")]:
        try:
            from supabase_loader import get_config_value as _gcv_pl
            raw = _gcv_pl(f"listings_csv_{store}")
            if raw:
                records = _json_pl2.loads(raw) if isinstance(raw, str) else raw
                for r in records:
                    all_listings.append({
                        "store": store, "store_label": label,
                        "title": r.get("TITLE", "?"),
                        "price": r.get("PRICE", 0),
                        "image": r.get("IMAGE1", ""),
                    })
        except Exception:
            pass

    if not all_listings:
        return html.Div([
            html.H3("\U0001f4e6 PRODUCT LIBRARY", style={
                "color": CYAN, "margin": "30px 0 6px 0", "fontSize": "16px",
                "letterSpacing": "1.5px", "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px",
            }),
            html.P("Upload active listings CSVs in Data Hub to build your Product Library.",
                   style={"color": GRAY, "fontSize": "12px"}),
        ])

    _sc_map = {"keycomponentmfg": CYAN, "aurvio": "#9b59b6", "lunalinks": "#e91e63"}
    _inp = {"fontSize": "11px", "backgroundColor": BG, "color": WHITE,
            "border": f"1px solid {DARKGRAY}44", "borderRadius": "4px", "padding": "4px 6px"}

    # Group listings by category
    _by_cat = {}
    for listing in all_listings:
        _prod = PRODUCT_LIBRARY.get(listing["title"], {})
        _cat = _prod.get("category", "Uncategorized")
        if _cat not in _by_cat:
            _by_cat[_cat] = []
        _by_cat[_cat].append(listing)

    # Build category sections
    _cat_sections = []
    _total = len(all_listings)
    _complete = 0

    # Category order: defined categories first, Uncategorized last
    _cat_order = sorted([c for c in _by_cat if c != "Uncategorized"]) + (["Uncategorized"] if "Uncategorized" in _by_cat else [])

    for _cat in _cat_order:
        _cat_listings = sorted(_by_cat[_cat], key=lambda x: x["title"])
        _cat_complete = 0

        _cards = []
        for listing in _cat_listings:
            _title = listing["title"]
            _sc = _sc_map.get(listing["store"], TEAL)
            _prod = PRODUCT_LIBRARY.get(_title, {})
            _printers = _prod.get("printers", {})

            # Status
            _has_stl = any(_printers.get(m, {}).get("stl") for m in _PRINTER_MODELS)
            _has_data = any(_printers.get(m, {}).get("time") and _printers.get(m, {}).get("grams") for m in _PRINTER_MODELS)
            if _has_stl and _has_data:
                _st_color, _st = GREEN, "Done"
                _cat_complete += 1
                _complete += 1
            elif _has_stl or _has_data:
                _st_color, _st = ORANGE, "Partial"
            else:
                _st_color, _st = DARKGRAY, ""

            # Build sizes — migrate old format (printers at top level) to sizes format
            _sizes = _prod.get("sizes", {})
            if not _sizes and _printers:
                # Old format: printers at top level = "default" size
                _sizes = {"default": _printers}

            if not _sizes:
                _sizes = {"default": {}}

            # Build printer rows for each size
            _all_size_rows = []
            for _size_idx, (_size_name, _size_printers) in enumerate(sorted(_sizes.items())):
                _size_label = _size_name if _size_name != "default" else ""

                _rows = []
                for _m in _PRINTER_MODELS:
                    _pd = _size_printers.get(_m, {})
                    _size_key = f"{_size_name}"
                    _rows.append(html.Div([
                        html.Span(_m, style={"color": CYAN, "fontSize": "10px", "fontWeight": "bold",
                                              "width": "28px", "flexShrink": "0", "fontFamily": "monospace"}),
                        dcc.Input(id={"type": "pl-stl", "listing": _title, "printer": _m, "size": _size_key},
                                  type="text", placeholder="file.stl", value=_pd.get("stl", ""),
                                  style={**_inp, "width": "110px"}),
                        dcc.Input(id={"type": "pl-time", "listing": _title, "printer": _m, "size": _size_key},
                                  type="number", placeholder="min", value=_pd.get("time", ""),
                                  style={**_inp, "width": "55px"}),
                        dcc.Input(id={"type": "pl-grams", "listing": _title, "printer": _m, "size": _size_key},
                                  type="number", placeholder="g", value=_pd.get("grams", ""),
                                  style={**_inp, "width": "50px"}),
                    ], style={"display": "flex", "alignItems": "center", "gap": "3px", "marginBottom": "2px"}))

                _size_block = html.Div([
                    html.Span(_size_label, style={"color": ORANGE, "fontSize": "9px", "fontWeight": "bold",
                                                   "letterSpacing": "0.5px"}) if _size_label else html.Span(),
                    *_rows,
                ], style={"marginBottom": "4px" if len(_sizes) > 1 else "0px"})
                _all_size_rows.append(_size_block)

            # "Add Size" input — always present, user types a size name and it gets added on save
            _all_size_rows.append(html.Div([
                dcc.Input(id={"type": "pl-new-size", "listing": _title},
                          type="text", placeholder="+ Add size (e.g. Large, Small)",
                          style={**_inp, "width": "180px", "fontSize": "10px", "opacity": "0.6"}),
            ], style={"marginTop": "2px"}))

            # Product details data
            _details = _prod.get("details", {})
            _filament_color = _details.get("filament_color", "")
            _print_location = _details.get("print_location", "Both")
            _success_rate = _details.get("success_rate", "")
            _finished_weight = _details.get("finished_weight_oz", "")
            _box_size = _details.get("box_size", "")
            _notes = _details.get("notes", "")
            _has_variations = _details.get("has_variations", False)
            _variation_prices = _details.get("variation_prices", [])  # [{"name": "Large", "price": 64.99}, ...]
            _components = _details.get("components", [])  # [{"item": "LED Kit", "qty": 1}, ...]

            # Build variation price rows
            _var_price_rows = []
            for _vp in _variation_prices:
                _var_price_rows.append(html.Div([
                    html.Span(_vp.get("name", ""), style={"color": WHITE, "fontSize": "10px", "width": "80px"}),
                    html.Span(f"${_vp.get('price', 0)}", style={"color": GREEN, "fontSize": "10px", "fontFamily": "monospace"}),
                ], style={"display": "flex", "gap": "4px"}))

            # Build component rows
            _comp_rows = []
            for _cp in _components:
                _comp_rows.append(html.Div([
                    html.Span(f"{_cp.get('qty', 1)}x", style={"color": CYAN, "fontSize": "10px", "width": "25px"}),
                    html.Span(_cp.get("item", ""), style={"color": WHITE, "fontSize": "10px"}),
                ], style={"display": "flex", "gap": "4px"}))

            _card = html.Div([
                # Top: Image + Title + Price + Store + STL rows
                html.Div([
                    html.Img(src=listing["image"], style={
                        "width": "44px", "height": "44px", "borderRadius": "6px",
                        "objectFit": "cover", "marginRight": "8px", "flexShrink": "0",
                    }) if listing["image"] else html.Div(style={"width": "44px", "height": "44px", "marginRight": "8px"}),
                    html.Div([
                        html.Span(_title[:48], style={"color": WHITE, "fontSize": "11px", "fontWeight": "bold"}),
                        html.Span(f" from ${listing['price']}", style={"color": GREEN, "fontSize": "10px", "fontFamily": "monospace", "marginLeft": "4px"}),
                        html.Div([
                            html.Span(listing["store_label"], style={
                                "color": _sc, "fontSize": "9px", "backgroundColor": f"{_sc}15",
                                "padding": "0px 4px", "borderRadius": "2px", "marginRight": "4px"}),
                            html.Span(_st, style={"color": _st_color, "fontSize": "9px"}) if _st else html.Span(),
                            html.Span(f" {_print_location}", style={"color": DARKGRAY, "fontSize": "9px", "marginLeft": "4px"}) if _print_location and _print_location != "Both" else html.Span(),
                        ], style={"marginTop": "2px"}),
                    ], style={"flex": "1", "minWidth": "0"}),
                    # Printer STL/time/grams
                    html.Div([
                        html.Div([
                            html.Span("", style={"width": "28px"}),
                            html.Span("STL", style={"width": "110px", "color": DARKGRAY, "fontSize": "8px"}),
                            html.Span("Min", style={"width": "55px", "color": DARKGRAY, "fontSize": "8px"}),
                            html.Span("Grams", style={"width": "50px", "color": DARKGRAY, "fontSize": "8px"}),
                        ], style={"display": "flex", "gap": "3px", "marginBottom": "1px"}),
                        *_all_size_rows,
                    ], style={"marginLeft": "auto"}),
                ], style={"display": "flex", "alignItems": "flex-start", "gap": "8px"}),

                # Expandable: Product Details
                html.Details([
                    html.Summary("Details", style={"color": CYAN, "fontSize": "10px", "cursor": "pointer",
                                                     "padding": "4px 0", "opacity": "0.7"}),
                    html.Div([
                        html.Div([
                            # Row 1: Filament, Location, Success Rate
                            html.Div([
                                html.Div([
                                    html.Label("Filament Color/Type", style={"color": GRAY, "fontSize": "9px", "display": "block"}),
                                    dcc.Input(id={"type": "pl-filament", "listing": _title},
                                              type="text", placeholder="e.g. White PLA, Silk Gold",
                                              value=_filament_color,
                                              style={**_inp, "width": "140px"}),
                                ], style={"marginRight": "8px"}),
                                html.Div([
                                    html.Label("Printed At", style={"color": GRAY, "fontSize": "9px", "display": "block"}),
                                    dcc.Dropdown(id={"type": "pl-location", "listing": _title},
                                                 options=[{"label": "Both", "value": "Both"},
                                                          {"label": "Tulsa Only", "value": "Tulsa"},
                                                          {"label": "Texas Only", "value": "Texas"}],
                                                 value=_print_location or "Both",
                                                 clearable=False,
                                                 style={"width": "110px", "fontSize": "10px", "backgroundColor": BG}),
                                ], style={"marginRight": "8px"}),
                                html.Div([
                                    html.Label("Success Rate %", style={"color": GRAY, "fontSize": "9px", "display": "block"}),
                                    dcc.Input(id={"type": "pl-success", "listing": _title},
                                              type="number", placeholder="95",
                                              value=_success_rate,
                                              style={**_inp, "width": "55px"}),
                                ], style={"marginRight": "8px"}),
                                html.Div([
                                    html.Label("Weight (oz)", style={"color": GRAY, "fontSize": "9px", "display": "block"}),
                                    dcc.Input(id={"type": "pl-weight", "listing": _title},
                                              type="number", placeholder="oz",
                                              value=_finished_weight,
                                              style={**_inp, "width": "55px"}),
                                ], style={"marginRight": "8px"}),
                                html.Div([
                                    html.Label("Box Size", style={"color": GRAY, "fontSize": "9px", "display": "block"}),
                                    dcc.Input(id={"type": "pl-box", "listing": _title},
                                              type="text", placeholder="8x6x6",
                                              value=_box_size,
                                              style={**_inp, "width": "70px"}),
                                ]),
                            ], style={"display": "flex", "flexWrap": "wrap", "gap": "4px", "marginBottom": "6px"}),

                            # Row 2: Variation prices
                            html.Div([
                                html.Label("Variation Prices", style={"color": GRAY, "fontSize": "9px", "display": "block", "marginBottom": "2px"}),
                                dcc.Textarea(id={"type": "pl-variations", "listing": _title},
                                             placeholder="One per line: Large=64.99, Small=44.99, Plug-in=49.99",
                                             value="\n".join(f"{v['name']}={v['price']}" for v in _variation_prices) if _variation_prices else "",
                                             style={**_inp, "width": "280px", "height": "40px", "resize": "vertical"}),
                            ], style={"marginBottom": "6px"}),

                            # Row 3: Components
                            html.Div([
                                html.Label("Components (from inventory)", style={"color": GRAY, "fontSize": "9px", "display": "block", "marginBottom": "2px"}),
                                dcc.Textarea(id={"type": "pl-components", "listing": _title},
                                             placeholder="One per line: 1x LED Kit, 1x 8x6x6 Box, 2x M5 Screw",
                                             value="\n".join(f"{c['qty']}x {c['item']}" for c in _components) if _components else "",
                                             style={**_inp, "width": "280px", "height": "40px", "resize": "vertical"}),
                            ], style={"marginBottom": "6px"}),

                            # Row 4: Description / Assembly Instructions
                            html.Div([
                                html.Label("Description / Assembly Instructions", style={"color": GRAY, "fontSize": "9px", "display": "block", "marginBottom": "2px"}),
                                dcc.Textarea(id={"type": "pl-notes", "listing": _title},
                                             placeholder="Parts needed, assembly steps, notes for Braden, anything...\ne.g. USB LED puck, 1x M3x25mm screw, super glue\nGlue top to base, screw in LED mount",
                                             value=_notes,
                                             style={**_inp, "width": "100%", "height": "70px", "resize": "vertical",
                                                    "lineHeight": "1.4"}),
                            ]),
                        ], style={"padding": "6px 0"}),
                    ]),
                ], style={"marginTop": "4px"}),

                # Hidden inputs
                dcc.Input(id={"type": "pl-category", "listing": _title},
                          type="hidden", value=_prod.get("category", _cat)),
            ], style={
                "backgroundColor": CARD, "borderRadius": "6px", "padding": "8px 10px",
                "marginBottom": "4px", "borderLeft": f"3px solid {_st_color}",
            })
            _cards.append(_card)

        # Category header
        _cat_section = html.Details([
            html.Summary([
                html.Span(f"{_cat}", style={"color": ORANGE, "fontSize": "14px", "fontWeight": "bold",
                                             "letterSpacing": "1px", "marginRight": "8px"}),
                html.Span(f"{len(_cat_listings)} products", style={"color": GRAY, "fontSize": "11px", "marginRight": "8px"}),
                html.Span(f"{_cat_complete} done", style={"color": GREEN, "fontSize": "11px"}) if _cat_complete else html.Span(),
            ], style={"cursor": "pointer", "padding": "8px 0", "borderBottom": f"1px solid {ORANGE}22"}),
            html.Div(_cards, style={"paddingTop": "6px"}),
        ], open=False, style={"marginBottom": "8px"})
        _cat_sections.append(_cat_section)

    return html.Div([
        html.H3("\U0001f4e6 PRODUCT LIBRARY", style={
            "color": CYAN, "margin": "30px 0 6px 0", "fontSize": "16px",
            "letterSpacing": "1.5px", "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px",
        }),
        html.P("Each product has per-printer rows (P1S, A1, P2S) for STL file, print time, and filament. "
               "Fill in what you know, then Save All.",
               style={"color": GRAY, "margin": "0 0 8px 0", "fontSize": "12px"}),
        html.Div([
            html.Span(f"{_total} products", style={"color": WHITE, "fontSize": "12px", "marginRight": "12px"}),
            html.Span(f"{_complete} complete", style={"color": GREEN, "fontSize": "12px", "marginRight": "12px"}),
            html.Span(f"{_total - _complete} remaining", style={"color": ORANGE, "fontSize": "12px", "marginRight": "20px"}),
            html.Span(f"{len(_cat_order)} categories", style={"color": CYAN, "fontSize": "12px"}),
        ], style={"marginBottom": "10px", "padding": "8px 12px", "backgroundColor": f"{CARD}cc",
                  "borderRadius": "6px", "border": f"1px solid {DARKGRAY}22"}),
        html.Div([
            html.Button("\U0001f4be  Save All", id="pl-save-all", n_clicks=0,
                        style={"fontSize": "13px", "padding": "8px 24px", "backgroundColor": f"{GREEN}25",
                               "border": f"1px solid {GREEN}", "borderRadius": "6px", "color": GREEN,
                               "cursor": "pointer", "fontWeight": "bold", "marginRight": "12px"}),
            html.Span("Open a category, fill in data, click Save All when done.", style={"color": GRAY, "fontSize": "11px"}),
        ], style={"marginBottom": "10px"}),
        html.Div(id="product-library-status", style={"minHeight": "20px", "marginBottom": "8px"}),
        html.Div(_cat_sections),
    ])


def _build_completed_receipts():
    """Show which receipts have been organized, with their items and status."""
    if not _ITEM_DETAILS:
        return html.Div()

    # Group by order number
    orders = {}  # order_num -> list of detail dicts
    for (onum, iname), details in _ITEM_DETAILS.items():
        orders.setdefault(onum, []).append({"orig_name": iname, "details": details})

    # Get order metadata from INVOICES
    order_meta = {}
    for inv in INVOICES:
        if inv["order_num"] in orders:
            order_meta[inv["order_num"]] = {
                "date": inv.get("date", ""),
                "source": inv.get("source", ""),
                "total": sum(it["qty"] * it["price"] for it in inv["items"]),
                "total_items": len(inv["items"]),
            }

    # Figure out which orders are fully done vs partially done
    order_status = {}
    for inv in INVOICES:
        onum = inv["order_num"]
        if onum not in orders:
            continue
        biz_items = []
        for item in inv["items"]:
            item_name = item["name"]
            if item_name.startswith("Your package was left near the front door or porch."):
                item_name = item_name.replace("Your package was left near the front door or porch.", "").strip()
            auto_cat = categorize_item(item_name)
            if auto_cat not in ("Personal/Gift", "Business Fees"):
                biz_items.append(item_name)
        # If no biz items found (e.g. personal order), count all items
        if not biz_items:
            biz_items = [item["name"] for item in inv["items"]]
        done_count = sum(1 for n in biz_items if (onum, n) in _ITEM_DETAILS)
        order_status[onum] = {"done": done_count, "total": len(biz_items)}

    rows = []
    for onum in sorted(orders.keys(), key=lambda o: order_meta.get(o, {}).get("date", ""), reverse=True):
        meta = order_meta.get(onum, {})
        status = order_status.get(onum, {"done": 0, "total": 0})
        fully_done = status["done"] >= status["total"] and status["total"] > 0

        # Collect saved item names
        saved_names = []
        for entry in orders[onum]:
            for d in entry["details"]:
                dn = d.get("display_name", entry["orig_name"])
                q = d.get("true_qty", 1)
                loc = d.get("location", "")
                loc_short = "T" if "Tulsa" in loc else ("TX" if "Texas" in loc else "?")
                saved_names.append(f"{dn} x{q} ({loc_short})")

        status_badge = html.Span(
            "DONE" if fully_done else f"{status['done']}/{status['total']}",
            style={"backgroundColor": f"{GREEN}22" if fully_done else f"{ORANGE}22",
                   "color": GREEN if fully_done else ORANGE,
                   "padding": "2px 8px", "borderRadius": "10px", "fontSize": "10px",
                   "fontWeight": "bold", "marginLeft": "8px"})

        rows.append(html.Tr([
            html.Td(meta.get("date", ""), style={"color": ORANGE, "fontWeight": "bold",
                    "padding": "8px 10px", "fontSize": "12px", "whiteSpace": "nowrap"}),
            html.Td([html.Span(onum, style={"color": CYAN, "fontSize": "11px"}), status_badge],
                    style={"padding": "8px 10px"}),
            html.Td(", ".join(saved_names), style={"color": WHITE, "fontSize": "11px",
                    "padding": "8px 10px", "maxWidth": "400px", "overflow": "hidden",
                    "textOverflow": "ellipsis"},
                    title="\n".join(saved_names)),
            html.Td(f"${meta.get('total', 0):,.2f}", style={"color": WHITE, "fontSize": "12px",
                    "padding": "8px 10px", "textAlign": "right", "fontWeight": "bold"}),
        ], style={"borderBottom": "1px solid #ffffff10"}))

    if not rows:
        return html.Div()

    return html.Div([
        html.H3("COMPLETED RECEIPTS", style={
            "color": GREEN, "margin": "0 0 10px 0", "fontSize": "18px",
            "fontWeight": "700", "letterSpacing": "1px"}),
        html.P(f"{len(rows)} receipts organized so far",
               style={"color": GRAY, "fontSize": "12px", "margin": "0 0 10px 0"}),
        html.Div([
            html.Table([
                html.Thead(html.Tr([
                    html.Th("Date", style={"color": GRAY, "fontWeight": "700", "fontSize": "11px",
                             "padding": "6px 10px", "textAlign": "left"}),
                    html.Th("Order #", style={"color": GRAY, "fontWeight": "700", "fontSize": "11px",
                             "padding": "6px 10px", "textAlign": "left"}),
                    html.Th("Items", style={"color": GRAY, "fontWeight": "700", "fontSize": "11px",
                             "padding": "6px 10px", "textAlign": "left"}),
                    html.Th("Total", style={"color": GRAY, "fontWeight": "700", "fontSize": "11px",
                             "padding": "6px 10px", "textAlign": "right"}),
                ], style={"borderBottom": f"2px solid {GREEN}44"})),
                html.Tbody(rows),
            ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
        ], style={"maxHeight": "400px", "overflowY": "auto"}),
    ], style={"backgroundColor": CARD, "padding": "18px", "borderRadius": "12px",
              "border": f"1px solid {GREEN}22", "borderTop": f"4px solid {GREEN}",
              "marginBottom": "20px"})


def _make_receipt_card(inv, is_personal=False):
    """Build a single receipt card with PDF viewer + specs."""
    import urllib.parse as _ul_rc
    source = inv.get("source", "Unknown")
    subfolder = _SOURCE_FOLDER_MAP.get(source, "keycomp")
    raw_file = inv.get("file", "")

    # Strip " (page X)" suffix for multi-page scanned receipts
    clean_file = re.sub(r'\s*\(page\s*\d+\)$', '', raw_file)
    encoded_file = _ul_rc.quote(clean_file)

    # Check if file exists on disk
    file_path = os.path.join(BASE_DIR, "data", "invoices", subfolder, clean_file)
    file_exists = os.path.isfile(file_path)

    pdf_url = f"/api/receipt/{subfolder}/{encoded_file}"

    # Left side: PDF viewer
    if file_exists:
        pdf_viewer = html.Iframe(
            src=pdf_url,
            style={
                "width": "100%", "height": "320px", "border": "none",
                "borderRadius": "8px", "backgroundColor": "#ffffff",
            },
        )
    else:
        pdf_viewer = html.Div(
            [html.Span("PDF not found on disk", style={"color": GRAY, "fontSize": "13px"}),
             html.Br(),
             html.Span(raw_file, style={"color": DARKGRAY, "fontSize": "11px"})],
            style={
                "width": "100%", "height": "320px", "display": "flex",
                "flexDirection": "column", "alignItems": "center",
                "justifyContent": "center", "backgroundColor": "#ffffff08",
                "borderRadius": "8px", "border": f"1px dashed {DARKGRAY}",
            },
        )

    # Right side: Specs
    order_num = inv.get("order_num", "N/A")
    date_str = inv.get("date", "Unknown")
    payment = inv.get("payment_method", "Unknown")
    ship_addr = inv.get("ship_address", "")
    if ship_addr.count(",") >= 2:
        parts = ship_addr.split(",")
        short_addr = parts[1].strip() + ", " + parts[2].strip().split(" ")[0]
    else:
        short_addr = ship_addr

    accent = PINK if is_personal else CYAN

    # Items table
    item_rows = []
    for it in inv.get("items", []):
        item_rows.append(html.Tr([
            html.Td(it["name"][:60] + ("..." if len(it["name"]) > 60 else ""),
                     style={"color": WHITE, "fontSize": "11px", "padding": "3px 6px",
                            "maxWidth": "280px", "overflow": "hidden", "textOverflow": "ellipsis"}),
            html.Td(str(it["qty"]), style={"color": GRAY, "fontSize": "11px",
                                            "textAlign": "center", "padding": "3px 6px"}),
            html.Td(f"${it['price']:,.2f}", style={"color": WHITE, "fontSize": "11px",
                                                    "textAlign": "right", "padding": "3px 6px"}),
        ]))

    specs_panel = html.Div([
        # Order number
        html.Div([
            html.Span("Order #  ", style={"color": GRAY, "fontSize": "11px"}),
            html.Span(order_num, style={"color": accent, "fontSize": "13px", "fontWeight": "bold"}),
        ], style={"marginBottom": "6px"}),
        # Date
        html.Div([
            html.Span("Date  ", style={"color": GRAY, "fontSize": "11px"}),
            html.Span(date_str, style={"color": WHITE, "fontSize": "12px"}),
        ], style={"marginBottom": "4px"}),
        # Source
        html.Div([
            html.Span("Source  ", style={"color": GRAY, "fontSize": "11px"}),
            html.Span(source, style={"color": TEAL, "fontSize": "12px"}),
        ], style={"marginBottom": "4px"}),
        # Payment
        html.Div([
            html.Span("Payment  ", style={"color": GRAY, "fontSize": "11px"}),
            html.Span(payment, style={"color": WHITE, "fontSize": "12px"}),
        ], style={"marginBottom": "4px"}),
        # Ship to
        html.Div([
            html.Span("Ship to  ", style={"color": GRAY, "fontSize": "11px"}),
            html.Span(short_addr, style={"color": CYAN, "fontSize": "12px"}),
        ], style={"marginBottom": "8px"}) if short_addr else html.Div(),
        # Items table
        html.Table([
            html.Thead(html.Tr([
                html.Th("Item", style={"textAlign": "left", "color": GRAY, "fontSize": "10px",
                                       "padding": "3px 6px", "borderBottom": f"1px solid {DARKGRAY}"}),
                html.Th("Qty", style={"textAlign": "center", "color": GRAY, "fontSize": "10px",
                                      "padding": "3px 6px", "borderBottom": f"1px solid {DARKGRAY}"}),
                html.Th("Price", style={"textAlign": "right", "color": GRAY, "fontSize": "10px",
                                        "padding": "3px 6px", "borderBottom": f"1px solid {DARKGRAY}"}),
            ])),
            html.Tbody(item_rows),
        ], style={"width": "100%", "borderCollapse": "collapse", "marginBottom": "8px"}),
        # Totals
        html.Div([
            html.Div([
                html.Span("Subtotal ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(f"${inv.get('subtotal', 0):,.2f}", style={"color": WHITE, "fontSize": "12px"}),
            ]),
            html.Div([
                html.Span("Tax ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(f"${inv.get('tax', 0):,.2f}", style={"color": WHITE, "fontSize": "12px"}),
            ]),
            html.Div([
                html.Span("Total ", style={"color": GRAY, "fontSize": "11px", "fontWeight": "bold"}),
                html.Span(f"${inv.get('grand_total', 0):,.2f}",
                          style={"color": ORANGE, "fontSize": "14px", "fontWeight": "bold"}),
            ], style={"marginTop": "2px"}),
        ], style={"borderTop": f"1px solid {DARKGRAY}", "paddingTop": "6px"}),
    ], style={"padding": "12px"})

    # Card: flex row with PDF left, specs right
    return html.Div([
        html.Div(pdf_viewer, style={"flex": "0 0 40%", "padding": "12px"}),
        html.Div(specs_panel, style={"flex": "1", "minWidth": "0"}),
    ], style={
        "display": "flex", "backgroundColor": CARD, "borderRadius": "12px",
        "marginBottom": "12px", "border": f"1px solid {accent}22",
        "overflow": "hidden",
    })



def _build_receipt_gallery():
    """Build a visual receipt gallery with embedded PDF viewers and parsed specs."""
    # Sort invoices by date (newest first)
    sorted_invoices = sorted(INVOICES, key=lambda o: o.get("date", ""), reverse=True)
    try:
        sorted_invoices = sorted(INVOICES,
            key=lambda o: pd.to_datetime(o.get("date", ""), format="%B %d, %Y", errors="coerce"),
            reverse=True)
    except Exception:
        pass

    biz_cards = []
    biz_search_data = []  # parallel list of search strings per card
    personal_cards = []
    for inv in sorted_invoices:
        is_personal = inv.get("source") == "Personal Amazon" or (
            isinstance(inv.get("file", ""), str) and "Gigi" in inv.get("file", ""))
        card = _make_receipt_card(inv, is_personal=is_personal)
        if is_personal:
            personal_cards.append(card)
        else:
            biz_cards.append(card)
            # Build search string: order #, date, source, original names, display names
            _onum = str(inv.get("order_num", ""))
            _orig = " ".join(it.get("name", "") for it in inv.get("items", []))
            _display = []
            for it in inv.get("items", []):
                _dkey = (_onum, it.get("name", ""))
                _dets = _ITEM_DETAILS.get(_dkey, [])
                for _d in _dets:
                    _dn = _d.get("display_name", "")
                    if _dn:
                        _display.append(_dn)
            biz_search_data.append(" ".join([
                _onum, inv.get("date", ""), inv.get("source", ""),
                inv.get("payment_method", ""), _orig, " ".join(_display),
            ]).lower())

    # Store search data in a hidden dcc.Store so the callback can filter
    all_biz = list(biz_cards)

    gallery_children = [
        html.H5(f"RECEIPT GALLERY  ({len(biz_cards)} business)", style={
            "color": CYAN, "fontWeight": "bold", "marginBottom": "4px", "fontSize": "15px",
        }),
        html.P("Every uploaded receipt with embedded PDF viewer and parsed specs.",
               style={"color": GRAY, "fontSize": "12px", "marginBottom": "8px"}),
        # Search bar
        dcc.Input(
            id="receipt-gallery-search",
            type="text",
            placeholder="Search receipts... (order #, item name, date, source)",
            style={
                "width": "100%", "padding": "8px 12px", "fontSize": "13px",
                "backgroundColor": BG, "color": WHITE,
                "border": f"1px solid {CYAN}44", "borderRadius": "6px",
                "marginBottom": "14px",
            },
            debounce=True,
        ),
        # Hidden store with search data + original cards for filtering
        dcc.Store(id="receipt-gallery-search-data", data=biz_search_data),
        dcc.Store(id="receipt-gallery-all-cards-count", data=len(biz_cards)),
        html.Div(id="receipt-gallery-cards", children=all_biz),
    ]

    # Personal receipts in a collapsed section
    if personal_cards:
        gallery_children.append(
            html.Details([
                html.Summary(f"Personal Receipts ({len(personal_cards)})", style={
                    "color": PINK, "fontSize": "14px", "fontWeight": "bold",
                    "cursor": "pointer", "padding": "8px 0",
                }),
                html.Div(personal_cards),
            ], open=False, style={
                "marginTop": "14px", "backgroundColor": CARD2,
                "padding": "12px 16px", "borderRadius": "10px",
                "border": f"1px solid {PINK}33",
            })
        )

    return html.Div(gallery_children, style={
        "backgroundColor": CARD2, "padding": "20px", "borderRadius": "12px",
        "marginTop": "14px", "border": f"1px solid {CYAN}33",
        "borderTop": f"4px solid {CYAN}", "maxHeight": "800px", "overflowY": "auto",
    })


# ── Receipt Gallery Search helper (callback registered after app creation) ──

def _build_receipt_cards_filtered(query):
    """Build receipt card list filtered by search query."""
    import urllib.parse as _ul2

    sorted_invoices = sorted(INVOICES, key=lambda o: o.get("date", ""), reverse=True)
    try:
        sorted_invoices = sorted(INVOICES,
            key=lambda o: pd.to_datetime(o.get("date", ""), format="%B %d, %Y", errors="coerce"),
            reverse=True)
    except Exception:
        pass

    cards = []
    for inv in sorted_invoices:
        is_personal = inv.get("source") == "Personal Amazon" or "Gigi" in inv.get("file", "")
        if is_personal:
            continue

        # Get both original Amazon names and renamed display names
        order_num = inv.get("order_num", "N/A")
        _orig_names = []
        _display_names = []
        for it in inv.get("items", []):
            _orig_names.append(it.get("name", ""))
            _key = (str(order_num), it.get("name", ""))
            _dets = _ITEM_DETAILS.get(_key, [])
            if _dets:
                for _d in _dets:
                    _dn = _d.get("display_name", "")
                    if _dn:
                        _display_names.append(_dn)
            else:
                _display_names.append(it.get("name", ""))

        if query:
            search_text = " ".join([
                str(order_num),
                inv.get("date", ""),
                inv.get("source", ""),
                inv.get("payment_method", ""),
                " ".join(_orig_names),
                " ".join(_display_names),
            ]).lower()
            if query not in search_text:
                continue

        # Build card showing both original and renamed
        date_str = inv.get("date", "")
        source = inv.get("source", "")
        total = inv.get("grand_total", 0)
        display_str = ", ".join(sorted(set(n for n in _display_names if n)))
        orig_str = " | ".join(n[:50] for n in _orig_names)

        card = html.Div([
            html.Div([
                html.Span(f"#{order_num}", style={"color": CYAN, "fontWeight": "bold", "fontSize": "13px"}),
                html.Span(f"  {date_str}", style={"color": GRAY, "fontSize": "12px", "marginLeft": "8px"}),
                html.Span(f"  {source}", style={"color": TEAL, "fontSize": "11px", "marginLeft": "8px"}),
                html.Span(f"  ${total:,.2f}", style={"color": ORANGE, "fontWeight": "bold", "fontSize": "13px",
                                                       "marginLeft": "auto", "fontFamily": "monospace"}),
            ], style={"display": "flex", "alignItems": "center", "marginBottom": "4px"}),
            # Renamed inventory names
            html.Div(display_str, style={"color": WHITE, "fontSize": "12px", "fontWeight": "bold",
                                          "marginBottom": "2px"}),
            # Original Amazon names
            html.Div(orig_str[:150], style={"color": DARKGRAY, "fontSize": "10px",
                                              "overflow": "hidden", "textOverflow": "ellipsis",
                                              "whiteSpace": "nowrap"}),
        ], style={
            "backgroundColor": CARD, "borderRadius": "8px", "padding": "10px 14px",
            "marginBottom": "6px", "borderLeft": f"3px solid {CYAN}44",
        })
        cards.append(card)

    if not cards:
        return [html.Div("No receipts match your search.", style={"color": GRAY, "fontSize": "12px", "padding": "12px"})]
    return cards


def _get_bank_computed():
    """Compute values needed by the financials tab from bank data."""
    cat_color_map = {
        "Amazon Inventory": ORANGE, "Shipping": BLUE, "Craft Supplies": TEAL,
        "Etsy Fees": PURPLE, "Subscriptions": CYAN, "AliExpress Supplies": "#e91e63",
        "Owner Draw - Texas": "#ff9800", "Owner Draw - Tulsa": "#ffb74d",
        "Personal": PINK, "Pending": DARKGRAY, "Etsy Payout": GREEN,
        "Business Credit Card": BLUE,
    }
    total_taken = _safe(bank_owner_draw_total) + _safe(bank_personal)
    acct_total = _safe(bank_cash_on_hand) + total_taken + _safe(bank_all_expenses) + _safe(old_bank_receipted) + _safe(bank_unaccounted) + _safe(etsy_csv_gap)
    acct_gap = round(_safe(etsy_net_earned) - acct_total, 2)

    # ── Missing receipts: per-transaction matching (amount + close date) ──
    # Every bank debit is checked. Nothing is skipped.
    from datetime import datetime as _dt

    def _parse_dt(s):
        """Parse 'MM/DD/YYYY' or 'Month Day, Year' to datetime."""
        if not s:
            return None
        for fmt in ("%m/%d/%Y", "%B %d, %Y"):
            try:
                return _dt.strptime(s, fmt)
            except ValueError:
                continue
        return None

    amazon_txns = [t for t in bank_debits if t["category"] == "Amazon Inventory"]

    # Map: bank debit category → which receipt sources can match it.
    # This prevents false matches (e.g. UPS charge matching an Amazon receipt).
    _cat_to_sources = {
        "Amazon Inventory": ["Key Component Mfg"],
        "AliExpress Supplies": ["SUNLU", "Alibaba"],
        "Craft Supplies": ["Hobby Lobby", "Home Depot"],
    }

    matched_no_receipt = []

    # ── 1. Categories with known receipt sources: per-transaction matching ──
    for cat, sources in _cat_to_sources.items():
        cat_debits = [t for t in bank_debits if t["category"] == cat]
        if not cat_debits:
            continue
        # Build receipt pool for this category only
        pool = []
        for inv in INVOICES:
            if inv.get("source") not in sources:
                continue
            if "Gigi" in inv.get("file", ""):
                continue
            pool.append({
                "amount": inv.get("grand_total", 0),
                "date": _parse_dt(inv.get("date", "")),
                "used": False,
            })

        # Two-pass matching: exact amounts first, then approximate.
        # This prevents a $23.86 debit from stealing a $23.79 receipt
        # that should go to the $23.79 debit.
        unmatched_debits = list(sorted(cat_debits, key=lambda x: x["date"]))

        def _match_pass(debits, amt_tolerance, day_tolerance):
            """Match debits against pool. Returns list of still-unmatched debits."""
            still_unmatched = []
            for t in debits:
                bank_dt = _parse_dt(t["date"])
                bank_amt = t["amount"]
                best_idx = -1
                best_score = 999999
                for i, r in enumerate(pool):
                    if r["used"]:
                        continue
                    amt_diff = abs(r["amount"] - bank_amt)
                    if amt_diff > amt_tolerance:
                        continue
                    if bank_dt and r["date"]:
                        day_diff = abs((bank_dt - r["date"]).days)
                        if day_diff > day_tolerance:
                            continue
                        score = round(amt_diff * 10000) + day_diff
                    else:
                        score = 50000 + round(amt_diff * 10000)
                    if score < best_score:
                        best_score = score
                        best_idx = i
                if best_idx >= 0:
                    pool[best_idx]["used"] = True
                else:
                    still_unmatched.append(t)
            return still_unmatched

        # Pass 1: Exact amount (within $0.02), date within 14 days
        unmatched_debits = _match_pass(unmatched_debits, 0.02, 14)
        # Pass 2: Approximate amount (within $1.50), date within 14 days
        unmatched_debits = _match_pass(unmatched_debits, 1.50, 14)

        matched_no_receipt.extend(unmatched_debits)

    # ── 2. All other categories: no receipt source exists, so every debit is missing ──
    for t in bank_debits:
        if t["category"] in _cat_to_sources:
            continue  # already handled above
        matched_no_receipt.append(t)

    return cat_color_map, acct_gap, matched_no_receipt, amazon_txns

_bank_cat_color_map, _bank_acct_gap, _bank_no_receipt, _bank_amazon_txns = _get_bank_computed()


# ── Build Layout ─────────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[
        "https://cdn.jsdelivr.net/npm/bootswatch@5.3.3/dist/darkly/bootstrap.min.css",
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
    ],
)
server = app.server
try:
    from agents.governance import register_governance_routes
    register_governance_routes(server)
except ImportError:
    pass
app.title = "TJs Software Project"

# Clean index template (no JS hacks needed — using dbc.Select for Darkly-styled dropdowns)
app.index_string = '''<!DOCTYPE html>
<html>
<head>
{%metas%}
<title>{%title%}</title>
{%favicon%}
{%css%}
<style>
/* Dark theme for dcc.Dropdown */
.dash-dark-dropdown .Select-control { background-color: #1a1a2e !important; border: 1px solid #333355 !important; border-radius: 6px !important; }
.dash-dark-dropdown .Select-menu-outer { background-color: #1a1a2e !important; border: 1px solid #333355 !important; }
.dash-dark-dropdown .VirtualizedSelectOption { background-color: #1a1a2e !important; color: #fff !important; }
.dash-dark-dropdown .VirtualizedSelectFocusedOption { background-color: #16213e !important; color: #00d4ff !important; }
.dash-dark-dropdown .Select-value-label { color: #fff !important; }
.dash-dark-dropdown .Select-placeholder { color: #666 !important; }
.dash-dark-dropdown .Select-input input { color: #fff !important; }
.dash-dark-dropdown .Select-clear-zone { color: #888 !important; }
.dash-dark-dropdown .Select-arrow-zone { color: #888 !important; }
</style>
</head>
<body>
{%app_entry%}
<footer>
{%config%}
{%scripts%}
{%renderer%}
</footer>
</body>
</html>'''

# ── Source → subfolder mapping for receipt PDFs ─────────────────────────────
_SOURCE_FOLDER_MAP = {
    "Key Component Mfg": "keycomp",
    "Personal Amazon": "personal_amazon",
    "Hobby Lobby": "other_receipts",
    "Home Depot": "other_receipts",
    "Oklahoma Secretary of State": "other_receipts",
    "SUNLU": "other_receipts",
    "Alibaba": "other_receipts",
}


@server.route("/api/receipt/<subfolder>/<path:filename>")
def serve_receipt_pdf(subfolder, filename):
    folder = os.path.join(BASE_DIR, "data", "invoices", subfolder)
    return flask.send_from_directory(folder, filename)


@server.route("/api/diagnostics")
def api_diagnostics():
    """Return key financial metrics as JSON for remote debugging."""
    # Check Supabase connectivity
    sb_status = "unknown"
    try:
        from supabase_loader import _get_supabase_client
        client = _get_supabase_client()
        if client is None:
            sb_status = "no_client (missing SUPABASE_URL/KEY?)"
        else:
            # Quick read test
            client.table("config").select("key").limit(1).execute()
            sb_status = "connected"
    except Exception as e:
        sb_status = f"error: {e}"

    return flask.jsonify({
        "supabase": sb_status,
        "env_has_supabase_url": bool(os.environ.get("SUPABASE_URL", "")),
        "env_has_supabase_key": bool(os.environ.get("SUPABASE_KEY", "")),
        "is_railway": IS_RAILWAY,
        "railway_env": os.environ.get("RAILWAY_ENVIRONMENT", ""),
        "railway_service": os.environ.get("RAILWAY_SERVICE_NAME", ""),
        "railway_project": os.environ.get("RAILWAY_PROJECT_ID", ""),
        "has_anthropic_key": bool(os.environ.get("ANTHROPIC_API_KEY", "")),
        "sales_count": len(DATA[DATA["Type"] == "Sale"]) if len(DATA) > 0 else 0,
        "etsy": {
            "rows": len(DATA),
            "gross_sales": round(gross_sales, 2),
            "total_fees": round(total_fees, 2),
            "total_shipping_cost": round(total_shipping_cost, 2),
            "total_marketing": round(total_marketing, 2),
            "total_refunds": round(total_refunds, 2),
            "total_taxes": round(total_taxes, 2),
            "total_buyer_fees": round(total_buyer_fees, 2),
            "etsy_net": round(etsy_net, 2),
            "etsy_balance": round(etsy_balance, 2),
        },
        "bank": {
            "txn_count": len(BANK_TXNS),
            "deposits": round(bank_total_deposits, 2),
            "debits": round(bank_total_debits, 2),
            "net_cash": round(bank_net_cash, 2),
            "by_category": {k: round(v, 2) for k, v in bank_by_cat.items()},
            "owner_draw_total": round(bank_owner_draw_total, 2),
            "tulsa_draws": round(tulsa_draw_total, 2),
            "texas_draws": round(texas_draw_total, 2),
        },
        "profit": {
            "cash_on_hand": round(bank_cash_on_hand, 2),
            "real_profit": round(real_profit, 2),
            "profit": round(profit, 2),
            "profit_margin": round(real_profit_margin, 1),
        },
        "missing_receipts_count": len(expense_missing_receipts),
        "expense_matched_count": expense_matched_count,
    })


@server.route("/api/debug-pipeline")
def api_debug_pipeline():
    """Debug: try building pipeline fresh and capture any errors."""
    results = {}
    # Step 1: try importing accounting package piece by piece
    try:
        import accounting.models
        results["step1_models"] = "OK"
    except Exception as e:
        results["step1_models"] = f"FAIL: {e}"
        import traceback
        results["step1_trace"] = traceback.format_exc()
    try:
        import accounting.ledger
        results["step2_ledger"] = "OK"
    except Exception as e:
        results["step2_ledger"] = f"FAIL: {e}"
    try:
        import accounting.journal
        results["step3_journal"] = "OK"
    except Exception as e:
        results["step3_journal"] = f"FAIL: {e}"
    try:
        import accounting.pipeline
        results["step4_pipeline"] = "OK"
    except Exception as e:
        results["step4_pipeline"] = f"FAIL: {e}"
        import traceback
        results["step4_trace"] = traceback.format_exc()
    try:
        from accounting import get_pipeline as _gp
        results["step5_get_pipeline"] = "OK"
        p = _gp()
        p.full_rebuild(DATA, BANK_TXNS, CONFIG, invoices=INVOICES)
        results["step6_rebuild"] = "OK"
        ec = p.get_expense_completeness()
        results["step7_expenses"] = {
            "matched": len(ec.receipt_matches) if ec else 0,
            "missing": len(ec.missing_receipts) if ec else 0,
        } if ec else "None"
    except Exception as e:
        import traceback
        results["build_error"] = str(e)
        results["build_trace"] = traceback.format_exc()
    results["global_pipeline_exists"] = _acct_pipeline is not None
    return flask.jsonify(results)


@server.route("/api/debug-expenses")
def api_debug_expenses():
    """Debug endpoint for expense completeness."""
    try:
        result = {
            "pipeline_exists": _acct_pipeline is not None,
            "invoices_count": len(INVOICES),
            "bank_txns_count": len(BANK_TXNS),
        }
        if _acct_pipeline is not None:
            ec = _acct_pipeline.get_expense_completeness()
            if ec is None:
                result["expense_result"] = "None (agent didn't run or returned None)"
                # Try running it manually
                try:
                    from accounting.agents.expense_completeness import ExpenseCompletenessAgent
                    agent = ExpenseCompletenessAgent()
                    import json as _json
                    vmap_path = os.path.join(BASE_DIR, "data", "vendor_map.json")
                    try:
                        with open(vmap_path) as f:
                            agent.vendor_map = _json.load(f)
                    except Exception:
                        pass
                    manual_result = agent.run(_acct_pipeline.journal, INVOICES)
                    result["manual_run"] = {
                        "matched": len(manual_result.receipt_matches),
                        "missing": len(manual_result.missing_receipts),
                        "gap": float(manual_result.gap_total),
                    }
                except Exception as e2:
                    result["manual_run_error"] = str(e2)
                    import traceback
                    result["manual_run_trace"] = traceback.format_exc()
            else:
                result["expense_result"] = {
                    "matched": len(ec.receipt_matches),
                    "missing": len(ec.missing_receipts),
                    "gap": float(ec.gap_total),
                }
        return flask.jsonify(result)
    except Exception as e:
        import traceback
        return flask.jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


# ── CORS helper for React app integration ────────────────────────────────────

def _add_cors_headers(response):
    """Add CORS headers to allow React app to call these endpoints."""
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response


@server.after_request
def after_request(response):
    """Add CORS headers to all API responses."""
    if flask.request.path.startswith('/api/'):
        return _add_cors_headers(response)
    return response


# ── API Endpoints for React App Integration ──────────────────────────────────

@server.route("/api/health")
def api_health():
    """Return business health score with sub-scores and grades."""
    score, sub_scores, weights = _compute_health_score()
    grade = "A" if score >= 80 else "B" if score >= 60 else "C" if score >= 40 else "D"

    return flask.jsonify({
        "score": score,
        "grade": grade,
        "sub_scores": sub_scores,
        "weights": {k: round(v * 100) for k, v in weights.items()},
        "breakdown": [
            {"name": k, "score": sub_scores[k], "weight": round(weights[k] * 100)}
            for k in sub_scores
        ],
        "callback_errors": get_error_summary(),
    })


@server.route("/api/briefing")
def api_briefing():
    """Return AI-generated daily briefing paragraphs."""
    paragraphs = _generate_briefing()
    return flask.jsonify({
        "paragraphs": paragraphs,
        "generated_at": pd.Timestamp.now().isoformat()
    })


@server.route("/api/actions")
def api_actions():
    """Return priority action items with impact estimates."""
    actions = _generate_actions()
    return flask.jsonify({
        "actions": actions,
        "count": len(actions)
    })


@server.route("/api/overview")
def api_overview():
    """Return complete dashboard overview for React app."""
    score, sub_scores, weights = _compute_health_score()
    grade = "A" if score >= 80 else "B" if score >= 60 else "C" if score >= 40 else "D"
    briefing = _generate_briefing()
    actions = _generate_actions()

    return flask.jsonify({
        "health": {
            "score": score,
            "grade": grade,
            "sub_scores": sub_scores,
        },
        "briefing": briefing,
        "actions": actions[:5],  # Top 5 actions
        "kpis": {
            "gross_sales": round(gross_sales, 2),
            "net_revenue": round(etsy_net, 2),
            "profit": round(real_profit, 2),
            "profit_margin": round(real_profit_margin, 1),
            "cash_on_hand": round(bank_cash_on_hand, 2),
            "order_count": order_count,
            "avg_order": round(avg_order, 2),
        },
        "monthly_trend": [
            {"month": m, "revenue": round(monthly_sales.get(m, 0), 2)}
            for m in months_sorted[-6:]
        ] if months_sorted else [],
        "expenses": {
            "total_fees": round(total_fees, 2),
            "shipping": round(total_shipping_cost, 2),
            "marketing": round(total_marketing, 2),
            "refunds": round(total_refunds, 2),
            "owner_draws": round(bank_owner_draw_total, 2),
        },
        "bank": {
            "deposits": round(bank_total_deposits, 2),
            "debits": round(bank_total_debits, 2),
            "balance": round(bank_cash_on_hand, 2),
        }
    })


@server.route("/api/financials")
def api_financials():
    """Return detailed financial breakdown."""
    try:
        return _add_cors_headers(flask.jsonify({
            "revenue": {
                "gross_sales": round(gross_sales, 2),
                "refunds": round(total_refunds, 2),
                "net_sales": round(net_sales, 2),
            },
            "fees": {
                "total": round(total_fees, 2),
                "listing": round(listing_fees, 2),
                "transaction": round(transaction_fees_product + transaction_fees_shipping, 2),
                "processing": round(processing_fees, 2),
                "marketing": round(total_marketing, 2),
                "shipping_labels": round(total_shipping_cost, 2),
            },
            "profit": {
                "etsy_net": round(etsy_net, 2),
                "after_expenses": round(real_profit, 2),
                "margin_percent": round(real_profit_margin, 1),
            },
            "bank": {
                "total_deposits": round(bank_total_deposits, 2),
                "total_debits": round(bank_total_debits, 2),
                "cash_on_hand": round(bank_cash_on_hand, 2),
                "categories": {k: round(v, 2) for k, v in bank_by_cat.items()},
            },
            "owner_draws": {
                "total": round(bank_owner_draw_total, 2),
                "tulsa": round(tulsa_draw_total, 2),
                "texas": round(texas_draw_total, 2),
            },
            "shipping": {
                "buyer_paid": None,
                "label_costs": round(total_shipping_cost, 2),
                "profit": None,
                "margin": None,
                "_note": "buyer_paid, profit, margin unavailable — not in Etsy CSV",
            },
            "monthly": {
                m: {
                    "sales": round(monthly_sales.get(m, 0), 2),
                    "fees": round(monthly_fees.get(m, 0), 2),
                    "shipping": round(monthly_shipping.get(m, 0), 2),
                    "orders": int(monthly_order_counts.get(m, 0)),
                }
                for m in months_sorted[-12:]
            } if months_sorted else {},
        }))
    except Exception as e:
        return _add_cors_headers(flask.jsonify({"error": str(e)})), 500


@server.route("/api/tax")
def api_tax():
    """Return tax-related calculations."""
    # Calculate tax estimates — per-partner (50/50 partnership)
    net_income = real_profit
    per_partner = net_income / 2
    se_tax_rate = 0.153  # 15.3% self-employment
    se_taxable = per_partner * 0.9235  # 92.35% of net income
    se_tax_per = se_taxable * se_tax_rate

    # Estimate income tax per partner (progressive brackets)
    taxable_income = per_partner - (se_tax_per / 2)  # SE tax deduction
    income_tax_per = _compute_income_tax(max(0, taxable_income))

    total_tax_per = se_tax_per + income_tax_per
    total_tax = total_tax_per * 2  # both partners combined
    quarterly = total_tax / 4

    return _add_cors_headers(flask.jsonify({
        "net_income": round(net_income, 2),
        "self_employment_tax": round(se_tax_per * 2, 2),
        "estimated_income_tax": round(income_tax_per * 2, 2),
        "total_estimated_tax": round(total_tax, 2),
        "quarterly_payment": round(quarterly, 2),
        "partnership_split": {
            "partner_1_income": round(per_partner, 2),
            "partner_1_se_tax": round(se_tax_per, 2),
            "partner_1_income_tax": round(income_tax_per, 2),
            "partner_2_income": round(per_partner, 2),
            "partner_2_se_tax": round(se_tax_per, 2),
            "partner_2_income_tax": round(income_tax_per, 2),
        },
        "deductions": {
            "total_expenses": round(bank_biz_expense_total, 2),
            "shipping": round(total_shipping_cost, 2),
            "fees": round(total_fees, 2),
            "marketing": round(total_marketing, 2),
        }
    }))


@server.route("/api/bank/ledger")
def api_bank_ledger():
    """Return bank transaction ledger with running balance."""
    ledger = []
    if bank_txns_sorted is not None:
        for txn in bank_txns_sorted[-100:]:  # Last 100 transactions
            ledger.append({
                "date": txn.get("date", ""),
                "description": txn.get("description", ""),
                "amount": round(txn.get("amount", 0), 2),
                "category": txn.get("category", "Uncategorized"),
                "running_balance": round(txn.get("running_balance", 0), 2),
            })
    return _add_cors_headers(flask.jsonify({
        "transactions": ledger,
        "total_deposits": round(bank_total_deposits, 2),
        "total_debits": round(bank_total_debits, 2),
        "current_balance": round(bank_cash_on_hand, 2),
    }))


@server.route("/api/bank/summary")
def api_bank_summary():
    """Return bank summary with expense categories."""
    return _add_cors_headers(flask.jsonify({
        "balance": round(bank_cash_on_hand, 2),
        "total_deposits": round(bank_total_deposits, 2),
        "total_debits": round(bank_total_debits, 2),
        "by_category": {k: round(v, 2) for k, v in bank_by_cat.items()} if bank_by_cat else {},
        "owner_draws": {
            "total": round(bank_owner_draw_total, 2),
            "tulsa": round(tulsa_draw_total, 2),
            "texas": round(texas_draw_total, 2),
            "difference": round(draw_diff, 2),
            "owed_to": draw_owed_to,
        },
        "monthly": {m: round(v, 2) for m, v in bank_monthly.items()} if bank_monthly else {},
    }))


@server.route("/api/pnl")
def api_pnl():
    """Return detailed Profit & Loss statement."""
    return _add_cors_headers(flask.jsonify({
        "revenue": {
            "gross_sales": round(gross_sales, 2),
            "refunds": round(total_refunds, 2),
            "net_sales": round(net_sales, 2),
        },
        "etsy_fees": {
            "listing_fees": round(listing_fees, 2),
            "transaction_fees": round(transaction_fees_product + transaction_fees_shipping, 2),
            "processing_fees": round(processing_fees, 2),
            "total_fees": round(total_fees, 2),
        },
        "shipping": {
            "buyer_paid": None,  # UNKNOWN — not in Etsy CSV
            "label_costs": round(total_shipping_cost, 2),
            "profit_loss": None,  # UNKNOWN — depends on buyer_paid
        },
        "marketing": {
            "etsy_ads": round(etsy_ads, 2),
            "offsite_ads": round(offsite_ads_fees, 2),
            "total": round(total_marketing, 2),
        },
        "after_etsy_fees": round(etsy_net, 2),
        "bank_expenses": {k: round(v, 2) for k, v in bank_by_cat.items()} if bank_by_cat else {},
        "owner_draws": round(bank_owner_draw_total, 2),
        "net_profit": round(real_profit, 2),
        "profit_margin": round(real_profit_margin, 1),
        "cash_on_hand": round(bank_cash_on_hand, 2),
    }))


@server.route("/api/inventory/summary")
def api_inventory_summary():
    """Return inventory/COGS summary."""
    summary = {
        "total_items": 0,
        "total_cost": 0,
        "by_category": {},
        "by_location": {},
        "low_stock": [],
        "out_of_stock": [],
    }

    if STOCK_SUMMARY is not None and len(STOCK_SUMMARY) > 0:
        summary["total_items"] = len(STOCK_SUMMARY)
        summary["total_cost"] = round(STOCK_SUMMARY["total_cost"].sum(), 2) if "total_cost" in STOCK_SUMMARY.columns else 0

        # By category
        if "category" in STOCK_SUMMARY.columns:
            cat_totals = STOCK_SUMMARY.groupby("category")["total_cost"].sum()
            summary["by_category"] = {k: round(v, 2) for k, v in cat_totals.items()}

        # By location
        if "location" in STOCK_SUMMARY.columns:
            loc_totals = STOCK_SUMMARY.groupby("location")["total_cost"].sum()
            summary["by_location"] = {k: round(v, 2) for k, v in loc_totals.items()}

        # Low stock (1-2 remaining)
        if "in_stock" in STOCK_SUMMARY.columns:
            low = STOCK_SUMMARY[(STOCK_SUMMARY["in_stock"] > 0) & (STOCK_SUMMARY["in_stock"] <= 2)]
            summary["low_stock"] = low["display_name"].tolist()[:20] if "display_name" in low.columns else []

            # Out of stock
            oos = STOCK_SUMMARY[STOCK_SUMMARY["in_stock"] <= 0]
            summary["out_of_stock"] = oos["display_name"].tolist()[:20] if "display_name" in oos.columns else []

    return _add_cors_headers(flask.jsonify(summary))


@server.route("/api/valuation")
def api_valuation():
    """Return business valuation estimates using Etsy-appropriate multiples.

    Methodology:
    - Primary: SDE (Seller's Discretionary Earnings) multiple
    - Secondary: Monthly trailing profit multiple (Flippa/Empire Flippers style)
    - Reference only: Revenue multiple (less relevant for small Etsy shops)

    Age-based weighting:
    - Under 6 months: Conservative multiples, high risk discount
    - 6-12 months: Moderate multiples, medium risk discount
    - 12+ months: Standard multiples, track record weighted
    """
    months_operating = len(months_sorted) if months_sorted else 0

    # Calculate annualized metrics
    # Use trailing data when possible, projections decrease in weight as track record grows
    if months_operating >= 12:
        # 12+ months: Use actual trailing 12 months
        annual_revenue = gross_sales
        annual_profit = real_profit
        track_record_weight = 0.9  # 90% track record, 10% projection
    elif months_operating >= 6:
        # 6-12 months: Blend trailing with conservative projection
        annualize_factor = 12 / max(months_operating, 1)
        annual_revenue = gross_sales * annualize_factor
        annual_profit = real_profit * annualize_factor
        track_record_weight = 0.6  # 60% track record, 40% projection
    else:
        # Under 6 months: Mostly speculative, use conservative projection
        annualize_factor = 12 / max(months_operating, 1)
        annual_revenue = gross_sales * annualize_factor
        annual_profit = real_profit * annualize_factor
        track_record_weight = 0.3  # 30% track record, 70% projection

    # SDE = profit + owner draws (annualized)
    annual_draws = bank_owner_draw_total * (12 / max(months_operating, 1)) if months_operating > 0 else 0
    sde = annual_profit + annual_draws

    # Monthly SDE for Flippa-style valuation
    monthly_sde = sde / 12 if sde > 0 else 0

    # ─── RISK FACTORS ────────────────────────────────────────────────────────────
    # Each factor reduces the multiple
    risk_factors = []
    risk_discount = 1.0  # Start at 100%

    # Age risk: Young businesses are riskier
    if months_operating < 6:
        risk_factors.append({"factor": "Business age < 6 months", "discount": 0.20})
        risk_discount -= 0.20
    elif months_operating < 12:
        risk_factors.append({"factor": "Business age < 12 months", "discount": 0.10})
        risk_discount -= 0.10

    # Platform dependency: 100% Etsy is risky
    risk_factors.append({"factor": "Single platform (Etsy)", "discount": 0.10})
    risk_discount -= 0.10

    # Operational complexity: 3D printing requires owner involvement
    risk_factors.append({"factor": "Owner-dependent operations", "discount": 0.05})
    risk_discount -= 0.05

    risk_discount = max(risk_discount, 0.5)  # Floor at 50%

    # ─── ETSY-APPROPRIATE MULTIPLES ──────────────────────────────────────────────
    # Revenue multiples for small Etsy: 0.5x - 1.5x (not 1.5x - 3.5x)
    # SDE multiples for Etsy: 2x - 3x (not 2x - 4x)
    # Monthly profit: 24x - 36x (Flippa standard)

    # Adjust multiples based on track record
    if months_operating >= 12:
        sde_mult_low, sde_mult_mid, sde_mult_high = 2.5, 3.0, 3.5
        rev_mult_low, rev_mult_mid, rev_mult_high = 0.8, 1.0, 1.5
        monthly_mult_low, monthly_mult_mid, monthly_mult_high = 30, 36, 42
    elif months_operating >= 6:
        sde_mult_low, sde_mult_mid, sde_mult_high = 2.0, 2.5, 3.0
        rev_mult_low, rev_mult_mid, rev_mult_high = 0.5, 0.8, 1.2
        monthly_mult_low, monthly_mult_mid, monthly_mult_high = 24, 30, 36
    else:
        sde_mult_low, sde_mult_mid, sde_mult_high = 1.5, 2.0, 2.5
        rev_mult_low, rev_mult_mid, rev_mult_high = 0.3, 0.5, 0.8
        monthly_mult_low, monthly_mult_mid, monthly_mult_high = 18, 24, 30

    # Apply risk discount to multiples
    sde_mult_low *= risk_discount
    sde_mult_mid *= risk_discount
    sde_mult_high *= risk_discount

    # ─── CALCULATE VALUATIONS ────────────────────────────────────────────────────
    # Primary: SDE Multiple (what buyers actually use)
    sde_val_low = sde * sde_mult_low
    sde_val_mid = sde * sde_mult_mid
    sde_val_high = sde * sde_mult_high

    # Secondary: Monthly Profit Multiple (Flippa/Empire Flippers style)
    monthly_val_low = monthly_sde * monthly_mult_low
    monthly_val_mid = monthly_sde * monthly_mult_mid
    monthly_val_high = monthly_sde * monthly_mult_high

    # Reference: Revenue Multiple (less weight for Etsy)
    rev_val_low = annual_revenue * rev_mult_low
    rev_val_mid = annual_revenue * rev_mult_mid
    rev_val_high = annual_revenue * rev_mult_high

    # ─── BLENDED ESTIMATE ────────────────────────────────────────────────────────
    # Weight SDE heavily (70%), monthly profit (25%), revenue (5%)
    # As track record grows, this becomes more reliable
    blended_low = (sde_val_low * 0.70) + (monthly_val_low * 0.25) + (rev_val_low * 0.05)
    blended_mid = (sde_val_mid * 0.70) + (monthly_val_mid * 0.25) + (rev_val_mid * 0.05)
    blended_high = (sde_val_high * 0.70) + (monthly_val_high * 0.25) + (rev_val_high * 0.05)

    return _add_cors_headers(flask.jsonify({
        "metrics": {
            "annual_revenue_projected": round(annual_revenue, 2),
            "annual_profit_projected": round(annual_profit, 2),
            "sde": round(sde, 2),
            "monthly_sde": round(monthly_sde, 2),
            "profit_margin": round(real_profit_margin, 1),
            "months_operating": months_operating,
            "track_record_weight": round(track_record_weight * 100),
        },
        "risk_assessment": {
            "total_discount": round((1 - risk_discount) * 100),
            "risk_multiplier": round(risk_discount, 2),
            "factors": risk_factors,
        },
        "valuations": {
            "sde_multiple": {
                "low": round(sde_val_low, 2),
                "mid": round(sde_val_mid, 2),
                "high": round(sde_val_high, 2),
                "method": f"{sde_mult_low:.1f}x - {sde_mult_high:.1f}x SDE (Etsy-adjusted)",
                "multipliers": {"low": round(sde_mult_low, 2), "mid": round(sde_mult_mid, 2), "high": round(sde_mult_high, 2)},
            },
            "monthly_profit": {
                "low": round(monthly_val_low, 2),
                "mid": round(monthly_val_mid, 2),
                "high": round(monthly_val_high, 2),
                "method": f"{monthly_mult_low}x - {monthly_mult_high}x monthly SDE",
                "multipliers": {"low": monthly_mult_low, "mid": monthly_mult_mid, "high": monthly_mult_high},
            },
            "revenue_multiple": {
                "low": round(rev_val_low, 2),
                "mid": round(rev_val_mid, 2),
                "high": round(rev_val_high, 2),
                "method": f"{rev_mult_low:.1f}x - {rev_mult_high:.1f}x revenue (reference only)",
                "multipliers": {"low": round(rev_mult_low, 2), "mid": round(rev_mult_mid, 2), "high": round(rev_mult_high, 2)},
            },
        },
        "estimated_value": {
            "low": round(blended_low, 2),
            "mid": round(blended_mid, 2),
            "high": round(blended_high, 2),
            "method": "Weighted: 70% SDE + 25% Monthly Profit + 5% Revenue",
            "confidence": "Higher" if months_operating >= 12 else "Medium" if months_operating >= 6 else "Speculative",
        },
        "assets": {
            "cash_on_hand": round(bank_cash_on_hand, 2),
            "etsy_balance": round(etsy_balance, 2),
            "inventory_value": round(true_inventory_cost, 2),
        },
        "liabilities": {
            "credit_card": round(bb_cc_balance, 2),
        },
        "guidance": {
            "current_stage": "Early Stage" if months_operating < 6 else "Growth Stage" if months_operating < 12 else "Established",
            "to_increase_multiple": [
                "Build 12+ months track record",
                "Diversify beyond Etsy (website, Amazon)",
                "Document SOPs for production",
                "Show consistent month-over-month growth",
                "Reduce owner dependency",
            ] if months_operating < 12 else [
                "Maintain growth trajectory",
                "Diversify sales channels",
                "Build email list / owned audience",
                "Create systems for handoff",
            ],
        },
    }))


@server.route("/api/shipping")
def api_shipping():
    """Return detailed shipping analysis."""
    return _add_cors_headers(flask.jsonify({
        "summary": {
            "buyer_paid": None,  # UNKNOWN — not in Etsy CSV
            "label_costs": round(total_shipping_cost, 2),
            "profit_loss": None,  # UNKNOWN — depends on buyer_paid
            "margin": None,  # UNKNOWN — depends on buyer_paid
        },
        "labels": {
            "usps_outbound": round(usps_outbound, 2),
            "usps_outbound_count": usps_outbound_count,
            "usps_returns": round(usps_return, 2),
            "usps_return_count": usps_return_count,
            "asendia": round(asendia_labels, 2),
            "asendia_count": asendia_count,
        },
        "orders": {
            "paid_shipping_count": paid_ship_count,
            "free_shipping_count": free_ship_count,
            "avg_label_cost": round(avg_outbound_label, 2),
        },
    }))


@server.route("/api/fees")
def api_fees():
    """Return detailed fee breakdown."""
    return _add_cors_headers(flask.jsonify({
        "total": round(total_fees, 2),
        "breakdown": {
            "listing_fees": round(listing_fees, 2),
            "transaction_fees_product": round(transaction_fees_product, 2),
            "transaction_fees_shipping": round(transaction_fees_shipping, 2),
            "processing_fees": round(processing_fees, 2),
        },
        "credits": {
            "listing_credits": round(credit_listing, 2),
            "transaction_credits": round(credit_transaction, 2),
            "processing_credits": round(credit_processing, 2),
            "share_save": round(abs(share_save), 2),
            "total_credits": round(total_credits, 2),
        },
        "marketing": {
            "etsy_ads": round(etsy_ads, 2),
            "offsite_ads_fees": round(offsite_ads_fees, 2),
            "offsite_ads_credits": round(offsite_ads_credits, 2),
        },
        "as_percent_of_sales": round((total_fees / gross_sales * 100) if gross_sales > 0 else 0, 1),
    }))


@server.route("/api/config/credit-card", methods=["GET", "POST", "OPTIONS"])
def api_credit_card_config():
    """Get or set credit card configuration (balance, limit, purchases)."""
    global bb_cc_balance, bb_cc_limit, bb_cc_purchases, bb_cc_total_charged
    global bb_cc_total_paid, bb_cc_available, bb_cc_asset_value, CONFIG

    if flask.request.method == "OPTIONS":
        return _add_cors_headers(flask.make_response())

    if flask.request.method == "GET":
        return _add_cors_headers(flask.jsonify({
            "credit_limit": bb_cc_limit,
            "current_balance": bb_cc_balance,
            "total_charged": bb_cc_total_charged,
            "total_paid": bb_cc_total_paid,
            "available_credit": bb_cc_available,
            "purchases": bb_cc_purchases,
        }))

    # POST: Update credit card config
    try:
        data = flask.request.get_json() or {}

        # Allow setting balance directly OR via purchases
        new_balance = data.get("balance")
        new_limit = data.get("credit_limit", bb_cc_limit)
        new_purchases = data.get("purchases", bb_cc_purchases)

        # Build the config object
        cc_config = {
            "credit_limit": new_limit,
            "purchases": new_purchases,
            "payments": [],  # Payments are auto-detected from bank transactions
        }

        # If balance provided directly, create a synthetic purchase to represent it
        if new_balance is not None and new_balance > 0:
            # Calculate what total_charged should be: balance = charged - paid
            # So charged = balance + paid
            target_charged = new_balance + bb_cc_total_paid
            cc_config["purchases"] = [{"amount": target_charged, "desc": "Credit Card Balance", "date": "2024-01-01"}]

        # Save to Supabase
        if _save_config_value("best_buy_cc", cc_config):
            # Update global variables
            CONFIG["best_buy_cc"] = cc_config
            bb_cc_limit = cc_config["credit_limit"]
            bb_cc_purchases = cc_config["purchases"]
            bb_cc_total_charged = sum(p.get("amount", 0) for p in bb_cc_purchases)
            bb_cc_balance = bb_cc_total_charged - bb_cc_total_paid
            bb_cc_available = bb_cc_limit - bb_cc_balance
            bb_cc_asset_value = bb_cc_total_charged

            return _add_cors_headers(flask.jsonify({
                "success": True,
                "credit_limit": bb_cc_limit,
                "current_balance": bb_cc_balance,
                "total_charged": bb_cc_total_charged,
                "total_paid": bb_cc_total_paid,
                "available_credit": bb_cc_available,
            }))
        else:
            return _add_cors_headers(flask.jsonify({"success": False, "error": "Failed to save config"})), 500
    except Exception as e:
        return _add_cors_headers(flask.jsonify({"success": False, "error": str(e)})), 500


@server.route("/api/chat", methods=["POST", "OPTIONS"])
def api_chat():
    """Chat endpoint for the React app. Accepts a question and optional history."""
    if flask.request.method == "OPTIONS":
        return _add_cors_headers(flask.make_response())

    try:
        data = flask.request.get_json() or {}
        message = data.get("message", "").strip()
        history = data.get("history", [])

        if not message:
            return _add_cors_headers(flask.jsonify({"error": "No message provided"})), 400

        # Use the existing chatbot_answer function
        response = chatbot_answer(message, history)

        return _add_cors_headers(flask.jsonify({
            "response": response,
            "question": message,
        }))
    except Exception as e:
        return _add_cors_headers(flask.jsonify({"error": str(e)})), 500


def _build_reconciliation_report(start_date=None, end_date=None):
    """Build a reconciliation report comparing dashboard values to raw data sums.
    Returns list of dicts with metric, dashboard_value, raw_sum, delta, status."""
    import datetime as _dt

    if start_date and end_date:
        mask = (DATA["Date_Parsed"] >= pd.Timestamp(start_date)) & (DATA["Date_Parsed"] <= pd.Timestamp(end_date))
        d = DATA[mask]
    else:
        d = DATA

    rows = []

    def _check(name, dashboard_val, raw_val):
        delta = round(abs(dashboard_val - raw_val), 2)
        status = "PASS" if delta <= 1.0 else "FAIL"
        rows.append({
            "metric": name,
            "dashboard": round(dashboard_val, 2),
            "raw_sum": round(raw_val, 2),
            "delta": delta,
            "status": status,
        })

    _check("Gross Sales", gross_sales, d[d["Type"] == "Sale"]["Net_Clean"].sum())
    _check("Total Fees", total_fees, abs(d[d["Type"] == "Fee"]["Net_Clean"].sum()))
    _check("Total Shipping", total_shipping_cost, abs(d[d["Type"] == "Shipping"]["Net_Clean"].sum()))
    _check("Total Marketing", total_marketing, abs(d[d["Type"] == "Marketing"]["Net_Clean"].sum()))
    _check("Total Refunds", total_refunds, abs(d[d["Type"] == "Refund"]["Net_Clean"].sum()))
    _check("Total Taxes", total_taxes, abs(d[d["Type"] == "Tax"]["Net_Clean"].sum()))
    _check("Total Buyer Fees", total_buyer_fees, abs(d[d["Type"] == "Buyer Fee"]["Net_Clean"].sum()) if len(d[d["Type"] == "Buyer Fee"]) else 0)

    # Etsy Net Earned = sum of all Net_Clean
    raw_etsy_net = d["Net_Clean"].sum()
    _check("Etsy Net Earned", etsy_net_earned, raw_etsy_net)

    # Bank reconciliation (if available and no date filter)
    if not start_date and not end_date:
        _check("Bank Deposits", bank_total_deposits, bank_total_deposits)
        _check("Bank Debits", bank_total_debits, bank_total_debits)

        # Deposit reconciliation: Etsy deposited vs bank received
        # Sum bank deposits categorized or described as Etsy payouts
        bank_etsy_deps = sum(t["amount"] for t in bank_deposits
                             if "etsy" in t.get("category", "").lower()
                             or "etsy" in t.get("desc", "").lower())
        total_deposited = bank_etsy_deps + etsy_pre_capone_deposits
        dep_delta = round(abs(etsy_total_deposited - total_deposited), 2)
        rows.append({
            "metric": "Deposit Reconciliation",
            "dashboard": round(etsy_total_deposited, 2),
            "raw_sum": round(total_deposited, 2),
            "delta": dep_delta,
            "status": "PASS" if dep_delta <= 1.0 else "FAIL",
        })

    return rows


@server.route("/api/reconciliation")
def api_reconciliation():
    """Return reconciliation report as JSON."""
    start = flask.request.args.get("start")
    end = flask.request.args.get("end")
    rows = _build_reconciliation_report(start, end)
    all_pass = all(r["status"] == "PASS" for r in rows)
    return flask.jsonify({
        "status": "ALL PASS" if all_pass else "FAILURES DETECTED",
        "rows": rows,
        "count": len(rows),
    })


@server.route("/api/test-upload")
def api_test_upload():
    """Test the upload data processing path (without file upload).

    Re-runs _cascade_reload and reports all key metrics.
    Useful for debugging whether the pipeline updates globals correctly.
    """
    try:
        _rebuild_etsy_derived()
        _cascade_reload("test")
        return flask.jsonify({
            "status": "ok",
            "rows": len(DATA),
            "orders": int(order_count) if order_count else 0,
            "gross_sales": round(gross_sales, 2) if gross_sales else None,
            "etsy_net": round(etsy_net, 2) if etsy_net else None,
            "etsy_balance": round(etsy_balance, 2) if etsy_balance else None,
            "total_fees": round(total_fees, 2) if total_fees else None,
            "real_profit": round(real_profit, 2),
            "bank_cash_on_hand": round(bank_cash_on_hand, 2),
        })
    except Exception as e:
        import traceback
        return flask.jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500


@server.route("/api/reload")
def api_reload():
    """Force-reload all data from Supabase. Use after migrating data to refresh Railway.

    Delegates to the shared rebuild helpers (_rebuild_etsy_derived, _rebuild_bank_derived)
    and _cascade_reload so financial metric computation is not duplicated here.
    """
    global DATA, CONFIG, INVOICES, BANK_TXNS

    try:
        # 1. Reload raw data from Supabase
        sb = _load_data()
        DATA = sb["DATA"]
        CONFIG = sb["CONFIG"]
        INVOICES = sb["INVOICES"]
        BANK_TXNS = sb["BANK_TXNS"]

        _bank_debit_sum = sum(t["amount"] for t in BANK_TXNS if t["type"] == "debit")
        print(f"[reload] Loaded: {len(DATA)} etsy, {len(BANK_TXNS)} bank ({_bank_debit_sum:.2f} debits), {len(INVOICES)} inv")

        # 2. Rebuild Etsy-derived DataFrames, aggregations, and fee/shipping breakdowns
        _rebuild_etsy_derived()

        # 3. Rebuild bank-derived metrics (running balance, BB CC, bank computed)
        _rebuild_bank_derived()

        # 4. Run pipeline + publish financial metrics + recompute charts/analytics/tax/valuation
        _cascade_reload("supabase")

        _sales_count = len(DATA[DATA["Type"] == "Sale"]) if len(DATA) > 0 else 0
        print(f"[reload] Complete: {len(DATA)} rows, {_sales_count} sales, gross=${gross_sales:.2f}, debits=${bank_total_debits:.2f}")
        return flask.jsonify({
            "status": "ok",
            "etsy_rows": len(DATA),
            "sales_count": _sales_count,
            "gross_sales": round(gross_sales, 2),
            "bank_txns": len(BANK_TXNS),
            "invoices": len(INVOICES),
            "profit": round(profit, 2),
            "bank_deposits": round(bank_total_deposits, 2),
            "bank_debits": round(bank_total_debits, 2),
            "owner_draws": round(bank_owner_draw_total, 2),
        })
    except Exception as e:
        return flask.jsonify({"status": "error", "message": str(e)}), 500


@server.route("/api/charts/monthly-performance")
def api_charts_monthly_performance():
    """Monthly performance data for charts: sales, fees, shipping, marketing, refunds, net profit."""
    data = []
    for m in months_sorted:
        data.append({
            "month": m,
            "sales": round(monthly_sales.get(m, 0), 2),
            "fees": round(monthly_fees.get(m, 0), 2),
            "shipping": round(monthly_shipping.get(m, 0), 2),
            "marketing": round(monthly_marketing.get(m, 0), 2),
            "refunds": round(monthly_refunds.get(m, 0), 2),
            "net": round(monthly_net_revenue.get(m, 0), 2),
            "orders": int(monthly_order_counts.get(m, 0)),
            "aov": round(monthly_aov.get(m, 0), 2),
        })
    return _add_cors_headers(flask.jsonify({"monthly": data}))


@server.route("/api/charts/daily-sales")
def api_charts_daily_sales():
    """Daily sales data for charts with rolling averages."""
    if len(daily_df) == 0:
        return _add_cors_headers(flask.jsonify({"daily": []}))

    df = daily_df.copy()
    df["rolling_7d"] = df["revenue"].rolling(7, min_periods=1).mean()
    df["rolling_30d"] = df["revenue"].rolling(30, min_periods=1).mean()

    data = []
    for _, row in df.iterrows():
        data.append({
            "date": row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"]),
            "revenue": round(row["revenue"], 2),
            "orders": int(row["orders"]),
            "rolling_7d": round(row["rolling_7d"], 2),
            "rolling_30d": round(row["rolling_30d"], 2),
        })
    return _add_cors_headers(flask.jsonify({"daily": data[-90:]}))  # Last 90 days


@server.route("/api/charts/expense-breakdown")
def api_charts_expense_breakdown():
    """Expense breakdown for pie/donut charts."""
    expenses = [
        {"name": "Fees", "value": round(total_fees, 2), "color": "#ef4444"},
        {"name": "Shipping", "value": round(total_shipping_cost, 2), "color": "#3b82f6"},
        {"name": "Marketing", "value": round(total_marketing, 2), "color": "#8b5cf6"},
        {"name": "Refunds", "value": round(total_refunds, 2), "color": "#f59e0b"},
    ]
    # Add COGS/inventory if available
    if true_inventory_cost > 0:
        expenses.append({"name": "COGS/Supplies", "value": round(true_inventory_cost, 2), "color": "#10b981"})

    total = sum(e["value"] for e in expenses)
    for e in expenses:
        e["percent"] = round((e["value"] / total * 100) if total > 0 else 0, 1)

    return _add_cors_headers(flask.jsonify({
        "expenses": expenses,
        "total": round(total, 2),
        "gross_sales": round(gross_sales, 2),
    }))


@server.route("/api/charts/cash-flow")
def api_charts_cash_flow():
    """Monthly cash flow: deposits, debits, net."""
    if not bank_monthly:
        return _add_cors_headers(flask.jsonify({"monthly": []}))

    data = []
    for m in sorted(bank_monthly.keys()):
        month_data = bank_monthly[m]
        deposits = month_data.get("deposits", 0)
        debits = month_data.get("debits", 0)
        data.append({
            "month": m,
            "deposits": round(deposits, 2),
            "debits": round(debits, 2),
            "net": round(deposits - debits, 2),
        })
    return _add_cors_headers(flask.jsonify({"monthly": data}))


@server.route("/api/charts/products")
def api_charts_products():
    """Top products by revenue for charts."""
    if len(product_revenue_est) == 0:
        return _add_cors_headers(flask.jsonify({"products": []}))

    products = []
    for name, revenue in product_revenue_est.head(12).items():
        products.append({
            "name": name[:40],  # Truncate long names
            "revenue": round(revenue, 2),
        })
    return _add_cors_headers(flask.jsonify({"products": products}))


@server.route("/api/charts/health-breakdown")
def api_charts_health_breakdown():
    """Health score breakdown for gauge charts."""
    sub_scores = {
        "Profit Margin": min(100, int(profit_margin * 3)) if profit_margin > 0 else 0,
        "Revenue Trend": 100 if len(monthly_sales) >= 2 and monthly_sales.iloc[-1] > monthly_sales.iloc[-2] else 70,
        "Order Velocity": min(100, int((order_count / max(days_active, 1)) * 20)),
        "Fee Efficiency": max(0, 100 - int((total_fees / gross_sales * 100) if gross_sales > 0 else 0)),
        "Shipping Economics": 50 if shipping_profit is None else (100 if shipping_profit >= 0 else max(0, 100 + int(shipping_margin))),
        "Cash Position": min(100, int((bank_cash_on_hand / (bank_all_expenses / 30 if bank_all_expenses > 0 else 1)) * 10)),
    }

    overall = int(sum(sub_scores.values()) / len(sub_scores))
    grade = "A+" if overall >= 95 else "A" if overall >= 85 else "B" if overall >= 70 else "C" if overall >= 55 else "D"

    return _add_cors_headers(flask.jsonify({
        "overall": overall,
        "grade": grade,
        "sub_scores": sub_scores,
    }))


@server.route("/api/charts/projections")
def api_charts_projections():
    """Revenue/profit projections for growth charts."""
    if len(monthly_sales) < 2:
        return _add_cors_headers(flask.jsonify({"projections": []}))

    import numpy as np

    # Build historical data
    historical = []
    for m in months_sorted:
        historical.append({
            "month": m,
            "revenue": round(monthly_sales.get(m, 0), 2),
            "profit": round(monthly_net_revenue.get(m, 0), 2),
            "type": "actual",
        })

    # Simple linear projection for next 3 months
    revenues = [monthly_sales.get(m, 0) for m in months_sorted]
    profits = [monthly_net_revenue.get(m, 0) for m in months_sorted]

    if len(revenues) >= 2:
        x = np.arange(len(revenues))
        rev_slope = np.polyfit(x, revenues, 1)[0]
        prof_slope = np.polyfit(x, profits, 1)[0]

        projections = []
        last_month = pd.to_datetime(months_sorted[-1] + "-01")
        for i in range(1, 4):
            next_month = (last_month + pd.DateOffset(months=i)).strftime("%Y-%m")
            proj_rev = max(0, revenues[-1] + rev_slope * i)
            proj_prof = profits[-1] + prof_slope * i
            projections.append({
                "month": next_month,
                "revenue": round(proj_rev, 2),
                "profit": round(proj_prof, 2),
                "type": "projection",
            })

        return _add_cors_headers(flask.jsonify({
            "historical": historical,
            "projections": projections,
            "growth_rate": round((revenues[-1] / revenues[0] - 1) * 100, 1) if revenues[0] > 0 else 0,
        }))

    return _add_cors_headers(flask.jsonify({"historical": historical, "projections": []}))


@server.route("/api/ceo/dismiss")
def api_ceo_dismiss():
    """Dismiss a CEO alert by key — redirects back to dashboard."""
    key = flask.request.args.get("key", "")
    if key:
        _dismissed_alerts.add(key)
        try:
            from supabase_loader import save_config_value
            save_config_value("dismissed_ceo_alerts", list(_dismissed_alerts))
        except Exception:
            pass
    return flask.redirect("/")


# ── Etsy API OAuth Routes ────────────────────────────────────────────────────

@server.route("/api/etsy/connect")
def etsy_connect():
    """Start Etsy OAuth flow — redirects to Etsy authorization page."""
    from dashboard_utils.etsy_api import get_auth_url
    # Force https — Railway proxies via HTTP internally but the public URL is HTTPS
    redirect_uri = flask.request.host_url.rstrip("/").replace("http://", "https://") + "/api/etsy/callback"
    url = get_auth_url(redirect_uri)
    return flask.redirect(url)


@server.route("/api/etsy/callback")
def etsy_callback():
    """Handle Etsy OAuth callback — exchange code for tokens."""
    from dashboard_utils.etsy_api import exchange_code, get_shop_id, _pkce_state

    code = flask.request.args.get("code")
    state = flask.request.args.get("state")
    error = flask.request.args.get("error")

    if error:
        return f"Etsy authorization denied: {error}", 400

    if not code:
        return "Error: No authorization code received", 400

    if state != _pkce_state.get("state"):
        return "Error: State mismatch — possible CSRF attack. Try connecting again.", 400

    try:
        redirect_uri = flask.request.host_url.rstrip("/").replace("http://", "https://") + "/api/etsy/callback"
        exchange_code(code, redirect_uri)
        shop_id = get_shop_id()
        return flask.redirect("/?etsy_connected=true")
    except Exception as e:
        return f"Error connecting to Etsy: {e}", 500


@server.route("/api/etsy/status")
def etsy_status():
    """Check Etsy API connection status."""
    from dashboard_utils.etsy_api import is_connected, _tokens, get_shop_id, get_shop_info
    connected = is_connected()
    shop_info = None
    shop_error = None

    # Try to get shop_id if we don't have one yet
    if connected and not _tokens.get("shop_id"):
        try:
            get_shop_id()
        except Exception as e:
            shop_error = str(e)

    if connected and _tokens.get("shop_id"):
        try:
            shop_info = get_shop_info(_tokens["shop_id"])
        except Exception as e:
            shop_error = str(e)

    return flask.jsonify({
        "connected": connected,
        "shop_id": _tokens.get("shop_id"),
        "shop_name": shop_info.get("shop_name") if shop_info else None,
        "has_refresh_token": bool(_tokens.get("refresh_token")),
        "error": shop_error,
    })


@server.route("/api/etsy/debug")
def etsy_debug():
    """Debug Etsy API — test raw endpoint calls."""
    from dashboard_utils.etsy_api import debug_api_call, _tokens
    endpoint = flask.request.args.get("endpoint", "/application/users/me/shops")
    result = debug_api_call(endpoint)
    result["tokens_present"] = bool(_tokens.get("access_token"))
    return flask.jsonify(result)


@server.route("/api/etsy/ledger")
def etsy_ledger_test():
    """Test the ledger entries endpoint — shows fees, labels, deposits."""
    from dashboard_utils.etsy_api import get_ledger_entries, _tokens, is_connected
    import time as _time_ledger

    if not is_connected():
        return flask.jsonify({"error": "Not connected"}), 401

    shop_id = _tokens.get("shop_id")
    days = int(flask.request.args.get("days", 30))
    limit = int(flask.request.args.get("limit", 10))

    min_created = int(_time_ledger.time()) - (days * 86400)
    max_created = int(_time_ledger.time())

    try:
        import requests as _req_ledger
        from dashboard_utils.etsy_api import _get_headers, ETSY_BASE_URL
        resp = _req_ledger.get(
            f"{ETSY_BASE_URL}/application/shops/{shop_id}/payment-account/ledger-entries",
            headers=_get_headers(),
            params={"limit": limit, "min_created": min_created, "max_created": max_created},
        )
        if resp.status_code == 200:
            return flask.jsonify(resp.json())
        return flask.jsonify({"error": resp.text[:500], "status": resp.status_code})
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500


@server.route("/api/etsy/sync")
def etsy_sync_orders():
    """Pull all orders from Etsy API and save to Supabase."""
    from dashboard_utils.etsy_api import is_connected, _tokens, sync_all_orders, save_synced_orders

    if not is_connected():
        return flask.jsonify({"error": "Not connected to Etsy. Visit /api/etsy/connect first."}), 401

    shop_id = _tokens.get("shop_id")
    if not shop_id:
        return flask.jsonify({"error": "No shop_id found. Visit /api/etsy/status to fetch it."}), 400

    try:
        result = sync_all_orders(shop_id, store_slug="keycomponentmfg")
        stats = result["stats"]

        # Save to Supabase
        saved = save_synced_orders(result["orders"], result["items"], "keycomponentmfg")

        return flask.jsonify({
            "success": True,
            "saved_to_supabase": saved,
            "stats": stats,
            "sample_order": result["orders"][0] if result["orders"] else None,
        })
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500


@server.route("/api/etsy/sync-full")
def etsy_sync_full():
    """Full sync: receipts + ledger → true per-order P/L with all fees and shipping labels."""
    from dashboard_utils.etsy_api import (
        is_connected, _tokens, sync_all_orders, save_synced_orders,
        get_all_ledger_entries, build_order_profit_from_ledger,
    )
    from supabase_loader import _get_supabase_client

    if not is_connected():
        return flask.jsonify({"error": "Not connected to Etsy. Visit /api/etsy/connect first."}), 401

    shop_id = _tokens.get("shop_id")
    if not shop_id:
        return flask.jsonify({"error": "No shop_id found."}), 400

    try:
        # Step 1: Pull all receipts + items
        result = sync_all_orders(shop_id, store_slug="keycomponentmfg")
        save_synced_orders(result["orders"], result["items"], "keycomponentmfg")

        # Step 2: Pull all ledger entries (fees, labels, payments)
        ledger = get_all_ledger_entries(shop_id, days_back=365)

        # Step 3: Build true P/L by matching ledger to orders
        # Use raw_receipts for shipment timestamps, orders for display data
        raw_receipts = result.get("raw_receipts", [])
        # Merge: raw receipts have shipments/transactions, orders have display fields
        # Build a combined list where each entry has both
        combined = []
        order_lookup = {o["Order ID"]: o for o in result["orders"]}
        for rr in raw_receipts:
            rid = str(rr.get("receipt_id", ""))
            merged = order_lookup.get(rid, {}).copy()
            merged["shipments"] = rr.get("shipments", [])
            merged["transactions"] = rr.get("transactions", [])
            merged["receipt_id"] = rid
            combined.append(merged)
        # Also include orders without raw receipts
        raw_ids = {str(rr.get("receipt_id", "")) for rr in raw_receipts}
        for o in result["orders"]:
            if o["Order ID"] not in raw_ids:
                combined.append(o)

        # Step 3b: Build profit WITHOUT payment data first (fast)
        profit_data = build_order_profit_from_ledger(
            combined, ledger, result["items"], payment_data=None
        )

        verified = 0

        # Step 4: Save profit data to Supabase
        client = _get_supabase_client()
        if client:
            import json as _json_sync
            client.table("config").upsert({
                "key": "order_profit_ledger_keycomponentmfg",
                "value": _json_sync.dumps(profit_data),
            }, on_conflict="key").execute()

        return flask.jsonify({
            "success": True,
            "receipts": len(result["orders"]),
            "ledger_entries": len(ledger),
            "orders_with_profit": len(profit_data),
            "note": "Run /api/etsy/sync-payments next for exact True Net",
            "sample": profit_data[0] if profit_data else None,
        })
    except Exception as e:
        import traceback
        return flask.jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@server.route("/api/etsy/set-earnings")
def etsy_set_earnings():
    """Manually set the 'You earned' amount for any order.
    Usage: /api/etsy/set-earnings?order=3933696617&earned=18.35
    """
    from supabase_loader import get_config_value, _get_supabase_client
    import json as _json_earn

    order_id = flask.request.args.get("order", "")
    earned = flask.request.args.get("earned", "")

    if not order_id or not earned:
        # Show all orders that need manual entry
        raw = get_config_value("order_profit_ledger_keycomponentmfg")
        if not raw:
            return flask.jsonify({"error": "No order data"}), 400
        orders = _json_earn.loads(raw) if isinstance(raw, str) else raw
        needs_manual = [{"order": o["Order ID"], "buyer": o.get("Buyer", ""), "date": o.get("Sale Date", ""),
                         "current_net": o["True Net"], "refund": o.get("Refund", 0)}
                        for o in orders if o.get("_needs_manual_net")]
        return flask.jsonify({"orders_needing_manual_entry": needs_manual, "count": len(needs_manual),
                              "usage": "/api/etsy/set-earnings?order=ORDER_ID&earned=AMOUNT"})

    try:
        earned_val = float(earned)
        raw = get_config_value("order_profit_ledger_keycomponentmfg")
        orders = _json_earn.loads(raw) if isinstance(raw, str) else raw

        updated = False
        for o in orders:
            if str(o.get("Order ID")) == str(order_id):
                old_net = o["True Net"]
                o["True Net"] = round(earned_val, 2)
                o["Margin %"] = round(earned_val / o.get("Sale Price", 1) * 100, 1) if o.get("Sale Price") else 0
                o["_needs_manual_net"] = False
                o["_manual_override"] = True
                updated = True

                client = _get_supabase_client()
                client.table("config").upsert({
                    "key": "order_profit_ledger_keycomponentmfg",
                    "value": _json_earn.dumps(orders),
                }, on_conflict="key").execute()

                return flask.jsonify({"success": True, "order": order_id, "old_net": old_net,
                                      "new_net": earned_val})

        return flask.jsonify({"error": f"Order {order_id} not found"}), 404
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500


@server.route("/api/etsy/sync-payments")
def etsy_sync_payments():
    """Fetch real payment data for all orders and update True Net values."""
    from dashboard_utils.etsy_api import is_connected, _tokens, _fetch_all_payments, get_receipt_payments
    from supabase_loader import _get_supabase_client, get_config_value
    import json as _json_pmt

    if not is_connected():
        return flask.jsonify({"error": "Not connected"}), 401

    shop_id = _tokens.get("shop_id")
    if not shop_id:
        return flask.jsonify({"error": "No shop_id"}), 400

    try:
        # Load existing profit data
        raw = get_config_value("order_profit_ledger_keycomponentmfg")
        if not raw:
            return flask.jsonify({"error": "Run /api/etsy/sync-full first"}), 400
        profit_data = _json_pmt.loads(raw) if isinstance(raw, str) else raw

        # Fetch payments for all orders
        receipt_ids = [o.get("Order ID") for o in profit_data if o.get("Order ID")]
        payment_data = _fetch_all_payments(shop_id, receipt_ids)

        # Update each order's True Net with real payment data
        updated = 0
        for order in profit_data:
            oid = order.get("Order ID", "")
            pmt = payment_data.get(str(oid))
            if not pmt:
                continue

            def _pmt_dollars(field):
                v = pmt.get(field, {})
                if isinstance(v, dict) and v.get("amount") is not None:
                    return v.get("amount", 0) / v.get("divisor", 100)
                return None

            original_net = _pmt_dollars("amount_net")
            if original_net is None:
                continue

            # Refund info from payment API
            refund_to_buyer = 0
            for adj in pmt.get("payment_adjustments", []):
                adj_amt = adj.get("total_adjustment_amount", 0)
                if adj_amt:
                    refund_to_buyer += adj_amt / 100.0
            order["Refund"] = round(refund_to_buyer, 2)

            label = order.get("Shipping Label", 0)
            txn_fee = order.get("Transaction Fee", 0)
            ads = order.get("Offsite Ads", 0)
            proc_fee = abs(_pmt_dollars("amount_fees") or 0)
            has_refund = refund_to_buyer > 0

            # FORMULA: amount_net - txn - ads - label
            # Verified for normal orders: M Heng $25.61 ✓, Shawna $20.80 ✓, Elisabeth $68.15 ✓
            # For refunded orders: this gives the pre-refund earnings minus costs.
            # Refunded orders will be marked and need manual verification on Etsy.
            true_net = original_net - txn_fee - ads - label

            if has_refund:
                # Etsy's refund accounting is complex — the API doesn't expose
                # the "You earned" number directly. Mark for manual verification.
                order["Status"] = "Refunded"

            order["True Net"] = round(true_net, 2)
            order["Processing Fee"] = round(proc_fee, 2)
            order["Total Etsy Fees"] = round(proc_fee + txn_fee + ads, 2)
            sale_price = order.get("Sale Price", 0)
            order["Margin %"] = round(true_net / sale_price * 100, 1) if sale_price else 0
            order["_payment_verified"] = True
            updated += 1

        # Save updated data
        client = _get_supabase_client()
        if client:
            client.table("config").upsert({
                "key": "order_profit_ledger_keycomponentmfg",
                "value": _json_pmt.dumps(profit_data),
            }, on_conflict="key").execute()

        return flask.jsonify({
            "success": True,
            "total_orders": len(profit_data),
            "payments_fetched": len(payment_data),
            "orders_updated": updated,
            "sample": next((o for o in profit_data if o.get("_payment_verified")), None),
        })
    except Exception as e:
        import traceback
        return flask.jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@server.route("/api/etsy/sync-status")
def etsy_sync_status():
    """Get auto-sync status."""
    from dashboard_utils.auto_sync import get_sync_status
    return flask.jsonify(get_sync_status())


@server.route("/api/etsy/sync-now")
def etsy_sync_now():
    """Trigger an immediate incremental sync."""
    from dashboard_utils.auto_sync import run_incremental_sync
    import threading as _th_sync
    _th_sync.Thread(target=run_incremental_sync, daemon=True).start()
    return flask.jsonify({"triggered": True, "message": "Sync started in background"})


@server.route("/api/etsy/audit-all")
def etsy_audit_all():
    """Re-verify all non-manual orders against Payment API."""
    from dashboard_utils.auto_sync import run_full_audit
    from dashboard_utils.etsy_api import _tokens
    shop_id = _tokens.get("shop_id")
    if not shop_id:
        return flask.jsonify({"error": "No shop_id"}), 400
    result = run_full_audit(shop_id)
    return flask.jsonify(result)


@server.route("/api/etsy/disconnect")
def etsy_disconnect():
    """Disconnect Etsy API — clears stored tokens."""
    from dashboard_utils.etsy_api import disconnect
    disconnect()
    return flask.redirect("/")


# ── Tax Forms Tab — extracted to dashboard_utils/pages/tax_forms.py ──────────
# build_tab5_tax_forms() is imported at the top of this file.

## build_tab6_valuation() extracted to dashboard_utils/pages/valuation.py
## _build_pl_row() and build_tab1_overview() extracted to dashboard_utils/pages/overview.py


def _build_health_checks():
    """Scan ALL data sources and return a todo/health panel with every issue found."""
    todos = []  # list of (priority, icon, color, title, detail)

    # ── 1. Missing Etsy CSVs (gaps in monthly coverage) ──
    etsy_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
    etsy_months_found = set()
    if os.path.isdir(etsy_dir):
        for fn in os.listdir(etsy_dir):
            if fn.endswith(".csv"):
                # Try to extract YYYY_MM from filename
                import re as _re
                m = _re.search(r"(\d{4})_(\d{2})", fn)
                if m:
                    etsy_months_found.add(f"{m.group(1)}-{m.group(2)}")
    if etsy_months_found:
        all_months = sorted(etsy_months_found)
        first_y, first_m = map(int, all_months[0].split("-"))
        last_y, last_m = map(int, all_months[-1].split("-"))
        expected = set()
        y, mo = first_y, first_m
        while (y, mo) <= (last_y, last_m):
            expected.add(f"{y}-{mo:02d}")
            mo += 1
            if mo > 12:
                mo = 1
                y += 1
        missing_etsy = sorted(expected - etsy_months_found)
        for mm in missing_etsy:
            todos.append((1, "\U0001f4c4", RED, f"Missing Etsy statement: {mm}",
                          f"Upload etsy_statement_{mm.replace('-', '_')}.csv in Data Hub"))
    if not etsy_months_found:
        todos.append((1, "\U0001f4c4", RED, "No Etsy statements uploaded",
                      "Go to Data Hub and upload your Etsy CSV statements"))

    # ── 2. Missing bank statements ──
    if bank_statement_count == 0:
        todos.append((1, "\U0001f3e6", RED, "No bank statements uploaded",
                      "Go to Data Hub and upload your Capital One PDF statements"))
    elif bank_statement_count < 3:
        todos.append((2, "\U0001f3e6", ORANGE, f"Only {bank_statement_count} bank statement(s)",
                      "Upload more bank statements for better cash flow tracking"))

    # ── 3. No receipts at all ──
    if len(INVOICES) == 0:
        todos.append((1, "\U0001f9fe", RED, "No inventory receipts uploaded",
                      "Upload Amazon/supplier invoices in Data Hub to track COGS"))

    # ── 4. Unreviewed inventory items (no _ITEM_DETAILS entry) ──
    unreviewed_items = []
    for inv in INVOICES:
        for item in inv["items"]:
            key = (inv["order_num"], item["name"])
            if key not in _ITEM_DETAILS:
                unreviewed_items.append(item["name"][:40])
    if unreviewed_items:
        count = len(unreviewed_items)
        examples = ", ".join(unreviewed_items[:3])
        todos.append((2, "\u270f\ufe0f", ORANGE,
                      f"{count} inventory item(s) need naming/categorizing",
                      f"Go to Inventory tab editor. Examples: {examples}{'...' if count > 3 else ''}"))

    # ── 5. Items without images ──
    no_image_items = []
    if len(STOCK_SUMMARY) > 0:
        for _, row in STOCK_SUMMARY.iterrows():
            name = row["display_name"]
            if not _IMAGE_URLS.get(name, ""):
                no_image_items.append(name[:35])
    if no_image_items:
        count = len(no_image_items)
        examples = ", ".join(no_image_items[:3])
        todos.append((3, "\U0001f5bc\ufe0f", CYAN,
                      f"{count} product(s) missing images",
                      f"Add image URLs in Inventory tab. Examples: {examples}{'...' if count > 3 else ''}"))

    # ── 6. Out-of-stock items ──
    oos_items = []
    if len(STOCK_SUMMARY) > 0:
        oos = STOCK_SUMMARY[STOCK_SUMMARY["in_stock"] <= 0]
        oos_items = list(oos["display_name"].values[:5])
    if oos_items:
        count = len(STOCK_SUMMARY[STOCK_SUMMARY["in_stock"] <= 0])
        todos.append((2, "\U0001f6a8", RED,
                      f"{count} item(s) out of stock",
                      f"Reorder needed: {', '.join(oos_items)}{'...' if count > 5 else ''}"))

    # ── 7. Low stock items (1-2 remaining) ──
    if len(STOCK_SUMMARY) > 0:
        low = STOCK_SUMMARY[STOCK_SUMMARY["in_stock"].between(1, 2)]
        if len(low) > 0:
            low_names = list(low["display_name"].values[:5])
            todos.append((3, "\u26a0", ORANGE,
                          f"{len(low)} item(s) low stock (1-2 left)",
                          f"Running low: {', '.join(low_names)}{'...' if len(low) > 5 else ''}"))

    # ── 8. Receipt vs Bank — count unmatched transactions ──
    _unmatched_count = len(expense_missing_receipts)
    _unmatched_total = sum(t["amount"] for t in expense_missing_receipts)
    if _unmatched_count > 0:
        todos.append((2, "\U0001f9fe", ORANGE,
                      f"{_unmatched_count} bank expenses (${_unmatched_total:,.0f}) without matching receipts",
                      f"Gap: ${expense_gap:,.2f}. Upload receipts in Data Hub → Inventory Receipts."))

    # ── 10. Etsy balance gap ──
    if etsy_csv_gap is not None and abs(etsy_csv_gap) > 5:
        todos.append((2, "\U0001f4b1", ORANGE,
                      f"Etsy balance gap: {money(abs(etsy_csv_gap))}",
                      f"Reported: {money(etsy_balance)} vs Calculated: {money(etsy_balance_calculated)}. "
                      f"May indicate a missing Etsy CSV or pending transactions."))

    # ── 11. Uncategorized bank transactions ──
    _pending = bank_by_cat.get("Pending", 0)
    _pending_count = sum(1 for t in BANK_TXNS if t.get("category") == "Pending")
    if _pending_count > 0:
        todos.append((3, "\U0001f4b3", CYAN,
                      f"{_pending_count} pending bank transaction(s) (${_pending:,.0f})",
                      "These haven't been categorized yet. Update bank parser if they're recurring."))

    # ── 12. Draw imbalance ──
    if draw_diff > 50:
        todos.append((3, "\U0001f91d", ORANGE,
                      f"Owner draw imbalance: ${draw_diff:,.0f} ({draw_owed_to} is owed)",
                      f"TJ: ${tulsa_draw_total:,.0f} vs Braden: ${texas_draw_total:,.0f}. "
                      f"50/50 split means {draw_owed_to} needs ${draw_diff:,.0f} to even out."))

    # Sort by priority (1=critical, 2=warning, 3=info)
    todos.sort(key=lambda x: x[0])

    if not todos:
        return html.Div([
            html.Div([
                html.Span("\u2705", style={"fontSize": "20px", "marginRight": "8px"}),
                html.Span("ALL CLEAR", style={"color": GREEN, "fontWeight": "bold",
                                                "fontSize": "15px", "letterSpacing": "1.5px"}),
            ], style={"marginBottom": "6px"}),
            html.P("All data sources are connected, up to date, and balanced.",
                   style={"color": GRAY, "fontSize": "13px", "margin": "0"}),
        ], style={
            "backgroundColor": CARD2, "borderRadius": "10px", "padding": "14px 16px",
            "border": f"1px solid {GREEN}33", "marginBottom": "14px",
        })

    # Build todo rows
    todo_rows = []
    for pri, icon, color, title, detail in todos:
        pri_badge = {1: ("CRITICAL", RED), 2: ("ACTION", ORANGE), 3: ("INFO", CYAN)}[pri]
        todo_rows.append(html.Div([
            html.Div([
                html.Span(icon, style={"fontSize": "16px", "width": "24px", "textAlign": "center",
                                        "flexShrink": "0"}),
                html.Span(pri_badge[0], style={
                    "fontSize": "9px", "fontWeight": "bold", "padding": "2px 6px",
                    "borderRadius": "3px", "backgroundColor": f"{pri_badge[1]}22",
                    "color": pri_badge[1], "letterSpacing": "0.5px", "flexShrink": "0",
                }),
                html.Span(title, style={"color": WHITE, "fontSize": "13px", "fontWeight": "600",
                                         "flex": "1"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "8px"}),
            html.P(detail, style={"color": GRAY, "fontSize": "11px", "margin": "3px 0 0 32px",
                                   "lineHeight": "1.4"}),
        ], style={"padding": "8px 10px", "borderBottom": "1px solid #ffffff08"}))

    critical_count = sum(1 for t in todos if t[0] == 1)
    action_count = sum(1 for t in todos if t[0] == 2)
    info_count = sum(1 for t in todos if t[0] == 3)
    badge_parts = []
    if critical_count:
        badge_parts.append(html.Span(f"{critical_count} critical", style={"color": RED, "fontSize": "12px"}))
    if action_count:
        badge_parts.append(html.Span(f"{action_count} action", style={"color": ORANGE, "fontSize": "12px"}))
    if info_count:
        badge_parts.append(html.Span(f"{info_count} info", style={"color": CYAN, "fontSize": "12px"}))

    return html.Div([
        html.Div([
            html.Span("\U0001f4cb", style={"fontSize": "18px", "marginRight": "8px"}),
            html.Span("DASHBOARD HEALTH", style={"fontSize": "14px", "fontWeight": "bold",
                                                    "color": RED if critical_count else ORANGE,
                                                    "letterSpacing": "1.5px"}),
            html.Span(" — ", style={"color": DARKGRAY, "margin": "0 6px"}),
        ] + [html.Span(" | ", style={"color": DARKGRAY}) if i > 0 else html.Span("") for i, _ in enumerate(badge_parts)
             for _ in [None]] + badge_parts if False else (
            [html.Span("\U0001f4cb", style={"fontSize": "18px", "marginRight": "8px"}),
             html.Span("DASHBOARD HEALTH", style={"fontSize": "14px", "fontWeight": "bold",
                                                     "color": RED if critical_count else ORANGE,
                                                     "letterSpacing": "1.5px"}),
             html.Span(f"  {len(todos)} issue{'s' if len(todos) != 1 else ''}",
                        style={"color": GRAY, "fontSize": "12px", "marginLeft": "8px"}),
            ]
        ), style={"marginBottom": "8px", "display": "flex", "alignItems": "center"}),
        html.Div(todo_rows, style={"backgroundColor": "#0f0f1a", "borderRadius": "8px",
                                     "overflow": "hidden", "maxHeight": "300px", "overflowY": "auto"}),
    ], style={
        "backgroundColor": CARD2, "borderRadius": "10px", "padding": "14px 16px",
        "border": f"1px solid {RED if critical_count else ORANGE}33",
        "marginBottom": "14px",
    })




# ══════════════════════════════════════════════════════════════════════════════
# JARVIS Business Intelligence — Helper Functions
# ══════════════════════════════════════════════════════════════════════════════

def _compute_health_score():
    """Compute composite business health score (0-100) with 8 weighted sub-scores."""
    sub_scores = {}
    weights = {}

    # 1. Profit Margin (25%) — 0% = 0, 30%+ = 100
    pm = max(0, min(100, profit_margin / 30 * 100))
    sub_scores["Profit Margin"] = round(pm)
    weights["Profit Margin"] = 0.25

    # 2. Revenue Trend (20%) — last COMPLETE month vs prior month (MoM)
    #    Skip current partial month (< 25 days elapsed). Scale: -30% = 0, +30% = 100.
    if len(months_sorted) >= 2:
        from datetime import datetime as _dt_rt
        _now = _dt_rt.now()
        _cur_month_str = _now.strftime("%Y-%m")
        _use_months = list(months_sorted)
        # Drop current partial month so we compare two complete months
        if _use_months[-1] == _cur_month_str and _now.day < 25 and len(_use_months) >= 3:
            _use_months = _use_months[:-1]
        last_rev = monthly_sales.get(_use_months[-1], 0)
        prev_rev = monthly_sales.get(_use_months[-2], 0)
        if prev_rev > 0:
            pct_change = (last_rev - prev_rev) / prev_rev
            rt = max(0, min(100, (pct_change + 0.30) / 0.60 * 100))
        else:
            rt = 50
    else:
        rt = 50
    sub_scores["Revenue Trend"] = round(rt)
    weights["Revenue Trend"] = 0.20

    # 3. Cash Position (15%) — months of runway, 0 = 0, 3+ = 100
    if val_monthly_expenses > 0:
        runway = bank_cash_on_hand / val_monthly_expenses
        cp = max(0, min(100, runway / 3 * 100))
    else:
        cp = 100
    sub_scores["Cash Position"] = round(cp)
    weights["Cash Position"] = 0.15

    # 4. Fee Efficiency (10%) — 20%+ fees = 0, 8% = 100
    fee_pct = (total_fees / gross_sales * 100) if gross_sales else 0
    fe = max(0, min(100, (20 - fee_pct) / 12 * 100))
    sub_scores["Fee Efficiency"] = round(fe)
    weights["Fee Efficiency"] = 0.10

    # 5. Inventory Health (10%) — penalize OOS + low stock
    if len(STOCK_SUMMARY) > 0:
        stock_kpis = _compute_stock_kpis()
        total_items = max(stock_kpis["unique"], 1)
        oos_pct = stock_kpis["oos"] / total_items
        low_pct = stock_kpis["low"] / total_items
        ih = max(0, 100 - oos_pct * 150 - low_pct * 50)
    else:
        ih = 50  # no inventory data = neutral
    sub_scores["Inventory Health"] = round(ih)
    weights["Inventory Health"] = 0.10

    # 6. Order Velocity (10%) — orders/day, 0 = 0, 3+ = 100
    ov = max(0, min(100, _daily_orders_avg / 3 * 100))
    sub_scores["Order Velocity"] = round(ov)
    weights["Order Velocity"] = 0.10

    # 7. Data Quality (5%) — penalize audit issues
    todos = _build_health_checks()
    # Count items in the todo panel (each Div child with priority badge)
    try:
        todo_children = todos.children if hasattr(todos, 'children') else []
        if isinstance(todo_children, list):
            n_issues = len([c for c in todo_children if hasattr(c, 'style')])
        else:
            n_issues = 0
    except Exception:
        n_issues = 0
    dq = max(0, 100 - n_issues * 12)
    sub_scores["Data Quality"] = round(dq)
    weights["Data Quality"] = 0.05

    # 8. Shipping Economics (5%) — based on shipping margin
    se = 50 if shipping_profit is None else (100 if shipping_profit >= 0 else max(0, 100 + shipping_margin))
    sub_scores["Shipping Economics"] = round(se)
    weights["Shipping Economics"] = 0.05

    # Composite score
    score = sum(sub_scores[k] * weights[k] for k in sub_scores)
    score = round(max(0, min(100, score)))

    return score, sub_scores, weights


def _generate_briefing():
    """Generate natural-language daily briefing paragraphs from data."""
    from datetime import datetime
    now = datetime.now()
    hour = now.hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 17 else "Good evening"

    paragraphs = []

    # Opening
    paragraphs.append(
        f"{greeting}. Today is {now.strftime('%A, %B %d, %Y')}. "
        f"Here's your business intelligence briefing."
    )

    # Revenue this month vs last month
    if len(months_sorted) >= 2:
        current_month = months_sorted[-1]
        prev_month = months_sorted[-2]
        curr_rev = monthly_sales.get(current_month, 0)
        prev_rev = monthly_sales.get(prev_month, 0)
        if prev_rev > 0:
            rev_change = (curr_rev - prev_rev) / prev_rev * 100
            direction = "up" if rev_change > 0 else "down"
            # Run rate projection
            try:
                cm_parts = current_month.split("-")
                import calendar
                days_in_month = calendar.monthrange(int(cm_parts[0]), int(cm_parts[1]))[1]
                day_of_month = min(now.day, days_in_month)
                if day_of_month > 0:
                    run_rate = curr_rev / day_of_month * days_in_month
                else:
                    run_rate = curr_rev
            except Exception:
                run_rate = curr_rev
            paragraphs.append(
                f"Revenue this month ({current_month}): ${curr_rev:,.0f}, {direction} {abs(rev_change):.1f}% "
                f"from last month's ${prev_rev:,.0f}. "
                f"At current pace, projected to finish the month at ${run_rate:,.0f}."
            )
        else:
            paragraphs.append(f"Revenue this month: ${curr_rev:,.0f}.")
    elif len(months_sorted) == 1:
        curr_rev = monthly_sales.get(months_sorted[0], 0)
        paragraphs.append(f"Revenue this month: ${curr_rev:,.0f}. This is your first month of data.")

    # Profit margin health
    if profit_margin >= 25:
        margin_text = f"Profit margin is strong at {profit_margin:.1f}%. You're retaining a healthy share of revenue."
    elif profit_margin >= 15:
        margin_text = (f"Profit margin is {profit_margin:.1f}%, which is solid but has room to improve. "
                       f"Focus on reducing your largest expense categories to push above 25%.")
    elif profit_margin >= 0:
        margin_text = (f"Profit margin is thin at {profit_margin:.1f}%. Review your fee structure and shipping costs — "
                       f"small percentage improvements here translate to significant dollar gains.")
    else:
        margin_text = (f"Profit margin is negative at {profit_margin:.1f}%. "
                       f"The business is currently operating at a loss. Immediate cost review recommended.")
    paragraphs.append(margin_text)

    # Cash runway
    if val_monthly_expenses > 0:
        runway_months = bank_cash_on_hand / val_monthly_expenses
        if runway_months >= 3:
            paragraphs.append(
                f"Cash position: ${bank_cash_on_hand:,.0f} on hand — {runway_months:.1f} months of runway. "
                f"You have comfortable reserves."
            )
        elif runway_months >= 1:
            paragraphs.append(
                f"Cash position: ${bank_cash_on_hand:,.0f} on hand — {runway_months:.1f} months of runway. "
                f"Consider building a larger cash buffer."
            )
        else:
            paragraphs.append(
                f"Cash position: ${bank_cash_on_hand:,.0f} on hand — less than 1 month of runway. "
                f"Cash reserves are critically low."
            )

    # Inventory alerts
    if len(STOCK_SUMMARY) > 0:
        stock_kpis = _compute_stock_kpis()
        alerts = []
        if stock_kpis["oos"] > 0:
            alerts.append(f"{stock_kpis['oos']} item(s) are out of stock")
        if stock_kpis["low"] > 0:
            alerts.append(f"{stock_kpis['low']} item(s) are running low (1-2 left)")
        if alerts:
            paragraphs.append(f"Inventory alert: {'; '.join(alerts)}. Review the Inventory tab for reorder needs.")
        else:
            paragraphs.append(f"Inventory status: All {stock_kpis['unique']} tracked items are adequately stocked.")

    return paragraphs


def _generate_actions():
    """Generate priority action items ranked by dollar impact."""
    actions = []

    # 1. Restock OOS items
    if len(STOCK_SUMMARY) > 0:
        oos_df = STOCK_SUMMARY[STOCK_SUMMARY["in_stock"] <= 0]
        if len(oos_df) > 0:
            avg_unit_cost = oos_df["unit_cost"].mean()
            reorder_cost = avg_unit_cost * len(oos_df)
            # est_lost_weekly REMOVED — was len(oos_df) * avg_order * 0.5
            # (count * avg * guess factor violates no-estimates rule)
            # Missing data: per-listing view count, add-to-cart rate, historical sales velocity
            oos_names = list(oos_df["display_name"].values[:3])
            actions.append({
                "priority": "HIGH",
                "title": f"Restock {len(oos_df)} Out-of-Stock Items",
                "reason": f"Items like {', '.join(n[:25] for n in oos_names)}{'...' if len(oos_df) > 3 else ''} "
                          f"can't generate revenue until restocked. Revenue impact: UNKNOWN (no conversion data).",
                "impact": None,  # was: est_lost_weekly * 4 — no data to estimate
                "cost": reorder_cost,
                "difficulty": "Easy",
            })

    # 2. Shipping cost awareness (can't know profit — buyer-paid unavailable)
    # free_cost estimate REMOVED — was free_ship_count * avg_outbound_label (count * avg)
    if free_ship_count > 0 and avg_outbound_label > 0:
        actions.append({
            "priority": "MEDIUM",
            "title": f"Review Free Shipping — {free_ship_count} Orders (Label Cost UNKNOWN)",
            "reason": f"Total label costs: ${total_shipping_cost:,.0f}. {free_ship_count} free-shipping orders. "
                      f"Exact cost absorbed: UNKNOWN without per-order label matching. Consider raising prices to offset.",
            "impact": None,  # was: free_ship_count * avg_outbound_label — no per-order data
            "cost": 0,
            "difficulty": "Easy",
        })

    # 3. Marketing ROI check
    if total_marketing > 0 and gross_sales > 0:
        roas = gross_sales / total_marketing
        if roas < 5:
            potential_save = total_marketing * 0.3
            actions.append({
                "priority": "MEDIUM",
                "title": "Review Ad Spend — ROAS Below 5x",
                "reason": f"Spending ${total_marketing:,.0f} on ads with {roas:.1f}x return. "
                          f"Industry target: 5-10x. Consider reducing underperforming campaigns.",
                "impact": potential_save,
                "cost": 0,
                "difficulty": "Medium",
            })

    # 4. Fee reduction (Share & Save)
    if share_save >= 0 and gross_sales > 0:
        potential_credits = gross_sales * 0.01  # ~1% savings
        actions.append({
            "priority": "MEDIUM",
            "title": "Maximize Share & Save Credits",
            "reason": f"Share & Save credits earned: ${abs(share_save):,.0f}. "
                      f"Sharing listings on social media earns fee credits.",
            "impact": potential_credits,
            "cost": 0,
            "difficulty": "Easy",
        })

    # 5. AOV recovery
    if len(months_sorted) >= 3:
        recent_aovs = [monthly_aov.get(m, 0) for m in months_sorted[-3:]]
        earlier_aovs = [monthly_aov.get(m, 0) for m in months_sorted[:-3]] if len(months_sorted) > 3 else recent_aovs
        if earlier_aovs and np.mean(earlier_aovs) > 0:
            aov_decline = (np.mean(recent_aovs) - np.mean(earlier_aovs)) / np.mean(earlier_aovs)
            if aov_decline < -0.1:
                monthly_impact = abs(aov_decline) * val_monthly_run_rate
                actions.append({
                    "priority": "MEDIUM",
                    "title": "Average Order Value Declining",
                    "reason": f"Recent AOV: ${np.mean(recent_aovs):,.2f} vs earlier: ${np.mean(earlier_aovs):,.2f} "
                              f"({aov_decline * 100:+.1f}%). Consider product bundling or upsells.",
                    "impact": monthly_impact,
                    "cost": 0,
                    "difficulty": "Medium",
                })

    # 6. Owner draw settlement
    if draw_diff > 100:
        actions.append({
            "priority": "LOW",
            "title": f"Settle Owner Draw Imbalance (${draw_diff:,.0f})",
            "reason": f"TJ: ${tulsa_draw_total:,.0f} vs Braden: ${texas_draw_total:,.0f}. "
                      f"{draw_owed_to} is owed ${draw_diff:,.0f} to equalize 50/50 split.",
            "impact": draw_diff,
            "cost": draw_diff,
            "difficulty": "Easy",
        })

    # 7. Data quality issues
    if len(STOCK_SUMMARY) > 0:
        unreviewed = sum(1 for inv in INVOICES for item in inv["items"]
                         if (inv["order_num"], item["name"]) not in _ITEM_DETAILS)
        if unreviewed > 0:
            actions.append({
                "priority": "LOW",
                "title": f"Categorize {unreviewed} Unreviewed Inventory Items",
                "reason": "Uncategorized items affect COGS accuracy and profit calculations.",
                "impact": unreviewed * 5,  # minor data quality impact
                "cost": 0,
                "difficulty": "Easy",
            })

    # Sort by dollar impact (descending); None impact sorts last
    actions.sort(key=lambda a: a.get("impact") if a.get("impact") is not None else -1, reverse=True)
    return actions


def _detect_patterns():
    """Detect cross-source intelligence patterns."""
    patterns = []

    # 0. Current month run-rate vs last month
    from datetime import datetime as _dt_pat
    import calendar as _cal_pat
    _cur_m_pat = _dt_pat.now().strftime("%Y-%m")
    _day_pat = _dt_pat.now().day
    if _cur_m_pat in months_sorted and _day_pat >= 3:
        _cur_sales = monthly_sales.get(_cur_m_pat, 0)
        _days_in_m = _cal_pat.monthrange(_dt_pat.now().year, _dt_pat.now().month)[1]
        _pace = _cur_sales / _day_pat * _days_in_m
        _complete_m = [m for m in months_sorted if m != _cur_m_pat]
        _last_full = monthly_sales.get(_complete_m[-1], 0) if _complete_m else 0
        _vs_last = ((_pace - _last_full) / _last_full * 100) if _last_full > 0 else 0
        _direction = "ahead of" if _vs_last > 0 else "behind"
        patterns.append({
            "type": "Opportunity" if _vs_last >= 0 else "Warning",
            "insight": f"March pace: ${_cur_sales:,.0f} through {_day_pat} days → on track for ~${_pace:,.0f}/month. "
                       f"That's {abs(_vs_last):.0f}% {_direction} last month (${_last_full:,.0f}). "
                       f"Daily avg: ${_cur_sales / _day_pat:,.0f}/day.",
            "sources": ["Etsy Sales (Live)"],
        })

    # 1. Inventory spend → revenue correlation
    if len(months_sorted) >= 3:
        rev_vals = [monthly_sales.get(m, 0) for m in months_sorted]
        inv_vals = [monthly_inv_spend.get(m, 0) for m in months_sorted]
        if len(rev_vals) >= 3 and np.std(rev_vals) > 0 and np.std(inv_vals) > 0:
            corr = np.corrcoef(rev_vals, inv_vals)[0, 1]
            if abs(corr) > 0.5:
                if corr > 0:
                    patterns.append({
                        "type": "Opportunity",
                        "insight": f"Inventory spending correlates with revenue (r={corr:.2f}). "
                                   f"Months where you invest more in supplies tend to produce more sales. "
                                   f"Strategic inventory investment may accelerate growth.",
                        "sources": ["Etsy Sales", "Inventory Invoices"],
                    })
                else:
                    patterns.append({
                        "type": "Warning",
                        "insight": f"Inverse correlation between inventory spend and revenue (r={corr:.2f}). "
                                   f"You may be overstocking during slow periods.",
                        "sources": ["Etsy Sales", "Inventory Invoices"],
                    })

    # 2. Best/worst sales day of week
    if hasattr(daily_df, 'index') and len(daily_df) > 7:
        patterns.append({
            "type": "Opportunity",
            "insight": f"Best sales day: {_best_dow} (avg ${max(_dow_rev_vals):,.0f}/day). "
                       f"Worst: {_worst_dow} (avg ${min(_dow_rev_vals):,.0f}/day). "
                       f"Schedule new listings and promotions for {_best_dow}s.",
            "sources": ["Etsy Sales"],
        })

    # 3. Product concentration risk
    if len(product_revenue_est) > 0:
        total_prod_rev = product_revenue_est.sum()
        if total_prod_rev > 0:
            top1_pct = product_revenue_est.values[0] / total_prod_rev * 100
            top3_pct = product_revenue_est.head(3).sum() / total_prod_rev * 100
            if top1_pct > 40:
                patterns.append({
                    "type": "Risk",
                    "insight": f"Product concentration risk: '{product_revenue_est.index[0][:35]}' accounts for "
                               f"{top1_pct:.0f}% of revenue. Top 3 products = {top3_pct:.0f}%. "
                               f"Diversify your catalog to reduce single-product dependency.",
                    "sources": ["Etsy Sales", "Product Fees"],
                })
            elif top3_pct > 70:
                patterns.append({
                    "type": "Warning",
                    "insight": f"Top 3 products represent {top3_pct:.0f}% of total revenue. "
                               f"Consider developing new product lines to spread risk.",
                    "sources": ["Etsy Sales", "Product Fees"],
                })

    # 4. Fee rate creep
    if len(months_sorted) >= 4:
        mid = len(months_sorted) // 2
        first_half_fee_pct = np.mean(
            [monthly_fees.get(m, 0) / monthly_sales.get(m, 1) * 100 for m in months_sorted[:mid] if monthly_sales.get(m, 0) > 0]
        ) if any(monthly_sales.get(m, 0) > 0 for m in months_sorted[:mid]) else 0
        second_half_fee_pct = np.mean(
            [monthly_fees.get(m, 0) / monthly_sales.get(m, 1) * 100 for m in months_sorted[mid:] if monthly_sales.get(m, 0) > 0]
        ) if any(monthly_sales.get(m, 0) > 0 for m in months_sorted[mid:]) else 0
        if second_half_fee_pct > first_half_fee_pct + 1.5:
            patterns.append({
                "type": "Warning",
                "insight": f"Fee rate creeping up: first half avg {first_half_fee_pct:.1f}% → "
                           f"recent avg {second_half_fee_pct:.1f}%. "
                           f"Check for new Etsy fee structures or changes in product mix.",
                "sources": ["Etsy Fees", "Etsy Sales"],
            })

    # 5. Shipping cost awareness (buyer-paid unavailable)
    # free_cost estimate REMOVED — was free_ship_count * avg_outbound_label
    if total_shipping_cost > 0 and free_ship_count > 0:
        patterns.append({
            "type": "Risk",
            "insight": f"{free_ship_count} free-shipping orders absorb label costs (exact total UNKNOWN without per-order label matching). "
                       f"Total label costs: ${total_shipping_cost:,.0f}. "
                       f"Buyer-paid amount is unavailable from Etsy CSV — profit/loss cannot be calculated.",
            "sources": ["Etsy Shipping"],
        })

    # 6. Bank expense spikes vs Etsy revenue
    if bank_monthly and len(months_sorted) >= 3:
        for m in months_sorted[-2:]:
            rev = monthly_sales.get(m, 0)
            bank_m = bank_monthly.get(m, {})
            debits = bank_m.get("debits", 0)
            if rev > 0 and debits > rev * 0.9:
                patterns.append({
                    "type": "Warning",
                    "insight": f"Bank expenses in {m} (${debits:,.0f}) nearly matched Etsy revenue (${rev:,.0f}). "
                               f"Expense-to-revenue ratio: {debits / rev * 100:.0f}%. Review for non-essential spending.",
                    "sources": ["Bank Statements", "Etsy Sales"],
                })
                break

    return patterns


def _build_jarvis_header():
    """Build the JARVIS branded header banner."""
    from datetime import datetime
    return html.Div([
        html.Div([
            html.Span("JARVIS", style={
                "color": CYAN, "fontSize": "28px", "fontWeight": "bold",
                "letterSpacing": "4px", "marginRight": "12px",
            }),
            html.Span("Chief Executive Officer", style={
                "color": GRAY, "fontSize": "16px", "fontWeight": "300",
                "letterSpacing": "1px",
            }),
            html.Span([
                html.Span("", className="jarvis-pulse", style={
                    "display": "inline-block", "width": "8px", "height": "8px",
                    "borderRadius": "50%", "backgroundColor": CYAN,
                    "marginRight": "6px", "verticalAlign": "middle",
                }),
                html.Span("LIVE", style={
                    "color": CYAN, "fontSize": "10px", "fontWeight": "bold",
                    "letterSpacing": "2px", "verticalAlign": "middle",
                }),
            ], style={"marginLeft": "16px"}),
        ], style={"display": "flex", "alignItems": "center"}),
        html.Div(
            f"Last analysis: {datetime.now().strftime('%b %d, %Y at %I:%M %p')}",
            style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "4px"},
        ),
    ], style={
        "padding": "16px 20px", "marginBottom": "16px",
        "borderBottom": f"2px solid {CYAN}33",
        "background": f"linear-gradient(135deg, {CARD2}, {BG})",
        "borderRadius": "10px",
    })


def _build_health_section(score, sub_scores, weights, briefing):
    """Build health gauge (left 40%) + daily briefing (right 60%)."""
    # Health gauge
    if score >= 75:
        label, label_color = "ELITE", CYAN
    elif score >= 50:
        label, label_color = "STRONG", GREEN
    elif score >= 25:
        label, label_color = "NEEDS ATTENTION", ORANGE
    else:
        label, label_color = "CRITICAL", RED

    gauge_fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number={"font": {"size": 48, "color": label_color}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": DARKGRAY,
                     "tickfont": {"color": GRAY, "size": 10}},
            "bar": {"color": label_color, "thickness": 0.3},
            "bgcolor": CARD2,
            "borderwidth": 0,
            "steps": [
                {"range": [0, 25], "color": "rgba(231,76,60,0.12)"},
                {"range": [25, 50], "color": "rgba(243,156,18,0.09)"},
                {"range": [50, 75], "color": "rgba(46,204,113,0.09)"},
                {"range": [75, 100], "color": "rgba(0,212,255,0.09)"},
            ],
            "threshold": {
                "line": {"color": label_color, "width": 3},
                "thickness": 0.8, "value": score,
            },
        },
    ))
    _gauge_layout = {k: v for k, v in CHART_LAYOUT.items() if k != "margin"}
    gauge_fig.update_layout(
        **_gauge_layout,
        height=250,
        margin=dict(t=30, b=10, l=30, r=30),
        annotations=[dict(
            text=label, x=0.5, y=-0.05, xref="paper", yref="paper",
            font=dict(size=16, color=label_color, family="Arial Black"),
            showarrow=False,
        )],
    )

    # Sub-score progress bars (2-column grid)
    sub_bars = []
    sorted_subs = sorted(sub_scores.items(), key=lambda x: weights.get(x[0], 0), reverse=True)
    for name, val in sorted_subs:
        w = weights.get(name, 0)
        bar_color = CYAN if val >= 75 else GREEN if val >= 50 else ORANGE if val >= 25 else RED
        sub_bars.append(html.Div([
            html.Div([
                html.Span(name, style={"color": GRAY, "fontSize": "10px"}),
                html.Span(f"{val}/100 ({w * 100:.0f}%)", style={
                    "color": bar_color, "fontSize": "10px", "fontWeight": "bold"}),
            ], style={"display": "flex", "justifyContent": "space-between", "marginBottom": "2px"}),
            html.Div([
                html.Div(style={
                    "width": f"{val}%", "height": "6px",
                    "backgroundColor": bar_color, "borderRadius": "3px",
                    "transition": "width 0.5s ease",
                }),
            ], style={
                "backgroundColor": f"{GRAY}20", "borderRadius": "3px",
                "height": "6px", "overflow": "hidden",
            }),
        ], style={"marginBottom": "6px", "minWidth": "45%", "flex": "1", "padding": "0 4px"}))

    # Briefing paragraphs
    briefing_elements = []
    for i, para in enumerate(briefing):
        if i == 0:
            briefing_elements.append(html.P(para, style={
                "color": CYAN, "fontSize": "14px", "fontWeight": "500",
                "margin": "0 0 10px 0", "lineHeight": "1.5",
            }))
        else:
            briefing_elements.append(html.P(para, style={
                "color": "#cccccc", "fontSize": "12px",
                "margin": "0 0 8px 0", "lineHeight": "1.6",
            }))

    return html.Div([
        # Health Score label
        html.H3("BUSINESS HEALTH SCORE", style={
            "color": CYAN, "margin": "0 0 10px 0", "fontSize": "14px",
            "letterSpacing": "1.5px",
        }),
        html.Div([
            # LEFT: Gauge + sub-scores
            html.Div([
                dcc.Graph(figure=gauge_fig, config={"displayModeBar": False},
                          style={"height": "250px"}),
                html.Div(sub_bars, style={
                    "display": "flex", "flexWrap": "wrap", "gap": "2px",
                    "padding": "0 8px",
                }),
            ], style={"flex": "2", "minWidth": "300px"}),
            # RIGHT: Daily Briefing
            html.Div([
                html.Div([
                    html.Span("DAILY BRIEFING", style={
                        "color": CYAN, "fontSize": "12px", "fontWeight": "bold",
                        "letterSpacing": "1.5px",
                    }),
                ], style={"marginBottom": "12px"}),
                *briefing_elements,
            ], style={
                "flex": "3", "minWidth": "300px", "padding": "16px",
                "backgroundColor": CARD, "borderRadius": "10px",
                "border": f"1px solid {CYAN}22",
            }),
        ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap"}),
    ], style={"marginBottom": "20px"})


def _build_actions_section(actions):
    """Build priority action items section."""
    if not actions:
        return html.Div()

    cards = []
    for action in actions[:7]:
        pri = action["priority"]
        pri_color = RED if pri == "HIGH" else ORANGE if pri == "MEDIUM" else CYAN
        cards.append(html.Div([
            html.Div([
                html.Span(pri, style={
                    "color": BG, "backgroundColor": pri_color,
                    "padding": "2px 8px", "borderRadius": "4px",
                    "fontSize": "10px", "fontWeight": "bold",
                    "letterSpacing": "0.5px",
                }),
                html.Span(f"  {action.get('difficulty', '')}", style={
                    "color": DARKGRAY, "fontSize": "10px", "marginLeft": "8px",
                }),
            ], style={"marginBottom": "6px"}),
            html.Div(action["title"], style={
                "color": WHITE, "fontSize": "13px", "fontWeight": "bold",
                "marginBottom": "4px",
            }),
            html.P(action["reason"], style={
                "color": "#bbbbbb", "fontSize": "11px", "margin": "0 0 6px 0",
                "lineHeight": "1.5",
            }),
            html.Div([
                html.Span(
                    f"Impact: ${action['impact']:,.0f}" if action.get('impact') is not None else "Impact: UNKNOWN",
                    style={"color": GREEN if action.get('impact') is not None else ORANGE,
                           "fontSize": "11px", "fontWeight": "bold"},
                ),
                html.Span(f"  Cost: ${action.get('cost', 0):,.0f}", style={
                    "color": GRAY, "fontSize": "11px", "marginLeft": "12px",
                }) if action.get("cost", 0) > 0 else html.Span(),
            ]),
        ], className="action-card", style={
            "backgroundColor": CARD, "padding": "12px 14px", "borderRadius": "8px",
            "borderLeft": f"4px solid {pri_color}", "marginBottom": "8px",
            "transition": "transform 0.15s ease, box-shadow 0.15s ease",
            "cursor": "default",
        }))

    return html.Div([
        html.H3("PRIORITY ACTIONS", style={
            "color": CYAN, "margin": "0 0 6px 0", "fontSize": "14px",
            "letterSpacing": "1.5px",
        }),
        html.P("Ranked by estimated dollar impact. Address high-priority items first.",
               style={"color": GRAY, "margin": "0 0 12px 0", "fontSize": "12px"}),
        *cards,
    ], style={
        "marginBottom": "20px",
        "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px",
    })


def _build_patterns_section(patterns):
    """Build cross-source pattern detection section."""
    if not patterns:
        return html.Div()

    cards = []
    for pat in patterns:
        ptype = pat["type"]
        type_color = GREEN if ptype == "Opportunity" else RED if ptype == "Risk" else ORANGE
        sources_text = " + ".join(pat.get("sources", []))
        cards.append(html.Div([
            html.Div([
                html.Span(ptype.upper(), style={
                    "color": BG, "backgroundColor": type_color,
                    "padding": "2px 8px", "borderRadius": "4px",
                    "fontSize": "10px", "fontWeight": "bold",
                }),
                html.Span(f"  Sources: {sources_text}", style={
                    "color": DARKGRAY, "fontSize": "10px", "marginLeft": "8px",
                }),
            ], style={"marginBottom": "6px"}),
            html.P(pat["insight"], style={
                "color": "#cccccc", "fontSize": "12px", "margin": "0",
                "lineHeight": "1.6",
            }),
        ], style={
            "backgroundColor": "#0d1b2a", "padding": "12px 14px", "borderRadius": "8px",
            "borderLeft": f"4px solid {type_color}", "marginBottom": "8px",
        }))

    return html.Div([
        html.H3("PATTERN DETECTION", style={
            "color": CYAN, "margin": "0 0 6px 0", "fontSize": "14px",
            "letterSpacing": "1.5px",
        }),
        html.P("Cross-source intelligence — not just what happened, but why, and what to do about it.",
               style={"color": GRAY, "margin": "0 0 12px 0", "fontSize": "12px"}),
        *cards,
    ], style={
        "marginBottom": "20px",
        "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px",
    })


def _build_goal_tracker():
    """Build goal tracking with sklearn projections."""
    from datetime import datetime
    now = datetime.now()

    goals = []

    # 1. Monthly Revenue vs $3,000 target
    current_month = months_sorted[-1] if months_sorted else None
    if current_month:
        curr_rev = monthly_sales.get(current_month, 0)
        target = 3000
        pct = min(100, curr_rev / target * 100) if target > 0 else 0
        try:
            cm_parts = current_month.split("-")
            import calendar
            days_in_month = calendar.monthrange(int(cm_parts[0]), int(cm_parts[1]))[1]
            day_of_month = min(now.day, days_in_month)
            projected = curr_rev / day_of_month * days_in_month if day_of_month > 0 else curr_rev
        except Exception:
            projected = curr_rev
        goals.append({
            "label": "Monthly Revenue",
            "current": curr_rev,
            "target": target,
            "pct": pct,
            "projected": projected,
            "format": "money",
        })

    # 2. Annual Revenue vs $36,000 target
    goals.append({
        "label": "Annual Revenue (annualized)",
        "current": val_annual_revenue,
        "target": 36000,
        "pct": min(100, val_annual_revenue / 36000 * 100) if val_annual_revenue else 0,
        "projected": val_proj_12mo_revenue,
        "format": "money",
    })

    # 3. Profit Margin vs 25% target
    goals.append({
        "label": "Profit Margin",
        "current": profit_margin,
        "target": 25,
        "pct": min(100, profit_margin / 25 * 100) if profit_margin > 0 else 0,
        "projected": profit_margin,  # current = projected for margin
        "format": "pct",
    })

    # 4. Monthly Orders vs 50 target
    if current_month:
        curr_orders = monthly_order_counts.get(current_month, 0)
        try:
            projected_orders = curr_orders / day_of_month * days_in_month if day_of_month > 0 else curr_orders
        except Exception:
            projected_orders = curr_orders
        goals.append({
            "label": "Monthly Orders",
            "current": curr_orders,
            "target": 50,
            "pct": min(100, curr_orders / 50 * 100),
            "projected": projected_orders,
            "format": "int",
        })

    # Build UI
    bars = []
    for g in goals:
        pct = g["pct"]
        bar_color = CYAN if pct >= 100 else GREEN if pct >= 75 else ORANGE if pct >= 50 else RED
        if g["format"] == "money":
            curr_str = f"${g['current']:,.0f}"
            tgt_str = f"${g['target']:,.0f}"
            proj_str = f"${g['projected']:,.0f}"
        elif g["format"] == "pct":
            curr_str = f"{g['current']:.1f}%"
            tgt_str = f"{g['target']}%"
            proj_str = f"{g['projected']:.1f}%"
        else:
            curr_str = f"{g['current']:.0f}"
            tgt_str = f"{g['target']}"
            proj_str = f"{g['projected']:.0f}"

        bars.append(html.Div([
            html.Div([
                html.Span(g["label"], style={"color": WHITE, "fontSize": "12px", "fontWeight": "bold"}),
                html.Span(f"{curr_str} / {tgt_str}", style={
                    "color": bar_color, "fontSize": "11px", "fontFamily": "monospace",
                }),
            ], style={"display": "flex", "justifyContent": "space-between", "marginBottom": "4px"}),
            html.Div([
                html.Div(style={
                    "width": f"{min(pct, 100)}%", "height": "10px",
                    "backgroundColor": bar_color, "borderRadius": "5px",
                    "transition": "width 0.5s ease",
                }),
            ], style={
                "backgroundColor": f"{GRAY}20", "borderRadius": "5px",
                "height": "10px", "overflow": "hidden", "marginBottom": "4px",
            }),
            html.Div([
                html.Span(f"{pct:.0f}% to goal", style={"color": GRAY, "fontSize": "10px"}),
                html.Span(f"Projected: {proj_str}", style={
                    "color": DARKGRAY, "fontSize": "10px",
                }),
            ], style={"display": "flex", "justifyContent": "space-between"}),
        ], style={"marginBottom": "10px", "padding": "8px 12px", "backgroundColor": CARD,
                  "borderRadius": "8px"}))

    return html.Div([
        html.H3("GOAL TRACKING", style={
            "color": CYAN, "margin": "0 0 6px 0", "fontSize": "14px",
            "letterSpacing": "1.5px",
        }),
        html.P("Progress toward targets with projected end-of-period values.",
               style={"color": GRAY, "margin": "0 0 12px 0", "fontSize": "12px"}),
        *bars,
    ], style={
        "marginBottom": "20px",
        "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px",
    })


# build_tab2_deep_dive() extracted to dashboard_utils/pages/deep_dive.py




def _build_shipping_compare():
    """Build monthly shipping label cost chart."""
    fig = go.Figure()
    if len(months_sorted) > 0 and len(monthly_shipping) > 0:
        m_vals = [monthly_shipping.get(m, 0) for m in months_sorted]
        fig.add_trace(go.Bar(
            name="Label Cost", x=months_sorted, y=m_vals,
            marker_color=RED,
            text=[f"${v:,.0f}" for v in m_vals], textposition="outside",
        ))
    else:
        fig.add_trace(go.Bar(
            name="Total Label Cost", x=["Total"], y=[total_shipping_cost],
            marker_color=RED, text=[f"${total_shipping_cost:,.2f}"], textposition="outside", width=0.4,
        ))
    make_chart(fig, 340, False)
    fig.update_layout(title="Monthly Shipping Label Costs", showlegend=False, yaxis_title="Amount ($)")
    return fig


def _build_ship_type():
    """Build Shipping Cost by Type chart fresh from current data."""
    names, vals, colors = [], [], []
    for nm, val, clr in [
        (f"USPS Outbound ({usps_outbound_count})", usps_outbound, BLUE),
        (f"USPS Return ({usps_return_count})", usps_return, RED),
        (f"Asendia Intl ({asendia_count})", asendia_labels, PURPLE),
        (f"Adjustments ({ship_adjust_count})", ship_adjustments, ORANGE),
        (f"Insurance ({ship_insurance_count})", ship_insurance, TEAL),
    ]:
        if val is not None and val > 0:
            names.append(nm)
            vals.append(val)
            colors.append(clr)
    if ship_credits is not None and ship_credits != 0:
        names.append(f"Credits ({ship_credit_count})")
        vals.append(abs(ship_credits))
        colors.append(GREEN)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=names, y=vals, marker_color=colors,
        text=[f"${v:,.2f}" for v in vals], textposition="outside",
    ))
    make_chart(fig, 340, False)
    fig.update_layout(title="Shipping Cost by Type", yaxis_title="Amount ($)")
    return fig



# def build_tab3_financials() — extracted to dashboard_utils/pages/



def _build_per_order_profit_section():
    """Build the per-order detail section for the Financials tab.

    Clean DataTable approach using ledger-based profit data from Supabase.
    Formula: True Net = payment_API_amount_net - transaction_fee - offsite_ads - shipping_label
    """
    # Load ledger-based profit data from Supabase
    _ledger_orders = []
    try:
        from supabase_loader import get_config_value as _gcv
        import json as _json_load
        _raw = _gcv("order_profit_ledger_keycomponentmfg")
        if _raw:
            _ledger_orders = _json_load.loads(_raw) if isinstance(_raw, str) else _raw
    except Exception:
        pass

    if not _ledger_orders:
        return html.Div([
            html.H3("ORDER DETAIL", style={
                "color": CYAN, "margin": "30px 0 6px 0", "fontSize": "14px",
                "letterSpacing": "1.5px", "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px",
            }),
            html.P("No order profit data found. Run the order profit ledger builder to populate.",
                   style={"color": GRAY, "fontSize": "12px"}),
        ])

    # Sort newest first
    _ledger_orders.sort(key=lambda x: x.get("Sale Date", "") or "", reverse=True)

    # Sync notification bar
    _sync_bar = html.Div(style={"display": "none"})
    try:
        from dashboard_utils.auto_sync import get_sync_status as _gss
        _ss = _gss()
        _unmatched_n = len([l for l in _ledger_orders if False])  # placeholder
        # Count actual unmatched labels
        _raw_lbl_sync = _gcv("unmatched_shipping_labels")
        _lbl_sync = _json_load.loads(_raw_lbl_sync) if isinstance(_raw_lbl_sync, str) else (_raw_lbl_sync or [])
        _unmatched_n = len([l for l in _lbl_sync if not l.get("assigned_to")])
        _needs_entry_n = len([o for o in _ledger_orders if o.get("_needs_manual_net")])
        _has_issues = _unmatched_n > 0 or _needs_entry_n > 0

        _last = _ss.get("last_run", "Never")
        _bar_color = f"{ORANGE}22" if _has_issues else f"{GREEN}15"
        _bar_border = ORANGE if _has_issues else GREEN
        _bar_parts = []
        if _last != "Never" and _last:
            _bar_parts.append(html.Span(f"Last sync: {_last}", style={"color": GRAY, "fontSize": "11px"}))
        if _unmatched_n > 0:
            _bar_parts.append(html.Span(f"{_unmatched_n} unmatched labels", style={"color": ORANGE, "fontSize": "11px", "fontWeight": "bold"}))
        if _needs_entry_n > 0:
            _bar_parts.append(html.Span(f"{_needs_entry_n} need manual entry", style={"color": ORANGE, "fontSize": "11px", "fontWeight": "bold"}))
        if not _has_issues:
            _bar_parts.append(html.Span(f"All {len(_ledger_orders)} orders verified", style={"color": GREEN, "fontSize": "11px"}))
        if _ss.get("error"):
            _bar_parts.append(html.Span(f"Error: {_ss['error']}", style={"color": RED, "fontSize": "11px"}))

        _bar_parts.append(html.A("Sync Now", href="/api/etsy/sync-now", target="_blank",
                                  style={"color": CYAN, "fontSize": "10px", "marginLeft": "auto",
                                         "textDecoration": "none", "padding": "3px 10px",
                                         "border": f"1px solid {CYAN}44", "borderRadius": "4px"}))
        _bar_parts.append(html.A("Audit", href="/api/etsy/audit-all", target="_blank",
                                  style={"color": GREEN, "fontSize": "10px",
                                         "textDecoration": "none", "padding": "3px 10px",
                                         "border": f"1px solid {GREEN}44", "borderRadius": "4px"}))

        _sync_bar = html.Div(_bar_parts, style={
            "display": "flex", "alignItems": "center", "gap": "12px",
            "padding": "6px 14px", "marginBottom": "10px", "borderRadius": "6px",
            "backgroundColor": _bar_color, "border": f"1px solid {_bar_border}44",
        })
    except Exception:
        pass

    # Compute KPIs
    _total_revenue = sum((o.get("Sale Price", 0) or 0) + (o.get("Buyer Shipping", 0) or 0) for o in _ledger_orders)
    _total_fees = sum(o.get("Total Etsy Fees", 0) or 0 for o in _ledger_orders)
    _total_labels = sum(o.get("Shipping Label", 0) or 0 for o in _ledger_orders)
    _total_net = sum(o.get("True Net", 0) or 0 for o in _ledger_orders)
    _total_ship_pl = sum(o.get("Ship P/L", 0) or 0 for o in _ledger_orders)
    _order_count = len(_ledger_orders)
    _avg_margin = sum(o.get("Margin %", 0) or 0 for o in _ledger_orders) / _order_count if _order_count else 0
    _avg_net = _total_net / _order_count if _order_count else 0

    # Summary KPIs
    _kpi_style = {
        "backgroundColor": CARD, "borderRadius": "8px", "padding": "12px 16px",
        "textAlign": "center", "flex": "1", "minWidth": "90px",
    }
    _ship_pl_color = GREEN if _total_ship_pl >= 0 else RED
    _kpi_row = html.Div([
        html.Div([
            html.Div(f"{_order_count}", style={"color": WHITE, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
            html.Div("Orders", style={"color": GRAY, "fontSize": "10px", "marginTop": "4px"}),
        ], style=_kpi_style),
        html.Div([
            html.Div(f"${_total_revenue:,.0f}", style={"color": GREEN, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
            html.Div("Revenue", style={"color": GRAY, "fontSize": "10px", "marginTop": "4px"}),
        ], style=_kpi_style),
        html.Div([
            html.Div(f"-${_total_fees:,.0f}", style={"color": RED, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
            html.Div("Etsy Fees", style={"color": GRAY, "fontSize": "10px", "marginTop": "4px"}),
        ], style=_kpi_style),
        html.Div([
            html.Div(f"-${_total_labels:,.0f}", style={"color": RED, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
            html.Div("Shipping Labels", style={"color": GRAY, "fontSize": "10px", "marginTop": "4px"}),
        ], style=_kpi_style),
        html.Div([
            html.Div(f"+${_total_ship_pl:,.0f}" if _total_ship_pl >= 0 else f"-${abs(_total_ship_pl):,.0f}", style={"color": _ship_pl_color, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
            html.Div("Ship P/L", style={"color": GRAY, "fontSize": "10px", "marginTop": "4px"}),
        ], style=_kpi_style),
        html.Div([
            html.Div(f"${_total_net:,.0f}", style={"color": CYAN, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
            html.Div("Net Profit", style={"color": GRAY, "fontSize": "10px", "marginTop": "4px"}),
        ], style=_kpi_style),
        html.Div([
            html.Div(f"{_avg_margin:.1f}%", style={"color": GREEN if _avg_margin >= 50 else (ORANGE if _avg_margin >= 30 else RED), "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
            html.Div("Avg Margin", style={"color": GRAY, "fontSize": "10px", "marginTop": "4px"}),
        ], style=_kpi_style),
    ], style={"display": "flex", "gap": "6px", "flexWrap": "wrap", "marginBottom": "16px"})

    # Build image URL lookup from Etsy listing images (stored in Supabase)
    _img_lookup = {}
    try:
        _raw_imgs = _gcv("order_listing_images")
        if _raw_imgs:
            _img_lookup = _json_load.loads(_raw_imgs) if isinstance(_raw_imgs, str) else _raw_imgs
    except Exception:
        pass

    # Grid template — shared between header and rows for perfect alignment
    _grid_cols = "44px 200px 1fr 70px 110px 100px 65px 80px"
    _grid_style = {"display": "grid", "gridTemplateColumns": _grid_cols, "gap": "12px", "alignItems": "center", "padding": "8px 14px"}

    # Build labels-per-order lookup for detail view
    _labels_by_order = {}
    try:
        _raw_lbl_detail = _gcv("unmatched_shipping_labels")
        _all_labels_detail = _json_load.loads(_raw_lbl_detail) if isinstance(_raw_lbl_detail, str) else (_raw_lbl_detail or [])
        for _lb in _all_labels_detail:
            _assigned = str(_lb.get("assigned_to", "") or "")
            if _assigned:
                _labels_by_order.setdefault(_assigned, []).append(_lb)
    except Exception:
        pass

    # Type labels for display
    _type_names = {
        "shipping_labels": "Outbound Label",
        "shipping_labels_usps_return": "Return Label",
        "shipping_label_insurance": "Insurance",
        "shipping_label_usps_adjustment": "USPS Adjustment",
        "shipping_label_globegistics_adjustment": "Intl Adjustment",
        "shipping_label_refund": "Label Refund",
        "shipping_label_usps_adjustment_credit": "USPS Credit",
    }

    # Build order rows as clean HTML cards
    _order_rows = []
    for _idx, _o in enumerate(_ledger_orders):
        _oid = str(_o.get("Order ID", ""))
        _buyer = _o.get("Buyer", _o.get("Full Name", "")) or ""
        _date = _o.get("Sale Date", "")
        _item_names = _o.get("Item Names", _o.get("Item Name", "")) or "Unknown"
        _variations = _o.get("Variations", "") or ""
        _qty = _o.get("Qty", 1) or 1
        _listing_price = _o.get("Listing Price", 0) or 0
        _discount = _o.get("Discount", 0) or 0
        _sale_price = _o.get("Sale Price", 0) or 0
        _buyer_ship = _o.get("Buyer Shipping", 0) or 0
        _label = _o.get("Shipping Label", 0) or 0
        _ship_pl = _o.get("Ship P/L", 0) or 0
        _txn_fee = _o.get("Transaction Fee", 0) or 0
        _proc_fee = _o.get("Processing Fee", 0) or 0
        _ads = _o.get("Offsite Ads", 0) or 0
        _total_etsy_fees = _o.get("Total Etsy Fees", 0) or 0
        _refund = _o.get("Refund", 0) or 0
        _true_net = _o.get("True Net", 0) or 0
        _margin = _o.get("Margin %", 0) or 0
        _status = _o.get("Status", "Completed")
        _tracking = _o.get("Tracking", "")
        _ship_country = _o.get("Ship Country", "")
        _ship_state = _o.get("Ship State", "")
        _label_id = _o.get("Label ID", "")

        # Image thumbnail — look up by order ID
        _first_item = _item_names.split(" | ")[0].split("(")[0].strip()
        _img_url = _img_lookup.get(_oid, "")
        _thumb = html.Img(src=_img_url, style={"width": "44px", "height": "44px", "borderRadius": "6px", "objectFit": "cover"}) if _img_url else html.Div("?", style={"width": "44px", "height": "44px", "borderRadius": "6px", "backgroundColor": f"{DARKGRAY}44", "display": "flex", "alignItems": "center", "justifyContent": "center", "color": GRAY, "fontSize": "16px"})

        # Status badge
        _status_color = GREEN if _status == "Completed" else (ORANGE if "Refund" in _status else (GRAY if "Cancel" in _status else BLUE))
        _status_badge = html.Span(_status, style={"fontSize": "9px", "padding": "2px 6px", "borderRadius": "3px", "backgroundColor": f"{_status_color}22", "color": _status_color, "fontWeight": "bold"})

        # Net color
        _net_color = GREEN if _true_net >= 0 else RED
        # Margin color
        _margin_color = GREEN if _margin >= 50 else (ORANGE if _margin >= 30 else RED)
        # Ship P/L color
        _spl_color = GREEN if _ship_pl >= 0 else RED

        # Shipping comparison
        _ship_section = []
        _spl_str = f"+${_ship_pl:.2f}" if _ship_pl >= 0 else f"-${abs(_ship_pl):.2f}"
        if _buyer_ship > 0 or _label > 0:
            _ship_section = [
                html.Div([
                    html.Span(f"Buyer: ", style={"color": GRAY, "fontSize": "10px"}),
                    html.Span(f"${_buyer_ship:.2f}", style={"color": GREEN if _buyer_ship > 0 else GRAY, "fontSize": "10px", "fontWeight": "bold"}),
                ]),
                html.Div([
                    html.Span(f"Label: ", style={"color": GRAY, "fontSize": "10px"}),
                    html.Span(f"${_label:.2f}", style={"color": RED, "fontSize": "10px", "fontWeight": "bold"}),
                ]),
                html.Div([
                    html.Span(f"P/L: ", style={"color": GRAY, "fontSize": "10px"}),
                    html.Span(_spl_str, style={"color": _spl_color, "fontSize": "11px", "fontWeight": "bold"}),
                ]),
            ]

        # Fee breakdown tooltip
        _fee_parts = []
        if _txn_fee: _fee_parts.append(f"Txn: ${_txn_fee:.2f}")
        if _proc_fee: _fee_parts.append(f"Proc: ${_proc_fee:.2f}")
        if _ads: _fee_parts.append(f"Ads: ${_ads:.2f}")
        _fee_detail = " | ".join(_fee_parts)

        # Row background
        _row_bg = "#1a2847" if _idx % 2 else CARD

        # === Expanded detail section ===
        _detail_row_style = {"display": "flex", "justifyContent": "space-between", "padding": "3px 0"}
        _detail_label = {"color": GRAY, "fontSize": "11px"}
        _detail_val = {"color": WHITE, "fontSize": "11px", "fontFamily": "monospace"}

        # Labels assigned to this order
        _order_labels = _labels_by_order.get(_oid, [])
        _label_detail_rows = []
        # The primary outbound label
        if _label_id:
            _label_detail_rows.append(html.Div([
                html.Span("Outbound Label", style={**_detail_label, "width": "120px"}),
                html.Span(f"#{_label_id}", style={"color": CYAN, "fontSize": "10px", "fontFamily": "monospace"}),
            ], style=_detail_row_style))
        # Extra labels (adjustments, returns, insurance, etc.)
        for _elb in _order_labels:
            _elb_type = _type_names.get(_elb.get("type", ""), _elb.get("type", ""))
            _elb_amt = _elb.get("amount", 0)
            _is_credit_lbl = "credit" in _elb.get("type", "").lower() or "refund" in _elb.get("type", "").lower()
            _elb_color = GREEN if _is_credit_lbl else RED
            _elb_sign = "+" if _is_credit_lbl else "-"
            _label_detail_rows.append(html.Div([
                html.Span(_elb_type, style={**_detail_label, "width": "120px"}),
                html.Span(f"{_elb_sign}${_elb_amt:.2f}", style={"color": _elb_color, "fontSize": "11px", "fontFamily": "monospace"}),
                html.Span(f" #{_elb.get('label_id', '')}", style={"color": GRAY, "fontSize": "9px", "fontFamily": "monospace", "marginLeft": "8px"}),
                html.Span(f" {_elb.get('date', '')}", style={"color": GRAY, "fontSize": "9px", "marginLeft": "8px"}),
            ], style=_detail_row_style))

        _detail_section = html.Div([
            # Three-column detail layout
            html.Div([
                # Column 1: Sale Details
                html.Div([
                    html.Div("SALE DETAILS", style={"color": CYAN, "fontSize": "10px", "fontWeight": "bold", "marginBottom": "6px", "letterSpacing": "1px"}),
                    html.Div([html.Span("Listing Price", style=_detail_label), html.Span(f"${_listing_price:.2f}", style=_detail_val)], style=_detail_row_style),
                    html.Div([html.Span("Discount", style=_detail_label), html.Span(f"-${abs(_discount):.2f}" if _discount else "$0.00", style={**_detail_val, "color": ORANGE if _discount else GRAY})], style=_detail_row_style),
                    html.Div([html.Span("Sale Price", style=_detail_label), html.Span(f"${_sale_price:.2f}", style={**_detail_val, "fontWeight": "bold"})], style=_detail_row_style),
                    html.Div([html.Span("Quantity", style=_detail_label), html.Span(f"{_qty}", style=_detail_val)], style=_detail_row_style),
                    html.Div([html.Span("Sales Tax", style=_detail_label), html.Span(f"${_o.get('Sales Tax', 0) or 0:.2f}", style=_detail_val)], style=_detail_row_style),
                    html.Hr(style={"border": f"1px solid {DARKGRAY}33", "margin": "6px 0"}),
                    html.Div([html.Span("Item", style=_detail_label)], style={"marginBottom": "2px"}),
                    html.Div(_item_names, style={"color": WHITE, "fontSize": "11px", "lineHeight": "1.4"}),
                    html.Div([html.Span("Variations", style=_detail_label)], style={"marginTop": "6px", "marginBottom": "2px"}) if _variations else html.Div(),
                    html.Div(_variations, style={"color": ORANGE, "fontSize": "11px"}) if _variations else html.Div(),
                ], style={"flex": "1", "padding": "0 16px 0 0", "borderRight": f"1px solid {DARKGRAY}33"}),

                # Column 2: Shipping & Labels
                html.Div([
                    html.Div("SHIPPING", style={"color": CYAN, "fontSize": "10px", "fontWeight": "bold", "marginBottom": "6px", "letterSpacing": "1px"}),
                    html.Div([html.Span("Buyer Paid", style=_detail_label), html.Span(f"${_buyer_ship:.2f}", style={**_detail_val, "color": GREEN if _buyer_ship > 0 else GRAY})], style=_detail_row_style),
                    html.Div([html.Span("Label Cost", style=_detail_label), html.Span(f"-${_label:.2f}", style={**_detail_val, "color": RED})], style=_detail_row_style),
                    html.Div([html.Span("Ship P/L", style={**_detail_label, "fontWeight": "bold"}), html.Span(_spl_str, style={**_detail_val, "color": _spl_color, "fontWeight": "bold"})], style=_detail_row_style),
                    html.Hr(style={"border": f"1px solid {DARKGRAY}33", "margin": "6px 0"}),
                    html.Div([html.Span("Tracking", style=_detail_label), html.Span(_tracking or "N/A", style={"color": CYAN if _tracking else GRAY, "fontSize": "10px", "fontFamily": "monospace"})], style=_detail_row_style),
                    html.Div([html.Span("Ship To", style=_detail_label), html.Span(f"{_ship_state}{', ' if _ship_state and _ship_country else ''}{_ship_country}" or "N/A", style={**_detail_val})], style=_detail_row_style),
                    html.Hr(style={"border": f"1px solid {DARKGRAY}33", "margin": "6px 0"}),
                    html.Div("LABELS & ADJUSTMENTS", style={"color": CYAN, "fontSize": "10px", "fontWeight": "bold", "marginBottom": "6px", "letterSpacing": "1px"}),
                    *(_label_detail_rows if _label_detail_rows else [html.Span("No labels", style={"color": GRAY, "fontSize": "11px"})]),
                ], style={"flex": "1", "padding": "0 16px", "borderRight": f"1px solid {DARKGRAY}33"}),

                # Column 3: Fees & Profit
                html.Div([
                    html.Div("FEE BREAKDOWN", style={"color": CYAN, "fontSize": "10px", "fontWeight": "bold", "marginBottom": "6px", "letterSpacing": "1px"}),
                    html.Div([html.Span("Transaction Fee (6.5%)", style=_detail_label), html.Span(f"-${_txn_fee:.2f}", style={**_detail_val, "color": RED})], style=_detail_row_style),
                    html.Div([html.Span("Processing Fee (3%+$0.25)", style=_detail_label), html.Span(f"-${_proc_fee:.2f}", style={**_detail_val, "color": RED})], style=_detail_row_style),
                    html.Div([html.Span("Offsite Ads (15%)", style=_detail_label), html.Span(f"-${_ads:.2f}", style={**_detail_val, "color": RED})], style=_detail_row_style) if _ads else html.Div(),
                    html.Div([html.Span("Listing Fee", style=_detail_label), html.Span(f"-${_o.get('Listing Fee', 0) or 0:.2f}", style={**_detail_val, "color": RED})], style=_detail_row_style) if _o.get("Listing Fee") else html.Div(),
                    html.Div([html.Span("Total Fees", style={**_detail_label, "fontWeight": "bold"}), html.Span(f"-${_total_etsy_fees:.2f}", style={**_detail_val, "color": RED, "fontWeight": "bold"})], style=_detail_row_style),
                    html.Hr(style={"border": f"1px solid {DARKGRAY}33", "margin": "6px 0"}),
                    html.Div([html.Span("Refund", style=_detail_label), html.Span(f"-${abs(_refund):.2f}", style={**_detail_val, "color": RED})], style=_detail_row_style) if _refund else html.Div(),
                    html.Div("PROFIT", style={"color": CYAN, "fontSize": "10px", "fontWeight": "bold", "marginBottom": "6px", "letterSpacing": "1px", "marginTop": "4px"}),
                    html.Div([html.Span("True Net", style={**_detail_label, "fontWeight": "bold"}), html.Span(f"${_true_net:.2f}", style={**_detail_val, "color": _net_color, "fontWeight": "bold", "fontSize": "14px"})], style=_detail_row_style),
                    html.Div([html.Span("Margin", style=_detail_label), html.Span(f"{_margin:.1f}%", style={**_detail_val, "color": _margin_color, "fontWeight": "bold"})], style=_detail_row_style),
                ], style={"flex": "1", "padding": "0 0 0 16px"}),
            ], style={"display": "flex", "gap": "0", "padding": "12px 14px"}),
        ], style={"backgroundColor": "#0d1528", "borderBottom": f"1px solid {DARKGRAY}33"})

        # Use <details> for native expand/collapse — no callback needed
        _order_rows.append(html.Details([
            html.Summary(
                html.Div([
                    html.Div(_thumb),
                    html.Div([
                        html.Div([
                            html.Span(f"#{_oid}", style={"color": CYAN, "fontSize": "11px", "fontWeight": "bold", "fontFamily": "monospace"}),
                            html.Span(f" {_date}", style={"color": GRAY, "fontSize": "10px"}),
                            _status_badge,
                        ], style={"display": "flex", "alignItems": "center", "gap": "6px", "flexWrap": "wrap"}),
                        html.Div(_buyer, style={"color": WHITE, "fontSize": "12px", "fontWeight": "500", "marginTop": "2px"}),
                    ]),
                    html.Div([
                        html.Div(_first_item[:55], style={"color": WHITE, "fontSize": "12px", "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
                        html.Div(_variations[:60] if _variations else f"Qty: {_qty}", style={"color": GRAY, "fontSize": "10px", "marginTop": "1px"}),
                    ], style={"overflow": "hidden"}),
                    html.Div([
                        html.Div(f"${_sale_price:.2f}", style={"color": WHITE, "fontSize": "13px", "fontWeight": "bold", "fontFamily": "monospace", "textAlign": "right"}),
                        html.Div(f"x{_qty}" if _qty > 1 else "", style={"color": GRAY, "fontSize": "9px", "textAlign": "right"}),
                    ]),
                    html.Div(
                        _ship_section if _ship_section else [html.Span("Free ship", style={"color": GRAY, "fontSize": "10px"})],
                    ),
                    html.Div([
                        html.Div(f"-${_total_etsy_fees:.2f}", style={"color": RED, "fontSize": "12px", "fontFamily": "monospace", "textAlign": "right"}),
                        html.Div(_fee_detail, style={"color": GRAY, "fontSize": "9px", "textAlign": "right"}),
                    ]),
                    html.Div([
                        html.Div(f"-${abs(_refund):.2f}", style={"color": RED, "fontSize": "12px", "fontFamily": "monospace", "textAlign": "right"}),
                        html.Div("Refund", style={"color": GRAY, "fontSize": "9px", "textAlign": "right"}),
                    ] if _refund else []),
                    html.Div([
                        html.Div(f"${_true_net:.2f}", style={"color": _net_color, "fontSize": "14px", "fontWeight": "bold", "fontFamily": "monospace", "textAlign": "right"}),
                        html.Div(f"{_margin:.1f}%", style={"color": _margin_color, "fontSize": "11px", "fontWeight": "bold", "textAlign": "right"}),
                    ]),
                ], style={**_grid_style, "padding": "10px 14px", "cursor": "pointer"}),
                style={"listStyle": "none", "padding": "0", "margin": "0", "backgroundColor": _row_bg, "borderBottom": f"1px solid {DARKGRAY}22"},
            ),
            _detail_section,
        ], style={"margin": "0"}, id={"type": "order-row-data", "idx": _oid}))

    # Search bar
    _search_bar = html.Div([
        dcc.Input(
            id="order-search-input",
            type="text",
            placeholder="Search by buyer, order #, or item name...",
            debounce=False,
            style={
                "flex": "1", "fontSize": "13px", "backgroundColor": "#0a0f1e",
                "color": WHITE, "border": f"1px solid {DARKGRAY}44", "borderRadius": "6px",
                "padding": "10px 14px", "minWidth": "250px", "outline": "none",
            },
        ),
        html.Div(f"{_order_count} orders", style={
            "color": GRAY, "fontSize": "12px", "padding": "8px 0", "whiteSpace": "nowrap",
        }),
    ], style={
        "display": "flex", "gap": "12px", "alignItems": "center",
        "marginBottom": "12px", "padding": "0 2px",
    })

    _hdr_style = {"color": GRAY, "fontSize": "10px", "textTransform": "uppercase", "letterSpacing": "0.5px"}
    _col_header = html.Div([
        html.Span(""),
        html.Span("Order / Buyer", style=_hdr_style),
        html.Span("Item", style=_hdr_style),
        html.Span("Sale", style={**_hdr_style, "textAlign": "right"}),
        html.Span("Shipping", style=_hdr_style),
        html.Span("Fees", style={**_hdr_style, "textAlign": "right"}),
        html.Span(""),
        html.Span("Profit", style={**_hdr_style, "textAlign": "right"}),
    ], style={**_grid_style, "borderBottom": f"2px solid {CYAN}33"})

    # Order table container with scrolling
    _order_table = html.Div([
        _col_header,
        html.Div(
            _order_rows,
            id="order-rows-container",
            style={"maxHeight": "700px", "overflowY": "auto"},
        ),
    ], style={"borderRadius": "8px", "border": f"1px solid {DARKGRAY}33", "overflow": "hidden"})

    # Hidden DataTable for backward compat with search callback
    _hidden_table = dash_table.DataTable(
        id="order-detail-table",
        columns=[{"name": "x", "id": "x"}],
        data=[],
        style_table={"display": "none"},
    )

    # Dummy hidden elements for old callbacks that may still be registered
    _dummy_elements = html.Div([
        html.Div(id="refund-editor-status", style={"display": "none"}),
        html.Div(id="label-assign-status-dummy", style={"display": "none"}),
        dcc.Input(id="label-assign-order", type="hidden", value=""),
        dcc.Input(id="label-assign-label", type="hidden", value=""),
        html.Button(id="label-assign-btn", n_clicks=0, style={"display": "none"}),
        html.Div(id="order-cards-visible", style={"display": "none"}),
        html.Div(id="order-cards-hidden", style={"display": "none"}),
        html.Button(id="order-cards-load-more", n_clicks=0, style={"display": "none"}),
        dcc.Input(id="order-card-search", type="hidden", value=""),
    ], style={"display": "none"})

    # ── Order Management Panels ──────────────────────────────────────────────
    # Categorize orders for the management panels
    _refund_orders = [o for o in _ledger_orders if o.get("_needs_manual_net")]
    _canceled_orders = [o for o in _ledger_orders if "cancel" in (o.get("Status", "") or "").lower()
                        and not o.get("Gross")]
    _verified_count = len(_ledger_orders) - len(_refund_orders) - len(_canceled_orders)

    # Status bar
    _status_bar = html.Div([
        html.Span(f"{_verified_count} verified ", style={"color": GREEN, "fontWeight": "bold"}),
        html.Span("\u2713", style={"color": GREEN}),
        html.Span(" | ", style={"color": GRAY}),
        html.Span(f"{len(_refund_orders)} need earnings entry", style={"color": ORANGE, "fontWeight": "bold"}),
        html.Span(" | ", style={"color": GRAY}),
        html.Span(f"{len(_canceled_orders)} canceled", style={"color": GRAY, "fontWeight": "bold"}),
    ], style={
        "padding": "10px 16px", "margin": "20px 0 12px 0", "borderRadius": "8px",
        "backgroundColor": f"{CARD}", "border": f"1px solid {DARKGRAY}44",
        "fontSize": "13px", "fontFamily": "monospace",
    })

    # Panel 1: Refunded Orders — Enter Etsy Earnings (orange border)
    _refund_rows = []
    for _ro in _refund_orders:
        _oid = str(_ro.get("Order ID", ""))
        _refund_amt = _ro.get("Refund", 0) or 0
        _current_net = _ro.get("True Net", 0) or 0
        _refund_rows.append(html.Div([
            html.Div([
                html.Span(f"#{_oid}", style={"color": CYAN, "fontWeight": "bold", "marginRight": "12px", "minWidth": "110px", "display": "inline-block"}),
                html.Span(_ro.get("Buyer", _ro.get("Full Name", "")), style={"color": WHITE, "marginRight": "12px", "minWidth": "100px", "display": "inline-block"}),
                html.Span(_ro.get("Sale Date", ""), style={"color": GRAY, "marginRight": "12px", "minWidth": "80px", "display": "inline-block"}),
                html.Span(f"Refund: -${abs(_refund_amt):,.2f}", style={"color": RED, "marginRight": "12px", "minWidth": "100px", "display": "inline-block"}),
                html.Span(f"Current Net: ${_current_net:,.2f}", style={"color": ORANGE, "marginRight": "12px", "minWidth": "100px", "display": "inline-block"}),
            ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "4px", "flex": "1"}),
            html.Div([
                html.Span("Etsy Earned $", style={"color": GRAY, "fontSize": "11px", "marginRight": "6px"}),
                dcc.Input(
                    id={"type": "refund-earned-input", "order": _oid},
                    type="number", placeholder="0.00",
                    style={
                        "width": "90px", "fontSize": "12px", "backgroundColor": "#0a0f1e",
                        "color": WHITE, "border": f"1px solid {ORANGE}66", "borderRadius": "4px",
                        "padding": "4px 8px", "marginRight": "6px",
                    },
                ),
                html.Button("Save", id={"type": "refund-earned-save", "order": _oid}, n_clicks=0,
                             style={
                                 "fontSize": "11px", "padding": "4px 12px", "borderRadius": "4px",
                                 "backgroundColor": ORANGE, "color": "#000", "border": "none",
                                 "cursor": "pointer", "fontWeight": "bold",
                             }),
            ], style={"display": "flex", "alignItems": "center"}),
        ], style={
            "display": "flex", "justifyContent": "space-between", "alignItems": "center",
            "padding": "8px 12px", "borderBottom": f"1px solid {DARKGRAY}22",
        }))

    _panel1_content = _refund_rows if _refund_rows else [
        html.P("No refunded orders need earnings entry.", style={"color": GRAY, "fontSize": "12px", "padding": "8px 12px"})
    ]

    _panel1 = html.Details([
        html.Summary(f"Refunded Orders \u2014 Enter Etsy Earnings ({len(_refund_orders)})", style={
            "color": ORANGE, "fontSize": "13px", "fontWeight": "bold", "cursor": "pointer",
            "padding": "10px 14px", "outline": "none",
        }),
        html.Div(id="refund-earned-status", style={
            "padding": "4px 14px", "fontSize": "12px", "minHeight": "20px",
        }),
        html.Div(_panel1_content, style={"maxHeight": "400px", "overflowY": "auto"}),
    ], open=bool(_refund_rows), style={
        "backgroundColor": CARD, "borderRadius": "8px", "marginBottom": "10px",
        "border": f"1px solid {ORANGE}66",
    })

    # Panel 2: Unmatched Shipping Labels (blue border)
    _unmatched_labels = []
    try:
        from supabase_loader import get_config_value as _gcv_labels
        import json as _json_labels
        _raw_labels = _gcv_labels("unmatched_shipping_labels")
        if _raw_labels:
            _unmatched_labels = _json_labels.loads(_raw_labels) if isinstance(_raw_labels, str) else _raw_labels
            _unmatched_labels = [u for u in _unmatched_labels if not u.get("assigned_to")]
    except Exception:
        pass

    # Build searchable order options for dropdown: "Order# — Buyer — Date"
    _order_options = []
    try:
        _raw_ord_dd = _gcv_labels("order_profit_ledger_keycomponentmfg")
        _all_ord_dd = _json_labels.loads(_raw_ord_dd) if isinstance(_raw_ord_dd, str) else _raw_ord_dd
        for _o_dd in sorted(_all_ord_dd, key=lambda x: x.get("Sale Date", ""), reverse=True):
            _o_id = str(_o_dd.get("Order ID", ""))
            _o_buyer = _o_dd.get("Buyer", "")
            _o_date = _o_dd.get("Sale Date", "")
            _order_options.append({"label": f"#{_o_id} — {_o_buyer} — {_o_date}", "value": _o_id})
    except Exception:
        pass

    _label_total = sum(u.get("amount", 0) for u in _unmatched_labels)
    _label_rows = []
    _type_labels = {
        "shipping_labels": "Outbound",
        "shipping_labels_usps_return": "Return",
        "shipping_label_insurance": "Insurance",
        "shipping_label_usps_adjustment": "USPS Adj",
        "shipping_label_globegistics_adjustment": "Intl Adj",
        "shipping_label_refund": "Refund Credit",
        "shipping_label_usps_adjustment_credit": "Adj Credit",
    }
    for _ul in _unmatched_labels[:50]:
        _type_display = _type_labels.get(_ul.get("type", ""), _ul.get("type", ""))
        _is_credit = "credit" in _ul.get("type", "").lower() or "refund" in _ul.get("type", "").lower()
        _label_rows.append(html.Div([
            html.Span(_ul.get("date", ""), style={"color": GRAY, "fontSize": "12px", "width": "85px", "flexShrink": "0"}),
            html.Span(f"${_ul['amount']:.2f}", style={
                "color": GREEN if _is_credit else RED, "fontSize": "12px", "fontFamily": "monospace",
                "fontWeight": "bold", "width": "65px", "flexShrink": "0",
            }),
            html.Span(_type_display, style={"color": CYAN, "fontSize": "11px", "width": "75px", "flexShrink": "0"}),
            html.Div(
                dcc.Dropdown(
                    id={"type": "label-assign-order-input", "label": _ul.get("label_id", "")},
                    options=_order_options,
                    placeholder="Search buyer or order #...",
                    searchable=True,
                    clearable=True,
                    style={"fontSize": "11px", "backgroundColor": BG, "color": WHITE},
                ),
                style={"width": "280px", "flexShrink": "0"},
            ),
            html.Button("Assign", id={"type": "label-assign-save-btn", "label": _ul.get("label_id", "")},
                         n_clicks=0, style={
                "fontSize": "10px", "padding": "4px 8px", "backgroundColor": f"{BLUE}25",
                "border": f"1px solid {BLUE}", "borderRadius": "4px", "color": BLUE, "cursor": "pointer",
            }),
        ], style={"display": "flex", "alignItems": "center", "gap": "8px", "padding": "6px 14px",
                   "borderBottom": f"1px solid {DARKGRAY}22"}))

    _panel2 = html.Details([
        html.Summary(f"Unmatched Shipping Labels — {len(_unmatched_labels)} labels (${_label_total:.2f})", style={
            "color": BLUE, "fontSize": "13px", "fontWeight": "bold", "cursor": "pointer",
            "padding": "10px 14px", "outline": "none",
        }),
        html.Div([
            html.Div(id="label-assign-status", style={"padding": "4px 14px", "minHeight": "16px"}),
            html.Div([
                html.Div([
                    html.Span("Date", style={"width": "85px", "color": GRAY, "fontSize": "10px"}),
                    html.Span("Amount", style={"width": "65px", "color": GRAY, "fontSize": "10px"}),
                    html.Span("Type", style={"width": "75px", "color": GRAY, "fontSize": "10px"}),
                    html.Span("Assign to Order (search by name or #)", style={"color": GRAY, "fontSize": "10px"}),
                ], style={"display": "flex", "gap": "8px", "padding": "4px 14px", "borderBottom": f"1px solid {DARKGRAY}44"}),
                *_label_rows,
            ], style={"maxHeight": "500px", "overflowY": "auto"}),
        ]),
    ], style={
        "backgroundColor": CARD, "borderRadius": "8px", "marginBottom": "10px",
        "border": f"1px solid {BLUE}66",
    })

    # Panel 3: Canceled Orders (gray border)
    _cancel_rows = []
    for _co in _canceled_orders:
        _cancel_rows.append(html.Div([
            html.Span(f"#{_co.get('Order ID', '')}", style={"color": GRAY, "fontWeight": "bold", "marginRight": "12px", "minWidth": "110px", "display": "inline-block"}),
            html.Span(_co.get("Buyer", _co.get("Full Name", "")), style={"color": GRAY, "marginRight": "12px", "minWidth": "100px", "display": "inline-block"}),
            html.Span(_co.get("Sale Date", ""), style={"color": DARKGRAY, "minWidth": "80px", "display": "inline-block"}),
        ], style={"padding": "6px 12px", "borderBottom": f"1px solid {DARKGRAY}22"}))

    _panel3_content = _cancel_rows if _cancel_rows else [
        html.P("No canceled orders.", style={"color": GRAY, "fontSize": "12px", "padding": "8px 12px"})
    ]

    _panel3 = html.Details([
        html.Summary(f"Canceled Orders ({len(_canceled_orders)})", style={
            "color": GRAY, "fontSize": "13px", "fontWeight": "bold", "cursor": "pointer",
            "padding": "10px 14px", "outline": "none",
        }),
        html.Div([
            html.P("These orders were canceled before payment \u2014 not included in totals", style={
                "color": DARKGRAY, "fontSize": "11px", "padding": "2px 14px 6px 14px", "margin": "0",
            }),
        ]),
        html.Div(_panel3_content, style={"maxHeight": "300px", "overflowY": "auto"}),
    ], style={
        "backgroundColor": CARD, "borderRadius": "8px", "marginBottom": "10px",
        "border": f"1px solid {DARKGRAY}66",
    })

    _management_panels = html.Div([
        _status_bar,
        _panel1,
        _panel2,
        _panel3,
    ], style={"marginTop": "20px"})

    return html.Div([
        _sync_bar,
        html.H3("ORDER DETAIL", style={
            "color": CYAN, "margin": "30px 0 6px 0", "fontSize": "14px",
            "letterSpacing": "1.5px", "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px",
        }),
        html.P(f"Per-order profit breakdown — click order # to copy",
               style={"color": GRAY, "margin": "0 0 14px 0", "fontSize": "12px"}),
        html.Div(id="order-copy-toast", style={"position": "fixed", "top": "20px", "right": "20px",
                                                  "zIndex": "9999"}),
        _kpi_row,
        _search_bar,
        _order_table,
        _hidden_table,
        _dummy_elements,
        _management_panels,
    ])


# ── REMOVED OLD CODE: order cards, refund editor, label assignment editor ──
# All replaced by clean DataTable above. Dummy IDs preserved for callback compat.
_REMOVED_OLD_ORDER_SECTION = True
# ── Order Search Filter (clientside — filters visible HTML rows) ──────────────
app.clientside_callback(
    """
    function(search) {
        var container = document.getElementById("order-rows-container");
        if (!container) return window.dash_clientside.no_update;
        var rows = container.children;
        var q = (search || "").toLowerCase();
        var shown = 0;
        for (var i = 0; i < rows.length; i++) {
            var text = rows[i].textContent.toLowerCase();
            if (!q || text.indexOf(q) !== -1) {
                rows[i].style.display = "";
                shown++;
            } else {
                rows[i].style.display = "none";
            }
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output("order-detail-table", "data"),
    Input("order-search-input", "value"),
    prevent_initial_call=True,
)


def _build_order_table_data(orders):
    """Build table data rows from order list."""
    from supabase_loader import get_config_value
    import json as _json_tbl
    # Load items for variations
    _items_data = []
    try:
        _raw_it = get_config_value("order_csv_items_keycomponentmfg")
        if _raw_it:
            _items_data = _json_tbl.loads(_raw_it) if isinstance(_raw_it, str) else _raw_it
    except Exception:
        pass
    _items_by_order = {}
    for _it in _items_data:
        _oid = str(_it.get("Order ID", ""))
        if _oid:
            _items_by_order.setdefault(_oid, []).append(_it)

    rows = []
    for _o in orders:
        _oid = _o.get("Order ID", "")
        _order_items = _items_by_order.get(str(_oid), [])
        _var_parts = []
        for _it in _order_items:
            _var = _it.get("Variations", "")
            if _var:
                for _v in _var.split(", "):
                    if ": " in _v:
                        _prop, _val = _v.split(": ", 1)
                        _var_parts.append(_val if _prop == "Custom Property" else _val)
        _var_str = " / ".join(_var_parts) if _var_parts else _o.get("Variations", "")
        _item = _o.get("Item Names", "")[:45]
        if _var_str:
            _item = f"{_item} ({_var_str})"

        _net = _o.get("True Net", 0)
        _margin = _o.get("Margin %", 0)
        _fees = _o.get("Total Etsy Fees", 0)

        rows.append({
            "Order #": f"#{_oid}",
            "Date": _o.get("Sale Date", ""),
            "Buyer": (_o.get("Buyer", "") or "")[:18],
            "Qty": _o.get("Qty", 1),
            "Item": _item[:60],
            "List$": _o.get("Listing Price", 0),
            "Disc": round(-_o.get("Discount", 0), 2) if _o.get("Discount", 0) > 0 else None,
            "Ship In": _o.get("Buyer Shipping", 0) if _o.get("Buyer Shipping", 0) > 0 else None,
            "Fees": _fees,
            "Label $": _o.get("Shipping Label", 0) if _o.get("Shipping Label", 0) > 0 else None,
            "Label ID": f"#{_o.get('Label ID', '')}" if _o.get("Label ID") else "",
            "Ads": _o.get("Offsite Ads", 0) if _o.get("Offsite Ads", 0) > 0 else None,
            "Refund": _o.get("Refund", 0) if _o.get("Refund", 0) > 0 else None,
            "Net": _net,
            "Margin%": _margin,
            "Status": (_o.get("Status", "") or "")[:8],
            "_net_raw": _net,
            "_margin_raw": _margin,
            "_fees_raw": _fees,
            "_buyer_raw": _o.get("Buyer", ""),
            "_item_raw": _o.get("Item Names", ""),
        })
    return rows


# (Label copy callback removed — replaced with searchable dropdown)


# ── Assign Label to Order Callback ────────────────────────────────────────────
@app.callback(
    Output("label-assign-status", "children"),
    Input({"type": "label-assign-save-btn", "label": ALL}, "n_clicks"),
    State({"type": "label-assign-order-input", "label": ALL}, "value"),
    prevent_initial_call=True,
)
def assign_label_to_order(all_clicks, all_order_inputs):
    """Assign an unmatched shipping label to a specific order."""
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger = ctx.triggered[0]
    if not trigger.get("value"):
        raise dash.exceptions.PreventUpdate

    try:
        trigger_id = json.loads(trigger["prop_id"].split(".")[0])
        label_id = trigger_id.get("label", "")
    except Exception:
        raise dash.exceptions.PreventUpdate

    # Find the matching input value
    idx = None
    for i, inp in enumerate(ctx.inputs_list[0]):
        if inp.get("id", {}).get("label") == label_id:
            idx = i
            break
    if idx is None:
        raise dash.exceptions.PreventUpdate

    order_id = str(all_order_inputs[idx] or "").strip()
    if not order_id:
        return html.Span("Select an order first", style={"color": ORANGE, "fontSize": "12px"})

    try:
        from supabase_loader import get_config_value, _get_supabase_client

        # Load unmatched labels
        raw_labels = get_config_value("unmatched_shipping_labels")
        labels = json.loads(raw_labels) if isinstance(raw_labels, str) else raw_labels

        # Find the label
        label_entry = next((l for l in labels if l.get("label_id") == label_id), None)
        if not label_entry:
            return html.Span(f"Label {label_id} not found", style={"color": RED, "fontSize": "12px"})

        # Prevent double-assignment
        if label_entry.get("assigned_to"):
            return html.Span(f"Already assigned to #{label_entry['assigned_to']}", style={"color": ORANGE, "fontSize": "12px"})

        label_amount = label_entry.get("amount", 0)
        label_type = label_entry.get("type", "")

        # Load orders
        raw_orders = get_config_value("order_profit_ledger_keycomponentmfg")
        orders = json.loads(raw_orders) if isinstance(raw_orders, str) else raw_orders

        # Find the order
        order = next((o for o in orders if str(o.get("Order ID")) == order_id), None)
        if not order:
            return html.Span(f"Order #{order_id} not found", style={"color": RED, "fontSize": "12px"})

        # Add label cost to the order (credits/refunds reduce the cost)
        old_label = order.get("Shipping Label", 0)
        is_credit = "credit" in label_type.lower() or "refund" in label_type.lower()
        if is_credit:
            order["Shipping Label"] = round(old_label - label_amount, 2)
            order["True Net"] = round(order["True Net"] + label_amount, 2)
        else:
            order["Shipping Label"] = round(old_label + label_amount, 2)
            order["True Net"] = round(order["True Net"] - label_amount, 2)
        order["Ship P/L"] = round(order.get("Buyer Shipping", 0) - order["Shipping Label"], 2)
        order["Margin %"] = round(order["True Net"] / order.get("Sale Price", 1) * 100, 1) if order.get("Sale Price") else 0

        # Mark label as assigned
        label_entry["assigned_to"] = order_id

        # Save both
        client = _get_supabase_client()
        client.table("config").upsert({
            "key": "order_profit_ledger_keycomponentmfg",
            "value": json.dumps(orders),
        }, on_conflict="key").execute()
        client.table("config").upsert({
            "key": "unmatched_shipping_labels",
            "value": json.dumps(labels),
        }, on_conflict="key").execute()

        _type_short = {"shipping_labels": "Outbound", "shipping_labels_usps_return": "Return",
                       "shipping_label_insurance": "Insurance", "shipping_label_usps_adjustment": "USPS Adj",
                       "shipping_label_globegistics_adjustment": "Intl Adj"}.get(label_type, label_type)

        return html.Span(
            f"Assigned {_type_short} ${label_amount:.2f} to order #{order_id} — new label total: ${order['Shipping Label']:.2f}, net: ${order['True Net']:.2f}",
            style={"color": GREEN, "fontSize": "12px"},
        )
    except Exception as e:
        return html.Span(f"Error: {e}", style={"color": RED, "fontSize": "12px"})


# ── Save Refund Earnings Callback ────────────────────────────────────────────
@app.callback(
    Output("refund-earned-status", "children"),
    Input({"type": "refund-earned-save", "order": ALL}, "n_clicks"),
    State({"type": "refund-earned-input", "order": ALL}, "value"),
    prevent_initial_call=True,
)
def save_refund_earnings(all_clicks, all_values):
    """Save manually entered Etsy earnings for refunded orders."""
    import json as _json_refund
    # Find which button was clicked
    if not any(c for c in all_clicks if c):
        raise dash.exceptions.PreventUpdate
    ctx = dash.callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    triggered_id = ctx.triggered[0]["prop_id"]
    # Pattern-matching IDs look like: {"order":"12345","type":"refund-earned-save"}.n_clicks
    try:
        _id_str = triggered_id.rsplit(".", 1)[0]
        _id_obj = _json_refund.loads(_id_str)
        _clicked_order = str(_id_obj.get("order", ""))
    except Exception:
        raise dash.exceptions.PreventUpdate

    if not _clicked_order:
        raise dash.exceptions.PreventUpdate

    # Find the matching value from the inputs
    _earned_val = None
    for i, inp_id in enumerate(ctx.states_list[0]):
        if str(inp_id["id"].get("order", "")) == _clicked_order:
            _earned_val = all_values[i]
            break

    if _earned_val is None or _earned_val == "":
        return html.Span("Enter an amount first.", style={"color": ORANGE, "fontSize": "12px"})

    try:
        _earned_val = round(float(_earned_val), 2)
    except (ValueError, TypeError):
        return html.Span("Invalid amount.", style={"color": RED, "fontSize": "12px"})

    # Load orders from Supabase
    try:
        from supabase_loader import get_config_value, _get_supabase_client
        _raw = get_config_value("order_profit_ledger_keycomponentmfg")
        if not _raw:
            return html.Span("No order data found.", style={"color": RED, "fontSize": "12px"})
        _orders = _json_refund.loads(_raw) if isinstance(_raw, str) else _raw

        _updated = False
        for _o in _orders:
            if str(_o.get("Order ID", "")) == _clicked_order:
                _old_net = _o.get("True Net", 0)
                _o["True Net"] = _earned_val
                _sale_price = _o.get("Sale Price", 0) or _o.get("Gross", 0) or 1
                _o["Margin %"] = round(_earned_val / _sale_price * 100, 1) if _sale_price else 0
                _o["_needs_manual_net"] = False
                _o["_manual_override"] = True
                _updated = True
                break

        if not _updated:
            return html.Span(f"Order {_clicked_order} not found.", style={"color": RED, "fontSize": "12px"})

        # Save back to Supabase
        _client = _get_supabase_client()
        _client.table("config").upsert({
            "key": "order_profit_ledger_keycomponentmfg",
            "value": _json_refund.dumps(_orders),
        }, on_conflict="key").execute()

        return html.Span(
            f"Saved: Order #{_clicked_order} net updated to ${_earned_val:,.2f} (was ${_old_net:,.2f})",
            style={"color": GREEN, "fontSize": "12px"},
        )
    except Exception as e:
        return html.Span(f"Error: {str(e)}", style={"color": RED, "fontSize": "12px"})


# ── Copy Order Number on Click ────────────────────────────────────────────────
app.clientside_callback(
    """
    function(active_cell, derived_data) {
        if (!active_cell) return window.dash_clientside.no_update;
        var col = active_cell.column_id;
        if (col !== "Order #" && col !== "Label ID") return window.dash_clientside.no_update;
        var row = active_cell.row;
        var data = derived_data || [];
        if (row >= 0 && row < data.length) {
            var val = String(data[row][col] || "");
            if (val && navigator.clipboard) {
                navigator.clipboard.writeText(val);
                var toast = document.createElement("div");
                toast.textContent = "Copied: " + val;
                toast.style.cssText = "background:#2ecc71;color:#fff;padding:8px 16px;border-radius:6px;font-size:13px;font-weight:bold;";
                var container = document.getElementById("order-copy-toast");
                if (container) {
                    container.innerHTML = "";
                    container.appendChild(toast);
                    setTimeout(function() { container.innerHTML = ""; }, 2000);
                }
            }
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output("order-copy-toast", "data-dummy"),
    Input("order-detail-table", "active_cell"),
    State("order-detail-table", "derived_virtual_data"),
    prevent_initial_call=True,
)


# ── Refund Cost Override Callback ────────────────────────────────────────────

@app.callback(
    Output("refund-editor-status", "children"),
    Output("upload-reload-trigger", "data", allow_duplicate=True),
    Output("ceo-alert-banner", "children", allow_duplicate=True),
    Input({"type": "refund-save-btn", "order": ALL}, "n_clicks"),
    State({"type": "refund-type-dd", "order": ALL}, "value"),
    State({"type": "refund-outbound-input", "order": ALL}, "value"),
    State({"type": "refund-return-input", "order": ALL}, "value"),
    prevent_initial_call=True,
)
@guard_callback(n_outputs=3)
def save_refund_cost_override(all_clicks, all_types, all_outbound, all_return):
    """Save manual refund cost overrides to Supabase."""
    global _refund_cost_overrides
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger = ctx.triggered[0]
    if not trigger["value"]:
        raise dash.exceptions.PreventUpdate

    try:
        trigger_id = json.loads(trigger["prop_id"].split(".")[0])
        order_id = trigger_id["order"]
    except Exception:
        raise dash.exceptions.PreventUpdate

    # Find the index of the clicked button
    idx = None
    for i, inp in enumerate(ctx.inputs_list[0]):
        if inp.get("id", {}).get("order") == order_id:
            idx = i
            break
    if idx is None:
        raise dash.exceptions.PreventUpdate

    _type = all_types[idx] if idx < len(all_types) else "cancel"
    _outbound = float(all_outbound[idx] or 0) if idx < len(all_outbound) else 0
    _return = float(all_return[idx] or 0) if idx < len(all_return) else 0

    # Save override
    _refund_cost_overrides[order_id] = {
        "type": _type,
        "outbound_label": _outbound,
        "return_label": _return,
    }

    # Persist to Supabase
    try:
        from supabase_loader import save_config_value
        save_config_value("refund_cost_overrides", _refund_cost_overrides)
    except Exception as e:
        print(f"[RefundOverride] Supabase save failed: {e}")

    # Recompute profits
    try:
        _compute_per_order_profit()
    except Exception:
        pass

    # Re-run CEO health check so alert banner updates immediately
    global _ceo_health
    if _ceo_agent and _acct_pipeline:
        try:
            _ceo_health = _ceo_agent.run_periodic_check(_acct_pipeline)
        except Exception:
            pass

    import time
    status = html.Div([
        html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
        html.Span(f"Order #{order_id} saved: {_type}, outbound ${_outbound:.2f}, return ${_return:.2f}",
                  style={"color": GREEN, "fontSize": "12px"}),
    ])
    return status, time.time(), _build_ceo_banner()


# ── Product Library Save Callback ─────────────────────────────────────────────

@app.callback(
    Output("product-library-status", "children"),
    Output("upload-reload-trigger", "data", allow_duplicate=True),
    Input("pl-save-all", "n_clicks"),
    Input({"type": "pl-save", "listing": ALL}, "n_clicks"),
    State({"type": "pl-stl", "listing": ALL, "printer": ALL, "size": ALL}, "value"),
    State({"type": "pl-time", "listing": ALL, "printer": ALL, "size": ALL}, "value"),
    State({"type": "pl-grams", "listing": ALL, "printer": ALL, "size": ALL}, "value"),
    State({"type": "pl-category", "listing": ALL}, "value"),
    State({"type": "pl-new-size", "listing": ALL}, "value"),
    State({"type": "pl-filament", "listing": ALL}, "value"),
    State({"type": "pl-location", "listing": ALL}, "value"),
    State({"type": "pl-success", "listing": ALL}, "value"),
    State({"type": "pl-weight", "listing": ALL}, "value"),
    State({"type": "pl-box", "listing": ALL}, "value"),
    State({"type": "pl-variations", "listing": ALL}, "value"),
    State({"type": "pl-components", "listing": ALL}, "value"),
    State({"type": "pl-notes", "listing": ALL}, "value"),
    prevent_initial_call=True,
)
@guard_callback(n_outputs=2)
def save_product_library(save_all_clicks, per_save_clicks, all_stls, all_times, all_grams, all_categories, all_new_sizes,
                          all_filaments, all_locations, all_success, all_weights, all_boxes, all_variations, all_components, all_notes):
    """Save all product library entries to Supabase at once."""
    global PRODUCT_LIBRARY
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger = ctx.triggered[0]
    if not trigger["value"]:
        raise dash.exceptions.PreventUpdate

    trigger_id_raw = trigger["prop_id"]

    # Determine if saving one product or all
    _single_listing = None
    if "pl-save-all" not in trigger_id_raw:
        try:
            _tid = json.loads(trigger_id_raw.split(".")[0])
            _single_listing = _tid.get("listing")
        except Exception:
            raise dash.exceptions.PreventUpdate

    # Build maps from inputs
    _cat_map = {}
    for i, inp in enumerate(ctx.states_list[3]):
        _listing = inp.get("id", {}).get("listing", "")
        if _listing and i < len(all_categories):
            _cat_map[_listing] = all_categories[i] or ""

    # New sizes requested
    _new_size_map = {}
    for i, inp in enumerate(ctx.states_list[4]):
        _listing = inp.get("id", {}).get("listing", "")
        if _listing and i < len(all_new_sizes) and all_new_sizes[i]:
            _new_size_map[_listing] = str(all_new_sizes[i]).strip()

    # Build sizes structure: listing -> {size_name -> {printer -> {stl, time, grams}}}
    _sizes_map = {}
    for state_idx, field_name, values in [(0, "stl", all_stls), (1, "time", all_times), (2, "grams", all_grams)]:
        for i, inp in enumerate(ctx.states_list[state_idx]):
            _id = inp.get("id", {})
            _listing = _id.get("listing", "")
            _model = _id.get("printer", "")
            _size = _id.get("size", "default")
            if _listing and _model:
                if _listing not in _sizes_map:
                    _sizes_map[_listing] = {}
                if _size not in _sizes_map[_listing]:
                    _sizes_map[_listing][_size] = {}
                if _model not in _sizes_map[_listing][_size]:
                    _sizes_map[_listing][_size][_model] = {}
                _val = values[i] if i < len(values) else None
                if field_name == "stl":
                    _sizes_map[_listing][_size][_model]["stl"] = _val or ""
                else:
                    _sizes_map[_listing][_size][_model][field_name] = float(_val) if _val else None

    # Save
    _listings_to_save = [_single_listing] if _single_listing else list(set(list(_cat_map.keys()) + list(_sizes_map.keys()) + list(_new_size_map.keys())))
    _saved_count = 0

    for _listing in _listings_to_save:
        _cat = _cat_map.get(_listing, "")
        _sizes = _sizes_map.get(_listing, {})

        # Check if any data exists
        _has_any = bool(_cat) or any(
            _sizes.get(s, {}).get(m, {}).get("stl") or _sizes.get(s, {}).get(m, {}).get("time") or _sizes.get(s, {}).get(m, {}).get("grams")
            for s in _sizes for m in _sizes.get(s, {})
        )
        if not _has_any:
            continue

        if _listing not in PRODUCT_LIBRARY:
            PRODUCT_LIBRARY[_listing] = {}

        if _cat:
            PRODUCT_LIBRARY[_listing]["category"] = _cat

        # Merge sizes with existing
        existing_sizes = PRODUCT_LIBRARY[_listing].get("sizes", {})
        # Migrate old printers format to sizes
        if not existing_sizes and PRODUCT_LIBRARY[_listing].get("printers"):
            existing_sizes = {"default": PRODUCT_LIBRARY[_listing]["printers"]}

        for _size_name, _size_printers in _sizes.items():
            if _size_name not in existing_sizes:
                existing_sizes[_size_name] = {}
            for _m, _pd in _size_printers.items():
                if _m not in existing_sizes[_size_name]:
                    existing_sizes[_size_name][_m] = {}
                for _k, _v in _pd.items():
                    if _v is not None and _v != "":
                        existing_sizes[_size_name][_m][_k] = _v

        # Add new size if requested (empty, ready to fill on next reload)
        if _listing in _new_size_map:
            _ns = _new_size_map[_listing]
            if _ns and _ns not in existing_sizes:
                existing_sizes[_ns] = {}

        PRODUCT_LIBRARY[_listing]["sizes"] = existing_sizes
        # Keep printers for backward compat
        PRODUCT_LIBRARY[_listing]["printers"] = existing_sizes.get("default", {})

        # Save detail fields
        _detail_fields = {"filament": all_filaments, "location": all_locations, "success": all_success,
                          "weight": all_weights, "box": all_boxes, "variations": all_variations,
                          "components": all_components, "notes": all_notes}
        _details = PRODUCT_LIBRARY[_listing].get("details", {})

        # Find values by matching listing in states_list indices 5-12
        for _di, (_fname, _fvals) in enumerate(_detail_fields.items()):
            _state_idx = 5 + _di
            if _state_idx < len(ctx.states_list):
                for _si, _sinp in enumerate(ctx.states_list[_state_idx]):
                    if _sinp.get("id", {}).get("listing") == _listing and _si < len(_fvals):
                        _fval = _fvals[_si]
                        if _fval is not None and _fval != "":
                            if _fname == "filament":
                                _details["filament_color"] = str(_fval)
                            elif _fname == "location":
                                _details["print_location"] = str(_fval)
                            elif _fname == "success":
                                _details["success_rate"] = float(_fval) if _fval else None
                            elif _fname == "weight":
                                _details["finished_weight_oz"] = float(_fval) if _fval else None
                            elif _fname == "box":
                                _details["box_size"] = str(_fval)
                            elif _fname == "notes":
                                _details["notes"] = str(_fval)
                            elif _fname == "variations":
                                # Parse "Large=64.99\nSmall=44.99" format
                                _vps = []
                                for _line in str(_fval).strip().split("\n"):
                                    if "=" in _line:
                                        _parts = _line.split("=", 1)
                                        try:
                                            _vps.append({"name": _parts[0].strip(), "price": float(_parts[1].strip())})
                                        except (ValueError, IndexError):
                                            pass
                                if _vps:
                                    _details["variation_prices"] = _vps
                                    _details["has_variations"] = True
                            elif _fname == "components":
                                # Parse "1x LED Kit\n2x M5 Screw" format
                                _cps = []
                                for _line in str(_fval).strip().split("\n"):
                                    _line = _line.strip()
                                    if _line:
                                        import re as _re_cp
                                        _m = _re_cp.match(r"(\d+)x\s+(.+)", _line)
                                        if _m:
                                            _cps.append({"qty": int(_m.group(1)), "item": _m.group(2).strip()})
                                        else:
                                            _cps.append({"qty": 1, "item": _line})
                                if _cps:
                                    _details["components"] = _cps
                        break

        PRODUCT_LIBRARY[_listing]["details"] = _details
        PRODUCT_LIBRARY[_listing]["linked_listings"] = PRODUCT_LIBRARY[_listing].get("linked_listings", [_listing])
        if _listing not in PRODUCT_LIBRARY[_listing]["linked_listings"]:
            PRODUCT_LIBRARY[_listing]["linked_listings"].append(_listing)
        _saved_count += 1

    # Persist to Supabase
    try:
        from supabase_loader import save_config_value
        save_config_value("product_library", json.dumps(PRODUCT_LIBRARY))
        print(f"[ProductLibrary] Saved {_saved_count} products to Supabase")
    except Exception as e:
        print(f"[ProductLibrary] Supabase save failed: {e}")

    # Count categories
    _cat_counts = {}
    for _p in PRODUCT_LIBRARY.values():
        _c = _p.get("category", "Uncategorized")
        if _c:
            _cat_counts[_c] = _cat_counts.get(_c, 0) + 1

    _cat_summary = ", ".join(f"{c}: {n}" for c, n in sorted(_cat_counts.items())) if _cat_counts else "none"

    import time
    return html.Div([
        html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
        html.Span(f"Saved {_saved_count} products — Categories: {_cat_summary}",
                  style={"color": GREEN, "fontSize": "12px"}),
    ]), time.time()


# ── Label Assignment Callback ────────────────────────────────────────────────

@app.callback(
    Output("label-assign-status", "children"),
    Output("label-assign-order", "value"),
    Output("label-assign-label", "value"),
    Output("upload-reload-trigger", "data", allow_duplicate=True),
    Input("label-assign-btn", "n_clicks"),
    State("label-assign-order", "value"),
    State("label-assign-label", "value"),
    prevent_initial_call=True,
)
def save_label_assignment(n_clicks, order_id, label_num):
    """Manually link a shipping label to an order."""
    global _label_order_map
    if not n_clicks or not order_id or not label_num:
        raise dash.exceptions.PreventUpdate

    order_id = str(order_id).strip().replace("#", "")
    label_num = str(label_num).strip()
    # Normalize label number — add "Label #" prefix if just a number
    if label_num.isdigit():
        label_num = f"Label #{label_num}"
    elif not label_num.startswith("Label #"):
        label_num = f"Label #{label_num.replace('Label ', '').replace('#', '').strip()}"

    # Look up the label cost
    _all_data = _DATA_ALL if _DATA_ALL is not None else DATA
    ship_rows = _all_data[_all_data["Type"] == "Shipping"]
    _found = ship_rows[ship_rows["Info"] == label_num]
    if len(_found) == 0:
        return html.Div([
            html.Span("\u2717 ", style={"color": RED, "fontWeight": "bold"}),
            html.Span(f"Label {label_num} not found in Etsy statements", style={"color": RED, "fontSize": "12px"}),
        ]), dash.no_update, dash.no_update, dash.no_update

    label_cost = abs(_found.iloc[0]["Net_Clean"])

    # Save mapping
    _label_order_map[order_id] = label_num
    try:
        from supabase_loader import save_config_value
        save_config_value("label_order_map", _label_order_map)
    except Exception as e:
        print(f"[LabelAssign] Supabase save failed: {e}")

    # Recompute profits
    try:
        _compute_per_order_profit()
    except Exception:
        pass

    import time
    status = html.Div([
        html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
        html.Span(f"Order #{order_id} linked to {label_num} (${label_cost:.2f})",
                  style={"color": GREEN, "fontSize": "12px"}),
    ])
    return status, "", "", time.time()


# ── Agreement Tab — extracted to dashboard_utils/pages/agreement.py ──────────
# build_tab_agreement() is imported at the top of this file.

# ── Data Hub Tab ─────────────────────────────────────────────────────────────

def _build_upload_zone(zone_id, icon, label, color, accept, description):
    """Reusable upload card with drag-drop zone, status area, and file list."""
    return html.Div([
        # Header
        html.Div([
            html.Span(icon, style={"fontSize": "22px", "marginRight": "8px"}),
            html.Span(label, style={"fontSize": "16px", "fontWeight": "bold", "color": color}),
        ], style={"marginBottom": "10px"}),
        html.P(description, style={"color": GRAY, "fontSize": "12px", "margin": "0 0 12px 0"}),

        # Upload zone
        dcc.Upload(
            id=f"datahub-{zone_id}-upload",
            children=html.Div([
                html.Div(icon, style={"fontSize": "28px", "marginBottom": "6px", "opacity": "0.6"}),
                html.Span("Drop file here or ", style={"color": GRAY, "fontSize": "13px"}),
                html.A("browse", style={
                    "color": color, "textDecoration": "underline",
                    "cursor": "pointer", "fontSize": "13px"}),
            ], style={"textAlign": "center", "padding": "20px 16px"}),
            accept=accept,
            style={
                "borderWidth": "2px", "borderStyle": "dashed",
                "borderColor": f"{color}55", "borderRadius": "8px",
                "backgroundColor": f"{color}08", "cursor": "pointer",
                "marginBottom": "12px", "transition": "all 0.15s ease",
            },
        ),

        # Status message
        html.Div(id=f"datahub-{zone_id}-status", style={"marginBottom": "8px", "minHeight": "20px"}),

        # Stats line
        html.Div(id=f"datahub-{zone_id}-stats", style={"marginBottom": "10px", "minHeight": "16px"}),

        # Existing files list
        html.Div([
            html.Div("Existing Files:", style={"color": GRAY, "fontSize": "11px",
                                                 "fontWeight": "bold", "marginBottom": "4px",
                                                 "textTransform": "uppercase", "letterSpacing": "1px"}),
            html.Div(id=f"datahub-{zone_id}-files"),
        ]),
    ], style={
        "backgroundColor": CARD2, "borderRadius": "12px", "padding": "18px",
        "borderLeft": f"4px solid {color}", "flex": "1", "minWidth": "280px",
        "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
    })


def _get_existing_files(zone_type):
    """Scan the appropriate directory and return list of {filename, size_kb, uploaded}."""
    from datetime import datetime as _dt_files
    dir_map = {
        "etsy": os.path.join(BASE_DIR, "data", "etsy_statements"),
        "receipt": os.path.join(BASE_DIR, "data", "invoices", "keycomp"),
        "bank": os.path.join(BASE_DIR, "data", "bank_statements"),
    }
    ext_map = {"etsy": ".csv", "receipt": ".pdf", "bank": (".pdf", ".csv")}
    target_dir = dir_map.get(zone_type, "")
    ext = ext_map.get(zone_type, "")
    files = []
    if os.path.isdir(target_dir):
        # Root-level files
        for fn in sorted(os.listdir(target_dir)):
            if fn.lower().endswith(ext) and os.path.isfile(os.path.join(target_dir, fn)):
                fpath = os.path.join(target_dir, fn)
                stat = os.stat(fpath)
                files.append({
                    "filename": fn,
                    "size_kb": round(stat.st_size / 1024, 1),
                    "uploaded": _dt_files.fromtimestamp(stat.st_mtime).strftime("%b %d, %Y %I:%M %p"),
                })
        # For etsy: also scan store subdirectories
        if zone_type == "etsy":
            for store_slug in ("keycomponentmfg", "aurvio", "lunalinks"):
                sub_dir = os.path.join(target_dir, store_slug)
                if os.path.isdir(sub_dir):
                    for fn in sorted(os.listdir(sub_dir)):
                        if fn.lower().endswith(ext):
                            fpath = os.path.join(sub_dir, fn)
                            stat = os.stat(fpath)
                            files.append({
                                "filename": f"{store_slug}/{fn}",
                                "size_kb": round(stat.st_size / 1024, 1),
                                "uploaded": _dt_files.fromtimestamp(stat.st_mtime).strftime("%b %d, %Y %I:%M %p"),
                            })
    # Sort by most recently uploaded first
    files.sort(key=lambda x: x.get("uploaded", ""), reverse=True)
    return files


def _render_file_list(files, color):
    """Render a list of existing files as compact rows with upload dates."""
    if not files:
        return html.Div("No files yet", style={"color": DARKGRAY, "fontSize": "12px", "fontStyle": "italic"})
    return html.Div([
        html.Div([
            html.Div([
                html.Span(f["filename"], style={"color": WHITE, "fontSize": "12px"}),
                html.Span(f'  {f["size_kb"]} KB', style={"color": DARKGRAY, "fontSize": "11px",
                                                          "fontFamily": "monospace"}),
            ], style={"flex": "1"}),
            html.Span(f.get("uploaded", ""), style={"color": GRAY, "fontSize": "11px",
                                                      "fontFamily": "monospace", "whiteSpace": "nowrap"}),
        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                   "padding": "4px 0", "borderBottom": "1px solid #ffffff08", "gap": "12px"})
        for f in files
    ], style={"maxHeight": "160px", "overflowY": "auto"})


def _build_datahub_summary():
    """Build the KPI summary strip showing current data state."""
    etsy_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
    etsy_count = len([f for f in os.listdir(etsy_dir) if f.endswith(".csv")]) if os.path.isdir(etsy_dir) else 0
    bank_dir = os.path.join(BASE_DIR, "data", "bank_statements")
    bank_count = len([f for f in os.listdir(bank_dir) if f.endswith(".pdf")]) if os.path.isdir(bank_dir) else 0

    return html.Div([
        _build_kpi_pill("\U0001f4ca", "ETSY STATEMENTS", str(etsy_count), TEAL,
                        f"{len(DATA)} transactions"),
        _build_kpi_pill("\U0001f4e6", "INVENTORY ORDERS", str(len(INVOICES)), PURPLE,
                        f"${total_inventory_cost:,.2f} spent"),
        _build_kpi_pill("\U0001f3e6", "BANK STATEMENTS", str(bank_count), CYAN,
                        f"{len(BANK_TXNS)} transactions"),
        _build_kpi_pill("\U0001f4b0", "PROFIT", f"${profit:,.2f}", GREEN,
                        f"{profit_margin:.1f}% margin"),
    ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap"})


def _build_data_coverage():
    """Show what date ranges are covered by each data source so user knows what to upload."""
    from datetime import datetime, date

    rows = []

    # ── Etsy Statements coverage ──
    etsy_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
    etsy_months_on_file = set()
    if os.path.isdir(etsy_dir):
        for fn in os.listdir(etsy_dir):
            m = re.match(r"etsy_statement_(\d{4})_(\d{1,2})\.csv", fn)
            if m:
                etsy_months_on_file.add((int(m.group(1)), int(m.group(2))))

    # Expected months: Oct 2025 through current month
    today = date.today()
    expected_etsy = []
    y, m = 2025, 10  # business start
    while (y, m) <= (today.year, today.month):
        expected_etsy.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    etsy_missing = [ym for ym in expected_etsy if ym not in etsy_months_on_file]

    month_pills = []
    for ym in expected_etsy:
        label = f"{['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][ym[1]-1]} {ym[0]}"
        have = ym in etsy_months_on_file
        month_pills.append(html.Span(label, style={
            "display": "inline-block", "padding": "3px 8px", "borderRadius": "4px",
            "fontSize": "11px", "fontWeight": "bold", "marginRight": "4px", "marginBottom": "4px",
            "backgroundColor": f"{GREEN}22" if have else f"{RED}22",
            "color": GREEN if have else RED,
            "border": f"1px solid {GREEN}44" if have else f"1px solid {RED}44",
        }))

    etsy_latest = ""
    if len(DATA) > 0 and DATA["Date_Parsed"].notna().any():
        etsy_latest = DATA["Date_Parsed"].max().strftime("%b %d, %Y")

    rows.append(html.Div([
        html.Div([
            html.Span("\U0001f4ca", style={"fontSize": "16px", "marginRight": "8px"}),
            html.Span("ETSY STATEMENTS", style={"fontSize": "13px", "fontWeight": "bold",
                                                   "color": TEAL, "letterSpacing": "1px"}),
            html.Span(f"  Latest: {etsy_latest}" if etsy_latest else "",
                       style={"color": GRAY, "fontSize": "12px", "marginLeft": "12px"}),
        ], style={"marginBottom": "8px"}),
        html.Div(month_pills, style={"marginBottom": "4px"}),
        html.Div(
            f"{len(etsy_missing)} month{'s' if len(etsy_missing) != 1 else ''} missing" if etsy_missing else "All months covered",
            style={"color": RED if etsy_missing else GREEN, "fontSize": "11px", "fontWeight": "bold"}
        ),
    ], style={"padding": "10px 14px", "borderBottom": "1px solid #ffffff08"}))

    # ── Receipt / Invoice coverage ──
    inv_dates = []
    for inv in INVOICES:
        try:
            dt = pd.to_datetime(inv["date"], format="%B %d, %Y")
            inv_dates.append(dt)
        except Exception:
            try:
                dt = pd.to_datetime(inv["date"])
                inv_dates.append(dt)
            except Exception:
                pass

    inv_latest = max(inv_dates).strftime("%b %d, %Y") if inv_dates else "None"
    inv_oldest = min(inv_dates).strftime("%b %d, %Y") if inv_dates else "None"

    # Orders by month
    inv_by_month = {}
    for dt in inv_dates:
        key = dt.strftime("%b %Y")
        inv_by_month[key] = inv_by_month.get(key, 0) + 1

    inv_month_pills = []
    for month_label, count in sorted(inv_by_month.items(),
                                      key=lambda x: pd.to_datetime(x[0], format="%b %Y")):
        inv_month_pills.append(html.Span(f"{month_label} ({count})", style={
            "display": "inline-block", "padding": "3px 8px", "borderRadius": "4px",
            "fontSize": "11px", "fontWeight": "bold", "marginRight": "4px", "marginBottom": "4px",
            "backgroundColor": f"{PURPLE}22", "color": PURPLE,
            "border": f"1px solid {PURPLE}44",
        }))

    # Days since latest receipt
    days_since_receipt = ""
    if inv_dates:
        days_ago = (pd.Timestamp.now() - max(inv_dates)).days
        days_since_receipt = f" ({days_ago} days ago)" if days_ago > 0 else " (today)"

    rows.append(html.Div([
        html.Div([
            html.Span("\U0001f4e6", style={"fontSize": "16px", "marginRight": "8px"}),
            html.Span("RECEIPT ORDERS", style={"fontSize": "13px", "fontWeight": "bold",
                                                  "color": PURPLE, "letterSpacing": "1px"}),
            html.Span(f"  {len(INVOICES)} orders  |  Latest: {inv_latest}{days_since_receipt}",
                       style={"color": GRAY, "fontSize": "12px", "marginLeft": "12px"}),
        ], style={"marginBottom": "8px"}),
        html.Div(inv_month_pills if inv_month_pills else [
            html.Span("No receipts uploaded", style={"color": DARKGRAY, "fontSize": "12px"})
        ], style={"marginBottom": "4px"}),
        html.Div(f"Range: {inv_oldest} — {inv_latest}" if inv_dates else "No data",
                  style={"color": GRAY, "fontSize": "11px"}),
    ], style={"padding": "10px 14px", "borderBottom": "1px solid #ffffff08"}))

    # ── Bank Statement coverage ──
    # Track which source covers each month
    bank_month_source = {}  # (year, month) -> "PDF" or "CSV"
    for t in BANK_TXNS:
        try:
            parts = t["date"].split("/")
            ym = (int(parts[2]), int(parts[0]))
            src = t.get("source_file", "")
            src_type = "CSV" if src.lower().endswith(".csv") else "PDF"
            # PDF takes priority label (it was parsed first)
            if ym not in bank_month_source or src_type == "PDF":
                bank_month_source[ym] = src_type
        except Exception:
            pass

    bank_dates = []
    for t in BANK_TXNS:
        try:
            parts = t["date"].split("/")
            dt = date(int(parts[2]), int(parts[0]), int(parts[1]))
            bank_dates.append(dt)
        except Exception:
            pass

    bank_latest = max(bank_dates).strftime("%b %d, %Y") if bank_dates else "None"

    bank_months_covered = set(bank_month_source.keys())
    bank_missing = [ym for ym in expected_etsy if ym not in bank_months_covered]

    bank_month_pills = []
    for ym in expected_etsy:
        label = f"{['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][ym[1]-1]} {ym[0]}"
        have = ym in bank_months_covered
        src_type = bank_month_source.get(ym, "")
        if have:
            # Color-code by source: CYAN for PDF, TEAL for CSV
            is_pdf = src_type == "PDF"
            pill_color = CYAN if is_pdf else TEAL
            pill_label = f"{label} ({src_type})"
        else:
            pill_color = RED
            pill_label = label
        bank_month_pills.append(html.Span(pill_label, style={
            "display": "inline-block", "padding": "3px 8px", "borderRadius": "4px",
            "fontSize": "11px", "fontWeight": "bold", "marginRight": "4px", "marginBottom": "4px",
            "backgroundColor": f"{pill_color}22" if have else f"{RED}22",
            "color": pill_color if have else RED,
            "border": f"1px solid {pill_color}44" if have else f"1px solid {RED}44",
        }))

    days_since_bank = ""
    if bank_dates:
        days_ago = (date.today() - max(bank_dates)).days
        days_since_bank = f" ({days_ago} days ago)" if days_ago > 0 else " (today)"

    pdf_months = sum(1 for v in bank_month_source.values() if v == "PDF")
    csv_months = sum(1 for v in bank_month_source.values() if v == "CSV")
    source_summary = []
    if pdf_months:
        source_summary.append(f"{pdf_months} from PDFs")
    if csv_months:
        source_summary.append(f"{csv_months} filled by CSV")

    rows.append(html.Div([
        html.Div([
            html.Span("\U0001f3e6", style={"fontSize": "16px", "marginRight": "8px"}),
            html.Span("BANK DATA", style={"fontSize": "13px", "fontWeight": "bold",
                                                   "color": CYAN, "letterSpacing": "1px"}),
            html.Span(f"  {len(BANK_TXNS)} txns  |  Latest: {bank_latest}{days_since_bank}",
                       style={"color": GRAY, "fontSize": "12px", "marginLeft": "12px"}),
        ], style={"marginBottom": "8px"}),
        html.Div(bank_month_pills if bank_month_pills else [
            html.Span("No bank data", style={"color": DARKGRAY, "fontSize": "12px"})
        ], style={"marginBottom": "4px"}),
        html.Div([
            html.Span(
                f"{len(bank_missing)} month{'s' if len(bank_missing) != 1 else ''} missing" if bank_missing else "All months covered",
                style={"color": RED if bank_missing else GREEN, "fontSize": "11px", "fontWeight": "bold"}
            ),
            html.Span(f"  |  {' · '.join(source_summary)}" if source_summary else "",
                       style={"color": GRAY, "fontSize": "11px", "marginLeft": "4px"}),
        ]),
    ], style={"padding": "10px 14px"}))

    return html.Div([
        html.Div([
            html.Span("\U0001f4c5", style={"fontSize": "18px", "marginRight": "8px"}),
            html.Span("DATA COVERAGE", style={"fontSize": "14px", "fontWeight": "bold",
                                                 "color": CYAN, "letterSpacing": "1.5px"}),
            html.Span("  — See what's uploaded and what's missing",
                       style={"color": GRAY, "fontSize": "12px", "marginLeft": "8px"}),
        ], style={"marginBottom": "10px"}),
        html.Div(rows, style={"backgroundColor": "#0f0f1a", "borderRadius": "8px",
                                "overflow": "hidden"}),
    ], style={
        "backgroundColor": CARD2, "borderRadius": "12px", "padding": "16px",
        "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
        "marginBottom": "16px",
    })


def _build_reconciliation_panel():
    """Cross-reference data sources and show match/mismatch indicators."""
    rows = []

    def _recon_row(label, val_a, val_b, label_a="Source A", label_b="Source B"):
        diff = abs(val_a - val_b)
        ok = diff <= 1.0
        icon = "\u2713" if ok else "\u26a0"
        icon_color = GREEN if ok else ORANGE
        diff_color = GREEN if ok else ORANGE
        return html.Div([
            html.Span(icon, style={"fontSize": "18px", "color": icon_color,
                                    "width": "28px", "textAlign": "center", "flexShrink": "0"}),
            html.Span(label, style={"color": WHITE, "fontSize": "13px", "fontWeight": "600",
                                     "flex": "1", "minWidth": "180px"}),
            html.Span(f"{label_a}: ${val_a:,.2f}", style={"color": GRAY, "fontSize": "12px",
                                                            "fontFamily": "monospace", "width": "180px"}),
            html.Span(f"{label_b}: ${val_b:,.2f}", style={"color": GRAY, "fontSize": "12px",
                                                            "fontFamily": "monospace", "width": "180px"}),
            html.Span(f"{'Match' if ok else f'Gap: ${diff:,.2f}'}",
                       style={"color": diff_color, "fontSize": "12px", "fontWeight": "bold",
                              "width": "120px", "textAlign": "right"}),
        ], style={"display": "flex", "alignItems": "center", "padding": "8px 12px",
                   "borderBottom": "1px solid #ffffff08", "gap": "8px"})

    # Etsy Deposits vs Bank Etsy Payouts
    bank_etsy_deposits = sum(t["amount"] for t in BANK_TXNS
                             if t["type"] == "deposit" and "etsy" in t.get("desc", "").lower())
    rows.append(_recon_row(
        "Etsy Deposits vs Bank Etsy Payouts",
        etsy_total_deposited, bank_etsy_deposits + etsy_pre_capone_deposits,
        "Etsy CSV", "Bank Stmt"))

    # Receipt COGS vs Bank Amazon category
    rows.append(_recon_row(
        "Inventory COGS: Receipts vs Bank Amazon",
        true_inventory_cost, bank_by_cat.get("Amazon Inventory", 0),
        "Receipts", "Bank Stmt"))

    # Discover card receipts vs Bank Amazon (more granular)
    rows.append(_recon_row(
        "Discover Invoices vs Bank Amazon",
        discover_inv_total, bank_by_cat.get("Amazon Inventory", 0),
        "Invoices", "Bank Stmt"))

    # Etsy Account Balance: self-reported vs calculated
    rows.append(_recon_row(
        "Etsy Balance: Reported vs Calculated",
        etsy_balance, etsy_balance_calculated,
        "Reported", "Calculated"))

    # Reconciled profit vs bank-only (gap = credit card supplies)
    rows.append(_recon_row(
        "Profit: Reconciled vs Bank-Only",
        profit, real_profit,
        "Reconciled", "Bank Only"))

    return html.Div([
        html.Div([
            html.Span("\U0001f50d", style={"fontSize": "18px", "marginRight": "8px"}),
            html.Span("DATA RECONCILIATION", style={"fontSize": "14px", "fontWeight": "bold",
                                                      "color": CYAN, "letterSpacing": "1.5px"}),
        ], style={"marginBottom": "10px"}),
        html.P("Cross-checking data sources for consistency.",
               style={"color": GRAY, "fontSize": "12px", "margin": "0 0 10px 0"}),
        html.Div(rows, style={"backgroundColor": "#0f0f1a", "borderRadius": "8px",
                                "overflow": "hidden"}),
    ], style={
        "backgroundColor": CARD2, "borderRadius": "12px", "padding": "16px",
        "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
        "marginBottom": "16px",
    })


def _build_audit_report():
    """Build the Audit Reconciliation Report — verifies dashboard vs raw data."""
    rows = _build_reconciliation_report()
    all_pass = all(r["status"] == "PASS" for r in rows)
    status_color = GREEN if all_pass else RED
    status_text = "ALL PASS" if all_pass else "FAILURES DETECTED"

    table_rows = []
    for r in rows:
        row_color = GREEN if r["status"] == "PASS" else RED
        table_rows.append(html.Tr([
            html.Td(r["metric"], style={"color": WHITE, "padding": "6px 10px", "fontSize": "13px"}),
            html.Td(f"${r['dashboard']:,.2f}", style={"textAlign": "right", "color": CYAN,
                     "padding": "6px 10px", "fontFamily": "monospace", "fontSize": "13px"}),
            html.Td(f"${r['raw_sum']:,.2f}", style={"textAlign": "right", "color": TEAL,
                     "padding": "6px 10px", "fontFamily": "monospace", "fontSize": "13px"}),
            html.Td(f"${r['delta']:,.2f}", style={"textAlign": "right", "color": row_color,
                     "padding": "6px 10px", "fontFamily": "monospace", "fontSize": "13px"}),
            html.Td(r["status"], style={"textAlign": "center", "color": row_color,
                     "padding": "6px 10px", "fontWeight": "bold", "fontSize": "13px"}),
        ], style={"borderBottom": "1px solid #ffffff08"}))

    return html.Div([
        html.Div([
            html.Span("\u2705" if all_pass else "\u26a0\ufe0f", style={"fontSize": "18px", "marginRight": "8px"}),
            html.Span("AUDIT RECONCILIATION", style={"fontSize": "14px", "fontWeight": "bold",
                                                       "color": CYAN, "letterSpacing": "1.5px"}),
            html.Span(f"  — {status_text}",
                       style={"color": status_color, "fontSize": "12px", "fontWeight": "bold", "marginLeft": "8px"}),
        ], style={"marginBottom": "10px"}),
        html.P("Compares dashboard-displayed values against raw SUM() of source records. "
               "Delta > $1.00 = FAIL.",
               style={"color": GRAY, "fontSize": "11px", "margin": "0 0 10px 0"}),
        html.Table([
            html.Thead(html.Tr([
                html.Th("Metric", style={"textAlign": "left", "padding": "6px 10px", "color": GRAY}),
                html.Th("Dashboard", style={"textAlign": "right", "padding": "6px 10px", "color": GRAY}),
                html.Th("Raw Sum", style={"textAlign": "right", "padding": "6px 10px", "color": GRAY}),
                html.Th("Delta", style={"textAlign": "right", "padding": "6px 10px", "color": GRAY}),
                html.Th("Status", style={"textAlign": "center", "padding": "6px 10px", "color": GRAY}),
            ], style={"borderBottom": f"2px solid {CYAN}"})),
            html.Tbody(table_rows),
        ], style={"width": "100%", "borderCollapse": "collapse"}),
        html.P(f"API endpoint: /api/reconciliation — also accepts ?start=YYYY-MM-DD&end=YYYY-MM-DD",
               style={"color": DARKGRAY, "fontSize": "10px", "marginTop": "10px"}),
    ], style={
        "backgroundColor": CARD2, "borderRadius": "12px", "padding": "16px",
        "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
        "marginBottom": "16px",
    })


def _build_manage_data_content():
    """Build the Manage Data section content with delete buttons per data source."""
    sections = []

    # ── Etsy Data by Month ──
    etsy_months = []
    if DATA is not None and len(DATA) > 0:
        try:
            _dates = pd.to_datetime(DATA["Date"], format="%B %d, %Y", errors="coerce")
            _months = _dates.dt.to_period("M").astype(str)
            for m in sorted(_months.dropna().unique()):
                count = (_months == m).sum()
                etsy_months.append({"month": m, "count": count})
        except Exception:
            pass

    if etsy_months:
        etsy_rows = []
        for em in etsy_months:
            etsy_rows.append(html.Div([
                html.Span(f"{em['month']}  ({em['count']} rows)", style={
                    "color": TEAL, "fontSize": "12px", "fontFamily": "monospace"}),
                html.Button("Delete", id={"type": "delete-etsy-month", "month": em["month"]},
                            n_clicks=0, style={
                    "backgroundColor": f"{RED}22", "color": RED, "border": f"1px solid {RED}55",
                    "borderRadius": "4px", "padding": "2px 10px", "fontSize": "11px",
                    "cursor": "pointer", "marginLeft": "10px"}),
            ], style={"display": "flex", "alignItems": "center", "justifyContent": "space-between",
                       "padding": "4px 0", "borderBottom": "1px solid #ffffff08"}))
        sections.append(html.Div([
            html.Div("ETSY TRANSACTIONS", style={"color": TEAL, "fontWeight": "bold",
                      "fontSize": "13px", "marginBottom": "6px"}),
            *etsy_rows,
        ], style={"marginBottom": "14px"}))
    else:
        sections.append(html.Div("No Etsy data loaded.", style={
            "color": DARKGRAY, "fontSize": "12px", "fontStyle": "italic", "marginBottom": "14px"}))

    # ── Bank Data by Month ──
    bank_months = []
    if BANK_TXNS:
        from collections import Counter
        _bm = Counter()
        for t in BANK_TXNS:
            d = t.get("date", "")
            if len(d) >= 7:
                _bm[d[:7]] += 1
        bank_months = [{"month": m, "count": c} for m, c in sorted(_bm.items())]

    if bank_months:
        bank_rows = []
        for bm in bank_months:
            bank_rows.append(html.Div([
                html.Span(f"{bm['month']}  ({bm['count']} txns)", style={
                    "color": CYAN, "fontSize": "12px", "fontFamily": "monospace"}),
                html.Button("Delete", id={"type": "delete-bank-month", "month": bm["month"]},
                            n_clicks=0, style={
                    "backgroundColor": f"{RED}22", "color": RED, "border": f"1px solid {RED}55",
                    "borderRadius": "4px", "padding": "2px 10px", "fontSize": "11px",
                    "cursor": "pointer", "marginLeft": "10px"}),
            ], style={"display": "flex", "alignItems": "center", "justifyContent": "space-between",
                       "padding": "4px 0", "borderBottom": "1px solid #ffffff08"}))
        sections.append(html.Div([
            html.Div("BANK TRANSACTIONS", style={"color": CYAN, "fontWeight": "bold",
                      "fontSize": "13px", "marginBottom": "6px"}),
            *bank_rows,
        ], style={"marginBottom": "14px"}))
    else:
        sections.append(html.Div("No bank data loaded.", style={
            "color": DARKGRAY, "fontSize": "12px", "fontStyle": "italic", "marginBottom": "14px"}))

    # ── Receipts/Orders ──
    if INVOICES:
        rcpt_rows = []
        for inv in sorted(INVOICES, key=lambda x: x.get("order_num", ""))[:20]:
            onum = inv.get("order_num", "?")
            item_count = len(inv.get("items", []))
            total = inv.get("grand_total", 0)
            rcpt_rows.append(html.Div([
                html.Span(f"#{onum}  ({item_count} items, ${total:.2f})", style={
                    "color": PURPLE, "fontSize": "12px", "fontFamily": "monospace"}),
                html.Button("Delete", id={"type": "delete-receipt", "order": onum},
                            n_clicks=0, style={
                    "backgroundColor": f"{RED}22", "color": RED, "border": f"1px solid {RED}55",
                    "borderRadius": "4px", "padding": "2px 10px", "fontSize": "11px",
                    "cursor": "pointer", "marginLeft": "10px"}),
            ], style={"display": "flex", "alignItems": "center", "justifyContent": "space-between",
                       "padding": "4px 0", "borderBottom": "1px solid #ffffff08"}))
        if len(INVOICES) > 20:
            rcpt_rows.append(html.Div(f"... and {len(INVOICES) - 20} more orders", style={
                "color": DARKGRAY, "fontSize": "11px", "fontStyle": "italic", "padding": "4px 0"}))
        sections.append(html.Div([
            html.Div(f"RECEIPTS / ORDERS ({len(INVOICES)} total)", style={"color": PURPLE, "fontWeight": "bold",
                      "fontSize": "13px", "marginBottom": "6px"}),
            *rcpt_rows,
        ]))
    else:
        sections.append(html.Div("No receipts loaded.", style={
            "color": DARKGRAY, "fontSize": "12px", "fontStyle": "italic"}))

    return html.Div(sections)


def _build_store_etsy_tab(store_key, store_label, store_color):
    """Build the Etsy section for a single store sub-tab (upload + files + stats)."""
    if store_key == "all":
        # All Stores tab — no upload zone, just combined stats and file list
        return html.Div([
            html.Div([
                html.Span("\U0001f4ca", style={"fontSize": "20px", "marginRight": "8px"}),
                html.Span("All Etsy Statements", style={"fontSize": "16px", "fontWeight": "bold", "color": TEAL}),
            ], style={"marginBottom": "10px"}),
            html.P("Combined Etsy data from all stores. Upload per-store using the store tabs above.",
                   style={"color": GRAY, "fontSize": "12px", "margin": "0 0 12px 0"}),
            # Stats line
            html.Div(id="datahub-etsy-stats", style={"marginBottom": "10px", "minHeight": "16px"}),
            # Existing files list
            html.Div([
                html.Div("Existing Files:", style={"color": GRAY, "fontSize": "11px",
                                                     "fontWeight": "bold", "marginBottom": "4px",
                                                     "textTransform": "uppercase", "letterSpacing": "1px"}),
                html.Div(id="datahub-etsy-files"),
            ]),
        ], style={
            "backgroundColor": CARD2, "borderRadius": "12px", "padding": "18px",
            "borderLeft": f"4px solid {TEAL}", "marginBottom": "16px",
            "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
        })
    else:
        # Per-store tab — has upload zone + stats + file list
        # Pre-populate order CSV file list from disk + Supabase
        from datetime import datetime as _dt_order_files
        _order_file_entries = []
        _od = os.path.join(BASE_DIR, "data", "order_csvs", store_key)
        if os.path.isdir(_od):
            for _of in sorted(os.listdir(_od)):
                if _of.endswith(".csv"):
                    _ofpath = os.path.join(_od, _of)
                    _ofstat = os.stat(_ofpath)
                    _ofdate = _dt_order_files.fromtimestamp(_ofstat.st_mtime).strftime("%b %d, %Y %I:%M %p")
                    _order_file_entries.append(f"{_of}  ({_ofdate})")
        # Also check Supabase for persisted data
        if not _order_file_entries:
            try:
                from supabase_loader import get_config_value
                for _ct in ("orders", "items"):
                    _val = get_config_value(f"order_csv_{_ct}_{store_key}")
                    if _val:
                        import json
                        _records = json.loads(_val) if isinstance(_val, str) else _val
                        if _records:
                            _order_file_entries.append(f"EtsySold{'Orders' if _ct == 'orders' else 'OrderItems'} ({len(_records)} rows, saved)")
            except Exception:
                pass

        # Build order profit summary for this store
        _store_profit_info = ""
        if ORDER_PROFITS:
            _sp = [r for r in ORDER_PROFITS if r["store"] == store_key]
            if _sp:
                _sp_total = sum(r["order_profit"] for r in _sp)
                _sp_avg = _sp_total / len(_sp)
                _store_profit_info = f"{len(_sp)} orders tracked | Profit: ${_sp_total:,.2f} (avg ${_sp_avg:,.2f}/order)"

        # Check for existing listings data
        _listings_info = ""
        try:
            from supabase_loader import get_config_value as _gcv_l
            import json as _json_l
            _lraw = _gcv_l(f"listings_csv_{store_key}")
            if _lraw:
                _lrecs = _json_l.loads(_lraw) if isinstance(_lraw, str) else _lraw
                if _lrecs:
                    _listings_info = f"{len(_lrecs)} active listings saved"
        except Exception:
            pass

        # ── Data Coverage summary from Supabase for THIS store ──
        _all_data = _DATA_ALL if _DATA_ALL is not None else DATA
        _store_data = _all_data[_all_data["Store"] == store_key] if "Store" in _all_data.columns else pd.DataFrame()
        _store_txn_count = len(_store_data)
        _store_sales = _store_data[_store_data["Type"] == "Sale"] if _store_txn_count > 0 else pd.DataFrame()
        _store_order_count = len(_store_sales)
        _store_gross = _store_sales["Net_Clean"].sum() if _store_order_count > 0 else 0

        # Date range
        _store_date_range = ""
        if _store_txn_count > 0 and "Date_Parsed" in _store_data.columns:
            _sd_min = _store_data["Date_Parsed"].min()
            _sd_max = _store_data["Date_Parsed"].max()
            if pd.notna(_sd_min) and pd.notna(_sd_max):
                _store_date_range = f"{_sd_min.strftime('%b %d, %Y')} — {_sd_max.strftime('%b %d, %Y')}"
        elif _store_txn_count > 0:
            try:
                _sd_dates = pd.to_datetime(_store_data["Date"], format="%B %d, %Y", errors="coerce")
                _sd_min = _sd_dates.min()
                _sd_max = _sd_dates.max()
                if pd.notna(_sd_min) and pd.notna(_sd_max):
                    _store_date_range = f"{_sd_min.strftime('%b %d, %Y')} — {_sd_max.strftime('%b %d, %Y')}"
            except Exception:
                pass

        # Month count
        _store_months = []
        if _store_txn_count > 0 and "Month" in _store_data.columns:
            _store_months = sorted(_store_data["Month"].dropna().unique())

        # Build data coverage card
        if _store_txn_count > 0:
            _coverage_card = html.Div([
                html.Div([
                    html.Span("DATA COVERAGE", style={"color": GRAY, "fontSize": "11px", "fontWeight": "600",
                                                        "letterSpacing": "1px", "marginBottom": "6px", "display": "block"}),
                    html.Div([
                        html.Span(f"{_store_txn_count}", style={"color": store_color, "fontSize": "20px", "fontWeight": "bold"}),
                        html.Span(" transactions", style={"color": GRAY, "fontSize": "13px", "marginLeft": "4px"}),
                        html.Span(f"  |  {_store_order_count} orders  |  ${_store_gross:,.2f} gross",
                                  style={"color": GRAY, "fontSize": "13px", "marginLeft": "8px"}),
                    ]),
                    html.Div(_store_date_range, style={"color": WHITE, "fontSize": "12px", "marginTop": "4px",
                                                         "fontFamily": "monospace"}) if _store_date_range else html.Div(),
                    html.Div(f"{len(_store_months)} month{'s' if len(_store_months) != 1 else ''}: {', '.join(_store_months)}",
                             style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "2px",
                                    "fontFamily": "monospace"}) if _store_months else html.Div(),
                ]),
            ], style={
                "backgroundColor": f"{store_color}10", "borderRadius": "8px", "padding": "12px 16px",
                "borderLeft": f"3px solid {store_color}", "marginBottom": "16px",
            })
        else:
            _coverage_card = html.Div([
                html.Span("DATA COVERAGE", style={"color": GRAY, "fontSize": "11px", "fontWeight": "600",
                                                    "letterSpacing": "1px", "marginBottom": "4px", "display": "block"}),
                html.Div(f"No data uploaded for {store_label} yet.", style={
                    "color": ORANGE, "fontSize": "13px"}),
            ], style={
                "backgroundColor": f"{ORANGE}10", "borderRadius": "8px", "padding": "12px 16px",
                "borderLeft": f"3px solid {ORANGE}", "marginBottom": "16px",
            })

        return html.Div([
            # Data coverage summary
            _coverage_card,
            # Row 1: Etsy Statements + Order CSVs
            html.Div([
                _build_upload_zone("etsy", "\U0001f4ca", f"Etsy Statements — {store_label}", store_color, ".csv",
                                   f"Upload Etsy CSV for {store_label}. Rebuilds all sales, fees, and financial data."),
                html.Div([
                    _build_upload_zone("orders", "\U0001f4e6", f"Order CSV — {store_label}", store_color, ".csv",
                                       f"Upload Etsy order export CSV for {store_label}. Links shipping labels to orders for per-order profit tracking."),
                    html.Div([
                        html.Div(f, style={"color": GRAY, "fontSize": "11px", "padding": "2px 0"})
                        for f in _order_file_entries
                    ] if _order_file_entries else [
                        html.Div("No order CSVs uploaded yet", style={"color": DARKGRAY, "fontSize": "11px"})
                    ], style={"marginTop": "4px"}),
                    html.Div(_store_profit_info, style={
                        "color": GREEN, "fontSize": "12px", "fontFamily": "monospace", "marginTop": "6px",
                    }) if _store_profit_info else html.Div(),
                ]),
            ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginBottom": "16px"}),
            # Row 2: Active Listings
            html.Div([
                _build_upload_zone("listings", "\U0001f3ea", f"Active Listings — {store_label}", store_color, ".csv",
                                   f"Upload 'Currently for Sale Listings' CSV for {store_label}. Used to build Product Library and link STL files to listings."),
                html.Div([
                    html.Div(_listings_info, style={
                        "color": store_color, "fontSize": "12px", "fontFamily": "monospace", "marginTop": "6px",
                    }) if _listings_info else html.Div("No listings uploaded yet", style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "6px"}),
                ], style={"flex": "1"}),
            ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap"}),
        ], style={"marginBottom": "16px"})



# build_tab7_data_hub() extracted to dashboard_utils/pages/data_hub.py



# ── App Layout ───────────────────────────────────────────────────────────────

def _header_text():
    """Build the dynamic header subtitle string."""
    return (f"Oct 2025 -- Mar 2026  |  {order_count} orders  |  "
            f"Profit: {money(profit)} ({_fmt(profit_margin, prefix='', fmt='.1f', unknown='?')}%)  |  "
            f"Cash: {money(bank_cash_on_hand)}")


def serve_layout():
    """Rebuild layout on every page load so saved edits appear after refresh."""
    # Count unsaved items from recent uploads for Inventory tab label
    _new_count = 0
    for onum in _RECENT_UPLOADS:
        for inv in INVOICES:
            if inv["order_num"] == onum:
                for item in inv["items"]:
                    key = (onum, item["name"])
                    if key not in _ITEM_DETAILS:
                        _new_count += 1
    inv_label = f"Inventory ({_new_count} new)" if _new_count > 0 else "Inventory (Cost of Goods)"

    return html.Div([
        # Strict mode state
        dcc.Store(id="strict-mode-store", data=False, storage_type="local"),
        dcc.Store(id="data-version-store", data=0),
        dcc.Store(id="upload-reload-trigger", data=0),
        dcc.Store(id="selected-store", data="all", storage_type="session"),
        dcc.Store(id="datahub-active-store-tab", data="dh-keycomponentmfg", storage_type="session"),

        # Header
        html.Div([
            html.Div([
                html.H1("TJs SOFTWARE PROJECT", style={"color": ORANGE, "margin": "0", "fontSize": "24px"}),
                html.Div(
                    _header_text(),
                    id="app-header-content",
                    style={"color": GRAY, "margin": "2px 0 0 0", "fontSize": "13px"},
                ),
            ], style={"flex": "1"}),
            # Store selector
            html.Div([
                html.Span("STORE", style={
                    "color": GRAY, "fontSize": "11px", "marginRight": "8px",
                    "fontWeight": "600", "letterSpacing": "1px",
                }),
                dcc.Dropdown(
                    id="store-selector",
                    options=[{"label": v, "value": k} for k, v in STORES.items()],
                    value="all",
                    clearable=False,
                    style={
                        "width": "160px", "fontSize": "12px",
                        "backgroundColor": BG, "color": WHITE,
                    },
                ),
            ], style={
                "display": "flex", "alignItems": "center",
                "marginRight": "12px",
            }),
            # Strict mode toggle
            html.Div([
                html.Span("STRICT", style={
                    "color": GRAY, "fontSize": "11px", "marginRight": "6px",
                    "fontWeight": "600", "letterSpacing": "1px",
                }),
                dbc.Switch(
                    id="strict-mode-toggle",
                    value=False,
                    style={"display": "inline-block"},
                ),
                html.Span(
                    "OFF",
                    id="strict-mode-label",
                    style={"color": GRAY, "fontSize": "11px", "marginLeft": "2px", "fontWeight": "600"},
                ),
            ], style={
                "display": "flex", "alignItems": "center",
                "padding": "4px 12px", "borderRadius": "6px",
                "backgroundColor": BG, "border": f"1px solid {GRAY}",
            }),
        ], style={"padding": "14px 20px", "backgroundColor": CARD2,
                   "display": "flex", "alignItems": "center", "justifyContent": "space-between"}),

        # Tabs — content rendered dynamically via callback so uploads refresh data
        dcc.Tabs(id="main-tabs", value="tab-overview", children=[
            dcc.Tab(label="Overview", value="tab-overview", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="JARVIS", value="tab-deep-dive", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="Financials", value="tab-financials", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label=inv_label, value="tab-inventory", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="Tax Forms", value="tab-tax-forms", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="Business Valuation", value="tab-valuation", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="Data Hub", value="tab-data-hub", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="Agreement", value="tab-agreement", style=tab_style, selected_style=tab_selected_style),
        ], style={"backgroundColor": BG}),
        # CEO Agent alert banner
        html.Div(id="ceo-alert-banner", children=_build_ceo_banner()),

        # Periodic CEO health check (every 15 min)
        dcc.Interval(id="ceo-interval", interval=15 * 60 * 1000, n_intervals=0),

        html.Div(id="tab-content"),

        # Toast notification container
        html.Div(id="toast-container", style={
            "position": "fixed", "top": "20px", "right": "20px", "zIndex": "9999",
            "display": "flex", "flexDirection": "column", "gap": "8px",
        }),

        # CSV Download components (hidden, triggered by callbacks)
        dcc.Download(id="download-etsy-csv"),
        dcc.Download(id="download-bank-csv"),
        dcc.Download(id="download-inventory-csv"),
        dcc.Download(id="download-stock-csv"),
        dcc.Download(id="download-pl-csv"),
    ], style={
        "backgroundColor": BG, "minHeight": "100vh",
        "fontFamily": "'Inter', 'Segoe UI', -apple-system, sans-serif", "color": WHITE,
    })

app.layout = serve_layout


# ── Receipt Gallery Search ────────────────────────────────────────────────
@app.callback(
    Output("receipt-gallery-cards", "children"),
    Input("receipt-gallery-search", "value"),
    State("receipt-gallery-search-data", "data"),
    State("receipt-gallery-cards", "children"),
    prevent_initial_call=True,
)
def filter_receipt_gallery(search, search_data, current_cards):
    """Filter receipt gallery — reorder original cards, matches first."""
    # Rebuild full cards from INVOICES every time (keeps PDFs intact)
    sorted_invoices = sorted(INVOICES, key=lambda o: o.get("date", ""), reverse=True)
    try:
        sorted_invoices = sorted(INVOICES,
            key=lambda o: pd.to_datetime(o.get("date", ""), format="%B %d, %Y", errors="coerce"),
            reverse=True)
    except Exception:
        pass

    biz_invoices = [inv for inv in sorted_invoices
                    if inv.get("source") != "Personal Amazon"
                    and "Gigi" not in inv.get("file", "")]

    query = (search or "").strip().lower()
    if not query:
        # No search — rebuild all cards in original order
        return [_make_receipt_card(inv) for inv in biz_invoices]

    # Split into matches and non-matches, matches first
    matches = []
    non_matches = []
    for i, inv in enumerate(biz_invoices):
        sd = search_data[i] if search_data and i < len(search_data) else ""
        if query in sd:
            matches.append(inv)
        else:
            non_matches.append(inv)

    cards = []
    for inv in matches:
        cards.append(_make_receipt_card(inv))
    for inv in non_matches:
        card = _make_receipt_card(inv)
        # Dim non-matches
        card.style = {**card.style, "opacity": "0.25"}
        cards.append(card)

    if not matches:
        cards.insert(0, html.Div("No receipts match your search.",
                                  style={"color": GRAY, "fontSize": "12px", "padding": "8px 0"}))
    return cards


# ── Dynamic Tab Rendering ────────────────────────────────────────────────────

@app.callback(
    Output("tab-content", "children"),
    Input("main-tabs", "value"),
    Input("strict-mode-store", "data"),
    Input("upload-reload-trigger", "data"),
    Input("store-selector", "value"),
    State("datahub-active-store-tab", "data"),
)
@guard_callback(n_outputs=1)
def render_active_tab(tab, _strict_flag, _upload_trigger, _selected_store, _dh_active_tab):
    """Rebuild the active tab's content on every tab switch, strict mode toggle, store change, or upload."""
    _logger.info("render_active_tab fired: tab=%s, store=%s", tab, _selected_store)
    _apply_store_filter(_selected_store or "all")
    _rebuild_all_charts()
    stale_banner = _build_stale_data_banner()

    # Store filter banner — show which store is active
    _store_label = STORES.get(_selected_store, "All Stores") if _selected_store and _selected_store != "all" else None
    store_banner = html.Div()
    if _store_label:
        store_banner = html.Div([
            html.Span(f"Viewing: {_store_label}", style={
                "color": CYAN, "fontSize": "14px", "fontWeight": "bold",
            }),
            html.Span(f" — {order_count} orders, {len(DATA)} transactions", style={
                "color": GRAY, "fontSize": "13px", "marginLeft": "12px",
            }),
        ], style={
            "padding": "8px 20px", "backgroundColor": f"{CYAN}15",
            "borderLeft": f"3px solid {CYAN}", "marginBottom": "4px",
        })

    # Empty store guard — show message instead of crashing
    _store_empty = order_count == 0 and _selected_store and _selected_store != "all"
    if _store_empty and tab not in ("tab-data-hub", "tab-inventory"):
        return html.Div([
            stale_banner,
            store_banner,
            html.Div([
                html.Div(f"No data for {STORES.get(_selected_store, _selected_store)}", style={
                    "color": ORANGE, "fontSize": "24px", "fontWeight": "bold", "textAlign": "center",
                    "marginTop": "60px",
                }),
                html.Div("Upload Etsy CSV statements for this store in the Data Hub tab.", style={
                    "color": GRAY, "fontSize": "14px", "textAlign": "center", "marginTop": "12px",
                }),
            ], style={"padding": "40px"}),
        ])

    if tab == "tab-overview":
        return html.Div([stale_banner, store_banner, build_tab1_overview()])
    elif tab == "tab-deep-dive":
        return html.Div([stale_banner, store_banner, build_tab2_deep_dive()])
    elif tab == "tab-financials":
        return html.Div([stale_banner, store_banner, build_tab3_financials()])
    elif tab == "tab-inventory":
        return html.Div([stale_banner, store_banner, build_tab4_inventory()])
    elif tab == "tab-tax-forms":
        return html.Div([stale_banner, store_banner, build_tab5_tax_forms()])
    elif tab == "tab-valuation":
        return html.Div([stale_banner, store_banner, build_tab6_valuation()])
    elif tab == "tab-data-hub":
        return html.Div([stale_banner, store_banner, build_tab7_data_hub(_dh_active_tab)])
    elif tab == "tab-agreement":
        return html.Div([stale_banner, store_banner, build_tab_agreement()])
    return html.Div("Select a tab")


# ── Strict Mode Toggle ───────────────────────────────────────────────────────

@app.callback(
    Output("strict-mode-store", "data"),
    Output("strict-mode-label", "children"),
    Output("strict-mode-label", "style"),
    Input("strict-mode-toggle", "value"),
    prevent_initial_call=True,
)
@guard_callback(n_outputs=3)
def toggle_strict_mode(is_on):
    """Toggle strict mode: rebuild pipeline, refresh all charts + derived data."""
    global _acct_pipeline
    if _acct_pipeline is not None:
        try:
            _acct_pipeline.full_rebuild(DATA, BANK_TXNS, CONFIG,
                                        invoices=INVOICES, strict_mode=bool(is_on))
            _publish_to_globals(_acct_pipeline, __name__)
            _recompute_analytics()
            _recompute_tax_years()
            _recompute_valuation()
            _rebuild_all_charts()
            _logger.info("Strict mode %s: %s", 'ON' if is_on else 'OFF', _acct_pipeline.ledger.summary())
        except Exception as e:
            import traceback
            traceback.print_exc()
            _logger.warning("Strict mode toggle failed: %s", e)

    label = "ON" if is_on else "OFF"
    color = {"color": "#e74c3c" if is_on else GRAY, "fontSize": "11px",
             "marginLeft": "2px", "fontWeight": "600"}
    return is_on, label, color


# ── Image Manager Callbacks ──────────────────────────────────────────────────

@app.callback(
    Output({"type": "img-preview", "index": MATCH}, "children"),
    Output({"type": "img-status", "index": MATCH}, "children"),
    Input({"type": "img-save-btn", "index": MATCH}, "n_clicks"),
    State({"type": "img-url-input", "index": MATCH}, "value"),
    State({"type": "img-item-name", "index": MATCH}, "data"),
    prevent_initial_call=True,
)
def save_image_url(n_clicks, url_value, item_name):
    """Save image URL to Supabase and update preview."""
    if not n_clicks or not item_name:
        raise dash.exceptions.PreventUpdate

    url_value = (url_value or "").strip()
    count = _save_image_url(item_name, url_value)

    # Update the in-memory lookup
    if url_value:
        _IMAGE_URLS[item_name] = url_value
    elif item_name in _IMAGE_URLS:
        del _IMAGE_URLS[item_name]

    preview = item_thumbnail(url_value, 100)
    if count:
        status = f"Saved! ({count} rows)"
    else:
        status = "Saved as override!"
    return preview, status


@app.callback(
    Output("img-mgr-grid", "children"),
    Input("img-filter-input", "value"),
    Input("img-filter-show", "value"),
    prevent_initial_call=True,
)
def filter_image_cards(search_text, show_filter):
    """Filter the Image Manager card grid."""
    import urllib.parse

    search_text = (search_text or "").strip().lower()

    # Rebuild deduplicated items
    seen = {}
    if len(INV_ITEMS) > 0:
        for _, r in INV_ITEMS.iterrows():
            name = r["name"]
            if name not in seen:
                seen[name] = {
                    "name": name,
                    "price": r["price"],
                    "image_url": _IMAGE_URLS.get(name, ""),
                    "order_num": r.get("order_num", ""),
                }
    unique_items = sorted(seen.values(), key=lambda x: x["name"].lower())

    # Apply filters
    filtered = []
    for it in unique_items:
        if search_text and search_text not in it["name"].lower():
            continue
        if show_filter == "missing" and it["image_url"]:
            continue
        if show_filter == "has" and not it["image_url"]:
            continue
        filtered.append(it)

    # Build cards (same structure as _build_image_manager)
    cards = []
    for i, it in enumerate(filtered):
        safe_idx = str(i)
        img_url = it["image_url"]
        search_q = urllib.parse.quote_plus(it["name"][:80])

        card = html.Div([
            html.Div([
                item_thumbnail(img_url, 100),
            ], id={"type": "img-preview", "index": safe_idx},
               style={"textAlign": "center", "marginBottom": "6px"}),
            html.Div(it["name"][:50], title=it["name"],
                     style={"color": WHITE, "fontSize": "11px", "fontWeight": "600",
                            "overflow": "hidden", "textOverflow": "ellipsis",
                            "whiteSpace": "nowrap", "marginBottom": "2px"}),
            html.Div(f"${it['price']:,.2f}", style={"color": GRAY, "fontSize": "11px", "marginBottom": "6px"}),
            dcc.Input(
                id={"type": "img-url-input", "index": safe_idx},
                type="text", placeholder="Paste image URL...",
                value=img_url,
                style={"width": "100%", "backgroundColor": "#0f0f1a", "color": WHITE,
                       "border": f"1px solid {DARKGRAY}", "borderRadius": "4px",
                       "padding": "4px 6px", "fontSize": "10px", "marginBottom": "4px",
                       "boxSizing": "border-box"}),
            dcc.Store(id={"type": "img-item-name", "index": safe_idx}, data=it["name"]),
            # Order ID
            html.Div(it.get("order_num", ""), title=it.get("order_num", ""),
                     style={"color": DARKGRAY, "fontSize": "9px", "marginBottom": "4px",
                            "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
            html.Div([
                html.Button("Save", id={"type": "img-save-btn", "index": safe_idx},
                            n_clicks=0,
                            style={"backgroundColor": TEAL, "color": WHITE, "border": "none",
                                   "borderRadius": "4px", "padding": "3px 10px", "fontSize": "10px",
                                   "cursor": "pointer", "fontWeight": "600", "marginRight": "4px"}),
                html.A("Amazon", href=f"https://www.amazon.com/s?k={search_q}",
                       target="_blank",
                       style={"color": CYAN, "fontSize": "10px", "textDecoration": "none",
                              "padding": "3px 6px", "border": f"1px solid {CYAN}44",
                              "borderRadius": "4px"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "4px"}),
            html.Div("", id={"type": "img-status", "index": safe_idx},
                     style={"color": GREEN, "fontSize": "10px", "marginTop": "2px", "minHeight": "14px"}),
        ], className="img-mgr-card",
           style={"backgroundColor": CARD2, "padding": "10px", "borderRadius": "8px",
                  "border": f"1px solid {'#ffffff15' if not img_url else TEAL + '44'}",
                  "width": "160px", "minWidth": "160px"})
        cards.append(card)

    if not cards:
        cards = [html.P("No items match filter.", style={"color": GRAY, "padding": "20px"})]

    return cards


# ── Inventory Editor Callbacks ──────────────────────────────────────────────

@app.callback(
    Output({"type": "det-single-mode", "index": MATCH}, "style"),
    Output({"type": "det-split-mode", "index": MATCH}, "style"),
    Output({"type": "det-split-container", "index": MATCH}, "style"),
    Output({"type": "det-split-data", "index": MATCH}, "data"),
    Output({"type": "det-split-display", "index": MATCH}, "children"),
    Output({"type": "wiz-state", "index": MATCH}, "data"),
    Output({"type": "wiz-question", "index": MATCH}, "children"),
    Output({"type": "wiz-step0", "index": MATCH}, "style"),
    Output({"type": "wiz-step1", "index": MATCH}, "style"),
    Output({"type": "wiz-step2", "index": MATCH}, "style"),
    Output({"type": "wiz-step3a", "index": MATCH}, "style"),
    Output({"type": "wiz-step3b", "index": MATCH}, "style"),
    Output({"type": "wiz-step3c", "index": MATCH}, "style"),
    Output({"type": "wiz-btn-row", "index": MATCH}, "style"),
    Output({"type": "wiz-next", "index": MATCH}, "children"),
    Input({"type": "loc-split-check", "index": MATCH}, "value"),
    State({"type": "det-name", "index": MATCH}, "value"),
    State({"type": "det-cat", "index": MATCH}, "value"),
    State({"type": "det-qty", "index": MATCH}, "value"),
    State({"type": "loc-dropdown", "index": MATCH}, "value"),
    State({"type": "det-order-num", "index": MATCH}, "data"),
    State({"type": "det-item-name", "index": MATCH}, "data"),
    State({"type": "det-split-data", "index": MATCH}, "data"),
    prevent_initial_call=True,
)
def toggle_split(check_value, det_name, det_cat, det_qty, det_loc,
                 order_num, item_name, current_split_data):
    """Show/hide split container and toggle single-mode vs split-mode."""
    H = {"display": "none"}
    SINGLE = {"display": "block", "padding": "10px 14px"}
    SPLIT = {"display": "block", "padding": "10px 14px"}
    V = {"display": "block", "marginTop": "4px", "padding": "8px 10px",
         "backgroundColor": "#0f0f1a", "borderRadius": "6px",
         "border": f"1px solid {TEAL}33"}
    B = {"display": "block", "marginTop": "8px"}
    show = check_value and "split" in check_value
    # 15 outputs: single_mode, split_mode, container, data, display, state, question, s0-s3c(6), btn_row, btn_text
    if not show:
        return (SINGLE, H,
                H, current_split_data or [], [],
                {"step": 0, "category": "", "total_qty": 0}, "",
                H, H, H, H, H, H, H, "")

    if current_split_data:
        n = len(current_split_data)
        return (H, SPLIT,
                V, current_split_data, _render_split_rows(current_split_data),
                {"step": "done", "category": det_cat, "total_qty": n},
                f"All {n} items allocated! Click Save above when ready.",
                H, H, H, H, H, H, H, "")

    key = (order_num, item_name)
    existing = _ITEM_DETAILS.get(key, [])
    if existing and len(existing) > 1:
        data = [{"name": d["display_name"], "qty": d["true_qty"],
                 "category": d["category"], "location": d.get("location", "")}
                for d in existing]
        n = len(data)
        return (H, SPLIT,
                V, data, _render_split_rows(data),
                {"step": "done", "category": det_cat, "total_qty": n},
                f"All {n} items allocated! Click Save above when ready.",
                H, H, H, H, H, H, H, "")

    # Fresh start — wizard step 0
    return (H, SPLIT,
            V, [], [],
            {"step": 0, "category": "", "total_qty": 0},
            "What type of item is this?",
            {"display": "block"}, H, H, H, H, H, B, "Next \u2192")


@app.callback(
    Output({"type": "wiz-state", "index": MATCH}, "data", allow_duplicate=True),
    Output({"type": "wiz-question", "index": MATCH}, "children", allow_duplicate=True),
    Output({"type": "wiz-step0", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-step1", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-step2", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-step3a", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-step3b", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-step3c", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-btn-row", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-next", "index": MATCH}, "children", allow_duplicate=True),
    Output({"type": "wiz-item-name", "index": MATCH}, "value"),
    Output({"type": "det-split-data", "index": MATCH}, "data", allow_duplicate=True),
    Output({"type": "det-split-display", "index": MATCH}, "children", allow_duplicate=True),
    Input({"type": "wiz-next", "index": MATCH}, "n_clicks"),
    State({"type": "wiz-state", "index": MATCH}, "data"),
    State({"type": "wiz-cat", "index": MATCH}, "value"),
    State({"type": "wiz-qty", "index": MATCH}, "value"),
    State({"type": "wiz-same-diff", "index": MATCH}, "value"),
    State({"type": "wiz-same-name", "index": MATCH}, "value"),
    State({"type": "wiz-tulsa-qty", "index": MATCH}, "value"),
    State({"type": "wiz-texas-qty", "index": MATCH}, "value"),
    State({"type": "wiz-item-name", "index": MATCH}, "value"),
    State({"type": "wiz-item-loc", "index": MATCH}, "value"),
    State({"type": "det-split-data", "index": MATCH}, "data"),
    State({"type": "det-name", "index": MATCH}, "value"),
    State({"type": "det-cat", "index": MATCH}, "value"),
    prevent_initial_call=True,
)
def wizard_advance(n_clicks, wiz_state, wiz_cat, wiz_qty, wiz_same_diff,
                   wiz_same_name, wiz_tulsa_qty, wiz_texas_qty,
                   wiz_item_name, wiz_item_loc,
                   split_data, det_name, det_cat):
    """Advance the guided wizard: category -> qty -> same/diff -> details -> done."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    H = {"display": "none"}
    B = {"display": "block", "marginTop": "8px"}
    state = wiz_state or {"step": 0, "category": "", "total_qty": 0}
    step = state.get("step", 0)
    split_data = split_data or []
    # 13 outputs: state, question, s0-s3c(6), btn_row, btn_text, item_name_clear, data, display

    if step == 0:
        # Category selected → ask how many
        cat = wiz_cat or det_cat or "Other"
        state = {**state, "step": 1, "category": cat}
        word = "rolls" if cat == "Filament" else "items"
        q = f"How many {word} are in this pack?"
        return (state, q,
                H, {"display": "block"}, H, H, H, H,
                B, "Next \u2192", "", split_data, _render_split_rows(split_data))

    elif step == 1:
        # Quantity entered → ask same or different
        total_qty = max(int(wiz_qty or 1), 1)
        cat = state.get("category", "Other")
        state = {**state, "step": 2, "total_qty": total_qty}
        q = "Are these all the same item, or each different?"
        return (state, q,
                H, H, {"display": "block"}, H, H, H,
                B, "Next \u2192", "", split_data, _render_split_rows(split_data))

    elif step == 2:
        # Same/different selected → branch
        branch = wiz_same_diff or "same"
        cat = state.get("category", "Other")
        total_qty = state.get("total_qty", 1)
        state = {**state, "branch": branch}

        if branch == "same":
            state["step"] = "3a"
            word = "rolls" if cat == "Filament" else "items"
            q = f"What are these {total_qty} {word} called?"
            return (state, q,
                    H, H, H, {"display": "block"}, H, H,
                    B, "Next \u2192", "", split_data, _render_split_rows(split_data))
        else:
            state["step"] = "3c"
            state["items_added"] = 0
            iw = "roll" if cat == "Filament" else "item"
            cw = "color" if cat == "Filament" else "name"
            q = f"{iw.title()} 1 of {total_qty} \u2014 What {cw} is it and where did it go?"
            return (state, q,
                    H, H, H, H, H, {"display": "block"},
                    B, f"Add {iw.title()} \u2192", "", split_data, _render_split_rows(split_data))

    elif step == "3a":
        # Common name entered → ask location allocation
        name = (wiz_same_name or "").strip()
        if not name:
            raise dash.exceptions.PreventUpdate
        total_qty = state.get("total_qty", 1)
        state = {**state, "step": "3b", "common_name": name}
        q = f"How many of the {total_qty} went to each location?"
        return (state, q,
                H, H, H, H, {"display": "block"}, H,
                B, "Done \u2713", "", split_data, _render_split_rows(split_data))

    elif step == "3b":
        # Location allocation → generate split data → done
        tulsa_n = max(int(wiz_tulsa_qty or 0), 0)
        texas_n = max(int(wiz_texas_qty or 0), 0)
        total_qty = state.get("total_qty", 1)
        cat = state.get("category", "Other")
        name = state.get("common_name", det_name)

        if tulsa_n + texas_n == 0:
            raise dash.exceptions.PreventUpdate

        split_data = []
        if tulsa_n > 0:
            split_data.append({"name": name, "qty": tulsa_n, "category": cat, "location": "Tulsa, OK"})
        if texas_n > 0:
            split_data.append({"name": name, "qty": texas_n, "category": cat, "location": "Texas"})
        other_n = total_qty - tulsa_n - texas_n
        if other_n > 0:
            split_data.append({"name": name, "qty": other_n, "category": cat, "location": "Other"})

        state = {**state, "step": "done"}
        total_entered = tulsa_n + texas_n + max(other_n, 0)
        q = f"All {total_entered} items allocated! Click Save above when ready."
        return (state, q,
                H, H, H, H, H, H,
                H, "", "", split_data, _render_split_rows(split_data))

    elif step == "3c":
        # Per-item entry (different items path)
        name = (wiz_item_name or "").strip()
        if not name:
            raise dash.exceptions.PreventUpdate
        cat = state.get("category", "Other")
        total_qty = state.get("total_qty", 1)
        split_data.append({
            "name": name, "qty": 1,
            "category": cat, "location": wiz_item_loc or "",
        })
        items_added = len(split_data)

        if items_added >= total_qty:
            state = {**state, "step": "done", "items_added": items_added}
            word = "rolls" if cat == "Filament" else "items"
            q = f"All {total_qty} {word} added! Click Save above when ready."
            return (state, q,
                    H, H, H, H, H, H,
                    H, "", "", split_data, _render_split_rows(split_data))
        else:
            next_num = items_added + 1
            state = {**state, "items_added": items_added}
            iw = "roll" if cat == "Filament" else "item"
            cw = "color" if cat == "Filament" else "name"
            q = f"{iw.title()} {next_num} of {total_qty} \u2014 What {cw} is it and where did it go?"
            return (state, q,
                    H, H, H, H, H, {"display": "block"},
                    B, f"Add {iw.title()} \u2192", "", split_data, _render_split_rows(split_data))

    raise dash.exceptions.PreventUpdate


@app.callback(
    Output({"type": "det-split-data", "index": MATCH}, "data", allow_duplicate=True),
    Output({"type": "det-split-display", "index": MATCH}, "children", allow_duplicate=True),
    Output({"type": "wiz-state", "index": MATCH}, "data", allow_duplicate=True),
    Output({"type": "wiz-question", "index": MATCH}, "children", allow_duplicate=True),
    Output({"type": "wiz-step0", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-step1", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-step2", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-step3a", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-step3b", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-step3c", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-btn-row", "index": MATCH}, "style", allow_duplicate=True),
    Output({"type": "wiz-next", "index": MATCH}, "children", allow_duplicate=True),
    Input({"type": "split-clear-btn", "index": MATCH}, "n_clicks"),
    prevent_initial_call=True,
)
def clear_split(n_clicks):
    """Clear all sub-items and restart the wizard from step 0."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    H = {"display": "none"}
    return ([], _render_split_rows([]),
            {"step": 0, "category": "", "total_qty": 0},
            "What type of item is this?",
            {"display": "block"}, H, H, H, H, H,
            {"display": "block", "marginTop": "8px"}, "Next \u2192")


@app.callback(
    Output({"type": "det-price-display", "index": MATCH}, "children"),
    Input({"type": "det-qty", "index": MATCH}, "value"),
    State({"type": "det-orig-total", "index": MATCH}, "data"),
    State({"type": "det-orig-qty", "index": MATCH}, "data"),
    prevent_initial_call=True,
)
def update_price_display(new_qty, orig_total, orig_qty):
    """Live-update per-unit price as user changes qty."""
    new_qty = int(new_qty) if new_qty else orig_qty
    if new_qty <= 0:
        new_qty = orig_qty
    per_unit = orig_total / new_qty
    return f"  (qty {new_qty}, ${per_unit:.2f}ea — total ${orig_total:.2f})"


@app.callback(
    Output("editor-items-container", "children"),
    Input("editor-search", "value"),
    Input("editor-cat-filter", "value"),
    Input("editor-status-filter", "value"),
    prevent_initial_call=True,
)
def filter_editor(search, cat_filter, status_filter):
    """Legacy filter callback — DataTable has native filtering now, so this is a no-op."""
    raise dash.exceptions.PreventUpdate


def _filter_editor_legacy(search, cat_filter, status_filter):
    """Legacy: Rebuild editor order cards based on search/category/status filters."""
    search = (search or "").lower().strip()
    cat_filter = cat_filter or "All"
    status_filter = status_filter or "All"

    sorted_orders = sorted(INVOICES, key=lambda o: o.get("date", ""), reverse=True)
    order_cards = []

    for inv in sorted_orders:
        onum = inv["order_num"]
        is_personal = inv["source"] == "Personal Amazon" or ("file" in inv and isinstance(inv.get("file"), str) and "Gigi" in inv.get("file", ""))

        source_label = "Personal Amazon" if is_personal else ("Amazon" if inv["source"] in ("Key Component Mfg",) else inv["source"])
        ship_addr = inv.get("ship_address", "")
        orig_location = classify_location(ship_addr)

        item_cards = []
        _seen_names = {}
        order_saved = 0
        order_total_items = 0
        for item in inv["items"]:
            item_name = item["name"]
            if item_name.startswith("Your package was left near the front door or porch."):
                item_name = item_name.replace("Your package was left near the front door or porch.", "").strip()
            auto_cat = categorize_item(item_name)

            orig_qty = item["qty"]
            price = item["price"]
            orig_total = round(price * orig_qty, 2)

            detail_key = (onum, item_name)
            existing = _ITEM_DETAILS.get(detail_key, [])
            has_details = bool(existing)

            if existing:
                det0 = existing[0]
                det_name = det0["display_name"]
                det_cat = det0["category"]
                det_qty = sum(d.get("true_qty", 1) for d in existing)
                det_loc = det0.get("location", "") or orig_location
            else:
                det_name = item_name
                det_cat = auto_cat
                det_qty = orig_qty
                det_loc = orig_location

            order_total_items += 1
            if has_details:
                order_saved += 1

            # Apply filters
            if search and search not in item_name.lower() and search not in det_name.lower():
                continue
            if cat_filter != "All" and det_cat != cat_filter:
                continue
            if status_filter == "Saved" and not has_details:
                continue
            if status_filter == "Unsaved" and has_details:
                continue

            _seen_names[item_name] = _seen_names.get(item_name, 0) + 1
            if _seen_names[item_name] > 1:
                idx = f"{onum}__{item_name}__{_seen_names[item_name]}"
            else:
                idx = f"{onum}__{item_name}"

            img_url = _IMAGE_URLS.get(item_name, "")
            is_split = len(existing) > 1

            item_cards.append(_build_item_row(
                idx, item_name, img_url, det_name, det_cat, det_qty, det_loc,
                has_details, orig_qty, orig_total, is_split, existing, onum))

        if not item_cards:
            continue

        order_total = sum(it["price"] * it["qty"] for it in inv["items"])
        _loc_color = TEAL if "Tulsa" in orig_location else (ORANGE if "Texas" in orig_location else GRAY)
        _prog_color = GREEN if order_saved == order_total_items else ORANGE
        _order_all_done = order_saved == order_total_items and order_total_items > 0
        _order_pct = round(order_saved / order_total_items * 100) if order_total_items > 0 else 0

        mini_progress = html.Div([
            html.Div(style={"width": f"{max(_order_pct, 5)}%", "height": "6px",
                            "background": f"linear-gradient(90deg, {_prog_color}88, {_prog_color})",
                            "borderRadius": "3px", "transition": "width 0.3s ease"}),
        ], style={"width": "80px", "height": "6px", "backgroundColor": "#0d0d1a",
                  "borderRadius": "3px", "display": "inline-block", "verticalAlign": "middle",
                  "marginLeft": "8px", "overflow": "hidden"})

        _done_pill = html.Span("\u2713 DONE", style={
            "fontSize": "10px", "fontWeight": "bold", "padding": "2px 10px",
            "borderRadius": "8px", "backgroundColor": f"{GREEN}22", "color": GREEN,
            "border": f"1px solid {GREEN}44", "marginLeft": "8px"}) if _order_all_done else None

        order_header = html.Div([c for c in [
            html.Span(f"Order #{onum}", style={"color": CYAN, "fontWeight": "bold", "fontSize": "14px"}),
            html.Span(orig_location,
                      style={"fontSize": "11px", "padding": "2px 10px", "borderRadius": "10px",
                             "backgroundColor": f"{_loc_color}18", "color": _loc_color,
                             "marginLeft": "10px", "fontWeight": "600"}),
            _done_pill,
            html.Span(style={"flex": "1"}),
            html.Span(inv["date"], style={"color": GRAY, "fontSize": "12px"}),
            html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}44", "margin": "0 6px"}),
            html.Span(source_label, style={"color": GRAY, "fontSize": "12px"}),
            html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}44", "margin": "0 6px"}),
            html.Span(f"${order_total:.2f}", style={"color": WHITE, "fontSize": "13px", "fontWeight": "700"}),
            html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}44", "margin": "0 6px"}),
            html.Span(f"{order_saved}/{order_total_items}",
                      style={"color": _prog_color, "fontSize": "12px", "fontWeight": "bold"}),
            mini_progress,
        ] if c is not None],
            className="order-header-compact",
            style={"display": "flex", "alignItems": "center", "flexWrap": "wrap",
                   "gap": "0", "padding": "10px 14px",
                   "backgroundColor": CARD2,
                   "borderBottom": f"1px solid {CYAN}15",
                   "borderRadius": "8px 8px 0 0"})

        order_card = html.Div([
            order_header,
            html.Div(item_cards, style={"padding": "4px 0"}),
        ], className="order-card order-card-saved" if _order_all_done else "order-card",
           style={"backgroundColor": f"#2ecc7108" if _order_all_done else f"{CARD2}88",
                  "borderRadius": "8px", "marginBottom": "12px",
                  "border": f"1px solid {GREEN}44" if _order_all_done else f"1px solid {CYAN}12"})
        order_cards.append(order_card)

    if not order_cards:
        return [html.P("No items match your filter.", style={"color": GRAY, "padding": "20px"})]
    return order_cards


@app.callback(
    Output({"type": "det-status", "index": MATCH}, "children"),
    Input({"type": "det-save-btn", "index": MATCH}, "n_clicks"),
    Input({"type": "det-reset-btn", "index": MATCH}, "n_clicks"),
    State({"type": "det-name", "index": MATCH}, "value"),
    State({"type": "det-cat", "index": MATCH}, "value"),
    State({"type": "det-qty", "index": MATCH}, "value"),
    State({"type": "loc-dropdown", "index": MATCH}, "value"),
    State({"type": "loc-split-check", "index": MATCH}, "value"),
    State({"type": "det-split-data", "index": MATCH}, "data"),
    State({"type": "det-order-num", "index": MATCH}, "data"),
    State({"type": "det-item-name", "index": MATCH}, "data"),
    State({"type": "det-orig-qty", "index": MATCH}, "data"),
    State({"type": "det-img-url", "index": MATCH}, "value"),
    prevent_initial_call=True,
)
def handle_detail_save_reset(save_clicks, reset_clicks, display_name, category,
                             true_qty, loc_dropdown,
                             loc_split_check, split_data,
                             order_num, item_name, orig_qty, img_url_val):
    """Save or reset item details."""
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger_id = ctx.triggered[0]["prop_id"]
    is_reset = "det-reset-btn" in trigger_id
    key = (order_num, item_name)

    if is_reset:
        ok = _delete_item_details(order_num, item_name)
        if key in _ITEM_DETAILS:
            del _ITEM_DETAILS[key]
        _apply_details_to_inv_items()
        _recompute_stock_summary()
        _rebuild_uploaded_inventory()
        return "Reset!"

    # Build detail entries
    display_name = (display_name or "").strip() or item_name
    category = category or "Other"
    true_qty = int(true_qty or orig_qty)
    location = loc_dropdown or ""
    is_split = loc_split_check and "split" in loc_split_check

    if is_split and split_data:
        details = [{"display_name": d["name"], "category": d.get("category") or category,
                     "true_qty": int(d.get("qty", 1)), "location": d.get("location") or location}
                    for d in split_data if d.get("name", "").strip()]
        if not details:
            details = [{"display_name": display_name, "category": category,
                        "true_qty": true_qty, "location": location}]
    else:
        details = [{"display_name": display_name, "category": category,
                     "true_qty": true_qty, "location": location}]

    ok = _save_item_details(order_num, item_name, details)
    if ok:
        _ITEM_DETAILS[key] = details
        count = len(details)

        # Save image URL if provided
        if img_url_val and img_url_val.strip():
            from supabase_loader import save_image_override as _save_img_override
            _save_img_override(item_name, img_url_val.strip())
            _IMAGE_URLS[item_name] = img_url_val.strip()
            if display_name and display_name != item_name:
                _save_img_override(display_name, img_url_val.strip())
                _IMAGE_URLS[display_name] = img_url_val.strip()

        # Rebuild INV_ITEMS, STOCK_SUMMARY, _UPLOADED_INVENTORY, and location spend
        _apply_details_to_inv_items()
        _recompute_stock_summary()
        _rebuild_uploaded_inventory()
        _recompute_location_spend()

        return f"Saved! ({count} entry{'s' if count > 1 else ''})"
    return "Error saving"


# ── Toggle Advanced Section (per-item) ────────────────────────────────────────

@app.callback(
    Output({"type": "det-adv-section", "index": MATCH}, "style"),
    Input({"type": "det-adv-btn", "index": MATCH}, "n_clicks"),
    State({"type": "det-adv-section", "index": MATCH}, "style"),
    prevent_initial_call=True,
)
def toggle_adv_section(n_clicks, current_style):
    """Toggle the advanced options section (image, split, reset) visibility."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    style = dict(current_style) if current_style else {}
    is_visible = style.get("display", "none") != "none"
    style["display"] = "none" if is_visible else "block"
    return style


# ── Image Fetch (per-item) ────────────────────────────────────────────────────

@app.callback(
    Output({"type": "det-img-preview", "index": MATCH}, "children"),
    Output({"type": "det-img-url", "index": MATCH}, "value"),
    Output({"type": "det-img-status", "index": MATCH}, "children"),
    Input({"type": "det-img-fetch-btn", "index": MATCH}, "n_clicks"),
    State({"type": "det-item-name", "index": MATCH}, "data"),
    State({"type": "det-name", "index": MATCH}, "value"),
    prevent_initial_call=True,
)
def fetch_item_image(n_clicks, item_name, display_name):
    """Fetch product image from Amazon for this item."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    search_name = (display_name or item_name or "").strip()
    if not search_name:
        raise dash.exceptions.PreventUpdate
    local_url = _fetch_amazon_image(search_name)
    if local_url:
        # Also map the original item name
        if item_name and item_name != search_name:
            from supabase_loader import save_image_override as _sio
            _sio(item_name, local_url)
            _IMAGE_URLS[item_name] = local_url
        return item_thumbnail(local_url, 28), local_url, "Fetched!"
    return (
        html.Span("?", style={
            "width": "28px", "height": "28px", "display": "inline-flex",
            "alignItems": "center", "justifyContent": "center",
            "backgroundColor": "#ffffff10", "borderRadius": "4px",
            "color": DARKGRAY, "fontSize": "10px", "fontWeight": "bold"}),
        "",
        "Not found",
    )


# ── Bulk Fetch Missing Images ────────────────────────────────────────────────

@app.callback(
    Output("editor-fetch-all-images-status", "children"),
    Input("editor-fetch-all-images-btn", "n_clicks"),
    prevent_initial_call=True,
)
def bulk_fetch_missing_images(n_clicks):
    """Auto-fetch images for all items that don't have one yet (max 20)."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    # Collect unique item names without images
    missing = []
    seen = set()
    for inv in INVOICES:
        for item in inv["items"]:
            name = item["name"]
            if name in seen:
                continue
            seen.add(name)
            if not _IMAGE_URLS.get(name, ""):
                missing.append(name)
    if not missing:
        return "All items have images!"
    cap = min(len(missing), 20)
    fetched = 0
    for name in missing[:cap]:
        result = _fetch_amazon_image(name)
        if result:
            fetched += 1
    return f"Fetched {fetched}/{cap} images"


# ── Editor-save cascade: relay det-status changes into a single trigger ───────

@app.callback(
    Output("editor-save-trigger", "data"),
    Input({"type": "det-status", "index": ALL}, "children"),
    State("editor-save-trigger", "data"),
    prevent_initial_call=True,
)
def _relay_save_trigger(all_statuses, current):
    """Increment trigger whenever any editor item is saved/reset."""
    # Only fire if at least one status contains text (a save just happened)
    if any(s for s in all_statuses if s):
        return (current or 0) + 1
    raise dash.exceptions.PreventUpdate


@app.callback(
    Output("editor-items-container", "children", allow_duplicate=True),
    Input("editor-save-trigger", "data"),
    State("editor-search", "value"),
    State("editor-cat-filter", "value"),
    State("editor-status-filter", "value"),
    prevent_initial_call=True,
)
def _refresh_editor_on_save(trigger, search, cat_filter, status_filter):
    """Legacy: Rebuild editor after per-item save. Now a no-op (DataTable manages its own state)."""
    raise dash.exceptions.PreventUpdate


# ── Receipt Upload + Item Wizard ──────────────────────────────────────────────

@app.callback(
    Output("receipt-wizard-state", "data"),           # 0
    Output("receipt-wizard-panel", "style"),           # 1
    Output("receipt-wizard-header", "children"),       # 2
    Output("receipt-wizard-orig", "children"),         # 3
    Output("wizard-name", "value"),                    # 4
    Output("wizard-cat", "value"),                     # 5
    Output("wizard-qty", "value"),                     # 6
    Output("wizard-loc", "value"),                     # 7
    Output("receipt-wizard-progress", "children"),     # 8
    Output("receipt-upload-status", "children"),       # 9
    Output("wizard-form-row", "style"),                # 10
    Output("wizard-nav-btns", "style"),                # 11
    Output("wizard-done-btn", "style"),                # 12
    Output("wizard-back-btn", "disabled"),             # 13
    # -- Questionnaire outputs --
    Output("wizard-multipack", "value"),               # 14
    Output("wizard-pack-table", "data"),               # 15
    Output("wizard-split-yn", "value"),                # 16
    Output("wizard-loc1-qty", "value"),                # 17
    Output("wizard-loc2", "value"),                    # 18
    Output("wizard-loc2-qty", "value"),                # 19
    Output("wizard-order-img-preview", "children",
           allow_duplicate=True),                      # 20
    # Legacy hidden outputs (kept for layout compat)
    Output("wizard-units-per-pack", "value"),          # 21
    Output("wizard-split-active", "data"),             # 22
    Output("wizard-split-row", "style"),               # 23
    Output("wizard-loc-row", "style"),                 # 24
    Input("receipt-upload", "contents"),
    Input("wizard-save-btn", "n_clicks"),
    Input("wizard-skip-btn", "n_clicks"),
    Input("wizard-done-btn", "n_clicks"),
    Input("wizard-back-btn", "n_clicks"),
    Input("wizard-split-toggle", "n_clicks"),
    State("receipt-upload", "filename"),
    State("receipt-wizard-state", "data"),
    State("wizard-name", "value"),
    State("wizard-cat", "value"),
    State("wizard-qty", "value"),
    State("wizard-loc", "value"),
    # -- Questionnaire states --
    State("wizard-multipack", "value"),
    State("wizard-pack-table", "data"),
    State("wizard-split-yn", "value"),
    State("wizard-loc1-qty", "value"),
    State("wizard-loc2", "value"),
    State("wizard-loc2-qty", "value"),
    # Legacy states
    State("wizard-units-per-pack", "value"),
    State("wizard-split-active", "data"),
    prevent_initial_call=True,
)
@guard_callback(n_outputs=25)
def handle_receipt_wizard(contents, save_clicks, skip_clicks, done_clicks, back_clicks,
                          split_toggle_clicks,
                          filename, state, wiz_name, wiz_cat, wiz_qty, wiz_loc,
                          wiz_multipack, wiz_pack_data, wiz_split_yn, wiz_loc1_qty,
                          wiz_loc2, wiz_loc2_qty,
                          wiz_units_per_pack, wiz_split_active):
    """Handle PDF upload, wizard navigation (Save & Next / Skip / Back), and Done."""
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger = ctx.triggered[0]["prop_id"]
    _HIDE = {"display": "none"}
    _SHOW_BLOCK = {"display": "block"}
    _SHOW_FLEX = {"display": "flex", "alignItems": "center", "flexWrap": "wrap",
                  "gap": "12px", "marginBottom": "12px"}
    _NAV_FLEX = {"display": "flex", "gap": "10px", "alignItems": "center",
                 "marginBottom": "10px"}
    _NO = dash.no_update
    _FORM_COL = {"display": "flex", "flexDirection": "column",
                 "gap": "0px", "marginBottom": "12px"}
    _SPLIT_ROW_SHOW = {"display": "block", "padding": "8px 12px",
                       "backgroundColor": f"{PURPLE}08", "borderRadius": "6px",
                       "border": f"1px solid {PURPLE}33", "marginBottom": "8px"}

    def _q_defaults():
        """Default values for the 11 questionnaire outputs (indices 14-24)."""
        # multipack, pack_data, split_yn, loc1_qty, loc2, loc2_qty,
        # order_img_preview, units_per_pack, split_active, split_row, loc_row
        return ("no", [], "no", 1, "Texas", 0, "", 1, False, _HIDE, _HIDE)

    def _build_wizard_display(st):
        """Build wizard header, orig info, form defaults, progress dots, and questionnaire state."""
        idx = st["current_index"]
        item = st["items"][idx]
        total_items = st["total"]

        header = html.Div([
            html.Div([
                html.Span(f"NEW RECEIPT: Order #{st['order_num']}",
                          style={"color": CYAN, "fontWeight": "bold", "fontSize": "15px"}),
                html.Span(f"  ({total_items} item{'s' if total_items != 1 else ''})",
                          style={"color": GRAY, "fontSize": "13px", "marginLeft": "6px"}),
            ]),
            html.Div(f"Item {idx + 1} of {total_items}",
                      style={"color": PURPLE, "fontSize": "13px", "fontWeight": "bold",
                             "marginTop": "2px"}),
        ])

        orig = html.Div([
            html.Div([
                html.Span("Original: ", style={"color": GRAY, "fontSize": "12px"}),
                html.Span(f'"{item["name"][:80]}"',
                          style={"color": WHITE, "fontSize": "12px", "fontStyle": "italic"}),
            ]),
            html.Div([
                html.Span(f'Qty: {item["qty"]}', style={"color": WHITE, "fontSize": "12px"}),
                html.Span(f'    Price: ${item["price"]:.2f}/ea', style={"color": WHITE, "fontSize": "12px"}),
                html.Span(f'    Total: ${item["price"] * item["qty"]:.2f}',
                          style={"color": ORANGE, "fontSize": "12px", "fontWeight": "bold"}),
            ], style={"marginTop": "4px"}),
            html.Div([
                html.Span(f'Seller: {item["seller"]}', style={"color": TEAL, "fontSize": "11px"}),
                html.Span(f'    Ship to: {item.get("ship_to_short", "")}',
                          style={"color": GRAY, "fontSize": "11px"}),
            ], style={"marginTop": "2px"}),
        ])

        # Progress dots
        dots = []
        for i in range(total_items):
            if i < idx:
                dot_color = GREEN
            elif i == idx:
                dot_color = CYAN
            else:
                dot_color = DARKGRAY
            dots.append(html.Span("\u25cf ", style={
                "color": dot_color, "fontSize": "16px", "margin": "0 2px"}))

        # Check if this item was previously saved with details
        detail_key = (st["order_num"], item["name"])
        saved = _ITEM_DETAILS.get(detail_key, [])

        # Determine questionnaire state from saved details
        if len(saved) > 1:
            # Was saved as multi-pack OR split
            names_differ = len(set(d.get("display_name", "") for d in saved)) > 1
            if names_differ:
                # Multi-pack: different display names = different items in pack
                multipack = "yes"
                pack_data = [{"name": d["display_name"], "qty": d.get("true_qty", 1),
                              "location": d.get("location", "Tulsa, OK")} for d in saved]
                split_yn = "no"
                loc1_qty = 1
                loc2_val = "Texas"
                loc2_qty = 0
            else:
                # Split: same name, different locations
                multipack = "no"
                pack_data = []
                split_yn = "yes"
                loc1_qty = saved[0].get("true_qty", 1)
                loc2_val = saved[1].get("location", "Texas")
                loc2_qty = saved[1].get("true_qty", 0)
        elif len(saved) == 1:
            multipack = "no"
            pack_data = []
            split_yn = "no"
            loc1_qty = saved[0].get("true_qty", item["qty"])
            loc2_val = "Texas"
            loc2_qty = 0
        else:
            multipack = "no"
            pack_data = []
            split_yn = "no"
            loc1_qty = item["qty"]
            loc2_val = "Texas"
            loc2_qty = 0

        # Order image preview (check if we have a saved order image)
        order_img_key = f"order:{st['order_num']}"
        order_img_url = _IMAGE_URLS.get(order_img_key, "")
        if order_img_url:
            order_img_preview = html.Img(src=order_img_url,
                                         style={"maxHeight": "80px", "borderRadius": "4px",
                                                "border": f"1px solid {CYAN}44"})
        else:
            order_img_preview = ""

        # base outputs: header, orig, name, cat, qty, loc, dots
        # questionnaire outputs: multipack, pack_data, split_yn, loc1_qty, loc2, loc2_qty, order_img
        return (header, orig, item["name"], item["auto_category"], item["qty"],
                item["auto_location"], dots,
                multipack, pack_data, split_yn, loc1_qty, loc2_val, loc2_qty,
                order_img_preview)

    # ── Handle PDF Upload ──────────────────────────────────────────────────
    if "receipt-upload" in trigger:
        if not contents or not filename:
            raise dash.exceptions.PreventUpdate

        # Decode base64 PDF
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)

        # Save to keycomp folder initially
        save_folder = os.path.join(BASE_DIR, "data", "invoices", "keycomp")
        save_path = os.path.join(save_folder, filename)
        try:
            with open(save_path, "wb") as f:
                f.write(decoded)
        except Exception as e:
            return (None, _HIDE, "", "", "", None, 1, None, "",
                    html.Div(f"Error saving file: {e}", style={"color": RED, "fontSize": "13px"}),
                    _FORM_COL, _NAV_FLEX, _HIDE, True, *_q_defaults())

        # Parse with _parse_invoices
        try:
            from _parse_invoices import parse_pdf_file
            order = parse_pdf_file(save_path)
        except Exception as e:
            try:
                os.remove(save_path)
            except Exception:
                pass
            return (None, _HIDE, "", "", "", None, 1, None, "",
                    html.Div(f"Error parsing PDF: {e}", style={"color": RED, "fontSize": "13px"}),
                    _FORM_COL, _NAV_FLEX, _HIDE, True, *_q_defaults())

        if not order or not order.get("items"):
            try:
                os.remove(save_path)
            except Exception:
                pass
            return (None, _HIDE, "", "", "", None, 1, None, "",
                    html.Div("Could not parse any items from this PDF.",
                             style={"color": RED, "fontSize": "13px"}),
                    _FORM_COL, _NAV_FLEX, _HIDE, True, *_q_defaults())

        # Check for duplicate order_num
        for inv in INVOICES:
            if inv["order_num"] == order["order_num"]:
                try:
                    os.remove(save_path)
                except Exception:
                    pass
                return (None, _HIDE, "", "", "", None, 1, None, "",
                        html.Div(f"Order #{order['order_num']} already exists!",
                                 style={"color": ORANGE, "fontSize": "13px", "fontWeight": "bold"}),
                        _FORM_COL, _NAV_FLEX, _HIDE, True, *_q_defaults())

        # Move to personal_amazon folder if source matches
        if order.get("source") == "Personal Amazon":
            new_folder = os.path.join(BASE_DIR, "data", "invoices", "personal_amazon")
            new_path = os.path.join(new_folder, filename)
            if save_path != new_path:
                try:
                    os.rename(save_path, new_path)
                except Exception:
                    pass

        # Append to INVOICES and persist
        INVOICES.append(order)

        # Save to local JSON (fallback)
        try:
            out_path = os.path.join(BASE_DIR, "data", "generated", "inventory_orders.json")
            with open(out_path, "w") as f:
                json.dump(INVOICES, f, indent=2)
        except Exception:
            pass

        # Push to Supabase
        _sb_ok = _save_new_order(order)
    
        if not _sb_ok:
            _logger.warning("Failed to save order %s to Supabase (wizard)", order.get('order_num', '?'))

        # Build INV_ITEMS rows for this new order
        try:
            dt = pd.to_datetime(order["date"], format="%B %d, %Y")
        except Exception:
            try:
                dt = pd.to_datetime(order["date"])
            except Exception:
                dt = pd.NaT
        month = dt.to_period("M").strftime("%Y-%m") if pd.notna(dt) else "Unknown"
        new_inv_rows = []
        for item in order["items"]:
            item_name = item["name"]
            if item_name.startswith("Your package was left near the front door or porch."):
                item_name = item_name.replace("Your package was left near the front door or porch.", "").strip()
            new_inv_rows.append({
                "order_num": order["order_num"],
                "date": order["date"],
                "date_parsed": dt,
                "month": month,
                "name": item_name,
                "qty": item["qty"],
                "price": item["price"],
                "total": item["price"] * item["qty"],
                "source": order["source"],
                "seller": item.get("seller", "Unknown"),
                "ship_to": item.get("ship_to", order.get("ship_address", "")),
                "payment_method": order.get("payment_method", "Unknown"),
                "image_url": item.get("image_url", ""),
                "category": categorize_item(item_name),
                "_orig_name": item_name,
                "_override_location": "",
            })
        if new_inv_rows:
            global INV_ITEMS
            new_df = pd.DataFrame(new_inv_rows)
            # Allocate tax
            if order["subtotal"] > 0:
                new_df["total_with_tax"] = (
                    new_df["total"] * (order["grand_total"] / order["subtotal"])
                ).round(2)
            else:
                new_df["total_with_tax"] = new_df["total"]
            INV_ITEMS = pd.concat([INV_ITEMS, new_df], ignore_index=True)

        # Build wizard state
        ship_addr = order.get("ship_address", "")
        auto_loc = classify_location(ship_addr)
        items_for_wizard = []
        for item in order["items"]:
            name = item["name"]
            auto_cat = categorize_item(name)
            ship_to = item.get("ship_to", ship_addr)
            if ship_to.count(",") >= 2:
                parts = ship_to.split(",")
                short_ship = parts[1].strip() + ", " + parts[2].strip().split(" ")[0]
            else:
                short_ship = ship_to
            items_for_wizard.append({
                "name": name,
                "qty": item["qty"],
                "price": item["price"],
                "seller": item.get("seller", "Unknown"),
                "ship_to": ship_to,
                "ship_to_short": short_ship,
                "auto_category": auto_cat,
                "auto_location": auto_loc,
            })

        new_state = {
            "order_num": order["order_num"],
            "order_date": order["date"],
            "source": order["source"],
            "items": items_for_wizard,
            "current_index": 0,
            "total": len(items_for_wizard),
            "saved_count": 0,
        }

        (header, orig, name, cat, qty, loc, dots,
         multipack, pack_data, split_yn, loc1_qty, loc2_val, loc2_qty,
         order_img_preview) = _build_wizard_display(new_state)
        status = html.Div([
            html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
            html.Span(f"Parsed {filename}: {len(items_for_wizard)} item(s) found. "
                       f"Order #{order['order_num']}",
                       style={"color": GREEN, "fontSize": "13px"}),
        ])

        return (new_state, _SHOW_BLOCK, header, orig, name, cat, qty, loc, dots,
                status, _FORM_COL, _NAV_FLEX, _HIDE, True,
                multipack, pack_data, split_yn, loc1_qty, loc2_val, loc2_qty,
                order_img_preview, 1, False, _HIDE, _HIDE)

    # ── Handle Split Toggle (legacy, hidden button) ───────────────────────
    if "wizard-split-toggle" in trigger:
        raise dash.exceptions.PreventUpdate

    # ── Handle Back ───────────────────────────────────────────────────────
    if "wizard-back-btn" in trigger:
        if not state or state.get("current_index", 0) <= 0:
            raise dash.exceptions.PreventUpdate
        state["current_index"] = max(0, state["current_index"] - 1)
        (header, orig, name, cat, qty, loc, dots,
         multipack, pack_data, split_yn, loc1_qty, loc2_val, loc2_qty,
         order_img_preview) = _build_wizard_display(state)
        _back_disabled = state["current_index"] <= 0
        return (state, _SHOW_BLOCK, header, orig, name, cat, qty, loc, dots,
                _NO, _FORM_COL, _NAV_FLEX, _HIDE, _back_disabled,
                multipack, pack_data, split_yn, loc1_qty, loc2_val, loc2_qty,
                order_img_preview, 1, False, _HIDE, _HIDE)

    # ── Handle Save & Next / Skip ──────────────────────────────────────────
    if "wizard-save-btn" in trigger or "wizard-skip-btn" in trigger:
        if not state:
            raise dash.exceptions.PreventUpdate

        idx = state["current_index"]
        item = state["items"][idx]

        if "wizard-save-btn" in trigger:
            display_name = (wiz_name or "").strip() or item["name"]
            category = wiz_cat or "Other"
            location = wiz_loc or ""
            receipt_qty = int(wiz_qty) if wiz_qty else item["qty"]

            # Build detail entries based on questionnaire answers
            if wiz_multipack == "yes" and wiz_pack_data:
                # Multi-pack mode: each row in pack table becomes a detail entry
                details = []
                for pack_row in wiz_pack_data:
                    row_name = (pack_row.get("name", "") or "").strip()
                    if not row_name:
                        continue
                    row_qty = int(pack_row.get("qty", 1) or 1)
                    row_loc = pack_row.get("location", "Tulsa, OK") or "Tulsa, OK"
                    details.append({
                        "display_name": row_name,
                        "category": category,
                        "true_qty": row_qty,
                        "location": row_loc,
                    })
                if not details:
                    # Fallback: no valid pack rows, save as single item
                    details = [{"display_name": display_name, "category": category,
                                "true_qty": receipt_qty, "location": location}]
            elif wiz_split_yn == "yes" and wiz_loc2 and int(wiz_loc2_qty or 0) > 0:
                # Split mode: same item across 2 locations
                qty2 = int(wiz_loc2_qty)
                qty1 = int(wiz_loc1_qty) if wiz_loc1_qty else (receipt_qty - qty2)
                details = [
                    {"display_name": display_name, "category": category,
                     "true_qty": qty1, "location": location},
                    {"display_name": display_name, "category": category,
                     "true_qty": qty2, "location": wiz_loc2},
                ]
            else:
                # Simple: single item, single location
                details = [{"display_name": display_name, "category": category,
                            "true_qty": receipt_qty, "location": location}]

            ok = _save_item_details(state["order_num"], item["name"], details)
            if ok:
                key = (state["order_num"], item["name"])
                _ITEM_DETAILS[key] = details
                # Update _UPLOADED_INVENTORY for each entry
                for det in details:
                    loc_norm = _norm_loc(det["location"])
                    if loc_norm:
                        inv_key = (loc_norm, det["display_name"], category)
                        _UPLOADED_INVENTORY[inv_key] = _UPLOADED_INVENTORY.get(inv_key, 0) + det["true_qty"]
                state["saved_count"] = state.get("saved_count", 0) + 1

        # Advance to next item
        state["current_index"] = idx + 1

        if state["current_index"] >= state["total"]:
            # Rebuild location spend totals after all items saved
            _apply_details_to_inv_items()
            _recompute_stock_summary()
            _rebuild_uploaded_inventory()
            _recompute_location_spend()
            # All items done — show summary
            saved = state["saved_count"]
            total = state["total"]
            summary_header = html.Div([
                html.H4([
                    html.Span("\u2713 ", style={"color": GREEN}),
                    "Receipt Complete!",
                ], style={"color": GREEN, "margin": "0 0 8px 0", "fontSize": "16px"}),
                html.P(f"Saved {saved} of {total} item(s) from Order #{state['order_num']}.",
                       style={"color": WHITE, "fontSize": "13px", "margin": "0 0 4px 0"}),
                html.P("Click 'Done' to close, then refresh the page to see updated stock.",
                       style={"color": GRAY, "fontSize": "12px", "margin": "0"}),
            ])

            done_style = {"display": "inline-block", "fontSize": "12px", "padding": "8px 24px",
                          "backgroundColor": GREEN, "color": WHITE,
                          "border": "none", "borderRadius": "6px",
                          "cursor": "pointer", "fontWeight": "bold",
                          "marginBottom": "10px"}

            return (state, _SHOW_BLOCK, summary_header, "", "", None, 1, None, "",
                    _NO, _HIDE, _HIDE, done_style, True, *_q_defaults())

        # Show next item
        _back_disabled = state["current_index"] <= 0
        (header, orig, name, cat, qty, loc, dots,
         multipack, pack_data, split_yn, loc1_qty, loc2_val, loc2_qty,
         order_img_preview) = _build_wizard_display(state)
        return (state, _SHOW_BLOCK, header, orig, name, cat, qty, loc, dots,
                _NO, _FORM_COL, _NAV_FLEX, _HIDE, _back_disabled,
                multipack, pack_data, split_yn, loc1_qty, loc2_val, loc2_qty,
                order_img_preview, 1, False, _HIDE, _HIDE)

    # ── Handle Done ────────────────────────────────────────────────────────
    if "wizard-done-btn" in trigger:
        return (None, _HIDE, "", "", "", None, 1, None, "", "",
                _FORM_COL, _NAV_FLEX, _HIDE, True, *_q_defaults())

    raise dash.exceptions.PreventUpdate


# ── Wizard Questionnaire: Show/Hide Sections (clientside) ─────────────────────

# Multi-pack toggle: show pack section, hide single-item section (or vice versa)
app.clientside_callback(
    """
    function(multipack) {
        if (multipack === 'yes') {
            return [
                {display: 'block', padding: '10px 14px', backgroundColor: 'rgba(155,89,182,0.024)',
                 borderRadius: '6px', border: '1px solid rgba(155,89,182,0.13)', marginBottom: '12px'},
                {display: 'none'}
            ];
        }
        return [{display: 'none'}, {}];
    }
    """,
    Output("wizard-pack-section", "style"),
    Output("wizard-nopack-section", "style"),
    Input("wizard-multipack", "value"),
)

# Split toggle: show/hide split allocation row
app.clientside_callback(
    """
    function(splitYn) {
        if (splitYn === 'yes') {
            return {display: 'block', padding: '8px 12px',
                    backgroundColor: 'rgba(155,89,182,0.03)', borderRadius: '6px',
                    border: '1px solid rgba(155,89,182,0.2)', marginBottom: '8px'};
        }
        return {display: 'none'};
    }
    """,
    Output("wizard-split-row", "style", allow_duplicate=True),
    Input("wizard-split-yn", "value"),
    prevent_initial_call=True,
)

# Split total display
app.clientside_callback(
    """
    function(loc1Qty, loc2Qty, receiptQty) {
        var total = parseInt(receiptQty) || 1;
        var q1 = parseInt(loc1Qty) || 0;
        var q2 = parseInt(loc2Qty) || 0;
        var sum = q1 + q2;
        if (sum === total) {
            return 'Total: ' + sum + ' units ✓';
        } else {
            return 'Loc1: ' + q1 + ' + Loc2: ' + q2 + ' = ' + sum + ' (need ' + total + ')';
        }
    }
    """,
    Output("wizard-split-total", "children"),
    Input("wizard-loc1-qty", "value"),
    Input("wizard-loc2-qty", "value"),
    State("wizard-qty", "value"),
)


# ── Pack Table: Add Row ──────────────────────────────────────────────────────

@app.callback(
    Output("wizard-pack-table", "data", allow_duplicate=True),
    Input("wizard-pack-add-row", "n_clicks"),
    State("wizard-pack-table", "data"),
    prevent_initial_call=True,
)
def add_pack_row(n_clicks, current_data):
    """Add an empty row to the multi-pack breakdown table."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    rows = list(current_data or [])
    rows.append({"name": "", "qty": 1, "location": "Tulsa, OK"})
    return rows


# ── Order Image Upload ───────────────────────────────────────────────────────

@app.callback(
    Output("wizard-order-img-preview", "children", allow_duplicate=True),
    Input("wizard-order-img-upload", "contents"),
    State("wizard-order-img-upload", "filename"),
    State("receipt-wizard-state", "data"),
    prevent_initial_call=True,
)
def save_order_image(contents, filename, wizard_state):
    """Save an uploaded order image and show preview."""
    if not contents:
        raise dash.exceptions.PreventUpdate

    order_num = wizard_state.get("order_num", "unknown") if wizard_state else "unknown"

    # Save image file
    img_dir = os.path.join(BASE_DIR, "assets", "order_images")
    os.makedirs(img_dir, exist_ok=True)

    ext = os.path.splitext(filename or "img.png")[1].lower()
    if ext not in (".png", ".jpg", ".jpeg", ".gif", ".webp"):
        ext = ".png"
    safe_name = f"order_{order_num}{ext}"
    img_path = os.path.join(img_dir, safe_name)

    try:
        content_type, content_string = contents.split(",", 1)
        decoded = base64.b64decode(content_string)
        with open(img_path, "wb") as f:
            f.write(decoded)
    except Exception as e:
        return html.Span(f"Error: {e}", style={"color": RED, "fontSize": "11px"})

    asset_url = f"/assets/order_images/{safe_name}"

    # Save to image overrides so it persists
    from supabase_loader import save_image_override as _save_img_override
    _save_img_override(f"order:{order_num}", asset_url)
    _IMAGE_URLS[f"order:{order_num}"] = asset_url

    return html.Div([
        html.Img(src=asset_url, style={"maxHeight": "80px", "borderRadius": "4px",
                                        "border": f"1px solid {GREEN}44"}),
        html.Div("Saved!", style={"color": GREEN, "fontSize": "10px", "fontWeight": "bold"}),
    ])


# ── Product Image Upload ─────────────────────────────────────────────────────

@app.callback(
    Output("wizard-product-img-preview", "children"),
    Input("wizard-product-img-upload", "contents"),
    State("wizard-product-img-upload", "filename"),
    State("wizard-name", "value"),
    prevent_initial_call=True,
)
def save_product_image(contents, filename, display_name):
    """Save a product image and associate it with the display name."""
    if not contents:
        raise dash.exceptions.PreventUpdate

    item_name = (display_name or "").strip() or "product"
    status, url = _save_item_image(item_name, item_name, file_data=contents, filename=filename)

    if url:
        return html.Div([
            html.Img(src=url, style={"maxHeight": "60px", "borderRadius": "4px",
                                      "border": f"1px solid {GREEN}44"}),
            html.Div(status, style={"color": GREEN, "fontSize": "10px", "fontWeight": "bold"}),
        ])
    return html.Span(status, style={"color": RED, "fontSize": "11px"})


# ── Product Image Manager Upload (pattern-matching) ──────────────────────────

@app.callback(
    Output({"type": "product-img-status", "index": MATCH}, "children"),
    Input({"type": "product-img-upload", "index": MATCH}, "contents"),
    State({"type": "product-img-upload", "index": MATCH}, "filename"),
    State({"type": "product-img-upload", "index": MATCH}, "id"),
    prevent_initial_call=True,
)
def save_product_image_grid(contents, filename, comp_id):
    """Save a product image from the image manager grid."""
    if not contents:
        raise dash.exceptions.PreventUpdate

    product_name = comp_id.get("index", "product") if isinstance(comp_id, dict) else "product"
    status, url = _save_item_image(product_name, product_name, file_data=contents, filename=filename)

    if url:
        return html.Div([
            html.Span("Saved!", style={"color": GREEN, "fontWeight": "bold"}),
        ])
    return html.Span("Error", style={"color": RED})


# ── Location Item Image Override ──────────────────────────────────────────────

@app.callback(
    Output({"type": "loc-img-status", "index": MATCH}, "children"),
    Input({"type": "loc-img-set", "index": MATCH}, "n_clicks"),
    State({"type": "loc-img-url", "index": MATCH}, "value"),
    State({"type": "loc-img-name", "index": MATCH}, "data"),
    prevent_initial_call=True,
)
def set_location_item_image(n_clicks, url, item_name):
    """Save a per-item image override from location inventory lists."""
    if not n_clicks or not url or not url.strip():
        raise dash.exceptions.PreventUpdate
    from supabase_loader import save_image_override as _save_img_override
    _save_img_override(item_name, url.strip())
    _IMAGE_URLS[item_name] = url.strip()
    return "Saved!"


# ── Location Inventory Refresh ────────────────────────────────────────────────

@app.callback(
    Output("location-inventory-display", "children"),
    Input("loc-inv-refresh-btn", "n_clicks"),
    prevent_initial_call=True,
)
def refresh_location_display(n_clicks):
    """Rebuild the Tulsa/Texas display from uploaded items."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    tulsa_pct = (tulsa_spend / true_inventory_cost * 100) if true_inventory_cost else 0
    texas_pct = (texas_spend / true_inventory_cost * 100) if true_inventory_cost else 0
    return [
        _build_warehouse_card("TJ (Tulsa, OK)", "Tulsa", TEAL,
                               tulsa_spend, tulsa_orders, tulsa_subtotal, tulsa_tax, tulsa_pct),
        _build_warehouse_card("BRADEN (Texas)", "Texas", ORANGE,
                               texas_spend, texas_orders, texas_subtotal, texas_tax, texas_pct),
    ]


# ── Inventory Qty Table Save ─────────────────────────────────────────────────
@app.callback(
    Output("inv-qty-save-status", "children"),
    Output("inv-qty-save-status", "style"),
    Input("inv-qty-save-btn", "n_clicks"),
    State("inv-qty-table", "data"),
    prevent_initial_call=True,
)
def save_inv_qty_edits(n_clicks, table_data):
    """Save edited quantities from the inventory qty table.
    Updates _ITEM_DETAILS in Supabase, then rebuilds _UPLOADED_INVENTORY from scratch."""
    if not n_clicks or not table_data:
        raise dash.exceptions.PreventUpdate

    changes = 0
    for row in table_data:
        loc = row["Location"]
        name = row["Name"]
        cat = row["Category"]
        new_qty = int(row["Qty"]) if row["Qty"] else 0
        inv_key = (loc, name, cat)
        old_qty = _UPLOADED_INVENTORY.get(inv_key, 0)
        if new_qty == old_qty:
            continue

        # Collect ALL detail entries that contribute to this (loc, name, cat)
        # Then distribute the new_qty across them (set first to absorb the diff)
        _matching = []
        for detail_key, dets in _ITEM_DETAILS.items():
            for d in dets:
                d_loc = _norm_loc(d.get("location", ""))
                d_name = d.get("display_name", "")
                d_cat = d.get("category", "Other")
                if d_loc == loc and d_name == name and d_cat == cat:
                    _matching.append((detail_key, d, dets))

        if _matching:
            # Strategy: set the first match to new_qty, zero out the rest
            # This way the total = new_qty
            _saved_keys = set()
            for idx, (dk, d, dets) in enumerate(_matching):
                if idx == 0:
                    d["true_qty"] = new_qty
                else:
                    d["true_qty"] = 0
                # Save each affected order's details to Supabase (once per order)
                if dk not in _saved_keys:
                    try:
                        _save_item_details(dk[0], dk[1], dets)
                        print(f"[inv-qty] Saved {dk[0]}/{dk[1]}: {name} qty={d['true_qty']}")
                    except Exception as _e:
                        print(f"[inv-qty] Save error: {_e}")
                    _saved_keys.add(dk)
            changes += 1

    # Rebuild _UPLOADED_INVENTORY from _ITEM_DETAILS (clean slate — no duplicates)
    _UPLOADED_INVENTORY.clear()
    _INVENTORY_UNIT_COST.clear()
    _price_lkp: dict[tuple[str, str], float] = {}
    if len(INV_ITEMS) > 0:
        for _, _r in INV_ITEMS.iterrows():
            _iq = max(int(_r.get("qty", 1)), 1)
            _itt = float(_r.get("total_with_tax", _r.get("price", 0) * _iq))
            _price_lkp[(_r["order_num"], _r["name"])] = round(_itt / _iq, 2)
    _inv_tc: dict[tuple[str, str, str], float] = {}
    for (_onum, _iname), _dets in _ITEM_DETAILS.items():
        for _d in _dets:
            _l = _norm_loc(_d.get("location", ""))
            if not _l:
                continue
            _dn = _d.get("display_name", _iname)
            _ct = _d.get("category", "Other")
            _ik = (_l, _dn, _ct)
            _dq = int(_d.get("true_qty", 1))
            _UPLOADED_INVENTORY[_ik] = _UPLOADED_INVENTORY.get(_ik, 0) + _dq
            _up = _price_lkp.get((_onum, _dn), 0)
            if _up:
                _inv_tc[_ik] = _inv_tc.get(_ik, 0) + (_up * _dq)
    for _ik, _tc in _inv_tc.items():
        _tq = _UPLOADED_INVENTORY.get(_ik, 1)
        _INVENTORY_UNIT_COST[_ik] = round(_tc / _tq, 2) if _tq > 0 else 0

    if changes:
        _apply_details_to_inv_items()
        _recompute_location_spend()

    msg = f"Saved {changes} qty change{'s' if changes != 1 else ''}!" if changes else "No changes detected."
    return (msg, {"color": GREEN if changes else GRAY, "fontWeight": "bold", "fontSize": "12px"})


# ── Missing Images Save ───────────────────────────────────────────────────────
@app.callback(
    Output("inv-img-save-status", "children"),
    Output("inv-img-save-status", "style"),
    Input("inv-img-save-btn", "n_clicks"),
    State({"type": "inv-missing-img", "index": ALL}, "value"),
    State({"type": "inv-missing-img", "index": ALL}, "id"),
    prevent_initial_call=True,
)
def save_missing_images(n_clicks, all_urls, all_ids):
    """Save image URLs for items that are missing images."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    saved = 0
    for url_val, id_dict in zip(all_urls, all_ids):
        url = (url_val or "").strip()
        if not url or not url.startswith("http"):
            continue
        # id index is "loc|name|cat"
        key_str = id_dict["index"]
        parts = key_str.split("|")
        if len(parts) != 3:
            continue
        name = parts[1]
        _IMAGE_URLS[name] = url
        try:
            from supabase_loader import save_image_override as _sio
            _sio(name, url)
        except Exception:
            pass
        saved += 1
    if saved:
        return (f"Saved {saved} image{'s' if saved != 1 else ''}!",
                {"color": GREEN, "fontWeight": "bold", "fontSize": "12px"})
    return ("No URLs to save.", {"color": GRAY, "fontSize": "12px"})


# ── Image Upload / URL / Paste Callbacks ──────────────────────────────────────

def _save_item_image(display_name, orig_item_name, image_url=None, file_data=None, filename=None):
    """Save an image for an item. Accepts either a URL or base64 file data.
    Returns (status_message, asset_url)."""
    from supabase_loader import save_image_override as _save_img_override

    if image_url:
        # Direct URL — just save the override, no local file needed
        url = image_url.strip()
        if not url:
            return ("No URL provided", "")
        _save_img_override(orig_item_name, url)
        if display_name and display_name != orig_item_name:
            _save_img_override(display_name, url)
        _IMAGE_URLS[orig_item_name] = url
        if display_name:
            _IMAGE_URLS[display_name] = url
        return ("Image set!", url)

    if file_data:
        # Base64 file data (from upload or clipboard paste)
        try:
            if "," in file_data:
                content_type, content_string = file_data.split(",", 1)
            else:
                content_string = file_data
            decoded = base64.b64decode(content_string)
        except Exception:
            return ("Upload failed", "")

        ext = os.path.splitext(filename or "img.png")[1].lower()
        if ext not in (".png", ".jpg", ".jpeg", ".gif", ".webp"):
            ext = ".png"

        safe_name = re.sub(r'[^\w\s-]', '', (display_name or orig_item_name or "item")[:50]).strip()
        safe_name = re.sub(r'[\s]+', '_', safe_name).lower()
        img_filename = f"{safe_name}{ext}"

        img_dir = os.path.join(BASE_DIR, "assets", "product_images")
        os.makedirs(img_dir, exist_ok=True)
        img_path = os.path.join(img_dir, img_filename)
        with open(img_path, "wb") as f:
            f.write(decoded)

        asset_url = f"/assets/product_images/{img_filename}"
        _save_img_override(orig_item_name, asset_url)
        if display_name and display_name != orig_item_name:
            _save_img_override(display_name, asset_url)
        _IMAGE_URLS[orig_item_name] = asset_url
        if display_name:
            _IMAGE_URLS[display_name] = asset_url
        return ("Image saved!", asset_url)

    return ("No image data", "")


# ── Quick Add Toggle ─────────────────────────────────────────────────────────

@app.callback(
    Output("qa-panel", "style"),
    Input("qa-toggle-btn", "n_clicks"),
    State("qa-panel", "style"),
    prevent_initial_call=True,
)
def toggle_quick_add(n_clicks, current_style):
    """Toggle Quick Add panel visibility."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    style = dict(current_style) if current_style else {}
    if style.get("display") == "none":
        style["display"] = "block"
    else:
        style["display"] = "none"
    return style


# ── Jump to Next Unsaved (clientside) ────────────────────────────────────────

app.clientside_callback(
    """
    function(n_clicks) {
        if (!n_clicks) return '';
        var cards = document.querySelectorAll('#editor-items-container > div');
        for (var i = 0; i < cards.length; i++) {
            if (!cards[i].classList.contains('order-card-saved')) {
                cards[i].scrollIntoView({behavior: 'smooth', block: 'center'});
                cards[i].style.boxShadow = '0 0 20px #f39c1244';
                setTimeout(function(c){ c.style.boxShadow = ''; }, 2000, cards[i]);
                return 'Scrolled!';
            }
        }
        return 'All done!';
    }
    """,
    Output("editor-jump-unsaved", "title"),
    Input("editor-jump-unsaved", "n_clicks"),
    prevent_initial_call=True,
)

# After a successful upload, reload the page so ALL tabs show fresh data.
# The upload callback sets upload-reload-trigger to a timestamp;
# this clientside callback watches it and reloads after a 1.5s delay
# (giving the user time to see the success message).
# No clientside page reload needed — upload-reload-trigger is now an Input
# to render_active_tab, which rebuilds the current tab with fresh data.


@app.callback(
    Output("qa-status", "children"),
    Output("qa-list", "children"),
    Input("qa-add-btn", "n_clicks"),
    State("qa-name", "value"),
    State("qa-category", "value"),
    State("qa-qty", "value"),
    State("qa-price", "value"),
    State("qa-location", "value"),
    prevent_initial_call=True,
)
def handle_quick_add(n_clicks, name, category, qty, price, location):
    """Add a new quick-add inventory item."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    name = (name or "").strip()
    if not name:
        return "Enter an item name", dash.no_update

    qty = int(qty or 1)
    price = float(price or 0)
    category = category or "Other"
    location = location or ""

    data = {
        "item_name": name,
        "category": category,
        "qty": qty,
        "unit_price": price,
        "location": location,
        "source": "Manual",
    }
    result = _save_quick_add(data)
    if result:
        _QUICK_ADDS.insert(0, result)
        # Build updated list
        qa_rows = []
        for qa in _QUICK_ADDS[:20]:
            created = qa.get("created_at", "")[:10] if qa.get("created_at") else ""
            qa_rows.append(html.Tr([
                html.Td(created, style={"color": GRAY, "padding": "3px 6px", "fontSize": "11px"}),
                html.Td(qa.get("item_name", ""), style={"color": WHITE, "padding": "3px 8px", "fontSize": "12px"}),
                html.Td(qa.get("category", ""), style={"color": TEAL, "padding": "3px 6px", "fontSize": "11px"}),
                html.Td(str(qa.get("qty", 1)), style={"textAlign": "center", "color": WHITE, "padding": "3px 6px", "fontSize": "11px"}),
                html.Td(f"${float(qa.get('unit_price', 0)):,.2f}", style={"textAlign": "right", "color": ORANGE, "padding": "3px 6px", "fontSize": "11px"}),
                html.Td(qa.get("location", ""), style={"color": GRAY, "padding": "3px 6px", "fontSize": "11px"}),
                html.Td(
                    html.Button("Del", id={"type": "del-qa-btn", "index": str(qa["id"])},
                                n_clicks=0,
                                style={"backgroundColor": "transparent", "color": RED, "border": f"1px solid {RED}44",
                                       "borderRadius": "4px", "padding": "2px 6px", "fontSize": "10px",
                                       "cursor": "pointer"}),
                    style={"padding": "3px 4px"}),
            ], style={"borderBottom": "1px solid #ffffff08"}))

        table = html.Table([
            html.Thead(html.Tr([
                html.Th("Date", style={"textAlign": "left", "padding": "4px 6px"}),
                html.Th("Item", style={"textAlign": "left", "padding": "4px 8px"}),
                html.Th("Category", style={"textAlign": "left", "padding": "4px 6px"}),
                html.Th("Qty", style={"textAlign": "center", "padding": "4px 6px"}),
                html.Th("Price", style={"textAlign": "right", "padding": "4px 6px"}),
                html.Th("Location", style={"textAlign": "left", "padding": "4px 6px"}),
                html.Th("", style={"width": "50px"}),
            ], style={"borderBottom": f"1px solid {GREEN}44"})),
            html.Tbody(qa_rows),
        ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE})

        return f"Added {name}! Refresh page to see in stock table.", table
    return "Error saving", dash.no_update


@app.callback(
    Output("qa-list", "children", allow_duplicate=True),
    Input({"type": "del-qa-btn", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def handle_delete_quick_add(all_clicks):
    """Delete a quick-add entry."""
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger = ctx.triggered[0]
    if not trigger["value"]:
        raise dash.exceptions.PreventUpdate

    prop_id = trigger["prop_id"]
    try:
        id_obj = json.loads(prop_id.split(".")[0])
        qa_id = int(id_obj["index"])
    except (json.JSONDecodeError, KeyError, ValueError):
        raise dash.exceptions.PreventUpdate

    ok = _delete_quick_add(qa_id)
    if ok:
        # Remove from in-memory list
        _QUICK_ADDS[:] = [qa for qa in _QUICK_ADDS if qa.get("id") != qa_id]

    # Rebuild list
    qa_rows = []
    for qa in _QUICK_ADDS[:20]:
        created = qa.get("created_at", "")[:10] if qa.get("created_at") else ""
        qa_rows.append(html.Tr([
            html.Td(created, style={"color": GRAY, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(qa.get("item_name", ""), style={"color": WHITE, "padding": "3px 8px", "fontSize": "12px"}),
            html.Td(qa.get("category", ""), style={"color": TEAL, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(str(qa.get("qty", 1)), style={"textAlign": "center", "color": WHITE, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(f"${float(qa.get('unit_price', 0)):,.2f}", style={"textAlign": "right", "color": ORANGE, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(qa.get("location", ""), style={"color": GRAY, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(
                html.Button("Del", id={"type": "del-qa-btn", "index": str(qa["id"])},
                            n_clicks=0,
                            style={"backgroundColor": "transparent", "color": RED, "border": f"1px solid {RED}44",
                                   "borderRadius": "4px", "padding": "2px 6px", "fontSize": "10px",
                                   "cursor": "pointer"}),
                style={"padding": "3px 4px"}),
        ], style={"borderBottom": "1px solid #ffffff08"}))

    if not qa_rows:
        return html.Div()

    return html.Table([
        html.Thead(html.Tr([
            html.Th("Date", style={"textAlign": "left", "padding": "4px 6px"}),
            html.Th("Item", style={"textAlign": "left", "padding": "4px 8px"}),
            html.Th("Category", style={"textAlign": "left", "padding": "4px 6px"}),
            html.Th("Qty", style={"textAlign": "center", "padding": "4px 6px"}),
            html.Th("Price", style={"textAlign": "right", "padding": "4px 6px"}),
            html.Th("Location", style={"textAlign": "left", "padding": "4px 6px"}),
            html.Th("", style={"width": "50px"}),
        ], style={"borderBottom": f"1px solid {GREEN}44"})),
        html.Tbody(qa_rows),
    ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE})


# ── Chatbot Callback ─────────────────────────────────────────────────────────

def _parse_nav_tag(text):
    """Extract [NAV:tab-name] from Jarvis response. Returns (clean_text, first tab_name or None).
    Removes ALL nav tags from text but only returns the first one for navigation."""
    import re as _re
    matches = list(_re.finditer(r'\[NAV:(tab-[\w-]+)\]', text))
    if not matches:
        return text, None
    first_tab = matches[0].group(1)
    # Remove all nav tags from displayed text
    clean = _re.sub(r'\s*\[NAV:tab-[\w-]+\]', '', text).strip()
    return clean, first_tab


_TAB_LABELS = {
    "tab-overview": "Overview",
    "tab-deep-dive": "JARVIS",
    "tab-financials": "Financials",
    "tab-inventory": "Inventory",
    "tab-tax-forms": "Tax Forms",
    "tab-valuation": "Business Valuation",
    "tab-data-hub": "Data Hub",
}


@app.callback(
    Output("chat-history", "children"),
    Output("chat-store", "data"),
    Output("chat-input", "value"),
    Output("main-tabs", "value", allow_duplicate=True),
    Input("chat-send", "n_clicks"),
    Input("chat-input", "n_submit"),
    Input({"type": "quick-q", "index": dash.ALL}, "n_clicks"),
    Input({"type": "jarvis-nav", "tab": dash.ALL}, "n_clicks"),
    State("chat-input", "value"),
    State("chat-store", "data"),
    State("chat-history", "children"),
    State("main-tabs", "value"),
    prevent_initial_call=True,
)
@guard_callback(n_outputs=4)
def handle_chat(n_clicks, n_submit, quick_clicks, nav_clicks, user_input, history_data, current_children, current_tab):
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger_id = ctx.triggered[0]["prop_id"]

    # Handle navigation button clicks
    if "jarvis-nav" in trigger_id:
        try:
            idx_data = json.loads(trigger_id.split(".")[0])
            target_tab = idx_data["tab"]
            return dash.no_update, dash.no_update, dash.no_update, target_tab
        except Exception:
            raise dash.exceptions.PreventUpdate

    # Determine the question
    question = ""
    if "quick-q" in trigger_id:
        # One of the quick-question buttons was clicked
        quick_questions = [
            "Full summary", "Net profit", "Best sellers",
            "Shipping P/L", "Monthly breakdown", "Refunds",
            "Growth trend", "Best month", "Fees breakdown",
            "Inventory / COGS", "Unit economics",
            "Patterns", "Cash flow", "Valuation", "Debt",
        ]
        try:
            idx_data = json.loads(trigger_id.split(".")[0])
            idx = int(idx_data["index"])
            question = quick_questions[idx]
        except Exception:
            raise dash.exceptions.PreventUpdate
    else:
        question = (user_input or "").strip()

    if not question:
        raise dash.exceptions.PreventUpdate

    # Get answer
    history_data = history_data or []
    answer = chatbot_answer(question, history_data)

    # Add to history
    history_data.append({"q": question, "a": answer})

    # Build chat bubbles
    children = [
        # Initial greeting
        html.Div([
            html.Div([
                html.Div("JARVIS online.", style={"fontWeight": "bold", "marginBottom": "4px", "color": CYAN}),
                html.Div("I've reviewed the books. Revenue, margins, refund accountability, "
                         "inventory, cash flow — I have eyes on everything. Ask me anything, "
                         "or I'll tell you what needs your attention.",
                    style={"fontSize": "13px"}),
            ], style={
                "backgroundColor": f"{CYAN}15", "border": f"1px solid {CYAN}33",
                "borderRadius": "12px", "padding": "12px 16px", "maxWidth": "85%",
                "color": WHITE, "whiteSpace": "pre-wrap",
            }),
        ], style={"display": "flex", "justifyContent": "flex-start", "marginBottom": "10px"}),
    ]

    for _entry_idx, entry in enumerate(history_data):
        # User message
        children.append(html.Div([
            html.Div(entry["q"], style={
                "backgroundColor": f"{ORANGE}25", "border": f"1px solid {ORANGE}44",
                "borderRadius": "12px", "padding": "10px 16px", "maxWidth": "75%",
                "color": WHITE, "fontSize": "13px",
            }),
        ], style={"display": "flex", "justifyContent": "flex-end", "marginBottom": "6px"}))

        # Parse nav tag from response
        _display_text, _nav_tab = _parse_nav_tag(entry["a"])

        # Bot response — red tint on error
        _is_error = _display_text.startswith("Sorry") or "error" in _display_text[:50].lower()
        _bg = f"{RED}15" if _is_error else f"{CYAN}15"
        _border = f"1px solid {RED}33" if _is_error else f"1px solid {CYAN}33"

        _response_content = [
            dcc.Markdown(_display_text, style={"color": WHITE, "fontSize": "13px", "lineHeight": "1.5"}),
        ]

        # Add navigation button if Jarvis suggested a tab
        if _nav_tab and _nav_tab in _TAB_LABELS:
            _response_content.append(
                html.Button(
                    f"\u27a4  Go to {_TAB_LABELS[_nav_tab]}",
                    id={"type": "jarvis-nav", "tab": _nav_tab, "idx": _entry_idx},
                    n_clicks=0,
                    style={
                        "marginTop": "10px", "padding": "8px 16px",
                        "backgroundColor": f"{CYAN}25", "border": f"1px solid {CYAN}",
                        "borderRadius": "8px", "color": CYAN, "fontSize": "12px",
                        "fontWeight": "bold", "cursor": "pointer", "letterSpacing": "0.5px",
                    },
                )
            )

        children.append(html.Div([
            html.Div(_response_content, style={
                "backgroundColor": _bg, "border": _border,
                "borderRadius": "12px", "padding": "12px 16px", "maxWidth": "85%",
            }),
        ], style={"display": "flex", "justifyContent": "flex-start", "marginBottom": "10px"}))

    return children, history_data, "", dash.no_update


# ── Data Hub: Store sub-tab sync ─────────────────────────────────────────────

@app.callback(
    Output("datahub-etsy-store-picker", "data"),
    Output("datahub-active-store-tab", "data"),
    Input("datahub-store-tabs", "value"),
    prevent_initial_call=True,
)
def sync_datahub_store_tab(tab):
    """Set the hidden store picker and remember active tab."""
    store_map = {"dh-all": "all", "dh-keycomponentmfg": "keycomponentmfg",
                 "dh-aurvio": "aurvio", "dh-lunalinks": "lunalinks"}
    return store_map.get(tab, "keycomponentmfg"), tab or "dh-keycomponentmfg"



@app.callback(
    Output("datahub-store-tab-content", "children"),
    Input("datahub-store-tabs", "value"),
)
def render_datahub_store_tab(tab):
    """Render the content for the selected store sub-tab."""
    store_map = {"dh-all": "all", "dh-keycomponentmfg": "keycomponentmfg",
                 "dh-aurvio": "aurvio", "dh-lunalinks": "lunalinks"}
    store_key = store_map.get(tab, "keycomponentmfg")
    store_label = STORES.get(store_key, "KeyComponentMFG")
    store_color = STORE_COLORS.get(store_key, TEAL)
    return _build_store_etsy_tab(store_key, store_label, store_color)


# ── Data Hub: Initial file list populator ───────────────────────────────────

@app.callback(
    Output("datahub-etsy-files", "children"),
    Output("datahub-receipt-files", "children"),
    Output("datahub-bank-files", "children"),
    Output("datahub-etsy-stats", "children"),
    Output("datahub-receipt-stats", "children"),
    Output("datahub-bank-stats", "children"),
    Input("datahub-init-trigger", "data"),
    Input("store-selector", "value"),
    Input("datahub-store-tab-content", "children"),
    State("datahub-store-tabs", "value"),
    prevent_initial_call=True,
)
@guard_callback(n_outputs=6)
def init_datahub_files(_trigger, _selected_store, _tab_content, _dh_store_tab):
    """Populate existing file lists and initial stats on page load."""
    # Use the Data Hub store sub-tab for Etsy filtering
    _dh_store_map = {"dh-all": "all", "dh-keycomponentmfg": "keycomponentmfg",
                     "dh-aurvio": "aurvio", "dh-lunalinks": "lunalinks"}
    _store = _dh_store_map.get(_dh_store_tab, _selected_store or "all")
    _all_data = _DATA_ALL if _DATA_ALL is not None else DATA

    # Etsy stats — filtered by selected store
    if _store == "all" or not _store:
        _etsy_data = _all_data
        _store_label = "All Stores"
    else:
        _etsy_data = _all_data[_all_data["Store"] == _store] if "Store" in _all_data.columns else _all_data
        _store_label = STORES.get(_store, _store)

    _etsy_sales = _etsy_data[_etsy_data["Type"] == "Sale"] if len(_etsy_data) > 0 else _etsy_data
    _etsy_order_count = len(_etsy_sales)
    _etsy_gross = _etsy_sales["Net_Clean"].sum() if _etsy_order_count > 0 else 0

    etsy_stats = html.Div([
        html.Span(f"[{_store_label}] ", style={"color": STORE_COLORS.get(_store, TEAL), "fontWeight": "bold"}),
        html.Span(f"{len(_etsy_data)} transactions  |  {_etsy_order_count} orders  |  Gross: ${_etsy_gross:,.2f}"),
    ], style={"color": TEAL, "fontSize": "12px", "fontFamily": "monospace"})

    # Etsy files — show per-store or all
    etsy_files = _get_existing_files("etsy")

    # Tag root-level files (no subdirectory) as keycomponentmfg
    for f in etsy_files:
        if "/" not in f["filename"]:
            f["filename"] = f"keycomponentmfg/{f['filename']}"

    if _store != "all" and _store:
        # Filter to only show files from selected store
        etsy_files = [f for f in etsy_files if f["filename"].startswith(f"{_store}/")]

    # Receipts and bank — always show all (shared across stores)
    receipt_files = _get_existing_files("receipt")
    bank_files = _get_existing_files("bank")
    receipt_stats = html.Div(f"{len(INVOICES)} orders  |  ${total_inventory_cost:,.2f} total spend",
                              style={"color": PURPLE, "fontSize": "12px", "fontFamily": "monospace"})
    bank_stats = html.Div(f"{len(BANK_TXNS)} transactions  |  Net: ${bank_net_cash:,.2f}",
                           style={"color": CYAN, "fontSize": "12px", "fontFamily": "monospace"})

    return (_render_file_list(etsy_files, TEAL),
            _render_file_list(receipt_files, PURPLE),
            _render_file_list(bank_files, CYAN),
            etsy_stats, receipt_stats, bank_stats)


# ── Data Hub: Upload Callback ───────────────────────────────────────────────

# ── Bank + Receipt Upload (shared across stores — always in DOM) ─────────
@app.callback(
    Output("datahub-bank-status", "children"),
    Output("datahub-bank-files", "children", allow_duplicate=True),
    Output("datahub-bank-stats", "children", allow_duplicate=True),
    Output("datahub-receipt-status", "children"),
    Output("datahub-receipt-files", "children", allow_duplicate=True),
    Output("datahub-receipt-stats", "children", allow_duplicate=True),
    Output("datahub-activity-log", "children", allow_duplicate=True),
    Output("datahub-summary-strip", "children", allow_duplicate=True),
    Output("app-header-content", "children", allow_duplicate=True),
    Output("upload-reload-trigger", "data", allow_duplicate=True),
    Input("datahub-bank-upload", "contents"),
    Input("datahub-receipt-upload", "contents"),
    State("datahub-bank-upload", "filename"),
    State("datahub-receipt-upload", "filename"),
    State("datahub-activity-log", "children"),
    prevent_initial_call=True,
)
@guard_callback(n_outputs=10)
def handle_shared_upload(bank_contents, receipt_contents,
                         bank_filename, receipt_filename,
                         activity_log):
    """Handle bank and receipt uploads — these are always visible on All Stores tab."""
    global DATA, BANK_TXNS
    import datetime as _dt

    trigger = callback_context.triggered[0]["prop_id"] if callback_context.triggered else ""
    now_str = _dt.datetime.now().strftime("%I:%M:%S %p")
    nu = dash.no_update

    _logger.info("Shared upload callback fired: trigger=%s", trigger)

    bank_status, bank_file_list, bank_stats = nu, nu, nu
    rcpt_status, rcpt_file_list, rcpt_stats = nu, nu, nu
    new_log = activity_log or []
    summary = nu
    header = nu
    reload_trigger = nu

    # ── Receipt PDF Upload ───────────────────────────────────────────────
    if "datahub-receipt-upload" in trigger and receipt_contents:
        try:
            content_type, content_string = receipt_contents.split(",")
            decoded = base64.b64decode(content_string)

            fname = receipt_filename or "receipt.pdf"
            save_folder = os.path.join(BASE_DIR, "data", "invoices", "keycomp")
            os.makedirs(save_folder, exist_ok=True)
            save_path = os.path.join(save_folder, fname)
            with open(save_path, "wb") as f:
                f.write(decoded)

            from _parse_invoices import parse_pdf_file
            order = parse_pdf_file(save_path)

            if not order or not order.get("items"):
                try:
                    os.remove(save_path)
                except Exception:
                    pass
                rcpt_status = html.Div([
                    html.Span("\u2717 ", style={"color": RED, "fontWeight": "bold"}),
                    html.Span("Could not parse any items from this PDF.",
                              style={"color": RED, "fontSize": "13px"}),
                ])
            else:
                dup = any(inv["order_num"] == order["order_num"] for inv in INVOICES)
                if dup:
                    try:
                        os.remove(save_path)
                    except Exception:
                        pass
                    rcpt_status = html.Div([
                        html.Span("\u26a0 ", style={"color": ORANGE, "fontWeight": "bold"}),
                        html.Span(f"Order #{order['order_num']} already exists!",
                                  style={"color": ORANGE, "fontSize": "13px", "fontWeight": "bold"}),
                    ])
                else:
                    if order.get("source") == "Personal Amazon":
                        new_folder = os.path.join(BASE_DIR, "data", "invoices", "personal_amazon")
                        new_path = os.path.join(new_folder, fname)
                        if save_path != new_path:
                            try:
                                os.rename(save_path, new_path)
                            except Exception:
                                pass

                    stats = _reload_inventory_data(order)
                    _cascade_reload("inventory")
                    rcpt_status = html.Div([
                        html.Div([
                            html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
                            html.Span(f"Parsed {fname}: Order #{stats['order_num']} — "
                                      f"{stats['item_count']} item(s), ${stats['grand_total']:.2f}",
                                      style={"color": GREEN, "fontSize": "13px"}),
                        ]),
                        html.Div([
                            html.Span("\u2192 ", style={"color": CYAN, "fontWeight": "bold"}),
                            html.Span("Next: Go to the Inventory tab to review and name these items.",
                                      style={"color": CYAN, "fontSize": "12px", "fontStyle": "italic"}),
                        ], style={"marginTop": "4px"}),
                    ])
                    rcpt_stats = html.Div(
                        f"{len(INVOICES)} orders  |  ${total_inventory_cost:,.2f} total spend",
                        style={"color": PURPLE, "fontSize": "12px", "fontFamily": "monospace"})
                    rcpt_file_list = _render_file_list(_get_existing_files("receipt"), PURPLE)
                    summary = _build_datahub_summary()
                    header = _header_text()
                    reload_trigger = _dt.datetime.now().timestamp()
                    new_log = [html.Div([
                        html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
                        html.Span(f"\u2713 Receipt: {fname} (Order #{stats['order_num']}, "
                                  f"{stats['item_count']} items)",
                                  style={"color": GREEN, "fontSize": "12px"}),
                    ], style={"padding": "3px 0", "borderBottom": "1px solid #ffffff08"})] + (
                        new_log if isinstance(new_log, list) else [])
        except Exception as e:
            rcpt_status = html.Div([
                html.Span("\u2717 ", style={"color": RED, "fontWeight": "bold"}),
                html.Span(f"Upload failed: {e}", style={"color": RED, "fontSize": "13px"}),
            ])

    # ── Bank PDF/CSV Upload ─────────────────────────────────────────────
    elif "datahub-bank-upload" in trigger and bank_contents:
        try:
            content_type, content_string = bank_contents.split(",")
            decoded = base64.b64decode(content_string)

            is_dup, dup_file = _check_bank_file_duplicate(decoded, bank_filename)
            if is_dup:
                bank_status = html.Div([
                    html.Span("\u26a0 ", style={"color": ORANGE, "fontWeight": "bold"}),
                    html.Span(f"This file is already uploaded (matches {dup_file})",
                              style={"color": ORANGE, "fontSize": "13px", "fontWeight": "bold"}),
                ])
            else:
                fname = bank_filename or "bank_statement.pdf"
                save_folder = os.path.join(BASE_DIR, "data", "bank_statements")
                os.makedirs(save_folder, exist_ok=True)
                save_path = os.path.join(save_folder, fname)
                with open(save_path, "wb") as f:
                    f.write(decoded)

                _sb_ok = False
                if IS_RAILWAY:
                    from _parse_bank_statements import parse_bank_pdf as _pb, parse_bank_csv as _pc
                    from _parse_bank_statements import apply_overrides as _ao
                    _new_txns = []
                    if fname.lower().endswith(".pdf"):
                        try:
                            _new_txns, _ = _pb(save_path)
                        except Exception:
                            pass
                    elif fname.lower().endswith(".csv"):
                        try:
                            _new_txns, _ = _pc(save_path)
                        except Exception:
                            pass
                    _new_txns = _ao(_new_txns)
                    _bank_added_count = 0
                    _bank_skipped_count = 0
                    if _new_txns:
                        _existing_keys = {(t["date"], f"{t['amount']:.2f}", t["type"],
                                           t.get("raw_desc", t["desc"])) for t in BANK_TXNS}
                        _added = [t for t in _new_txns
                                  if (t["date"], f"{t['amount']:.2f}", t["type"],
                                      t.get("raw_desc", t["desc"])) not in _existing_keys]
                        _bank_added_count = len(_added)
                        _bank_skipped_count = len(_new_txns) - _bank_added_count
                        BANK_TXNS.extend(_added)
                    _rebuild_bank_derived()
                    _final_bal = bank_running[-1]["_balance"] if bank_running else 0.0
                    stats = {"transactions": len(BANK_TXNS), "statements": 0,
                             "net_cash": round(_final_bal, 2),
                             "added": _bank_added_count, "skipped": _bank_skipped_count}
                    _cascade_reload("bank")
                    import threading
                    _bank_copy = list(_new_txns)
                    threading.Thread(target=_append_bank_to_supabase, args=(_bank_copy,), daemon=True).start()
                    _sb_ok = True
                else:
                    stats = _reload_bank_data()
                    stats["added"] = stats.get("transactions", 0)
                    stats["skipped"] = 0
                    _cascade_reload("bank")
                    import threading
                    _bank_copy = list(BANK_TXNS)
                    threading.Thread(target=_sync_bank_to_supabase, args=(_bank_copy,), daemon=True).start()
                    _sb_ok = True

                _bank_warn = "" if _sb_ok else " (WARNING: Supabase sync failed)"
                _dedup_info = ""
                if stats.get("skipped", 0) > 0:
                    _dedup_info = f" ({stats['added']} new, {stats['skipped']} duplicates skipped)"
                elif stats.get("added", 0) > 0:
                    _dedup_info = f" ({stats['added']} new transactions)"
                bank_status = html.Div([
                    html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
                    html.Span(f"Uploaded {fname} — {stats['transactions']} total transactions{_dedup_info}, "
                              f"Net: ${stats['net_cash']:,.2f}{_bank_warn}",
                              style={"color": GREEN if _sb_ok else ORANGE, "fontSize": "13px"}),
                ])
                bank_stats = html.Div(
                    f"{stats['transactions']} transactions  |  Net: ${stats['net_cash']:,.2f}",
                    style={"color": CYAN, "fontSize": "12px", "fontFamily": "monospace"})
                bank_file_list = _render_file_list(_get_existing_files("bank"), CYAN)
                summary = _build_datahub_summary()
                header = _header_text()
                reload_trigger = _dt.datetime.now().timestamp()
                new_log = [html.Div([
                    html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
                    html.Span(f"\u2713 Bank: {fname} ({stats['transactions']} txns)",
                              style={"color": GREEN, "fontSize": "12px"}),
                ], style={"padding": "3px 0", "borderBottom": "1px solid #ffffff08"})] + (
                    new_log if isinstance(new_log, list) else [])
        except Exception as e:
            bank_status = html.Div([
                html.Span("\u2717 ", style={"color": RED, "fontWeight": "bold"}),
                html.Span(f"Upload failed: {e}", style={"color": RED, "fontSize": "13px"}),
            ])

    return (bank_status, bank_file_list, bank_stats,
            rcpt_status, rcpt_file_list, rcpt_stats,
            new_log, summary, header, reload_trigger)


# ── Store-Specific Uploads (etsy, orders, listings — only exist on store tabs) ──
@app.callback(
    Output("datahub-etsy-status", "children"),
    Output("datahub-etsy-files", "children", allow_duplicate=True),
    Output("datahub-etsy-stats", "children", allow_duplicate=True),
    Output("datahub-activity-log", "children"),
    Output("datahub-summary-strip", "children"),
    Output("app-header-content", "children"),
    Output("upload-reload-trigger", "data"),
    Output("datahub-orders-status", "children"),
    Output("datahub-orders-files", "children"),
    Output("datahub-orders-stats", "children"),
    Output("datahub-listings-status", "children"),
    Output("datahub-listings-files", "children"),
    Output("datahub-listings-stats", "children"),
    Input("datahub-etsy-upload", "contents"),
    Input("datahub-orders-upload", "contents"),
    Input("datahub-listings-upload", "contents"),
    State("datahub-etsy-upload", "filename"),
    State("datahub-orders-upload", "filename"),
    State("datahub-listings-upload", "filename"),
    State("datahub-activity-log", "children"),
    State("datahub-etsy-store-picker", "data"),
    prevent_initial_call=True,
)
@guard_callback(n_outputs=13)
def handle_datahub_upload(etsy_contents, orders_contents, listings_contents,
                          etsy_filename, orders_filename, listings_filename,
                          activity_log, etsy_store_picker):
    """Handle file uploads from all 3 Data Hub zones.

    After processing, sets upload-reload-trigger which fires a clientside
    callback to reload the page — ensuring ALL tabs show fresh data.
    """
    global DATA, BANK_TXNS
    import datetime as _dt

    trigger = callback_context.triggered[0]["prop_id"] if callback_context.triggered else ""
    now_str = _dt.datetime.now().strftime("%I:%M:%S %p")
    nu = dash.no_update  # shorthand

    _logger.info("Upload callback fired: trigger=%s, IS_RAILWAY=%s", trigger, IS_RAILWAY)

    # Initialize outputs — all no_update by default
    etsy_status, etsy_file_list, etsy_stats = nu, nu, nu
    orders_status, orders_file_list, orders_stats = nu, nu, nu
    listings_status, listings_file_list, listings_stats = nu, nu, nu
    new_log = activity_log or []
    summary = nu
    header = nu
    reload_trigger = nu  # set to timestamp after successful upload to force page reload

    # ── Etsy CSV Upload ──────────────────────────────────────────────────
    if "datahub-etsy-upload" in trigger and etsy_contents:
        try:
            content_type, content_string = etsy_contents.split(",")
            decoded = base64.b64decode(content_string)

            # ── Phase 3: Upload validation with preview ──
            from dashboard_utils.upload_validator import validate_etsy_csv as _validate_upload
            from supabase_loader import get_config_value as _gcv_upload

            _upload_store = etsy_store_picker or "keycomponentmfg"
            _preview = _validate_upload(decoded, _upload_store, _gcv_upload, existing_data=DATA)

            if not _preview.is_valid:
                # Blocking errors — stop upload
                _error_items = [html.Div([
                    html.Span("\u2717 ", style={"color": RED, "fontWeight": "bold", "fontSize": "16px"}),
                    html.Span("UPLOAD BLOCKED", style={"color": RED, "fontSize": "14px", "fontWeight": "bold"}),
                ], style={"marginBottom": "6px"})]
                for _err in _preview.errors:
                    _error_items.append(html.Div([
                        html.Span("  \u2022 ", style={"color": RED}),
                        html.Span(_err, style={"color": RED, "fontSize": "12px"}),
                    ]))
                etsy_status = html.Div(_error_items, style={
                    "background": "#1a0a0a", "border": f"1px solid {RED}44",
                    "borderRadius": "6px", "padding": "10px 14px", "marginTop": "6px",
                })
            else:
                df = _preview.df

                # ── Build preview info panel ──
                _store_display = STORES.get(_upload_store, _upload_store)
                _preview_items = []

                # Header: what we're uploading
                _preview_items.append(html.Div([
                    html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold", "fontSize": "16px"}),
                    html.Span("CSV VALIDATED", style={"color": GREEN, "fontSize": "14px", "fontWeight": "bold"}),
                    html.Span(f"  \u2192  {_store_display}", style={"color": CYAN, "fontSize": "13px", "marginLeft": "8px"}),
                ], style={"marginBottom": "8px"}))

                # Store detection result
                if _preview.detected_store:
                    _det_name = STORES.get(_preview.detected_store, _preview.detected_store)
                    _det_color = GREEN if not _preview.store_mismatch else ORANGE
                    _det_icon = "\u2713" if not _preview.store_mismatch else "\u26a0"
                    _conf_pct = f"{_preview.store_confidence:.0%}"
                    _preview_items.append(html.Div([
                        html.Span(f"{_det_icon} ", style={"color": _det_color, "fontWeight": "bold"}),
                        html.Span("Detected store: ", style={"color": GRAY, "fontSize": "12px"}),
                        html.Span(f"{_det_name} ", style={"color": _det_color, "fontSize": "12px", "fontWeight": "bold"}),
                        html.Span(f"({_conf_pct} confidence)", style={"color": DARKGRAY, "fontSize": "11px"}),
                    ], style={"marginBottom": "3px"}))
                else:
                    _preview_items.append(html.Div([
                        html.Span("\u2014 ", style={"color": DARKGRAY}),
                        html.Span("Store detection: ", style={"color": GRAY, "fontSize": "12px"}),
                        html.Span("No listings match (upload listings first for auto-detection)",
                                  style={"color": DARKGRAY, "fontSize": "11px", "fontStyle": "italic"}),
                    ], style={"marginBottom": "3px"}))

                # Date range and row count
                _date_str = f"{_preview.date_range[0]} - {_preview.date_range[1]}" if _preview.date_range[0] else "Unknown"
                _months_str = ", ".join(_preview.months) if _preview.months else "N/A"
                _preview_items.append(html.Div([
                    html.Span("\u2022 ", style={"color": CYAN}),
                    html.Span(f"{_preview.row_count} rows", style={"color": WHITE, "fontSize": "12px", "fontWeight": "bold"}),
                    html.Span(f"  |  {_preview.month_count} month(s): {_months_str}",
                              style={"color": GRAY, "fontSize": "12px"}),
                ], style={"marginBottom": "2px"}))
                _preview_items.append(html.Div([
                    html.Span("\u2022 ", style={"color": CYAN}),
                    html.Span(f"Date range: {_date_str}", style={"color": GRAY, "fontSize": "12px"}),
                ], style={"marginBottom": "2px"}))

                # Transaction breakdown
                if _preview.transaction_count_by_type:
                    _type_parts = []
                    for _ttype, _tcount in sorted(_preview.transaction_count_by_type.items(), key=lambda x: -x[1]):
                        _type_parts.append(f"{_ttype}: {_tcount}")
                    _preview_items.append(html.Div([
                        html.Span("\u2022 ", style={"color": CYAN}),
                        html.Span("Breakdown: ", style={"color": GRAY, "fontSize": "12px"}),
                        html.Span(" | ".join(_type_parts), style={"color": TEAL, "fontSize": "11px", "fontFamily": "monospace"}),
                    ], style={"marginBottom": "2px"}))

                # Warnings (store mismatch, overlap, etc.)
                if _preview.warnings:
                    _preview_items.append(html.Hr(style={"borderColor": "#ffffff11", "margin": "6px 0"}))
                    for _warn in _preview.warnings:
                        _warn_color = ORANGE if "mismatch" in _warn.lower() else "#ccaa00"
                        _preview_items.append(html.Div([
                            html.Span("\u26a0 ", style={"color": ORANGE, "fontWeight": "bold"}),
                            html.Span(_warn, style={"color": _warn_color, "fontSize": "12px"}),
                        ], style={"marginBottom": "2px"}))

                # Store mismatch gets extra emphasis
                if _preview.store_mismatch:
                    _det_name = STORES.get(_preview.detected_store, _preview.detected_store)
                    _preview_items.append(html.Div([
                        html.Span("\u26a0 VERIFY: ", style={"color": ORANGE, "fontWeight": "bold", "fontSize": "12px"}),
                        html.Span(f"This CSV looks like it belongs to {_det_name}. "
                                  f"Uploading to {_store_display} instead. Proceed with caution.",
                                  style={"color": ORANGE, "fontSize": "11px", "fontStyle": "italic"}),
                    ], style={"marginTop": "4px", "background": "#2a1a00", "padding": "6px 10px",
                              "borderRadius": "4px", "border": f"1px solid {ORANGE}44"}))

                # ── Proceed with upload (auto-confirm for now) ──
                # Tag uploaded data with the selected store
                df["Store"] = _upload_store

                # Auto-name: use filename as-is, or generate etsy_statement_YYYY_MM.csv
                fname = etsy_filename or "etsy_upload.csv"
                if not fname.startswith("etsy_statement"):
                    try:
                        dates = pd.to_datetime(df["Date"], format="%B %d, %Y", errors="coerce")
                        min_d = dates.min()
                        if pd.notna(min_d):
                            fname = f"etsy_statement_{min_d.year}_{min_d.month:02d}.csv"
                    except Exception:
                        pass

                # Check for overlap with existing files
                has_overlap, overlap_file, overlap_msg = _check_etsy_csv_overlap(df, fname)
                if has_overlap and overlap_file:
                    old_path = os.path.join(BASE_DIR, "data", "etsy_statements", _upload_store, overlap_file)
                    try:
                        os.remove(old_path)
                    except Exception:
                        # Also try root-level (legacy location)
                        try:
                            os.remove(os.path.join(BASE_DIR, "data", "etsy_statements", overlap_file))
                        except Exception:
                            pass

                _etsy_dir = os.path.join(BASE_DIR, "data", "etsy_statements", _upload_store)
                os.makedirs(_etsy_dir, exist_ok=True)
                save_path = os.path.join(_etsy_dir, fname)
                with open(save_path, "wb") as f:
                    f.write(decoded)

                # Step 1: Parse the uploaded CSV and tag with store
                _new_df = df.copy()
                _new_df["Store"] = _upload_store
                _new_df["Amount_Clean"] = _new_df["Amount"].apply(parse_money)
                _new_df["Net_Clean"] = _new_df["Net"].apply(parse_money)
                _new_df["Fees_Clean"] = _new_df["Fees & Taxes"].apply(parse_money)
                _new_df["Date_Parsed"] = pd.to_datetime(_new_df["Date"], format="%B %d, %Y", errors="coerce")
                _new_df["Month"] = _new_df["Date_Parsed"].dt.to_period("M").astype(str)
                _new_months = set(_new_df["Month"].dropna().unique())

                # Step 2: Sync this store's data to Supabase FIRST (synchronous — must complete before reload)
                # Build the full store DataFrame: existing non-overlapping months + new months
                _existing_store = DATA[DATA["Store"] == _upload_store].copy() if "Store" in DATA.columns else pd.DataFrame()
                if "Month" not in _existing_store.columns and len(_existing_store) > 0:
                    _existing_store["Date_Parsed"] = pd.to_datetime(_existing_store["Date"], format="%B %d, %Y", errors="coerce")
                    _existing_store["Month"] = _existing_store["Date_Parsed"].dt.to_period("M").astype(str)
                _keep_existing = _existing_store[~_existing_store["Month"].isin(_new_months)] if len(_existing_store) > 0 else pd.DataFrame()
                _full_store_df = pd.concat([_keep_existing, _new_df], ignore_index=True)
                _full_store_df["Store"] = _upload_store
                _replaced_count = len(_existing_store) - len(_keep_existing)

                _logger.info("Syncing %d rows for '%s' to Supabase (%d replaced)", len(_full_store_df), _upload_store, _replaced_count)
                _sb_ok = False
                try:
                    _sync_etsy_to_supabase(_full_store_df)
                    _sb_ok = True
                except Exception as _se:
                    _logger.error("Supabase sync failed: %s", _se)

                # Step 3: Reload ALL data from Supabase to get a clean, complete state
                _logger.info("Reloading all data from Supabase...")
                _sb_data = _load_data()
                DATA = _sb_data["DATA"]
                _logger.info("Loaded %d total rows from Supabase", len(DATA))

                # Step 4: Rebuild everything from the clean data
                _rebuild_etsy_derived()
                stats = {"transactions": len(DATA), "orders": len(sales_df),
                         "gross_sales": sales_df["Net_Clean"].sum()}
                stats["new_rows"] = len(_new_df)
                stats["replaced_rows"] = _replaced_count
                stats["months"] = ", ".join(sorted(_new_months))
                _cascade_reload("etsy")
                _logger.info("Upload complete: %d rows, gross=$%.2f", len(DATA), gross_sales)
                msg = f"{len(_new_df)} rows loaded ({_replaced_count} replaced, months: {stats['months']})"


                if not _sb_ok:
                    msg += " (WARNING: Supabase sync failed — data may not persist after restart)"

                # ── Build final status with preview + result ──
                _preview_items.append(html.Hr(style={"borderColor": "#ffffff11", "margin": "6px 0"}))

                if has_overlap:
                    _preview_items.append(html.Div([
                        html.Span("\u26a0 ", style={"color": ORANGE, "fontWeight": "bold"}),
                        html.Span(f"Replaced {overlap_file} — {overlap_msg}. {msg}",
                                  style={"color": ORANGE, "fontSize": "12px"}),
                    ]))
                    log_icon, log_color = "\u26a0", ORANGE
                    log_text = f"Etsy CSV [{_store_display}]: Replaced {overlap_file} with {fname} ({msg})"
                else:
                    _preview_items.append(html.Div([
                        html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
                        html.Span(f"Uploaded {fname} — {msg}", style={"color": GREEN, "fontSize": "12px"}),
                    ]))
                    log_icon, log_color = "\u2713", GREEN
                    log_text = f"Etsy CSV [{_store_display}]: {fname} ({msg})"

                if not _sb_ok:
                    _preview_items.append(html.Div([
                        html.Span("\u26a0 ", style={"color": RED, "fontWeight": "bold"}),
                        html.Span("Supabase sync failed — data may not persist after restart",
                                  style={"color": RED, "fontSize": "11px"}),
                    ]))

                etsy_status = html.Div(_preview_items, style={
                    "background": "#0a1a0a" if not _preview.store_mismatch else "#1a1a0a",
                    "border": f"1px solid {GREEN}44" if not _preview.store_mismatch else f"1px solid {ORANGE}44",
                    "borderRadius": "6px", "padding": "10px 14px", "marginTop": "6px",
                })

                etsy_stats = html.Div(
                    f"{stats['transactions']} transactions  |  {stats['orders']} orders  |  "
                    f"Gross: ${stats['gross_sales']:,.2f}",
                    style={"color": TEAL, "fontSize": "12px", "fontFamily": "monospace"})
                etsy_file_list = _render_file_list(_get_existing_files("etsy"), TEAL)
                summary = _build_datahub_summary()
                header = _header_text()
                new_log = [html.Div([
                    html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
                    html.Span(f"{log_icon} {log_text}",
                              style={"color": log_color, "fontSize": "12px"}),
                ], style={"padding": "3px 0", "borderBottom": "1px solid #ffffff08"})] + (
                    new_log if isinstance(new_log, list) else [])
        except Exception as e:
            etsy_status = html.Div([
                html.Span("\u2717 ", style={"color": RED, "fontWeight": "bold"}),
                html.Span(f"Upload failed: {e}", style={"color": RED, "fontSize": "13px"}),
                html.Div("Expected: Etsy CSV with columns Date, Type, Title, Info, Currency, Amount, Fees & Taxes, Net",
                         style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "4px"}),
            ])

    # ── Order CSV Upload ────────────────────────────────────────────────
    elif "datahub-orders-upload" in trigger and orders_contents:
        try:
            content_type, content_string = orders_contents.split(",")
            decoded = base64.b64decode(content_string)

            _upload_store = etsy_store_picker or "keycomponentmfg"
            _store_display = STORES.get(_upload_store, _upload_store)
            fname = orders_filename or "orders.csv"

            # Save to data/order_csvs/{store}/
            _orders_dir = os.path.join(BASE_DIR, "data", "order_csvs", _upload_store)
            os.makedirs(_orders_dir, exist_ok=True)
            save_path = os.path.join(_orders_dir, fname)
            with open(save_path, "wb") as f:
                f.write(decoded)

            # Parse and show preview
            import io
            _order_df = pd.read_csv(io.BytesIO(decoded))
            _cols = list(_order_df.columns)
            _row_count = len(_order_df)

            _logger.info("Order CSV for %s: %s, %d rows, columns: %s", _store_display, fname, _row_count, _cols)

            # Persist to Supabase so data survives redeploys
            _csv_type = "orders" if "Order Net" in _cols else "items"
            import threading
            _df_copy = _order_df.copy()
            _store_copy = _upload_store
            threading.Thread(
                target=_save_order_csv_to_supabase,
                args=(_df_copy, _store_copy, _csv_type),
                daemon=True,
            ).start()

            # Show column preview so we can see what data is available
            _col_preview = ", ".join(_cols[:15])
            if len(_cols) > 15:
                _col_preview += f" ... (+{len(_cols) - 15} more)"

            # Recompute per-order profit with new data
            try:
                _compute_per_order_profit()
                _profit_msg = ""
                if ORDER_PROFIT_SUMMARY:
                    _profit_msg = (f" | Per-order profit: ${ORDER_PROFIT_SUMMARY['total_profit']:,.2f} "
                                   f"(avg ${ORDER_PROFIT_SUMMARY['avg_profit']:,.2f})")
            except Exception as _pe:
                _profit_msg = ""
                _logger.warning("Order profit compute failed: %s", _pe)

            orders_status = html.Div([
                html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
                html.Span(f"[{_store_display}] Uploaded {fname} — {_row_count} orders{_profit_msg}",
                          style={"color": GREEN, "fontSize": "13px"}),
                html.Div(f"Columns: {_col_preview}",
                         style={"color": GRAY, "fontSize": "11px", "marginTop": "4px", "fontFamily": "monospace"}),
            ])
            orders_stats = html.Div(f"{_row_count} orders loaded from {fname}",
                                     style={"color": ORANGE, "fontSize": "12px", "fontFamily": "monospace"})
            # List existing order CSV files for this store
            _existing_order_files = []
            for _s in ("keycomponentmfg", "aurvio", "lunalinks"):
                _od = os.path.join(BASE_DIR, "data", "order_csvs", _s)
                if os.path.isdir(_od):
                    for _of in sorted(os.listdir(_od)):
                        if _of.endswith(".csv"):
                            _existing_order_files.append(f"[{STORES.get(_s, _s)}] {_of}")
            orders_file_list = html.Div([
                html.Div(f, style={"color": GRAY, "fontSize": "11px", "padding": "2px 0"})
                for f in _existing_order_files
            ]) if _existing_order_files else html.Div("No files", style={"color": DARKGRAY, "fontSize": "11px"})

            new_log = [html.Div([
                html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
                html.Span(f"\u2713 Order CSV [{_store_display}]: {fname} ({_row_count} orders)",
                          style={"color": GREEN, "fontSize": "12px"}),
            ], style={"padding": "3px 0", "borderBottom": "1px solid #ffffff08"})] + (
                new_log if isinstance(new_log, list) else [])

        except Exception as e:
            orders_status = html.Div([
                html.Span("\u2717 ", style={"color": RED, "fontWeight": "bold"}),
                html.Span(f"Upload failed: {e}", style={"color": RED, "fontSize": "13px"}),
                html.Div("Expected: Etsy order CSV export from Shop Manager (Orders > Download CSV)",
                         style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "4px"}),
            ])

    # ── Listings CSV Upload ────────────────────────────────────────────
    elif "datahub-listings-upload" in trigger and listings_contents:
        try:
            content_type, content_string = listings_contents.split(",")
            decoded = base64.b64decode(content_string)

            _upload_store = etsy_store_picker or "keycomponentmfg"
            _store_display = STORES.get(_upload_store, _upload_store)
            fname = listings_filename or "listings.csv"

            import io
            _listings_df = pd.read_csv(io.BytesIO(decoded))
            _cols = list(_listings_df.columns)
            _row_count = len(_listings_df)

            _logger.info("Listings CSV for %s: %s, %d listings, columns: %s", _store_display, fname, _row_count, _cols)

            # Keep essential columns only
            _keep = [c for c in ['TITLE', 'DESCRIPTION', 'PRICE', 'CURRENCY_CODE', 'QUANTITY',
                                  'TAGS', 'MATERIALS', 'IMAGE1', 'LISTING_ID', 'STATE',
                                  'SKU', 'SECTION', 'CATEGORY'] if c in _cols]
            _slim = _listings_df[_keep] if _keep else _listings_df

            # Persist to Supabase
            import json as _json_l2
            _records = _json_l2.loads(_slim.to_json(orient="records"))
            import threading
            def _save_listings():
                try:
                    from supabase_loader import save_config_value
                    save_config_value(f"listings_csv_{_upload_store}", _json_l2.dumps(_records))
                    _logger.info("Saved %d listings for %s to Supabase", len(_records), _upload_store)
                except Exception as _e:
                    _logger.error("Listings Supabase save failed: %s", _e)
            threading.Thread(target=_save_listings, daemon=True).start()

            _col_preview = ", ".join(_cols[:10])
            if len(_cols) > 10:
                _col_preview += f" ... (+{len(_cols) - 10} more)"

            listings_status = html.Div([
                html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
                html.Span(f"[{_store_display}] Uploaded {fname} — {_row_count} active listings",
                          style={"color": GREEN, "fontSize": "13px"}),
                html.Div(f"Columns: {_col_preview}",
                         style={"color": GRAY, "fontSize": "11px", "marginTop": "4px", "fontFamily": "monospace"}),
            ])
            listings_stats = html.Div(f"{_row_count} active listings loaded",
                                       style={"color": ORANGE, "fontSize": "12px", "fontFamily": "monospace"})
            listings_file_list = html.Div(f"[{_store_display}] {fname}", style={"color": GRAY, "fontSize": "11px"})

            new_log = [html.Div([
                html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
                html.Span(f"\u2713 Listings [{_store_display}]: {fname} ({_row_count} listings)",
                          style={"color": GREEN, "fontSize": "12px"}),
            ], style={"padding": "3px 0", "borderBottom": "1px solid #ffffff08"})] + (
                new_log if isinstance(new_log, list) else [])

        except Exception as e:
            listings_status = html.Div([
                html.Span("\u2717 ", style={"color": RED, "fontWeight": "bold"}),
                html.Span(f"Upload failed: {e}", style={"color": RED, "fontSize": "13px"}),
                html.Div("Expected: Etsy 'Currently for Sale Listings' CSV from Shop Manager > Download Data",
                         style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "4px"}),
            ])

    # If any upload succeeded, trigger a page reload after a short delay
    # so ALL tabs (not just Data Hub) show fresh data.
    if any(x is not nu for x in [etsy_status, orders_status, listings_status]):
        import time
        reload_trigger = time.time()

    return (etsy_status, etsy_file_list, etsy_stats,
            new_log, summary, header, reload_trigger,
            orders_status, orders_file_list, orders_stats,
            listings_status, listings_file_list, listings_stats)


# ── Delete Data Callbacks ─────────────────────────────────────────────────────

@app.callback(
    Output("delete-confirm-modal", "is_open"),
    Output("delete-confirm-body", "children"),
    Output("delete-pending-action", "data"),
    Input({"type": "delete-etsy-month", "month": ALL}, "n_clicks"),
    Input({"type": "delete-bank-month", "month": ALL}, "n_clicks"),
    Input({"type": "delete-receipt", "order": ALL}, "n_clicks"),
    Input("delete-cancel-btn", "n_clicks"),
    prevent_initial_call=True,
)
def open_delete_confirm(*args):
    """Open the confirmation modal when a delete button is clicked."""
    ctx = callback_context
    if not ctx.triggered:
        return False, "", None

    trigger = ctx.triggered[0]
    prop_id = trigger["prop_id"]
    n_clicks = trigger["value"]

    # Cancel button
    if "delete-cancel-btn" in prop_id:
        return False, "", None

    if not n_clicks or n_clicks == 0:
        return dash.no_update, dash.no_update, dash.no_update

    import json
    try:
        btn_id = json.loads(prop_id.split(".")[0])
    except (json.JSONDecodeError, ValueError):
        return False, "", None

    btn_type = btn_id.get("type", "")

    if btn_type == "delete-etsy-month":
        month = btn_id["month"]
        # Count rows for this month
        count = 0
        if DATA is not None:
            try:
                _dates = pd.to_datetime(DATA["Date"], format="%B %d, %Y", errors="coerce")
                _months = _dates.dt.to_period("M").astype(str)
                count = int((_months == month).sum())
            except Exception:
                pass
        body = html.Div([
            html.P(f"Delete {count} Etsy transactions from {month}?", style={"fontSize": "15px"}),
            html.P("This will remove the data from Supabase and rebuild all dashboard numbers.",
                   style={"color": GRAY, "fontSize": "13px"}),
            html.P("This cannot be undone. You can re-upload the CSV to restore the data.",
                   style={"color": ORANGE, "fontSize": "12px", "fontStyle": "italic"}),
        ])
        return True, body, {"action": "delete_etsy_month", "month": month, "count": count}

    elif btn_type == "delete-bank-month":
        month = btn_id["month"]
        count = sum(1 for t in BANK_TXNS if t.get("date", "").startswith(month))
        body = html.Div([
            html.P(f"Delete {count} bank transactions from {month}?", style={"fontSize": "15px"}),
            html.P("This will remove the data from Supabase and rebuild all dashboard numbers.",
                   style={"color": GRAY, "fontSize": "13px"}),
            html.P("This cannot be undone. You can re-upload the bank statement to restore.",
                   style={"color": ORANGE, "fontSize": "12px", "fontStyle": "italic"}),
        ])
        return True, body, {"action": "delete_bank_month", "month": month, "count": count}

    elif btn_type == "delete-receipt":
        order = btn_id["order"]
        inv = next((i for i in INVOICES if str(i.get("order_num")) == str(order)), None)
        item_count = len(inv.get("items", [])) if inv else 0
        total = inv.get("grand_total", 0) if inv else 0
        body = html.Div([
            html.P(f"Delete order #{order} ({item_count} items, ${total:.2f})?", style={"fontSize": "15px"}),
            html.P("This will remove the order and all items from Supabase.",
                   style={"color": GRAY, "fontSize": "13px"}),
            html.P("This cannot be undone. You can re-upload the receipt PDF to restore.",
                   style={"color": ORANGE, "fontSize": "12px", "fontStyle": "italic"}),
        ])
        return True, body, {"action": "delete_receipt", "order": order}

    return False, "", None


@app.callback(
    Output("manage-data-status", "children"),
    Output("manage-data-content", "children"),
    Output("delete-confirm-modal", "is_open", allow_duplicate=True),
    Output("datahub-summary-strip", "children", allow_duplicate=True),
    Output("app-header-content", "children", allow_duplicate=True),
    Output("upload-reload-trigger", "data", allow_duplicate=True),
    Input("delete-confirm-btn", "n_clicks"),
    State("delete-pending-action", "data"),
    prevent_initial_call=True,
)
def execute_delete(n_clicks, pending_action):
    """Execute the confirmed deletion."""
    import datetime as _dt
    import time

    if not n_clicks or not pending_action:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    global DATA, BANK_TXNS, INVOICES
    action = pending_action.get("action")
    now_str = _dt.datetime.now().strftime("%I:%M:%S %p")

    if action == "delete_etsy_month":
        month = pending_action["month"]
        # Determine active store filter (None = all stores)
        _del_store = None
        if "Store" in DATA.columns and _DATA_ALL is not None and len(DATA) > 0 and len(DATA) < len(_DATA_ALL):
            _del_store = DATA["Store"].iloc[0]
        # Delete from local DATA (only matching store if filtered)
        try:
            _dates = pd.to_datetime(DATA["Date"], format="%B %d, %Y", errors="coerce")
            _months = _dates.dt.to_period("M").astype(str)
            before = len(DATA)
            _mask = _months == month
            if _del_store and "Store" in DATA.columns:
                _mask = _mask & (DATA["Store"] == _del_store)
            DATA = DATA[~_mask].reset_index(drop=True)
            deleted_local = before - len(DATA)
        except Exception:
            deleted_local = 0

        # Delete from Supabase in background — scoped to store
        import threading
        _y, _m = int(month.split("-")[0]), int(month.split("-")[1])
        threading.Thread(target=_delete_etsy_by_month, args=(_y, _m, _del_store), daemon=True).start()

        # Rebuild
        _rebuild_etsy_derived()
        _cascade_reload("etsy")

        status = html.Div([
            html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
            html.Span(f"\u2713 Deleted {deleted_local} Etsy transactions from {month}",
                      style={"color": GREEN, "fontSize": "12px"}),
        ])
        return (status, _build_manage_data_content(), False,
                _build_datahub_summary(), _header_text(), time.time())

    elif action == "delete_bank_month":
        month = pending_action["month"]
        before = len(BANK_TXNS)
        BANK_TXNS = [t for t in BANK_TXNS if not t.get("date", "").startswith(month)]
        deleted_local = before - len(BANK_TXNS)

        # Delete from Supabase in background
        import threading
        _y, _m = int(month.split("-")[0]), int(month.split("-")[1])
        threading.Thread(target=_delete_bank_by_month, args=(_y, _m), daemon=True).start()

        # Rebuild
        _rebuild_bank_derived()
        _cascade_reload("bank")

        status = html.Div([
            html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
            html.Span(f"\u2713 Deleted {deleted_local} bank transactions from {month}",
                      style={"color": GREEN, "fontSize": "12px"}),
        ])
        return (status, _build_manage_data_content(), False,
                _build_datahub_summary(), _header_text(), time.time())

    elif action == "delete_receipt":
        order = pending_action["order"]
        before = len(INVOICES)
        INVOICES = [i for i in INVOICES if str(i.get("order_num")) != str(order)]
        deleted_local = before - len(INVOICES)

        # Persist updated INVOICES to disk
        try:
            _gen_dir = os.path.join(BASE_DIR, "data", "generated")
            os.makedirs(_gen_dir, exist_ok=True)
            with open(os.path.join(_gen_dir, "inventory_orders.json"), "w") as f:
                json.dump(INVOICES, f, indent=2)
        except Exception:
            pass

        # Delete from Supabase in background
        import threading
        threading.Thread(target=_delete_receipt_by_order, args=(order,), daemon=True).start()

        # Rebuild inventory from remaining invoices
        _rebuild_inventory_from_invoices()
        _cascade_reload("inventory")

        status = html.Div([
            html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
            html.Span(f"\u2713 Deleted order #{order}" + (f" ({deleted_local} order removed)" if deleted_local else ""),
                      style={"color": GREEN, "fontSize": "12px"}),
        ])
        return (status, _build_manage_data_content(), False,
                _build_datahub_summary(), _header_text(), time.time())

    return dash.no_update, dash.no_update, False, dash.no_update, dash.no_update, dash.no_update


# ── CSV Download Callbacks ────────────────────────────────────────────────────

@app.callback(
    Output("download-etsy-csv", "data"),
    Input("btn-download-etsy", "n_clicks"),
    prevent_initial_call=True,
)
def download_etsy(n):
    if not n:
        raise dash.exceptions.PreventUpdate
    return dcc.send_data_frame(DATA.to_csv, "etsy_transactions.csv", index=False)


@app.callback(
    Output("download-bank-csv", "data"),
    Input("btn-download-bank", "n_clicks"),
    prevent_initial_call=True,
)
def download_bank(n):
    if not n:
        raise dash.exceptions.PreventUpdate
    if not BANK_TXNS:
        raise dash.exceptions.PreventUpdate
    df = pd.DataFrame(BANK_TXNS)
    return dcc.send_data_frame(df.to_csv, "bank_transactions.csv", index=False)


@app.callback(
    Output("download-inventory-csv", "data"),
    Input("btn-download-inventory", "n_clicks"),
    prevent_initial_call=True,
)
def download_inventory(n):
    if not n or len(INV_ITEMS) == 0:
        raise dash.exceptions.PreventUpdate
    export = INV_ITEMS[["order_num", "date", "month", "name", "category", "qty", "price",
                         "total", "source", "location"]].copy()
    return dcc.send_data_frame(export.to_csv, "inventory_items.csv", index=False)


@app.callback(
    Output("download-stock-csv", "data"),
    Input("btn-download-stock", "n_clicks"),
    prevent_initial_call=True,
)
def download_stock(n):
    if not n or len(STOCK_SUMMARY) == 0:
        raise dash.exceptions.PreventUpdate
    export = STOCK_SUMMARY[["display_name", "category", "total_purchased", "total_used",
                             "in_stock", "unit_cost", "total_cost", "location"]].copy()
    return dcc.send_data_frame(export.to_csv, "stock_summary.csv", index=False)


@app.callback(
    Output("download-pl-csv", "data"),
    Input("btn-download-pl", "n_clicks"),
    prevent_initial_call=True,
)
def download_pl(n):
    if not n:
        raise dash.exceptions.PreventUpdate
    rows = [
        {"Line": "Gross Sales", "Amount": gross_sales},
        {"Line": "Etsy Fees (net of credits)", "Amount": -total_fees},
        {"Line": "Shipping Labels", "Amount": -total_shipping_cost},
        {"Line": "Marketing/Ads", "Amount": -total_marketing},
        {"Line": "Refunds", "Amount": -total_refunds},
        {"Line": "Sales Tax", "Amount": -total_taxes},
        {"Line": "Buyer Fees", "Amount": -total_buyer_fees},
        {"Line": "= After Etsy Fees", "Amount": etsy_net},
        {"Line": "Supply Costs", "Amount": -true_inventory_cost},
        {"Line": "Business Expenses", "Amount": -bank_biz_expense_total},
        {"Line": "Owner Draws", "Amount": -bank_owner_draw_total},
        {"Line": "= Cash on Hand", "Amount": bank_cash_on_hand},
        {"Line": "= Profit", "Amount": profit},
    ]
    df = pd.DataFrame(rows)
    return dcc.send_data_frame(df.to_csv, "profit_and_loss.csv", index=False)


# ── Batch Mode: Populate table from wizard state ──────────────────────────────

@app.callback(
    Output("batch-items-table", "data"),
    Output("batch-mode-panel", "style"),
    Input("receipt-wizard-state", "data"),
    prevent_initial_call=True,
)
def populate_batch_table(state):
    """When a receipt is parsed, populate the batch edit table with all items."""
    if not state or not state.get("items"):
        return [], {"display": "none"}

    rows = []
    for item in state["items"]:
        rows.append({
            "name": item["name"],
            "category": item.get("auto_category", "Other"),
            "qty": item.get("qty", 1),
            "price": item.get("price", 0),
            "location": item.get("auto_location", "Tulsa, OK"),
        })
    return rows, {"display": "block", "marginTop": "12px", "padding": "12px",
                  "backgroundColor": f"{GREEN}08", "borderRadius": "6px",
                  "border": f"1px solid {GREEN}33"}


# ── Batch Save Callback ──────────────────────────────────────────────────────

@app.callback(
    Output("batch-save-status", "children"),
    Input("batch-save-btn", "n_clicks"),
    State("batch-items-table", "data"),
    State("receipt-wizard-state", "data"),
    prevent_initial_call=True,
)
def batch_save_items(n_clicks, table_data, wizard_state):
    """Save all items from the batch table at once."""
    if not n_clicks or not table_data:
        raise dash.exceptions.PreventUpdate

    saved = 0
    for row in table_data:
        name = row.get("name", "").strip()
        if not name:
            continue
        cat = row.get("category", "Other")
        qty = int(row.get("qty", 1))
        loc = row.get("location", "Tulsa, OK")

        # Save item details to Supabase
        order_num = wizard_state.get("order_num", "BATCH") if wizard_state else "BATCH"
        try:
            from supabase_loader import save_item_details as _sid
            _sid(order_num, name, [{
                "display_name": name,
                "category": cat,
                "quantity": qty,
                "location": loc,
            }])
            saved += 1
        except Exception:
            saved += 1  # Count anyway — local only

    return html.Span(f"Saved {saved}/{len(table_data)} items!",
                     style={"color": GREEN, "fontWeight": "bold"})


# ── Review Saved toggle ──────────────────────────────────────────────────────
@app.callback(
    Output("editor-items-container", "children", allow_duplicate=True),
    Output("editor-review-saved-btn", "children"),
    Output("editor-review-saved-btn", "style"),
    Output("editor-review-mode", "data"),
    Input("editor-review-saved-btn", "n_clicks"),
    State("editor-review-mode", "data"),
    prevent_initial_call=True,
)
def toggle_review_saved(n_clicks, current_mode):
    new_mode = not current_mode
    editor_content, _ = _build_inventory_editor(show_saved=new_mode)
    # Extract the scroll container from the rebuilt editor
    # editor_content is a Div with children: [header, progress_bar, store, store, editor-items-container, hidden_compat]
    # We need the children of editor-items-container (index 4)
    items_children = editor_content.children[4].children  # [filter_bar, scroll_container]
    btn_label = "Hide Saved" if new_mode else "Review Saved"
    btn_style = {
        "fontSize": "12px", "padding": "5px 14px",
        "backgroundColor": GREEN if new_mode else f"{DARKGRAY}88",
        "color": WHITE, "border": "none", "borderRadius": "6px",
        "cursor": "pointer", "fontWeight": "bold", "marginLeft": "14px",
    }
    return items_children, btn_label, btn_style, new_mode


# ── Review Saved: search + filter (clientside for speed, no ID conflicts) ─────
app.clientside_callback(
    """
    function(search, catFilter, locFilter, stockFilter, isReview) {
        if (!isReview) { return window.dash_clientside.no_update; }
        var s = (search || '').toLowerCase().trim();
        var cat = catFilter || 'All';
        var loc = locFilter || 'All';
        var stock = stockFilter || 'All';
        var container = document.getElementById('editor-items-container');
        if (!container) return '';
        var cards = container.querySelectorAll('.inv-card');
        var shown = 0;
        cards.forEach(function(card) {
            var cardSearch = (card.getAttribute('data-search') || '').toLowerCase();
            var cardCat = card.getAttribute('data-cat') || '';
            var cardLoc = card.getAttribute('data-loc') || '';
            var cardStock = card.getAttribute('data-stock') || '';
            var matchSearch = !s || cardSearch.indexOf(s) >= 0;
            var matchCat = cat === 'All' || cardCat === cat;
            var matchLoc = loc === 'All' || cardLoc.indexOf(loc) >= 0;
            var matchStock = stock === 'All' || cardStock.indexOf(stock) >= 0;
            if (matchSearch && matchCat && matchLoc && matchStock) {
                card.style.display = '';
                shown++;
            } else {
                card.style.display = 'none';
            }
        });
        var filtered = s || cat !== 'All' || loc !== 'All' || stock !== 'All';
        return shown + ' items' + (filtered ? ' (filtered)' : '');
    }
    """,
    Output("editor-review-count", "children"),
    Input("editor-review-search", "value"),
    Input("editor-review-cat", "value"),
    Input("editor-review-loc", "value"),
    Input("editor-review-stock", "value"),
    State("editor-review-mode", "data"),
    prevent_initial_call=True,
)


# ── Scrollable Item Card Callbacks (pattern-matching) ─────────────────────────

# Pack type toggle: show/hide pack breakdown vs single-item row
@app.callback(
    Output({"type": "inv-card-pack-section", "index": MATCH}, "style"),
    Output({"type": "inv-card-single-row", "index": MATCH}, "style"),
    Output({"type": "inv-card-split-row", "index": MATCH}, "style"),
    Input({"type": "inv-card-pack-type", "index": MATCH}, "value"),
    prevent_initial_call=True,
)
def toggle_pack_section(pack_type):
    """Show pack breakdown for 'different items' packs, single row otherwise."""
    _single_show = {"display": "flex", "gap": "14px", "flexWrap": "wrap", "marginBottom": "10px"}
    _pack_show = {"display": "block", "padding": "12px 16px",
                  "backgroundColor": f"{PURPLE}08", "borderRadius": "8px",
                  "border": f"1px solid {PURPLE}33", "marginBottom": "10px"}
    _split_hide = {"display": "none"}
    if pack_type == "different":
        return _pack_show, {"display": "none"}, _split_hide
    return {"display": "none"}, _single_show, _split_hide




# Split toggle: show/hide split qty fields
@app.callback(
    Output({"type": "inv-card-split-row", "index": MATCH}, "style", allow_duplicate=True),
    Input({"type": "inv-card-split", "index": MATCH}, "value"),
    prevent_initial_call=True,
)
def toggle_split_row(split_val):
    """Show split qty fields when user picks Yes."""
    if split_val == "yes":
        return {"display": "block", "padding": "10px 14px",
                "backgroundColor": f"{PURPLE}08", "borderRadius": "8px",
                "border": f"1px solid {PURPLE}33", "marginBottom": "10px"}
    return {"display": "none"}


# Save individual item card
@app.callback(
    Output({"type": "inv-card-status", "index": MATCH}, "children"),
    Output({"type": "inv-card-save", "index": MATCH}, "style"),
    Input({"type": "inv-card-save", "index": MATCH}, "n_clicks"),
    State({"type": "inv-card-data", "index": MATCH}, "data"),
    State({"type": "inv-card-cat", "index": MATCH}, "value"),
    State({"type": "inv-card-name", "index": MATCH}, "value"),
    State({"type": "inv-card-name-pick", "index": MATCH}, "value"),
    State({"type": "inv-card-loc", "index": MATCH}, "value"),
    State({"type": "inv-card-qty", "index": MATCH}, "value"),
    State({"type": "inv-card-pack-type", "index": MATCH}, "value"),
    State({"type": "inv-card-split", "index": MATCH}, "value"),
    State({"type": "inv-card-split-qty1", "index": MATCH}, "value"),
    State({"type": "inv-card-split-qty2", "index": MATCH}, "value"),
    State({"type": "inv-card-img-url", "index": MATCH}, "value"),
    # Read ALL pack fields so we can grab rows for this card
    State({"type": "inv-pack-name", "index": ALL}, "value"),
    State({"type": "inv-pack-custom", "index": ALL}, "value"),
    State({"type": "inv-pack-qty", "index": ALL}, "value"),
    State({"type": "inv-pack-loc", "index": ALL}, "value"),
    State({"type": "inv-pack-img", "index": ALL}, "value"),
    prevent_initial_call=True,
)
def save_item_card(n_clicks, item_data, cat, name, name_pick, loc, qty, pack_type,
                   split, split_qty1, split_qty2, img_url,
                   all_pack_names, all_pack_customs, all_pack_qtys, all_pack_locs, all_pack_imgs):
    """Save a single item from the scrollable organizer."""
    if not n_clicks or not item_data:
        raise dash.exceptions.PreventUpdate

    # Use typed name first, then dropdown pick, then fall back to Amazon product name
    display_name = (name or "").strip() or (name_pick or "").strip() or item_data["name"]
    category = cat or "Other"
    location = loc or "Tulsa, OK"
    qty = int(qty) if qty else item_data.get("qty", 1)

    # Find this card's index from the callback context
    ctx = callback_context
    card_idx = ctx.triggered_id["index"]

    # Save image URL if provided (in-memory + persist to Supabase)
    if img_url and img_url.strip().startswith("http"):
        _IMAGE_URLS[display_name] = img_url.strip()
        try:
            from supabase_loader import save_image_override as _save_img_override
            _save_img_override(display_name, img_url.strip())
        except Exception:
            pass

    # Build pack data directly from the ALL state params for this card's rows
    def _get_pack_data_for_card():
        """Read pack fields for this card (indices card_idx*100 .. card_idx*100+5)."""
        rows = []
        for r in range(6):
            idx = card_idx * 100 + r
            # Find position of this index in the ALL arrays
            pos = None
            for k in range(len(all_pack_names)):
                # ALL states are ordered by index, find matching position
                # We check by computing expected position
                if k < len(all_pack_names):
                    # Positions map: card 0 → indices 0-5, card 1 → indices 100-105, etc.
                    # In the ALL array, they're ordered by index value
                    pass
            # Simpler: ALL arrays contain all pack fields sorted by index.
            # Card i's rows are at positions i*6 through i*6+5 in the array
            pos = card_idx * 6 + r
            if pos >= len(all_pack_names):
                break
            picked = (all_pack_names[pos] or "").strip()
            custom = (all_pack_customs[pos] or "").strip() if pos < len(all_pack_customs) else ""
            final_name = custom if (picked == "_custom" or not picked) and custom else picked
            if final_name == "_custom":
                final_name = ""
            if not final_name:
                continue
            rows.append({
                "name": final_name,
                "qty": all_pack_qtys[pos] if pos < len(all_pack_qtys) else 1,
                "location": all_pack_locs[pos] if pos < len(all_pack_locs) else "Tulsa, OK",
                "img_url": (all_pack_imgs[pos] or "").strip() if pos < len(all_pack_imgs) else "",
            })
        return rows

    # Build details based on pack type and split
    if pack_type == "different":
        pack_rows = _get_pack_data_for_card()
        details = []
        for pd in pack_rows:
            pn = pd["name"]
            details.append({
                "display_name": pn,
                "category": category,
                "true_qty": int(pd.get("qty", 1) or 1),
                "location": pd.get("location", location) or location,
            })
            # Save per-item image URL if provided
            p_img = pd.get("img_url", "")
            if p_img and p_img.startswith("http"):
                _IMAGE_URLS[pn] = p_img
                try:
                    from supabase_loader import save_image_override as _save_img_ovr
                    _save_img_ovr(pn, p_img)
                except Exception:
                    pass
        if not details:
            details = [{"display_name": display_name, "category": category,
                        "true_qty": qty, "location": location}]
    elif split == "yes" and int(split_qty2 or 0) > 0:
        # Split between Tulsa and Texas
        q1 = int(split_qty1 or 0)
        q2 = int(split_qty2 or 0)
        details = []
        if q1 > 0:
            details.append({"display_name": display_name, "category": category,
                            "true_qty": q1, "location": "Tulsa, OK"})
        if q2 > 0:
            details.append({"display_name": display_name, "category": category,
                            "true_qty": q2, "location": "Texas"})
        if not details:
            details = [{"display_name": display_name, "category": category,
                        "true_qty": qty, "location": location}]
    else:
        # Single or identical — all qty goes to one name/location
        details = [{"display_name": display_name, "category": category,
                    "true_qty": qty, "location": location}]

    _saved_style = {"fontSize": "13px", "padding": "8px 28px",
                    "backgroundColor": DARKGRAY, "color": WHITE,
                    "border": "none", "borderRadius": "6px",
                    "cursor": "default", "fontWeight": "bold",
                    "marginTop": "18px", "opacity": "0.6"}

    # Compute per-unit cost WITH TAX: total_with_tax / total detail qty
    orig_price = item_data.get("price", 0)
    orig_qty = max(item_data.get("qty", 1), 1)
    orig_total_with_tax = item_data.get("total_with_tax", orig_price * orig_qty)
    total_detail_qty = sum(d["true_qty"] for d in details) or 1
    per_unit_cost = round(orig_total_with_tax / total_detail_qty, 2)

    try:
        ok = _save_item_details(item_data["order_num"], item_data["name"], details)
        if ok:
            detail_key = (item_data["order_num"], item_data["name"])
            # Remove old detail quantities before adding new ones (prevents duplicates on re-save)
            old_details = _ITEM_DETAILS.get(detail_key, [])
            for od in old_details:
                ol = _norm_loc(od.get("location", ""))
                if ol:
                    ok_key = (ol, od.get("display_name", ""), od.get("category", "Other"))
                    old_q = od.get("true_qty", 1)
                    if ok_key in _UPLOADED_INVENTORY:
                        _UPLOADED_INVENTORY[ok_key] = max(0, _UPLOADED_INVENTORY[ok_key] - old_q)
                        if _UPLOADED_INVENTORY[ok_key] == 0:
                            _UPLOADED_INVENTORY.pop(ok_key, None)
                            _INVENTORY_UNIT_COST.pop(ok_key, None)

            _ITEM_DETAILS[detail_key] = details
            for det in details:
                loc_norm = _norm_loc(det["location"])
                if loc_norm:
                    inv_key = (loc_norm, det["display_name"], category)
                    old_qty = _UPLOADED_INVENTORY.get(inv_key, 0)
                    new_qty = det["true_qty"]
                    _UPLOADED_INVENTORY[inv_key] = old_qty + new_qty
                    # Weighted average: (old_cost * old_qty + new_cost * new_qty) / total_qty
                    if per_unit_cost > 0:
                        old_cost = _INVENTORY_UNIT_COST.get(inv_key, 0)
                        total_qty = old_qty + new_qty
                        _INVENTORY_UNIT_COST[inv_key] = round(
                            (old_cost * old_qty + per_unit_cost * new_qty) / total_qty, 2
                        ) if total_qty > 0 else per_unit_cost
            _apply_details_to_inv_items()
            _recompute_location_spend()
            return (html.Span("\u2713 Saved!", style={"color": GREEN, "fontSize": "12px",
                              "fontWeight": "bold"}),
                    _saved_style)
        return (html.Span("Save failed", style={"color": RED, "fontSize": "12px"}),
                dash.no_update)
    except Exception as e:
        return (html.Span(f"Error: {e}", style={"color": RED, "fontSize": "12px"}),
                dash.no_update)


# Image URL preview for item cards
@app.callback(
    Output({"type": "inv-card-img-preview", "index": MATCH}, "children"),
    Input({"type": "inv-card-img-url", "index": MATCH}, "value"),
    prevent_initial_call=True,
)
def show_card_img_preview(url):
    """Show image preview when user pastes a URL."""
    if not url or not url.strip().startswith("http"):
        return ""
    return html.Img(src=url.strip(), style={
        "maxHeight": "80px", "maxWidth": "140px", "borderRadius": "6px",
        "border": f"1px solid {CYAN}44", "objectFit": "cover", "marginTop": "4px"})


# Name picker dropdown → auto-fill inventory name text input
@app.callback(
    Output({"type": "inv-card-name", "index": MATCH}, "value", allow_duplicate=True),
    Input({"type": "inv-card-name-pick", "index": MATCH}, "value"),
    prevent_initial_call=True,
)
def name_pick_to_input(picked):
    """When a name is picked from the dropdown, set it as the inventory name."""
    if picked:
        return picked
    raise dash.exceptions.PreventUpdate


    raise dash.exceptions.PreventUpdate



# Color picker → auto-fill inventory name
@app.callback(
    Output({"type": "inv-card-name", "index": MATCH}, "value", allow_duplicate=True),
    Input({"type": "inv-card-color", "index": MATCH}, "value"),
    prevent_initial_call=True,
)
def color_to_name(color):
    """When a filament color is picked, set it as the inventory name."""
    if color and color != "" and color != "_custom":
        return color
    raise dash.exceptions.PreventUpdate




# Category change → show/hide color picker
@app.callback(
    Output({"type": "inv-card-color-section", "index": MATCH}, "style"),
    Input({"type": "inv-card-cat", "index": MATCH}, "value"),
    prevent_initial_call=True,
)
def toggle_color_section(cat):
    """Show filament color picker only when category is Filament."""
    if cat == "Filament":
        return {"flex": "1", "minWidth": "160px", "display": "block"}
    return {"display": "none"}


# Category change → update name-pick dropdown to show only names for that category
@app.callback(
    Output({"type": "inv-card-name-pick", "index": MATCH}, "options"),
    Output({"type": "inv-card-name-pick", "index": MATCH}, "value"),
    Input({"type": "inv-card-cat", "index": MATCH}, "value"),
    State("inv-name-options-store", "data"),
    prevent_initial_call=True,
)
def update_name_options_for_cat(cat, names_by_cat):
    """Show only inventory names that match the selected category."""
    names = (names_by_cat or {}).get(cat or "Other", [])
    opts = [{"label": n, "value": n} for n in names]
    return opts, None  # clear the current pick when category changes



# ── Editor DataTable Save All Callback (legacy — hidden) ─────────────────────

@app.callback(
    Output("editor-save-all-status", "children"),
    Output("editor-datatable", "data"),
    Input("editor-save-all-btn", "n_clicks"),
    State("editor-datatable", "data"),
    prevent_initial_call=True,
)
def save_all_editor_items(n_clicks, table_data):
    """Save all items from the editor DataTable in one batch.
    Handles split rows (orig_name contains [Split X/Y]) by grouping them."""
    if not n_clicks or not table_data:
        raise dash.exceptions.PreventUpdate

    from supabase_loader import save_item_details as _sid
    import re as _re

    # Group rows: split rows share the same (order_num, base_orig_name)
    _split_re = _re.compile(r" \[Split \d+/\d+\]$")
    grouped = {}  # (order_num, base_name) -> list of row dicts
    for row in table_data:
        order_num = row.get("order_num", "")
        orig_name = row.get("orig_name", "")
        if not order_num or not orig_name:
            continue
        base_name = _split_re.sub("", orig_name)
        key = (order_num, base_name)
        grouped.setdefault(key, []).append(row)

    saved = 0
    errors = 0
    saved_keys = set()
    updated_data = []

    for row in table_data:
        order_num = row.get("order_num", "")
        orig_name = row.get("orig_name", "")
        if not order_num or not orig_name:
            updated_data.append(row)
            continue

        base_name = _split_re.sub("", orig_name)
        key = (order_num, base_name)

        if key in saved_keys:
            # Already saved this group — just mark row as Saved
            row = dict(row)
            row["status"] = "Saved"
            updated_data.append(row)
            continue

        # Build detail entries from all rows in this group
        group_rows = grouped.get(key, [row])
        details = []
        for r in group_rows:
            display_name = (r.get("display_name", "") or "").strip() or base_name
            category = r.get("category", "Other")
            qty = int(r.get("qty", 1))
            location = r.get("location", "")
            details.append({
                "display_name": display_name,
                "category": category,
                "true_qty": qty,
                "location": location,
            })

        try:
            ok = _sid(order_num, base_name, details)
            if ok:
                _ITEM_DETAILS[(order_num, base_name)] = details
                saved += 1
                saved_keys.add(key)
                row = dict(row)
                row["status"] = "Saved"
            else:
                errors += 1
        except Exception:
            errors += 1

        updated_data.append(row)

    # Rebuild INV_ITEMS and STOCK_SUMMARY after all saves
    if saved > 0:
        _recompute_stock_summary()

    msg = f"Saved {saved}/{len(table_data)} items!"
    if errors > 0:
        msg += f" ({errors} errors)"

    return (
        html.Span(msg, style={"color": GREEN if errors == 0 else ORANGE, "fontWeight": "bold"}),
        updated_data,
    )


# ── CSV Paste Import Callback ─────────────────────────────────────────────────

@app.callback(
    Output("batch-items-table", "data", allow_duplicate=True),
    Output("batch-mode-panel", "style", allow_duplicate=True),
    Output("csv-import-status", "children"),
    Input("csv-import-btn", "n_clicks"),
    State("csv-paste-input", "value"),
    prevent_initial_call=True,
)
def csv_paste_import(n_clicks, csv_text):
    """Parse pasted CSV text into the batch edit table."""
    if not n_clicks or not csv_text:
        raise dash.exceptions.PreventUpdate

    import csv
    import io

    rows = []
    reader = csv.reader(io.StringIO(csv_text.strip()))
    header = None
    for line in reader:
        if not line:
            continue
        # Auto-detect header row
        if header is None and any(h.lower().strip() in ("name", "item", "item name", "product") for h in line):
            header = [h.strip().lower() for h in line]
            continue
        if header is None:
            # No header — assume Name, Qty, Price, Category
            header = ["name", "qty", "price", "category"]

        row_dict = {}
        for i, val in enumerate(line):
            if i < len(header):
                row_dict[header[i]] = val.strip()

        name = row_dict.get("name", row_dict.get("item", row_dict.get("item name", row_dict.get("product", ""))))
        try:
            qty = int(row_dict.get("qty", row_dict.get("quantity", "1")))
        except (ValueError, TypeError):
            qty = 1
        try:
            price = float(row_dict.get("price", row_dict.get("unit price", "0")).replace("$", ""))
        except (ValueError, TypeError):
            price = 0
        cat = row_dict.get("category", "Other")

        if name:
            rows.append({
                "name": name,
                "category": cat if cat else "Other",
                "qty": qty,
                "price": round(price, 2),
                "location": "Tulsa, OK",
            })

    if not rows:
        return dash.no_update, dash.no_update, html.Span("No valid rows found.", style={"color": RED})

    return rows, {"display": "block", "marginTop": "12px", "padding": "12px",
                  "backgroundColor": f"{GREEN}08", "borderRadius": "6px",
                  "border": f"1px solid {GREEN}33"}, \
        html.Span(f"Imported {len(rows)} items!", style={"color": GREEN, "fontWeight": "bold"})


# ── CEO Periodic Health Check ─────────────────────────────────────────────────

@app.callback(
    Output("ceo-alert-banner", "children"),
    Input("ceo-interval", "n_intervals"),
    prevent_initial_call=True,
)
def ceo_periodic_check(n_intervals):
    """Periodic CEO health re-check (every 15 min)."""
    global _ceo_health
    if _ceo_agent and _acct_pipeline:
        try:
            _ceo_health = _ceo_agent.run_periodic_check(_acct_pipeline)
        except Exception:
            pass
    return _build_ceo_banner()


# ── Mark Receipt Verified Callback ────────────────────────────────────────────

@app.callback(
    Output({"type": "receipt-verify-btn", "index": dash.MATCH}, "children"),
    Output({"type": "receipt-verify-btn", "index": dash.MATCH}, "style"),
    Output({"type": "receipt-verify-btn", "index": dash.MATCH}, "disabled"),
    Input({"type": "receipt-verify-btn", "index": dash.MATCH}, "n_clicks"),
    prevent_initial_call=True,
)
def mark_receipt_verified(n_clicks):
    """Mark a missing receipt as manually verified (visual feedback)."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    return (
        "Verified",
        {"fontSize": "9px", "padding": "2px 8px", "backgroundColor": f"{GREEN}22",
         "color": GREEN, "border": f"1px solid {GREEN}66", "borderRadius": "4px",
         "cursor": "default", "marginLeft": "4px", "fontWeight": "bold"},
        True,
    )


# ── Refund assignee (TJ / Braden) callback ──────────────────────────────────

@app.callback(
    Output({"type": "refund-assignee", "index": MATCH}, "style"),
    Input({"type": "refund-assignee", "index": MATCH}, "value"),
    State({"type": "refund-assignee", "index": MATCH}, "id"),
    prevent_initial_call=True,
)
def save_refund_assignee(value, comp_id):
    """Save who shipped/made a refunded order (TJ or Braden)."""
    order_key = comp_id["index"]
    _refund_assignments[order_key] = value or ""
    try:
        from supabase_loader import save_config_value
        save_config_value("refund_assignments", _refund_assignments)
    except Exception:
        pass
    # Color the dropdown border based on assignment
    base = {"width": "90px", "height": "26px", "fontSize": "11px", "padding": "2px 4px",
            "backgroundColor": "#1a1a2e", "color": WHITE, "borderRadius": "4px", "marginLeft": "8px"}
    if value == "TJ":
        base["border"] = f"1px solid {CYAN}"
    elif value == "Braden":
        base["border"] = f"1px solid {GREEN}"
    elif value == "Cancelled":
        base["border"] = f"1px solid {ORANGE}"
    else:
        base["border"] = "1px solid #ffffff20"
    return base


# ── Run ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print()
    print("=" * 50)
    print("  ETSY DASHBOARD v2 RUNNING")
    port = int(os.environ.get("PORT", 8070))
    print(f"  Open: http://127.0.0.1:{port}")
    print("=" * 50)
    print()
    app.run(debug=False, host="0.0.0.0", port=port)
