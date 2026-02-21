"""
Etsy Financial Dashboard v2 — Tabbed, Trend-Heavy, Deep Analytics
Run: python etsy_dashboard.py
Open: http://127.0.0.1:8070
"""

import dash
from dash import dcc, html, callback_context
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

# ── Theme ────────────────────────────────────────────────────────────────────

BG = "#0f0f1a"
CARD = "#16213e"
CARD2 = "#1a1a2e"
GREEN = "#2ecc71"
RED = "#e74c3c"
BLUE = "#3498db"
ORANGE = "#f39c12"
PURPLE = "#9b59b6"
TEAL = "#1abc9c"
PINK = "#e91e8f"
WHITE = "#ffffff"
GRAY = "#aaaaaa"
DARKGRAY = "#666666"
CYAN = "#00d4ff"

TAB_PADDING = "14px 16px"

CHART_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font={"color": WHITE},
    margin=dict(t=50, b=30, l=60, r=20),
)

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
)

RAILWAY_URL = os.environ.get("RAILWAY_URL", "https://web-production-7f385.up.railway.app")


def _notify_railway_reload():
    """Ping Railway's /api/reload in background after syncing data to Supabase."""
    import threading
    import urllib.request
    def _ping():
        try:
            urllib.request.urlopen(f"{RAILWAY_URL}/api/reload", timeout=30)
        except Exception:
            pass  # Railway might be down, not critical
    threading.Thread(target=_ping, daemon=True).start()


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
_sb = _load_data()
CONFIG = _sb["CONFIG"]
INVOICES = _sb["INVOICES"]

# Always re-parse Etsy data from local CSVs so new uploads are picked up immediately
import glob as _glob_mod
_etsy_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
_etsy_frames = []
if os.path.isdir(_etsy_dir):
    for _ef in sorted(_glob_mod.glob(os.path.join(_etsy_dir, "etsy_statement*.csv"))):
        try:
            _etsy_frames.append(pd.read_csv(_ef))
        except Exception:
            pass
if _etsy_frames:
    DATA = pd.concat(_etsy_frames, ignore_index=True)
    # Add computed columns (same as supabase_loader._add_computed_columns)
    def _pm(val):
        if pd.isna(val) or val == "--" or val == "":
            return 0.0
        val = str(val).replace("$", "").replace(",", "").replace('"', "")
        try:
            return float(val)
        except Exception:
            return 0.0
    DATA["Amount_Clean"] = DATA["Amount"].apply(_pm)
    DATA["Net_Clean"] = DATA["Net"].apply(_pm)
    DATA["Fees_Clean"] = DATA["Fees & Taxes"].apply(_pm)
    DATA["Date_Parsed"] = pd.to_datetime(DATA["Date"], format="%B %d, %Y", errors="coerce")
    DATA["Month"] = DATA["Date_Parsed"].dt.to_period("M").astype(str)
    DATA["Week"] = DATA["Date_Parsed"].dt.to_period("W").apply(lambda p: p.start_time)
    print(f"Loaded {len(DATA)} Etsy transactions from local CSVs")
else:
    DATA = _sb["DATA"]
    print("Using Etsy data from Supabase (no local CSVs found)")

# Auto-calculate Etsy balance from CSV deposit titles instead of stale config value
import re as _re_mod
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
_etsy_balance_auto = max(0, round(_etsy_all_net - _etsy_deposit_total, 2))

# Always re-parse bank data from local files (PDFs + CSVs) so new uploads are picked up
from _parse_bank_statements import parse_bank_pdf as _init_parse_bank
from _parse_bank_statements import parse_bank_csv as _init_parse_csv
from _parse_bank_statements import apply_overrides as _init_apply_overrides

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
            except Exception:
                pass
    _csv_txns = []
    _csv_cov = set()
    for _fn in sorted(os.listdir(_init_bank_dir)):
        if _fn.lower().endswith(".csv"):
            try:
                _txns, _cov = _init_parse_csv(os.path.join(_init_bank_dir, _fn))
                _csv_txns.extend(_txns)
                _csv_cov.update(_cov)
            except Exception:
                pass
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
BANK_TXNS = _init_bank_txns if _init_bank_txns else _sb["BANK_TXNS"]

# ── Extract config values ───────────────────────────────────────────────────
# Use auto-calculated Etsy balance from CSV data (not stale config value)
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
    if pd.isna(val) or val == "--" or val == "":
        return 0.0
    val = str(val).replace("$", "").replace(",", "").replace('"', "")
    try:
        return float(val)
    except Exception:
        return 0.0

# ── Pre-compute metrics ─────────────────────────────────────────────────────

sales_df = DATA[DATA["Type"] == "Sale"]
fee_df = DATA[DATA["Type"] == "Fee"]
ship_df = DATA[DATA["Type"] == "Shipping"]
mkt_df = DATA[DATA["Type"] == "Marketing"]
refund_df = DATA[DATA["Type"] == "Refund"]
tax_df = DATA[DATA["Type"] == "Tax"]
deposit_df = DATA[DATA["Type"] == "Deposit"]
buyer_fee_df = DATA[DATA["Type"] == "Buyer Fee"]

# Top-level numbers
gross_sales = sales_df["Net_Clean"].sum()
total_refunds = abs(refund_df["Net_Clean"].sum())
net_sales = gross_sales - total_refunds

total_fees = abs(fee_df["Net_Clean"].sum())
total_shipping_cost = abs(ship_df["Net_Clean"].sum())
total_marketing = abs(mkt_df["Net_Clean"].sum())
total_taxes = abs(tax_df["Net_Clean"].sum())

etsy_net = gross_sales - total_fees - total_shipping_cost - total_marketing - total_refunds
order_count = len(sales_df)
avg_order = gross_sales / order_count if order_count else 0
etsy_net_margin = (etsy_net / gross_sales * 100) if gross_sales else 0

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
try:
    _raw_details = _load_item_details()
    for d in _raw_details:
        key = (d["order_num"], d["item_name"])
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
except Exception:
    pass  # table may not exist yet

# ── Location Overrides ─────────────────────────────────────────────────────
# Load overrides from Supabase (keyed by (order_num, item_name))
_LOC_OVERRIDES: dict[tuple[str, str], list[dict]] = {}
try:
    _raw_overrides = _load_location_overrides()
    for ov in _raw_overrides:
        key = (ov["order_num"], ov["item_name"])
        _LOC_OVERRIDES.setdefault(key, []).append({"location": ov["location"], "qty": ov["qty"]})
except Exception:
    pass  # table may not exist yet

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

def _norm_loc(loc_str):
    """Normalize location string to 'Tulsa' or 'Texas' or ''."""
    loc_str = (loc_str or "").strip().lower()
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

# Rebuild _UPLOADED_INVENTORY from expanded INV_ITEMS (more accurate than
# reading _ITEM_DETAILS directly — handles duplicate receipt line items)
_UPLOADED_INVENTORY.clear()
if len(INV_ITEMS) > 0:
    for _, _r in INV_ITEMS.iterrows():
        _loc = _norm_loc(_r.get("_override_location", ""))
        if not _loc:
            continue
        _inv_key = (_loc, _r["name"], _r.get("category", "Other"))
        _UPLOADED_INVENTORY[_inv_key] = _UPLOADED_INVENTORY.get(_inv_key, 0) + int(_r["qty"])

# Apply persistent image overrides (for renamed items saved via Image Manager)
try:
    _img_overrides = _load_image_overrides()
    if _img_overrides:
        _IMAGE_URLS.update(_img_overrides)  # overrides take priority
except Exception:
    pass

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
etsy_net_earned = gross_sales - total_fees - total_shipping_cost - total_marketing - total_refunds - total_taxes - total_buyer_fees
etsy_net = etsy_net_earned  # override earlier calc that missed tax + buyer fees
etsy_net_margin = (etsy_net / gross_sales * 100) if gross_sales else 0
# etsy_pre_capone_deposits and etsy_balance loaded from config.json above
etsy_total_deposited = etsy_pre_capone_deposits + bank_total_deposits
etsy_balance_calculated = etsy_net_earned - etsy_total_deposited
etsy_csv_gap = round(etsy_balance_calculated - etsy_balance, 2)

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

# ── Hot-Reload Functions (Data Hub) ──────────────────────────────────────────

_RECENT_UPLOADS: set = set()  # Order numbers uploaded this session via Data Hub


def _reload_etsy_data():
    """Re-read all Etsy CSVs from local files and rebuild every Etsy-derived metric.

    Returns dict with summary stats for the UI status message.
    """
    global DATA, sales_df, fee_df, ship_df, mkt_df, refund_df, tax_df
    global deposit_df, buyer_fee_df
    global gross_sales, total_refunds, net_sales, total_fees, total_fees_gross
    global total_shipping_cost, total_marketing, total_taxes
    global etsy_net, order_count, avg_order, etsy_net_margin
    global total_buyer_fees, etsy_net_earned, etsy_total_deposited
    global etsy_balance_calculated, etsy_csv_gap, real_profit, real_profit_margin
    global bank_all_expenses, bank_cash_on_hand, etsy_balance
    global monthly_sales, monthly_fees, monthly_shipping, monthly_marketing
    global monthly_refunds, monthly_taxes, monthly_raw_fees, monthly_raw_shipping
    global monthly_raw_marketing, monthly_raw_refunds, monthly_net_revenue
    global daily_sales, daily_orders, daily_df, weekly_aov
    global monthly_order_counts, monthly_aov, monthly_profit_per_order
    global months_sorted, days_active
    global listing_fees, transaction_fees_product, transaction_fees_shipping
    global processing_fees, credit_transaction, credit_listing, credit_processing
    global share_save, total_credits
    global etsy_ads, offsite_ads_fees, offsite_ads_credits
    global usps_outbound, usps_outbound_count, usps_return, usps_return_count
    global asendia_labels, asendia_count, ship_adjustments, ship_adjust_count
    global ship_credits, ship_credit_count, ship_insurance, ship_insurance_count
    global buyer_paid_shipping, shipping_profit, shipping_margin
    global paid_ship_count, free_ship_count, avg_outbound_label
    global product_fee_totals, product_revenue_est

    # Always read from local CSV files (not stale Supabase)
    import glob as _gl
    _ed = os.path.join(BASE_DIR, "data", "etsy_statements")
    _frames = []
    for _f in sorted(_gl.glob(os.path.join(_ed, "etsy_statement*.csv"))):
        try:
            _frames.append(pd.read_csv(_f))
        except Exception:
            pass
    if _frames:
        DATA = pd.concat(_frames, ignore_index=True)
        DATA["Amount_Clean"] = DATA["Amount"].apply(parse_money)
        DATA["Net_Clean"] = DATA["Net"].apply(parse_money)
        DATA["Fees_Clean"] = DATA["Fees & Taxes"].apply(parse_money)
        DATA["Date_Parsed"] = pd.to_datetime(DATA["Date"], format="%B %d, %Y", errors="coerce")
        DATA["Month"] = DATA["Date_Parsed"].dt.to_period("M").astype(str)
        DATA["Week"] = DATA["Date_Parsed"].dt.to_period("W").apply(lambda p: p.start_time)

    # Rebuild filtered DataFrames
    sales_df = DATA[DATA["Type"] == "Sale"]
    fee_df = DATA[DATA["Type"] == "Fee"]
    ship_df = DATA[DATA["Type"] == "Shipping"]
    mkt_df = DATA[DATA["Type"] == "Marketing"]
    refund_df = DATA[DATA["Type"] == "Refund"]
    tax_df = DATA[DATA["Type"] == "Tax"]
    deposit_df = DATA[DATA["Type"] == "Deposit"]
    buyer_fee_df = DATA[DATA["Type"] == "Buyer Fee"]

    # Top-level numbers
    gross_sales = sales_df["Net_Clean"].sum()
    total_refunds = abs(refund_df["Net_Clean"].sum())
    net_sales = gross_sales - total_refunds
    total_fees = abs(fee_df["Net_Clean"].sum())
    total_shipping_cost = abs(ship_df["Net_Clean"].sum())
    total_marketing = abs(mkt_df["Net_Clean"].sum())
    total_taxes = abs(tax_df["Net_Clean"].sum())
    order_count = len(sales_df)
    avg_order = gross_sales / order_count if order_count else 0

    # Full accounting
    total_buyer_fees = abs(buyer_fee_df["Net_Clean"].sum()) if len(buyer_fee_df) else 0.0
    etsy_net_earned = (gross_sales - total_fees - total_shipping_cost
                       - total_marketing - total_refunds - total_taxes - total_buyer_fees)
    etsy_net = etsy_net_earned
    etsy_net_margin = (etsy_net / gross_sales * 100) if gross_sales else 0

    # Auto-calculate Etsy balance from deposit titles
    import re as _re
    _dep_total = 0.0
    for _, _dr in deposit_df.iterrows():
        _m = _re.search(r'([\d,]+\.\d+)', str(_dr.get("Title", "")))
        if _m:
            _dep_total += float(_m.group(1).replace(",", ""))
    etsy_balance = max(0, round(DATA["Net_Clean"].sum() - _dep_total, 2))

    etsy_total_deposited = etsy_pre_capone_deposits + bank_total_deposits
    etsy_balance_calculated = etsy_net_earned - etsy_total_deposited
    etsy_csv_gap = round(etsy_balance_calculated - etsy_balance, 2)

    # Recalculate cross-source metrics that depend on Etsy data
    bank_cash_on_hand = bank_net_cash + etsy_balance
    real_profit = bank_cash_on_hand + bank_owner_draw_total
    real_profit_margin = (real_profit / gross_sales * 100) if gross_sales else 0

    # ── Recompute all daily/monthly/fee/shipping/product metrics ──

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

    # Buyer paid shipping
    ship_fee_gross = abs(
        fee_df[fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)]["Net_Clean"].sum()
    )
    ship_fee_credits_amt = abs(
        fee_df[fee_df["Title"].str.contains("Credit for transaction fee on shipping", na=False)]["Net_Clean"].sum()
    )
    net_ship_tx_fees = ship_fee_gross - ship_fee_credits_amt
    buyer_paid_shipping = net_ship_tx_fees / 0.065 if net_ship_tx_fees else 0
    shipping_profit = buyer_paid_shipping - total_shipping_cost
    shipping_margin = (shipping_profit / buyer_paid_shipping * 100) if buyer_paid_shipping else 0

    # Paid vs free shipping orders
    ship_fee_rows = fee_df[fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)].copy()
    orders_with_paid_shipping = set(ship_fee_rows["Info"].dropna())
    all_order_ids = set(sales_df["Title"].str.extract(r"(Order #\d+)", expand=False).dropna())
    orders_free_shipping = all_order_ids - orders_with_paid_shipping
    paid_ship_count = len(orders_with_paid_shipping & all_order_ids)
    free_ship_count = len(orders_free_shipping)
    avg_outbound_label = usps_outbound / usps_outbound_count if usps_outbound_count else 0

    # Product performance
    prod_fees = fee_df[
        fee_df["Title"].str.startswith("Transaction fee:", na=False)
        & ~fee_df["Title"].str.contains("Shipping", na=False)
    ].copy()
    prod_fees["Product"] = prod_fees["Title"].str.replace("Transaction fee: ", "", regex=False)
    product_fee_totals = prod_fees.groupby("Product")["Net_Clean"].sum().abs().sort_values(ascending=False)
    if len(product_fee_totals) > 0:
        product_revenue_est = (product_fee_totals / 0.065).round(2)
    else:
        product_revenue_est = pd.Series(dtype=float)

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

    monthly_net_revenue = {}
    for m in months_sorted:
        monthly_net_revenue[m] = (
            monthly_sales.get(m, 0)
            + monthly_raw_fees.get(m, 0)
            + monthly_raw_shipping.get(m, 0)
            + monthly_raw_marketing.get(m, 0)
            + monthly_raw_refunds.get(m, 0)
        )

    # Daily aggregations
    daily_sales = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
    daily_orders = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].count()
    daily_fee_cost = fee_df.groupby(fee_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
    daily_ship_cost = ship_df.groupby(ship_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
    daily_mkt_cost = mkt_df.groupby(mkt_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
    daily_refund_cost = refund_df.groupby(refund_df["Date_Parsed"].dt.date)["Net_Clean"].sum()

    all_dates = sorted(set(daily_sales.index) | set(daily_fee_cost.index) | set(daily_ship_cost.index))
    daily_df = pd.DataFrame(index=all_dates)
    daily_df["revenue"] = pd.Series(daily_sales)
    daily_df["fees"] = pd.Series(daily_fee_cost)
    daily_df["shipping"] = pd.Series(daily_ship_cost)
    daily_df["marketing"] = pd.Series(daily_mkt_cost)
    daily_df["refunds"] = pd.Series(daily_refund_cost)
    daily_df["orders"] = pd.Series(daily_orders)
    daily_df = daily_df.fillna(0)
    daily_df["profit"] = daily_df["revenue"] + daily_df["fees"] + daily_df["shipping"] + daily_df["marketing"] + daily_df["refunds"]
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
        days_active = max((DATA["Date_Parsed"].max() - DATA["Date_Parsed"].min()).days, 1)
    else:
        days_active = 1

    return {
        "transactions": len(DATA),
        "orders": order_count,
        "gross_sales": gross_sales,
        "etsy_net": etsy_net,
    }


def _reload_bank_data():
    """Re-parse all bank PDFs and rebuild bank-derived metrics in-place.

    Returns dict with summary stats for the UI status message.
    """
    global BANK_TXNS, bank_deposits, bank_debits
    global bank_total_deposits, bank_total_debits, bank_net_cash
    global bank_by_cat, bank_monthly, bank_statement_count
    global bank_tax_deductible, bank_personal, bank_pending
    global bank_biz_expense_total, bank_all_expenses, bank_cash_on_hand
    global bank_owner_draw_total, real_profit, real_profit_margin
    global tulsa_draws, texas_draws, tulsa_draw_total, texas_draw_total
    global draw_diff, draw_owed_to
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

    # Save updated JSON locally
    out_path = os.path.join(BASE_DIR, "data", "generated", "bank_transactions.json")
    try:
        with open(out_path, "w") as f:
            json.dump({"metadata": {"source_files": source_files}, "transactions": all_txns}, f, indent=2)
    except Exception:
        pass

    BANK_TXNS = all_txns
    bank_statement_count = len(source_files)

    # Rebuild aggregates (mirrors lines 407-474)
    bank_deposits = [t for t in BANK_TXNS if t["type"] == "deposit"]
    bank_debits = [t for t in BANK_TXNS if t["type"] == "debit"]
    bank_total_deposits = sum(t["amount"] for t in bank_deposits)
    bank_total_debits = sum(t["amount"] for t in bank_debits)
    bank_net_cash = bank_total_deposits - bank_total_debits

    bank_by_cat = {}
    for t in bank_debits:
        cat = t["category"]
        bank_by_cat[cat] = bank_by_cat.get(cat, 0) + t["amount"]
    bank_by_cat = dict(sorted(bank_by_cat.items(), key=lambda x: -x[1]))

    bank_monthly = {}
    for t in BANK_TXNS:
        parts = t["date"].split("/")
        month_key = f"{parts[2]}-{parts[0]}"
        if month_key not in bank_monthly:
            bank_monthly[month_key] = {"deposits": 0, "debits": 0}
        if t["type"] == "deposit":
            bank_monthly[month_key]["deposits"] += t["amount"]
        else:
            bank_monthly[month_key]["debits"] += t["amount"]

    bank_tax_deductible = sum(amt for cat, amt in bank_by_cat.items() if cat in BANK_TAX_DEDUCTIBLE)
    bank_personal = bank_by_cat.get("Personal", 0)
    bank_pending = bank_by_cat.get("Pending", 0)

    _biz_expense_cats = ["Shipping", "Craft Supplies", "Etsy Fees", "Subscriptions",
                         "AliExpress Supplies", "Business Credit Card"]
    bank_biz_expense_total = sum(bank_by_cat.get(c, 0) for c in _biz_expense_cats)
    bank_all_expenses = bank_by_cat.get("Amazon Inventory", 0) + bank_biz_expense_total
    bank_cash_on_hand = bank_net_cash + etsy_balance
    bank_owner_draw_total = sum(bank_by_cat.get(c, 0) for c in bank_by_cat if c.startswith("Owner Draw"))
    real_profit = bank_cash_on_hand + bank_owner_draw_total
    real_profit_margin = (real_profit / gross_sales * 100) if gross_sales else 0

    tulsa_draws = [t for t in bank_debits if t["category"] == "Owner Draw - Tulsa"]
    texas_draws = [t for t in bank_debits if t["category"] == "Owner Draw - Texas"]
    tulsa_draw_total = sum(t["amount"] for t in tulsa_draws)
    texas_draw_total = sum(t["amount"] for t in texas_draws)
    draw_diff = abs(tulsa_draw_total - texas_draw_total)
    draw_owed_to = "Braden" if tulsa_draw_total > texas_draw_total else "TJ"

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

    return {
        "transactions": len(BANK_TXNS),
        "statements": bank_statement_count,
        "net_cash": bank_net_cash,
    }


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

    # Append to INVOICES and persist (mirrors lines 8796-8807)
    INVOICES.append(new_order)
    _RECENT_UPLOADS.add(new_order["order_num"])
    try:
        out_path = os.path.join(BASE_DIR, "data", "generated", "inventory_orders.json")
        with open(out_path, "w") as f:
            json.dump(INVOICES, f, indent=2)
    except Exception:
        pass
    _save_new_order(new_order)
    _notify_railway_reload()

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
        personal_total = INV_ITEMS[INV_ITEMS["category"] == "Personal/Gift"]["total"].sum()
        biz_fee_total = INV_ITEMS[INV_ITEMS["category"] == "Business Fees"]["total"].sum()
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

    _recompute_stock_summary()

    return {
        "order_num": new_order["order_num"],
        "item_count": len(new_order["items"]),
        "grand_total": new_order["grand_total"],
    }


def _cascade_reload(source="etsy"):
    """Recalculate cross-source derived metrics after any data upload.

    Call this after _reload_etsy_data, _reload_bank_data, or _reload_inventory_data
    to ensure globals that span multiple data sources stay consistent.
    """
    global real_profit, real_profit_margin, bank_cash_on_hand, bank_all_expenses
    global profit, profit_margin, receipt_cogs_outside_bank
    global bank_amazon_inv

    # bank_cash_on_hand depends on bank_net_cash + etsy_balance
    bank_cash_on_hand = bank_net_cash + etsy_balance
    # real_profit depends on bank_cash_on_hand + bank_owner_draw_total
    real_profit = bank_cash_on_hand + bank_owner_draw_total
    real_profit_margin = (real_profit / gross_sales * 100) if gross_sales else 0

    # All spending flows through the bank; CC balance is a liability, not expense
    bank_amazon_inv = bank_by_cat.get("Amazon Inventory", 0)
    receipt_cogs_outside_bank = 0
    profit = real_profit
    profit_margin = (profit / gross_sales * 100) if gross_sales else 0

    # Recompute all derived metrics and charts
    _recompute_shipping_details()
    _recompute_analytics()
    _recompute_tax_years()
    _recompute_valuation()
    _rebuild_all_charts()


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

    for fn in os.listdir(etsy_dir):
        if not fn.lower().endswith(".csv"):
            continue
        # Exact filename match
        if fn == new_filename:
            return True, fn, f"Replacing existing file {fn}"
        # Date range overlap check
        try:
            existing_df = pd.read_csv(os.path.join(etsy_dir, fn))
            ex_dates = pd.to_datetime(existing_df["Date"], format="%B %d, %Y", errors="coerce")
            ex_min, ex_max = ex_dates.min(), ex_dates.max()
            if pd.isna(ex_min) or pd.isna(ex_max):
                continue
            # Ranges overlap if one starts before the other ends
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
    if any(w in name_l for w in ["articles of organization", "credit card surcharge", "llc filing"]):
        return "Business Fees"
    # Crafts - check before filament so clock kit doesn't get caught by "pla" in "replacement"
    if any(w in name_l for w in ["balsa", "basswood", "wood sheet", "magnet", "clock movement",
                                  "clock mechanism", "clock kit", "quartz clock"]):
        return "Crafts"
    if any(w in name_l for w in ["soldering", "3d pen"]):
        return "Tools"
    if any(w in name_l for w in ["build plate", "bed plate", "print surface"]):
        return "Printer Parts"
    if any(w in name_l for w in ["earring", "jewelry"]):
        return "Jewelry"
    if any(w in name_l for w in ["pla", "filament", "3d printer filament"]):
        return "Filament"
    if any(w in name_l for w in ["gift box", "box", "mailer", "bubble", "wrapping", "packing", "packaging",
                                  "shipping label", "label printer", "fragile sticker"]):
        return "Packaging"
    if any(w in name_l for w in ["led", "lamp", "light", "bulb", "socket", "pendant", "lantern", " cord"]):
        return "Lighting"
    if any(w in name_l for w in ["screw", "bolt", "glue", "adhesive", "wire", "hook", "ring"]):
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
    personal_total = INV_ITEMS[INV_ITEMS["category"] == "Personal/Gift"]["total"].sum()
    biz_fee_total = INV_ITEMS[INV_ITEMS["category"] == "Business Fees"]["total"].sum()
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


def _build_kpi_pill(icon, label, value, color, subtitle="", detail=""):
    """Premium KPI pill with gradient icon, bold value, depth shadows, and optional expandable detail."""
    text_children = [
        html.Div(label, style={"color": GRAY, "fontSize": "11px", "fontWeight": "600",
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

# Per-location aggregates (business orders only)
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
    tulsa_monthly = BIZ_INV_DF[BIZ_INV_DF["location"] == "Tulsa, OK"].groupby("month")["grand_total"].sum()
    texas_monthly = BIZ_INV_DF[BIZ_INV_DF["location"] == "Texas"].groupby("month")["grand_total"].sum()
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

# Buyer paid shipping
ship_fee_gross = abs(
    fee_df[fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)]["Net_Clean"].sum()
)
ship_fee_credits_amt = abs(
    fee_df[fee_df["Title"].str.contains("Credit for transaction fee on shipping", na=False)]["Net_Clean"].sum()
)
net_ship_tx_fees = ship_fee_gross - ship_fee_credits_amt
buyer_paid_shipping = net_ship_tx_fees / 0.065
shipping_profit = buyer_paid_shipping - total_shipping_cost
shipping_margin = (shipping_profit / buyer_paid_shipping * 100) if buyer_paid_shipping else 0

# Paid vs free shipping orders
ship_fee_rows = fee_df[fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)].copy()
orders_with_paid_shipping = set(ship_fee_rows["Info"].dropna())
all_order_ids = set(sales_df["Title"].str.extract(r"(Order #\d+)", expand=False).dropna())
orders_free_shipping = all_order_ids - orders_with_paid_shipping
paid_ship_count = len(orders_with_paid_shipping & all_order_ids)
free_ship_count = len(orders_free_shipping)

avg_outbound_label = usps_outbound / usps_outbound_count if usps_outbound_count else 0


def _recompute_shipping_details():
    """Recompute paid-vs-free shipping estimates and return label matches."""
    global est_label_cost_paid_orders, paid_shipping_profit, est_label_cost_free_orders
    global refund_buyer_shipping, est_refund_label_cost, return_label_matches

    est_label_cost_paid_orders = paid_ship_count * avg_outbound_label
    paid_shipping_profit = buyer_paid_shipping - est_label_cost_paid_orders
    est_label_cost_free_orders = free_ship_count * avg_outbound_label

    # Refunded orders shipping
    refund_df_orders = refund_df.copy()
    refund_df_orders["Order"] = refund_df_orders["Title"].str.extract(r"(Order #\d+)")
    refunded_order_ids = set(refund_df_orders["Order"].dropna())

    refund_ship_fees = 0.0
    for oid in refunded_order_ids:
        order_ship = fee_df[
            (fee_df["Info"] == oid) & fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)
        ]
        if len(order_ship):
            refund_ship_fees += abs(order_ship["Net_Clean"].sum())
    refund_buyer_shipping = refund_ship_fees / 0.065 if refund_ship_fees else 0
    est_refund_label_cost = len(refunded_order_ids) * avg_outbound_label

    # Match return labels to refunds by date proximity
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
est_label_cost_paid_orders = 0.0
paid_shipping_profit = 0.0
est_label_cost_free_orders = 0.0
refund_buyer_shipping = 0.0
est_refund_label_cost = 0.0
return_label_matches = []
_recompute_shipping_details()

# Product performance
prod_fees = fee_df[
    fee_df["Title"].str.startswith("Transaction fee:", na=False)
    & ~fee_df["Title"].str.contains("Shipping", na=False)
].copy()
prod_fees["Product"] = prod_fees["Title"].str.replace("Transaction fee: ", "", regex=False)
product_fee_totals = prod_fees.groupby("Product")["Net_Clean"].sum().abs().sort_values(ascending=False)
if len(product_fee_totals) > 0:
    product_revenue_est = (product_fee_totals / 0.065).round(2)
else:
    product_revenue_est = pd.Series(dtype=float)

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

monthly_net_revenue = {}
for m in months_sorted:
    monthly_net_revenue[m] = (
        monthly_sales.get(m, 0)
        + monthly_raw_fees.get(m, 0)
        + monthly_raw_shipping.get(m, 0)
        + monthly_raw_marketing.get(m, 0)
        + monthly_raw_refunds.get(m, 0)
    )

# Daily aggregations
daily_sales = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
daily_orders = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Net_Clean"].count()

# Daily costs for profit calculation
daily_fee_cost = fee_df.groupby(fee_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
daily_ship_cost = ship_df.groupby(ship_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
daily_mkt_cost = mkt_df.groupby(mkt_df["Date_Parsed"].dt.date)["Net_Clean"].sum()
daily_refund_cost = refund_df.groupby(refund_df["Date_Parsed"].dt.date)["Net_Clean"].sum()

# Build a unified daily DataFrame
all_dates = sorted(set(daily_sales.index) | set(daily_fee_cost.index) | set(daily_ship_cost.index))
daily_df = pd.DataFrame(index=all_dates)
daily_df["revenue"] = pd.Series(daily_sales)
daily_df["fees"] = pd.Series(daily_fee_cost)
daily_df["shipping"] = pd.Series(daily_ship_cost)
daily_df["marketing"] = pd.Series(daily_mkt_cost)
daily_df["refunds"] = pd.Series(daily_refund_cost)
daily_df["orders"] = pd.Series(daily_orders)
daily_df = daily_df.fillna(0)
daily_df["profit"] = daily_df["revenue"] + daily_df["fees"] + daily_df["shipping"] + daily_df["marketing"] + daily_df["refunds"]
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
    days_active = max((DATA["Date_Parsed"].max() - DATA["Date_Parsed"].min()).days, 1)
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
    monthly_rev_series = pd.Series({m: monthly_sales.get(m, 0) for m in months_sorted})
    monthly_net_series = pd.Series({m: monthly_net_revenue.get(m, 0) for m in months_sorted})

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

    # 3. SHIPPING ROOT CAUSE
    if shipping_profit < 0:
        reasons = []
        free_cost = free_ship_count * avg_outbound_label
        paid_profit_val = buyer_paid_shipping - (paid_ship_count * avg_outbound_label)
        reasons.append(
            f"FREE SHIPPING DRAIN: {free_ship_count} free-shipping orders cost ~${free_cost:,.0f} in labels "
            f"with $0 shipping revenue. That alone accounts for "
            f"{'more than the total shipping loss' if free_cost > abs(shipping_profit) else f'${free_cost:,.0f} of the ${abs(shipping_profit):,.0f} loss'}."
        )
        if paid_profit_val > 0:
            reasons.append(
                f"PAID ORDERS ARE FINE: Orders where buyers paid shipping netted +${paid_profit_val:,.0f}. "
                f"The problem is purely the free shipping orders."
            )
        else:
            avg_buyer_ship = buyer_paid_shipping / paid_ship_count if paid_ship_count else 0
            reasons.append(
                f"EVEN PAID SHIPPING LOSES: Buyers pay ${avg_buyer_ship:.2f} avg but labels cost ${avg_outbound_label:.2f} avg. "
                f"Shipping prices need to go up by at least ${avg_outbound_label - avg_buyer_ship:.2f}/order."
            )
        if asendia_labels > 0:
            intl_avg = asendia_labels / asendia_count if asendia_count else 0
            reasons.append(
                f"INTERNATIONAL IS EXPENSIVE: {asendia_count} Asendia labels at ${intl_avg:.2f} avg "
                f"(vs ${avg_outbound_label:.2f} domestic = {intl_avg / avg_outbound_label:.1f}x more). "
                f"Total intl shipping cost: ${asendia_labels:,.0f}."
            )
        fix_text = (
            f"FIX: Option A) Raise prices on free-shipping listings by ${avg_outbound_label:.0f}-${avg_outbound_label + 3:.0f} "
            f"to absorb label cost -- would recover ~${free_cost:,.0f}. "
            f"Option B) Switch to calculated shipping on heavy/large items. "
            f"Option C) Set minimum order for free shipping (e.g., orders over $50)."
        )
        insights.append((3, "SHIPPING LEAK",
            f"Losing ${abs(shipping_profit):,.2f} on shipping -- here's why",
            " ".join(reasons) + " " + fix_text, "bad"))
    else:
        insights.append((3, "SHIPPING",
            f"Shipping is profitable: +${shipping_profit:,.2f}",
            f"Buyers paid ~${buyer_paid_shipping:,.0f} for shipping, labels cost ${total_shipping_cost:,.0f}. "
            f"Netting ${shipping_profit:,.0f}. Keep monitoring -- USPS rates change annually (typically January).",
            "good"))

    # 4. MARKETING DEEP DIVE
    if total_marketing > 0 and gross_sales > 0:
        marketing_pct = total_marketing / gross_sales * 100
        mkt_by_month = {m: monthly_marketing.get(m, 0) for m in months_sorted if monthly_sales.get(m, 0) > 0}
        mkt_pcts = {m: (v / monthly_sales.get(m, 1) * 100) for m, v in mkt_by_month.items()}
        mkt_trend_text = " -> ".join(f"{m}: {mkt_pcts[m]:.0f}%" for m in months_sorted if m in mkt_pcts)

        offsite_sales_est = offsite_ads_fees / 0.15 if offsite_ads_fees else 0
        offsite_roi = (offsite_sales_est - offsite_ads_fees) / offsite_ads_fees if offsite_ads_fees else 0

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
                f"OFFSITE ADS: ${offsite_ads_fees:,.2f} in fees = Etsy drove ~${offsite_sales_est:,.0f} in sales "
                f"from Google/social ads (ROI: {offsite_roi:.1f}x). "
                f"{'Good return -- offsite ads are profitable.' if offsite_roi > 2 else 'Marginal -- the 15% fee is steep but you cant opt out under $10k/yr.'} "
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
    true_cost_per_refund = avg_refund + avg_outbound_label + avg_return_label

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
        f"${total_refunds:,.2f} refunded ({len(refund_df)} orders, {refund_rate:.1f}% rate) -- true cost ~${true_cost_per_refund * len(refund_df):,.0f}",
        f"Refund trend by month: {ref_trend}. "
        f"Each refund truly costs ~${true_cost_per_refund:,.0f} "
        f"(${avg_refund:.0f} refund + ${avg_outbound_label:.0f} wasted outbound label + ~${avg_return_label:.0f} return label). "
        f"Total true cost of all refunds: ~${true_cost_per_refund * len(refund_df):,.0f}. "
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
    daily_profit_avg = etsy_net / days_active
    daily_revenue_avg = gross_sales / days_active
    orders_per_day = order_count / days_active

    current_margin_rate = etsy_net / gross_sales if gross_sales else 0
    revenue_to_double = (etsy_net * 2) / current_margin_rate if current_margin_rate > 0 else 0
    extra_orders_needed = (revenue_to_double - gross_sales) / avg_order if avg_order else 0
    extra_per_day = extra_orders_needed / days_active

    insights.append((10, "GOALS & TARGETS",
        f"${daily_profit_avg:,.0f}/day profit | ${daily_revenue_avg:,.0f}/day revenue | {orders_per_day:.1f} orders/day",
        f"Current monthly run rate: ${daily_revenue_avg * 30:,.0f} revenue, ${daily_profit_avg * 30:,.0f} profit. "
        f"To double profit to ${etsy_net * 2:,.0f}, you'd need ~${revenue_to_double:,.0f} in total sales "
        f"(~{extra_per_day:.1f} more orders/day at current avg order of ${avg_order:.0f}). "
        f"FASTEST PATHS: 1) Raise prices 10% = instant ~${gross_sales * 0.10:,.0f} more revenue with same orders. "
        f"2) Cut free shipping = save ~${free_ship_count * avg_outbound_label:,.0f}. "
        f"3) Reduce refunds by 50% = save ~${total_refunds / 2:,.0f}. "
        f"Combined that's ~${gross_sales * 0.10 + free_ship_count * avg_outbound_label + total_refunds / 2:,.0f} extra -- "
        f"roughly {((gross_sales * 0.10 + free_ship_count * avg_outbound_label + total_refunds / 2) / etsy_net * 100):.0f}% profit increase without a single extra sale." if etsy_net > 0 else
        f"Focus on reducing costs first -- shipping and refunds are the biggest levers.",
        "info"))

    # 11. INVENTORY / COGS ANALYSIS
    cogs_pct = total_inventory_cost / gross_sales * 100 if gross_sales else 0
    biggest_cat = inv_by_category.index[0] if len(inv_by_category) > 0 else "Unknown"
    biggest_cat_amt = inv_by_category.values[0] if len(inv_by_category) > 0 else 0
    biggest_cat_pct = biggest_cat_amt / INV_ITEMS["total"].sum() * 100 if len(INV_ITEMS) > 0 and INV_ITEMS["total"].sum() > 0 else 0

    inv_detail = (
        f"Total supplies: ${total_inventory_cost:,.2f} ({inv_order_count} Amazon orders). "
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
        f"${total_inventory_cost:,.2f} in supplies ({cogs_pct:.1f}% of revenue) -- Profit: ${profit:,.2f}",
        inv_detail,
        "good" if cogs_pct < 25 else "warning" if cogs_pct < 40 else "bad"))

    insights.sort(key=lambda x: x[0])
    return insights, projections



def _recompute_analytics():
    global analytics_insights, analytics_projections
    analytics_insights, analytics_projections = run_analytics()


_recompute_analytics()


# ── Chatbot Engine ──────────────────────────────────────────────────────────

def chatbot_answer(question):
    """Self-contained chatbot that answers ANY question about the Etsy data."""
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
            f"\n\n**Weekly AOV** ranges from ${weekly_aov['aov'].min():,.2f} to ${weekly_aov['aov'].max():,.2f}."
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
                f"**Paid shipping orders:**\n"
                f"- Buyers paid: ~${buyer_paid_shipping:,.2f}\n"
                f"- Estimated label cost: ~${est_label_cost_paid_orders:,.2f}\n"
                f"- Profit from paid shipping: ${paid_shipping_profit:,.2f}\n\n"
                f"**Free shipping orders:**\n"
                f"- You absorbed: ~${est_label_cost_free_orders:,.2f} in labels\n"
                f"- This is your biggest shipping loss"
            )

        if any(w in q for w in ["profit", "loss", "making", "losing"]):
            return (
                f"**Shipping Profit/Loss:** ${shipping_profit:,.2f} ({'PROFIT' if shipping_profit > 0 else 'LOSS'})\n\n"
                f"- Buyers paid: ~${buyer_paid_shipping:,.2f}\n"
                f"- Total label costs: ${total_shipping_cost:,.2f}\n"
                f"- Margin: {shipping_margin:.1f}%\n\n"
                f"Breakdown:\n"
                f"- USPS outbound: ${usps_outbound:,.2f} ({usps_outbound_count} labels, avg ${avg_outbound_label:.2f})\n"
                f"- USPS returns: ${usps_return:,.2f} ({usps_return_count} labels)\n"
                f"- Asendia (intl): ${asendia_labels:,.2f} ({asendia_count} labels)\n"
                f"- Adjustments: ${ship_adjustments:,.2f}\n"
                f"- Credits: ${abs(ship_credits):,.2f}"
            )

        return (
            f"**Shipping Overview:**\n\n"
            f"- Total shipping cost: ${total_shipping_cost:,.2f}\n"
            f"- Buyers paid for shipping: ~${buyer_paid_shipping:,.2f}\n"
            f"- **Shipping P/L: ${shipping_profit:,.2f}** ({shipping_margin:.1f}%)\n\n"
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

    # ── Refund / Return questions ──
    if any(w in q for w in ["refund", "return", "refunded"]):
        refund_rate = len(refund_df) / len(sales_df) * 100 if len(sales_df) else 0
        avg_ref = total_refunds / len(refund_df) if len(refund_df) else 0

        lines = [
            f"**Refunds:** ${total_refunds:,.2f} ({len(refund_df)} orders, {refund_rate:.1f}% rate)\n",
            f"Average refund: ${avg_ref:,.2f}\n",
            f"**Monthly refunds:**",
        ]
        for m in months_sorted:
            lines.append(f"- {m}: ${monthly_refunds.get(m, 0):,.2f}")

        if return_label_matches:
            lines.append(f"\n**Return labels:** {usps_return_count} totaling ${usps_return:,.2f}")
            for match in return_label_matches:
                lines.append(f"- {match['date']}: ${match['cost']:.2f} -- {match['product'][:45]}")

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
            lines = ["**Bottom 10 Products by Estimated Revenue:**\n"]
            for i, (name, rev) in enumerate(bottom.items(), 1):
                lines.append(f"{i}. {name[:50]} -- ${rev:,.2f}")
            return "\n".join(lines)

        top = product_revenue_est.head(10)
        total_prod = product_revenue_est.sum()
        lines = ["**Top 10 Products by Estimated Revenue:**\n"]
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
                    f"- Estimated revenue: ${prod_rev:,.2f}\n"
                    f"- Estimated units sold: {est_units}\n"
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
                f"- Estimated revenue: ${prod_rev:,.2f}\n"
                f"- Estimated units sold: {est_units}\n"
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

        yr_gross = _s["Net_Clean"].sum()
        yr_refunds = abs(_rf["Net_Clean"].sum())
        yr_fees = abs(_f["Net_Clean"].sum())
        yr_shipping = abs(_sh["Net_Clean"].sum())
        yr_marketing = abs(_mk["Net_Clean"].sum())
        yr_taxes = abs(_tx["Net_Clean"].sum())
        yr_buyer_fees = abs(_bf["Net_Clean"].sum()) if len(_bf) else 0.0

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

        yr_etsy_net = yr_gross - yr_fees - yr_shipping - yr_marketing - yr_refunds - yr_taxes - yr_buyer_fees

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
        _biz_cats = ["Shipping", "Craft Supplies", "Etsy Fees", "Subscriptions",
                     "AliExpress Supplies", "Business Credit Card"]
        yr_bank_biz_expense = sum(yr_bank_by_cat.get(c, 0) for c in _biz_cats)

        # Net income (Etsy net minus bank operating expenses minus inventory)
        yr_net_income = yr_etsy_net - yr_bank_biz_expense - yr_inventory_cost

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
            "etsy_net": yr_etsy_net,
            "cogs": yr_cogs,
            "inventory_cost": yr_inventory_cost,
            "bank_inv": yr_bank_inv,
            "bank_inv_gap": yr_bank_inv_gap,
            "bank_by_cat": yr_bank_by_cat,
            "bank_deposits": yr_bank_deposits,
            "bank_debits": yr_bank_debits,
            "bank_biz_expense": yr_bank_biz_expense,
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

    _val_months_operating = max(len(months_sorted), 1)
    _val_annualize = 12 / _val_months_operating

    # Annual metrics
    val_annual_revenue = gross_sales * _val_annualize
    val_annual_etsy_net = etsy_net * _val_annualize
    val_annual_real_profit = real_profit * _val_annualize

    # SDE = real_profit + owner_draws (owner draws are discretionary, added back)
    val_sde = profit + bank_owner_draw_total
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
    val_total_assets = bank_cash_on_hand + bb_cc_asset_value + true_inventory_cost
    val_total_liabilities = bb_cc_balance
    val_asset_val = val_total_assets - val_total_liabilities

    # Blended valuation (50% SDE + 25% Revenue + 25% Asset)
    val_blended_low = val_sde_low * 0.50 + val_rev_low * 0.25 + val_asset_val * 0.25
    val_blended_mid = val_sde_mid * 0.50 + val_rev_mid * 0.25 + val_asset_val * 0.25
    val_blended_high = val_sde_high * 0.50 + val_rev_high * 0.25 + val_asset_val * 0.25

    # Monthly run rate
    val_monthly_run_rate = gross_sales / _val_months_operating
    val_monthly_profit_rate = profit / _val_months_operating

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
    _hs_profit = min(25, max(0, profit_margin / 2))  # 0-25 pts: 50%+ margin = full
    _hs_growth = min(25, max(0, (_val_growth_pct + 10) * 1.25)) if _val_growth_pct > -10 else 0  # 0-25 pts
    _prod_count = len(product_revenue_est) if len(product_revenue_est) > 0 else 1
    _top3_conc = product_revenue_est.head(3).sum() / product_revenue_est.sum() * 100 if product_revenue_est.sum() > 0 else 100
    _hs_diversity = min(15, max(0, (100 - _top3_conc) / 3))  # 0-15 pts
    _hs_cash = min(15, max(0, bank_cash_on_hand / val_monthly_run_rate * 5)) if val_monthly_run_rate > 0 else 0  # 0-15 pts: 3+ months runway = full
    _hs_debt = 10 if bb_cc_balance == 0 else max(0, 10 - bb_cc_balance / 500)  # 0-10 pts
    _hs_shipping = 10 if shipping_profit >= 0 else max(0, 10 + shipping_profit / 100)  # 0-10 pts
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
    if shipping_profit < 0:
        val_risks.append(("Shipping Loss", f"Losing ${abs(shipping_profit):,.0f} on shipping", "MED"))
    val_risks.append(("Platform Dependency", "100% revenue from Etsy — single platform risk", "MED"))
    if gross_sales and total_refunds / gross_sales > 0.05:
        val_risks.append(("Refund Rate", f"{total_refunds / gross_sales * 100:.1f}% refund rate", "MED"))

    # Strengths list
    val_strengths = []
    if profit_margin > 20:
        val_strengths.append(("Strong Margins", f"{profit_margin:.1f}% profit margin"))
    if _val_growth_pct > 5:
        val_strengths.append(("Growing Revenue", f"{_val_growth_pct:+.1f}% monthly growth"))
    if bank_cash_on_hand > val_monthly_run_rate:
        _runway = bank_cash_on_hand / val_monthly_run_rate if val_monthly_run_rate > 0 else 0
        val_strengths.append(("Cash Reserves", f"${bank_cash_on_hand:,.0f} — {_runway:.1f} months runway"))
    if _prod_count > 10:
        val_strengths.append(("Product Diversity", f"{_prod_count} active products"))
    if bb_cc_asset_value > 0:
        val_strengths.append(("Equipment Assets", f"${bb_cc_asset_value:,.0f} in equipment"))
    if bank_owner_draw_total > 0:
        val_strengths.append(("Owner Compensation", f"${bank_owner_draw_total:,.0f} in draws taken"))

    # Burn rate (monthly expenses)
    val_monthly_expenses = (total_fees + total_shipping_cost + total_marketing + total_refunds + total_taxes + total_buyer_fees + bank_all_expenses) / _val_months_operating
    val_runway_months = bank_cash_on_hand / val_monthly_expenses if val_monthly_expenses > 0 else 99


_recompute_valuation()


# ── Helper Functions ─────────────────────────────────────────────────────────

def money(val, sign=True):
    if val < 0:
        return f"-${abs(val):,.2f}"
    return f"${val:,.2f}"


def kpi_card(title, value, color, subtitle="", detail=""):
    children = [
        html.P(title, style={"color": GRAY, "margin": "0", "fontSize": "12px", "fontWeight": "600", "letterSpacing": "0.5px"}),
        html.H2(value, style={"color": color, "margin": "4px 0", "fontSize": "26px"}),
        html.P(subtitle, style={"color": DARKGRAY, "margin": "0", "fontSize": "11px"}),
    ]
    if detail:
        children.append(html.Details([
            html.Summary("details", style={
                "color": f"{CYAN}88", "fontSize": "10px", "cursor": "pointer",
                "marginTop": "6px", "listStyle": "none", "textAlign": "center",
                "userSelect": "none",
            }),
            html.P(detail, style={
                "color": GRAY, "fontSize": "11px", "margin": "6px 0 0 0",
                "textAlign": "left", "lineHeight": "1.4", "padding": "6px",
                "backgroundColor": f"{CARD}dd", "borderRadius": "4px",
                "borderTop": f"1px solid {color}33",
            }),
        ]))
    return html.Div(children, style={
        "backgroundColor": CARD2, "padding": "14px 12px", "borderRadius": "8px",
        "textAlign": "center", "border": f"1px solid {color}33", "flex": "1", "minWidth": "130px",
    })


def section(title, children, color=ORANGE):
    return html.Div([
        html.H3(title, style={"color": color, "borderBottom": f"2px solid {color}", "paddingBottom": "6px", "marginTop": "0", "fontSize": "16px"}),
        *children,
    ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px", "marginBottom": "14px"})


def row_item(label, amount, indent=0, bold=False, color=WHITE, neg_color=RED):
    display_color = neg_color if amount < 0 else color
    style = {
        "display": "flex", "justifyContent": "space-between",
        "padding": "4px 0", "borderBottom": "1px solid #ffffff10",
        "marginLeft": f"{indent * 24}px",
    }
    if bold:
        style["fontWeight"] = "bold"
        style["borderBottom"] = "2px solid #ffffff30"
        style["padding"] = "8px 0"
    return html.Div([
        html.Span(label, style={"color": color if not bold else display_color, "fontSize": "13px"}),
        html.Span(money(amount), style={"color": display_color, "fontFamily": "monospace", "fontSize": "13px"}),
    ], style=style)


def severity_color(sev):
    return GREEN if sev == "good" else RED if sev == "bad" else ORANGE if sev == "warning" else BLUE


def make_chart(fig, height=360, legend_h=True):
    layout = {**CHART_LAYOUT, "height": height}
    if legend_h:
        layout["legend"] = dict(orientation="h", y=1.12, x=0.5, xanchor="center")
    fig.update_layout(**layout)
    return fig


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

    # ── Build Charts ─────────────────────────────────────────────────────────────

    # --- TAB 1: OVERVIEW CHARTS ---

    # Expense donut
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
        if val > 0:
            expense_labels_list.append(name)
            expense_values_list.append(val)
            expense_colors_list.append(clr)

    expense_pie = go.Figure(go.Pie(
        labels=expense_labels_list, values=expense_values_list, hole=0.45,
        marker_colors=expense_colors_list, textinfo="label+percent", textposition="outside",
    ))
    make_chart(expense_pie, 380, False)
    expense_pie.update_layout(title="Where Your Money Goes", showlegend=False)

    # Monthly stacked bar + net profit line
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

    # Daily sales trend (compact for overview)
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


    # --- TAB 2: TRENDS & PATTERNS CHARTS ---

    # 1) Daily profit/revenue with 7-day and 30-day rolling averages
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

    # 2) Cost ratios over time (fees%, shipping%, marketing%, refunds% as % of monthly sales)
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

    # 3) Avg order value trend by week
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

    # 4) Orders per day trend with rolling avg
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

    # 5) Cumulative revenue + cumulative profit
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

    # 6) Profit per day rolling 14-day average
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

    # 7) Avg profit per order over time by month
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

    # --- TAB 2: DEEP DIVE ADVANCED ANALYTICS ---

    # 8) Day-of-week analysis
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

    # 9) Revenue vs Inventory Spend overlay (monthly)
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

    # 10) Inventory category breakdown (what you're spending on)
    inv_cat_bar = go.Figure()
    if len(biz_inv_by_category) > 0:
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

    # 11) Anomaly detection (z-score on daily revenue)
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

    # 12) Product performance heatmap (top products by month)
    _top_n_products = 8
    _top_prod_names = product_revenue_est.head(_top_n_products).index.tolist()
    # Build product-month revenue matrix from transaction fees
    _prod_monthly = {}
    for prod_name in _top_prod_names:
        _pmask = prod_fees["Product"] == prod_name
        _prod_month_fees = prod_fees[_pmask].groupby(
            prod_fees[_pmask]["Date_Parsed"].dt.to_period("M").astype(str)
        )["Net_Clean"].sum().abs()
        _prod_monthly[prod_name[:25]] = {m: (_prod_month_fees.get(m, 0) / 0.065) for m in months_sorted}

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
        product_heat = go.Figure()
        product_heat.add_annotation(text="No product data available", showarrow=False)
        make_chart(product_heat, 200, False)

    # 13) Correlation: Ads Spend vs Sales (monthly)
    corr_fig = go.Figure()
    _corr_ad_vals = [monthly_marketing.get(m, 0) for m in months_sorted]
    _corr_rev_vals = [monthly_sales.get(m, 0) for m in months_sorted]
    corr_fig.add_trace(go.Scatter(
        x=_corr_ad_vals, y=_corr_rev_vals, mode="markers+text",
        text=months_sorted, textposition="top center", textfont=dict(size=9, color=GRAY),
        marker=dict(color=CYAN, size=14, line=dict(color=WHITE, width=1)),
    ))
    # Fit line
    if len(months_sorted) >= 3 and sum(_corr_ad_vals) > 0:
        _corr_X = np.array(_corr_ad_vals).reshape(-1, 1)
        _corr_y = np.array(_corr_rev_vals)
        _corr_lr = LinearRegression().fit(_corr_X, _corr_y)
        _corr_r2 = _corr_lr.score(_corr_X, _corr_y)
        _corr_line_x = [min(_corr_ad_vals), max(_corr_ad_vals)]
        _corr_line_y = [_corr_lr.predict([[v]])[0] for v in _corr_line_x]
        corr_fig.add_trace(go.Scatter(
            x=_corr_line_x, y=_corr_line_y, mode="lines",
            line=dict(color=ORANGE, width=2, dash="dash"), name=f"R²={_corr_r2:.2f}",
        ))
    else:
        _corr_r2 = 0
    make_chart(corr_fig, 300, False)
    corr_fig.update_layout(title="Ad Spend vs Revenue Correlation",
                           xaxis_title="Monthly Ad Spend ($)", yaxis_title="Monthly Revenue ($)")

    # 14) Unit economics waterfall (per-order breakdown)
    _unit_rev = avg_order
    _unit_fees = total_fees / order_count if order_count else 0
    _unit_ship = total_shipping_cost / order_count if order_count else 0
    _unit_ads = total_marketing / order_count if order_count else 0
    _unit_refund = total_refunds / order_count if order_count else 0
    _unit_cogs = true_inventory_cost / order_count if order_count else 0
    _unit_profit = _unit_rev - _unit_fees - _unit_ship - _unit_ads - _unit_refund - _unit_cogs
    _unit_margin = (_unit_profit / _unit_rev * 100) if _unit_rev else 0

    unit_wf = go.Figure(go.Waterfall(
        orientation="v",
        measure=["absolute", "relative", "relative", "relative", "relative", "relative", "total"],
        x=["Revenue", "Fees", "Shipping", "Ads", "Refunds", "Supplies", "Profit"],
        y=[_unit_rev, -_unit_fees, -_unit_ship, -_unit_ads, -_unit_refund, -_unit_cogs, 0],
        connector={"line": {"color": GRAY, "width": 1, "dash": "dot"}},
        increasing={"marker": {"color": GREEN}},
        decreasing={"marker": {"color": RED}},
        totals={"marker": {"color": CYAN}},
        text=[f"${abs(v):,.2f}" for v in [_unit_rev, _unit_fees, _unit_ship, _unit_ads, _unit_refund, _unit_cogs, _unit_profit]],
        textposition="outside",
    ))
    make_chart(unit_wf, 340, False)
    unit_wf.update_layout(title=f"Unit Economics: Average Order Breakdown (margin: {_unit_margin:.1f}%)",
                          showlegend=False, yaxis_title="Per Order ($)")

    # 15) Inventory location split (Tulsa vs Texas)
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
        make_chart(loc_fig, 200, False)

    # 16) Cash flow timeline (deposits - expenses by month)
    cashflow_fig = go.Figure()
    _cf_months = sorted(bank_monthly.keys())
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
    cashflow_fig.update_layout(title="Cash Flow by Month (Bank Deposits vs Expenses)", barmode="relative",
                               yaxis_title="Amount ($)")

    # 17) Break-even analysis
    _monthly_fixed = (bank_biz_expense_total + total_marketing) / _val_months_operating if _val_months_operating else 0
    _contrib_margin_pct = (gross_sales - total_fees - total_shipping_cost - total_refunds - true_inventory_cost) / gross_sales if gross_sales else 0
    _breakeven_monthly = _monthly_fixed / _contrib_margin_pct if _contrib_margin_pct > 0 else 0
    _breakeven_daily = _breakeven_monthly / 30
    _breakeven_orders = _breakeven_monthly / avg_order if avg_order > 0 else 0

    # 18) Supplier spend analysis
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


    # --- TAB 3: AI ANALYTICS BOT CHARTS ---

    # Revenue projection chart
    proj_chart = go.Figure()
    if "proj_sales" in analytics_projections:
        proj_chart.add_trace(go.Scatter(
            name="Actual Sales", x=months_sorted,
            y=[monthly_sales.get(m, 0) for m in months_sorted],
            mode="lines+markers", line=dict(color=GREEN, width=3), marker=dict(size=8),
        ))
        proj_chart.add_trace(go.Scatter(
            name="Actual Net Revenue", x=months_sorted,
            y=[monthly_net_revenue.get(m, 0) for m in months_sorted],
            mode="lines+markers", line=dict(color=ORANGE, width=3), marker=dict(size=8),
        ))

        last_month_dt = pd.Period(months_sorted[-1], freq="M")
        proj_months = [(last_month_dt + i).strftime("%Y-%m") for i in range(1, 4)]

        proj_chart.add_trace(go.Scatter(
            name="Projected Sales",
            x=[months_sorted[-1]] + proj_months,
            y=[monthly_sales.get(months_sorted[-1], 0)] + list(np.maximum(analytics_projections["proj_sales"], 0)),
            mode="lines+markers", line=dict(color=GREEN, width=3, dash="dash"),
            marker=dict(size=8, symbol="diamond"),
        ))
        proj_chart.add_trace(go.Scatter(
            name="Projected Net Revenue",
            x=[months_sorted[-1]] + proj_months,
            y=[monthly_net_revenue.get(months_sorted[-1], 0)] + list(np.maximum(analytics_projections["proj_net"], 0)),
            mode="lines+markers", line=dict(color=ORANGE, width=3, dash="dash"),
            marker=dict(size=8, symbol="diamond"),
        ))

        # Confidence band
        proj_sales_arr = analytics_projections["proj_sales"]
        std_dev = analytics_projections.get("residual_std", np.std([monthly_sales.get(m, 0) for m in months_sorted]))
        upper = np.maximum(proj_sales_arr + std_dev, 0)
        lower = np.maximum(proj_sales_arr - std_dev, 0)
        proj_chart.add_trace(go.Scatter(
            name="Upper Bound", x=proj_months, y=list(upper),
            mode="lines", line=dict(width=0), showlegend=False,
        ))
        proj_chart.add_trace(go.Scatter(
            name="Confidence Range", x=proj_months, y=list(lower),
            mode="lines", line=dict(width=0), fill="tonexty", fillcolor="rgba(46,204,113,0.15)",
        ))
    make_chart(proj_chart, 380)
    proj_chart.update_layout(title="Revenue Projection (Linear Regression, 3-Month Forecast)",
        xaxis_title="Month", yaxis_title="Amount ($)")


    # --- TAB 4: SHIPPING CHARTS ---

    # Buyer paid vs cost comparison
    shipping_compare = go.Figure()
    shipping_compare.add_trace(go.Bar(
        name="Buyers Paid", x=["Shipping"], y=[buyer_paid_shipping],
        marker_color=GREEN, text=[f"${buyer_paid_shipping:,.2f}"], textposition="outside", width=0.3, offset=-0.15,
    ))
    shipping_compare.add_trace(go.Bar(
        name="Your Label Cost", x=["Shipping"], y=[total_shipping_cost],
        marker_color=RED, text=[f"${total_shipping_cost:,.2f}"], textposition="outside", width=0.3, offset=0.15,
    ))
    shipping_compare.add_annotation(
        x="Shipping", y=max(buyer_paid_shipping, total_shipping_cost) + 200,
        text=f"{'Loss' if shipping_profit < 0 else 'Profit'}: ${abs(shipping_profit):,.2f}",
        showarrow=False, font=dict(size=18, color=RED if shipping_profit < 0 else GREEN, family="Arial Black"),
    )
    make_chart(shipping_compare, 340, False)
    shipping_compare.update_layout(title="Shipping: Buyer Paid vs Your Cost",
        showlegend=True, yaxis_title="Amount ($)")

    # Shipping cost by type bar chart
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
        if val > 0:
            ship_type_names.append(nm)
            ship_type_vals.append(val)
            ship_type_colors.append(clr)

    if ship_credits != 0:
        ship_type_names.append(f"Credits ({ship_credit_count})")
        ship_type_vals.append(abs(ship_credits))
        ship_type_colors.append(GREEN)

    ship_type_fig.add_trace(go.Bar(
        x=ship_type_names, y=ship_type_vals, marker_color=ship_type_colors,
        text=[f"${v:,.2f}" for v in ship_type_vals], textposition="outside",
    ))
    make_chart(ship_type_fig, 340, False)
    ship_type_fig.update_layout(title="Shipping Cost by Type", yaxis_title="Amount ($)")

    # International analysis chart
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


    # --- TAB 5: FINANCIALS CHARTS ---

    # Top products bar chart
    top_n = 12
    top_products = product_revenue_est.head(top_n)
    product_fig = go.Figure(go.Bar(
        y=top_products.index, x=top_products.values, orientation="h",
        marker_color=TEAL, text=[f"${v:,.0f}" for v in top_products.values], textposition="outside",
    ))
    make_chart(product_fig, 400, False)
    product_fig.update_layout(title=f"Top {top_n} Products (Est. Revenue)",
        yaxis=dict(autorange="reversed"), margin=dict(l=300, t=50, b=30), xaxis_title="Estimated Revenue ($)")


    # --- TAB 7: INVENTORY / COGS CHARTS ---

    # Monthly inventory spend bar chart
    inv_monthly_fig = go.Figure()
    inv_months_sorted = sorted(monthly_inv_spend.index)
    inv_monthly_fig.add_trace(go.Bar(
        name="Inventory Spend", x=inv_months_sorted,
        y=[monthly_inv_spend.get(m, 0) for m in inv_months_sorted],
        marker_color=PURPLE,
        text=[f"${monthly_inv_spend.get(m, 0):,.0f}" for m in inv_months_sorted],
        textposition="outside",
    ))
    make_chart(inv_monthly_fig, 360)
    inv_monthly_fig.update_layout(title="Monthly Supply Costs", yaxis_title="Amount ($)")

    # Category breakdown donut
    inv_cat_fig = go.Figure()
    if len(biz_inv_by_category) > 0:
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

    # Revenue vs COGS vs True Profit monthly bar chart
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

    # --- LOCATION CHARTS ---

    # TJ vs Braden monthly comparison
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

    # TJ (Tulsa) category donut
    tulsa_cat_fig = go.Figure()
    if len(tulsa_by_cat) > 0:
        tulsa_cat_fig.add_trace(go.Pie(
            labels=tulsa_by_cat.index.tolist(), values=tulsa_by_cat.values.tolist(),
            hole=0.45, marker_colors=[TEAL, BLUE, GREEN, PURPLE, CYAN, PINK, ORANGE, RED][:len(tulsa_by_cat)],
            textinfo="label+percent", textposition="outside",
        ))
    make_chart(tulsa_cat_fig, 340, False)
    tulsa_cat_fig.update_layout(title="TJ (Tulsa) - Categories", showlegend=False)

    # Braden (Texas) category donut
    texas_cat_fig = go.Figure()
    if len(texas_by_cat) > 0:
        texas_cat_fig.add_trace(go.Pie(
            labels=texas_by_cat.index.tolist(), values=texas_by_cat.values.tolist(),
            hole=0.45, marker_colors=[ORANGE, RED, PURPLE, BLUE, TEAL, PINK, GREEN, CYAN][:len(texas_by_cat)],
            textinfo="label+percent", textposition="outside",
        ))
    make_chart(texas_cat_fig, 340, False)
    texas_cat_fig.update_layout(title="Braden (Texas) - Categories", showlegend=False)

    # --- TAB 8: BANK / CASH FLOW CHARTS ---

    # Monthly bar: Dec vs Jan deposits/debits with net line
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

    # --- SANKEY DIAGRAM (module level for reuse) ---
    _etsy_took = total_fees + total_shipping_cost + total_marketing + total_taxes + total_refunds + total_buyer_fees

    sankey_node_labels = [
        f"Customers Paid\n${gross_sales:,.0f}",
        f"Etsy Takes\n-${_etsy_took:,.0f}",
        f"Deposited to Bank\n${etsy_net_earned:,.0f}",
        f"Business Expenses\n${bank_all_expenses:,.0f}",
        f"Owner Draws\n${bank_owner_draw_total:,.0f}",
        f"Cash On Hand\n${bank_cash_on_hand:,.0f}",
        f"Prior Bank Activity\n${old_bank_receipted + bank_unaccounted + etsy_csv_gap:,.0f}",
        f"Amazon Inventory\n${bank_by_cat.get('Amazon Inventory', 0):,.0f}",
        f"AliExpress\n${bank_by_cat.get('AliExpress Supplies', 0):,.0f}",
        f"Best Buy CC\n${bank_by_cat.get('Business Credit Card', 0):,.0f}",
        f"Etsy Bank Fees\n${bank_by_cat.get('Etsy Fees', 0):,.0f}",
        f"Craft Supplies\n${bank_by_cat.get('Craft Supplies', 0):,.0f}",
        f"Subscriptions\n${bank_by_cat.get('Subscriptions', 0):,.0f}",
        f"Shipping Supplies\n${bank_by_cat.get('Shipping', 0):,.0f}",
        f"TJ (Owner)\n${bank_by_cat.get('Owner Draw - Tulsa', 0):,.0f}",
        f"Braden (Owner)\n${bank_by_cat.get('Owner Draw - Texas', 0):,.0f}",
        f"Capital One Bank\n${bank_net_cash:,.0f}",
        f"Etsy Account\n${etsy_balance:,.0f}",
        f"Prior Bank Receipts\n${old_bank_receipted:,.0f}",
        f"Untracked Etsy\n${etsy_csv_gap:,.0f}",
        f"Unmatched Bank\n${bank_unaccounted:,.0f}",
        f"Fees\n${abs(total_fees):,.0f}",
        f"Shipping Labels\n${abs(total_shipping_cost):,.0f}",
        f"Ads/Marketing\n${abs(total_marketing):,.0f}",
        f"Sales Tax\n${abs(total_taxes):,.0f}",
        f"Refunds\n${abs(total_refunds + total_buyer_fees):,.0f}",
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
        _etsy_took, etsy_net_earned,
        bank_all_expenses, bank_owner_draw_total, bank_cash_on_hand, old_bank_receipted + bank_unaccounted + etsy_csv_gap,
        bank_by_cat.get("Amazon Inventory", 0), bank_by_cat.get("AliExpress Supplies", 0),
        bank_by_cat.get("Business Credit Card", 0), bank_by_cat.get("Etsy Fees", 0),
        bank_by_cat.get("Craft Supplies", 0), bank_by_cat.get("Subscriptions", 0),
        bank_by_cat.get("Shipping", 0),
        bank_by_cat.get("Owner Draw - Tulsa", 0), bank_by_cat.get("Owner Draw - Texas", 0),
        bank_net_cash, etsy_balance,
        old_bank_receipted, max(bank_unaccounted, 0.01), max(etsy_csv_gap, 0.01),
        abs(total_fees), abs(total_shipping_cost), abs(total_marketing),
        abs(total_taxes), abs(total_refunds + total_buyer_fees),
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
                dcc.Dropdown(id={"type": "wiz-cat", "index": idx},
                             options=cat_options, value=det_cat or "", clearable=False,
                             placeholder="Select category...",
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
                    dcc.Dropdown(id={"type": "wiz-item-loc", "index": idx},
                                 options=loc_options, value=det_loc or "", clearable=False,
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


def _build_item_card(idx, item_name, img_url, det_name, det_cat, det_qty, det_loc,
                     has_details, orig_qty, orig_total, is_split, existing, onum):
    """Build a single inventory item card (compact row + expandable form).

    Shared by _build_inventory_editor() and filter_editor().
    """
    _label = {"color": GRAY, "fontSize": "12px", "marginRight": "6px", "whiteSpace": "nowrap", "fontWeight": "500"}
    _inp = {"fontSize": "13px", "backgroundColor": "#0d0d1a", "color": WHITE,
            "border": f"1px solid {DARKGRAY}55", "borderRadius": "6px", "padding": "7px 12px"}
    cat_options = [{"label": c, "value": c} for c in CATEGORY_OPTIONS]
    loc_options = [{"label": "Tulsa, OK", "value": "Tulsa, OK"},
                   {"label": "Texas", "value": "Texas"},
                   {"label": "Other", "value": "Other"}]

    per_unit = orig_total / det_qty if det_qty > 0 else (orig_total / orig_qty if orig_qty > 0 else 0)
    _card_border = GREEN if has_details else ORANGE
    _status_color = GREEN if has_details else ORANGE
    _loc_color = TEAL if "Tulsa" in det_loc else (ORANGE if "Texas" in det_loc else GRAY)

    # ── COMPACT ROW (two-line layout) ──
    # Line 1: name + price + qty
    # Line 2: category · location + status pill
    compact_row = html.Div([
        html.Div(
            item_thumbnail(img_url, 40),
            style={"flexShrink": "0"},
        ),
        html.Div([
            # Line 1
            html.Div([
                html.Span(det_name[:60], title=det_name,
                          style={"color": WHITE, "fontSize": "15px", "fontWeight": "bold",
                                 "flex": "1", "overflow": "hidden", "textOverflow": "ellipsis",
                                 "whiteSpace": "nowrap", "minWidth": "100px"}),
                html.Span(f"${orig_total:.2f}", style={
                    "color": ORANGE, "fontSize": "13px", "fontWeight": "bold",
                    "marginLeft": "auto", "flexShrink": "0"}),
                html.Span(f"x{det_qty}", style={
                    "color": WHITE, "fontSize": "13px", "fontWeight": "bold",
                    "marginLeft": "12px", "flexShrink": "0"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "8px"}),
            # Line 2
            html.Div([
                html.Span(det_cat, style={
                    "fontSize": "12px", "color": TEAL, "fontWeight": "500"}),
                html.Span("\u00b7", style={"color": f"{DARKGRAY}88", "margin": "0 6px",
                                            "fontSize": "14px"}),
                html.Span(det_loc if det_loc else "\u2014", style={
                    "fontSize": "12px", "color": _loc_color, "fontWeight": "500"}),
                html.Span(
                    "\u2713 SAVED" if has_details else "UNSAVED",
                    className="status-pill-saved" if has_details else "status-pill-unsaved",
                    style={"fontSize": "12px", "fontWeight": "bold", "padding": "3px 12px",
                           "borderRadius": "10px", "whiteSpace": "nowrap",
                           "backgroundColor": f"{_status_color}18", "color": _status_color,
                           "border": f"1px solid {_status_color}33",
                           "marginLeft": "auto", "flexShrink": "0"}),
                html.Span(
                    "NEW", className="new-badge",
                    style={"fontSize": "10px", "fontWeight": "bold", "padding": "2px 8px",
                           "borderRadius": "8px", "backgroundColor": f"{CYAN}22",
                           "color": CYAN, "border": f"1px solid {CYAN}55",
                           "marginLeft": "6px", "flexShrink": "0",
                           "letterSpacing": "0.5px"}
                ) if (onum in _RECENT_UPLOADS and not has_details) else None,
            ], style={"display": "flex", "alignItems": "center", "marginTop": "2px"}),
        ], style={"flex": "1", "minWidth": "0", "marginLeft": "12px"}),
    ], style={"display": "flex", "alignItems": "center",
              "cursor": "pointer", "padding": "4px 0"})

    # ── EXPANDED FORM ──
    # Original name + price info header
    show_orig = (det_name != item_name)
    orig_label = html.Div([
        html.Div(
            item_thumbnail(img_url, 56),
            style={"flexShrink": "0", "borderRadius": "6px", "overflow": "hidden"},
        ),
        html.Div([
            html.Div([
                html.Span("ORIGINAL: ", style={"color": GRAY, "fontSize": "11px", "fontWeight": "bold"}),
                html.Span(f'"{item_name[:90]}"',
                          style={"color": f"{WHITE}bb", "fontSize": "11px", "fontStyle": "italic"}),
            ] if show_orig else [
                html.Span(item_name[:90],
                          style={"color": WHITE, "fontSize": "13px", "fontWeight": "600",
                                 "wordBreak": "break-word"}),
            ], style={"marginBottom": "4px"}),
            html.Span(
                id={"type": "det-price-display", "index": idx},
                children=f"qty {det_qty}  \u00b7  ${per_unit:.2f}/ea  \u00b7  ${orig_total:.2f} total",
                style={"color": GRAY, "fontSize": "12px"}),
        ], style={"marginLeft": "14px", "flex": "1"}),
    ], style={"display": "flex", "alignItems": "flex-start", "marginBottom": "16px"})

    # Form fields — stacked rows with aligned labels
    form_rows = html.Div([
        html.Div([
            html.Span("Name", style={**_label, "width": "70px", "textAlign": "right"}),
            dcc.Input(id={"type": "det-name", "index": idx}, type="text",
                      value=det_name,
                      style={**_inp, "flex": "1", "minWidth": "180px"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "8px"}),
        html.Div([
            html.Span("Category", style={**_label, "width": "70px", "textAlign": "right"}),
            dcc.Dropdown(id={"type": "det-cat", "index": idx},
                         options=cat_options, value=det_cat, clearable=False,
                         style={"width": "180px", "fontSize": "13px",
                                "backgroundColor": "#0d0d1a", "color": WHITE}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "8px"}),
        html.Div([
            html.Span("Qty", style={**_label, "width": "70px", "textAlign": "right"}),
            dcc.Input(id={"type": "det-qty", "index": idx}, type="number",
                      min=1, value=det_qty, debounce=False,
                      style={**_inp, "width": "65px"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "8px"}),
        html.Div([
            html.Span("Location", style={**_label, "width": "70px", "textAlign": "right"}),
            dcc.Dropdown(id={"type": "loc-dropdown", "index": idx},
                         options=loc_options, value=det_loc, clearable=False,
                         style={"width": "180px", "fontSize": "13px",
                                "backgroundColor": "#0d0d1a", "color": WHITE}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "10px"}),
        # Split checkbox on its own row
        dcc.Checklist(
            id={"type": "loc-split-check", "index": idx},
            options=[{"label": " Split into multiple items", "value": "split"}],
            value=["split"] if is_split else [],
            style={"fontSize": "12px", "color": GRAY, "marginBottom": "12px", "marginLeft": "76px"},
            labelStyle={"cursor": "pointer"},
        ),
    ])

    # Action buttons — Save + Reset only
    action_row = html.Div([
        html.Button("Save", id={"type": "det-save-btn", "index": idx},
                    style={"fontSize": "12px", "padding": "8px 24px", "backgroundColor": TEAL,
                           "color": WHITE, "border": "none", "borderRadius": "6px",
                           "cursor": "pointer", "fontWeight": "bold",
                           "boxShadow": f"0 2px 6px {TEAL}55",
                           "transition": "all 0.15s ease"}),
        html.Button("Reset", id={"type": "det-reset-btn", "index": idx},
                    style={"fontSize": "12px", "padding": "7px 18px", "backgroundColor": "transparent",
                           "color": GRAY, "border": f"1px solid {DARKGRAY}55", "borderRadius": "6px",
                           "cursor": "pointer"}),
        html.Span(id={"type": "det-status", "index": idx}, children="",
                  style={"fontSize": "12px", "color": GREEN, "fontWeight": "bold", "marginLeft": "8px"}),
    ], style={"display": "flex", "gap": "12px", "alignItems": "center"})

    # Split container
    split_container = _build_split_container(idx, existing, det_name, det_cat, det_qty, det_loc, _inp,
                                               item_name=item_name, orig_total=orig_total)

    # Hidden stores
    hidden = html.Div([
        dcc.Store(id={"type": "det-order-num", "index": idx}, data=onum),
        dcc.Store(id={"type": "det-item-name", "index": idx}, data=item_name),
        dcc.Store(id={"type": "det-orig-qty", "index": idx}, data=orig_qty),
        dcc.Store(id={"type": "det-orig-name", "index": idx}, data=item_name),
        dcc.Store(id={"type": "det-orig-total", "index": idx}, data=orig_total),
    ])

    _item_shadow = f"0 2px 8px rgba(0,0,0,0.3), -4px 0 12px {GREEN}11" if has_details else "0 2px 8px rgba(0,0,0,0.3)"
    return html.Details([
        html.Summary(compact_row, style={
            "listStyle": "none", "outline": "none", "userSelect": "none",
            "WebkitAppearance": "none"}),
        html.Div([orig_label, form_rows, split_container, action_row, hidden],
                 style={"paddingTop": "12px", "borderTop": f"1px solid {DARKGRAY}22",
                        "marginTop": "8px"}),
    ], open=not has_details,
       className="item-card",
       style={"padding": "12px 16px", "marginBottom": "8px",
               "backgroundColor": "#0f1225", "borderRadius": "8px",
               "borderLeft": f"4px solid {_card_border}",
               "boxShadow": _item_shadow,
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
                dcc.Dropdown(
                    id={"type": "catmgr-cat", "index": item_key},
                    options=cat_options, value=cat, clearable=False,
                    style={"width": "140px", "fontSize": "11px",
                           "backgroundColor": "#1a1a2e", "color": WHITE},
                ),
                style={"padding": "4px 6px"}),
            # Location dropdown
            html.Td(
                dcc.Dropdown(
                    id={"type": "catmgr-loc", "index": item_key},
                    options=loc_options, value=loc if loc in ("Tulsa, OK", "Texas", "Other") else "",
                    clearable=True, placeholder="—",
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


def _build_inventory_editor():
    """Build the Inventory Editor — clean per-item form for naming, categorizing, and locating."""
    if len(INV_ITEMS) == 0:
        return html.Div(id="editor-items-container")

    # Sort: recent uploads first, then by date descending
    _recent = [o for o in INVOICES if o["order_num"] in _RECENT_UPLOADS]
    _rest = [o for o in INVOICES if o["order_num"] not in _RECENT_UPLOADS]
    _recent.sort(key=lambda o: o.get("date", ""), reverse=True)
    _rest.sort(key=lambda o: o.get("date", ""), reverse=True)
    sorted_orders = _recent + _rest
    order_cards = []
    saved_count = 0
    total_items = 0

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

            total_items += 1
            order_total_items += 1
            orig_qty = item["qty"]
            price = item["price"]
            orig_total = round(price * orig_qty, 2)

            _seen_names[item_name] = _seen_names.get(item_name, 0) + 1
            if _seen_names[item_name] > 1:
                idx = f"{onum}__{item_name}__{_seen_names[item_name]}"
            else:
                idx = f"{onum}__{item_name}"

            img_url = _IMAGE_URLS.get(item_name, "")

            # Load existing saved details
            detail_key = (onum, item_name)
            existing = _ITEM_DETAILS.get(detail_key, [])
            has_details = bool(existing)
            if has_details:
                saved_count += 1
                order_saved += 1

            # Values for form fields
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

            is_split = len(existing) > 1

            item_cards.append(_build_item_card(
                idx, item_name, img_url, det_name, det_cat, det_qty, det_loc,
                has_details, orig_qty, orig_total, is_split, existing, onum))

        if not item_cards:
            continue

        order_total = sum(it["price"] * it["qty"] for it in inv["items"])
        _loc_color = TEAL if "Tulsa" in orig_location else (ORANGE if "Texas" in orig_location else GRAY)
        _prog_color = GREEN if order_saved == order_total_items else ORANGE
        _order_all_done = order_saved == order_total_items and order_total_items > 0
        _order_pct = round(order_saved / order_total_items * 100) if order_total_items > 0 else 0

        # Order progress bar (6px, 120px wide, gradient)
        mini_progress = html.Div([
            html.Div(style={"width": f"{max(_order_pct, 3)}%", "height": "6px",
                            "background": f"linear-gradient(90deg, {_prog_color}88, {_prog_color})",
                            "borderRadius": "3px",
                            "transition": "width 0.3s ease"}),
        ], style={"width": "120px", "height": "6px", "backgroundColor": "#0d0d1a",
                  "borderRadius": "3px", "display": "inline-block", "verticalAlign": "middle",
                  "marginLeft": "10px", "overflow": "hidden"})

        # ALL DONE pill (larger, with checkmark, pulsing glow)
        done_pill = html.Span("\u2713 ALL DONE", className="pulse-complete", style={
            "fontSize": "12px", "fontWeight": "bold", "padding": "4px 14px",
            "borderRadius": "12px", "backgroundColor": f"{GREEN}22", "color": GREEN,
            "border": f"1px solid {GREEN}44", "marginLeft": "10px",
            "letterSpacing": "0.5px",
            "textShadow": "0 0 8px #2ecc7144"}) if _order_all_done else None

        _is_recent_upload = onum in _RECENT_UPLOADS
        _new_upload_pill = html.Span(
            "NEW UPLOAD", className="new-badge",
            style={"fontSize": "10px", "fontWeight": "bold", "padding": "3px 10px",
                   "borderRadius": "10px", "backgroundColor": f"{CYAN}22",
                   "color": CYAN, "border": f"1px solid {CYAN}55",
                   "marginLeft": "10px", "letterSpacing": "0.5px"}
        ) if _is_recent_upload else None
        _header_gradient = f"linear-gradient(180deg, {CYAN}08, transparent)" if not _order_all_done else f"linear-gradient(180deg, {GREEN}08, transparent)"
        order_card = html.Div([
            html.Div([
                html.Div([c for c in [
                    html.Span(f"ORDER #{onum}", style={"color": CYAN, "fontWeight": "bold", "fontSize": "18px",
                                                        "letterSpacing": "0.5px"}),
                    html.Span(orig_location,
                              style={"fontSize": "11px", "padding": "3px 12px", "borderRadius": "12px",
                                     "backgroundColor": f"{_loc_color}22", "color": _loc_color,
                                     "border": f"1px solid {_loc_color}33", "marginLeft": "12px",
                                     "fontWeight": "bold", "whiteSpace": "nowrap"}),
                    _new_upload_pill,
                    done_pill,
                ] if c is not None], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}),
                html.Div([
                    html.Span(f"{inv['date']}", style={"color": GRAY, "fontSize": "12px"}),
                    html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66", "margin": "0 6px"}),
                    html.Span(f"{source_label}", style={"color": GRAY, "fontSize": "12px"}),
                    html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66", "margin": "0 6px"}),
                    html.Span(f"${order_total:.2f}", style={"color": WHITE, "fontSize": "14px", "fontWeight": "700"}),
                    html.Span([
                        html.Span(f"{order_saved}/{order_total_items} saved",
                                  style={"color": _prog_color, "fontSize": "13px", "fontWeight": "bold"}),
                        mini_progress,
                    ], style={"marginLeft": "auto", "display": "flex", "alignItems": "center"}),
                ], style={"display": "flex", "alignItems": "center"}),
            ], style={"marginBottom": "12px", "paddingBottom": "12px",
                      "borderBottom": f"1px solid {CYAN}18",
                      "background": _header_gradient,
                      "margin": "-18px -20px 12px -20px", "padding": "18px 20px 12px 20px",
                      "borderRadius": "10px 10px 0 0"}),
            html.Div(item_cards),
        ], className="order-card order-card-saved" if _order_all_done else "order-card",
           style={"backgroundColor": f"#2ecc7108" if _order_all_done else CARD2,
                  "padding": "18px 20px", "borderRadius": "10px",
                  "marginBottom": "14px",
                  "border": f"1px solid {GREEN}55" if _order_all_done else f"1px solid {CYAN}18",
                  "boxShadow": ("0 0 16px #2ecc7122, 0 4px 16px rgba(0,0,0,0.3)" if _order_all_done
                                else "0 2px 12px rgba(0,0,0,0.25)")})
        order_cards.append(order_card)

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

    # Filter bar
    editor_cats = ["All"] + sorted(set(
        categorize_item(it["name"])
        for inv in INVOICES if inv["source"] != "Personal Amazon"
        for it in inv["items"]
        if categorize_item(it["name"]) not in ("Personal/Gift", "Business Fees")
    ))
    filter_bar = html.Div([
        html.Div([
            html.Span("\U0001f50d", style={"fontSize": "14px", "marginRight": "6px"}),
            dcc.Input(id="editor-search", type="text", placeholder="Search items...",
                      style={"backgroundColor": "#0d0d1a", "color": WHITE,
                             "border": f"1px solid {DARKGRAY}44", "borderRadius": "6px",
                             "padding": "8px 14px", "fontSize": "13px", "width": "240px"}),
        ], style={"display": "flex", "alignItems": "center"}),
        html.Div(style={"width": "1px", "height": "24px", "backgroundColor": f"{DARKGRAY}33"}),
        html.Div([
            html.Span("Category:", style={"color": GRAY, "fontSize": "12px", "marginRight": "6px",
                                           "fontWeight": "500"}),
            dcc.Dropdown(id="editor-cat-filter",
                         options=[{"label": c, "value": c} for c in editor_cats],
                         value="All", clearable=False,
                         style={"width": "170px", "fontSize": "13px",
                                "backgroundColor": "#0d0d1a", "color": WHITE}),
        ], style={"display": "flex", "alignItems": "center"}),
        html.Div(style={"width": "1px", "height": "24px", "backgroundColor": f"{DARKGRAY}33"}),
        html.Div([
            html.Span("Show:", style={"color": GRAY, "fontSize": "12px", "marginRight": "6px",
                                       "fontWeight": "500"}),
            dcc.Dropdown(id="editor-status-filter",
                         options=[{"label": s, "value": s} for s in ["All", "Saved", "Unsaved"]],
                         value="All", clearable=False,
                         style={"width": "130px", "fontSize": "13px",
                                "backgroundColor": "#0d0d1a", "color": WHITE}),
        ], style={"display": "flex", "alignItems": "center"}),
        html.Div(style={"flex": "1"}),
        html.Button("\u2193 Jump to Next Unsaved", id="editor-jump-unsaved", n_clicks=0,
                    style={"fontSize": "12px", "padding": "8px 18px",
                           "background": f"linear-gradient(135deg, {ORANGE}, #e67e22)",
                           "color": WHITE, "border": "none", "borderRadius": "6px",
                           "cursor": "pointer", "fontWeight": "bold", "whiteSpace": "nowrap",
                           "boxShadow": f"0 2px 8px {ORANGE}44"}),
    ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap",
              "gap": "16px", "marginBottom": "16px", "padding": "12px 16px",
              "backgroundColor": "#0f1225", "borderRadius": "8px",
              "boxShadow": "0 1px 4px rgba(0,0,0,0.2)"})

    return html.Div([
        html.Div([
            html.H4("INVENTORY EDITOR",
                     style={"color": ORANGE, "margin": "0", "fontSize": "20px", "fontWeight": "700",
                            "letterSpacing": "1px"}),
            html.P("Name each item, pick a category, set qty & location, then Save.",
                   style={"color": GRAY, "fontSize": "13px", "margin": "4px 0 0 0"}),
        ], style={"marginBottom": "16px"}),
        progress_bar,
        filter_bar,
        html.Div(order_cards, id="editor-items-container",
                 style={"maxHeight": "800px", "overflowY": "auto", "padding": "4px",
                        "scrollbarWidth": "thin"}),
    ], style={"backgroundColor": CARD, "padding": "24px", "borderRadius": "12px",
              "marginBottom": "18px", "border": f"1px solid {ORANGE}22",
              "borderTop": f"5px solid {ORANGE}",
              "boxShadow": "0 4px 20px rgba(0,0,0,0.3)"})


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
                dcc.Dropdown(id="qa-category", options=cat_options, value="Other", clearable=False,
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
                dcc.Dropdown(id="qa-location", options=loc_options, value="Tulsa, OK", clearable=False,
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
            html.Button("Refresh", id="loc-inv-refresh-btn",
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

    # Item list (compact)
    item_rows = []
    for cat in sorted(loc_items.keys()):
        for name, qty in sorted(loc_items[cat], key=lambda x: x[0]):
            thumb_url = _IMAGE_URLS.get(name, "")
            item_rows.append(html.Div([
                item_thumbnail(thumb_url, 36) if thumb_url else html.Div(style={
                    "width": "36px", "height": "36px", "borderRadius": "6px",
                    "backgroundColor": f"{CARD2}", "border": f"1px dashed {GRAY}33",
                    "flexShrink": "0"}),
                html.Span(name[:40], title=name, style={"color": WHITE, "fontSize": "12px",
                          "marginLeft": "10px", "flex": "1", "overflow": "hidden",
                          "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
                html.Span(f"x{qty}", style={"color": GRAY, "fontSize": "12px",
                          "fontFamily": "monospace", "marginLeft": "8px", "fontWeight": "600"}),
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

            # Form fields row
            html.Div([
                html.Div([
                    html.Span("Display Name:", style=_label),
                    dcc.Input(id="wizard-name", type="text", value="",
                              style={**_inp, "flex": "1", "minWidth": "200px"}),
                ], style={"display": "flex", "alignItems": "center", "flex": "2"}),
                html.Div([
                    html.Span("Category:", style=_label),
                    dcc.Dropdown(id="wizard-cat", options=cat_options, value="Other",
                                 clearable=False,
                                 style={"width": "150px", "fontSize": "13px",
                                        "backgroundColor": "#0d0d1a", "color": WHITE}),
                ], style={"display": "flex", "alignItems": "center"}),
                html.Div([
                    html.Span("Qty:", style=_label),
                    dcc.Input(id="wizard-qty", type="number", min=1, value=1,
                              style={**_inp, "width": "65px"}),
                ], style={"display": "flex", "alignItems": "center"}),
                html.Div([
                    html.Span("Location:", style=_label),
                    dcc.Dropdown(id="wizard-loc", options=loc_options, value="Tulsa, OK",
                                 clearable=False,
                                 style={"width": "140px", "fontSize": "13px",
                                        "backgroundColor": "#0d0d1a", "color": WHITE}),
                ], style={"display": "flex", "alignItems": "center"}),
            ], id="wizard-form-row",
               style={"display": "flex", "alignItems": "center", "flexWrap": "wrap",
                       "gap": "12px", "marginBottom": "12px"}),

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

    ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
              "marginBottom": "14px", "border": f"1px solid {PURPLE}33",
              "borderLeft": f"4px solid {PURPLE}"})


def build_tab4_inventory():
    """Tab 4 - Inventory: Business inventory only (personal items under Owner Draws)"""
    biz_order_count = len(BIZ_INV_DF)

    # Build order table rows — EXCLUDE personal orders
    order_table_rows = []
    for _, r in INV_DF.iterrows():
        is_personal = r["source"] == "Personal Amazon" or (isinstance(r["file"], str) and "Gigi" in r["file"])
        if is_personal:
            continue
        src = r["source"]
        store_label = "Amazon" if src in ("Key Component Mfg",) else src
        ship_addr = r.get("ship_address", "")
        short_addr = ship_addr.split(",")[1].strip() + ", " + ship_addr.split(",")[2].strip().split(" ")[0] if ship_addr.count(",") >= 2 else ship_addr
        order_table_rows.append(
            html.Tr([
                html.Td(r["date"], style={"color": GRAY, "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(r["order_num"], style={"color": WHITE, "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(store_label, style={"color": TEAL, "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(short_addr, style={"color": CYAN, "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(str(r["item_count"]), style={"textAlign": "center", "color": WHITE,
                                                      "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(f"${r['subtotal']:,.2f}", style={"textAlign": "right", "color": WHITE,
                                                          "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(f"${r['tax']:,.2f}", style={"textAlign": "right", "color": GRAY,
                                                      "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(f"${r['grand_total']:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                              "fontWeight": "bold", "padding": "4px 8px",
                                                              "fontSize": "12px"}),
            ], style={"borderBottom": "1px solid #ffffff10"})
        )

    # Build item detail table rows — exclude personal/gift and biz fees
    item_table_rows = []
    items_sorted = INV_ITEMS.sort_values("total", ascending=False)
    for _, r in items_sorted.iterrows():
        if r.get("category", "") in ("Personal/Gift", "Business Fees"):
            continue
        item_src = r.get("source", "")
        store_name = "Amazon" if item_src in ("Key Component Mfg",) else item_src
        ship_loc = r.get("ship_to", "")
        if ship_loc.count(",") >= 2:
            parts = ship_loc.split(",")
            short_ship = parts[1].strip() + ", " + parts[2].strip().split(" ")[0]
        else:
            short_ship = ship_loc
        _img_url = _IMAGE_URLS.get(r["name"], "") or r.get("image_url", "")
        _item_orig = r.get("_orig_name", r["name"])
        _item_renamed = _item_orig != r["name"]
        _item_name_parts = [html.Span(r["name"], style={"color": WHITE})]
        if _item_renamed:
            _item_name_parts.append(html.Br())
            _item_name_parts.append(html.Span(
                _item_orig[:55], title=_item_orig,
                style={"color": DARKGRAY, "fontSize": "9px", "fontStyle": "italic"}))
        item_table_rows.append(
            html.Tr([
                html.Td(item_thumbnail(_img_url, 32), style={"padding": "4px 6px", "textAlign": "center", "width": "40px"}),
                html.Td(html.Span("INVENTORY", style={
                    "backgroundColor": "#00e67622", "color": GREEN, "padding": "2px 8px",
                    "borderRadius": "10px", "fontSize": "10px", "fontWeight": "600",
                    "letterSpacing": "0.5px"}), style={"padding": "4px 8px", "textAlign": "center"}),
                html.Td(_item_name_parts, title=f"{r['name']}\nFrom: {_item_orig}" if _item_renamed else r["name"],
                         style={"padding": "4px 8px", "fontSize": "11px",
                                "maxWidth": "350px", "overflow": "hidden",
                                "textOverflow": "ellipsis"}),
                html.Td(r.get("category", "Other"), style={"color": TEAL, "padding": "4px 8px", "fontSize": "11px"}),
                html.Td(store_name, style={"color": CYAN, "padding": "4px 8px", "fontSize": "11px"}),
                html.Td(short_ship[:30], style={"color": GRAY, "padding": "4px 8px", "fontSize": "11px"}),
                html.Td(str(r["qty"]), style={"textAlign": "center", "color": WHITE,
                                               "padding": "4px 8px", "fontSize": "11px"}),
                html.Td(f"${r['price']:,.2f}", style={"textAlign": "right", "color": WHITE,
                                                        "padding": "4px 8px", "fontSize": "11px"}),
                html.Td(f"${r['total']:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                        "fontWeight": "bold", "padding": "4px 8px",
                                                        "fontSize": "11px"}),
                html.Td(r["date"], style={"color": GRAY, "padding": "4px 8px", "fontSize": "11px"}),
            ], style={"borderBottom": "1px solid #ffffff10"})
        )

    # Get categories for filter dropdown
    _stock_cats = ["All"] + sorted(STOCK_SUMMARY["category"].unique().tolist()) if len(STOCK_SUMMARY) > 0 else ["All"]

    _sort_options = [
        {"label": "Category", "value": "Category"},
        {"label": "Name", "value": "Name"},
        {"label": "Stock Low\u2192High", "value": "Stock Low\u2192High"},
        {"label": "Stock High\u2192Low", "value": "Stock High\u2192Low"},
        {"label": "Value High\u2192Low", "value": "Value High\u2192Low"},
    ]

    return html.Div([

        # ══════════════════════════════════════════════════════════════════════
        # 1. HEADER ROW: title + subtitle + Quick Add button (right)
        # ══════════════════════════════════════════════════════════════════════
        html.Div([
            html.Div([
                html.H3("INVENTORY MANAGEMENT", style={"color": CYAN, "margin": "0", "fontSize": "26px",
                                                        "fontWeight": "700", "letterSpacing": "1.5px",
                                                        "textShadow": f"0 0 20px {CYAN}22"}),
                html.P(["Business inventory with stock tracking. ",
                        html.Span(f"{unique_item_count} unique items", style={"color": GREEN, "fontWeight": "bold"}),
                        f" across {biz_order_count} orders."],
                       style={"color": GRAY, "margin": "2px 0 0 0", "fontSize": "13px"}),
            ], style={"flex": "1"}),
            html.Button("+ Quick Add", id="qa-toggle-btn", n_clicks=0,
                        className="btn-gradient-green",
                        style={"fontSize": "13px", "padding": "10px 22px",
                               "background": f"linear-gradient(135deg, {GREEN}, #27ae60)",
                               "color": WHITE, "border": "none", "borderRadius": "8px",
                               "cursor": "pointer", "fontWeight": "bold", "whiteSpace": "nowrap",
                               "alignSelf": "flex-start",
                               "boxShadow": f"0 3px 12px {GREEN}33"}),
        ], style={"display": "flex", "alignItems": "flex-start", "gap": "16px", "marginBottom": "14px"}),

        # Editor-save trigger store
        dcc.Store(id="editor-save-trigger", data=0),

        # ══════════════════════════════════════════════════════════════════════
        # 2. KPI PILL STRIP
        # ══════════════════════════════════════════════════════════════════════
        html.Div(id="inv-kpi-row", children=_build_inv_kpi_row()),

        # ══════════════════════════════════════════════════════════════════════
        # 3. QUICK ADD PANEL (hidden by default, toggles via button)
        # ══════════════════════════════════════════════════════════════════════
        html.Div([
            html.H4("QUICK ADD PURCHASE", style={"color": GREEN, "margin": "0 0 8px 0", "fontSize": "15px"}),
            _build_quick_add_form(),
        ], id="qa-panel",
           style={"display": "none", "backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
                  "marginBottom": "14px", "border": f"1px solid {GREEN}33",
                  "borderLeft": f"4px solid {GREEN}"}),

        # ══════════════════════════════════════════════════════════════════════
        # 4. STOCK LEVELS: Health gauges + Stock table + Usage log
        # ══════════════════════════════════════════════════════════════════════

        # Health Panel (above stock table)
        html.Div(id="inv-health-panel", children=[_build_inventory_health_panel()]),

        # Stock table with filters
        html.Div([
            html.H4("STOCK LEVELS", style={"color": TEAL, "margin": "0 0 8px 0", "fontSize": "16px",
                                             "fontWeight": "700", "letterSpacing": "0.5px"}),
            # Filter bar with sort dropdown
            html.Div([
                dcc.Input(id="stock-filter-input", type="text", placeholder="Search items...",
                          style={"backgroundColor": "#0f0f1a", "color": WHITE,
                                 "border": f"1px solid {DARKGRAY}", "borderRadius": "4px",
                                 "padding": "6px 10px", "fontSize": "12px", "width": "200px"}),
                html.Div(style={"width": "1px", "height": "20px", "backgroundColor": f"{DARKGRAY}44"}),
                html.Span("Category:", style={"color": GRAY, "fontSize": "12px", "marginRight": "4px"}),
                dcc.Dropdown(id="stock-filter-cat",
                             options=[{"label": c, "value": c} for c in _stock_cats],
                             value="All", clearable=False,
                             style={"width": "160px", "fontSize": "12px",
                                    "backgroundColor": "#1a1a2e", "color": WHITE}),
                html.Div(style={"width": "1px", "height": "20px", "backgroundColor": f"{DARKGRAY}44"}),
                html.Span("Status:", style={"color": GRAY, "fontSize": "12px", "marginRight": "4px"}),
                dcc.Dropdown(id="stock-filter-status",
                             options=[{"label": s, "value": s} for s in
                                      ["All", "In Stock", "Low Stock", "Out of Stock"]],
                             value="All", clearable=False,
                             style={"width": "130px", "fontSize": "12px",
                                    "backgroundColor": "#1a1a2e", "color": WHITE}),
                html.Div(style={"width": "1px", "height": "20px", "backgroundColor": f"{DARKGRAY}44"}),
                html.Span("Sort:", style={"color": GRAY, "fontSize": "12px", "marginRight": "4px"}),
                dcc.Dropdown(id="stock-sort-by",
                             options=_sort_options, value="Category", clearable=False,
                             style={"width": "150px", "fontSize": "12px",
                                    "backgroundColor": "#1a1a2e", "color": WHITE}),
                html.Div(style={"flex": "1"}),
                dcc.Checklist(
                    id="stock-include-tax",
                    options=[{"label": " Receipt cost", "value": "tax"}],
                    value=[],
                    style={"fontSize": "11px", "color": GRAY},
                    labelStyle={"cursor": "pointer", "display": "flex", "alignItems": "center"},
                ),
            ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap",
                      "gap": "8px", "marginBottom": "10px", "padding": "10px 12px",
                      "backgroundColor": "#0f1225", "borderRadius": "8px"}),
            # Stock table
            html.Div(
                _build_stock_table_html(STOCK_SUMMARY),
                id="stock-table-container",
                style={"maxHeight": "600px", "overflowY": "auto"},
            ),
            # Usage log (collapsed)
            html.Details([
                html.Summary("USAGE LOG", style={
                    "color": CYAN, "fontSize": "13px", "fontWeight": "bold", "cursor": "pointer",
                    "padding": "8px 0", "marginTop": "12px", "borderTop": f"1px solid {DARKGRAY}33",
                    "paddingTop": "12px"}),
                html.Div(
                    _build_usage_log_html(),
                    id="usage-log-container",
                ),
            ]),
        ], style={"backgroundColor": CARD, "padding": "18px", "borderRadius": "12px",
                  "marginBottom": "18px", "border": f"1px solid {TEAL}33",
                  "borderTop": f"5px solid {TEAL}",
                  "boxShadow": "0 4px 20px rgba(0,0,0,0.3)"}),

        # ══════════════════════════════════════════════════════════════════════
        # 5. CATEGORY MANAGER — fast inline categorization
        # ══════════════════════════════════════════════════════════════════════
        html.Div([
            html.H4([
                html.Span("CATEGORY MANAGER", style={"color": PURPLE, "letterSpacing": "1px"}),
                html.Span(f"  —  {len(INV_ITEMS)} items", style={"color": GRAY, "fontSize": "12px",
                                                                    "fontWeight": "normal", "marginLeft": "8px"}),
            ], style={"margin": "0 0 10px 0", "fontSize": "16px", "fontWeight": "700"}),
            html.P("Change any item's category or location — saves instantly to Supabase.",
                   style={"color": GRAY, "fontSize": "12px", "marginBottom": "10px"}),
            html.Div(id="catmgr-container", children=[_build_category_manager()]),
        ], style={"backgroundColor": CARD, "padding": "18px", "borderRadius": "12px",
                  "marginBottom": "18px", "border": f"1px solid {PURPLE}33",
                  "borderTop": f"5px solid {PURPLE}",
                  "boxShadow": "0 4px 20px rgba(0,0,0,0.3)"}),

        # ══════════════════════════════════════════════════════════════════════
        # 6. INVENTORY EDITOR (detailed per-item with split wizard)
        # ══════════════════════════════════════════════════════════════════════
        _build_inventory_editor(),

        # ══════════════════════════════════════════════════════════════════════
        # 6. RECEIPT UPLOAD
        # ══════════════════════════════════════════════════════════════════════
        _build_receipt_upload_section(),

        # ══════════════════════════════════════════════════════════════════════
        # 7. IMAGE MANAGER (collapsed)
        # ══════════════════════════════════════════════════════════════════════
        html.Details([
            html.Summary("IMAGE MANAGER", style={
                "color": CYAN, "fontSize": "15px", "fontWeight": "bold", "cursor": "pointer",
                "padding": "8px 0",
            }),
            _build_image_manager(),
        ], open=False,
           style={"backgroundColor": CARD, "padding": "12px 16px", "borderRadius": "10px",
                  "marginBottom": "14px", "border": f"1px solid {CYAN}33",
                  "borderLeft": f"4px solid {CYAN}"}),

        # ══════════════════════════════════════════════════════════════════════
        # 8. WAREHOUSES (location cards)
        # ══════════════════════════════════════════════════════════════════════
        _build_enhanced_location_section(),

        # ══════════════════════════════════════════════════════════════════════
        # 9. ANALYTICS (collapsed)
        # ══════════════════════════════════════════════════════════════════════
        html.Details([
            html.Summary("ANALYTICS & REFERENCE DATA", style={
                "color": PURPLE, "fontSize": "16px", "fontWeight": "700", "cursor": "pointer",
                "padding": "10px 0", "letterSpacing": "1.5px",
            }),
            html.Div([
                # Charts
                html.Div([
                    html.Div([dcc.Graph(figure=inv_monthly_fig, config={"displayModeBar": False})], style={"flex": "3"}),
                    html.Div([dcc.Graph(figure=inv_cat_fig, config={"displayModeBar": False})], style={"flex": "2"}),
                ], style={"display": "flex", "gap": "8px", "marginBottom": "10px"}),
                dcc.Graph(figure=rev_cogs_fig, config={"displayModeBar": False}),

                # Location Spending Breakdown
                html.Details([
                    html.Summary("LOCATION SPENDING BREAKDOWN", style={
                        "color": CYAN, "fontSize": "14px", "fontWeight": "bold", "cursor": "pointer",
                        "padding": "8px 0",
                    }),
                    html.Div([
                        dcc.Graph(figure=loc_monthly_fig, config={"displayModeBar": False}),
                        html.Div([
                            html.Div([dcc.Graph(figure=tulsa_cat_fig, config={"displayModeBar": False})], style={"flex": "1"}),
                            html.Div([dcc.Graph(figure=texas_cat_fig, config={"displayModeBar": False})], style={"flex": "1"}),
                        ], style={"display": "flex", "gap": "8px", "marginBottom": "10px"}),
                        html.Div([
                            html.Div([
                                html.H4("TJ (Tulsa) Categories", style={"color": TEAL, "margin": "0 0 8px 0", "fontSize": "14px"}),
                            ] + [
                                html.Div([
                                    html.Span(f"{cat}", style={"color": WHITE, "flex": "1"}),
                                    html.Span(f"${amt:,.2f}", style={"color": TEAL, "fontFamily": "monospace", "fontWeight": "bold"}),
                                ], style={"display": "flex", "padding": "3px 8px", "borderBottom": "1px solid #ffffff10"})
                                for cat, amt in tulsa_by_cat.items()
                            ] + [
                                html.Div([
                                    html.Span("TOTAL", style={"color": TEAL, "flex": "1", "fontWeight": "bold"}),
                                    html.Span(f"${tulsa_by_cat.sum():,.2f}", style={"color": TEAL, "fontFamily": "monospace", "fontWeight": "bold"}),
                                ], style={"display": "flex", "padding": "6px 8px", "borderTop": f"2px solid {TEAL}"}),
                            ], style={"backgroundColor": CARD, "padding": "12px", "borderRadius": "10px", "flex": "1"}),
                            html.Div([
                                html.H4("Braden (Texas) Categories", style={"color": ORANGE, "margin": "0 0 8px 0", "fontSize": "14px"}),
                            ] + [
                                html.Div([
                                    html.Span(f"{cat}", style={"color": WHITE, "flex": "1"}),
                                    html.Span(f"${amt:,.2f}", style={"color": ORANGE, "fontFamily": "monospace", "fontWeight": "bold"}),
                                ], style={"display": "flex", "padding": "3px 8px", "borderBottom": "1px solid #ffffff10"})
                                for cat, amt in texas_by_cat.items()
                            ] + [
                                html.Div([
                                    html.Span("TOTAL", style={"color": ORANGE, "flex": "1", "fontWeight": "bold"}),
                                    html.Span(f"${texas_by_cat.sum():,.2f}", style={"color": ORANGE, "fontFamily": "monospace", "fontWeight": "bold"}),
                                ], style={"display": "flex", "padding": "6px 8px", "borderTop": f"2px solid {ORANGE}"}),
                            ], style={"backgroundColor": CARD, "padding": "12px", "borderRadius": "10px", "flex": "1"}),
                        ], style={"display": "flex", "gap": "12px", "marginBottom": "14px"}),
                    ]),
                ], open=False,
                   style={"backgroundColor": CARD2, "padding": "12px 16px", "borderRadius": "10px",
                          "marginBottom": "14px", "border": f"1px solid {CYAN}33"}),

                # Payment Methods
                html.Details([
                    html.Summary("PAYMENT METHODS", style={
                        "color": CYAN, "fontSize": "14px", "fontWeight": "bold", "cursor": "pointer",
                        "padding": "8px 0",
                    }),
                    html.Div([
                        html.P("Breakdown by payment card.",
                               style={"color": GRAY, "margin": "0 0 12px 0", "fontSize": "13px"}),
                        *_build_payment_sections(),
                    ]),
                ], open=False,
                   style={"backgroundColor": CARD2, "padding": "12px 16px", "borderRadius": "10px",
                          "marginBottom": "14px", "border": f"1px solid {CYAN}33"}),

                # All Orders Table
                html.Details([
                    html.Summary(f"ALL ORDERS ({inv_order_count} orders)", style={
                        "color": PURPLE, "fontSize": "14px", "fontWeight": "bold", "cursor": "pointer",
                        "padding": "8px 0",
                    }),
                    html.Div([
                        html.Table([
                            html.Thead(html.Tr([
                                html.Th("Date", style={"textAlign": "left", "padding": "6px 8px"}),
                                html.Th("Order #", style={"textAlign": "left", "padding": "6px 8px"}),
                                html.Th("Store", style={"textAlign": "left", "padding": "6px 8px"}),
                                html.Th("Shipped To", style={"textAlign": "left", "padding": "6px 8px"}),
                                html.Th("Items", style={"textAlign": "center", "padding": "6px 8px"}),
                                html.Th("Subtotal", style={"textAlign": "right", "padding": "6px 8px"}),
                                html.Th("Tax", style={"textAlign": "right", "padding": "6px 8px"}),
                                html.Th("Total", style={"textAlign": "right", "padding": "6px 8px"}),
                            ], style={"borderBottom": f"2px solid {PURPLE}"})),
                            html.Tbody(order_table_rows + [
                                html.Tr([
                                    html.Td("TOTAL", style={"color": ORANGE, "fontWeight": "bold", "padding": "6px 8px"}),
                                    html.Td("", style={"padding": "6px 8px"}),
                                    html.Td("", style={"padding": "6px 8px"}),
                                    html.Td("", style={"padding": "6px 8px"}),
                                    html.Td(str(INV_DF["item_count"].sum()), style={"textAlign": "center", "color": ORANGE,
                                                                                     "fontWeight": "bold", "padding": "6px 8px"}),
                                    html.Td(f"${total_inv_subtotal:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                                                   "fontWeight": "bold", "padding": "6px 8px"}),
                                    html.Td(f"${total_inv_tax:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                                               "fontWeight": "bold", "padding": "6px 8px"}),
                                    html.Td(f"${total_inventory_cost:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                                                     "fontWeight": "bold", "fontSize": "14px",
                                                                                     "padding": "6px 8px"}),
                                ], style={"borderTop": f"3px solid {ORANGE}"}),
                            ]),
                        ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
                    ]),
                ], open=False,
                   style={"backgroundColor": CARD2, "padding": "12px 16px", "borderRadius": "10px",
                          "marginBottom": "14px", "border": f"1px solid {PURPLE}33"}),

                # All Items Table
                html.Details([
                    html.Summary("ALL INVENTORY ITEMS (sorted by cost)", style={
                        "color": TEAL, "fontSize": "14px", "fontWeight": "bold", "cursor": "pointer",
                        "padding": "8px 0",
                    }),
                    html.Div([
                        html.P("Business supplies only.", style={"color": GRAY, "fontSize": "12px", "marginBottom": "8px"}),
                        html.Div([
                            html.Table([
                                html.Thead(html.Tr([
                                    html.Th("", style={"padding": "6px 4px", "width": "44px"}),
                                    html.Th("Type", style={"textAlign": "center", "padding": "6px 8px", "width": "80px"}),
                                    html.Th("Item Name", style={"textAlign": "left", "padding": "6px 8px"}),
                                    html.Th("Category", style={"textAlign": "left", "padding": "6px 8px"}),
                                    html.Th("Store", style={"textAlign": "left", "padding": "6px 8px"}),
                                    html.Th("Shipped To", style={"textAlign": "left", "padding": "6px 8px"}),
                                    html.Th("Qty", style={"textAlign": "center", "padding": "6px 8px"}),
                                    html.Th("Unit Price", style={"textAlign": "right", "padding": "6px 8px"}),
                                    html.Th("Total", style={"textAlign": "right", "padding": "6px 8px"}),
                                    html.Th("Date", style={"textAlign": "left", "padding": "6px 8px"}),
                                ], style={"borderBottom": f"2px solid {TEAL}"})),
                                html.Tbody(item_table_rows),
                            ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
                        ], style={"maxHeight": "600px", "overflowY": "auto"}),
                    ]),
                ], open=False,
                   style={"backgroundColor": CARD2, "padding": "12px 16px", "borderRadius": "10px",
                          "marginBottom": "14px", "border": f"1px solid {TEAL}33"}),

                # Spending by Category
                html.Details([
                    html.Summary("SPENDING BY CATEGORY", style={
                        "color": ORANGE, "fontSize": "14px", "fontWeight": "bold", "cursor": "pointer",
                        "padding": "8px 0",
                    }),
                    html.Div([
                        html.Div([
                            row_item(f"{cat}", amt, color=WHITE)
                            for cat, amt in inv_by_category.items()
                        ] + [
                            html.Div(style={"borderTop": f"2px solid {ORANGE}", "marginTop": "8px"}),
                            row_item("TOTAL (all items, subtotal only)", INV_ITEMS["total"].sum(), bold=True, color=ORANGE),
                        ]) if len(inv_by_category) > 0 else html.P("No items found.", style={"color": GRAY}),
                    ]),
                ], open=False,
                   style={"backgroundColor": CARD2, "padding": "12px 16px", "borderRadius": "10px",
                          "marginBottom": "14px", "border": f"1px solid {ORANGE}33"}),
            ]),
        ], open=False,
           style={"backgroundColor": CARD2, "padding": "14px 20px", "borderRadius": "12px",
                  "marginBottom": "14px", "border": f"1px solid {PURPLE}33",
                  "borderTop": f"4px solid {PURPLE}"}),

        # ── Receipt Gallery ─────────────────────────────────────────────────
        _build_receipt_gallery(),

    ], style={"padding": TAB_PADDING})


def _build_receipt_gallery():
    """Build a visual receipt gallery with embedded PDF viewers and parsed specs."""
    import urllib.parse as _ul

    def _make_receipt_card(inv, is_personal=False):
        """Build a single receipt card with PDF viewer + specs."""
        source = inv.get("source", "Unknown")
        subfolder = _SOURCE_FOLDER_MAP.get(source, "keycomp")
        raw_file = inv.get("file", "")

        # Strip " (page X)" suffix for multi-page scanned receipts
        clean_file = re.sub(r'\s*\(page\s*\d+\)$', '', raw_file)
        encoded_file = _ul.quote(clean_file)

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

    # Sort invoices by date (newest first)
    sorted_invoices = sorted(INVOICES, key=lambda o: o.get("date", ""), reverse=True)
    try:
        sorted_invoices = sorted(INVOICES,
            key=lambda o: pd.to_datetime(o.get("date", ""), format="%B %d, %Y", errors="coerce"),
            reverse=True)
    except Exception:
        pass

    biz_cards = []
    personal_cards = []
    for inv in sorted_invoices:
        is_personal = inv.get("source") == "Personal Amazon" or (
            isinstance(inv.get("file", ""), str) and "Gigi" in inv.get("file", ""))
        card = _make_receipt_card(inv, is_personal=is_personal)
        if is_personal:
            personal_cards.append(card)
        else:
            biz_cards.append(card)

    gallery_children = [
        html.H5(f"RECEIPT GALLERY  ({len(biz_cards)} business)", style={
            "color": CYAN, "fontWeight": "bold", "marginBottom": "4px", "fontSize": "15px",
        }),
        html.P("Every uploaded receipt with embedded PDF viewer and parsed specs.",
               style={"color": GRAY, "fontSize": "12px", "marginBottom": "14px"}),
    ] + biz_cards

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


def _get_bank_computed():
    """Compute values needed by the financials tab from bank data."""
    cat_color_map = {
        "Amazon Inventory": ORANGE, "Shipping": BLUE, "Craft Supplies": TEAL,
        "Etsy Fees": PURPLE, "Subscriptions": CYAN, "AliExpress Supplies": "#e91e63",
        "Owner Draw - Texas": "#ff9800", "Owner Draw - Tulsa": "#ffb74d",
        "Personal": PINK, "Pending": DARKGRAY, "Etsy Payout": GREEN,
        "Business Credit Card": BLUE,
    }
    total_taken = bank_owner_draw_total + bank_personal
    acct_total = bank_cash_on_hand + total_taken + bank_all_expenses + old_bank_receipted + bank_unaccounted + etsy_csv_gap
    acct_gap = round(etsy_net_earned - acct_total, 2)

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
app.title = "TJs Software Project"

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
    return flask.jsonify({
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
    })


@server.route("/api/reload")
def api_reload():
    """Force-reload all data from Supabase. Use after migrating data to refresh Railway."""
    global DATA, CONFIG, INVOICES, BANK_TXNS
    global sales_df, fee_df, ship_df, mkt_df, refund_df, tax_df
    global deposit_df, buyer_fee_df
    global gross_sales, total_refunds, net_sales, total_fees, total_fees_gross
    global total_shipping_cost, total_marketing, total_taxes
    global etsy_net, order_count, avg_order, etsy_net_margin
    global total_buyer_fees, etsy_net_earned, etsy_total_deposited
    global etsy_balance_calculated, etsy_csv_gap, etsy_balance
    global bank_deposits, bank_debits
    global bank_total_deposits, bank_total_debits, bank_net_cash
    global bank_by_cat, bank_monthly, bank_statement_count
    global bank_tax_deductible, bank_personal, bank_pending
    global bank_biz_expense_total, bank_all_expenses, bank_cash_on_hand
    global bank_owner_draw_total, real_profit, real_profit_margin
    global tulsa_draws, texas_draws, tulsa_draw_total, texas_draw_total
    global draw_diff, draw_owed_to
    global bank_txns_sorted, bank_running
    global bb_cc_payments, bb_cc_total_paid, bb_cc_balance, bb_cc_available
    global _bank_cat_color_map, _bank_acct_gap, _bank_no_receipt, _bank_amazon_txns
    global monthly_sales, monthly_fees, monthly_shipping, monthly_marketing
    global monthly_refunds, monthly_taxes, monthly_raw_fees, monthly_raw_shipping
    global monthly_raw_marketing, monthly_raw_refunds, monthly_net_revenue
    global daily_sales, daily_orders, daily_df, weekly_aov
    global monthly_order_counts, monthly_aov, monthly_profit_per_order
    global months_sorted, days_active
    global listing_fees, transaction_fees_product, transaction_fees_shipping
    global processing_fees, credit_transaction, credit_listing, credit_processing
    global share_save, total_credits
    global etsy_ads, offsite_ads_fees, offsite_ads_credits
    global usps_outbound, usps_outbound_count, usps_return, usps_return_count
    global asendia_labels, asendia_count, ship_adjustments, ship_adjust_count
    global ship_credits, ship_credit_count, ship_insurance, ship_insurance_count
    global buyer_paid_shipping, shipping_profit, shipping_margin
    global paid_ship_count, free_ship_count, avg_outbound_label
    global product_fee_totals, product_revenue_est
    global profit, profit_margin, receipt_cogs_outside_bank, bank_amazon_inv

    try:
        # 1. Reload raw data from Supabase
        sb = _load_data()
        DATA = sb["DATA"]
        CONFIG = sb["CONFIG"]
        INVOICES = sb["INVOICES"]
        BANK_TXNS = sb["BANK_TXNS"]

        # 2. Rebuild Etsy derived metrics (mirrors _reload_etsy_data logic)
        sales_df = DATA[DATA["Type"] == "Sale"]
        fee_df = DATA[DATA["Type"] == "Fee"]
        ship_df = DATA[DATA["Type"] == "Shipping"]
        mkt_df = DATA[DATA["Type"] == "Marketing"]
        refund_df = DATA[DATA["Type"] == "Refund"]
        tax_df = DATA[DATA["Type"] == "Tax"]
        deposit_df = DATA[DATA["Type"] == "Deposit"]
        buyer_fee_df = fee_df[fee_df["Title"].str.contains("Regulatory operating fee|Sales tax paid", case=False, na=False)]

        gross_sales = sales_df["Amount_Clean"].sum()
        total_refunds = abs(refund_df["Net_Clean"].sum())
        net_sales = gross_sales - total_refunds
        total_fees = abs(fee_df["Net_Clean"].sum())
        total_fees_gross = abs(fee_df["Net_Clean"].sum())
        total_shipping_cost = abs(ship_df["Net_Clean"].sum())
        total_marketing = abs(mkt_df["Net_Clean"].sum())
        total_taxes = abs(tax_df["Net_Clean"].sum())
        total_buyer_fees = abs(buyer_fee_df["Net_Clean"].sum())
        etsy_net = DATA["Net_Clean"].sum()
        order_count = sales_df["Title"].str.extract(r"(Order #\d+)", expand=False).nunique()
        avg_order = gross_sales / order_count if order_count else 0
        etsy_net_margin = (etsy_net / gross_sales * 100) if gross_sales else 0

        # Etsy balance auto-calculation
        _dep_total = 0.0
        for _, _dr in deposit_df.iterrows():
            _m = _re_mod.search(r'([\d,]+\.\d+)', str(_dr.get("Title", "")))
            if _m:
                _dep_total += float(_m.group(1).replace(",", ""))
        etsy_total_deposited = _dep_total
        etsy_net_earned = etsy_net
        etsy_balance_calculated = round(etsy_net - _dep_total, 2)
        etsy_balance = max(0, etsy_balance_calculated)

        # Monthly aggregations
        monthly_sales = sales_df.groupby("Month")["Amount_Clean"].sum()
        monthly_fees = fee_df.groupby("Month")["Net_Clean"].sum().abs()
        monthly_shipping = ship_df.groupby("Month")["Net_Clean"].sum().abs()
        monthly_marketing = mkt_df.groupby("Month")["Net_Clean"].sum().abs()
        monthly_refunds = refund_df.groupby("Month")["Net_Clean"].sum().abs()
        monthly_taxes = tax_df.groupby("Month")["Net_Clean"].sum().abs()
        months_sorted = sorted(monthly_sales.index.tolist())

        # Daily aggregations
        daily_sales = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Amount_Clean"].sum()
        daily_orders = sales_df.groupby(sales_df["Date_Parsed"].dt.date)["Title"].apply(
            lambda x: x.str.extract(r"(Order #\d+)", expand=False).nunique())
        days_active = len(daily_sales)

        # 3. Rebuild bank derived metrics (mirrors _reload_bank_data logic)
        bank_deposits = [t for t in BANK_TXNS if t["type"] == "deposit"]
        bank_debits = [t for t in BANK_TXNS if t["type"] == "debit"]
        bank_total_deposits = sum(t["amount"] for t in bank_deposits)
        bank_total_debits = sum(t["amount"] for t in bank_debits)
        bank_net_cash = bank_total_deposits - bank_total_debits

        bank_by_cat = {}
        for t in bank_debits:
            cat = t["category"]
            bank_by_cat[cat] = bank_by_cat.get(cat, 0) + t["amount"]
        bank_by_cat = dict(sorted(bank_by_cat.items(), key=lambda x: -x[1]))

        bank_monthly = {}
        for t in BANK_TXNS:
            parts = t["date"].split("/")
            month_key = f"{parts[2]}-{parts[0]}"
            if month_key not in bank_monthly:
                bank_monthly[month_key] = {"deposits": 0, "debits": 0}
            if t["type"] == "deposit":
                bank_monthly[month_key]["deposits"] += t["amount"]
            else:
                bank_monthly[month_key]["debits"] += t["amount"]

        bank_tax_deductible = sum(amt for cat, amt in bank_by_cat.items() if cat in BANK_TAX_DEDUCTIBLE)
        bank_personal = bank_by_cat.get("Personal", 0)
        bank_pending = bank_by_cat.get("Pending", 0)

        _biz_cats = ["Shipping", "Craft Supplies", "Etsy Fees", "Subscriptions",
                     "AliExpress Supplies", "Business Credit Card"]
        bank_biz_expense_total = sum(bank_by_cat.get(c, 0) for c in _biz_cats)
        bank_all_expenses = bank_by_cat.get("Amazon Inventory", 0) + bank_biz_expense_total
        bank_cash_on_hand = bank_net_cash + etsy_balance
        bank_owner_draw_total = sum(v for k, v in bank_by_cat.items() if k.startswith("Owner Draw"))
        real_profit = bank_cash_on_hand + bank_owner_draw_total
        real_profit_margin = (real_profit / gross_sales * 100) if gross_sales else 0

        tulsa_draws = [t for t in bank_debits if t["category"] == "Owner Draw - Tulsa"]
        texas_draws = [t for t in bank_debits if t["category"] == "Owner Draw - Texas"]
        tulsa_draw_total = sum(t["amount"] for t in tulsa_draws)
        texas_draw_total = sum(t["amount"] for t in texas_draws)
        draw_diff = abs(tulsa_draw_total - texas_draw_total)
        draw_owed_to = "Braden" if tulsa_draw_total > texas_draw_total else "TJ"

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

        _bank_cat_color_map, _bank_acct_gap, _bank_no_receipt, _bank_amazon_txns = _get_bank_computed()

        bb_cc_payments = [{"date": t["date"], "desc": t["desc"], "amount": t["amount"]}
                          for t in BANK_TXNS if t["category"] == "Business Credit Card"
                          and "BEST BUY" in t.get("desc", "").upper()]
        bb_cc_total_paid = sum(p["amount"] for p in bb_cc_payments)
        bb_cc_balance = bb_cc_total_charged - bb_cc_total_paid
        bb_cc_available = bb_cc_limit - bb_cc_balance

        # 4. Profit
        bank_amazon_inv = bank_by_cat.get("Amazon Inventory", 0)
        receipt_cogs_outside_bank = 0
        profit = real_profit
        profit_margin = (profit / gross_sales * 100) if gross_sales else 0

        # 5. Rebuild charts and analytics
        _recompute_shipping_details()
        _recompute_analytics()
        _recompute_tax_years()
        _recompute_valuation()
        _rebuild_all_charts()

        return flask.jsonify({
            "status": "ok",
            "etsy_rows": len(DATA),
            "bank_txns": len(BANK_TXNS),
            "invoices": len(INVOICES),
            "profit": round(profit, 2),
            "bank_deposits": round(bank_total_deposits, 2),
            "bank_debits": round(bank_total_debits, 2),
            "owner_draws": round(bank_owner_draw_total, 2),
        })
    except Exception as e:
        return flask.jsonify({"status": "error", "message": str(e)}), 500


# Shared tab styling
tab_style = {
    "backgroundColor": CARD2, "color": GRAY, "border": "none",
    "padding": "10px 20px", "fontSize": "14px", "fontWeight": "600",
}
tab_selected_style = {
    **tab_style, "backgroundColor": CARD, "color": CYAN,
    "borderBottom": f"3px solid {CYAN}",
}


def build_tab5_tax_forms():
    """Tab 5 - Tax Forms: Balance Sheet, P&L, Form 1065, K-1s, SE Tax, Est. Payments"""

    # ── Local helpers ──

    def yr_header(year):
        period = "Oct - Dec (partial year)" if year == 2025 else "Jan - Feb (YTD)"
        return html.H4(f"TAX YEAR {year}  —  {period}",
                        style={"color": CYAN, "margin": "16px 0 8px 0", "fontSize": "14px",
                               "borderBottom": f"1px solid {CYAN}44", "paddingBottom": "4px"})

    def bs_row(label, beg, end, indent=0, bold=False):
        """Balance-sheet row with Beginning / End columns."""
        style = {
            "display": "flex", "padding": "3px 0", "borderBottom": "1px solid #ffffff10",
            "marginLeft": f"{indent * 20}px",
        }
        if bold:
            style["fontWeight"] = "bold"
            style["borderBottom"] = "2px solid #ffffff30"
            style["padding"] = "6px 0"
        return html.Div([
            html.Span(label, style={"flex": "2", "color": WHITE, "fontSize": "13px"}),
            html.Span(money(beg), style={"flex": "1", "textAlign": "right", "fontFamily": "monospace",
                       "fontSize": "13px", "color": GREEN if beg >= 0 else RED}),
            html.Span(money(end), style={"flex": "1", "textAlign": "right", "fontFamily": "monospace",
                       "fontSize": "13px", "color": GREEN if end >= 0 else RED}),
        ], style=style)

    def form_row(line_num, label, amount, indent=0, bold=False, color=WHITE):
        """IRS form line item row."""
        style = {
            "display": "flex", "justifyContent": "space-between",
            "padding": "4px 0", "borderBottom": "1px solid #ffffff10",
            "marginLeft": f"{indent * 20}px",
        }
        if bold:
            style["fontWeight"] = "bold"
            style["borderBottom"] = "2px solid #ffffff30"
            style["padding"] = "8px 0"
        disp_color = RED if amount < 0 else color
        prefix = f"Line {line_num}: " if line_num else ""
        return html.Div([
            html.Span(f"{prefix}{label}", style={"color": color if not bold else disp_color, "fontSize": "13px"}),
            html.Span(money(amount), style={"color": disp_color, "fontFamily": "monospace", "fontSize": "13px"}),
        ], style=style)

    def col_header():
        """Column headers for balance sheet."""
        return html.Div([
            html.Div("", style={"flex": "2"}),
            html.Div("Beginning of Year", style={"flex": "1", "textAlign": "right", "color": GRAY,
                      "fontSize": "11px", "fontWeight": "bold"}),
            html.Div("End of Year", style={"flex": "1", "textAlign": "right", "color": GRAY,
                      "fontSize": "11px", "fontWeight": "bold"}),
        ], style={"display": "flex", "padding": "4px 0", "borderBottom": f"2px solid {ORANGE}44"})

    def divider(color=ORANGE):
        return html.Div(style={"borderTop": f"2px solid {color}44", "margin": "6px 0"})

    children = []

    # ── Compute per-partner totals for summary bubbles ──
    _tj_total_tax = 0
    _br_total_tax = 0
    for _yr in (2025, 2026):
        _d = TAX_YEARS[_yr]
        _gp = _d["gross_sales"] - _d["refunds"] - _d["cogs"]
        _td = (_d["net_fees"] + _d["shipping"] + _d["marketing"]
               + _d["bank_biz_expense"] + _d["taxes_collected"] + _d["buyer_fees"])
        _oi = _gp - _td
        _ps = _oi / 2
        _nse = _ps * 0.9235
        _ssb = 168600 if _yr == 2025 else 176100
        _se = min(_nse, _ssb) * 0.124 + _nse * 0.029
        _eit = max(0, (_ps - _se / 2) * 0.22)
        _tj_total_tax += _se + _eit
        _br_total_tax += _se + _eit

    _tj_draws_all = sum(t["amount"] for t in tulsa_draws)
    _br_draws_all = sum(t["amount"] for t in texas_draws)

    children.append(html.H3("TAX LIABILITY SUMMARY",
                            style={"color": CYAN, "margin": "0 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Estimated total tax owed per partner (2025 + 2026 YTD combined). "
                           "Includes self-employment tax + estimated income tax (22% bracket).",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))
    children.append(html.Div([
        kpi_card("TJ OWES (tax)", money(_tj_total_tax), RED, f"Draws taken: {money(_tj_draws_all)}",
                 f"TJ's estimated tax liability: self-employment tax (Social Security 12.4% + Medicare 2.9% on 92.35% of net earnings) plus estimated income tax (22% bracket on partnership share minus half of SE tax). Draws taken: {money(_tj_draws_all)}. Draws are NOT taxable -- they're just advances against your partnership share."),
        kpi_card("BRADEN OWES (tax)", money(_br_total_tax), RED, f"Draws taken: {money(_br_draws_all)}",
                 f"Braden's estimated tax liability: same calculation as TJ (50/50 partnership). SE tax + income tax on his share of net income. Draws taken: {money(_br_draws_all)}. Tax is owed on partnership income regardless of draws taken."),
        kpi_card("COMBINED TAX", money(_tj_total_tax + _br_total_tax), ORANGE, "Both partners total",
                 f"Total tax owed by both partners combined across 2025 + 2026 YTD. This includes self-employment tax and estimated income tax. The partnership itself (LLC) doesn't pay taxes -- it passes through to partners via K-1 forms."),
        kpi_card("PER PARTNER", money(_tj_total_tax), CYAN, "50/50 split — identical",
                 f"Since it's a 50/50 LLC, each partner owes the same tax on their share. This should be paid quarterly via IRS Form 1040-ES to avoid underpayment penalties."),
    ], style={"display": "flex", "gap": "8px", "marginBottom": "20px", "flexWrap": "wrap"}))

    # ══════════════════════════════════════════════════════════════
    # SECTION A: BALANCE SHEET (Schedule L)
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("A: BALANCE SHEET  (Schedule L — Form 1065)",
                            style={"color": CYAN, "margin": "0 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Assets, liabilities, and partners' capital at beginning and end of each tax year.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]

        if yr == 2025:
            beg_cash = 0
            beg_equipment = 0
            beg_inventory = 0
            beg_cc_balance = 0
        else:
            p = TAX_YEARS[2025]
            beg_cash = p["bank_deposits"] - p["bank_debits"]
            beg_equipment = bb_cc_asset_value
            beg_inventory = p["inventory_cost"]
            beg_cc_balance = bb_cc_balance

        end_cash = d["bank_deposits"] - d["bank_debits"]
        if yr == 2026:
            end_cash = beg_cash + d["bank_deposits"] - d["bank_debits"]
        end_equipment = bb_cc_asset_value
        end_inventory = d["inventory_cost"]
        end_cc_balance = bb_cc_balance

        beg_total_assets = beg_cash + beg_equipment + beg_inventory
        end_total_assets = end_cash + end_equipment + end_inventory
        beg_capital = beg_total_assets - beg_cc_balance
        end_capital = end_total_assets - end_cc_balance

        children.append(yr_header(yr))
        children.append(section(f"BALANCE SHEET — {yr}", [
            col_header(),
            html.Div("ASSETS", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "8px", "marginBottom": "4px"}),
            bs_row("Cash (Bank + Etsy)", beg_cash, end_cash),
            bs_row("Equipment (3D Printers)", beg_equipment, end_equipment, indent=1),
            bs_row("Inventory", beg_inventory, end_inventory, indent=1),
            bs_row("TOTAL ASSETS", beg_total_assets, end_total_assets, bold=True),
            html.Div("LIABILITIES", style={"color": RED, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "8px", "marginBottom": "4px"}),
            bs_row("Best Buy Citi CC", beg_cc_balance, end_cc_balance),
            bs_row("TOTAL LIABILITIES", beg_cc_balance, end_cc_balance, bold=True),
            html.Div("PARTNERS' CAPITAL", style={"color": CYAN, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "8px", "marginBottom": "4px"}),
            bs_row("Total Partners' Capital", beg_capital, end_capital, bold=True),
            bs_row("  TJ (50%)", beg_capital / 2, end_capital / 2, indent=1),
            bs_row("  Braden (50%)", beg_capital / 2, end_capital / 2, indent=1),
        ], color=ORANGE))

    # ══════════════════════════════════════════════════════════════
    # SECTION B: INCOME STATEMENT / P&L
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("B: INCOME STATEMENT / P&L",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Profit & Loss per tax year. Revenue from Etsy, expenses from Etsy fees + bank statements.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        op_expenses = (d["net_fees"] + d["shipping"] + d["marketing"]
                       + d["bank_biz_expense"] + d["taxes_collected"] + d["buyer_fees"])
        net_inc = gross_profit - op_expenses

        children.append(yr_header(yr))
        children.append(section(f"INCOME STATEMENT — {yr}", [
            html.Div("REVENUE", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px",
                      "marginBottom": "4px"}),
            row_item("Gross Sales", d["gross_sales"], color=GREEN),
            row_item("Returns & Refunds", -d["refunds"], indent=1, color=GRAY),
            row_item("Net Sales", d["gross_sales"] - d["refunds"], bold=True),
            divider(),
            html.Div("COST OF GOODS SOLD", style={"color": ORANGE, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "4px", "marginBottom": "4px"}),
            row_item("Inventory (invoices)", -d["inventory_cost"], indent=1, color=GRAY),
            row_item("Additional bank Amazon (not in receipts)", -d["bank_inv_gap"], indent=1, color=GRAY) if d["bank_inv_gap"] > 0 else html.Div(),
            row_item("Total COGS", -d["cogs"], bold=True),
            divider(),
            row_item("GROSS PROFIT", gross_profit, bold=True, color=GREEN),
            divider(),
            html.Div("OPERATING EXPENSES", style={"color": RED, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "4px", "marginBottom": "4px"}),
            row_item("Etsy Fees (net of credits)", -d["net_fees"], indent=1, color=GRAY),
            row_item("Shipping Labels", -d["shipping"], indent=1, color=GRAY),
            row_item("Advertising / Marketing", -d["marketing"], indent=1, color=GRAY),
            row_item("Sales Tax Collected & Remitted", -d["taxes_collected"], indent=1, color=GRAY),
            row_item("Buyer Fees", -d["buyer_fees"], indent=1, color=GRAY),
            row_item("Bank: Shipping Supplies", -d["bank_by_cat"].get("Shipping", 0), indent=1, color=GRAY),
            row_item("Bank: Craft Supplies", -d["bank_by_cat"].get("Craft Supplies", 0), indent=1, color=GRAY),
            row_item("Bank: AliExpress Supplies", -d["bank_by_cat"].get("AliExpress Supplies", 0), indent=1, color=GRAY),
            row_item("Bank: Etsy Fees (bank-side)", -d["bank_by_cat"].get("Etsy Fees", 0), indent=1, color=GRAY),
            row_item("Bank: Subscriptions", -d["bank_by_cat"].get("Subscriptions", 0), indent=1, color=GRAY),
            row_item("Bank: Business CC Payment", -d["bank_by_cat"].get("Business Credit Card", 0), indent=1, color=GRAY),
            row_item("Total Operating Expenses", -op_expenses, bold=True),
            divider(CYAN),
            row_item("NET INCOME", net_inc, bold=True, color=GREEN if net_inc >= 0 else RED),
            html.Div([
                html.Span(f"  TJ share (50%): {money(net_inc / 2)}   |   Braden share (50%): {money(net_inc / 2)}",
                          style={"color": GRAY, "fontSize": "12px", "marginTop": "4px"}),
            ]),
        ], color=ORANGE))

    # ══════════════════════════════════════════════════════════════
    # SECTION C: FORM 1065 — Partnership Return Summary
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("C: FORM 1065 — U.S. Return of Partnership Income",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Key line items from IRS Form 1065, mapped to dashboard data.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        total_deductions = (d["net_fees"] + d["shipping"] + d["marketing"]
                            + d["bank_biz_expense"] + d["taxes_collected"] + d["buyer_fees"])
        ordinary_income = gross_profit - total_deductions

        children.append(yr_header(yr))
        children.append(section(f"FORM 1065 SUMMARY — {yr}", [
            html.Div("PAGE 1 — INCOME", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px",
                      "marginBottom": "4px"}),
            form_row("1a", "Gross receipts or sales", d["gross_sales"], color=GREEN),
            form_row("1b", "Returns and allowances", d["refunds"]),
            form_row("1c", "Balance (1a minus 1b)", d["gross_sales"] - d["refunds"]),
            form_row("2", "Cost of goods sold (Schedule A)", d["cogs"]),
            form_row("3", "Gross profit (1c minus 2)", gross_profit, bold=True),
            divider(),
            html.Div("DEDUCTIONS", style={"color": RED, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "4px", "marginBottom": "4px"}),
            form_row("10", "Guaranteed payments to partners", 0, color=GRAY),
            form_row("14", "Etsy fees + processing", d["net_fees"]),
            form_row("15", "Shipping costs", d["shipping"] + d["bank_by_cat"].get("Shipping", 0)),
            form_row("18", "Advertising (Etsy Ads)", d["marketing"]),
            form_row("20", "Other deductions (supplies, subscriptions)",
                     d["bank_by_cat"].get("Craft Supplies", 0) + d["bank_by_cat"].get("AliExpress Supplies", 0)
                     + d["bank_by_cat"].get("Subscriptions", 0) + d["bank_by_cat"].get("Business Credit Card", 0)
                     + d["taxes_collected"] + d["buyer_fees"]),
            form_row("21", "Total deductions", total_deductions, bold=True),
            divider(CYAN),
            form_row("22", "Ordinary business income (loss)", ordinary_income, bold=True,
                     color=GREEN if ordinary_income >= 0 else RED),
            html.Div([
                html.Span(f"  Each partner's 50% share: {money(ordinary_income / 2)}",
                          style={"color": CYAN, "fontSize": "12px", "fontStyle": "italic", "marginTop": "4px"}),
            ]),
        ], color=ORANGE))

    # ══════════════════════════════════════════════════════════════
    # SECTION D: SCHEDULE K-1 (side-by-side)
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("D: SCHEDULE K-1  (Partner's Share of Income)",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("One K-1 per partner — each receives 50% of partnership income.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        total_deductions = (d["net_fees"] + d["shipping"] + d["marketing"]
                            + d["bank_biz_expense"] + d["taxes_collected"] + d["buyer_fees"])
        ordinary_income = gross_profit - total_deductions
        partner_share = ordinary_income / 2

        # Capital account tracking
        if yr == 2025:
            tj_beg_capital = 0
            br_beg_capital = 0
        else:
            p25 = TAX_YEARS[2025]
            gp25 = p25["gross_sales"] - p25["refunds"] - p25["cogs"]
            td25 = (p25["net_fees"] + p25["shipping"] + p25["marketing"]
                    + p25["bank_biz_expense"] + p25["taxes_collected"] + p25["buyer_fees"])
            oi25 = gp25 - td25
            tj_beg_capital = oi25 / 2 - p25["tulsa_draws"]
            br_beg_capital = oi25 / 2 - p25["texas_draws"]

        tj_end_capital = tj_beg_capital + partner_share - d["tulsa_draws"]
        br_end_capital = br_beg_capital + partner_share - d["texas_draws"]

        def k1_card(name, beg_cap, end_cap, draws, share):
            return html.Div([
                html.Div(name, style={"color": CYAN, "fontWeight": "bold", "fontSize": "14px",
                          "marginBottom": "8px", "textAlign": "center"}),
                row_item("Ordinary business income (Box 1)", share, color=GREEN),
                row_item("Net rental real estate (Box 2)", 0, color=GRAY),
                row_item("Other net rental income (Box 3)", 0, color=GRAY),
                row_item("Guaranteed payments (Box 4)", 0, color=GRAY),
                row_item("Self-employment earnings (Box 14)", share, color=ORANGE),
                divider(),
                html.Div("CAPITAL ACCOUNT", style={"color": PURPLE, "fontWeight": "bold", "fontSize": "12px",
                          "marginTop": "4px", "marginBottom": "4px"}),
                row_item("Beginning capital", beg_cap),
                row_item("+ Capital contributed", 0, indent=1, color=GRAY),
                row_item("+ Share of income", share, indent=1, color=GREEN),
                row_item("- Distributions / draws", -draws, indent=1),
                row_item("Ending capital", end_cap, bold=True,
                         color=GREEN if end_cap >= 0 else RED),
            ], style={"flex": "1", "backgroundColor": CARD2, "padding": "12px", "borderRadius": "8px",
                      "border": f"1px solid {CYAN}33", "minWidth": "280px"})

        children.append(yr_header(yr))
        children.append(section(f"SCHEDULE K-1 — {yr}", [
            html.Div([
                k1_card("TJ  (Partner A — 50%)", tj_beg_capital, tj_end_capital, d["tulsa_draws"], partner_share),
                k1_card("Braden  (Partner B — 50%)", br_beg_capital, br_end_capital, d["texas_draws"], partner_share),
            ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}),
        ], color=PURPLE))

    # ══════════════════════════════════════════════════════════════
    # SECTION E: SCHEDULE SE — Self-Employment Tax
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("E: SCHEDULE SE  —  Self-Employment Tax",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Each partner owes SE tax on their share of partnership income. "
                           "SS tax (12.4%) applies up to $168,600 (2025) / $176,100 (2026). Medicare (2.9%) has no cap.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        total_deductions = (d["net_fees"] + d["shipping"] + d["marketing"]
                            + d["bank_biz_expense"] + d["taxes_collected"] + d["buyer_fees"])
        ordinary_income = gross_profit - total_deductions
        partner_share = ordinary_income / 2

        # SE tax calculation per IRS rules
        net_se = partner_share * 0.9235  # 92.35% of net self-employment income
        ss_wage_base = 168600 if yr == 2025 else 176100
        ss_taxable = min(net_se, ss_wage_base)
        ss_tax = ss_taxable * 0.124  # 12.4%
        medicare_tax = net_se * 0.029  # 2.9%
        total_se_tax = ss_tax + medicare_tax
        # Deductible half of SE tax
        se_deduction = total_se_tax / 2

        def se_card(name, share):
            _net = share * 0.9235
            _ss = min(_net, ss_wage_base) * 0.124
            _med = _net * 0.029
            _total = _ss + _med
            _ded = _total / 2
            return html.Div([
                html.Div(name, style={"color": ORANGE, "fontWeight": "bold", "fontSize": "14px",
                          "marginBottom": "8px", "textAlign": "center"}),
                row_item("Net earnings from K-1", share),
                row_item("x 92.35%", _net, indent=1, color=GRAY),
                row_item("Social Security (12.4%)", _ss, indent=1, color=RED),
                row_item(f"  (wage base: ${ss_wage_base:,.0f})", 0, indent=2, color=DARKGRAY),
                row_item("Medicare (2.9%)", _med, indent=1, color=RED),
                divider(),
                row_item("TOTAL SE TAX", _total, bold=True, color=RED),
                row_item("Deductible half (Sch 1)", _ded, color=GREEN),
            ], style={"flex": "1", "backgroundColor": CARD2, "padding": "12px", "borderRadius": "8px",
                      "border": f"1px solid {ORANGE}33", "minWidth": "280px"})

        children.append(yr_header(yr))
        children.append(section(f"SCHEDULE SE — {yr}", [
            html.Div([
                se_card("TJ", partner_share),
                se_card("Braden", partner_share),
            ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}),
        ], color=ORANGE))

    # ══════════════════════════════════════════════════════════════
    # SECTION F: ESTIMATED TAX SUMMARY
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("F: ESTIMATED TAX SUMMARY  (1040-ES)",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Quarterly estimated tax payments due from each partner. "
                           "Includes SE tax + estimated income tax (using 22% bracket as approximation).",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    q_dates = {
        2025: [("Q4", "Jan 15, 2026", "Oct-Dec 2025")],
        2026: [("Q1", "Apr 15, 2026", "Jan-Mar 2026"),
               ("Q2", "Jun 15, 2026", "Apr-Jun 2026"),
               ("Q3", "Sep 15, 2026", "Jul-Sep 2026"),
               ("Q4", "Jan 15, 2027", "Oct-Dec 2026")],
    }

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        total_deductions = (d["net_fees"] + d["shipping"] + d["marketing"]
                            + d["bank_biz_expense"] + d["taxes_collected"] + d["buyer_fees"])
        ordinary_income = gross_profit - total_deductions
        partner_share = ordinary_income / 2

        net_se = partner_share * 0.9235
        ss_wage_base = 168600 if yr == 2025 else 176100
        se_tax = min(net_se, ss_wage_base) * 0.124 + net_se * 0.029
        se_deduction = se_tax / 2

        # Estimated income tax at 22% marginal rate (approximation)
        taxable_income = partner_share - se_deduction  # after SE deduction
        est_income_tax = max(0, taxable_income * 0.22)
        total_annual_tax = se_tax + est_income_tax
        num_quarters = len(q_dates[yr])
        quarterly_payment = total_annual_tax / num_quarters if num_quarters else 0

        children.append(yr_header(yr))

        # Quarterly schedule table
        q_rows = []
        for q_label, due_date, period in q_dates[yr]:
            q_rows.append(html.Tr([
                html.Td(q_label, style={"color": CYAN, "padding": "6px 10px", "fontSize": "13px", "fontWeight": "bold"}),
                html.Td(period, style={"color": GRAY, "padding": "6px 10px", "fontSize": "13px"}),
                html.Td(due_date, style={"color": WHITE, "padding": "6px 10px", "fontSize": "13px"}),
                html.Td(money(quarterly_payment), style={"color": RED, "padding": "6px 10px",
                          "fontSize": "13px", "fontWeight": "bold", "fontFamily": "monospace", "textAlign": "right"}),
            ], style={"borderBottom": "1px solid #ffffff10"}))

        children.append(section(f"ESTIMATED TAX — {yr} (per partner)", [
            html.Div([
                kpi_card("SE TAX", money(se_tax), RED, "Per partner",
                         f"Self-employment tax per partner: Social Security (12.4% on first ${ss_wage_base:,.0f}) + Medicare (2.9% on all earnings). Calculated on 92.35% of net self-employment income ({money(net_se)}). This replaces the employer/employee FICA split since you're self-employed."),
                kpi_card("EST. INCOME TAX", money(est_income_tax), ORANGE, "22% bracket approx",
                         f"Estimated federal income tax on partnership share ({money(partner_share)}) minus half of SE tax deduction ({money(se_deduction)}). Uses 22% marginal rate as approximation. Actual rate depends on total household income, filing status, and other deductions."),
                kpi_card("TOTAL ANNUAL", money(total_annual_tax), RED, "Per partner",
                         f"SE tax ({money(se_tax)}) + income tax ({money(est_income_tax)}) = {money(total_annual_tax)} per partner per year. This is what each partner needs to set aside for taxes."),
                kpi_card("PER QUARTER", money(quarterly_payment), CYAN,
                         f"{num_quarters} payment(s)",
                         f"Divide annual tax ({money(total_annual_tax)}) by {num_quarters} quarters. Pay via IRS Form 1040-ES by each quarter's due date to avoid underpayment penalties (currently ~8% interest)."),
            ], style={"display": "flex", "gap": "8px", "marginBottom": "12px", "flexWrap": "wrap"}),
            html.Table([
                html.Thead(html.Tr([
                    html.Th("Quarter", style={"color": GRAY, "padding": "6px 10px", "fontSize": "11px", "textAlign": "left"}),
                    html.Th("Period", style={"color": GRAY, "padding": "6px 10px", "fontSize": "11px", "textAlign": "left"}),
                    html.Th("Due Date", style={"color": GRAY, "padding": "6px 10px", "fontSize": "11px", "textAlign": "left"}),
                    html.Th("Amount (each)", style={"color": GRAY, "padding": "6px 10px", "fontSize": "11px", "textAlign": "right"}),
                ], style={"borderBottom": f"2px solid {ORANGE}44"})),
                html.Tbody(q_rows),
            ], style={"width": "100%", "borderCollapse": "collapse"}),
            html.P(f"Note: Income tax estimate uses a flat 22% marginal rate as approximation. "
                   f"Actual rate depends on each partner's total taxable income and filing status.",
                   style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "10px", "fontStyle": "italic"}),
        ], color=ORANGE))

    # ══════════════════════════════════════════════════════════════
    # SECTION G: TAX WRITE-OFFS & DEDUCTIONS
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("G: TAX WRITE-OFFS & DEDUCTIONS",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Every dollar you can deduct reduces your taxable income — which lowers both income tax AND self-employment tax. "
                           "Below is every deduction the business has already claimed, plus deductions you may be missing.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]

        # ── Build the write-off items ──
        # Etsy platform deductions
        etsy_fees_ded = d["net_fees"]
        shipping_ded = d["shipping"]
        marketing_ded = d["marketing"]
        buyer_fees_ded = d["buyer_fees"]
        taxes_collected_ded = d["taxes_collected"]

        # Bank-side deductions
        bank_shipping = d["bank_by_cat"].get("Shipping", 0)
        bank_craft = d["bank_by_cat"].get("Craft Supplies", 0)
        bank_ali = d["bank_by_cat"].get("AliExpress Supplies", 0)
        bank_subs = d["bank_by_cat"].get("Subscriptions", 0)
        bank_etsy_fees = d["bank_by_cat"].get("Etsy Fees", 0)
        bank_cc_payment = d["bank_by_cat"].get("Business Credit Card", 0)

        # COGS deductions
        inv_cost_ded = d["inventory_cost"]
        bank_inv_ded = d["bank_inv"]
        total_cogs_ded = d["cogs"]

        # SE tax deduction (deductible half)
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        total_deductions_calc = (d["net_fees"] + d["shipping"] + d["marketing"]
                                 + d["bank_biz_expense"] + d["taxes_collected"] + d["buyer_fees"])
        ordinary_income = gross_profit - total_deductions_calc
        partner_share = ordinary_income / 2
        net_se = partner_share * 0.9235
        ss_wage_base = 168600 if yr == 2025 else 176100
        se_tax = min(net_se, ss_wage_base) * 0.124 + net_se * 0.029
        se_deduction = se_tax / 2

        # Total of all claimed deductions
        total_claimed = (total_cogs_ded + etsy_fees_ded + shipping_ded + bank_shipping
                         + marketing_ded + buyer_fees_ded + taxes_collected_ded
                         + bank_craft + bank_ali + bank_subs + bank_etsy_fees)

        # Potential missed deductions (estimates)
        # Home office: IRS simplified = $5/sqft x 300sqft max = $1,500/yr
        home_office_est = 1500 if yr == 2025 else int(1500 * (2 / 12))  # pro-rate for partial year
        # Internet portion: ~$80/mo x 30% biz use
        internet_est = 80 * 0.3 * (3 if yr == 2025 else 2)
        # Phone portion: ~$60/mo x 20% biz use
        phone_est = 60 * 0.2 * (3 if yr == 2025 else 2)
        # Mileage: IRS 2025 rate $0.70/mi — estimate trips
        mileage_rate = 0.70
        est_biz_miles = 200 if yr == 2025 else 80
        mileage_est = est_biz_miles * mileage_rate
        # Equipment depreciation / Section 179
        # Can deduct 100% of equipment cost in year 1 using Section 179
        section_179_est = bb_cc_asset_value if yr == 2025 else 0

        total_potential = home_office_est + internet_est + phone_est + mileage_est + section_179_est
        total_all_deductions = total_claimed + total_potential + (se_deduction * 2)  # both partners

        # Tax savings from each deduction (SE 15.3% x 92.35% + income 22%)
        effective_ded_rate = 0.9235 * 0.153 + 0.22  # ~36.13%

        children.append(yr_header(yr))

        # KPI strip for this year
        children.append(html.Div([
            kpi_card("TOTAL CLAIMED", money(total_claimed), GREEN, "Already deducted",
                     f"Sum of all business deductions from Etsy fees, shipping, COGS, advertising, and bank expenses for {yr}. These reduce your taxable income dollar-for-dollar."),
            kpi_card("TAX SAVINGS", money(total_claimed * effective_ded_rate), GREEN,
                     f"~{effective_ded_rate:.0%} effective rate",
                     f"Every $1 you deduct saves ~${effective_ded_rate:.2f} in combined SE tax + income tax. Total claimed ({money(total_claimed)}) x {effective_ded_rate:.0%} = {money(total_claimed * effective_ded_rate)} in tax you DON'T pay."),
            kpi_card("POTENTIAL MISSED", money(total_potential), ORANGE, "Unclaimed deductions",
                     f"Estimated additional deductions you may qualify for but haven't claimed: home office, internet, phone, mileage, Section 179 equipment. These are ESTIMATES — track actual expenses to claim them."),
            kpi_card("EXTRA SAVINGS", money(total_potential * effective_ded_rate), ORANGE,
                     "If you claim all",
                     f"If you claim all potential missed deductions ({money(total_potential)}), you'd save an additional {money(total_potential * effective_ded_rate)} in taxes. That's money back in your pocket."),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "12px", "flexWrap": "wrap"}))

        # Deduction table — CLAIMED
        def ded_row(desc, amount, irs_line="", note="", is_header=False, is_missed=False):
            bg = f"{ORANGE}08" if is_missed else "transparent"
            border_style = f"2px solid {GREEN}44" if is_header else "1px solid #ffffff10"
            icon = "✓ " if not is_missed and amount > 0 else "⚠ " if is_missed else ""
            return html.Div([
                html.Span(f"{icon}{desc}", style={"flex": "3", "color": WHITE if not is_header else GREEN,
                           "fontSize": "13px", "fontWeight": "bold" if is_header else "normal"}),
                html.Span(irs_line, style={"flex": "1", "color": GRAY, "fontSize": "11px", "textAlign": "center"}),
                html.Span(money(amount) if amount > 0 else "—", style={
                    "flex": "1", "textAlign": "right", "fontFamily": "monospace", "fontSize": "13px",
                    "color": GREEN if amount > 0 and not is_missed else ORANGE if is_missed and amount > 0 else DARKGRAY,
                    "fontWeight": "bold" if is_header else "normal"}),
                html.Span(note, style={"flex": "2", "color": DARKGRAY, "fontSize": "11px", "paddingLeft": "10px"}),
            ], style={"display": "flex", "alignItems": "center", "padding": "5px 0",
                      "borderBottom": border_style, "backgroundColor": bg})

        children.append(section(f"WRITE-OFFS — {yr}", [
            # Column header
            html.Div([
                html.Span("Deduction", style={"flex": "3", "color": GRAY, "fontSize": "11px", "fontWeight": "bold"}),
                html.Span("IRS Line", style={"flex": "1", "color": GRAY, "fontSize": "11px", "textAlign": "center", "fontWeight": "bold"}),
                html.Span("Amount", style={"flex": "1", "color": GRAY, "fontSize": "11px", "textAlign": "right", "fontWeight": "bold"}),
                html.Span("Notes", style={"flex": "2", "color": GRAY, "fontSize": "11px", "paddingLeft": "10px", "fontWeight": "bold"}),
            ], style={"display": "flex", "padding": "4px 0", "borderBottom": f"2px solid {CYAN}44"}),

            # COGS
            ded_row("COST OF GOODS SOLD", total_cogs_ded, "Sch A", "", True),
            ded_row("  Invoice-based inventory", inv_cost_ded, "1065 Ln 2", "Amazon Business orders, paper receipts"),
            ded_row("  Bank-categorized inventory", bank_inv_ded, "1065 Ln 2", "Amazon purchases from bank statement"),

            # Platform fees
            ded_row("PLATFORM & PROCESSING FEES", etsy_fees_ded + buyer_fees_ded + bank_etsy_fees, "Ln 14/20", "", True),
            ded_row("  Etsy fees (net of credits)", etsy_fees_ded, "1065 Ln 14", f"Transaction, listing, processing fees minus {money(d['total_credits'])} in credits"),
            ded_row("  Buyer shipping fees", buyer_fees_ded, "1065 Ln 20", "Fees charged to buyers by Etsy"),
            ded_row("  Etsy fees (bank-side)", bank_etsy_fees, "1065 Ln 20", "Additional Etsy charges seen in bank"),

            # Shipping
            ded_row("SHIPPING & POSTAGE", shipping_ded + bank_shipping, "Ln 15", "", True),
            ded_row("  Etsy shipping labels", shipping_ded, "1065 Ln 15", "Postage purchased through Etsy"),
            ded_row("  Shipping supplies (bank)", bank_shipping, "1065 Ln 15", "Boxes, mailers, tape, etc."),

            # Advertising
            ded_row("ADVERTISING", marketing_ded, "Ln 18", "", True),
            ded_row("  Etsy Ads", marketing_ded, "1065 Ln 18", "Promoted listings on Etsy"),

            # Supplies
            ded_row("SUPPLIES & MATERIALS", bank_craft + bank_ali, "Ln 20", "", True),
            ded_row("  Craft supplies (bank)", bank_craft, "1065 Ln 20", "Hobby Lobby, craft stores"),
            ded_row("  AliExpress supplies", bank_ali, "1065 Ln 20", "Bulk supplies from AliExpress"),

            # Other
            ded_row("OTHER DEDUCTIONS", bank_subs + taxes_collected_ded, "Ln 20", "", True),
            ded_row("  Software subscriptions", bank_subs, "1065 Ln 20", "Business software, tools"),
            ded_row("  Sales tax collected/remitted", taxes_collected_ded, "1065 Ln 20", "State sales tax (pass-through)"),
            ded_row("  SE tax deduction (per partner)", se_deduction, "1040 Sch 1", "Deductible half of self-employment tax"),

            # TOTAL CLAIMED
            html.Div(style={"borderTop": f"2px solid {CYAN}66", "margin": "6px 0"}),
            html.Div([
                html.Span("TOTAL CLAIMED DEDUCTIONS", style={"flex": "3", "color": CYAN, "fontSize": "14px", "fontWeight": "bold"}),
                html.Span("", style={"flex": "1"}),
                html.Span(money(total_claimed), style={"flex": "1", "textAlign": "right", "fontFamily": "monospace",
                           "fontSize": "14px", "color": CYAN, "fontWeight": "bold"}),
                html.Span("", style={"flex": "2"}),
            ], style={"display": "flex", "padding": "8px 0"}),

            # POTENTIAL MISSED
            html.Div(style={"borderTop": f"2px solid {ORANGE}66", "margin": "10px 0 4px 0"}),
            html.Div("POTENTIAL ADDITIONAL DEDUCTIONS (not yet claimed)", style={
                "color": ORANGE, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "6px"}),
            html.P("These are common small-business deductions you may qualify for. Track these expenses to claim them.",
                   style={"color": GRAY, "fontSize": "11px", "margin": "0 0 6px 0"}),
            ded_row("  Home office (simplified method)", home_office_est, "8829", f"$5/sqft x up to 300 sqft = $1,500/yr", is_missed=True),
            ded_row("  Internet (business portion)", internet_est, "1065 Ln 20", "~30% of monthly internet bill", is_missed=True),
            ded_row("  Cell phone (business portion)", phone_est, "1065 Ln 20", "~20% of monthly phone bill", is_missed=True),
            ded_row("  Business mileage", mileage_est, "1065 Ln 20", f"~{est_biz_miles} mi x ${mileage_rate}/mi (post office, supply runs)", is_missed=True),
            ded_row("  Section 179: Equipment", section_179_est, "4562", "Deduct full equipment cost in year 1 (3D printers)" if section_179_est > 0 else "Equipment purchased in 2025", is_missed=True),

            html.Div(style={"borderTop": f"2px solid {ORANGE}44", "margin": "6px 0"}),
            html.Div([
                html.Span("POTENTIAL EXTRA DEDUCTIONS", style={"flex": "3", "color": ORANGE, "fontSize": "13px", "fontWeight": "bold"}),
                html.Span("", style={"flex": "1"}),
                html.Span(money(total_potential), style={"flex": "1", "textAlign": "right", "fontFamily": "monospace",
                           "fontSize": "13px", "color": ORANGE, "fontWeight": "bold"}),
                html.Span(f"→ saves ~{money(total_potential * effective_ded_rate)} in tax", style={"flex": "2", "color": ORANGE,
                           "fontSize": "11px", "paddingLeft": "10px"}),
            ], style={"display": "flex", "padding": "6px 0"}),
        ], color=GREEN))

    # ══════════════════════════════════════════════════════════════
    # SECTION H: TAX STRATEGY & OPTIMIZATION
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("H: TAX STRATEGY & OPTIMIZATION",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Actionable strategies to legally minimize your tax bill. "
                           "Ranked by estimated impact — highest savings first.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    # Compute strategy values
    _combined_annual_income = sum(
        (TAX_YEARS[yr]["gross_sales"] - TAX_YEARS[yr]["refunds"] - TAX_YEARS[yr]["cogs"]
         - TAX_YEARS[yr]["net_fees"] - TAX_YEARS[yr]["shipping"] - TAX_YEARS[yr]["marketing"]
         - TAX_YEARS[yr]["bank_biz_expense"] - TAX_YEARS[yr]["taxes_collected"] - TAX_YEARS[yr]["buyer_fees"])
        for yr in (2025, 2026))
    _combined_partner_share = _combined_annual_income / 2
    _combined_se_net = _combined_partner_share * 0.9235
    _combined_se_tax = min(_combined_se_net, 176100) * 0.124 + _combined_se_net * 0.029
    _combined_income_tax = max(0, (_combined_partner_share - _combined_se_tax / 2) * 0.22)
    _combined_total_tax = _combined_se_tax + _combined_income_tax
    _total_draws = sum(TAX_YEARS[yr]["total_draws"] for yr in (2025, 2026))

    # S-Corp election savings estimate
    # If income > ~$40K, S-Corp can save on SE tax by splitting into salary + distributions
    _reasonable_salary = min(_combined_partner_share * 0.6, 50000)  # 60% of income or $50K
    _scorp_se_net = _reasonable_salary * 0.9235
    _scorp_se_tax = min(_scorp_se_net, 176100) * 0.124 + _scorp_se_net * 0.029
    _scorp_savings = max(0, _combined_se_tax - _scorp_se_tax)

    # Retirement contribution savings
    _sep_ira_limit = min(_combined_partner_share * 0.25, 69000)  # 2025 SEP-IRA limit
    _sep_tax_savings = _sep_ira_limit * 0.22  # income tax savings only

    def strategy_card(title, savings, priority, status, description, action_items, color):
        pri_colors = {"HIGH": RED, "MEDIUM": ORANGE, "LOW": TEAL}
        status_colors = {"DO NOW": RED, "PLAN FOR": ORANGE, "CONSIDER": CYAN, "TRACK": GREEN}
        return html.Div([
            html.Div([
                html.Span(title, style={"color": color, "fontWeight": "bold", "fontSize": "14px", "flex": "1"}),
                html.Span(priority, style={"backgroundColor": f"{pri_colors.get(priority, GRAY)}22",
                           "color": pri_colors.get(priority, GRAY), "padding": "2px 10px", "borderRadius": "4px",
                           "fontSize": "10px", "fontWeight": "bold", "marginRight": "6px"}),
                html.Span(status, style={"backgroundColor": f"{status_colors.get(status, GRAY)}22",
                           "color": status_colors.get(status, GRAY), "padding": "2px 10px", "borderRadius": "4px",
                           "fontSize": "10px", "fontWeight": "bold"}),
            ], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}),
            html.Div([
                html.Span("Est. savings: ", style={"color": GRAY, "fontSize": "12px"}),
                html.Span(money(savings), style={"color": GREEN, "fontWeight": "bold", "fontFamily": "monospace", "fontSize": "14px"}),
                html.Span(" / year", style={"color": GRAY, "fontSize": "11px"}),
            ], style={"marginBottom": "6px"}),
            html.P(description, style={"color": GRAY, "fontSize": "12px", "margin": "0 0 8px 0", "lineHeight": "1.4"}),
            html.Div([
                html.Div(f"→ {item}", style={"color": WHITE, "fontSize": "12px", "padding": "2px 0"})
                for item in action_items
            ]),
        ], style={"padding": "14px", "backgroundColor": CARD2, "borderRadius": "8px",
                  "border": f"1px solid {color}33", "borderLeft": f"4px solid {color}",
                  "marginBottom": "10px"})

    # Quarterly payment timing
    _q1_due = "Apr 15, 2026"
    _q1_amount = _combined_total_tax / 4
    _penalty_risk = _combined_total_tax > 1000

    strategies = []

    # 1. Quarterly estimated payments
    strategies.append(strategy_card(
        "Pay Quarterly Estimated Taxes", _combined_total_tax * 0.08 if _penalty_risk else 0,
        "HIGH", "DO NOW",
        f"You owe ~{money(_combined_total_tax)} per partner in total tax. If you don't pay quarterly, the IRS charges ~8% "
        f"underpayment penalty. Next payment: {_q1_due} for {money(_q1_amount)} each.",
        [f"Pay {money(_q1_amount)} per partner by {_q1_due} (Form 1040-ES)",
         "Set up IRS Direct Pay or EFTPS for auto-payments",
         "Mark calendar for Q2 (Jun 15), Q3 (Sep 15), Q4 (Jan 15)"],
        RED))

    # 2. Section 179 / Equipment deduction
    if bb_cc_asset_value > 0:
        _sec179_savings = bb_cc_asset_value * effective_ded_rate
        strategies.append(strategy_card(
            "Section 179: Deduct Equipment in Year 1", _sec179_savings,
            "HIGH", "DO NOW",
            f"You purchased {money(bb_cc_asset_value)} in 3D printing equipment. Under Section 179, you can deduct the "
            f"FULL cost in the year purchased (2025) instead of depreciating over 5-7 years. This gives you an immediate "
            f"{money(_sec179_savings)} tax reduction.",
            ["File Form 4562 with 2025 return to elect Section 179",
             f"Deduct full {money(bb_cc_asset_value)} against 2025 income",
             "Keep all Best Buy receipts — IRS requires proof of purchase",
             "Note: CC interest on business purchases is also deductible"],
            GREEN))

    # 3. Home office deduction
    _home_office_savings = 1500 * effective_ded_rate
    strategies.append(strategy_card(
        "Home Office Deduction", _home_office_savings,
        "HIGH", "DO NOW",
        "If you use a dedicated space at home exclusively for business (3D printing, packing orders), you qualify for "
        "the home office deduction. Simplified method: $5/sqft up to 300 sqft = $1,500/year. "
        "Regular method could be even higher based on your actual expenses (rent, utilities, etc).",
        ["Measure your dedicated workspace square footage",
         "Simplified: claim $5 x sqft (max 300 sqft = $1,500)",
         "Regular: calculate % of home used for biz, apply to rent/mortgage + utilities",
         "Must be used REGULARLY and EXCLUSIVELY for business"],
        GREEN))

    # 4. Track ALL business mileage
    strategies.append(strategy_card(
        "Business Mileage Deduction", mileage_rate * 500 * effective_ded_rate,
        "MEDIUM", "TRACK",
        f"Every trip to the post office, supply store, or anywhere for business purposes is deductible at "
        f"${mileage_rate:.2f}/mile (2025 IRS rate). Even 500 miles/year = {money(500 * mileage_rate)} deduction.",
        ["Download a mileage tracking app (MileIQ, Everlance, or free Stride)",
         "Log EVERY business trip: post office, Hobby Lobby, Home Depot, etc.",
         "Keep a simple spreadsheet: date, destination, miles, purpose",
         f"At ${mileage_rate:.2f}/mi, even small trips add up fast"],
        TEAL))

    # 5. Internet & phone deductions
    _utility_savings = (internet_est + phone_est) * 2 * effective_ded_rate  # full year estimate
    strategies.append(strategy_card(
        "Internet & Phone (Business Portion)", _utility_savings,
        "MEDIUM", "TRACK",
        "You can deduct the business-use percentage of your internet and cell phone bills. "
        "If you use internet 30% for business and phone 20%, those portions are deductible.",
        ["Calculate what % of internet use is for business (Etsy shop, research, shipping)",
         "Calculate what % of phone use is for business (Etsy app, customer messages)",
         "Keep phone/internet bills as documentation",
         "Conservative estimate: 25-30% internet, 15-20% phone"],
        TEAL))

    # 6. SEP-IRA / retirement
    if _combined_partner_share > 500:
        strategies.append(strategy_card(
            "SEP-IRA Retirement Contributions", _sep_tax_savings,
            "MEDIUM", "PLAN FOR",
            f"Self-employed individuals can contribute up to 25% of net self-employment earnings to a SEP-IRA "
            f"(max $69,000 for 2025). Your max contribution: ~{money(_sep_ira_limit)} per partner. This reduces "
            f"taxable income and builds retirement savings simultaneously.",
            [f"Open a SEP-IRA at Fidelity, Vanguard, or Schwab (free)",
             f"Contribute up to {money(_sep_ira_limit)} per partner before filing deadline",
             "Contributions are tax-deductible — reduces income tax immediately",
             "Can contribute for 2025 up until Apr 15, 2026 (or Oct 15 with extension)"],
            PURPLE))

    # 7. S-Corp election (if income grows)
    if _combined_partner_share > 500:
        strategies.append(strategy_card(
            "S-Corp Election (Future)", _scorp_savings,
            "LOW", "CONSIDER",
            f"If annual profits exceed ~$40K per partner, electing S-Corp status lets you split income into "
            f"salary (subject to SE tax) and distributions (NOT subject to SE tax). With current income of "
            f"~{money(_combined_partner_share)}/partner, this could save ~{money(_scorp_savings)}/year in SE tax. "
            f"However, S-Corps have more paperwork and payroll requirements.",
            ["Only worth it when consistent profit > $40K/partner/year",
             f"Reasonable salary: ~{money(_reasonable_salary)} → SE tax only on salary portion",
             "Requires payroll (Gusto ~$40/mo), separate tax return (Form 1120-S)",
             "File Form 2553 by Mar 15 of the year you want it to take effect"],
            CYAN))

    # 8. Timing strategy
    strategies.append(strategy_card(
        "Year-End Tax Planning (Timing)", _combined_total_tax * 0.05,
        "MEDIUM", "PLAN FOR",
        "You can shift income and expenses between tax years to minimize taxes. If 2026 is looking like a high-income year, "
        "accelerate expenses into 2026 (buy supplies in Dec). If 2026 is low, defer expenses to 2027.",
        ["Buy inventory & supplies before Dec 31 to deduct in current year",
         "Prepay subscriptions or advertising if cash allows",
         "Consider major equipment purchases in high-income years for Section 179",
         "Delay invoicing or deposits if you want to push income to next year"],
        ORANGE))

    # 9. Record keeping
    strategies.append(strategy_card(
        "Bulletproof Record Keeping", 0,
        "HIGH", "DO NOW",
        "The best tax strategy is worthless without documentation. If audited, you need receipts for every deduction. "
        "Good records also make tax filing faster and cheaper.",
        ["Save ALL receipts (digital photos or apps like Dext/Shoeboxed)",
         "Separate business and personal bank accounts (you already have this!)",
         "Keep a simple log of cash expenses (post office, supply runs)",
         "Back up Etsy CSV statements and bank statements monthly",
         "This dashboard IS your documentation — export/screenshot for records"],
        WHITE))

    # Build the section
    children.append(section("TAX STRATEGY OVERVIEW", [
        # Summary KPIs
        html.Div([
            kpi_card("CURRENT TAX BILL", money(_combined_total_tax * 2), RED, "Both partners combined",
                     f"Total estimated tax liability for both partners across 2025 + 2026 YTD. This includes self-employment tax and estimated income tax at 22% bracket."),
            kpi_card("MAX POTENTIAL SAVINGS", money(total_potential * effective_ded_rate * 2 + _scorp_savings),
                     GREEN, "If all strategies applied",
                     f"Maximum tax savings if you claim all missed deductions and implement all applicable strategies. Includes home office, mileage, utilities, equipment deduction, and structural optimization."),
            kpi_card("EASIEST WIN", money(1500 * effective_ded_rate), GREEN, "Home office deduction",
                     "The simplest deduction to claim with the biggest bang: $5/sqft x up to 300 sqft. Just need a dedicated workspace. Saves ~$542/year in taxes."),
            kpi_card("NEXT DEADLINE", _q1_due, ORANGE, f"Q1 payment: {money(_q1_amount)}/partner",
                     f"Next quarterly estimated tax payment due date. Pay {money(_q1_amount)} per partner to IRS via Form 1040-ES to avoid underpayment penalties (~8% interest)."),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "14px", "flexWrap": "wrap"}),
    ] + strategies, ORANGE))

    # Disclaimer
    children.append(html.Div([
        html.P("DISCLAIMER: These calculations are estimates based on dashboard data and standard IRS formulas. "
               "They are NOT a substitute for professional tax advice. Consult a CPA or tax professional "
               "before filing. Key Component Manufacturing LLC (EIN pending) — 50/50 multi-member LLC taxed as partnership.",
               style={"color": DARKGRAY, "fontSize": "11px", "fontStyle": "italic", "margin": "10px 0",
                      "padding": "10px", "backgroundColor": CARD2, "borderRadius": "6px",
                      "border": f"1px solid {RED}33"}),
    ]))

    return html.Div(children, style={"padding": TAB_PADDING})


def build_tab6_valuation():
    """Tab 6 - Business Valuation: Comprehensive business value analysis from every angle."""
    children = []

    # ── HERO KPI STRIP (6 bubbles) ──
    children.append(html.Div([
        kpi_card("BUSINESS VALUE", money(val_blended_mid), CYAN, f"Range: {money(val_blended_low)} — {money(val_blended_high)}",
                 f"Blended estimate using 3 methods: SDE Multiple (50% weight, {money(val_sde_mid)}), Revenue Multiple (25%, {money(val_rev_mid)}), Asset-Based (25%, {money(val_asset_val)}). Low estimate: {money(val_blended_low)}, high: {money(val_blended_high)}. This is what the business would likely sell for."),
        kpi_card("ANNUAL SDE", money(val_annual_sde), GREEN, f"Monthly: {money(val_sde / _val_months_operating)}",
                 f"Seller's Discretionary Earnings = Profit ({money(profit)}) + Owner Draws ({money(bank_owner_draw_total)}), annualized from {_val_months_operating} months. SDE represents what a single owner-operator could earn. It's the most common metric for valuing small businesses."),
        kpi_card("HEALTH SCORE", f"{val_health_score}/100 ({val_health_grade})", val_health_color,
                 "Profitability + Growth + Cash + Diversity",
                 f"Composite score: Profitability {_hs_profit:.0f}/25 + Growth {_hs_growth:.0f}/25 + Product Diversity {_hs_diversity:.0f}/15 + Cash Position {_hs_cash:.0f}/15 + Debt {_hs_debt:.0f}/10 + Shipping {_hs_shipping:.0f}/10. Grade: A (80+), B (60-79), C (40-59), D (below 40)."),
        kpi_card("ANNUAL REVENUE", money(val_annual_revenue), TEAL, f"{_val_months_operating}mo annualized",
                 f"Gross sales ({money(gross_sales)}) over {_val_months_operating} months, projected to 12 months ({money(gross_sales)} x 12/{_val_months_operating}). Assumes current sales pace continues. Seasonal variation could make this higher or lower."),
        kpi_card("EQUITY", money(val_equity), GREEN, f"Assets {money(val_total_assets)} - Liabilities {money(val_total_liabilities)}",
                 f"ASSETS: Bank cash {money(bank_cash_on_hand)}, Equipment (Best Buy CC purchases) {money(bb_cc_asset_value)}, Inventory on hand {money(true_inventory_cost)}. Total: {money(val_total_assets)}. LIABILITIES: CC debt {money(bb_cc_balance)}. Equity = Assets minus Liabilities."),
        kpi_card("GROWTH RATE", f"{_val_growth_pct:+.1f}%/mo",
                 GREEN if _val_growth_pct > 5 else ORANGE if _val_growth_pct > 0 else RED,
                 f"R² = {_val_r2:.0%} confidence",
                 f"Monthly revenue growth rate from linear regression on {_val_months_operating} months of data. Trend: {money(abs(_val_sales_trend))}/month {'increase' if _val_sales_trend > 0 else 'decrease'}. R² of {_val_r2:.0%} means the trend explains {_val_r2 * 100:.0f}% of the variation (higher = more predictable)."),
    ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginBottom": "14px"}))

    # ── SECTION A: VALUATION SUMMARY ──
    # Gauge chart
    val_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=val_blended_mid,
        number={"prefix": "$", "valueformat": ",.0f"},
        delta={"reference": val_blended_low, "valueformat": ",.0f", "prefix": "$"},
        title={"text": "Blended Business Valuation"},
        gauge={
            "axis": {"range": [0, val_blended_high * 1.2], "tickprefix": "$", "tickformat": ",.0f"},
            "bar": {"color": CYAN},
            "steps": [
                {"range": [0, val_blended_low], "color": "rgba(231,76,60,0.19)"},
                {"range": [val_blended_low, val_blended_mid], "color": "rgba(243,156,18,0.19)"},
                {"range": [val_blended_mid, val_blended_high], "color": "rgba(46,204,113,0.19)"},
            ],
            "threshold": {"line": {"color": WHITE, "width": 2}, "thickness": 0.75, "value": val_blended_mid},
        },
    ))
    make_chart(val_gauge, 280, False)
    val_gauge.update_layout(title="")

    # Method cards
    def method_card(title, weight, low, mid, high, color, detail):
        return html.Div([
            html.Div(f"{title} ({weight}% weight)", style={"color": color, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "6px"}),
            html.Div([
                html.Span("Low: ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(money(low), style={"color": ORANGE, "fontFamily": "monospace", "fontSize": "12px"}),
            ], style={"marginBottom": "2px"}),
            html.Div([
                html.Span("Mid: ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(money(mid), style={"color": GREEN, "fontFamily": "monospace", "fontSize": "14px", "fontWeight": "bold"}),
            ], style={"marginBottom": "2px"}),
            html.Div([
                html.Span("High: ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(money(high), style={"color": CYAN, "fontFamily": "monospace", "fontSize": "12px"}),
            ], style={"marginBottom": "6px"}),
            html.P(detail, style={"color": GRAY, "fontSize": "11px", "margin": "0", "lineHeight": "1.3"}),
        ], style={
            "flex": "1", "minWidth": "200px", "padding": "12px",
            "backgroundColor": f"{color}10", "borderLeft": f"3px solid {color}",
            "borderRadius": "4px",
        })

    children.append(section("A. VALUATION SUMMARY", [
        dcc.Graph(figure=val_gauge, config={"displayModeBar": False}),
        html.Div([
            method_card("SDE Multiple", 50, val_sde_low, val_sde_mid, val_sde_high, GREEN,
                        f"Annual SDE {money(val_annual_sde)} × 1.0/1.5/2.5x multiples"),
            method_card("Revenue Multiple", 25, val_rev_low, val_rev_mid, val_rev_high, TEAL,
                        f"Annual Revenue {money(val_annual_revenue)} × 0.3/0.5/1.0x multiples"),
            method_card("Asset-Based", 25, val_asset_val, val_asset_val, val_asset_val, PURPLE,
                        f"Assets {money(val_total_assets)} − Liabilities {money(val_total_liabilities)}"),
        ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginTop": "10px"}),
        html.Div([
            html.Span("BLENDED ESTIMATE: ", style={"color": GRAY, "fontSize": "13px"}),
            html.Span(money(val_blended_mid), style={"color": CYAN, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
            html.Span(f"  (range {money(val_blended_low)} — {money(val_blended_high)})", style={"color": GRAY, "fontSize": "12px"}),
        ], style={"textAlign": "center", "padding": "12px", "marginTop": "10px",
                  "backgroundColor": f"{CYAN}10", "borderRadius": "6px", "border": f"1px solid {CYAN}33"}),
    ], CYAN))

    # ── SECTION B: VALUATION COMPARISON CHART ──
    _comp_methods = ["SDE Multiple", "Revenue Multiple", "Asset-Based", "Blended"]
    _comp_lows = [val_sde_low, val_rev_low, val_asset_val, val_blended_low]
    _comp_mids = [val_sde_mid, val_rev_mid, val_asset_val, val_blended_mid]
    _comp_highs = [val_sde_high, val_rev_high, val_asset_val, val_blended_high]
    comp_fig = go.Figure()
    comp_fig.add_trace(go.Bar(name="Low", y=_comp_methods, x=_comp_lows, orientation="h",
                              marker_color=ORANGE, text=[f"${v:,.0f}" for v in _comp_lows], textposition="outside"))
    comp_fig.add_trace(go.Bar(name="Mid", y=_comp_methods, x=_comp_mids, orientation="h",
                              marker_color=GREEN, text=[f"${v:,.0f}" for v in _comp_mids], textposition="outside"))
    comp_fig.add_trace(go.Bar(name="High", y=_comp_methods, x=_comp_highs, orientation="h",
                              marker_color=CYAN, text=[f"${v:,.0f}" for v in _comp_highs], textposition="outside"))
    make_chart(comp_fig, 280)
    comp_fig.update_layout(title="Valuation Method Comparison (Low / Mid / High)", barmode="group",
                           yaxis={"categoryorder": "array", "categoryarray": _comp_methods[::-1]})

    children.append(section("B. VALUATION COMPARISON", [
        chart_context("Side-by-side comparison of all valuation methods. Stacked bars show low → mid → high ranges.",
                      metrics=[("Blended Mid", money(val_blended_mid), CYAN), ("SDE Weight", "50%", GREEN), ("Rev Weight", "25%", TEAL)],
                      simple="Each colored bar shows a different way to estimate what the business is worth. Longer bars = higher value. The 'Blended' row at top is the combined best estimate using all three methods."),
        dcc.Graph(figure=comp_fig, config={"displayModeBar": False}),
    ], TEAL))

    # ── SECTION C: BUSINESS HEALTH ASSESSMENT ──
    health_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=val_health_score,
        title={"text": f"Health Grade: {val_health_grade}"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": val_health_color},
            "steps": [
                {"range": [0, 40], "color": "rgba(231,76,60,0.15)"},
                {"range": [40, 60], "color": "rgba(243,156,18,0.15)"},
                {"range": [60, 80], "color": "rgba(26,188,156,0.15)"},
                {"range": [80, 100], "color": "rgba(46,204,113,0.15)"},
            ],
        },
    ))
    make_chart(health_gauge, 220, False)

    def severity_badge(sev):
        c = RED if sev == "HIGH" else ORANGE if sev == "MED" else GREEN
        return html.Span(sev, style={
            "backgroundColor": f"{c}25", "color": c, "padding": "2px 8px",
            "borderRadius": "4px", "fontSize": "10px", "fontWeight": "bold", "marginRight": "8px",
        })

    risk_items = [html.Div([
        severity_badge(sev),
        html.Span(name, style={"color": WHITE, "fontSize": "12px", "fontWeight": "bold", "marginRight": "8px"}),
        html.Span(f"— {desc}", style={"color": GRAY, "fontSize": "11px"}),
    ], style={"padding": "4px 0", "borderBottom": "1px solid #ffffff08"}) for name, desc, sev in val_risks]

    strength_items = [html.Div([
        html.Span("✓ ", style={"color": GREEN, "fontSize": "13px", "marginRight": "4px"}),
        html.Span(name, style={"color": GREEN, "fontSize": "12px", "fontWeight": "bold", "marginRight": "8px"}),
        html.Span(f"— {desc}", style={"color": GRAY, "fontSize": "11px"}),
    ], style={"padding": "4px 0", "borderBottom": "1px solid #ffffff08"}) for name, desc in val_strengths]

    children.append(section("C. BUSINESS HEALTH ASSESSMENT", [
        html.Div([
            html.Div([dcc.Graph(figure=health_gauge, config={"displayModeBar": False})],
                     style={"flex": "1", "minWidth": "250px"}),
            html.Div([
                html.Div([
                    html.Div(f"Profitability: {_hs_profit:.0f}/25", style={"color": GREEN if _hs_profit > 15 else ORANGE, "fontSize": "11px"}),
                    html.Div(f"Growth: {_hs_growth:.0f}/25", style={"color": GREEN if _hs_growth > 15 else ORANGE, "fontSize": "11px"}),
                    html.Div(f"Diversity: {_hs_diversity:.0f}/15", style={"color": GREEN if _hs_diversity > 8 else ORANGE, "fontSize": "11px"}),
                    html.Div(f"Cash Position: {_hs_cash:.0f}/15", style={"color": GREEN if _hs_cash > 8 else ORANGE, "fontSize": "11px"}),
                    html.Div(f"Debt: {_hs_debt:.0f}/10", style={"color": GREEN if _hs_debt > 5 else ORANGE, "fontSize": "11px"}),
                    html.Div(f"Shipping: {_hs_shipping:.0f}/10", style={"color": GREEN if _hs_shipping > 5 else ORANGE, "fontSize": "11px"}),
                ], style={"padding": "10px", "backgroundColor": f"{val_health_color}10", "borderRadius": "6px"}),
            ], style={"flex": "1", "minWidth": "200px"}),
        ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap"}),
        html.Div([
            html.H4("Risk Factors", style={"color": RED, "fontSize": "13px", "margin": "12px 0 6px 0"}),
            *risk_items,
            html.H4("Strengths", style={"color": GREEN, "fontSize": "13px", "margin": "12px 0 6px 0"}),
            *strength_items,
        ]),
    ], ORANGE))

    # ── SECTION D: GROWTH TRAJECTORY & PROJECTIONS ──
    proj_fig = go.Figure()
    # Actual revenue line
    rev_vals = [monthly_sales.get(m, 0) for m in months_sorted]
    net_vals = [monthly_net_revenue.get(m, 0) for m in months_sorted]
    proj_fig.add_trace(go.Scatter(
        x=months_sorted, y=rev_vals, mode="lines+markers",
        name="Actual Revenue", line=dict(color=GREEN, width=3), marker=dict(size=8),
    ))
    proj_fig.add_trace(go.Scatter(
        x=months_sorted, y=net_vals, mode="lines+markers",
        name="Actual Net Profit", line=dict(color=ORANGE, width=2, dash="dot"), marker=dict(size=6),
    ))
    # Projected revenue with confidence bands
    if "proj_sales" in analytics_projections and len(months_sorted) >= 3:
        # Generate future month labels
        from datetime import datetime
        _last_period = pd.Period(months_sorted[-1], freq="M")
        future_months = [str(_last_period + i) for i in range(1, 13)]
        _proj_x = [months_sorted[-1]] + future_months
        _proj_sales = analytics_projections["proj_sales"]
        _residual_std = analytics_projections.get("residual_std", 0)

        # Extend projections to 12 months using linear trend
        _lr_sales_trend = analytics_projections.get("sales_trend", 0)
        _base_idx = len(months_sorted)
        _proj_12 = [max(0, val_monthly_run_rate + _lr_sales_trend * (i - _base_idx + 1)) for i in range(_base_idx, _base_idx + 12)]

        proj_fig.add_trace(go.Scatter(
            x=_proj_x, y=[rev_vals[-1]] + _proj_12,
            mode="lines+markers", name="Projected Revenue",
            line=dict(color=CYAN, width=2, dash="dash"), marker=dict(size=5),
        ))
        # Confidence bands (widening)
        _upper = [rev_vals[-1]] + [max(0, _proj_12[i] + _residual_std * (i + 1) * 0.5) for i in range(12)]
        _lower = [rev_vals[-1]] + [max(0, _proj_12[i] - _residual_std * (i + 1) * 0.5) for i in range(12)]
        proj_fig.add_trace(go.Scatter(
            x=_proj_x + _proj_x[::-1], y=_upper + _lower[::-1],
            fill="toself", fillcolor="rgba(0,212,255,0.09)", line=dict(color="rgba(0,0,0,0)"),
            name="Confidence Band", showlegend=True,
        ))
        # Milestone annotations
        for milestone_val, milestone_label in [(5000, "$5K/mo"), (10000, "$10K/mo")]:
            for i, v in enumerate(_proj_12):
                if v >= milestone_val:
                    proj_fig.add_annotation(
                        x=future_months[i], y=v, text=milestone_label,
                        showarrow=True, arrowhead=2, arrowcolor=CYAN, font=dict(color=CYAN, size=10),
                    )
                    break
    make_chart(proj_fig, 360)
    proj_fig.update_layout(title="12-Month Growth Trajectory")

    children.append(section("D. GROWTH TRAJECTORY & PROJECTIONS", [
        chart_context(
            "Actual performance with 12-month linear projection and widening confidence bands.",
            metrics=[
                ("Monthly Run Rate", money(val_monthly_run_rate), GREEN),
                ("Proj 12mo Revenue", money(val_proj_12mo_revenue), CYAN),
                ("Growth", f"{_val_growth_pct:+.1f}%/mo", GREEN if _val_growth_pct > 0 else RED),
                ("R²", f"{_val_r2:.0%}", TEAL),
            ],
            simple="The solid lines show your real sales so far. The dashed line is where sales are headed if the current trend continues. The shaded area is the 'maybe' zone -- wider means less certain."
        ),
        dcc.Graph(figure=proj_fig, config={"displayModeBar": False}),
    ], GREEN))

    # ── SECTION E: REVENUE TO VALUE BRIDGE (Waterfall) ──
    wf_labels = ["Gross Sales", "Fees", "Shipping", "Ads", "Refunds", "Taxes"]
    wf_values = [gross_sales, -total_fees, -total_shipping_cost, -total_marketing, -total_refunds, -total_taxes]
    wf_measures = ["absolute", "relative", "relative", "relative", "relative", "relative"]
    wf_colors = [GREEN, RED, RED, RED, RED, RED]
    if total_buyer_fees > 0:
        wf_labels.append("CO Buyer Fee")
        wf_values.append(-total_buyer_fees)
        wf_measures.append("relative")
        wf_colors.append(RED)
    wf_labels += ["= After Etsy Fees", "Bank Expenses", "= PROFIT", "Owner Draws", "= SDE"]
    wf_values += [0, -bank_all_expenses, 0, bank_owner_draw_total, 0]
    wf_measures += ["total", "relative", "total", "relative", "total"]
    wf_colors += [TEAL, RED, GREEN, ORANGE, CYAN]

    waterfall_fig = go.Figure(go.Waterfall(
        orientation="v", measure=wf_measures,
        x=wf_labels, y=wf_values,
        connector={"line": {"color": GRAY, "width": 1, "dash": "dot"}},
        increasing={"marker": {"color": GREEN}},
        decreasing={"marker": {"color": RED}},
        totals={"marker": {"color": CYAN}},
        text=[money(abs(v)) if v != 0 else "" for v in wf_values],
        textposition="outside",
    ))
    make_chart(waterfall_fig, 380, False)
    waterfall_fig.update_layout(title="Revenue to SDE Bridge", showlegend=False)

    # Efficiency KPIs
    _profit_per_order = profit / order_count if order_count else 0
    _revenue_per_day = gross_sales / days_active if days_active else 0
    _etsy_take_rate = (total_fees + total_shipping_cost + total_marketing + total_taxes + total_buyer_fees) / gross_sales * 100 if gross_sales else 0

    children.append(section("E. REVENUE TO VALUE BRIDGE", [
        chart_context("Waterfall chart tracing every dollar from gross sales to SDE (Seller's Discretionary Earnings).",
                      simple="Start at the left with total sales. Each red bar takes money away (fees, shipping, etc). The blue bar at the end is what's left. Taller red bars = bigger expenses to investigate."),
        dcc.Graph(figure=waterfall_fig, config={"displayModeBar": False}),
        html.Div([
            kpi_card("Profit / Order", money(_profit_per_order), GREEN, f"{order_count} orders",
                     f"Profit ({money(profit)}) divided by {order_count} orders. This is how much profit you actually make per sale after ALL costs. Higher is better -- raise this by increasing prices, reducing shipping costs, or cutting refunds."),
            kpi_card("Monthly Run Rate", money(val_monthly_run_rate), TEAL, f"{_val_months_operating} months",
                     f"Average monthly gross sales: {money(gross_sales)} over {_val_months_operating} months. This is what you'd expect to earn in a typical month at current pace. Annualized: {money(val_annual_revenue)}."),
            kpi_card("Revenue / Day", money(_revenue_per_day), BLUE, f"{days_active} days active",
                     f"Gross sales ({money(gross_sales)}) divided by {days_active} days of operation. This is your daily earning rate. At this pace, you'd earn {money(_revenue_per_day * 365)} per year."),
            kpi_card("Etsy Take Rate", f"{_etsy_take_rate:.1f}%", ORANGE if _etsy_take_rate < 25 else RED, "All Etsy deductions",
                     f"What percentage Etsy takes from each dollar of sales: Fees {money(total_fees)} + Shipping {money(total_shipping_cost)} + Ads {money(total_marketing)} + Tax {money(total_taxes)} + Buyer Fees {money(total_buyer_fees)} = {money(total_fees + total_shipping_cost + total_marketing + total_taxes + total_buyer_fees)}. Industry typical: 20-30%."),
        ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginTop": "10px"}),
    ], ORANGE))

    # ── SECTION F: CASH POSITION & BALANCE SHEET ──
    bs_fig = go.Figure()
    bs_fig.add_trace(go.Bar(
        name="Assets", x=["Bank Cash", "Etsy Balance", "Equipment", "Inventory"],
        y=[bank_net_cash, etsy_balance, bb_cc_asset_value, true_inventory_cost],
        marker_color=[GREEN, TEAL, BLUE, PURPLE],
        text=[money(v) for v in [bank_net_cash, etsy_balance, bb_cc_asset_value, true_inventory_cost]],
        textposition="outside",
    ))
    bs_fig.add_trace(go.Bar(
        name="Liabilities", x=["CC Debt", "", "", ""],
        y=[bb_cc_balance, 0, 0, 0],
        marker_color=[RED, "rgba(0,0,0,0)", "rgba(0,0,0,0)", "rgba(0,0,0,0)"],
        text=[money(bb_cc_balance) if bb_cc_balance > 0 else "", "", "", ""],
        textposition="outside",
    ))
    make_chart(bs_fig, 300)
    bs_fig.update_layout(title="Assets vs Liabilities", barmode="group")

    _settlement_text = f"Company owes {draw_owed_to} {money(draw_diff)}" if draw_diff > 0 else "Draws are balanced"

    children.append(section("F. CASH POSITION & BALANCE SHEET", [
        dcc.Graph(figure=bs_fig, config={"displayModeBar": False}),
        html.Div([
            html.Div([
                row_item("Bank Cash (Capital One)", bank_net_cash, bold=True, color=GREEN),
                row_item("Etsy Balance (pending)", etsy_balance, indent=1),
                row_item("Total Cash", bank_cash_on_hand, bold=True, color=CYAN),
            ], style={"flex": "1", "minWidth": "250px"}),
            html.Div([
                row_item("Monthly Burn Rate", val_monthly_expenses, color=ORANGE),
                row_item("Runway (months)", val_runway_months, color=TEAL if val_runway_months > 3 else RED),
                row_item("Owner Draws Taken", bank_owner_draw_total, color=ORANGE),
                row_item("Draw Settlement", draw_diff, color=GRAY),
                html.P(_settlement_text, style={"color": GRAY, "fontSize": "11px", "marginTop": "4px"}),
            ], style={"flex": "1", "minWidth": "250px"}),
        ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginTop": "10px"}),
    ], BLUE))

    # ── SECTION G: PRODUCT PORTFOLIO VALUE ──
    _top_products = product_revenue_est.head(10)
    _other_rev = product_revenue_est.iloc[10:].sum() if len(product_revenue_est) > 10 else 0
    _donut_labels = [p[:30] for p in _top_products.index.tolist()]
    _donut_values = _top_products.values.tolist()
    if _other_rev > 0:
        _donut_labels.append("Other Products")
        _donut_values.append(_other_rev)

    product_donut = go.Figure(go.Pie(
        labels=_donut_labels, values=_donut_values, hole=0.5,
        textinfo="label+percent", textposition="outside",
    ))
    make_chart(product_donut, 340, False)
    product_donut.update_layout(title="Revenue by Product (Top 10)", showlegend=False)

    _total_prod_rev = product_revenue_est.sum()
    _top1_rev = product_revenue_est.values[0] if len(product_revenue_est) > 0 else 0
    _top1_name = product_revenue_est.index[0][:25] if len(product_revenue_est) > 0 else "N/A"

    children.append(section("G. PRODUCT PORTFOLIO VALUE", [
        dcc.Graph(figure=product_donut, config={"displayModeBar": False}),
        html.Div([
            kpi_card("Active Products", str(len(product_revenue_est)), TEAL, "unique product types",
                     f"{len(product_revenue_est)} unique products that generated sales. More products = more diversified revenue. Revenue is estimated by reverse-engineering the 6.5% Etsy transaction fee on each product."),
            kpi_card("Top-3 Concentration", f"{_top3_conc:.0f}%", ORANGE if _top3_conc > 60 else GREEN,
                     "of total estimated revenue",
                     f"How much revenue your top 3 products account for. {_top3_conc:.0f}% means {'most of your revenue depends on just 3 products -- risky if any stop selling' if _top3_conc > 60 else 'your revenue is reasonably spread across products -- good diversification'}. Below 50% is considered well-diversified."),
            kpi_card("Top Product", money(_top1_rev), GREEN, _top1_name,
                     f"Your best-selling product by estimated revenue. This single product accounts for {_top1_rev / _total_prod_rev * 100:.1f}% of total product revenue. Consider creating variations or bundles to capitalize on its popularity."),
            kpi_card("Avg Order Value", money(avg_order), BLUE, f"{order_count} total orders",
                     f"Total gross sales ({money(gross_sales)}) divided by {order_count} orders. Higher AOV means customers spend more per purchase. Increase AOV by bundling products, offering upsells, or raising prices on popular items."),
        ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginTop": "10px"}),
    ], PURPLE))

    # ── SECTION H: SENSITIVITY ANALYSIS ──
    sde_multiples = [1.0, 1.5, 2.0, 2.5, 3.0]
    growth_scenarios = [("-20%", 0.80), ("-10%", 0.90), ("Current", 1.00), ("+10%", 1.10), ("+20%", 1.20), ("+30%", 1.30)]

    table_rows = []
    # Header row
    table_rows.append(html.Tr([
        html.Th("SDE Multiple", style={"color": CYAN, "padding": "8px", "borderBottom": f"2px solid {CYAN}44", "textAlign": "left"}),
    ] + [
        html.Th(label, style={"color": GRAY, "padding": "8px", "borderBottom": f"2px solid {CYAN}44", "textAlign": "right"})
        for label, _ in growth_scenarios
    ]))

    for mult in sde_multiples:
        cells = [html.Td(f"{mult:.1f}x", style={"color": WHITE, "padding": "6px 8px", "fontWeight": "bold"})]
        for label, factor in growth_scenarios:
            val = val_annual_sde * factor * mult
            is_current = mult == 1.5 and label == "Current"
            cell_style = {
                "color": CYAN if is_current else WHITE,
                "padding": "6px 8px", "textAlign": "right", "fontFamily": "monospace", "fontSize": "12px",
                "backgroundColor": f"{CYAN}20" if is_current else "transparent",
                "fontWeight": "bold" if is_current else "normal",
                "borderRadius": "4px" if is_current else "0",
            }
            cells.append(html.Td(f"${val:,.0f}", style=cell_style))
        table_rows.append(html.Tr(cells, style={"borderBottom": "1px solid #ffffff08"}))

    children.append(section("H. SENSITIVITY ANALYSIS", [
        chart_context("How valuation changes with different SDE multiples and growth scenarios. "
                      "Current estimate highlighted in cyan.",
                      metrics=[("Current SDE", money(val_annual_sde), GREEN), ("Current Multiple", "1.5x", CYAN)],
                      simple="This table shows 'what if' scenarios. Each cell is a possible business value. The highlighted cell is the current estimate. Moving right = if the business grows more. Moving down = if a buyer pays a higher price multiple."),
        html.Table(table_rows, style={
            "width": "100%", "borderCollapse": "collapse",
            "backgroundColor": f"{CARD2}", "borderRadius": "6px",
        }),
    ], CYAN))

    # ── SECTION I: INDUSTRY BENCHMARKS ──
    _fee_rate = total_fees / gross_sales * 100 if gross_sales else 0
    _refund_rate = total_refunds / gross_sales * 100 if gross_sales else 0
    _cogs_ratio = true_inventory_cost / gross_sales * 100 if gross_sales else 0

    bench_categories = ["Profit Margin", "Fee Rate", "Refund Rate", "Supply Cost Ratio", "Monthly Growth"]
    bench_actual = [profit_margin, _fee_rate, _refund_rate, _cogs_ratio, _val_growth_pct]
    bench_avg = [15, 13, 3.0, 25, 5]  # Industry averages for small Etsy businesses
    bench_good = [25, 10, 1.5, 15, 10]  # "Good" benchmarks

    bench_fig = go.Figure()
    bench_fig.add_trace(go.Bar(
        name="Your Business", x=bench_categories, y=bench_actual,
        marker_color=CYAN, text=[f"{v:.1f}%" for v in bench_actual], textposition="outside",
    ))
    bench_fig.add_trace(go.Scatter(
        name="Industry Average", x=bench_categories, y=bench_avg,
        mode="markers+lines", marker=dict(color=ORANGE, size=10, symbol="diamond"),
        line=dict(color=ORANGE, dash="dash"),
    ))
    bench_fig.add_trace(go.Scatter(
        name="Good Benchmark", x=bench_categories, y=bench_good,
        mode="markers+lines", marker=dict(color=GREEN, size=10, symbol="star"),
        line=dict(color=GREEN, dash="dot"),
    ))
    make_chart(bench_fig, 320)
    bench_fig.update_layout(title="Your Business vs Industry Benchmarks (%)")

    children.append(section("I. INDUSTRY BENCHMARKS", [
        chart_context("Compare your key metrics against typical Etsy small business averages and 'good' benchmarks.",
                      legend=[
                          (CYAN, "Your Business", "actual performance"),
                          (ORANGE, "Industry Average", "typical small Etsy shop"),
                          (GREEN, "Good Benchmark", "top-performing shops"),
                      ],
                      simple="Blue bars are YOUR numbers. Orange diamonds are average Etsy shops. Green stars are top performers. You want your bars to beat the orange diamonds and get close to the green stars."),
        dcc.Graph(figure=bench_fig, config={"displayModeBar": False}),
    ], TEAL))

    # ── SECTION J: KEY METRICS TIMELINE ──
    timeline_fig = make_subplots(specs=[[{"secondary_y": True}]])
    timeline_fig.add_trace(go.Bar(
        name="Monthly Revenue", x=months_sorted,
        y=[monthly_sales.get(m, 0) for m in months_sorted],
        marker_color=GREEN, opacity=0.7,
    ))
    timeline_fig.add_trace(go.Scatter(
        name="Profit Margin %", x=months_sorted,
        y=[monthly_net_revenue.get(m, 0) / monthly_sales.get(m, 1) * 100 for m in months_sorted],
        mode="lines+markers+text",
        text=[f"{monthly_net_revenue.get(m, 0) / monthly_sales.get(m, 1) * 100:.0f}%" for m in months_sorted],
        textposition="top center", textfont=dict(color=ORANGE),
        line=dict(color=ORANGE, width=2), marker=dict(size=8),
    ), secondary_y=True)
    timeline_fig.add_trace(go.Scatter(
        name="AOV", x=months_sorted,
        y=[monthly_aov.get(m, 0) for m in months_sorted],
        mode="lines+markers",
        line=dict(color=PURPLE, width=2, dash="dot"), marker=dict(size=6),
    ), secondary_y=True)
    make_chart(timeline_fig, 340)
    timeline_fig.update_layout(title="Key Metrics Over Time")
    timeline_fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
    timeline_fig.update_yaxes(title_text="Margin % / AOV ($)", secondary_y=True)

    children.append(section("J. KEY METRICS TIMELINE", [
        chart_context("Monthly revenue (bars) with profit margin % and AOV overlaid.",
                      metrics=[
                          ("Avg Margin", f"{profit_margin:.1f}%", ORANGE),
                          ("Avg AOV", money(avg_order), PURPLE),
                          ("Months", str(_val_months_operating), GRAY),
                      ],
                      simple="Green bars show how much you sold each month (taller = more sales). The orange line shows what percentage you kept as profit. Both going up together = healthy growth."),
        dcc.Graph(figure=timeline_fig, config={"displayModeBar": False}),
    ], ORANGE))

    # ── SECTION K: VALUATION NOTES & METHODOLOGY ──
    children.append(section("K. VALUATION NOTES & METHODOLOGY", [
        html.Div([
            html.H4("Methodology", style={"color": CYAN, "fontSize": "13px", "margin": "0 0 8px 0"}),
            html.Ul([
                html.Li([html.B("SDE Multiple (50%): "), "Seller's Discretionary Earnings = Profit + Owner Draws. "
                         "Annualized from 5 months, multiplied by 1.0x (floor), 1.5x (typical), 2.5x (optimistic) for small e-commerce businesses."],
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
                html.Li([html.B("Revenue Multiple (25%): "), "Gross sales annualized, multiplied by 0.3x-1.0x. "
                         "Etsy shops typically sell for 0.3-0.8x annual revenue depending on growth and margins."],
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
                html.Li([html.B("Asset-Based (25%): "), "Tangible assets (cash + equipment + inventory) minus liabilities (CC debt). "
                         "Floor valuation — what the business is worth if liquidated today."],
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
                html.Li([html.B("Blended: "), "Weighted average: SDE (50%) + Revenue (25%) + Asset (25%). "
                         "Gives heaviest weight to earnings power while accounting for revenue scale and hard assets."],
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
            ]),
            html.H4("Data Period & Caveats", style={"color": ORANGE, "fontSize": "13px", "margin": "12px 0 8px 0"}),
            html.Ul([
                html.Li(f"Data covers {_val_months_operating} months (Oct 2025 — Feb 2026). "
                        "Annualization assumes current performance is representative. Seasonal businesses may see significant variance.",
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
                html.Li("Growth projections use linear regression on monthly data. "
                        f"R² = {_val_r2:.0%} — {'high confidence' if _val_r2 > 0.7 else 'moderate confidence, take projections as estimates'}.",
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
                html.Li("Product revenue estimates are derived from 6.5% transaction fee reverse-engineering, not exact sale prices.",
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
            ]),
            html.Div([
                html.Span("DISCLAIMER: ", style={"color": RED, "fontWeight": "bold", "fontSize": "11px"}),
                html.Span("This valuation is for internal planning purposes only. Actual business sale price depends on "
                          "buyer negotiations, market conditions, verified financials, and due diligence. "
                          "Consult a business broker or CPA for formal valuation.",
                          style={"color": GRAY, "fontSize": "11px"}),
            ], style={"padding": "10px", "backgroundColor": f"{RED}10", "borderRadius": "6px",
                      "border": f"1px solid {RED}33", "marginTop": "8px"}),
        ]),
    ], GRAY))

    return html.Div(children, style={"padding": TAB_PADDING})


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
    _unmatched_count = len([t for t in _bank_no_receipt
                            if t.get("category") in ("Amazon Inventory", "AliExpress Supplies", "Craft Supplies")])
    _unmatched_total = sum(t["amount"] for t in _bank_no_receipt
                           if t.get("category") in ("Amazon Inventory", "AliExpress Supplies", "Craft Supplies"))
    if _unmatched_count > 0:
        todos.append((2, "\U0001f9fe", ORANGE,
                      f"{_unmatched_count} bank charges (${_unmatched_total:,.0f}) without matching receipts",
                      f"Upload invoice PDFs for these purchases in Data Hub → Inventory Receipts."))

    # ── 10. Etsy balance gap ──
    if abs(etsy_csv_gap) > 5:
        todos.append((2, "\U0001f4b1", ORANGE,
                      f"Etsy balance gap: ${abs(etsy_csv_gap):,.2f}",
                      f"Reported: ${etsy_balance:,.2f} vs Calculated: ${etsy_balance_calculated:,.2f}. "
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


def _build_pl_row(label, amount_str, color, bold=False, border=False):
    """Return a list of Dash components for one P&L waterfall row.
    Use * splat to unpack into a parent's children list."""
    row_style = {
        "display": "flex", "justifyContent": "space-between",
        "padding": "4px 0",
        "fontWeight": "bold" if bold else "normal",
    }
    if border:
        row_style["borderTop"] = f"1px solid {DARKGRAY}44"
        row_style["marginTop"] = "2px"
        row_style["paddingTop"] = "6px"
    return [html.Div([
        html.Span(label, style={"color": color if bold else GRAY, "flex": "1",
                                 "fontSize": "14px" if bold else "13px"}),
        html.Span(amount_str, style={"color": color, "fontFamily": "monospace",
                                      "fontSize": "14px" if bold else "13px",
                                      "fontWeight": "bold" if bold else "normal"}),
    ], style=row_style)]


def build_tab1_overview():
    """Tab 1 - Overview: Business health at a glance. Single screen, no scrolling."""
    return html.Div([
        # KPI Strip (4 pills)
        html.Div([
            _build_kpi_pill("\U0001f4ca", "REVENUE", f"${gross_sales:,.2f}", TEAL,
                            f"{order_count} orders, avg ${avg_order:,.2f}",
                            f"Total from {order_count} orders, avg ${avg_order:,.2f} each."),
            _build_kpi_pill("\U0001f4b0", "PROFIT", f"${profit:,.2f}", GREEN,
                            f"{profit_margin:.1f}% margin",
                            f"Revenue minus all costs -- {profit_margin:.1f}% margin."),
            _build_kpi_pill("\U0001f3e6", "CASH", f"${bank_cash_on_hand:,.2f}", CYAN,
                            f"Bank ${bank_net_cash:,.0f} + Etsy ${etsy_balance:,.0f}",
                            f"Bank ${bank_net_cash:,.2f} + Etsy pending ${etsy_balance:,.2f}."),
            _build_kpi_pill("\U0001f4b3", "DEBT", f"${bb_cc_balance:,.2f}", RED,
                            f"Best Buy CC (${bb_cc_available:,.0f} avail)",
                            f"Best Buy Citi CC -- ${bb_cc_available:,.0f} available of ${bb_cc_limit:,.0f} limit."),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "14px", "flexWrap": "wrap"}),

        # Dashboard Health / To-Do panel
        _build_health_checks(),

        # Simplified data sources one-liner
        html.Div([
            html.Span(f"{order_count} orders", style={"color": TEAL, "fontWeight": "bold", "fontSize": "13px"}),
            html.Span("  |  ", style={"color": DARKGRAY}),
            html.Span(f"{len(BANK_TXNS)} bank transactions", style={"color": CYAN, "fontWeight": "bold", "fontSize": "13px"}),
            html.Span("  |  ", style={"color": DARKGRAY}),
            html.Span(f"{len(INVOICES)} receipts", style={"color": PURPLE, "fontWeight": "bold", "fontSize": "13px"}),
        ], style={"backgroundColor": CARD, "padding": "10px 16px", "borderRadius": "10px",
                  "borderLeft": f"4px solid {CYAN}", "marginBottom": "12px", "textAlign": "center"}),

        # Quick P&L + Monthly chart side by side
        html.Div([
            # Quick P&L
            html.Div([
                html.H3("Profit & Loss", style={"color": CYAN, "margin": "0 0 10px 0", "fontSize": "15px"}),
                *_build_pl_row("Gross Sales", f"${gross_sales:,.0f}", GREEN, bold=True),
                *_build_pl_row("  Etsy Deductions (fees, ship, ads, refunds, tax)",
                               f"-${gross_sales - etsy_net:,.0f}", RED),
                *_build_pl_row("= After Etsy Fees", f"${etsy_net:,.0f}", WHITE, bold=True, border=True),
                *_build_pl_row("  Supplies & Materials",
                               f"-${true_inventory_cost:,.0f}", RED),
                *_build_pl_row("  Business Expenses",
                               f"-${max(0, bank_total_debits - bank_amazon_inv - bank_owner_draw_total):,.0f}", RED),
                *_build_pl_row("  Owner Draws (TJ + Braden)",
                               f"-${bank_owner_draw_total:,.0f}", ORANGE),
                html.Div([
                    html.Span("= CASH ON HAND", style={"color": GREEN, "flex": "1", "fontSize": "16px",
                              "fontWeight": "bold"}),
                    html.Span(f"${bank_cash_on_hand:,.0f}", style={"color": GREEN, "fontFamily": "monospace",
                              "fontSize": "18px", "fontWeight": "bold"}),
                ], style={"display": "flex", "justifyContent": "space-between", "padding": "8px 0",
                          "borderTop": f"2px solid {GREEN}44", "marginTop": "4px"}),
            ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
                      "flex": "1", "minWidth": "280px"}),

            # Monthly Performance chart
            html.Div([
                dcc.Graph(figure=monthly_fig, config={"displayModeBar": False}),
            ], style={"flex": "2"}),
        ], style={"display": "flex", "gap": "12px"}),
    ], style={"padding": TAB_PADDING})


def build_tab2_deep_dive():
    """Tab 2 - Deep Dive: AI Analytics + Trends + Daily chart + Chatbot"""
    # AI Analytics insight cards
    insight_cards = []
    for _, cat, title, detail, sev in analytics_insights:
        sc = severity_color(sev)
        insight_cards.append(html.Div([
            html.Div([
                html.Span(cat, style={
                    "color": BG, "backgroundColor": sc, "padding": "2px 8px",
                    "borderRadius": "4px", "fontSize": "11px", "fontWeight": "bold",
                }),
            ], style={"marginBottom": "6px"}),
            html.H4(title, style={"color": sc, "margin": "0 0 6px 0", "fontSize": "14px"}),
            html.P(detail, style={"color": "#cccccc", "margin": "0", "fontSize": "12px", "lineHeight": "1.5"}),
        ], style={
            "backgroundColor": "#0d1b2a", "padding": "14px", "borderRadius": "8px",
            "borderLeft": f"4px solid {sc}", "marginBottom": "8px",
        }))

    return html.Div([
        # ── AI ANALYTICS ──
        html.Div([
            html.H3("AI ANALYTICS BOT", style={"color": CYAN, "margin": "0", "display": "inline-block"}),
            html.Span("  LIVE", style={
                "color": BG, "backgroundColor": CYAN, "padding": "2px 10px",
                "borderRadius": "12px", "fontSize": "11px", "fontWeight": "bold", "marginLeft": "12px",
                "verticalAlign": "middle",
            }),
            html.P("Deep analysis -- learns from all transaction data, detects money leaks, projects future revenue",
                   style={"color": GRAY, "margin": "4px 0 0 0", "fontSize": "13px"}),
        ], style={"marginBottom": "12px"}),

        chart_context(
            "Linear regression forecast based on your monthly revenue trend. Solid lines = actual, dashed = projected. "
            "The shaded band shows the confidence range — wider band means less predictable.",
            metrics=[
                ("Growth", f"{_growth_pct:+.1f}%/mo", GREEN if _growth_pct > 0 else RED),
                ("R\u00b2 Fit", f"{_r2_sales:.2f}", TEAL),
                ("Latest Month Rev", f"${_latest_month_rev:,.0f}", GREEN),
                ("Latest Month Net", f"${_latest_month_net:,.0f}", ORANGE),
            ],
            look_for="Dashed lines trending up = growing business. R\u00b2 close to 1.0 = very predictable trend.",
            simple="Solid lines = your actual sales and profit each month. Dashed lines = where a computer model predicts you're headed. If dashed lines point up, the business is growing."
        ),
        dcc.Graph(figure=proj_chart, config={"displayModeBar": False}, style={"height": "380px"}),
        html.Div(insight_cards),

        # ── TRENDS ──
        html.H3("TRENDS & PATTERNS", style={"color": CYAN, "margin": "30px 0 6px 0",
                 "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px"}),
        html.P("How every metric changes over time -- spot patterns, momentum shifts, and emerging risks.",
               style={"color": GRAY, "margin": "0 0 14px 0", "fontSize": "13px"}),

        chart_context(
            "Green bars = daily revenue, orange bars = daily profit. Dashed lines = 30-day rolling averages that smooth out noise.",
            metrics=[
                ("Avg Rev/Day", f"${_daily_rev_avg:,.0f}", GREEN),
                ("Avg Profit/Day", f"${_daily_profit_avg:,.0f}", ORANGE),
                ("Best Day", f"${_best_day_rev:,.0f}", TEAL),
            ],
            look_for="Rolling averages trending up together. If revenue rises but profit flattens, costs are eating your growth.",
            simple="Each bar is one day. Green = what you earned, orange = what you kept as profit. The smooth lines filter out the noise to show the overall direction. Lines going up = business improving.",
        ),
        dcc.Graph(figure=trend_profit_rev, config={"displayModeBar": False}, style={"height": "380px"}),

        chart_context(
            "Each line shows how many cents per dollar of sales go to that cost category. "
            "Net Margin is what you keep after all Etsy deductions.",
            metrics=[
                ("Fees", f"{_last_fee_pct:.1f}%", RED),
                ("Shipping", f"{_last_ship_pct:.1f}%", BLUE),
                ("Marketing", f"{_last_mkt_pct:.1f}%", PURPLE),
                ("Refunds", f"{_last_ref_pct:.1f}%", ORANGE),
                ("Net Margin", f"{_last_margin_pct:.1f}%", TEAL),
            ],
            legend=[
                (RED, "Fees %", "Etsy listing, transaction & payment processing fees combined"),
                (BLUE, "Shipping %", "Cost of USPS/Asendia shipping labels you purchased"),
                (PURPLE, "Marketing %", "Etsy Ads + Offsite Ads spend as % of sales"),
                (ORANGE, "Refunds %", "Money returned to buyers — lower is better"),
                (TEAL, "Net Margin %", "What you actually keep from each dollar of sales (dashed line)"),
            ],
            look_for="Lines trending down = improving efficiency. Net Margin trending up = you're keeping more per sale.",
            simple="Each line tracks a different cost as a percentage of sales. Lines going DOWN means you're getting more efficient. The teal dashed line (Net Margin) going UP is the best sign -- it means you're keeping more of each dollar.",
        ),
        dcc.Graph(figure=cost_ratio_fig, config={"displayModeBar": False}, style={"height": "380px"}),

        html.Div([
            html.Div([
                chart_context(
                    "Average Order Value = total revenue \u00f7 number of orders per week. Higher AOV means customers spend more per purchase.",
                    metrics=[
                        ("Overall AOV", f"${avg_order:,.2f}", TEAL),
                        ("Best Week", f"${_aov_best_week:,.2f}", GREEN),
                        ("Worst Week", f"${_aov_worst_week:,.2f}", RED),
                    ],
                    look_for="Upward trend = customers spending more. Sudden drops may mean discounting or smaller product mix.",
                    simple="This shows how much each customer spends on average per order. Line going up = customers spending more per purchase. The gray dashed line is your overall average.",
                ),
                dcc.Graph(figure=aov_fig, config={"displayModeBar": False}, style={"height": "340px"}),
            ], style={"flex": "1"}),
            html.Div([
                chart_context(
                    "How many orders you get each day, separated from dollar amounts. Blue bars = daily count, lines = smoothed averages.",
                    metrics=[
                        ("Avg Orders/Day", f"{_daily_orders_avg:.1f}", BLUE),
                        ("Peak Day", f"{_peak_orders_day:.0f}", GREEN),
                    ],
                    look_for="Rising trend = growing demand. Spikes often follow promotions, holidays, or viral listings.",
                    simple="Each blue bar is how many orders came in that day. The smooth lines show the trend. More bars and higher lines = more customers finding your shop.",
                ),
                dcc.Graph(figure=orders_day_fig, config={"displayModeBar": False}, style={"height": "340px"}),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "8px"}),

        chart_context(
            "Running totals since your first sale. The gap between the green (revenue) and orange (profit) lines = total costs paid to date.",
            metrics=[
                ("Total Revenue", f"${gross_sales:,.0f}", GREEN),
                ("Total Profit", f"${etsy_net:,.0f}", ORANGE),
                ("Total Costs", f"${_total_costs:,.0f}", RED),
                ("Margin", f"{_net_margin_overall:.1f}%", TEAL),
            ],
            look_for="Lines spreading apart = costs growing faster than revenue. Parallel lines = stable margins.",
            simple="These lines show your total earnings since day one -- green for revenue, orange for profit. The gap between them is your total costs. Both lines should keep climbing. If the gap widens, costs are growing faster than sales.",
        ),
        dcc.Graph(figure=cum_fig, config={"displayModeBar": False}, style={"height": "380px"}),

        html.Div([
            html.Div([
                chart_context(
                    "Smoothed daily profit using a 14-day rolling average. Above the red zero line = making money that period.",
                    metrics=[
                        ("Avg Profit/Day", f"${_daily_profit_avg:,.2f}", ORANGE),
                        ("Current 14d Avg", f"${_current_14d_profit_avg:,.2f}", TEAL),
                    ],
                    look_for="Line staying above zero and trending up. Dips below zero = you were losing money those weeks.",
                    simple="This smooths out daily ups and downs to show if you're actually making money. Line ABOVE the red zero line = making money. Line BELOW = losing money. The higher above zero, the better.",
                ),
                dcc.Graph(figure=profit_rolling_fig, config={"displayModeBar": False}, style={"height": "340px"}),
            ], style={"flex": "1"}),
            html.Div([
                chart_context(
                    "Green/red bars = average profit you make per order each month. Dashed line = AOV (average order value) on right axis.",
                    metrics=[
                        ("Latest Profit/Order", f"${_last_ppo_val:,.2f}", GREEN if _last_ppo_val >= 0 else RED),
                        ("Latest AOV", f"${_last_aov_val:,.2f}", TEAL),
                    ],
                    look_for="Green bars getting taller = more profit per sale. Red bars = losing money per order that month.",
                    simple="Green bars = you made money per order that month. Red bars = you lost money. The dashed line shows average order size. You want green bars getting taller over time.",
                ),
                dcc.Graph(figure=ppo_fig, config={"displayModeBar": False}, style={"height": "340px"}),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "8px"}),

        # Daily sales chart (from old Overview)
        chart_context(
            "Green bars = daily revenue, blue line = order count (right axis), orange line = 7-day average revenue.",
            metrics=[
                ("Avg Rev/Day", f"${_daily_rev_avg:,.0f}", GREEN),
                ("Avg Orders/Day", f"{_daily_orders_avg:.1f}", BLUE),
            ],
            look_for="Orange trend line direction shows your momentum. Blue line diverging from green = order size changing.",
            simple="Green bars show daily sales dollars. Blue line shows number of orders. Orange line smooths out the daily noise. All three going up = healthy growing business.",
        ),
        dcc.Graph(figure=daily_fig, config={"displayModeBar": False}, style={"height": "380px"}),

        # ── PATTERN RECOGNITION ──
        html.H3("PATTERN RECOGNITION", style={"color": CYAN, "margin": "30px 0 6px 0",
                 "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px"}),
        html.P("Statistical pattern detection across all data sources -- day-of-week cycles, anomalies, and correlations.",
               style={"color": GRAY, "margin": "0 0 14px 0", "fontSize": "13px"}),

        html.Div([
            html.Div([
                chart_context(
                    f"Average performance by day of week. Best day: {_best_dow}. Worst day: {_worst_dow}.",
                    metrics=[
                        ("Best Day", f"{_best_dow} (${max(_dow_rev_vals):,.0f})", GREEN),
                        ("Worst Day", f"{_worst_dow} (${min(_dow_rev_vals):,.0f})", RED),
                    ],
                    look_for="Consistent patterns reveal when to run promotions or launch new listings.",
                    simple="This shows which days of the week you sell the most. Taller bars = better days. Use this to know when to launch new products or run promotions.",
                ),
                dcc.Graph(figure=dow_fig, config={"displayModeBar": False}, style={"height": "340px"}),
            ], style={"flex": "1"}),
            html.Div([
                chart_context(
                    f"Revenue outliers detected with z-scores. Spikes (>2σ): {len(_anomaly_high)}. "
                    f"Drops (<-1.5σ): {len(_anomaly_low)}. Zero days: {len(_zero_days)}.",
                    metrics=[
                        ("Mean Revenue", f"${_daily_rev_mean:,.0f}/day", TEAL),
                        ("Std Dev", f"${_daily_rev_std:,.0f}", GRAY),
                        ("Spikes", str(len(_anomaly_high)), GREEN),
                        ("Drops", str(len(_anomaly_low)), RED),
                    ],
                    look_for="Investigate spikes (what sold?) and drops (listing issues? shipping delays?).",
                    simple="The gray line is your daily sales. Green triangles mark unusually GOOD days (big sales spikes). Red triangles mark unusually BAD days. Orange X's are days with zero sales. Investigate what happened on those days.",
                ),
                dcc.Graph(figure=anomaly_fig, config={"displayModeBar": False}, style={"height": "340px"}),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "8px"}),

        chart_context(
            "Does spending more on ads actually drive more sales? Each dot is one month.",
            metrics=[
                ("Correlation R²", f"{_corr_r2:.2f}", GREEN if _corr_r2 > 0.5 else ORANGE),
                ("Total Ad Spend", money(total_marketing), PURPLE),
                ("Ad % of Sales", f"{total_marketing / gross_sales * 100:.1f}%" if gross_sales else "0%", ORANGE),
            ],
            look_for=f"{'Strong correlation -- ads are driving sales.' if _corr_r2 > 0.5 else 'Weak correlation -- sales may not depend much on ad spend. Test cutting ads.'}",
            simple="Each dot is one month. If dots make a line going up-right, ads are working (spend more → earn more). If dots are scattered randomly, ads might not be helping much.",
        ),
        dcc.Graph(figure=corr_fig, config={"displayModeBar": False}, style={"height": "320px"}),

        chart_context(
            "Product performance over time. Bright = high revenue. Dark = low/no sales. "
            "Spot rising stars, declining products, and seasonal patterns.",
            metrics=[
                ("Products Tracked", str(len(_top_prod_names)), TEAL),
                ("Top Product", f"${product_revenue_est.values[0]:,.0f}" if len(product_revenue_est) > 0 else "N/A", GREEN),
            ],
            look_for="Products getting brighter over time are growing. Products going dark need attention or retirement.",
            simple="Each row is a product, each column is a month. Bright green = lots of sales. Dark purple = few sales. Black = no sales. Look for rows getting brighter (growing products) or darker (declining ones).",
        ),
        dcc.Graph(figure=product_heat, config={"displayModeBar": False}, style={"height": "360px"}),

        # ── UNIT ECONOMICS & INVENTORY ──
        html.H3("UNIT ECONOMICS & INVENTORY", style={"color": CYAN, "margin": "30px 0 6px 0",
                 "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px"}),
        html.P("Per-order profitability, inventory spend tracking, and cost-of-goods analysis from invoice data.",
               style={"color": GRAY, "margin": "0 0 14px 0", "fontSize": "13px"}),

        html.Div([
            html.Div([
                chart_context(
                    f"What happens to each ${avg_order:,.2f} order on average. Every dollar flows through fees, "
                    f"shipping, ads, refunds, and COGS before becoming profit.",
                    metrics=[
                        ("Avg Revenue", money(_unit_rev), GREEN),
                        ("Avg Profit", money(_unit_profit), CYAN),
                        ("Unit Margin", f"{_unit_margin:.1f}%", GREEN if _unit_margin > 20 else ORANGE),
                        ("COGS/Order", money(_unit_cogs), PURPLE),
                    ],
                    look_for="Profit bar should be at least 20% of revenue bar. If supply costs are the biggest deduction, find cheaper suppliers.",
                    simple="Start with what a customer pays (left bar). Each red bar shows where that money goes -- fees, shipping, etc. The blue bar at the end is your actual profit per order. If the blue bar is small, your costs are eating too much.",
                ),
                dcc.Graph(figure=unit_wf, config={"displayModeBar": False}, style={"height": "340px"}),
            ], style={"flex": "3"}),
            html.Div([
                html.Div("BREAK-EVEN ANALYSIS", style={"color": CYAN, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "8px"}),
                row_item("Monthly Fixed Costs", _monthly_fixed, color=RED),
                row_item("Contribution Margin", _contrib_margin_pct * 100, color=GREEN),
                html.Div(style={"borderTop": f"1px solid {CYAN}44", "margin": "6px 0"}),
                row_item("Break-Even Revenue/Mo", _breakeven_monthly, bold=True, color=CYAN),
                row_item("Break-Even Revenue/Day", _breakeven_daily, color=TEAL),
                row_item("Break-Even Orders/Mo", _breakeven_orders, color=BLUE),
                html.Div(style={"height": "10px"}),
                html.P(f"{'You are ABOVE break-even.' if val_monthly_run_rate > _breakeven_monthly else 'WARNING: Below break-even -- you need more revenue to cover fixed costs.'}"
                       if _breakeven_monthly > 0 else "Insufficient data for break-even.",
                       style={"color": GREEN if val_monthly_run_rate > _breakeven_monthly else RED,
                              "fontSize": "11px", "fontWeight": "bold"}),
                html.P(f"Surplus: {money(val_monthly_run_rate - _breakeven_monthly)}/mo above break-even"
                       if val_monthly_run_rate > _breakeven_monthly and _breakeven_monthly > 0 else "",
                       style={"color": TEAL, "fontSize": "11px"}),
            ], style={"flex": "2", "padding": "12px", "backgroundColor": CARD, "borderRadius": "8px",
                      "border": f"1px solid {CYAN}33"}),
        ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}),

        chart_context(
            "Revenue (green) vs inventory spend (purple) each month. Orange line = COGS ratio "
            "(% of revenue going to materials). Rising ratio = margins shrinking.",
            metrics=[
                ("Total Inventory", money(true_inventory_cost), PURPLE),
                ("Supply Cost Ratio", f"{true_inventory_cost / gross_sales * 100:.1f}%" if gross_sales else "N/A", ORANGE),
                ("Avg Monthly Spend", money(true_inventory_cost / _val_months_operating), TEAL),
            ],
            look_for="COGS ratio should stay flat or decrease. If it's rising, you're spending more on materials relative to sales.",
            simple="Green bars = what you earned. Purple bars = what you spent on supplies. The orange line shows supplies as a percentage of sales. If the orange line goes up, you're spending more on materials relative to what you're earning.",
        ),
        dcc.Graph(figure=rev_inv_fig, config={"displayModeBar": False}, style={"height": "360px"}),

        html.Div([
            html.Div([
                chart_context(
                    "What categories of supplies you're buying. Parsed from Amazon invoice PDFs.",
                    metrics=[
                        ("Total Categories", str(len(biz_inv_by_category)), TEAL),
                        ("Biggest", f"{biz_inv_by_category.index[0] if len(biz_inv_by_category) > 0 else 'N/A'}", PURPLE),
                    ],
                    simple="Each bar shows how much you spent on a type of supply (filament, packaging, etc). The tallest bars are where most of your supply money goes. Look for categories that seem too high.",
                ),
                dcc.Graph(figure=inv_cat_bar, config={"displayModeBar": False}, style={"height": "340px"}),
            ], style={"flex": "1"}),
            html.Div([
                chart_context(
                    "Inventory shipments split by location. Shows which partner location is ordering more supplies.",
                    metrics=[
                        ("Tulsa", money(tulsa_spend), TEAL),
                        ("Texas", money(texas_spend), ORANGE),
                    ],
                    simple="Shows where your supplies are being shipped -- Tulsa vs Texas. This helps see if one location is ordering way more than the other.",
                ),
                dcc.Graph(figure=loc_fig, config={"displayModeBar": False}, style={"height": "340px"}),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "8px"}),

        # Supplier analysis (top suppliers)
        *([html.Div([
            html.Div("TOP SUPPLIERS", style={"color": PURPLE, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "8px"}),
            *[html.Div([
                html.Span(seller[:30], style={"color": WHITE, "fontSize": "12px", "flex": "1"}),
                html.Span(f"{info['items']} items", style={"color": GRAY, "fontSize": "11px", "marginRight": "12px"}),
                html.Span(f"avg ${info['avg_price']:,.2f}", style={"color": GRAY, "fontSize": "11px", "marginRight": "12px"}),
                html.Span(money(info['total']), style={"color": PURPLE, "fontFamily": "monospace", "fontSize": "12px", "fontWeight": "bold"}),
            ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                      "padding": "4px 0", "borderBottom": "1px solid #ffffff08"})
              for seller, info in list(_supplier_spend.items())[:10]],
        ], style={"backgroundColor": CARD, "padding": "14px", "borderRadius": "8px",
                  "marginBottom": "14px", "borderLeft": f"3px solid {PURPLE}"})] if _supplier_spend else []),

        # ── CASH FLOW & FINANCIAL HEALTH ──
        html.H3("CASH FLOW & FINANCIAL HEALTH", style={"color": CYAN, "margin": "30px 0 6px 0",
                 "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px"}),
        html.P("Bank-reconciled cash movement -- every dollar deposited and spent through Capital One.",
               style={"color": GRAY, "margin": "0 0 14px 0", "fontSize": "13px"}),

        chart_context(
            "Green = Etsy deposits to your bank. Red = business expenses paid. Cyan line = net cash flow each month.",
            metrics=[
                ("Total Deposits", money(bank_total_deposits), GREEN),
                ("Total Expenses", money(bank_total_debits), RED),
                ("Net Cash", money(bank_net_cash), CYAN),
            ],
            look_for="Cyan line should stay positive. Negative months mean you spent more than Etsy deposited.",
            simple="Green bars = money coming IN (Etsy paying you). Red bars = money going OUT (expenses). The cyan line is the difference. Cyan line above zero = you kept more than you spent that month. Below zero = you overspent.",
        ),
        dcc.Graph(figure=cashflow_fig, config={"displayModeBar": False}, style={"height": "360px"}),

        # Key financial metrics grid
        html.Div([
            _build_kpi_pill("🔄", "INVENTORY TURNOVER",
                     f"{gross_sales / true_inventory_cost:.1f}x" if true_inventory_cost > 0 else "N/A",
                     TEAL,
                     detail=(f"How many times your inventory investment generates revenue. {gross_sales / true_inventory_cost:.1f}x means "
                             f"every $1 of inventory generates ${gross_sales / true_inventory_cost:.2f} in revenue. "
                             f"Higher is better. Benchmark: 4-8x for handmade goods." if true_inventory_cost > 0 else "No inventory data.")),
            _build_kpi_pill("📊", "GROSS MARGIN",
                     f"{(gross_sales - true_inventory_cost) / gross_sales * 100:.1f}%" if gross_sales else "N/A",
                     GREEN, subtitle="Revenue minus COGS only",
                     detail=(f"Revenue ({money(gross_sales)}) minus cost of goods ({money(true_inventory_cost)}) = "
                             f"{money(gross_sales - true_inventory_cost)} gross profit. This ignores Etsy fees and other expenses. "
                             f"Benchmark: >60% for handmade, >40% for resale.")),
            _build_kpi_pill("💰", "OPERATING MARGIN",
                     f"{profit_margin:.1f}%", GREEN if profit_margin > 15 else ORANGE,
                     subtitle="After ALL expenses",
                     detail=(f"Revenue minus all costs. Cash on hand ({money(bank_cash_on_hand)}) "
                             f"+ owner draws ({money(bank_owner_draw_total)}) = {money(profit)} ({profit_margin:.1f}%). "
                             f"All expenses flow through bank.")),
            _build_kpi_pill("💵", "CASH CONVERSION",
                     f"{bank_cash_on_hand / gross_sales * 100:.1f}%" if gross_sales else "N/A",
                     CYAN, subtitle="Cash retained / Revenue",
                     detail=(f"What % of gross sales you actually retained as cash: {money(bank_cash_on_hand)} / {money(gross_sales)}. "
                             f"The rest went to expenses, inventory, and owner draws. Higher = more efficient cash management.")),
        ], style={"display": "flex", "gap": "8px", "flexWrap": "wrap", "marginBottom": "14px"}),

        # ── CHATBOT ──
        html.H3("DATA CHATBOT", style={"color": CYAN, "margin": "30px 0 4px 0",
                 "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px"}),
        html.P("Ask any question about your Etsy store data. Try: \"How much money have I made?\" or \"What are my best sellers?\"",
               style={"color": GRAY, "margin": "0 0 14px 0", "fontSize": "13px"}),

        # Quick question buttons
        html.Div([
            html.Button(q, id={"type": "quick-q", "index": i},
                style={
                    "backgroundColor": CARD2, "color": CYAN, "border": f"1px solid {CYAN}44",
                    "borderRadius": "16px", "padding": "6px 14px", "cursor": "pointer",
                    "fontSize": "12px", "margin": "3px",
                })
            for i, q in enumerate([
                "Full summary", "Net profit", "Best sellers",
                "Shipping P/L", "Monthly breakdown", "Refunds",
                "Growth trend", "Best month", "Fees breakdown",
                "Inventory / COGS", "Unit economics",
                "Patterns", "Cash flow", "Valuation", "Debt",
            ])
        ], style={"marginBottom": "14px", "display": "flex", "flexWrap": "wrap", "gap": "4px"}),

        # Chat history
        html.Div(id="chat-history", children=[
            html.Div([
                html.Div("Hi! I'm your Etsy data assistant. Ask me anything about your store's "
                         "financial data -- revenue, products, shipping, fees, trends, and more. "
                         "Type **help** to see example questions!",
                    style={
                        "backgroundColor": f"{CYAN}15", "border": f"1px solid {CYAN}33",
                        "borderRadius": "12px", "padding": "12px 16px", "maxWidth": "85%",
                        "color": WHITE, "fontSize": "13px", "whiteSpace": "pre-wrap",
                    }),
            ], style={"display": "flex", "justifyContent": "flex-start", "marginBottom": "10px"}),
        ], style={
            "backgroundColor": CARD, "borderRadius": "10px", "padding": "16px",
            "minHeight": "400px", "maxHeight": "600px", "overflowY": "auto",
            "marginBottom": "10px",
        }),

        # Input area
        html.Div([
            dcc.Input(
                id="chat-input", type="text",
                placeholder="Ask a question about your data...",
                debounce=False, n_submit=0,
                style={
                    "flex": "1", "padding": "12px 16px", "backgroundColor": CARD2,
                    "color": WHITE, "border": f"1px solid {CYAN}44", "borderRadius": "8px",
                    "fontSize": "14px", "outline": "none",
                },
            ),
            html.Button("Send", id="chat-send", n_clicks=0,
                style={
                    "padding": "12px 28px", "backgroundColor": CYAN, "color": BG,
                    "border": "none", "borderRadius": "8px", "cursor": "pointer",
                    "fontSize": "14px", "fontWeight": "bold",
                }),
        ], style={"display": "flex", "gap": "8px"}),

        # Hidden store for conversation history
        dcc.Store(id="chat-store", data=[]),
    ], style={"padding": TAB_PADDING})


def _build_shipping_compare():
    """Build Shipping: Buyer Paid vs Your Cost chart fresh from current data."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Buyers Paid", x=["Shipping"], y=[buyer_paid_shipping],
        marker_color=GREEN, text=[f"${buyer_paid_shipping:,.2f}"], textposition="outside", width=0.3, offset=-0.15,
    ))
    fig.add_trace(go.Bar(
        name="Your Label Cost", x=["Shipping"], y=[total_shipping_cost],
        marker_color=RED, text=[f"${total_shipping_cost:,.2f}"], textposition="outside", width=0.3, offset=0.15,
    ))
    fig.add_annotation(
        x="Shipping", y=max(buyer_paid_shipping, total_shipping_cost) + 200,
        text=f"{'Loss' if shipping_profit < 0 else 'Profit'}: ${abs(shipping_profit):,.2f}",
        showarrow=False, font=dict(size=18, color=RED if shipping_profit < 0 else GREEN, family="Arial Black"),
    )
    make_chart(fig, 340, False)
    fig.update_layout(title="Shipping: Buyer Paid vs Your Cost", showlegend=True, yaxis_title="Amount ($)")
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
        if val > 0:
            names.append(nm)
            vals.append(val)
            colors.append(clr)
    if ship_credits != 0:
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


def build_tab3_financials():
    """Tab 3 - Financials: Full P&L + Cash Flow + Shipping + Monthly + Fees + Ledger"""
    net_fees_after_credits = total_fees_gross - abs(total_credits)

    # Shipping data needed
    usps_labels = ship_df[ship_df["Title"] == "USPS shipping label"]["Net_Clean"].abs()
    usps_min = usps_labels.min() if len(usps_labels) else 0
    usps_max = usps_labels.max() if len(usps_labels) else 0
    total_label_count = usps_outbound_count + usps_return_count + asendia_count

    # Reconciliation waterfall
    _wf_labels = [
        "Gross Sales",
        "Fees", "Ship Labels", "Ads", "Refunds", "Taxes",
    ]
    _wf_values = [
        gross_sales,
        -total_fees, -total_shipping_cost, -total_marketing, -total_refunds, -total_taxes,
    ]
    _wf_measures = [
        "absolute",
        "relative", "relative", "relative", "relative", "relative",
    ]
    if total_buyer_fees > 0:
        _wf_labels.append("CO Buyer Fee")
        _wf_values.append(-total_buyer_fees)
        _wf_measures.append("relative")
    _wf_labels += ["AFTER ETSY FEES"]
    _wf_values += [0]
    _wf_measures += ["total"]
    # Bank expenses — only include non-zero categories
    for _lbl, _val in [
        ("Amazon Inv.", bank_by_cat.get("Amazon Inventory", 0)),
        ("AliExpress", bank_by_cat.get("AliExpress Supplies", 0)),
        ("Craft", bank_by_cat.get("Craft Supplies", 0)),
        ("Etsy Fees", bank_by_cat.get("Etsy Fees", 0)),
        ("Subscriptions", bank_by_cat.get("Subscriptions", 0)),
        ("Ship Supplies", bank_by_cat.get("Shipping", 0)),
        ("Best Buy CC", bank_by_cat.get("Business Credit Card", 0)),
    ]:
        if _val > 0:
            _wf_labels.append(_lbl)
            _wf_values.append(-_val)
            _wf_measures.append("relative")
    if bank_owner_draw_total > 0:
        _wf_labels.append("Owner Draws")
        _wf_values.append(-bank_owner_draw_total)
        _wf_measures.append("relative")
    # Reconciliation items — only include non-zero
    for _lbl, _val in [
        ("Prior Bank Inv.", old_bank_receipted),
        ("Unmatched Bank", bank_unaccounted),
        ("Untracked Etsy", etsy_csv_gap),
    ]:
        if abs(_val) > 0.01:
            _wf_labels.append(_lbl)
            _wf_values.append(-_val)
            _wf_measures.append("relative")
    _wf_labels.append("CASH ON HAND")
    _wf_values.append(0)
    _wf_measures.append("total")
    recon_fig = go.Figure(go.Waterfall(
        orientation="v", measure=_wf_measures, x=_wf_labels, y=_wf_values,
        connector={"line": {"color": "#555"}},
        decreasing={"marker": {"color": RED}},
        increasing={"marker": {"color": GREEN}},
        totals={"marker": {"color": CYAN}},
        textposition="outside",
        text=[f"${abs(v):,.0f}" if v != 0 else "" for v in _wf_values],
    ))
    make_chart(recon_fig, 420)
    recon_fig.update_layout(
        title="HOW EVERY DOLLAR BALANCES",
        yaxis_title="Amount ($)",
        xaxis_tickangle=-35,
        xaxis_tickfont=dict(size=10),
    )

    return html.Div([
        # ══════════════════════════════════════════════════════════════
        # A: FINANCIAL SUMMARY
        # ══════════════════════════════════════════════════════════════
        html.H3("A: FINANCIAL SUMMARY", style={"color": CYAN, "margin": "0 0 10px 0",
                 "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}),
        html.P("Complete breakdown of every dollar in and out.",
               style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}),

        # KPI pills
        html.Div([
            _build_kpi_pill("\U0001f4b3", "DEBT", f"${bb_cc_balance:,.2f}", RED,
                            f"Best Buy CC (${bb_cc_available:,.0f} avail)",
                            f"Best Buy Citi CC for equipment. Charged: ${bb_cc_total_charged:,.2f}. Paid: ${bb_cc_total_paid:,.2f}. Balance: ${bb_cc_balance:,.2f}. Limit: ${bb_cc_limit:,.2f}. Asset value: ${bb_cc_asset_value:,.2f}."),
            _build_kpi_pill("\U0001f4e5", "AFTER ETSY FEES", f"${etsy_net:,.2f}", ORANGE,
                            "What Etsy deposits to your bank",
                            f"Gross (${gross_sales:,.2f}) minus fees (${total_fees:,.2f}), shipping (${total_shipping_cost:,.2f}), ads (${total_marketing:,.2f}), refunds (${total_refunds:,.2f}), taxes (${total_taxes:,.2f}), buyer fees (${total_buyer_fees:,.2f})."),
            _build_kpi_pill("\U0001f4c9", "TOTAL FEES", f"${net_fees_after_credits:,.2f}", RED,
                            f"{net_fees_after_credits / gross_sales * 100:.1f}% of sales" if gross_sales else "",
                            f"Listing: ${listing_fees:,.2f}. Transaction (product): ${transaction_fees_product:,.2f}. Transaction (shipping): ${transaction_fees_shipping:,.2f}. Processing: ${processing_fees:,.2f}. Credits: ${abs(total_credits):,.2f}. Net: ${net_fees_after_credits:,.2f}."),
            _build_kpi_pill("\u21a9\ufe0f", "REFUNDS", f"${total_refunds:,.2f}", PINK,
                            f"{len(refund_df)} orders ({len(refund_df) / order_count * 100:.1f}%)" if order_count else "",
                            f"{len(refund_df)} refunded of {order_count} total. Avg refund: ${total_refunds / max(len(refund_df), 1):,.2f}. Return labels: ${usps_return:,.2f} ({usps_return_count} labels)."),
            _build_kpi_pill("\U0001f4b0", "PROFIT", f"${profit:,.2f}", GREEN,
                            f"{profit_margin:.1f}% margin",
                            f"Revenue minus all costs. Cash ({money(bank_cash_on_hand)}) + draws ({money(bank_owner_draw_total)})."),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "14px", "flexWrap": "wrap"}),

        # Full P&L
        section("PROFIT & LOSS", [
            row_item("Gross Sales", gross_sales, color=GREEN),
            html.Div("ETSY DEDUCTIONS:", style={"color": RED, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "8px", "marginBottom": "4px"}),
            row_item("  Fees (listing + transaction + processing)", -total_fees_gross, indent=1, color=GRAY),
            row_item("  Fee Credits", abs(total_credits), indent=1, color=GREEN),
            row_item("  Shipping Labels", -total_shipping_cost, indent=1, color=GRAY),
            row_item("  Ads & Marketing", -total_marketing, indent=1, color=GRAY),
            row_item("  Refunds to Customers", -total_refunds, indent=1, color=GRAY),
            row_item("  Sales Tax Collected & Remitted", -total_taxes, indent=1, color=GRAY),
            row_item("  Buyer Fees", -total_buyer_fees, indent=1, color=GRAY) if total_buyer_fees > 0 else html.Div(),
            html.Div(style={"borderTop": f"2px solid {ORANGE}44", "margin": "6px 0"}),
            row_item("AFTER ETSY FEES (what Etsy pays you)", etsy_net, bold=True, color=ORANGE),
            html.Div("BANK EXPENSES (from Cap One statement):", style={"color": RED, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "10px", "marginBottom": "4px"}),
            row_item("  Amazon Inventory", -bank_by_cat.get("Amazon Inventory", 0), indent=1, color=GRAY),
            row_item("  AliExpress Supplies", -bank_by_cat.get("AliExpress Supplies", 0), indent=1, color=GRAY),
            row_item("  Craft Supplies", -bank_by_cat.get("Craft Supplies", 0), indent=1, color=GRAY),
            row_item("  Etsy Bank Fees", -bank_by_cat.get("Etsy Fees", 0), indent=1, color=GRAY),
            row_item("  Subscriptions", -bank_by_cat.get("Subscriptions", 0), indent=1, color=GRAY),
            row_item("  Shipping (UPS/USPS)", -bank_by_cat.get("Shipping", 0), indent=1, color=GRAY),
            row_item("  Best Buy CC Payment (toward equipment)", -bank_by_cat.get("Business Credit Card", 0), indent=1, color=GRAY),
            row_item("    CC Balance Still Owed (liability)", -bb_cc_balance, indent=2, color=BLUE),
            row_item("    Equipment Purchased (asset)", bb_cc_asset_value, indent=2, color=TEAL),
            row_item("  Owner Draws", -bank_owner_draw_total, indent=1, color=ORANGE),
            row_item("  Old bank inventory (receipted)", -old_bank_receipted, indent=1, color=ORANGE),
            row_item("  Untracked (prior bank + Etsy gap)", -(bank_unaccounted + etsy_csv_gap), indent=1, color=RED),
            html.Div(style={"borderTop": f"3px solid {GREEN}", "marginTop": "10px"}),
            html.Div([
                html.Span("PROFIT", style={"color": GREEN, "fontWeight": "bold", "fontSize": "22px"}),
                html.Span(f"${profit:,.2f}", style={"color": GREEN, "fontWeight": "bold", "fontSize": "22px", "fontFamily": "monospace"}),
            ], style={"display": "flex", "justifyContent": "space-between", "padding": "12px 0"}),
            html.Div(f"= Cash On Hand ${bank_cash_on_hand:,.2f} + Owner Draws ${bank_owner_draw_total:,.2f}",
                     style={"color": GRAY, "fontSize": "12px", "textAlign": "center"}),
            html.Div([
                html.Div([
                    html.Span("Profit/Day", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"${profit / days_active:,.2f}",
                             style={"color": GREEN, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Span("Profit/Order", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"${profit / order_count if order_count else 0:,.2f}",
                             style={"color": GREEN, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Span("Avg Order Value", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"${avg_order:,.2f}",
                             style={"color": TEAL, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Span("Revenue/Day", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"${gross_sales / days_active:,.2f}",
                             style={"color": TEAL, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
            ], style={"display": "flex", "gap": "8px", "padding": "10px",
                       "backgroundColor": "#ffffff06", "borderRadius": "8px"}),
        ], GREEN),

        # Expense donut + Reconciliation waterfall side by side
        html.Div([
            html.Div([dcc.Graph(figure=expense_pie, config={"displayModeBar": False})], style={"flex": "1"}),
            html.Div([dcc.Graph(figure=recon_fig, config={"displayModeBar": False})], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "10px"}),

        # ══════════════════════════════════════════════════════════════
        # B: CASH FLOW
        # ══════════════════════════════════════════════════════════════
        html.H3("B: CASH FLOW", style={"color": CYAN, "margin": "20px 0 10px 0",
                 "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}),
        html.P("Where your cash is right now and how it moved through the business.",
               style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}),

        # Cash KPIs
        html.Div([
            html.Div([
                html.Div(f"${profit:,.2f}", style={"color": GREEN, "fontSize": "22px", "fontWeight": "bold", "fontFamily": "monospace"}),
                html.Div("PROFIT", style={"color": GRAY, "fontSize": "10px"}),
            ], style={"textAlign": "center", "flex": "1"}),
            html.Div([
                html.Div(f"${bank_cash_on_hand:,.2f}", style={"color": CYAN, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                html.Div(f"Cash (Bank ${bank_net_cash:,.0f} + Etsy ${etsy_balance:,.0f})", style={"color": GRAY, "fontSize": "10px"}),
            ], style={"textAlign": "center", "flex": "1"}),
            html.Div([
                html.Div(f"${bank_owner_draw_total:,.2f}", style={"color": ORANGE, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                html.Div("Owner Draws", style={"color": GRAY, "fontSize": "10px"}),
            ], style={"textAlign": "center", "flex": "1"}),
            html.Div([
                html.Div(f"${bank_all_expenses:,.2f}", style={"color": RED, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                html.Div("Expenses", style={"color": GRAY, "fontSize": "10px"}),
            ], style={"textAlign": "center", "flex": "1"}),
        ], style={"display": "flex", "gap": "6px", "marginBottom": "10px",
                   "padding": "10px", "backgroundColor": "#ffffff06", "borderRadius": "8px"}),

        # Mini Balance Sheet
        section("BALANCE SHEET (snapshot)", [
            html.Div("ASSETS", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px", "marginBottom": "4px"}),
            row_item("  Cash On Hand (Bank + Etsy)", bank_cash_on_hand, indent=1, color=GREEN),
            row_item("  Equipment (3D Printers)", bb_cc_asset_value, indent=1, color=TEAL),
            row_item("TOTAL ASSETS", bank_cash_on_hand + bb_cc_asset_value, bold=True, color=GREEN),
            html.Div(style={"borderTop": f"1px solid {DARKGRAY}", "margin": "8px 0"}),
            html.Div("LIABILITIES", style={"color": RED, "fontWeight": "bold", "fontSize": "12px", "marginBottom": "4px"}),
            row_item("  Best Buy CC Balance", -bb_cc_balance, indent=1, color=RED),
            row_item("TOTAL LIABILITIES", -bb_cc_balance, bold=True, color=RED),
            html.Div(style={"borderTop": f"3px solid {CYAN}", "marginTop": "10px"}),
            html.Div([
                html.Span("NET WORTH", style={"color": CYAN, "fontWeight": "bold", "fontSize": "20px"}),
                html.Span(f"${bank_cash_on_hand + bb_cc_asset_value - bb_cc_balance:,.2f}",
                           style={"color": CYAN, "fontWeight": "bold", "fontSize": "20px", "fontFamily": "monospace"}),
            ], style={"display": "flex", "justifyContent": "space-between", "padding": "10px 0"}),
            html.Div(f"= Cash ${bank_cash_on_hand:,.2f} + Equipment ${bb_cc_asset_value:,.2f} - CC Debt ${bb_cc_balance:,.2f}",
                     style={"color": GRAY, "fontSize": "12px", "textAlign": "center"}),
        ], CYAN),

        # Sankey diagram
        dcc.Graph(figure=sankey_fig, config={"displayModeBar": False}),

        # Draw settlement banner
        html.Div([
            html.Div([
                html.Span(f"TJ: ${tulsa_draw_total:,.2f}", style={"color": "#ffb74d", "fontWeight": "bold", "fontSize": "14px"}),
                html.Span("  |  ", style={"color": DARKGRAY}),
                html.Span(f"Braden: ${texas_draw_total:,.2f}", style={"color": "#ff9800", "fontWeight": "bold", "fontSize": "14px"}),
                html.Span("  |  ", style={"color": DARKGRAY}),
                html.Span(
                    f"Company owes {draw_owed_to} ${draw_diff:,.2f}" if draw_diff >= 0.01
                    else "Even!",
                    style={"color": CYAN, "fontWeight": "bold", "fontSize": "14px"}
                ),
            ], style={"textAlign": "center"}),
        ], style={"padding": "10px", "marginBottom": "10px", "backgroundColor": "#ffffff06",
                   "borderRadius": "8px", "border": f"1px solid {CYAN}33"}),

        # Detail cards: Draws + Cash
        html.Div([
            cat_card(f"TJ DRAWS ({len(tulsa_draws)} txns)", "#ffb74d",
                [txn_row(t, ORANGE, get_draw_reason(t["desc"]))
                 for t in tulsa_draws] + (
                [html.Div("Personal / Gift Items", style={"color": PINK, "fontWeight": "bold",
                          "fontSize": "12px", "padding": "6px 0 2px 0", "borderTop": f"1px solid {PINK}44"})] +
                [html.Div([
                    html.Span(f"${r['total']:,.2f}", style={"color": PINK, "fontFamily": "monospace",
                              "fontSize": "11px", "fontWeight": "bold", "width": "60px", "display": "inline-block"}),
                    html.Span(r["name"][:50], style={"color": WHITE, "fontSize": "11px"}),
                ], style={"padding": "2px 0", "borderBottom": "1px solid #ffffff08"})
                 for _, r in personal_inv_items.iterrows()] if len(personal_inv_items) > 0 else []),
                tulsa_draw_total),
            cat_card(f"BRADEN DRAWS ({len(texas_draws)} txns)", "#ff9800",
                [txn_row(t, ORANGE, get_draw_reason(t["desc"]))
                 for t in texas_draws],
                texas_draw_total),
            cat_card("CASH ON HAND", GREEN, [
                html.Div([
                    html.Span("Capital One Bank", style={"color": WHITE, "fontSize": "12px", "width": "180px", "display": "inline-block"}),
                    html.Span(f"${bank_net_cash:,.2f}", style={"color": GREEN, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div([
                    html.Span("Etsy Account", style={"color": WHITE, "fontSize": "12px", "width": "180px", "display": "inline-block"}),
                    html.Span(f"${etsy_balance:,.2f}", style={"color": TEAL, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
            ], bank_cash_on_hand, f"Bank ~${bank_net_cash:,.0f} / Etsy ~${etsy_balance:,.0f}"),
            cat_card("BEST BUY CITI CC", BLUE, [
                html.Div([
                    html.Span("Credit Limit", style={"color": WHITE, "fontSize": "12px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bb_cc_limit:,.2f}", style={"color": BLUE, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div([
                    html.Span("Total Charged", style={"color": WHITE, "fontSize": "12px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bb_cc_total_charged:,.2f}", style={"color": RED, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div([
                    html.Span("Total Paid", style={"color": WHITE, "fontSize": "12px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bb_cc_total_paid:,.2f}", style={"color": GREEN, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div([
                    html.Span("Balance Owed", style={"color": WHITE, "fontSize": "12px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bb_cc_balance:,.2f}", style={"color": RED, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div([
                    html.Span("Available Credit", style={"color": WHITE, "fontSize": "12px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bb_cc_available:,.2f}", style={"color": TEAL, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div("Purchases:", style={"color": GRAY, "fontSize": "11px", "fontWeight": "bold",
                          "padding": "6px 0 2px 0", "borderTop": f"1px solid {BLUE}44"}),
            ] + [html.Div([
                    html.Span(p["date"], style={"color": GRAY, "fontSize": "11px", "width": "65px", "display": "inline-block"}),
                    html.Span(f"${p['amount']:,.2f}", style={"color": RED, "fontFamily": "monospace", "fontSize": "11px", "width": "70px", "display": "inline-block"}),
                    html.Span(p["desc"][:40], style={"color": WHITE, "fontSize": "11px"}),
                ], style={"padding": "2px 0", "borderBottom": "1px solid #ffffff08"})
                for p in bb_cc_purchases],
                bb_cc_balance, f"Limit ${bb_cc_limit:,.0f} | Paid ${bb_cc_total_paid:,.0f} | Avail ${bb_cc_available:,.0f}"),
        ], style={"display": "flex", "gap": "8px", "flexWrap": "wrap", "marginBottom": "8px"}),

        # Expenses + Receipts cards
        html.Div([
            cat_card(f"EXPENSES ({len([t for t in bank_debits if t['category'] in ['Amazon Inventory'] + _biz_expense_cats])} txns)", RED, [
                *[html.Div([
                    html.Span(c, style={"color": WHITE, "fontSize": "11px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bank_by_cat.get(c, 0):,.2f}", style={"color": RED, "fontFamily": "monospace", "fontSize": "11px", "width": "65px", "display": "inline-block"}),
                    html.Span({"Amazon Inventory": f"{len(_bank_amazon_txns)} charges (debit card)",
                               "Shipping": "UPS/USPS/Walmart", "Craft Supplies": "Hobby Lobby/Westlake",
                               "Etsy Fees": "ETSY COM US", "Subscriptions": "Thangs 3D x2",
                               "AliExpress Supplies": "PayPal/AliExpress LEDs",
                               "Business Credit Card": "Best Buy CC payment"}.get(c, ""),
                              style={"color": GRAY, "fontSize": "10px"}),
                ], style={"padding": "2px 0", "borderBottom": "1px solid #ffffff08"})
                  for c in ["Amazon Inventory"] + _biz_expense_cats if bank_by_cat.get(c, 0) > 0],
            ], bank_all_expenses),
            # Split missing receipts: purchase receipts vs other categories
            *[cat_card(
                f"MISSING {cat.upper()} RECEIPTS ({len(items)})", "#b71c1c",
                [html.Div([
                    html.Span(f"${t['amount']:,.2f}", style={"color": RED, "fontFamily": "monospace", "fontSize": "10px", "width": "55px", "display": "inline-block"}),
                    html.Span(t.get("date", ""), style={"color": GRAY, "fontSize": "9px", "width": "75px", "display": "inline-block"}),
                    html.Span(t["desc"][:30], style={"color": WHITE, "fontSize": "10px"}),
                ], style={"padding": "1px 0"}) for t in items],
                sum(t["amount"] for t in items),
            ) for cat, items in [
                (cat, [t for t in _bank_no_receipt if t["category"] == cat])
                for cat in dict.fromkeys(t["category"] for t in _bank_no_receipt)
            ] if items],
        ], style={"display": "flex", "gap": "8px", "flexWrap": "wrap", "marginBottom": "10px"}),

        # ══════════════════════════════════════════════════════════════
        # C: SHIPPING
        # ══════════════════════════════════════════════════════════════
        html.H3("C: SHIPPING", style={"color": CYAN, "margin": "20px 0 10px 0",
                 "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}),
        html.P("Are you making or losing money on shipping?",
               style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}),

        # Shipping KPIs
        html.Div([
            kpi_card("NET SHIPPING P&L", money(shipping_profit),
                GREEN if shipping_profit >= 0 else RED,
                "Profitable" if shipping_profit >= 0 else "Losing money",
                f"Buyer-paid shipping revenue (${buyer_paid_shipping:,.2f}) minus total label costs (${total_shipping_cost:,.2f}). {'You are making money on shipping.' if shipping_profit >= 0 else 'You are LOSING money on shipping, mostly from free-shipping orders where you absorb the label cost.'} Margin: {shipping_margin:.1f}%."),
            kpi_card("TOTAL LABEL COST", money(-total_shipping_cost), RED,
                f"{total_label_count} labels purchased",
                f"USPS outbound: {usps_outbound_count} labels (${usps_outbound:,.2f}). USPS return: {usps_return_count} labels (${usps_return:,.2f}). Asendia intl: {asendia_count} labels (${asendia_labels:,.2f}). Adjustments: ${ship_adjustments:,.2f}. Credits back: ${abs(ship_credits):,.2f}."),
            kpi_card("BUYER PAID", money(buyer_paid_shipping), GREEN,
                f"{paid_ship_count} paid-shipping orders",
                f"Estimated from Etsy transaction fees on shipping (6.5% rate). {paid_ship_count} orders where the buyer paid for shipping. Estimated profit on these orders: ${paid_shipping_profit:,.2f}."),
            kpi_card("FREE ORDERS", str(free_ship_count), ORANGE,
                f"~{money(-est_label_cost_free_orders)} est cost",
                f"{free_ship_count} orders shipped free (you absorbed the label cost). At avg ${avg_outbound_label:.2f}/label, that's ~${est_label_cost_free_orders:,.2f} in shipping you paid for. Consider raising prices on free-shipping listings by ${avg_outbound_label:.0f}-${avg_outbound_label + 3:.0f} to offset."),
            kpi_card("RETURN LABELS", str(usps_return_count), PINK,
                money(-usps_return) + " in return label cost",
                f"{usps_return_count} return shipping labels purchased for refunded orders. Total return label cost: ${usps_return:,.2f}. This is on top of the original outbound label cost and the refund amount -- returns are triple losses."),
            kpi_card("AVG LABEL", f"${avg_outbound_label:.2f}", BLUE,
                f"USPS ${usps_min:.2f}-${usps_max:.2f}" if usps_outbound_count else "No USPS labels",
                f"Average cost of a USPS outbound label. Range: ${usps_min:.2f} (lightest) to ${usps_max:.2f} (heaviest). {usps_outbound_count} labels total. Heavier/larger items cost more to ship."),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "14px", "flexWrap": "wrap"}),

        # Shipping charts — built inline so they always reflect current data
        html.Div([
            html.Div([dcc.Graph(figure=_build_shipping_compare(), config={"displayModeBar": False})], style={"flex": "1"}),
            html.Div([dcc.Graph(figure=_build_ship_type(), config={"displayModeBar": False})], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "10px"}),

        # Shipping P&L section
        section("SHIPPING P&L", [
            row_item(f"Buyers Paid for Shipping ({paid_ship_count} orders)", buyer_paid_shipping, color=GREEN),
            html.Div(style={"borderTop": f"1px solid {DARKGRAY}", "margin": "8px 0"}),
            row_item(f"USPS Outbound Labels ({usps_outbound_count})", -usps_outbound, indent=1),
            row_item(f"USPS Return Labels ({usps_return_count})", -usps_return, indent=1),
            row_item(f"Asendia / International ({asendia_count})", -asendia_labels, indent=1),
            row_item(f"Label Adjustments ({ship_adjust_count})", -ship_adjustments, indent=1),
            row_item(f"Insurance ({ship_insurance_count})", -ship_insurance, indent=1) if ship_insurance > 0 else html.Div(),
            row_item(f"Label Credits ({ship_credit_count})", ship_credits, indent=1, color=GREEN),
            row_item("TOTAL SHIPPING COST", -total_shipping_cost, bold=True),
            html.Div(style={"borderTop": f"3px solid {ORANGE}", "marginTop": "8px"}),
            html.Div([
                html.Span("NET SHIPPING P&L", style={"color": GREEN if shipping_profit >= 0 else RED, "fontWeight": "bold", "fontSize": "20px"}),
                html.Span(money(shipping_profit), style={"color": GREEN if shipping_profit >= 0 else RED, "fontWeight": "bold", "fontSize": "20px", "fontFamily": "monospace"}),
            ], style={"display": "flex", "justifyContent": "space-between", "padding": "10px 0"}),
        ], ORANGE),

        # Paid vs Free breakdown
        section("PAID vs FREE BREAKDOWN", [
            html.P("PAID SHIPPING", style={"color": TEAL, "fontWeight": "bold", "fontSize": "13px", "margin": "0 0 4px 0"}),
            row_item(f"Buyers Paid ({paid_ship_count} orders)", buyer_paid_shipping, color=GREEN, indent=1),
            row_item(f"Est. Label Cost ({paid_ship_count} x ${avg_outbound_label:.2f} avg)", -est_label_cost_paid_orders, indent=1),
            row_item("Profit on Paid Shipping", paid_shipping_profit, bold=True,
                color=GREEN if paid_shipping_profit >= 0 else RED),
            html.Div(style={"borderTop": f"1px solid {DARKGRAY}", "margin": "8px 0"}),
            html.P("FREE SHIPPING", style={"color": ORANGE, "fontWeight": "bold", "fontSize": "13px", "margin": "0 0 4px 0"}),
            row_item(f"Free Shipping Orders ({free_ship_count})", 0, color=GRAY, indent=1),
            row_item(f"Est. Label Cost ({free_ship_count} x ${avg_outbound_label:.2f} avg)", -est_label_cost_free_orders, indent=1),
            row_item("Loss on Free Shipping", -est_label_cost_free_orders, bold=True, color=RED),
        ], TEAL),

        # Returns section
        section("RETURNS & REFUNDS", [
            row_item(f"Return Labels Purchased ({usps_return_count})", -usps_return, indent=1),
        ] + [
            html.Div([
                html.Span(m["date"], style={"color": GRAY, "width": "140px", "display": "inline-block", "fontSize": "12px"}),
                html.Span(f"{m['product'][:40]}", style={"color": WHITE, "flex": "1", "fontSize": "12px"}),
                html.Span(f"Label: ${m['cost']:.2f}", style={"color": RED, "fontFamily": "monospace", "width": "100px", "textAlign": "right", "fontSize": "12px"}),
                html.Span(f"Refund: ${m['refund_amt']:.2f}", style={"color": ORANGE, "fontFamily": "monospace", "width": "110px", "textAlign": "right", "fontSize": "12px"}),
            ], style={"display": "flex", "padding": "3px 0", "borderBottom": "1px solid #ffffff08", "gap": "6px"})
            for m in return_label_matches
        ], PINK),

        # ══════════════════════════════════════════════════════════════
        # D: MONTHLY & PRODUCTS
        # ══════════════════════════════════════════════════════════════
        html.H3("D: MONTHLY & PRODUCTS", style={"color": CYAN, "margin": "20px 0 10px 0",
                 "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}),
        html.P("Month-by-month performance and top products.",
               style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}),

        # Monthly Breakdown table
        section("MONTHLY BREAKDOWN", [
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("Month", style={"textAlign": "left", "padding": "6px 8px"}),
                        html.Th("Sales", style={"textAlign": "right", "padding": "6px 8px"}),
                        html.Th("Fees", style={"textAlign": "right", "padding": "6px 8px"}),
                        html.Th("Shipping", style={"textAlign": "right", "padding": "6px 8px"}),
                        html.Th("Marketing", style={"textAlign": "right", "padding": "6px 8px"}),
                        html.Th("Refunds", style={"textAlign": "right", "padding": "6px 8px"}),
                        html.Th("Net", style={"textAlign": "right", "padding": "6px 8px", "fontWeight": "bold"}),
                        html.Th("Margin", style={"textAlign": "right", "padding": "6px 8px"}),
                    ], style={"borderBottom": f"2px solid {BLUE}"})),
                    html.Tbody(
                        [html.Tr([
                            html.Td(m, style={"color": WHITE, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_sales.get(m, 0):,.2f}", style={"textAlign": "right", "color": GREEN, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_fees.get(m, 0):,.2f}", style={"textAlign": "right", "color": RED, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_shipping.get(m, 0):,.2f}", style={"textAlign": "right", "color": BLUE, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_marketing.get(m, 0):,.2f}", style={"textAlign": "right", "color": PURPLE, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_refunds.get(m, 0):,.2f}", style={"textAlign": "right", "color": ORANGE, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_net_revenue.get(m, 0):,.2f}", style={"textAlign": "right", "color": ORANGE, "fontWeight": "bold", "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(
                                f"{(monthly_net_revenue.get(m, 0) / monthly_sales.get(m, 1) * 100):.1f}%"
                                if monthly_sales.get(m, 0) > 0 else "--",
                                style={"textAlign": "right", "color": GRAY, "padding": "4px 8px", "fontSize": "13px"}),
                        ], style={"borderBottom": "1px solid #ffffff10"}) for m in months_sorted]
                        + [html.Tr([
                            html.Td("TOTAL", style={"color": ORANGE, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_sales.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": GREEN, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_fees.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": RED, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_shipping.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": BLUE, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_marketing.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": PURPLE, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_refunds.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": ORANGE, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_net_revenue.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": ORANGE, "fontWeight": "bold", "fontSize": "15px", "padding": "6px 8px"}),
                            html.Td(
                                f"{(sum(monthly_net_revenue.get(m, 0) for m in months_sorted) / gross_sales * 100):.1f}%" if gross_sales else "--",
                                style={"textAlign": "right", "color": GRAY, "fontWeight": "bold", "padding": "6px 8px"}),
                        ], style={"borderTop": f"3px solid {ORANGE}"})]
                    ),
                ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
            ]),
        ], BLUE),

        # Top Products chart
        dcc.Graph(figure=product_fig, config={"displayModeBar": False}),

        # ══════════════════════════════════════════════════════════════
        # E: FEE DETAIL & LEDGER
        # ══════════════════════════════════════════════════════════════
        html.H3("E: FEE DETAIL & LEDGER", style={"color": CYAN, "margin": "20px 0 10px 0",
                 "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}),
        html.P("Detailed fee breakdown and full transaction ledger.",
               style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}),

        # Fee breakdown
        section("FEE & MARKETING DETAIL", [
            html.Div("FEES CHARGED", style={"color": RED, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "4px"}),
            row_item("  Listing Fees", -listing_fees, indent=1, color=GRAY),
            row_item("  Transaction Fees (product)", -transaction_fees_product, indent=1, color=GRAY),
            row_item("  Transaction Fees (shipping)", -transaction_fees_shipping, indent=1, color=GRAY),
            row_item("  Processing Fees", -processing_fees, indent=1, color=GRAY),
            row_item("Total Fees (gross)", -total_fees_gross, bold=True),
            html.Div(style={"height": "8px"}),
            html.Div("CREDITS RECEIVED", style={"color": GREEN, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "4px"}),
            row_item("  Transaction Credits", credit_transaction, indent=1, color=GREEN),
            row_item("  Listing Credits", credit_listing, indent=1, color=GREEN),
            row_item("  Processing Credits", credit_processing, indent=1, color=GREEN),
            row_item("  Share & Save", share_save, indent=1, color=GREEN),
            row_item("Total Credits", total_credits, bold=True, color=GREEN),
            html.Div(style={"height": "2px", "borderTop": f"1px solid {ORANGE}44", "margin": "8px 0"}),
            row_item("Net Fees (after credits)", -net_fees_after_credits, bold=True, color=ORANGE),
            html.Div(style={"height": "12px"}),
            html.Div("MARKETING", style={"color": PURPLE, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "4px"}),
            row_item("  Etsy Ads", -etsy_ads, indent=1, color=GRAY),
            row_item("  Offsite Ads", -offsite_ads_fees, indent=1, color=GRAY),
            *([] if offsite_ads_credits == 0 else [row_item("  Offsite Credits", offsite_ads_credits, indent=1, color=GREEN)]),
            row_item("Total Marketing", -total_marketing, bold=True),
        ], RED),

        # Refunds list
        section("REFUNDS", [
            html.Div([
                html.Div([
                    html.Span("Total Refunded", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"${total_refunds:,.2f}",
                             style={"color": RED, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Span("Count", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"{len(refund_df)}",
                             style={"color": ORANGE, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Span("Avg Refund", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"${total_refunds / len(refund_df) if len(refund_df) else 0:,.2f}",
                             style={"color": ORANGE, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
            ], style={"display": "flex", "gap": "8px", "padding": "10px",
                       "backgroundColor": "#ffffff06", "borderRadius": "8px", "marginBottom": "10px"}),
        ] + [
            html.Div([
                html.Span(f"{r['Date']}", style={"color": GRAY, "width": "120px", "display": "inline-block", "fontSize": "12px"}),
                html.Span(f"{r['Title'][:60]}", style={"color": WHITE, "flex": "1", "fontSize": "12px"}),
                html.Span(f"${abs(r['Net_Clean']):,.2f}", style={"color": RED, "fontFamily": "monospace", "width": "80px", "textAlign": "right", "fontSize": "12px"}),
            ], style={"display": "flex", "padding": "3px 0", "borderBottom": "1px solid #ffffff08"})
            for _, r in refund_df.sort_values("Date_Parsed", ascending=False).iterrows()
        ], ORANGE),

        # Missing statements
        section("MISSING STATEMENTS & RECEIPTS", [
            html.Div([
                html.Div("BEFORE CAPITAL ONE (Oct - early Dec 2025)", style={
                    "color": GREEN, "fontWeight": "bold", "fontSize": "14px", "marginBottom": "6px"}),
                html.P(f"Etsy deposited $941.99 to your old bank. "
                       f"${old_bank_receipted:,.2f} matched to inventory receipts (non-Discover cards).",
                       style={"color": GRAY, "fontSize": "12px", "margin": "0 0 8px 0"}),
                html.Div([
                    html.Span("STATUS: ", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px"}),
                    html.Span(f"Nearly fully accounted -- only ${bank_unaccounted:,.2f} unmatched",
                              style={"color": GRAY, "fontSize": "12px"}),
                ], style={"padding": "6px", "backgroundColor": "#4caf5008", "borderRadius": "6px"}),
            ], style={"padding": "12px", "backgroundColor": "#ffffff04", "borderRadius": "8px",
                       "borderLeft": f"4px solid {GREEN}", "marginBottom": "10px"}),
            html.Div([
                html.Div("ETSY CSV GAP (recent activity)", style={
                    "color": ORANGE, "fontWeight": "bold", "fontSize": "14px", "marginBottom": "6px"}),
                html.Div([
                    html.Span(f"${etsy_csv_gap:,.2f} ", style={"color": ORANGE, "fontWeight": "bold", "fontFamily": "monospace", "fontSize": "13px"}),
                    html.Span("in Etsy fees/activity since last CSV export (~39 sales not in CSVs)", style={"color": GRAY, "fontSize": "12px"}),
                ]),
            ], style={"padding": "12px", "backgroundColor": "#ffffff04", "borderRadius": "8px",
                       "borderLeft": f"4px solid {ORANGE}", "marginBottom": "10px"}),
        ], RED),

        # Monthly cash flow chart
        dcc.Graph(figure=bank_monthly_fig, config={"displayModeBar": False}),

        # Full running balance ledger
        section(f"FULL LEDGER ({len(BANK_TXNS)} Transactions)", [
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("Date", style={"textAlign": "left", "padding": "6px 8px"}),
                        html.Th("Description", style={"textAlign": "left", "padding": "6px 8px"}),
                        html.Th("Category", style={"textAlign": "left", "padding": "6px 8px"}),
                        html.Th("Deposit", style={"textAlign": "right", "padding": "6px 8px"}),
                        html.Th("Debit", style={"textAlign": "right", "padding": "6px 8px"}),
                        html.Th("Balance", style={"textAlign": "right", "padding": "6px 8px"}),
                    ], style={"borderBottom": f"2px solid {CYAN}"})),
                    html.Tbody([
                        html.Tr([
                            html.Td(t["date"], style={"color": GRAY, "padding": "4px 8px", "fontSize": "12px"}),
                            html.Td(t["desc"][:45], style={"color": WHITE, "padding": "4px 8px", "fontSize": "12px"}),
                            html.Td(t["category"], style={
                                "color": _bank_cat_color_map.get(t["category"], WHITE),
                                "padding": "4px 8px", "fontSize": "11px", "fontWeight": "600"}),
                            html.Td(
                                f"+${t['amount']:,.2f}" if t["type"] == "deposit" else "",
                                style={"textAlign": "right", "color": GREEN, "fontWeight": "bold",
                                       "padding": "4px 8px", "fontSize": "12px", "fontFamily": "monospace"}),
                            html.Td(
                                f"-${t['amount']:,.2f}" if t["type"] != "deposit" else "",
                                style={"textAlign": "right", "color": RED, "fontWeight": "bold",
                                       "padding": "4px 8px", "fontSize": "12px", "fontFamily": "monospace"}),
                            html.Td(
                                f"${t['_balance']:,.2f}",
                                style={"textAlign": "right",
                                       "color": GREEN if t["_balance"] >= 0 else RED,
                                       "fontWeight": "bold",
                                       "padding": "4px 8px", "fontSize": "12px", "fontFamily": "monospace"}),
                        ], style={"borderBottom": "1px solid #ffffff10",
                                   "backgroundColor": f"{GREEN}08" if t["type"] == "deposit" else f"{_bank_cat_color_map.get(t['category'], WHITE)}08"})
                        for t in bank_running
                    ] + [
                        html.Tr([
                            html.Td("TOTAL", colSpan="3", style={"color": CYAN, "fontWeight": "bold",
                                                                    "padding": "8px 8px", "fontSize": "13px"}),
                            html.Td(f"${bank_total_deposits:,.2f}", style={
                                "textAlign": "right", "color": GREEN, "fontWeight": "bold",
                                "padding": "8px 8px", "fontSize": "13px", "fontFamily": "monospace"}),
                            html.Td(f"${bank_total_debits:,.2f}", style={
                                "textAlign": "right", "color": RED, "fontWeight": "bold",
                                "padding": "8px 8px", "fontSize": "13px", "fontFamily": "monospace"}),
                            html.Td(f"${bank_net_cash:,.2f}", style={
                                "textAlign": "right", "color": CYAN, "fontWeight": "bold",
                                "padding": "8px 8px", "fontSize": "13px", "fontFamily": "monospace"}),
                        ], style={"borderTop": f"3px solid {CYAN}"}),
                    ]),
                ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
            ], style={"maxHeight": "700px", "overflowY": "auto"}),
        ], CYAN),

    ], style={"padding": TAB_PADDING})


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
    """Scan the appropriate directory and return list of {filename, size_kb, modified}."""
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
        for fn in sorted(os.listdir(target_dir)):
            if fn.lower().endswith(ext):
                fpath = os.path.join(target_dir, fn)
                stat = os.stat(fpath)
                files.append({
                    "filename": fn,
                    "size_kb": round(stat.st_size / 1024, 1),
                })
    return files


def _render_file_list(files, color):
    """Render a list of existing files as compact rows."""
    if not files:
        return html.Div("No files yet", style={"color": DARKGRAY, "fontSize": "12px", "fontStyle": "italic"})
    return html.Div([
        html.Div([
            html.Span(f["filename"], style={"color": WHITE, "fontSize": "12px", "flex": "1"}),
            html.Span(f'{f["size_kb"]} KB', style={"color": DARKGRAY, "fontSize": "11px",
                                                      "fontFamily": "monospace"}),
        ], style={"display": "flex", "justifyContent": "space-between",
                   "padding": "3px 0", "borderBottom": "1px solid #ffffff08"})
        for f in files
    ], style={"maxHeight": "120px", "overflowY": "auto"})


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


def build_tab7_data_hub():
    """Build the Data Hub tab — upload & auto-update everything."""
    etsy_files = _get_existing_files("etsy")
    receipt_files = _get_existing_files("receipt")
    bank_files = _get_existing_files("bank")

    return html.Div([
        # Title
        html.Div([
            html.H2("DATA HUB", style={"color": CYAN, "margin": "0", "fontSize": "22px",
                                         "letterSpacing": "2px"}),
            html.P("Upload files and auto-update all dashboard data. No restart needed.",
                   style={"color": GRAY, "margin": "4px 0 0 0", "fontSize": "13px"}),
        ], style={"marginBottom": "16px"}),

        # Summary KPI strip
        html.Div(id="datahub-summary-strip", children=[_build_datahub_summary()]),

        html.Hr(style={"border": "none", "borderTop": f"1px solid {DARKGRAY}33", "margin": "16px 0"}),

        # Data coverage panel — what's uploaded and what's missing
        _build_data_coverage(),

        # 3-column upload zones
        html.Div([
            _build_upload_zone("etsy", "\U0001f4ca", "Etsy Statements", TEAL, ".csv",
                               "Upload Etsy CSV statements. Rebuilds all sales, fees, and financial data."),
            _build_upload_zone("receipt", "\U0001f4e6", "Receipt PDFs", PURPLE, ".pdf",
                               "Upload Amazon/supplier invoice PDFs. Parses items and updates inventory."),
            _build_upload_zone("bank", "\U0001f3e6", "Bank Statements", CYAN, ".pdf,.csv",
                               "Upload Capital One bank PDFs or CSV transaction downloads. Deduplicates automatically."),
        ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginBottom": "20px"}),

        # Reconciliation panel
        html.Hr(style={"border": "none", "borderTop": f"1px solid {DARKGRAY}33", "margin": "0 0 16px 0"}),
        _build_reconciliation_panel(),

        # Pre-populate existing file lists (static on load)
        dcc.Store(id="datahub-init-trigger", data="init"),

        # ── EXPORT DATA ──────────────────────────────────────────────────
        html.Div([
            html.Div([
                html.Span("\U0001f4e5", style={"fontSize": "20px", "marginRight": "8px"}),
                html.Span("EXPORT DATA", style={"fontSize": "16px", "fontWeight": "bold", "color": GREEN,
                                                   "letterSpacing": "1.5px"}),
                html.Span("  — Download CSVs to verify in Excel or share with your accountant",
                           style={"color": GRAY, "fontSize": "12px", "marginLeft": "8px"}),
            ], style={"marginBottom": "14px"}),
            html.Div([
                html.Button(["\U0001f4ca  Etsy Transactions"], id="btn-download-etsy", n_clicks=0,
                            style={"backgroundColor": f"{TEAL}22", "color": TEAL,
                                   "border": f"1px solid {TEAL}55", "borderRadius": "8px",
                                   "padding": "10px 18px", "fontSize": "13px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
                html.Button(["\U0001f3e6  Bank Transactions"], id="btn-download-bank", n_clicks=0,
                            style={"backgroundColor": f"{CYAN}22", "color": CYAN,
                                   "border": f"1px solid {CYAN}55", "borderRadius": "8px",
                                   "padding": "10px 18px", "fontSize": "13px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
                html.Button(["\U0001f4e6  All Inventory Items"], id="btn-download-inventory", n_clicks=0,
                            style={"backgroundColor": f"{PURPLE}22", "color": PURPLE,
                                   "border": f"1px solid {PURPLE}55", "borderRadius": "8px",
                                   "padding": "10px 18px", "fontSize": "13px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
                html.Button(["\U0001f4ca  Stock Summary"], id="btn-download-stock", n_clicks=0,
                            style={"backgroundColor": f"{ORANGE}22", "color": ORANGE,
                                   "border": f"1px solid {ORANGE}55", "borderRadius": "8px",
                                   "padding": "10px 18px", "fontSize": "13px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
                html.Button(["\U0001f4b0  Profit & Loss"], id="btn-download-pl", n_clicks=0,
                            style={"backgroundColor": f"{GREEN}22", "color": GREEN,
                                   "border": f"1px solid {GREEN}55", "borderRadius": "8px",
                                   "padding": "10px 18px", "fontSize": "13px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
            ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap"}),
        ], style={
            "backgroundColor": CARD2, "borderRadius": "12px", "padding": "18px",
            "borderLeft": f"4px solid {GREEN}", "marginBottom": "20px",
            "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
        }),

        # Activity Log
        html.Div([
            html.Div([
                html.Span("\U0001f4dd", style={"marginRight": "8px"}),
                html.Span("Activity Log", style={"fontWeight": "bold", "color": WHITE}),
            ], style={"marginBottom": "8px"}),
            html.Div(id="datahub-activity-log", children=[
                html.Div("No uploads yet this session.", style={
                    "color": DARKGRAY, "fontSize": "12px", "fontStyle": "italic"}),
            ], style={"maxHeight": "200px", "overflowY": "auto"}),
        ], style={
            "backgroundColor": CARD2, "borderRadius": "12px", "padding": "16px",
            "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
        }),

        # Hidden initial file-list populator
        html.Div(id="datahub-etsy-files-init", children=[_render_file_list(etsy_files, TEAL)],
                  style={"display": "none"}),
        html.Div(id="datahub-receipt-files-init", children=[_render_file_list(receipt_files, PURPLE)],
                  style={"display": "none"}),
        html.Div(id="datahub-bank-files-init", children=[_render_file_list(bank_files, CYAN)],
                  style={"display": "none"}),
    ], style={"padding": TAB_PADDING})


# ── App Layout ───────────────────────────────────────────────────────────────

def _header_text():
    """Build the dynamic header subtitle string."""
    return (f"Oct 2025 -- Feb 2026  |  {order_count} orders  |  "
            f"Profit: ${profit:,.2f} ({profit_margin:.1f}%)  |  "
            f"Cash: ${bank_cash_on_hand:,.2f}")


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
        # Header
        html.Div([
            html.H1("TJs SOFTWARE PROJECT", style={"color": ORANGE, "margin": "0", "fontSize": "24px"}),
            html.Div(
                _header_text(),
                id="app-header-content",
                style={"color": GRAY, "margin": "2px 0 0 0", "fontSize": "13px"},
            ),
        ], style={"padding": "14px 20px", "backgroundColor": CARD2}),

        # Tabs — content rendered dynamically via callback so uploads refresh data
        dcc.Tabs(id="main-tabs", value="tab-overview", children=[
            dcc.Tab(label="Overview", value="tab-overview", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="Deep Dive", value="tab-deep-dive", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="Financials", value="tab-financials", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label=inv_label, value="tab-inventory", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="Tax Forms", value="tab-tax-forms", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="Business Valuation", value="tab-valuation", style=tab_style, selected_style=tab_selected_style),
            dcc.Tab(label="Data Hub", value="tab-data-hub", style=tab_style, selected_style=tab_selected_style),
        ], style={"backgroundColor": BG}),
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


# ── Dynamic Tab Rendering ────────────────────────────────────────────────────

@app.callback(
    Output("tab-content", "children"),
    Input("main-tabs", "value"),
)
def render_active_tab(tab):
    """Rebuild the active tab's content on every switch so uploads are reflected."""
    _rebuild_all_charts()
    if tab == "tab-overview":
        return build_tab1_overview()
    elif tab == "tab-deep-dive":
        return build_tab2_deep_dive()
    elif tab == "tab-financials":
        return build_tab3_financials()
    elif tab == "tab-inventory":
        return build_tab4_inventory()
    elif tab == "tab-tax-forms":
        return build_tab5_tax_forms()
    elif tab == "tab-valuation":
        return build_tab6_valuation()
    elif tab == "tab-data-hub":
        return build_tab7_data_hub()
    return html.Div("Select a tab")


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
    """Show/hide split container and initialize wizard."""
    H = {"display": "none"}
    V = {"display": "block", "marginTop": "4px", "padding": "8px 10px",
         "backgroundColor": "#0f0f1a", "borderRadius": "6px",
         "border": f"1px solid {TEAL}33"}
    B = {"display": "block", "marginTop": "8px"}
    show = check_value and "split" in check_value
    # 13 outputs: container, data, display, state, question, s0-s3c(6), btn_row, btn_text
    if not show:
        return (H, current_split_data or [], [],
                {"step": 0, "category": "", "total_qty": 0}, "",
                H, H, H, H, H, H, H, "")

    if current_split_data:
        n = len(current_split_data)
        return (V, current_split_data, _render_split_rows(current_split_data),
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
        return (V, data, _render_split_rows(data),
                {"step": "done", "category": det_cat, "total_qty": n},
                f"All {n} items allocated! Click Save above when ready.",
                H, H, H, H, H, H, H, "")

    # Fresh start — wizard step 0
    return (V, [], [],
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
    """Rebuild editor order cards based on search/category/status filters."""
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

            item_cards.append(_build_item_card(
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
            html.Div(style={"width": f"{max(_order_pct, 3)}%", "height": "6px",
                            "background": f"linear-gradient(90deg, {_prog_color}88, {_prog_color})",
                            "borderRadius": "3px",
                            "transition": "width 0.3s ease"}),
        ], style={"width": "120px", "height": "6px", "backgroundColor": "#0d0d1a",
                  "borderRadius": "3px", "display": "inline-block", "verticalAlign": "middle",
                  "marginLeft": "10px", "overflow": "hidden"})
        done_pill = html.Span("\u2713 ALL DONE", className="pulse-complete", style={
            "fontSize": "12px", "fontWeight": "bold", "padding": "4px 14px",
            "borderRadius": "12px", "backgroundColor": f"{GREEN}22", "color": GREEN,
            "border": f"1px solid {GREEN}44", "marginLeft": "10px",
            "textShadow": "0 0 8px #2ecc7144"}) if _order_all_done else None
        _header_gradient = f"linear-gradient(180deg, {CYAN}08, transparent)" if not _order_all_done else f"linear-gradient(180deg, {GREEN}08, transparent)"
        order_card = html.Div([
            html.Div([
                html.Div([
                    html.Span(f"ORDER #{onum}", style={"color": CYAN, "fontWeight": "bold", "fontSize": "18px",
                                                        "letterSpacing": "0.5px"}),
                    html.Span(orig_location,
                              style={"fontSize": "11px", "padding": "3px 12px", "borderRadius": "12px",
                                     "backgroundColor": f"{_loc_color}22", "color": _loc_color,
                                     "border": f"1px solid {_loc_color}33", "marginLeft": "12px",
                                     "fontWeight": "bold", "whiteSpace": "nowrap"}),
                    done_pill,
                ], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}),
                html.Div([
                    html.Span(f"{inv['date']}", style={"color": GRAY, "fontSize": "12px"}),
                    html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66", "margin": "0 6px"}),
                    html.Span(f"{source_label}", style={"color": GRAY, "fontSize": "12px"}),
                    html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66", "margin": "0 6px"}),
                    html.Span(f"${order_total:.2f}", style={"color": WHITE, "fontSize": "14px", "fontWeight": "700"}),
                    html.Span([
                        html.Span(f"{order_saved}/{order_total_items} saved",
                                  style={"color": _prog_color, "fontSize": "13px", "fontWeight": "bold"}),
                        mini_progress,
                    ], style={"marginLeft": "auto", "display": "flex", "alignItems": "center"}),
                ], style={"display": "flex", "alignItems": "center"}),
            ], style={"marginBottom": "12px", "paddingBottom": "12px",
                      "borderBottom": f"1px solid {CYAN}18",
                      "background": _header_gradient,
                      "margin": "-18px -20px 12px -20px", "padding": "18px 20px 12px 20px",
                      "borderRadius": "10px 10px 0 0"}),
            html.Div(item_cards),
        ], className="order-card order-card-saved" if _order_all_done else "order-card",
           style={"backgroundColor": f"#2ecc7108" if _order_all_done else CARD2,
                  "padding": "18px 20px", "borderRadius": "10px",
                  "marginBottom": "14px",
                  "border": f"1px solid {GREEN}55" if _order_all_done else f"1px solid {CYAN}18",
                  "boxShadow": ("0 0 16px #2ecc7122, 0 4px 16px rgba(0,0,0,0.3)" if _order_all_done
                                else "0 2px 12px rgba(0,0,0,0.25)")})
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
    prevent_initial_call=True,
)
def handle_detail_save_reset(save_clicks, reset_clicks, display_name, category,
                             true_qty, loc_dropdown,
                             loc_split_check, split_data,
                             order_num, item_name, orig_qty):
    """Save or reset item details."""
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger_id = ctx.triggered[0]["prop_id"]
    is_reset = "det-reset-btn" in trigger_id
    key = (order_num, item_name)

    def _apply_details_to_inv_items():
        """Re-apply _ITEM_DETAILS to INV_ITEMS so STOCK_SUMMARY stays fresh.

        Uses _processed set to avoid duplicating split items, and looks up
        original pricing from INVOICES so cost calculations stay correct.
        """
        global INV_ITEMS
        if len(INV_ITEMS) == 0:
            return
        # Build lookup for original item pricing from INVOICES
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
                continue  # Already expanded this item (prevents split duplication)
            _processed.add(rkey)
            if rkey in _ITEM_DETAILS:
                dets = _ITEM_DETAILS[rkey]
                # Use original pricing from INVOICES for correct totals
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
                    # Map renamed items to original image (fallback)
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
        # Ensure _orig_name exists
        if "_orig_name" not in INV_ITEMS.columns:
            INV_ITEMS["_orig_name"] = INV_ITEMS["name"]
        # Re-apply categories for items without details
        if "category" in INV_ITEMS.columns:
            _cm = INV_ITEMS["category"].isna() | (INV_ITEMS["category"] == "")
            INV_ITEMS.loc[_cm, "category"] = INV_ITEMS.loc[_cm, "name"].apply(categorize_item)
        # Re-apply locations
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

        # Rebuild INV_ITEMS, STOCK_SUMMARY, and _UPLOADED_INVENTORY
        _apply_details_to_inv_items()
        _recompute_stock_summary()
        _rebuild_uploaded_inventory()

        return f"Saved! ({count} entry{'s' if count > 1 else ''})"
    return "Error saving"


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
    Output("stock-table-container", "children", allow_duplicate=True),
    Input("editor-save-trigger", "data"),
    State("stock-filter-input", "value"),
    State("stock-filter-cat", "value"),
    State("stock-filter-status", "value"),
    State("stock-include-tax", "value"),
    State("stock-sort-by", "value"),
    prevent_initial_call=True,
)
def _refresh_stock_on_save(trigger, search, cat_filter, status_filter, include_tax, sort_by):
    """Rebuild stock table after an editor save."""
    with_tax = bool(include_tax and "tax" in include_tax)
    return _build_stock_table_html(STOCK_SUMMARY, search or "", cat_filter or "All",
                                   status_filter or "All", show_with_tax=with_tax,
                                   sort_by=sort_by or "Category")


@app.callback(
    Output("location-inventory-display", "children", allow_duplicate=True),
    Input("editor-save-trigger", "data"),
    prevent_initial_call=True,
)
def _refresh_location_on_save(trigger):
    """Rebuild location display after an editor save."""
    tulsa_pct = (tulsa_spend / true_inventory_cost * 100) if true_inventory_cost else 0
    texas_pct = (texas_spend / true_inventory_cost * 100) if true_inventory_cost else 0
    return [
        _build_warehouse_card("TJ (Tulsa, OK)", "Tulsa", TEAL,
                               tulsa_spend, tulsa_orders, tulsa_subtotal, tulsa_tax, tulsa_pct),
        _build_warehouse_card("BRADEN (Texas)", "Texas", ORANGE,
                               texas_spend, texas_orders, texas_subtotal, texas_tax, texas_pct),
    ]


@app.callback(
    Output("inv-kpi-row", "children"),
    Input("editor-save-trigger", "data"),
    prevent_initial_call=True,
)
def _refresh_kpis_on_save(trigger):
    """Rebuild inventory KPI cards after an editor save."""
    return _build_inv_kpi_row()


@app.callback(
    Output("inv-health-panel", "children"),
    Input("editor-save-trigger", "data"),
    prevent_initial_call=True,
)
def _refresh_health_on_save(trigger):
    """Rebuild health panel after an editor save."""
    return [_build_inventory_health_panel()]


@app.callback(
    Output("editor-items-container", "children", allow_duplicate=True),
    Input("editor-save-trigger", "data"),
    State("editor-search", "value"),
    State("editor-cat-filter", "value"),
    State("editor-status-filter", "value"),
    prevent_initial_call=True,
)
def _refresh_editor_on_save(trigger, search, cat_filter, status_filter):
    """Rebuild editor after save so saved items auto-compact."""
    return filter_editor(search or "", cat_filter or "All", status_filter or "All")


# ── Receipt Upload + Item Wizard ──────────────────────────────────────────────

@app.callback(
    Output("receipt-wizard-state", "data"),
    Output("receipt-wizard-panel", "style"),
    Output("receipt-wizard-header", "children"),
    Output("receipt-wizard-orig", "children"),
    Output("wizard-name", "value"),
    Output("wizard-cat", "value"),
    Output("wizard-qty", "value"),
    Output("wizard-loc", "value"),
    Output("receipt-wizard-progress", "children"),
    Output("receipt-upload-status", "children"),
    Output("wizard-form-row", "style"),
    Output("wizard-nav-btns", "style"),
    Output("wizard-done-btn", "style"),
    Output("wizard-back-btn", "disabled"),
    Input("receipt-upload", "contents"),
    Input("wizard-save-btn", "n_clicks"),
    Input("wizard-skip-btn", "n_clicks"),
    Input("wizard-done-btn", "n_clicks"),
    Input("wizard-back-btn", "n_clicks"),
    State("receipt-upload", "filename"),
    State("receipt-wizard-state", "data"),
    State("wizard-name", "value"),
    State("wizard-cat", "value"),
    State("wizard-qty", "value"),
    State("wizard-loc", "value"),
    prevent_initial_call=True,
)
def handle_receipt_wizard(contents, save_clicks, skip_clicks, done_clicks, back_clicks,
                          filename, state, wiz_name, wiz_cat, wiz_qty, wiz_loc):
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

    def _build_wizard_display(st):
        """Build wizard header, orig info, form defaults, and progress dots."""
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

        return header, orig, item["name"], item["auto_category"], item["qty"], item["auto_location"], dots

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
                    _SHOW_FLEX, _NAV_FLEX, _HIDE, True)

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
                    _SHOW_FLEX, _NAV_FLEX, _HIDE, True)

        if not order or not order.get("items"):
            try:
                os.remove(save_path)
            except Exception:
                pass
            return (None, _HIDE, "", "", "", None, 1, None, "",
                    html.Div("Could not parse any items from this PDF.",
                             style={"color": RED, "fontSize": "13px"}),
                    _SHOW_FLEX, _NAV_FLEX, _HIDE, True)

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
                        _SHOW_FLEX, _NAV_FLEX, _HIDE, True)

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
        _save_new_order(order)
        _notify_railway_reload()

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

        header, orig, name, cat, qty, loc, dots = _build_wizard_display(new_state)
        status = html.Div([
            html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
            html.Span(f"Parsed {filename}: {len(items_for_wizard)} item(s) found. "
                       f"Order #{order['order_num']}",
                       style={"color": GREEN, "fontSize": "13px"}),
        ])

        return (new_state, _SHOW_BLOCK, header, orig, name, cat, qty, loc, dots,
                status, _SHOW_FLEX, _NAV_FLEX, _HIDE, True)

    # ── Handle Back ───────────────────────────────────────────────────────
    if "wizard-back-btn" in trigger:
        if not state or state.get("current_index", 0) <= 0:
            raise dash.exceptions.PreventUpdate
        state["current_index"] = max(0, state["current_index"] - 1)
        header, orig, name, cat, qty, loc, dots = _build_wizard_display(state)
        _back_disabled = state["current_index"] <= 0
        return (state, _SHOW_BLOCK, header, orig, name, cat, qty, loc, dots,
                _NO, _SHOW_FLEX, _NAV_FLEX, _HIDE, _back_disabled)

    # ── Handle Save & Next / Skip ──────────────────────────────────────────
    if "wizard-save-btn" in trigger or "wizard-skip-btn" in trigger:
        if not state:
            raise dash.exceptions.PreventUpdate

        idx = state["current_index"]
        item = state["items"][idx]

        if "wizard-save-btn" in trigger:
            display_name = (wiz_name or "").strip() or item["name"]
            category = wiz_cat or "Other"
            true_qty = int(wiz_qty) if wiz_qty else item["qty"]
            location = wiz_loc or ""

            details = [{"display_name": display_name, "category": category,
                        "true_qty": true_qty, "location": location}]

            ok = _save_item_details(state["order_num"], item["name"], details)
            if ok:
                key = (state["order_num"], item["name"])
                _ITEM_DETAILS[key] = details
                # Update _UPLOADED_INVENTORY
                loc = _norm_loc(location)
                if loc:
                    inv_key = (loc, display_name, category)
                    _UPLOADED_INVENTORY[inv_key] = _UPLOADED_INVENTORY.get(inv_key, 0) + true_qty
                state["saved_count"] = state.get("saved_count", 0) + 1

        # Advance to next item
        state["current_index"] = idx + 1

        if state["current_index"] >= state["total"]:
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
                    _NO, _HIDE, _HIDE, done_style, True)

        # Show next item
        _back_disabled = state["current_index"] <= 0
        header, orig, name, cat, qty, loc, dots = _build_wizard_display(state)
        return (state, _SHOW_BLOCK, header, orig, name, cat, qty, loc, dots,
                _NO, _SHOW_FLEX, _NAV_FLEX, _HIDE, _back_disabled)

    # ── Handle Done ────────────────────────────────────────────────────────
    if "wizard-done-btn" in trigger:
        return (None, _HIDE, "", "", "", None, 1, None, "", "",
                _SHOW_FLEX, _NAV_FLEX, _HIDE, True)

    raise dash.exceptions.PreventUpdate


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


# ── Stock Management Callbacks ────────────────────────────────────────────────

@app.callback(
    Output("stock-table-container", "children"),
    Input("stock-filter-input", "value"),
    Input("stock-filter-cat", "value"),
    Input("stock-filter-status", "value"),
    Input("stock-include-tax", "value"),
    Input("stock-sort-by", "value"),
    prevent_initial_call=True,
)
def filter_stock_table(search, cat_filter, status_filter, include_tax, sort_by):
    """Rebuild stock table based on filter inputs."""
    with_tax = bool(include_tax and "tax" in include_tax)
    return _build_stock_table_html(STOCK_SUMMARY, search or "", cat_filter or "All",
                                   status_filter or "All", show_with_tax=with_tax,
                                   sort_by=sort_by or "Category")


@app.callback(
    Output({"type": "use-stock-status", "index": MATCH}, "children"),
    Input({"type": "use-stock-btn", "index": MATCH}, "n_clicks"),
    State({"type": "use-stock-qty", "index": MATCH}, "value"),
    prevent_initial_call=True,
)
def handle_use_stock(n_clicks, use_qty):
    """Log N units used for the clicked item."""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    qty = max(1, int(use_qty or 1))

    # Extract item name from the pattern-match id
    trigger = ctx.triggered[0]
    prop_id = trigger["prop_id"]
    try:
        id_obj = json.loads(prop_id.split(".")[0])
        item_name = id_obj["index"]
    except (json.JSONDecodeError, KeyError):
        return "Error"

    result = _save_usage(item_name, qty, "Quick use")
    if result:
        # Update in-memory usage tracking
        _usage_by_item[item_name] = _usage_by_item.get(item_name, 0) + qty
        _USAGE_LOG.insert(0, result)
        # Update STOCK_SUMMARY in-memory
        if len(STOCK_SUMMARY) > 0:
            mask = STOCK_SUMMARY["display_name"] == item_name
            if mask.any():
                sidx = STOCK_SUMMARY.index[mask][0]
                STOCK_SUMMARY.at[sidx, "total_used"] = _usage_by_item[item_name]
                STOCK_SUMMARY.at[sidx, "in_stock"] = (
                    STOCK_SUMMARY.at[sidx, "total_purchased"] - _usage_by_item[item_name])
        new_stock = STOCK_SUMMARY.loc[STOCK_SUMMARY["display_name"] == item_name, "in_stock"]
        stock_val = int(new_stock.values[0]) if len(new_stock) > 0 else "?"
        return f"-{qty}! ({stock_val})"
    return "Error"


@app.callback(
    Output("usage-log-container", "children"),
    Input({"type": "undo-usage-btn", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def handle_undo_usage(all_clicks):
    """Undo a usage entry."""
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger = ctx.triggered[0]
    if not trigger["value"]:
        raise dash.exceptions.PreventUpdate

    prop_id = trigger["prop_id"]
    try:
        id_obj = json.loads(prop_id.split(".")[0])
        usage_id = int(id_obj["index"])
    except (json.JSONDecodeError, KeyError, ValueError):
        raise dash.exceptions.PreventUpdate

    # Find the usage entry to undo
    entry = None
    for u in _USAGE_LOG:
        if u.get("id") == usage_id:
            entry = u
            break

    if entry is None:
        raise dash.exceptions.PreventUpdate

    ok = _delete_usage(usage_id)
    if ok:
        # Update in-memory
        item_name = entry["item_name"]
        qty = entry.get("qty", 1)
        _usage_by_item[item_name] = max(0, _usage_by_item.get(item_name, 0) - qty)
        _USAGE_LOG.remove(entry)
        # Update STOCK_SUMMARY
        if len(STOCK_SUMMARY) > 0:
            mask = STOCK_SUMMARY["display_name"] == item_name
            if mask.any():
                idx = STOCK_SUMMARY.index[mask][0]
                STOCK_SUMMARY.at[idx, "total_used"] = _usage_by_item[item_name]
                STOCK_SUMMARY.at[idx, "in_stock"] = (
                    STOCK_SUMMARY.at[idx, "total_purchased"] - _usage_by_item[item_name])

    return _build_usage_log_html()


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

@app.callback(
    Output("chat-history", "children"),
    Output("chat-store", "data"),
    Output("chat-input", "value"),
    Input("chat-send", "n_clicks"),
    Input("chat-input", "n_submit"),
    Input({"type": "quick-q", "index": dash.ALL}, "n_clicks"),
    State("chat-input", "value"),
    State("chat-store", "data"),
    State("chat-history", "children"),
    prevent_initial_call=True,
)
def handle_chat(n_clicks, n_submit, quick_clicks, user_input, history_data, current_children):
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger_id = ctx.triggered[0]["prop_id"]

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
    answer = chatbot_answer(question)

    # Add to history
    history_data = history_data or []
    history_data.append({"q": question, "a": answer})

    # Build chat bubbles
    children = [
        # Initial greeting
        html.Div([
            html.Div("Hi! I'm your Etsy data assistant. Ask me anything about your store's "
                     "financial data -- revenue, products, shipping, fees, trends, and more. "
                     "Type **help** to see example questions!",
                style={
                    "backgroundColor": f"{CYAN}15", "border": f"1px solid {CYAN}33",
                    "borderRadius": "12px", "padding": "12px 16px", "maxWidth": "85%",
                    "color": WHITE, "fontSize": "13px", "whiteSpace": "pre-wrap",
                }),
        ], style={"display": "flex", "justifyContent": "flex-start", "marginBottom": "10px"}),
    ]

    for entry in history_data:
        # User message
        children.append(html.Div([
            html.Div(entry["q"], style={
                "backgroundColor": f"{ORANGE}25", "border": f"1px solid {ORANGE}44",
                "borderRadius": "12px", "padding": "10px 16px", "maxWidth": "75%",
                "color": WHITE, "fontSize": "13px",
            }),
        ], style={"display": "flex", "justifyContent": "flex-end", "marginBottom": "6px"}))

        # Bot response
        children.append(html.Div([
            html.Div([
                dcc.Markdown(entry["a"], style={"color": WHITE, "fontSize": "13px", "lineHeight": "1.5"}),
            ], style={
                "backgroundColor": f"{CYAN}15", "border": f"1px solid {CYAN}33",
                "borderRadius": "12px", "padding": "12px 16px", "maxWidth": "85%",
            }),
        ], style={"display": "flex", "justifyContent": "flex-start", "marginBottom": "10px"}))

    return children, history_data, ""


# ── Data Hub: Initial file list populator ───────────────────────────────────

@app.callback(
    Output("datahub-etsy-files", "children"),
    Output("datahub-receipt-files", "children"),
    Output("datahub-bank-files", "children"),
    Output("datahub-etsy-stats", "children"),
    Output("datahub-receipt-stats", "children"),
    Output("datahub-bank-stats", "children"),
    Input("datahub-init-trigger", "data"),
)
def init_datahub_files(_trigger):
    """Populate existing file lists and initial stats on page load."""
    etsy_files = _get_existing_files("etsy")
    receipt_files = _get_existing_files("receipt")
    bank_files = _get_existing_files("bank")

    etsy_stats = html.Div(f"{len(DATA)} transactions  |  {order_count} orders  |  "
                           f"Gross: ${gross_sales:,.2f}",
                           style={"color": TEAL, "fontSize": "12px", "fontFamily": "monospace"})
    receipt_stats = html.Div(f"{len(INVOICES)} orders  |  ${total_inventory_cost:,.2f} total spend",
                              style={"color": PURPLE, "fontSize": "12px", "fontFamily": "monospace"})
    bank_stats = html.Div(f"{len(BANK_TXNS)} transactions  |  Net: ${bank_net_cash:,.2f}",
                           style={"color": CYAN, "fontSize": "12px", "fontFamily": "monospace"})

    return (_render_file_list(etsy_files, TEAL),
            _render_file_list(receipt_files, PURPLE),
            _render_file_list(bank_files, CYAN),
            etsy_stats, receipt_stats, bank_stats)


# ── Data Hub: Upload Callback ───────────────────────────────────────────────

@app.callback(
    Output("datahub-etsy-status", "children"),
    Output("datahub-etsy-files", "children", allow_duplicate=True),
    Output("datahub-etsy-stats", "children", allow_duplicate=True),
    Output("datahub-receipt-status", "children"),
    Output("datahub-receipt-files", "children", allow_duplicate=True),
    Output("datahub-receipt-stats", "children", allow_duplicate=True),
    Output("datahub-bank-status", "children"),
    Output("datahub-bank-files", "children", allow_duplicate=True),
    Output("datahub-bank-stats", "children", allow_duplicate=True),
    Output("datahub-activity-log", "children"),
    Output("datahub-summary-strip", "children"),
    Output("app-header-content", "children"),
    Input("datahub-etsy-upload", "contents"),
    Input("datahub-receipt-upload", "contents"),
    Input("datahub-bank-upload", "contents"),
    State("datahub-etsy-upload", "filename"),
    State("datahub-receipt-upload", "filename"),
    State("datahub-bank-upload", "filename"),
    State("datahub-activity-log", "children"),
    prevent_initial_call=True,
)
def handle_datahub_upload(etsy_contents, receipt_contents, bank_contents,
                          etsy_filename, receipt_filename, bank_filename,
                          activity_log):
    """Handle file uploads from all 3 Data Hub zones."""
    import datetime as _dt

    trigger = callback_context.triggered[0]["prop_id"] if callback_context.triggered else ""
    now_str = _dt.datetime.now().strftime("%I:%M:%S %p")
    nu = dash.no_update  # shorthand

    # Initialize outputs — all no_update by default
    etsy_status, etsy_file_list, etsy_stats = nu, nu, nu
    rcpt_status, rcpt_file_list, rcpt_stats = nu, nu, nu
    bank_status, bank_file_list, bank_stats = nu, nu, nu
    new_log = activity_log or []
    summary = nu
    header = nu

    # ── Etsy CSV Upload ──────────────────────────────────────────────────
    if "datahub-etsy-upload" in trigger and etsy_contents:
        try:
            content_type, content_string = etsy_contents.split(",")
            decoded = base64.b64decode(content_string)

            valid, msg, df = _validate_etsy_csv(decoded)
            if not valid:
                etsy_status = html.Div([
                    html.Span("\u2717 ", style={"color": RED, "fontWeight": "bold"}),
                    html.Span(f"Invalid CSV: {msg}", style={"color": RED, "fontSize": "13px"}),
                ])
            else:
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
                    old_path = os.path.join(BASE_DIR, "data", "etsy_statements", overlap_file)
                    try:
                        os.remove(old_path)
                    except Exception:
                        pass

                save_path = os.path.join(BASE_DIR, "data", "etsy_statements", fname)
                with open(save_path, "wb") as f:
                    f.write(decoded)

                stats = _reload_etsy_data()
                _cascade_reload("etsy")
                # Auto-sync to Supabase so Railway stays in sync
                _sync_etsy_to_supabase(DATA)
                _notify_railway_reload()

                if has_overlap:
                    etsy_status = html.Div([
                        html.Span("\u26a0 ", style={"color": ORANGE, "fontWeight": "bold"}),
                        html.Span(f"Replaced {overlap_file} — {overlap_msg}. {msg}",
                                  style={"color": ORANGE, "fontSize": "13px"}),
                    ])
                    log_icon, log_color = "\u26a0", ORANGE
                    log_text = f"Etsy CSV: Replaced {overlap_file} with {fname} ({msg})"
                else:
                    etsy_status = html.Div([
                        html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
                        html.Span(f"Uploaded {fname} — {msg}", style={"color": GREEN, "fontSize": "13px"}),
                    ])
                    log_icon, log_color = "\u2713", GREEN
                    log_text = f"Etsy CSV: {fname} ({msg})"

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
            etsy_status = html.Div(f"Error: {e}", style={"color": RED, "fontSize": "13px"})

    # ── Receipt PDF Upload ───────────────────────────────────────────────
    elif "datahub-receipt-upload" in trigger and receipt_contents:
        try:
            content_type, content_string = receipt_contents.split(",")
            decoded = base64.b64decode(content_string)

            fname = receipt_filename or "receipt.pdf"
            save_folder = os.path.join(BASE_DIR, "data", "invoices", "keycomp")
            save_path = os.path.join(save_folder, fname)
            with open(save_path, "wb") as f:
                f.write(decoded)

            # Parse with _parse_invoices
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
                # Check for duplicate order numbers
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
                    # Move to personal_amazon folder if source matches
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
                    new_log = [html.Div([
                        html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
                        html.Span(f"\u2713 Receipt: {fname} (Order #{stats['order_num']}, "
                                  f"{stats['item_count']} items)",
                                  style={"color": GREEN, "fontSize": "12px"}),
                    ], style={"padding": "3px 0", "borderBottom": "1px solid #ffffff08"})] + (
                        new_log if isinstance(new_log, list) else [])
        except Exception as e:
            rcpt_status = html.Div(f"Error: {e}", style={"color": RED, "fontSize": "13px"})

    # ── Bank PDF/CSV Upload ─────────────────────────────────────────────
    elif "datahub-bank-upload" in trigger and bank_contents:
        try:
            content_type, content_string = bank_contents.split(",")
            decoded = base64.b64decode(content_string)

            # Check for duplicate before saving
            is_dup, dup_file = _check_bank_file_duplicate(decoded, bank_filename)
            if is_dup:
                bank_status = html.Div([
                    html.Span("\u26a0 ", style={"color": ORANGE, "fontWeight": "bold"}),
                    html.Span(f"This file is already uploaded (matches {dup_file})",
                              style={"color": ORANGE, "fontSize": "13px", "fontWeight": "bold"}),
                ])
                new_log = [html.Div([
                    html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
                    html.Span(f"\u26a0 Bank: Duplicate of {dup_file}, skipped",
                              style={"color": ORANGE, "fontSize": "12px"}),
                ], style={"padding": "3px 0", "borderBottom": "1px solid #ffffff08"})] + (
                    new_log if isinstance(new_log, list) else [])
            else:
                fname = bank_filename or "bank_statement.pdf"
                save_folder = os.path.join(BASE_DIR, "data", "bank_statements")
                os.makedirs(save_folder, exist_ok=True)
                save_path = os.path.join(save_folder, fname)
                with open(save_path, "wb") as f:
                    f.write(decoded)

                stats = _reload_bank_data()
                _cascade_reload("bank")
                # Auto-sync to Supabase so Railway stays in sync
                _sync_bank_to_supabase(BANK_TXNS)
                _notify_railway_reload()
                bank_status = html.Div([
                    html.Span("\u2713 ", style={"color": GREEN, "fontWeight": "bold"}),
                    html.Span(f"Uploaded {fname} — {stats['transactions']} total transactions, "
                              f"Net: ${stats['net_cash']:,.2f}",
                              style={"color": GREEN, "fontSize": "13px"}),
                ])
                bank_stats = html.Div(
                    f"{stats['transactions']} transactions  |  Net: ${stats['net_cash']:,.2f}",
                    style={"color": CYAN, "fontSize": "12px", "fontFamily": "monospace"})
                bank_file_list = _render_file_list(_get_existing_files("bank"), CYAN)
                summary = _build_datahub_summary()
                header = _header_text()
                new_log = [html.Div([
                    html.Span(f"[{now_str}] ", style={"color": DARKGRAY, "fontSize": "11px"}),
                    html.Span(f"\u2713 Bank: {fname} ({stats['transactions']} txns)",
                              style={"color": GREEN, "fontSize": "12px"}),
                ], style={"padding": "3px 0", "borderBottom": "1px solid #ffffff08"})] + (
                    new_log if isinstance(new_log, list) else [])
        except Exception as e:
            bank_status = html.Div(f"Error: {e}", style={"color": RED, "fontSize": "13px"})

    return (etsy_status, etsy_file_list, etsy_stats,
            rcpt_status, rcpt_file_list, rcpt_stats,
            bank_status, bank_file_list, bank_stats,
            new_log, summary, header)


# ── Category Manager Callbacks ────────────────────────────────────────────────

@app.callback(
    Output({"type": "catmgr-status", "index": MATCH}, "children"),
    Input({"type": "catmgr-cat", "index": MATCH}, "value"),
    Input({"type": "catmgr-loc", "index": MATCH}, "value"),
    State({"type": "catmgr-cat", "index": MATCH}, "id"),
    prevent_initial_call=True,
)
def catmgr_save(new_cat, new_loc, component_id):
    """Save category/location change from Category Manager instantly."""
    global INV_ITEMS, STOCK_SUMMARY
    item_key = component_id["index"]  # "order_num||orig_name"
    parts = item_key.split("||", 1)
    if len(parts) != 2:
        raise dash.exceptions.PreventUpdate
    order_num, orig_name = parts

    if not new_cat:
        raise dash.exceptions.PreventUpdate

    # Find matching rows in INV_ITEMS
    if "_orig_name" in INV_ITEMS.columns:
        mask = (INV_ITEMS["order_num"] == order_num) & (
            (INV_ITEMS["_orig_name"] == orig_name) | (INV_ITEMS["name"] == orig_name)
        )
    else:
        mask = (INV_ITEMS["order_num"] == order_num) & (INV_ITEMS["name"] == orig_name)

    if mask.sum() == 0:
        raise dash.exceptions.PreventUpdate

    # Get current item info
    first_row = INV_ITEMS.loc[mask].iloc[0]
    item_name_for_db = orig_name  # Use orig_name for the DB key
    display_name = first_row["name"]

    # Update INV_ITEMS in memory
    INV_ITEMS.loc[mask, "category"] = new_cat
    if new_loc:
        INV_ITEMS.loc[mask, "location"] = new_loc
        INV_ITEMS.loc[mask, "_override_location"] = new_loc

    # Save to Supabase via item_details
    key = (order_num, item_name_for_db)
    if key in _ITEM_DETAILS:
        # Update existing details
        for det in _ITEM_DETAILS[key]:
            det["category"] = new_cat
            if new_loc:
                det["location"] = new_loc
        _save_item_details(order_num, item_name_for_db, _ITEM_DETAILS[key])
    else:
        # Create new detail entry
        true_qty = int(first_row["qty"])
        loc = new_loc or first_row.get("location", "")
        details = [{"display_name": display_name, "category": new_cat,
                     "true_qty": true_qty, "location": loc}]
        ok = _save_item_details(order_num, item_name_for_db, details)
        if ok:
            _ITEM_DETAILS[key] = details

    # Recompute globals
    _recompute_stock_summary()

    return "\u2713"


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
