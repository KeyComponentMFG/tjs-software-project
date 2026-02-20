"""
data_state.py — All data loading, computed metrics, and reload functions.
This is the single source of truth for dashboard data.
Every page/callback imports from here instead of using globals in the monolith.
"""

import os
import json
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

# ── Paths ────────────────────────────────────────────────────────────────────
# BASE_DIR points to the project root (parent of etsy_dashboard/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Import Supabase helpers ──────────────────────────────────────────────────
import sys
sys.path.insert(0, BASE_DIR)

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
    save_image_override,
    save_location_override,
    load_inventory_items_with_ids,
)

from etsy_dashboard.theme import BANK_TAX_DEDUCTIBLE, CATEGORY_OPTIONS


# ══════════════════════════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def parse_money(val):
    if pd.isna(val) or val == "--" or val == "":
        return 0.0
    val = str(val).replace("$", "").replace(",", "").replace('"', "")
    try:
        return float(val)
    except Exception:
        return 0.0


def money(val):
    """Format a number as $X,XXX.XX (convenience for templates)."""
    if val < 0:
        return f"-${abs(val):,.2f}"
    return f"${val:,.2f}"


def categorize_item(name):
    """Auto-categorize items. Names match CATEGORY_OPTIONS."""
    name_l = name.lower()
    if any(w in name_l for w in ["pottery", "meat grinder", "slicer"]):
        return "Personal/Gift"
    if any(w in name_l for w in ["articles of organization", "credit card surcharge", "llc filing"]):
        return "Business Fees"
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


def classify_location(addr):
    if not addr:
        return "Unknown"
    addr_u = addr.upper()
    if "TULSA" in addr_u or ", OK " in addr_u or ", OK" in addr_u:
        return "Tulsa, OK"
    if "CELINA" in addr_u or "PROSPER" in addr_u or ", TX " in addr_u or ", TX" in addr_u:
        return "Texas"
    return "Other"


def _norm_loc(loc_str):
    """Normalize location string to 'Tulsa' or 'Texas' or ''."""
    loc_str = (loc_str or "").strip().lower()
    if "tulsa" in loc_str or "tj" in loc_str or loc_str in ("ok", "oklahoma"):
        return "Tulsa"
    elif "texas" in loc_str or "braden" in loc_str or loc_str in ("tx", "celina", "prosper"):
        return "Texas"
    return ""


def get_draw_reason(desc):
    d = desc.upper()
    for key, reason in draw_reasons.items():
        if key.upper() in d:
            return reason
    return ""


def _parse_bank_date(d):
    parts = d.split("/")
    return (int(parts[2]), int(parts[0]), int(parts[1]))


# ══════════════════════════════════════════════════════════════════════════════
#  LOAD ALL DATA
# ══════════════════════════════════════════════════════════════════════════════

_sb = _load_data()
DATA = _sb["DATA"]
CONFIG = _sb["CONFIG"]
INVOICES = _sb["INVOICES"]
BANK_TXNS = _sb["BANK_TXNS"]

# ── Extract config values ────────────────────────────────────────────────────
etsy_balance = CONFIG.get("etsy_balance", 0)
etsy_pre_capone_deposits = CONFIG.get("etsy_pre_capone_deposits", 0)
pre_capone_detail = [tuple(row) for row in CONFIG.get("pre_capone_detail", [])]
draw_reasons = CONFIG.get("draw_reasons", {})

# ── Best Buy Citi Credit Card ────────────────────────────────────────────────
_bb_cc = CONFIG.get("best_buy_cc", {})
bb_cc_limit = _bb_cc.get("credit_limit", 0)
bb_cc_purchases = _bb_cc.get("purchases", [])
bb_cc_payments = _bb_cc.get("payments", [])
bb_cc_total_charged = sum(p["amount"] for p in bb_cc_purchases)
bb_cc_total_paid = sum(p["amount"] for p in bb_cc_payments)
bb_cc_balance = bb_cc_total_charged - bb_cc_total_paid
bb_cc_available = bb_cc_limit - bb_cc_balance
bb_cc_asset_value = bb_cc_total_charged

# ── Pre-compute Etsy metrics ────────────────────────────────────────────────
sales_df = DATA[DATA["Type"] == "Sale"]
fee_df = DATA[DATA["Type"] == "Fee"]
ship_df = DATA[DATA["Type"] == "Shipping"]
mkt_df = DATA[DATA["Type"] == "Marketing"]
refund_df = DATA[DATA["Type"] == "Refund"]
tax_df = DATA[DATA["Type"] == "Tax"]
deposit_df = DATA[DATA["Type"] == "Deposit"]
buyer_fee_df = DATA[DATA["Type"] == "Buyer Fee"]

gross_sales = sales_df["Net_Clean"].sum()
total_refunds = abs(refund_df["Net_Clean"].sum())
net_sales = gross_sales - total_refunds
total_fees = abs(fee_df["Net_Clean"].sum())
total_shipping_cost = abs(ship_df["Net_Clean"].sum())
total_marketing = abs(mkt_df["Net_Clean"].sum())
total_taxes = abs(tax_df["Net_Clean"].sum())

order_count = len(sales_df)
avg_order = gross_sales / order_count if order_count else 0

total_buyer_fees = abs(buyer_fee_df["Net_Clean"].sum()) if len(buyer_fee_df) else 0.0
etsy_net_earned = gross_sales - total_fees - total_shipping_cost - total_marketing - total_refunds - total_taxes - total_buyer_fees
net_profit = etsy_net_earned
profit_margin = (net_profit / gross_sales * 100) if gross_sales else 0

# ── Build inventory DataFrames ───────────────────────────────────────────────
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
    INV_DF = pd.DataFrame(inv_rows).sort_values("date_parsed")
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

# ── Tax-inclusive cost per item ───────────────────────────────────────────────
_order_totals_map = {}
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

# ── Item Details (rename / categorize / true qty) ────────────────────────────
_ITEM_DETAILS: dict[tuple[str, str], list[dict]] = {}
try:
    _raw_details = _load_item_details()
    for d in _raw_details:
        key = (d["order_num"], d["item_name"])
        if d.get("category") == "_JSON_":
            try:
                entries = json.loads(d["display_name"])
                for entry in entries:
                    _ITEM_DETAILS.setdefault(key, []).append(entry)
            except (json.JSONDecodeError, TypeError):
                pass
        else:
            _ITEM_DETAILS.setdefault(key, []).append({
                "display_name": d["display_name"],
                "category": d["category"],
                "true_qty": d["true_qty"],
                "location": d.get("location", ""),
            })
except Exception:
    pass

# ── Location Overrides ───────────────────────────────────────────────────────
_LOC_OVERRIDES: dict[tuple[str, str], list[dict]] = {}
try:
    _raw_overrides = _load_location_overrides()
    for ov in _raw_overrides:
        key = (ov["order_num"], ov["item_name"])
        _LOC_OVERRIDES.setdefault(key, []).append({"location": ov["location"], "qty": ov["qty"]})
except Exception:
    pass

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

_UPLOADED_INVENTORY: dict[tuple[str, str, str], int] = {}

# Build image_url lookup BEFORE renaming
_IMAGE_URLS: dict[str, str] = {}
if len(INV_ITEMS) > 0 and "image_url" in INV_ITEMS.columns:
    for _, _r in INV_ITEMS.iterrows():
        _n = _r["name"]
        _u = _r.get("image_url", "") or ""
        if _u and _n not in _IMAGE_URLS:
            _IMAGE_URLS[_n] = _u

# Apply item details: rename, recategorize, adjust qty
if len(INV_ITEMS) > 0 and _ITEM_DETAILS:
    detail_rows = []
    for _, row in INV_ITEMS.iterrows():
        key = (row["order_num"], row["name"])
        if key in _ITEM_DETAILS:
            dets = _ITEM_DETAILS[key]
            orig_total = row["price"] * row["qty"]
            orig_with_tax = row.get("total_with_tax", orig_total)
            total_detail_qty = sum(d["true_qty"] for d in dets)
            per_unit = orig_total / total_detail_qty if total_detail_qty > 0 else 0
            per_unit_tax = orig_with_tax / total_detail_qty if total_detail_qty > 0 else 0
            _orig_img = _IMAGE_URLS.get(row["name"], "")
            for det in dets:
                new_row = row.copy()
                new_row["_orig_name"] = row["name"]
                new_row["name"] = det["display_name"]
                new_row["category"] = det["category"]
                new_row["qty"] = det["true_qty"]
                new_row["price"] = round(per_unit, 2)
                new_row["total"] = round(per_unit * det["true_qty"], 2)
                new_row["total_with_tax"] = round(per_unit_tax * det["true_qty"], 2)
                new_row["image_url"] = ""
                if det.get("location"):
                    new_row["_override_location"] = det["location"]
                detail_rows.append(new_row)
                if _orig_img and det["display_name"] not in _IMAGE_URLS:
                    _IMAGE_URLS[det["display_name"]] = _orig_img
        else:
            rc = row.copy()
            rc["_orig_name"] = row["name"]
            detail_rows.append(rc)
    INV_ITEMS = pd.DataFrame(detail_rows)

if len(INV_ITEMS) > 0 and "_orig_name" not in INV_ITEMS.columns:
    INV_ITEMS["_orig_name"] = INV_ITEMS["name"]

# Rebuild _UPLOADED_INVENTORY
_UPLOADED_INVENTORY.clear()
if len(INV_ITEMS) > 0:
    for _, _r in INV_ITEMS.iterrows():
        _loc = _norm_loc(_r.get("_override_location", ""))
        if not _loc:
            continue
        _inv_key = (_loc, _r["name"], _r.get("category", "Other"))
        _UPLOADED_INVENTORY[_inv_key] = _UPLOADED_INVENTORY.get(_inv_key, 0) + int(_r["qty"])

# Apply persistent image overrides
try:
    _img_overrides = _load_image_overrides()
    if _img_overrides:
        _IMAGE_URLS.update(_img_overrides)
except Exception:
    pass

# ── Inventory aggregates ─────────────────────────────────────────────────────
total_inventory_cost = INV_DF["grand_total"].sum()
total_inv_subtotal = INV_DF["subtotal"].sum()
total_inv_tax = INV_DF["tax"].sum()
biz_inv_cost = INV_DF[INV_DF["source"] == "Key Component Mfg"]["grand_total"].sum()
personal_acct_cost = INV_DF[INV_DF["source"] == "Personal Amazon"]["grand_total"].sum()
inv_order_count = len(INV_DF)

gigi_mask = INV_DF["file"].str.contains("Gigi", na=False)
gigi_cost = INV_DF[gigi_mask]["grand_total"].sum()

monthly_inv_spend = INV_DF.groupby("month")["grand_total"].sum()
monthly_inv_subtotal = INV_DF.groupby("month")["subtotal"].sum()

# ── Auto-categorize items ────────────────────────────────────────────────────
if len(INV_ITEMS) > 0:
    if "category" in INV_ITEMS.columns:
        _cat_mask = INV_ITEMS["category"].isna() | (INV_ITEMS["category"] == "")
        INV_ITEMS.loc[_cat_mask, "category"] = INV_ITEMS.loc[_cat_mask, "name"].apply(categorize_item)
    else:
        INV_ITEMS["category"] = INV_ITEMS["name"].apply(categorize_item)
    inv_by_category = INV_ITEMS.groupby("category")["total"].sum().sort_values(ascending=False)
else:
    inv_by_category = pd.Series(dtype=float)

if len(INV_ITEMS) > 0:
    personal_total = INV_ITEMS[INV_ITEMS["category"] == "Personal/Gift"]["total"].sum()
    biz_fee_total = INV_ITEMS[INV_ITEMS["category"] == "Business Fees"]["total"].sum()
    true_inventory_cost = total_inventory_cost - personal_total - biz_fee_total
else:
    personal_total = 0.0
    biz_fee_total = 0.0
    true_inventory_cost = total_inventory_cost

# ── Bank Statement Data ─────────────────────────────────────────────────────
_bank_json_path = os.path.join(BASE_DIR, "data", "generated", "bank_transactions.json")
if os.path.exists(_bank_json_path):
    with open(_bank_json_path) as _bf:
        _bank_source_files = json.load(_bf).get("metadata", {}).get("source_files", [])
else:
    _bank_source_files = []
bank_statement_count = len(_bank_source_files)

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

# ── Etsy-side accounting ────────────────────────────────────────────────────
etsy_total_deposited = etsy_pre_capone_deposits + bank_total_deposits
etsy_balance_calculated = etsy_net_earned - etsy_total_deposited
etsy_csv_gap = round(etsy_balance_calculated - etsy_balance, 2)

# ── Bank-Reconciled Profit ──────────────────────────────────────────────────
_biz_expense_cats = ["Shipping", "Craft Supplies", "Etsy Fees", "Subscriptions", "AliExpress Supplies", "Business Credit Card"]
bank_biz_expense_total = sum(bank_by_cat.get(c, 0) for c in _biz_expense_cats)
bank_all_expenses = bank_by_cat.get("Amazon Inventory", 0) + bank_biz_expense_total
bank_cash_on_hand = bank_net_cash + etsy_balance
bank_owner_draw_total = sum(bank_by_cat.get(c, 0) for c in bank_by_cat if c.startswith("Owner Draw"))
real_profit = bank_cash_on_hand + bank_owner_draw_total
real_profit_margin = (real_profit / gross_sales * 100) if gross_sales else 0

# ── Old bank matching ───────────────────────────────────────────────────────
old_bank_receipted = INV_DF.loc[INV_DF["payment_method"] != "Discover ending in 4570", "grand_total"].sum()
old_bank_receipted = min(old_bank_receipted, etsy_pre_capone_deposits)
bank_unaccounted = round(etsy_pre_capone_deposits - old_bank_receipted, 2)

# ── Draw settlement ─────────────────────────────────────────────────────────
tulsa_draws = [t for t in bank_debits if t["category"] == "Owner Draw - Tulsa"]
texas_draws = [t for t in bank_debits if t["category"] == "Owner Draw - Texas"]
tulsa_draw_total = sum(t["amount"] for t in tulsa_draws)
texas_draw_total = sum(t["amount"] for t in texas_draws)
draw_diff = abs(tulsa_draw_total - texas_draw_total)
draw_owed_to = "Braden" if tulsa_draw_total > texas_draw_total else "TJ"

# ── Credit card / other account spending ─────────────────────────────────────
cc_by_method = INV_DF.groupby("payment_method").agg(
    count=("grand_total", "count"),
    total=("grand_total", "sum"),
).to_dict("index") if len(INV_DF) > 0 else {}

discover_inv_total = cc_by_method.get("Discover ending in 4570", {}).get("total", 0)
discover_inv_count = int(cc_by_method.get("Discover ending in 4570", {}).get("count", 0))
visa_methods = {k: v for k, v in cc_by_method.items() if k.startswith("Visa")}
visa_inv_total = sum(v["total"] for v in visa_methods.values())
visa_inv_count = int(sum(v["count"] for v in visa_methods.values()))
other_inv_methods = {k: v for k, v in cc_by_method.items()
                     if not k.startswith("Discover") and not k.startswith("Visa")}
other_inv_total = sum(v["total"] for v in other_inv_methods.values())
other_inv_count = int(sum(v["count"] for v in other_inv_methods.values()))

# ── Running balance for ledger ──────────────────────────────────────────────
bank_txns_sorted = sorted(BANK_TXNS, key=lambda x: (_parse_bank_date(x["date"]), 0 if x["type"] == "deposit" else 1))
bank_running = []
_bal = 0.0
for t in bank_txns_sorted:
    if t["type"] == "deposit":
        _bal += t["amount"]
    else:
        _bal -= t["amount"]
    bank_running.append({**t, "_balance": round(_bal, 2)})

# ── Cross-Source Profit ─────────────────────────────────────────────────────
bank_amazon_inv = bank_by_cat.get("Amazon Inventory", 0)
receipt_cogs_outside_bank = max(0, true_inventory_cost - bank_amazon_inv)
full_profit = real_profit - receipt_cogs_outside_bank
full_profit_margin = (full_profit / gross_sales * 100) if gross_sales else 0

# ── Location classification ─────────────────────────────────────────────────
INV_DF["location"] = INV_DF["ship_address"].apply(classify_location)
if len(INV_ITEMS) > 0:
    if "_override_location" in INV_ITEMS.columns:
        INV_ITEMS["location"] = INV_ITEMS.apply(
            lambda r: r["_override_location"] if r["_override_location"] else classify_location(r["ship_to"]),
            axis=1,
        )
    else:
        INV_ITEMS["location"] = INV_ITEMS["ship_to"].apply(classify_location)

# Business-only filtered data
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

monthly_inv_spend = BIZ_INV_DF.groupby("month")["grand_total"].sum()

# ── Quick-Adds ──────────────────────────────────────────────────────────────
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
        for col in INV_ITEMS.columns:
            if col not in qa_df.columns:
                qa_df[col] = ""
        INV_ITEMS = pd.concat([INV_ITEMS, qa_df[INV_ITEMS.columns]], ignore_index=True)

# ── Usage Log & Stock Summary ───────────────────────────────────────────────
_USAGE_LOG: list[dict] = []
try:
    _USAGE_LOG = _load_usage_log()
except Exception:
    pass

_usage_by_item: dict[str, int] = {}
for u in _USAGE_LOG:
    _usage_by_item[u["item_name"]] = _usage_by_item.get(u["item_name"], 0) + u.get("qty", 1)

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
        _stock_agg["total_used"] = _stock_agg["display_name"].map(lambda n: _usage_by_item.get(n, 0))
        _stock_agg["in_stock"] = _stock_agg["total_purchased"] - _stock_agg["total_used"]
        _stock_agg["unit_cost"] = (_stock_agg["total_cost"] / _stock_agg["total_purchased"]).round(2)
        if "total_cost_with_tax" in _stock_agg.columns:
            _stock_agg["unit_cost_with_tax"] = (_stock_agg["total_cost_with_tax"] / _stock_agg["total_purchased"]).round(2)
        else:
            _stock_agg["total_cost_with_tax"] = _stock_agg["total_cost"]
            _stock_agg["unit_cost_with_tax"] = _stock_agg["unit_cost"]
        _stock_agg["image_url"] = _stock_agg["display_name"].map(lambda n: _IMAGE_URLS.get(n, ""))
        STOCK_SUMMARY = _stock_agg.sort_values(["category", "display_name"]).reset_index(drop=True)


def _recompute_stock_summary():
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


# ── Stock KPI vars ──────────────────────────────────────────────────────────
total_in_stock = int(STOCK_SUMMARY["in_stock"].sum()) if len(STOCK_SUMMARY) > 0 else 0
total_stock_value = STOCK_SUMMARY["total_cost"].sum() if len(STOCK_SUMMARY) > 0 else 0
low_stock_count = int((STOCK_SUMMARY["in_stock"].between(1, 2)).sum()) if len(STOCK_SUMMARY) > 0 else 0
out_of_stock_count = int((STOCK_SUMMARY["in_stock"] <= 0).sum()) if len(STOCK_SUMMARY) > 0 else 0
unique_item_count = len(STOCK_SUMMARY) if len(STOCK_SUMMARY) > 0 else 0

# ── Payment method aggregates ───────────────────────────────────────────────
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
    if len(INV_ITEMS) > 0:
        for pm, grp in INV_ITEMS.groupby("payment_method"):
            if pm in payment_summary:
                payment_summary[pm]["items"] = grp.to_dict("records")

# ── Per-location aggregates ─────────────────────────────────────────────────
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
    tulsa_monthly = BIZ_INV_DF[BIZ_INV_DF["location"] == "Tulsa, OK"].groupby("month")["grand_total"].sum()
    texas_monthly = BIZ_INV_DF[BIZ_INV_DF["location"] == "Texas"].groupby("month")["grand_total"].sum()
else:
    tulsa_by_cat = pd.Series(dtype=float)
    texas_by_cat = pd.Series(dtype=float)
    tulsa_monthly = pd.Series(dtype=float)
    texas_monthly = pd.Series(dtype=float)

true_net_profit = net_profit - total_inventory_cost
true_profit_margin = (true_net_profit / gross_sales * 100) if gross_sales else 0

# ── Fee breakdown ───────────────────────────────────────────────────────────
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

ship_fee_rows = fee_df[fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)].copy()
orders_with_paid_shipping = set(ship_fee_rows["Info"].dropna())
all_order_ids = set(sales_df["Title"].str.extract(r"(Order #\d+)", expand=False).dropna())
orders_free_shipping = all_order_ids - orders_with_paid_shipping
paid_ship_count = len(orders_with_paid_shipping & all_order_ids)
free_ship_count = len(orders_free_shipping)
avg_outbound_label = usps_outbound / usps_outbound_count if usps_outbound_count else 0
est_label_cost_paid_orders = paid_ship_count * avg_outbound_label
paid_shipping_profit = buyer_paid_shipping - est_label_cost_paid_orders
est_label_cost_free_orders = free_ship_count * avg_outbound_label

# Refunded orders shipping
refund_df_orders = refund_df.copy()
refund_df_orders["Order"] = refund_df_orders["Title"].str.extract(r"(Order #\d+)")
refunded_order_ids = set(refund_df_orders["Order"].dropna())

refund_ship_fees = 0.0
refund_ship_count = 0
for oid in refunded_order_ids:
    order_ship = fee_df[
        (fee_df["Info"] == oid) & fee_df["Title"].str.contains("Transaction fee: Shipping", na=False)
    ]
    if len(order_ship):
        refund_ship_fees += abs(order_ship["Net_Clean"].sum())
        refund_ship_count += 1
refund_buyer_shipping = refund_ship_fees / 0.065 if refund_ship_fees else 0
est_refund_label_cost = len(refunded_order_ids) * avg_outbound_label

# Return label matches
return_labels = ship_df[ship_df["Title"] == "USPS return shipping label"].sort_values("Date_Parsed")
return_label_matches = []
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

# ── Monthly breakdown ───────────────────────────────────────────────────────
months_sorted = sorted(DATA["Month"].dropna().unique())
inv_months_sorted = sorted(INV_DF["month"].dropna().unique()) if len(INV_DF) > 0 else []


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
weekly_sales = sales_df.copy()
weekly_sales["WeekStart"] = weekly_sales["Date_Parsed"].dt.to_period("W").apply(lambda p: p.start_time)
weekly_aov = weekly_sales.groupby("WeekStart").agg(
    total=("Net_Clean", "sum"),
    count=("Net_Clean", "count"),
)
weekly_aov["aov"] = weekly_aov["total"] / weekly_aov["count"]

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


# ══════════════════════════════════════════════════════════════════════════════
#  RELOAD FUNCTIONS (Data Hub)
# ══════════════════════════════════════════════════════════════════════════════

_RECENT_UPLOADS: set = set()


def _reload_etsy_data():
    global DATA, sales_df, fee_df, ship_df, mkt_df, refund_df, tax_df
    global deposit_df, buyer_fee_df
    global gross_sales, total_refunds, net_sales, total_fees
    global total_shipping_cost, total_marketing, total_taxes
    global net_profit, order_count, avg_order, profit_margin
    global total_buyer_fees, etsy_net_earned, etsy_total_deposited
    global etsy_balance_calculated, etsy_csv_gap, real_profit, real_profit_margin
    global bank_all_expenses, bank_cash_on_hand

    fresh = _load_data()
    DATA = fresh["DATA"]

    sales_df = DATA[DATA["Type"] == "Sale"]
    fee_df = DATA[DATA["Type"] == "Fee"]
    ship_df = DATA[DATA["Type"] == "Shipping"]
    mkt_df = DATA[DATA["Type"] == "Marketing"]
    refund_df = DATA[DATA["Type"] == "Refund"]
    tax_df = DATA[DATA["Type"] == "Tax"]
    deposit_df = DATA[DATA["Type"] == "Deposit"]
    buyer_fee_df = DATA[DATA["Type"] == "Buyer Fee"]

    gross_sales = sales_df["Net_Clean"].sum()
    total_refunds = abs(refund_df["Net_Clean"].sum())
    net_sales = gross_sales - total_refunds
    total_fees = abs(fee_df["Net_Clean"].sum())
    total_shipping_cost = abs(ship_df["Net_Clean"].sum())
    total_marketing = abs(mkt_df["Net_Clean"].sum())
    total_taxes = abs(tax_df["Net_Clean"].sum())
    order_count = len(sales_df)
    avg_order = gross_sales / order_count if order_count else 0

    total_buyer_fees = abs(buyer_fee_df["Net_Clean"].sum()) if len(buyer_fee_df) else 0.0
    etsy_net_earned = (gross_sales - total_fees - total_shipping_cost
                       - total_marketing - total_refunds - total_taxes - total_buyer_fees)
    net_profit = etsy_net_earned
    profit_margin = (net_profit / gross_sales * 100) if gross_sales else 0
    etsy_total_deposited = etsy_pre_capone_deposits + bank_total_deposits
    etsy_balance_calculated = etsy_net_earned - etsy_total_deposited
    etsy_csv_gap = round(etsy_balance_calculated - etsy_balance, 2)
    bank_cash_on_hand = bank_net_cash + etsy_balance
    real_profit = bank_cash_on_hand + bank_owner_draw_total
    real_profit_margin = (real_profit / gross_sales * 100) if gross_sales else 0

    return {
        "transactions": len(DATA),
        "orders": order_count,
        "gross_sales": gross_sales,
        "net_profit": net_profit,
    }


def _reload_bank_data():
    global BANK_TXNS, bank_deposits, bank_debits
    global bank_total_deposits, bank_total_debits, bank_net_cash
    global bank_by_cat, bank_monthly, bank_statement_count
    global bank_tax_deductible, bank_personal, bank_pending
    global bank_biz_expense_total, bank_all_expenses, bank_cash_on_hand
    global bank_owner_draw_total, real_profit, real_profit_margin
    global tulsa_draws, texas_draws, tulsa_draw_total, texas_draw_total
    global draw_diff, draw_owed_to
    global bank_txns_sorted, bank_running

    bank_dir = os.path.join(BASE_DIR, "data", "bank_statements")
    if not os.path.isdir(bank_dir):
        return {"transactions": 0, "statements": 0, "net_cash": 0}

    sys.path.insert(0, BASE_DIR)
    from _parse_bank_statements import parse_bank_pdf as _parse_bank
    all_txns = []
    source_files = []
    for fn in sorted(os.listdir(bank_dir)):
        if fn.lower().endswith(".pdf"):
            fpath = os.path.join(bank_dir, fn)
            try:
                txns, _months = _parse_bank(fpath)
                all_txns.extend(txns)
                source_files.append(fn)
            except Exception:
                pass

    out_path = os.path.join(BASE_DIR, "data", "generated", "bank_transactions.json")
    try:
        with open(out_path, "w") as f:
            json.dump({"metadata": {"source_files": source_files}, "transactions": all_txns}, f, indent=2)
    except Exception:
        pass

    BANK_TXNS = all_txns
    bank_statement_count = len(source_files)

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
    bank_owner_draw_total = sum(bank_by_cat.get(c, 0) for c in bank_by_cat if c.startswith("Owner Draw"))
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

    return {
        "transactions": len(BANK_TXNS),
        "statements": bank_statement_count,
        "net_cash": bank_net_cash,
    }


def _reload_inventory_data(new_order):
    global INVOICES, INV_DF, INV_ITEMS, BIZ_INV_DF, STOCK_SUMMARY
    global total_inventory_cost, total_inv_subtotal, total_inv_tax
    global biz_inv_cost, personal_acct_cost, inv_order_count, true_inventory_cost

    INVOICES.append(new_order)
    _RECENT_UPLOADS.add(new_order["order_num"])
    try:
        out_path = os.path.join(BASE_DIR, "data", "generated", "inventory_orders.json")
        with open(out_path, "w") as f:
            json.dump(INVOICES, f, indent=2)
    except Exception:
        pass
    _save_new_order(new_order)

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

    total_inventory_cost = INV_DF["grand_total"].sum()
    total_inv_subtotal = INV_DF["subtotal"].sum()
    total_inv_tax = INV_DF["tax"].sum()
    biz_inv_cost = INV_DF[INV_DF["source"] == "Key Component Mfg"]["grand_total"].sum()
    personal_acct_cost = INV_DF[INV_DF["source"] == "Personal Amazon"]["grand_total"].sum()
    inv_order_count = len(INV_DF)

    if len(INV_ITEMS) > 0:
        _pt = INV_ITEMS[INV_ITEMS["category"] == "Personal/Gift"]["total"].sum()
        _bf = INV_ITEMS[INV_ITEMS["category"] == "Business Fees"]["total"].sum()
        true_inventory_cost = total_inventory_cost - _pt - _bf
    else:
        true_inventory_cost = total_inventory_cost

    _pm = (INV_DF["source"] == "Personal Amazon") | INV_DF["file"].str.contains("Gigi", na=False)
    BIZ_INV_DF = INV_DF[~_pm].copy()
    _recompute_stock_summary()

    return {
        "order_num": new_order["order_num"],
        "item_count": len(new_order["items"]),
        "grand_total": new_order["grand_total"],
    }


def _cascade_reload(source="etsy"):
    global real_profit, real_profit_margin, bank_cash_on_hand, bank_all_expenses
    global full_profit, full_profit_margin, receipt_cogs_outside_bank
    global bank_amazon_inv

    bank_cash_on_hand = bank_net_cash + etsy_balance
    real_profit = bank_cash_on_hand + bank_owner_draw_total
    real_profit_margin = (real_profit / gross_sales * 100) if gross_sales else 0
    bank_amazon_inv = bank_by_cat.get("Amazon Inventory", 0)
    receipt_cogs_outside_bank = max(0, true_inventory_cost - bank_amazon_inv)
    full_profit = real_profit - receipt_cogs_outside_bank
    full_profit_margin = (full_profit / gross_sales * 100) if gross_sales else 0


def _validate_etsy_csv(decoded_bytes):
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
    etsy_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
    if not os.path.isdir(etsy_dir):
        return False, None, ""
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
        if fn == new_filename:
            return True, fn, f"Replacing existing file {fn}"
        try:
            existing_df = pd.read_csv(os.path.join(etsy_dir, fn))
            ex_dates = pd.to_datetime(existing_df["Date"], format="%B %d, %Y", errors="coerce")
            ex_min, ex_max = ex_dates.min(), ex_dates.max()
            if pd.isna(ex_min) or pd.isna(ex_max):
                continue
            if new_min <= ex_max and ex_min <= new_max:
                return True, fn, f"Date range overlaps with {fn} ({ex_min.strftime('%b %Y')}–{ex_max.strftime('%b %Y')})"
        except Exception:
            continue
    return False, None, ""


def _check_bank_pdf_duplicate(decoded_bytes, filename):
    import hashlib
    bank_dir = os.path.join(BASE_DIR, "data", "bank_statements")
    if not os.path.isdir(bank_dir):
        return False, None
    new_hash = hashlib.md5(decoded_bytes).hexdigest()
    for fn in os.listdir(bank_dir):
        if not fn.lower().endswith(".pdf"):
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
    for item in items:
        item["category"] = categorize_item(item.get("name", ""))
    return items
