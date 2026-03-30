"""
csv_order_builder.py — Build per-order profit ledgers from Etsy Shop Manager CSV data.

For stores without API access (Aurvio, Luna&Links), this module converts
uploaded CSV order/item data + statement fee/shipping entries into the same
order profit ledger format used by the API-based KeyComp store.

Usage:
    from dashboard_utils.csv_order_builder import build_order_profit_from_csv
    result = build_order_profit_from_csv("aurvio")
"""

import json
import logging
import re
from datetime import datetime, timedelta

from supabase_loader import get_config_value, save_config_value, load_data

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _parse_date(date_str):
    """Parse MM/DD/YY or MM/DD/YYYY date strings into datetime."""
    if not date_str:
        return None
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, TypeError):
            continue
    return None


def _parse_statement_date(date_str):
    """Parse statement-style dates like 'February 25, 2026' into datetime."""
    if not date_str:
        return None
    try:
        return datetime.strptime(str(date_str).strip(), "%B %d, %Y")
    except (ValueError, TypeError):
        return None


def _normalize_date(date_str):
    """Convert any date format to MM/DD/YYYY for output consistency."""
    dt = _parse_date(date_str)
    if dt:
        return dt.strftime("%m/%d/%Y")
    return date_str or ""


# ---------------------------------------------------------------------------
# Fee extraction from statements
# ---------------------------------------------------------------------------

def extract_fees_from_statements(store_slug, order_ids):
    """Extract per-order fees from Etsy statement data.

    Scans Fee-type entries in statement DATA for the given store, matching
    fees to orders by extracting order numbers from the Info field
    (e.g. "Order #3985989382").

    Args:
        store_slug: Store identifier (e.g. "aurvio")
        order_ids: Set/list of order ID strings to match against

    Returns:
        dict: order_id -> {
            "transaction_fee": float (positive),
            "processing_fee": float (positive),
            "offsite_ads": float (positive),
            "listing_fee": float (positive),
        }
    """
    d = load_data()
    DATA = d["DATA"]

    order_ids_set = set(str(oid) for oid in order_ids)
    fees = DATA[(DATA["Type"] == "Fee") & (DATA["Store"] == store_slug)]

    result = {}

    for _, row in fees.iterrows():
        info = str(row.get("Info", ""))
        m = re.search(r"Order #(\d+)", info)
        if not m:
            continue

        order_id = m.group(1)
        if order_id not in order_ids_set:
            continue

        if order_id not in result:
            result[order_id] = {
                "transaction_fee": 0.0,
                "processing_fee": 0.0,
                "offsite_ads": 0.0,
                "listing_fee": 0.0,
            }

        title = str(row.get("Title", "")).lower()
        amount = abs(float(row.get("Net_Clean", 0) or 0))

        if "transaction fee" in title:
            result[order_id]["transaction_fee"] += amount
        elif "processing fee" in title:
            result[order_id]["processing_fee"] += amount
        elif "offsite" in title:
            result[order_id]["offsite_ads"] += amount
        elif "listing fee" in title:
            result[order_id]["listing_fee"] += amount
        # Credits (refunds of fees) — subtract back
        elif "credit for transaction" in title:
            result[order_id]["transaction_fee"] -= amount
        elif "credit for processing" in title:
            result[order_id]["processing_fee"] -= amount

    return result


# ---------------------------------------------------------------------------
# Shipping label matching from statements
# ---------------------------------------------------------------------------

def match_labels_from_statements(store_slug, order_data):
    """Match shipping label entries from statements to orders by ship date.

    Strategy:
    - Shipping entries in statements have Label #NNN and a date.
    - Orders from CSVs have a Date Shipped field.
    - Match labels to orders where the label purchase date is within
      a +/-2 day window of the order's ship date.
    - When multiple labels fall near the same ship date, use cost proximity
      to buyer-paid shipping to pick the best match.

    Args:
        store_slug: Store identifier
        order_data: List of dicts with order info including ship dates.
                    Each must have "Order ID" and "Date Shipped".

    Returns:
        tuple: (matched, unmatched)
            matched: dict of order_id -> {
                "label_cost": float (positive),
                "label_id": str,
                "carrier": str,
            }
            unmatched: list of dicts with label info not matched to any order
    """
    d = load_data()
    DATA = d["DATA"]

    ship_entries = DATA[
        (DATA["Type"] == "Shipping") & (DATA["Store"] == store_slug)
    ]

    # Build list of available labels
    labels = []
    for _, row in ship_entries.iterrows():
        info = str(row.get("Info", ""))
        title = str(row.get("Title", ""))

        # Extract label/adjustment ID
        label_match = re.search(r"(?:Label|Adjustment) #(\d+)", info)
        label_id = label_match.group(1) if label_match else ""

        label_date = _parse_statement_date(row.get("Date", ""))
        cost = abs(float(row.get("Net_Clean", 0) or 0))

        # Determine carrier from title
        carrier = "USPS"
        if "asendia" in title.lower():
            carrier = "Asendia"
        elif "globegistics" in title.lower():
            carrier = "Globegistics"
        elif "ups" in title.lower():
            carrier = "UPS"
        elif "fedex" in title.lower():
            carrier = "FedEx"

        # Adjustments are tied to existing labels, not new shipments
        is_adjustment = "adjustment" in title.lower()

        labels.append({
            "label_id": label_id,
            "date": label_date,
            "cost": cost,
            "carrier": carrier,
            "title": title,
            "is_adjustment": is_adjustment,
        })

    # Build order ship date index
    orders_by_ship_date = []
    for order in order_data:
        ship_date = _parse_date(order.get("Date Shipped"))
        if not ship_date:
            continue
        orders_by_ship_date.append({
            "order_id": str(order["Order ID"]),
            "ship_date": ship_date,
            "buyer_shipping": float(order.get("Shipping", 0) or 0),
        })

    # First pass: match adjustments to their parent labels by label_id
    adjustment_map = {}  # label_id -> list of adjustment costs
    non_adjustment_labels = []
    for lbl in labels:
        if lbl["is_adjustment"]:
            parent_id = lbl["label_id"]
            if parent_id not in adjustment_map:
                adjustment_map[parent_id] = []
            adjustment_map[parent_id].append(lbl["cost"])
        else:
            non_adjustment_labels.append(lbl)

    # Second pass: match non-adjustment labels to orders by date proximity
    matched = {}
    used_labels = set()

    for order_info in orders_by_ship_date:
        order_id = order_info["order_id"]
        ship_date = order_info["ship_date"]

        if order_id in matched:
            continue

        # Find labels within +/-2 days of ship date
        candidates = []
        for idx, lbl in enumerate(non_adjustment_labels):
            if idx in used_labels:
                continue
            if lbl["date"] is None:
                continue
            delta = abs((lbl["date"] - ship_date).days)
            if delta <= 2:
                candidates.append((delta, idx, lbl))

        if not candidates:
            continue

        # Sort by date proximity first, then by cost
        candidates.sort(key=lambda x: (x[0], x[2]["cost"]))
        best_delta, best_idx, best_label = candidates[0]

        total_cost = best_label["cost"]
        # Add any adjustments tied to this label
        if best_label["label_id"] in adjustment_map:
            total_cost += sum(adjustment_map[best_label["label_id"]])

        matched[order_id] = {
            "label_cost": round(total_cost, 2),
            "label_id": best_label["label_id"],
            "carrier": best_label["carrier"],
        }
        used_labels.add(best_idx)

    # Collect unmatched labels
    unmatched = []
    for idx, lbl in enumerate(non_adjustment_labels):
        if idx not in used_labels:
            unmatched.append({
                "label_id": lbl["label_id"],
                "date": lbl["date"].strftime("%m/%d/%Y") if lbl["date"] else "",
                "cost": lbl["cost"],
                "carrier": lbl["carrier"],
                "title": lbl["title"],
                "_store": store_slug,
            })

    return matched, unmatched


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_order_profit_from_csv(store_slug):
    """Build a per-order profit ledger from CSV data for a non-API store.

    Loads order and item CSVs from Supabase, extracts fees from statement
    data, matches shipping labels by date proximity, and produces a ledger
    in the exact same format as the KeyComp API-based ledger.

    Args:
        store_slug: Store identifier (e.g. "aurvio", "lunalinks")

    Returns:
        list: Order profit dicts in the standard ledger format, or empty
              list if no CSV data is available.
    """
    # Load order CSV data
    raw_orders = get_config_value(f"order_csv_orders_{store_slug}")
    if not raw_orders:
        _logger.warning("No order CSV data found for store: %s", store_slug)
        return []

    orders = json.loads(raw_orders) if isinstance(raw_orders, str) else raw_orders

    # Load items CSV data
    raw_items = get_config_value(f"order_csv_items_{store_slug}")
    items = json.loads(raw_items) if raw_items and isinstance(raw_items, str) else (raw_items or [])

    # Build items lookup: order_id -> list of items
    items_by_order = {}
    for item in items:
        oid = str(item.get("Order ID", ""))
        if oid:
            items_by_order.setdefault(oid, []).append(item)

    # Get all order IDs
    order_ids = [str(o.get("Order ID", "")) for o in orders]

    # Extract fees from statements
    _logger.info("Extracting fees from statements for %s (%d orders)...",
                 store_slug, len(orders))
    fee_map = extract_fees_from_statements(store_slug, order_ids)
    _logger.info("Found statement fees for %d/%d orders", len(fee_map), len(orders))

    # Match shipping labels from statements
    # Build order data with ship dates from items (items have Date Shipped)
    order_ship_data = []
    for order in orders:
        oid = str(order.get("Order ID", ""))
        # Get ship date from items (more reliable, has full date format)
        order_items = items_by_order.get(oid, [])
        ship_date = None
        for it in order_items:
            sd = it.get("Date Shipped")
            if sd:
                ship_date = sd
                break
        order_ship_data.append({
            "Order ID": oid,
            "Date Shipped": ship_date,
            "Shipping": order.get("Shipping", 0),
        })

    label_map, unmatched_labels = match_labels_from_statements(store_slug, order_ship_data)
    _logger.info("Label matching for %s: %d matched, %d unmatched",
                 store_slug, len(label_map), len(unmatched_labels))

    # Build the ledger
    result_orders = []

    for order in orders:
        oid = str(order.get("Order ID", ""))
        order_items = items_by_order.get(oid, [])

        # Item names and variations from items CSV
        item_names = " | ".join(
            it.get("Item Name", "")[:60] for it in order_items
        ) if order_items else ""

        variations_list = []
        total_qty = 0
        for it in order_items:
            total_qty += it.get("Quantity", 1) or 1
            var = it.get("Variations", "")
            if var:
                for v in str(var).split(", "):
                    if ": " in v:
                        _, val = v.split(": ", 1)
                        variations_list.append(val)
                    elif v.strip():
                        variations_list.append(v.strip())

        var_str = " / ".join(variations_list) if variations_list else ""

        # If no items data, fall back to order-level qty
        if total_qty == 0:
            total_qty = order.get("Number of Items", 1) or 1

        # Prices from order CSV
        listing_price = float(order.get("Order Value", 0) or 0)
        discount = float(order.get("Discount Amount", 0) or 0)
        sale_price = round(listing_price - discount, 2)
        buyer_shipping = float(order.get("Shipping", 0) or 0)
        sales_tax = float(order.get("Sales Tax", 0) or 0)

        # Fees from statement matching
        fees = fee_map.get(oid, {})
        transaction_fee = round(fees.get("transaction_fee", 0), 2)
        processing_fee = round(fees.get("processing_fee", 0), 2)
        offsite_ads = round(fees.get("offsite_ads", 0), 2)
        listing_fee = round(fees.get("listing_fee", 0), 2)

        # If no statement fees found, fall back to CSV processing fee
        if processing_fee == 0 and order.get("Card Processing Fees"):
            processing_fee = round(abs(float(order["Card Processing Fees"])), 2)

        total_etsy_fees = round(transaction_fee + processing_fee + offsite_ads, 2)

        # Shipping label from statement matching
        label_info = label_map.get(oid, {})
        label_cost = label_info.get("label_cost", 0)
        label_id = label_info.get("label_id", "")
        ship_pl = round(buyer_shipping - label_cost, 2)

        # Refund — check order status or CSV fields
        refund = 0  # CSV orders don't have explicit refund amounts yet

        # True Net = Sale Price + Buyer Shipping - Total Fees - Shipping Label
        true_net = round(
            sale_price + buyer_shipping
            - total_etsy_fees
            - label_cost
            - refund,
            2,
        )

        # Fee % and Margin %
        gross = sale_price + buyer_shipping
        fee_pct = round(total_etsy_fees / gross * 100, 1) if gross else 0
        margin_pct = round(true_net / sale_price * 100, 1) if sale_price else 0

        # Normalize country names
        country = order.get("Ship Country", "")
        if country == "United States":
            country = "US"

        # Normalize sale date to MM/DD/YYYY
        sale_date = _normalize_date(order.get("Sale Date", ""))

        # Determine status
        status = order.get("Status") or ""
        if not status:
            # Infer from ship date
            order_items_list = items_by_order.get(oid, [])
            has_shipped = any(it.get("Date Shipped") for it in order_items_list)
            status = "Completed" if has_shipped else "Open"

        result_orders.append({
            "Order ID": oid,
            "Sale Date": sale_date,
            "Buyer": order.get("Buyer", ""),
            "Qty": total_qty,
            "Item Names": item_names[:60] if item_names else "",
            "Variations": var_str,
            "Listing Price": round(listing_price, 2),
            "Discount": round(discount, 2),
            "Sale Price": round(sale_price, 2),
            "Buyer Shipping": round(buyer_shipping, 2),
            "Sales Tax": round(sales_tax, 2),
            "Gross": round(gross, 2),
            "Transaction Fee": transaction_fee,
            "Processing Fee": processing_fee,
            "Listing Fee": listing_fee,
            "Offsite Ads": offsite_ads,
            "Total Etsy Fees": total_etsy_fees,
            "Shipping Label": round(label_cost, 2),
            "Ship P/L": ship_pl,
            "Refund": refund,
            "True Net": true_net,
            "Fee %": fee_pct,
            "Margin %": margin_pct,
            "Status": status,
            "Ship State": order.get("Ship State", ""),
            "Ship Country": country,
            "Tracking": "",
            "_store": store_slug,
            "_payment_verified": False,
            "_data_source": "csv",
            "Label ID": label_id,
        })

    # Sort by sale date descending
    result_orders.sort(
        key=lambda x: _parse_date(x.get("Sale Date", "")) or datetime.min,
        reverse=True,
    )

    # Save to Supabase
    ledger_key = f"order_profit_ledger_{store_slug}"
    save_config_value(ledger_key, json.dumps(result_orders))
    _logger.info("Saved %d orders to %s", len(result_orders), ledger_key)

    # Save unmatched labels
    unmatched_key = f"unmatched_shipping_labels_{store_slug}"
    save_config_value(unmatched_key, json.dumps(unmatched_labels))
    _logger.info("Saved %d unmatched labels to %s",
                 len(unmatched_labels), unmatched_key)

    return result_orders
