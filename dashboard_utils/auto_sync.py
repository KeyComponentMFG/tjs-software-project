"""Automated incremental sync for Etsy order/label data.

Runs every 30 minutes in a background daemon thread:
- Fetches new receipts and ledger entries since last sync
- Matches outbound labels by exact-second timestamp
- Chains adjustments/insurance via reference_id
- Computes True Net from Payment API
- Flags unmatched labels and refunded orders for manual review
- NEVER touches manually overridden orders
"""
import json
import logging
import threading
import time
from datetime import datetime

_logger = logging.getLogger("dashboard.auto_sync")

_sync_lock = threading.Lock()
_sync_status = {
    "running": False,
    "last_run": None,
    "last_result": None,
    "new_orders": 0,
    "new_labels_matched": 0,
    "new_unmatched": 0,
    "needs_manual": 0,
    "error": None,
}


def get_sync_status():
    """Return current sync status for the notification bar."""
    return dict(_sync_status)


def run_incremental_sync():
    """Fetch new Etsy data and merge into existing orders."""
    if not _sync_lock.acquire(blocking=False):
        _logger.info("Sync already running, skipping")
        return {"skipped": True}

    _sync_status["running"] = True
    try:
        return _do_sync()
    except Exception as e:
        _sync_status["error"] = str(e)
        _logger.error("Auto-sync failed: %s", e, exc_info=True)
        return {"error": str(e)}
    finally:
        _sync_status["running"] = False
        _sync_status["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _sync_lock.release()


def _do_sync():
    from supabase_loader import get_config_value, _get_supabase_client
    from dashboard_utils.etsy_api import (
        is_connected, _load_tokens, _tokens,
        get_all_receipts, get_ledger_entries_since,
        get_receipt_payments, _fetch_all_payments,
    )

    _load_tokens()
    if not is_connected():
        _sync_status["error"] = "Not connected to Etsy API"
        return {"error": "Not connected"}

    shop_id = _tokens.get("shop_id")
    if not shop_id:
        _sync_status["error"] = "No shop_id"
        return {"error": "No shop_id"}

    # Load last sync timestamp (default: 24 hours ago)
    last_ts_raw = get_config_value("auto_sync_last_timestamp")
    if last_ts_raw:
        last_ts = int(last_ts_raw) if not isinstance(last_ts_raw, int) else last_ts_raw
    else:
        last_ts = int(time.time()) - 86400

    _logger.info("Incremental sync since %s", datetime.fromtimestamp(last_ts).strftime("%Y-%m-%d %H:%M"))

    # Fetch new data
    new_receipts = get_all_receipts(shop_id, min_created=last_ts)
    new_ledger = get_ledger_entries_since(shop_id, last_ts)

    if not new_receipts and not new_ledger:
        _logger.info("Nothing new since last sync")
        _save_timestamp(int(time.time()))
        _sync_status["error"] = None
        _sync_status["last_result"] = "No new data"
        return {"new_orders": 0, "new_entries": 0}

    _logger.info("New receipts: %d, new ledger entries: %d", len(new_receipts), len(new_ledger))

    # Load existing data
    raw_orders = get_config_value("order_profit_ledger_keycomponentmfg")
    orders = json.loads(raw_orders) if isinstance(raw_orders, str) else (raw_orders or [])
    existing_ids = {str(o["Order ID"]): o for o in orders}

    raw_labels = get_config_value("unmatched_shipping_labels")
    labels = json.loads(raw_labels) if isinstance(raw_labels, str) else (raw_labels or [])
    existing_label_ids = {l["label_id"] for l in labels}

    # === Process new receipts into orders ===
    new_order_count = 0
    new_receipt_ids = []

    for r in new_receipts:
        rid = str(r.get("receipt_id", ""))
        if not rid or rid in existing_ids:
            continue

        new_receipt_ids.append(rid)

        # Parse receipt into order format
        def _amt(field):
            v = r.get(field, {})
            if isinstance(v, dict):
                return v.get("amount", 0) / v.get("divisor", 100)
            return float(v) if v else 0

        total_price = _amt("total_price")
        shipping_cost = _amt("total_shipping_cost")
        discount = _amt("discount_amt")
        tax = _amt("total_tax_cost")

        # Shipment info
        shipments = r.get("shipments", [])
        tracking = ""
        if shipments:
            tracking = shipments[0].get("tracking_code", "")

        # Buyer info
        buyer = r.get("name", r.get("buyer_email", ""))

        # Items
        items = r.get("transactions", [])
        item_names = " | ".join(t.get("title", "")[:40] for t in items)
        total_qty = sum(t.get("quantity", 1) for t in items)
        variations = []
        for t in items:
            for v in (t.get("variations", []) or []):
                val = v.get("formatted_value", v.get("value", ""))
                if val:
                    variations.append(val)

        sale_price = total_price - discount
        created = r.get("created_timestamp", 0)
        sale_date = datetime.fromtimestamp(created).strftime("%m/%d/%Y") if created else ""

        # Ship info
        ship_country = ""
        ship_state = ""
        addr = r.get("formatted_address", {})
        if isinstance(addr, dict):
            ship_country = addr.get("country_iso", "")
            ship_state = addr.get("state", "")

        is_refunded = r.get("status", "") in ("refunded", "Refunded")
        is_canceled = r.get("status", "") in ("canceled", "Canceled")

        order = {
            "Order ID": rid,
            "Sale Date": sale_date,
            "Buyer": buyer,
            "Qty": total_qty,
            "Item Names": item_names[:60],
            "Variations": " / ".join(variations),
            "Listing Price": round(total_price, 2),
            "Discount": round(discount, 2),
            "Sale Price": round(sale_price, 2),
            "Buyer Shipping": round(shipping_cost, 2),
            "Sales Tax": round(tax, 2),
            "Transaction Fee": 0,
            "Processing Fee": 0,
            "Listing Fee": 0,
            "Offsite Ads": 0,
            "Total Etsy Fees": 0,
            "Shipping Label": 0,
            "Ship P/L": round(shipping_cost, 2),
            "Refund": 0,
            "True Net": 0,
            "Fee %": 0,
            "Margin %": 0,
            "Status": "Refunded" if is_refunded else ("Canceled" if is_canceled else "Completed"),
            "Ship State": ship_state,
            "Ship Country": ship_country,
            "Tracking": tracking,
            "Label ID": "",
            "_store": "keycomponentmfg",
            "_payment_verified": False,
            "_needs_manual_net": is_refunded or is_canceled,
        }

        orders.append(order)
        existing_ids[rid] = order
        new_order_count += 1

    _logger.info("Added %d new orders", new_order_count)

    # === Process new ledger entries ===
    # Build ship_ts_to_receipt for label matching (all receipts, not just new)
    ship_ts_to_receipt = {}
    for r_existing in (new_receipts + []):
        rid = str(r_existing.get("receipt_id", ""))
        for t in r_existing.get("transactions", []):
            shipped_ts = t.get("shipped_timestamp")
            if shipped_ts and isinstance(shipped_ts, (int, float)) and shipped_ts > 0:
                ship_ts_to_receipt[int(shipped_ts)] = rid

    # Also include timestamps from existing orders (for matching labels to older orders)
    # We need the full receipt data for this — use existing orders' Label IDs as fallback
    _label_id_to_order = {}
    for o in orders:
        lid = str(o.get("Label ID", ""))
        if lid:
            _label_id_to_order[lid] = str(o["Order ID"])

    # Build lookup from new outbound labels
    new_labels_matched = 0
    new_unmatched = 0

    # Pass 1: match new outbound shipping_labels
    for entry in new_ledger:
        if entry.get("reference_type") != "shipping_label":
            continue
        if entry.get("ledger_type") != "shipping_labels":
            continue
        ref_id = str(entry.get("reference_id", ""))
        ts = entry.get("created_timestamp", 0)
        amount = abs(entry.get("amount", 0)) / 100.0

        # Skip if already tracked
        if ref_id in existing_label_ids or ref_id in _label_id_to_order:
            continue

        # Exact-second timestamp match
        order_id = ship_ts_to_receipt.get(ts)
        if not order_id:
            for delta in (1, -1):
                order_id = ship_ts_to_receipt.get(ts + delta)
                if order_id:
                    break

        if order_id and order_id in existing_ids:
            o = existing_ids[order_id]
            if not o.get("_manual_override"):
                o["Shipping Label"] = round(o.get("Shipping Label", 0) + amount, 2)
                o["Ship P/L"] = round(o.get("Buyer Shipping", 0) - o["Shipping Label"], 2)
                o["Label ID"] = ref_id
                _label_id_to_order[ref_id] = order_id
                new_labels_matched += 1
        else:
            # Unmatched outbound label
            label_entry = {
                "date": datetime.fromtimestamp(ts).strftime("%m/%d/%Y"),
                "timestamp": ts,
                "amount": amount,
                "type": "shipping_labels",
                "label_id": ref_id,
                "assigned_to": None,
            }
            labels.append(label_entry)
            existing_label_ids.add(ref_id)
            new_unmatched += 1

    # Pass 2: match new adjustments/insurance/returns via _label_id_to_order
    for entry in new_ledger:
        if entry.get("reference_type") != "shipping_label":
            continue
        lt = entry.get("ledger_type", "")
        if lt == "shipping_labels":
            continue  # already handled in Pass 1

        ref_id = str(entry.get("reference_id", ""))
        ts = entry.get("created_timestamp", 0)
        amount = abs(entry.get("amount", 0)) / 100.0

        if ref_id in existing_label_ids:
            continue

        is_credit = lt in ("shipping_label_refund", "shipping_label_usps_adjustment_credit")

        if lt in ("shipping_label_usps_adjustment", "shipping_label_insurance",
                   "shipping_label_globegistics_adjustment", "shipping_labels_usps_return"):
            # Try to chain via _label_id_to_order
            order_id = _label_id_to_order.get(ref_id)
            if order_id and order_id in existing_ids:
                o = existing_ids[order_id]
                if not o.get("_manual_override"):
                    o["Shipping Label"] = round(o.get("Shipping Label", 0) + amount, 2)
                    o["Ship P/L"] = round(o.get("Buyer Shipping", 0) - o["Shipping Label"], 2)
                    new_labels_matched += 1
                    continue

        # Unmatched — add to pool for manual assignment
        label_entry = {
            "date": datetime.fromtimestamp(ts).strftime("%m/%d/%Y"),
            "timestamp": ts,
            "amount": amount,
            "type": lt,
            "label_id": ref_id,
            "assigned_to": None,
        }
        labels.append(label_entry)
        existing_label_ids.add(ref_id)
        new_unmatched += 1

    # === Process new fee entries ===
    # Build txn_id -> receipt_id from new receipts
    txn_to_receipt = {}
    for r in new_receipts:
        rid = str(r.get("receipt_id", ""))
        for t in r.get("transactions", []):
            txn_to_receipt[str(t.get("transaction_id", ""))] = rid

    for entry in new_ledger:
        lt = entry.get("ledger_type", "")
        rt = entry.get("reference_type", "")
        ref_id = str(entry.get("reference_id", ""))
        amount = abs(entry.get("amount", 0)) / 100.0

        order_id = None
        if rt == "receipt":
            order_id = ref_id
        elif rt == "etsy":
            order_id = ref_id
        elif rt == "transaction":
            order_id = txn_to_receipt.get(ref_id)

        if not order_id or order_id not in existing_ids:
            continue

        o = existing_ids[order_id]
        if o.get("_manual_override") or o.get("_payment_verified"):
            continue

        if lt in ("transaction", "shipping_transaction"):
            o["Transaction Fee"] = round(o.get("Transaction Fee", 0) + amount, 2)
        elif lt == "transaction_quantity":
            o["Listing Fee"] = round(o.get("Listing Fee", 0) + amount, 2)
        elif lt == "offsite_ads_fee":
            o["Offsite Ads"] = round(o.get("Offsite Ads", 0) + amount, 2)

    # === Fetch payment data for new orders and compute True Net ===
    if new_receipt_ids:
        _logger.info("Fetching payment data for %d new orders", len(new_receipt_ids))
        for rid in new_receipt_ids:
            o = existing_ids.get(rid)
            if not o or o.get("_manual_override"):
                continue

            try:
                pmt_data = get_receipt_payments(shop_id, int(rid))
                if pmt_data and pmt_data.get("results"):
                    pmt = pmt_data["results"][0]
                    an = pmt.get("amount_net", {})
                    api_net = an.get("amount", 0) / an.get("divisor", 100) if isinstance(an, dict) else 0
                    af = pmt.get("amount_fees", {})
                    proc_fee = abs(af.get("amount", 0) / af.get("divisor", 100)) if isinstance(af, dict) else 0

                    o["Processing Fee"] = round(proc_fee, 2)
                    o["Total Etsy Fees"] = round(o["Transaction Fee"] + o["Offsite Ads"] + proc_fee, 2)

                    # Check for refund
                    refund = 0
                    for adj in pmt.get("payment_adjustments", []):
                        adj_amt = adj.get("total_adjustment_amount", 0)
                        if adj_amt:
                            refund += adj_amt / 100.0
                    o["Refund"] = round(refund, 2)

                    if refund > 0:
                        o["Status"] = "Refunded"
                        o["_needs_manual_net"] = True
                    else:
                        # True Net = amount_net - txn - ads - label
                        true_net = api_net - o["Transaction Fee"] - o["Offsite Ads"] - o["Shipping Label"]
                        o["True Net"] = round(true_net, 2)
                        o["Margin %"] = round(true_net / o["Sale Price"] * 100, 1) if o.get("Sale Price") else 0
                        o["_payment_verified"] = True
                        o["_needs_manual_net"] = False

                time.sleep(0.25)
            except Exception as e:
                _logger.warning("Payment fetch failed for %s: %s", rid, e)

    # === Count items needing attention ===
    unmatched_count = len([l for l in labels if not l.get("assigned_to")])
    needs_manual = len([o for o in orders if o.get("_needs_manual_net")])

    # === Save atomically ===
    client = _get_supabase_client()
    client.table("config").upsert({
        "key": "order_profit_ledger_keycomponentmfg",
        "value": json.dumps(orders),
    }, on_conflict="key").execute()
    client.table("config").upsert({
        "key": "unmatched_shipping_labels",
        "value": json.dumps(labels),
    }, on_conflict="key").execute()
    _save_timestamp(int(time.time()))

    # Update status
    _sync_status.update({
        "error": None,
        "new_orders": new_order_count,
        "new_labels_matched": new_labels_matched,
        "new_unmatched": new_unmatched,
        "needs_manual": needs_manual,
        "last_result": f"{new_order_count} orders, {new_labels_matched} labels matched, {new_unmatched} unmatched",
    })

    _logger.info("Sync complete: %d new orders, %d labels matched, %d unmatched, %d need manual",
                 new_order_count, new_labels_matched, new_unmatched, needs_manual)

    return {
        "new_orders": new_order_count,
        "new_labels_matched": new_labels_matched,
        "new_unmatched": new_unmatched,
        "needs_manual": needs_manual,
        "total_orders": len(orders),
    }


def _save_timestamp(ts):
    from supabase_loader import _get_supabase_client
    client = _get_supabase_client()
    client.table("config").upsert({
        "key": "auto_sync_last_timestamp",
        "value": str(ts),
    }, on_conflict="key").execute()


def run_full_audit(shop_id):
    """Re-verify all non-manual orders against Payment API. Returns pass/fail counts."""
    from supabase_loader import get_config_value
    from dashboard_utils.etsy_api import get_receipt_payments

    raw_orders = get_config_value("order_profit_ledger_keycomponentmfg")
    orders = json.loads(raw_orders) if isinstance(raw_orders, str) else raw_orders

    normal = [o for o in orders if not o.get("_manual_override")]
    passed = 0
    failed = 0
    failures = []
    errors = 0
    batch = 0

    for o in normal:
        try:
            data = get_receipt_payments(shop_id, o["Order ID"])
            if not data or not data.get("results"):
                errors += 1
                continue

            p = data["results"][0]
            an = p.get("amount_net", {})
            api_net = an.get("amount", 0) / an.get("divisor", 100) if isinstance(an, dict) else 0

            expected = round(api_net - o.get("Transaction Fee", 0) - o.get("Offsite Ads", 0) - o.get("Shipping Label", 0), 2)
            actual = o["True Net"]

            if abs(expected - actual) > 0.02:
                failed += 1
                failures.append({
                    "order_id": o["Order ID"],
                    "buyer": o.get("Buyer", ""),
                    "expected": expected,
                    "actual": actual,
                    "diff": round(actual - expected, 2),
                })
            else:
                passed += 1

            batch += 1
            if batch % 4 == 0:
                time.sleep(0.25)
        except Exception:
            errors += 1

    return {
        "total": len(normal),
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "failures": failures,
    }


def start_sync_loop(interval_seconds=1800):
    """Start the background sync loop. Runs in a daemon thread."""
    def _loop():
        _logger.info("Auto-sync loop started (every %d seconds)", interval_seconds)
        while True:
            time.sleep(interval_seconds)
            try:
                run_incremental_sync()
            except Exception as e:
                _logger.error("Sync loop error: %s", e)

    thread = threading.Thread(target=_loop, daemon=True, name="etsy-auto-sync")
    thread.start()
    return thread
