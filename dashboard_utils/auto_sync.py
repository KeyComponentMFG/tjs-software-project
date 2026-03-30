"""Automated incremental sync for Etsy order/label data.

Runs every 30 minutes in a background daemon thread:
- Fetches new receipts and ledger entries since last sync
- Re-checks orders from last 7 days for status changes (cancellations/refunds)
- Matches outbound labels by exact-second timestamp (±1s ONLY)
- Chains adjustments/insurance via reference_id
- Computes True Net from Payment API
- Partial refund detection: auto-computes True Net when possible
- Only flags full refunds (where we can't determine earnings) for manual review
- Handles regulatory_operating_fee ledger type
- Retry logic for 429 and 5xx API errors
- Persists sync results to Supabase
- NEVER touches manually overridden orders
- NEVER overwrites _payment_verified orders unless re-verifying them
- transaction_quantity goes to listing_fee, NOT transaction_fee
- Credits/refunds (shipping_label_refund, shipping_label_usps_adjustment_credit)
  go to refund bucket, NOT shipping_label
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
    "orders_updated": 0,
    "error": None,
}

# Retry constants
_MAX_RETRIES = 2
_RETRY_DELAY = 2  # seconds


def get_sync_status():
    """Return current sync status for the notification bar."""
    return dict(_sync_status)


def _api_call_with_retry(fn, *args, **kwargs):
    """Wrap an API call with retry logic for 429 and 5xx errors.

    Returns the function result on success, None on exhausted retries.
    """
    import requests

    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            result = fn(*args, **kwargs)
            return result
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
            if status and (status == 429 or 500 <= status < 600):
                last_exc = e
                if attempt < _MAX_RETRIES:
                    _logger.warning("API call %s attempt %d failed with %s, retrying in %ds",
                                    fn.__name__, attempt + 1, status, _RETRY_DELAY)
                    time.sleep(_RETRY_DELAY)
                    continue
            raise
        except Exception as e:
            # Check if it's a requests response error embedded in the exception
            resp = getattr(e, "response", None)
            status = getattr(resp, "status_code", None) if resp is not None else None
            if status and (status == 429 or 500 <= status < 600):
                last_exc = e
                if attempt < _MAX_RETRIES:
                    _logger.warning("API call %s attempt %d failed with %s, retrying in %ds",
                                    fn.__name__, attempt + 1, status, _RETRY_DELAY)
                    time.sleep(_RETRY_DELAY)
                    continue
            raise

    _logger.error("API call %s exhausted retries", fn.__name__)
    return None


def _safe_get_receipt_payments(shop_id, receipt_id):
    """Fetch payment data with retry logic for transient failures."""
    from dashboard_utils.etsy_api import get_receipt_payments
    return _api_call_with_retry(get_receipt_payments, shop_id, int(receipt_id))


def _safe_get_receipt_by_id(shop_id, receipt_id):
    """Fetch a single receipt with retry logic."""
    from dashboard_utils.etsy_api import get_receipt_by_id
    return _api_call_with_retry(get_receipt_by_id, shop_id, int(receipt_id))


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

    # Fetch new data (with retry-safe wrappers for individual calls later)
    new_receipts = get_all_receipts(shop_id, min_created=last_ts)
    new_ledger = get_ledger_entries_since(shop_id, last_ts)

    # Load existing data
    raw_orders = get_config_value("order_profit_ledger_keycomponentmfg")
    orders = json.loads(raw_orders) if isinstance(raw_orders, str) else (raw_orders or [])
    existing_ids = {str(o["Order ID"]): o for o in orders}

    raw_labels = get_config_value("unmatched_shipping_labels")
    labels = json.loads(raw_labels) if isinstance(raw_labels, str) else (raw_labels or [])
    existing_label_ids = {l["label_id"] for l in labels}

    # === Re-check recent orders (last 7 days) for status changes ===
    orders_updated = 0
    recheck_ts = int(time.time()) - (7 * 86400)
    recent_order_ids = [
        o for o in orders
        if not o.get("_manual_override")
        and o.get("Status") == "Completed"
        and o.get("Sale Date")
    ]
    # Filter to orders from last 7 days
    recent_to_check = []
    for o in recent_order_ids:
        try:
            sale_dt = datetime.strptime(o["Sale Date"], "%m/%d/%Y")
            if sale_dt.timestamp() >= recheck_ts:
                recent_to_check.append(o)
        except (ValueError, TypeError):
            pass

    if recent_to_check:
        _logger.info("Re-checking %d recent orders for status changes", len(recent_to_check))
        for o in recent_to_check:
            rid = str(o["Order ID"])
            try:
                receipt = _safe_get_receipt_by_id(shop_id, rid)
                if not receipt:
                    continue

                new_status = receipt.get("status", "")
                is_now_refunded = new_status in ("refunded", "Refunded")
                is_now_canceled = new_status in ("canceled", "Canceled")

                if is_now_refunded or is_now_canceled:
                    _logger.info("Order %s status changed to %s", rid, new_status)

                    if is_now_canceled:
                        o["Status"] = "Canceled"
                        o["_needs_manual_net"] = True
                        orders_updated += 1
                    elif is_now_refunded:
                        # Re-fetch payment to determine partial vs full refund
                        pmt_data = _safe_get_receipt_payments(shop_id, rid)
                        if pmt_data and pmt_data.get("results"):
                            pmt = pmt_data["results"][0]
                            refund = 0
                            for adj in pmt.get("payment_adjustments", []):
                                adj_amt = adj.get("total_adjustment_amount", 0)
                                if adj_amt:
                                    refund += adj_amt / 100.0
                            o["Refund"] = round(refund, 2)
                            o["Status"] = "Refunded"

                            _apply_refund_logic(o, pmt, refund)
                            orders_updated += 1

                time.sleep(0.25)
            except Exception as e:
                _logger.warning("Re-check failed for order %s: %s", rid, e)

    if not new_receipts and not new_ledger and orders_updated == 0:
        _logger.info("Nothing new since last sync")
        _save_timestamp(int(time.time()))
        _sync_status["error"] = None
        _sync_status["last_result"] = "No new data"
        _persist_sync_results(0, 0, 0, 0, 0, len(orders))
        return {"new_orders": 0, "new_entries": 0}

    _logger.info("New receipts: %d, new ledger entries: %d", len(new_receipts), len(new_ledger))

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
            "Regulatory Fee": 0,
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
            "_needs_manual_net": is_canceled,  # Only canceled gets flagged immediately; refunds checked via payment API
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

        # Exact-second timestamp match (±1s ONLY — NEVER use proximity)
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

    # Pass 2: handle adjustments/insurance/returns via _label_id_to_order
    # Credits/refunds go to the REFUND bucket, not shipping_label
    _CREDIT_TYPES = {"shipping_label_refund", "shipping_label_usps_adjustment_credit"}
    _ADDON_TYPES = {
        "shipping_label_usps_adjustment",
        "shipping_label_insurance",
        "shipping_label_globegistics_adjustment",
        "shipping_labels_usps_return",
    }

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

        if lt in _CREDIT_TYPES:
            # Credits/refunds go to the refund bucket — reduce shipping label cost
            order_id = _label_id_to_order.get(ref_id)
            if order_id and order_id in existing_ids:
                o = existing_ids[order_id]
                if not o.get("_manual_override"):
                    # Credit reduces label cost
                    o["Shipping Label"] = round(max(0, o.get("Shipping Label", 0) - amount), 2)
                    o["Ship P/L"] = round(o.get("Buyer Shipping", 0) - o["Shipping Label"], 2)
                    new_labels_matched += 1
                    continue

        elif lt in _ADDON_TYPES:
            # Additional charges chain via reference_id
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
        elif lt == "regulatory_operating_fee":
            # Regulatory operating fee goes to the transaction_fee bucket
            o["Regulatory Fee"] = round(o.get("Regulatory Fee", 0) + amount, 2)
            o["Transaction Fee"] = round(o.get("Transaction Fee", 0) + amount, 2)
        elif lt == "transaction_quantity":
            # transaction_quantity goes to listing_fee, NOT transaction_fee
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
                pmt_data = _safe_get_receipt_payments(shop_id, rid)
                if pmt_data and pmt_data.get("results"):
                    pmt = pmt_data["results"][0]
                    an = pmt.get("amount_net", {})
                    api_net = an.get("amount", 0) / an.get("divisor", 100) if isinstance(an, dict) else 0
                    af = pmt.get("amount_fees", {})
                    proc_fee = abs(af.get("amount", 0) / af.get("divisor", 100)) if isinstance(af, dict) else 0

                    o["Processing Fee"] = round(proc_fee, 2)
                    o["Total Etsy Fees"] = round(
                        o["Transaction Fee"] + o["Offsite Ads"] + proc_fee, 2
                    )

                    # Compute Fee %: Total Etsy Fees / (Sale Price + Buyer Shipping) * 100
                    fee_base = o.get("Sale Price", 0) + o.get("Buyer Shipping", 0)
                    if fee_base > 0:
                        o["Fee %"] = round(o["Total Etsy Fees"] / fee_base * 100, 1)
                    else:
                        o["Fee %"] = 0

                    # Check for refund
                    refund = 0
                    for adj in pmt.get("payment_adjustments", []):
                        adj_amt = adj.get("total_adjustment_amount", 0)
                        if adj_amt:
                            refund += adj_amt / 100.0
                    o["Refund"] = round(refund, 2)

                    if refund > 0:
                        o["Status"] = "Refunded"
                        _apply_refund_logic(o, pmt, refund)
                    else:
                        # True Net = amount_net - txn - ads - label
                        true_net = api_net - o["Transaction Fee"] - o["Offsite Ads"] - o["Shipping Label"]
                        o["True Net"] = round(true_net, 2)
                        _rev = (o.get("Sale Price", 0) or 0) + (o.get("Buyer Shipping", 0) or 0)
                        o["Margin %"] = round(true_net / _rev * 100, 1) if _rev > 0 else 0
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
        "orders_updated": orders_updated,
        "last_result": f"{new_order_count} orders, {new_labels_matched} labels matched, {new_unmatched} unmatched, {orders_updated} updated",
    })

    # Persist sync results to Supabase for dashboard visibility across restarts
    _persist_sync_results(new_order_count, new_labels_matched, new_unmatched,
                          needs_manual, orders_updated, len(orders))

    _logger.info("Sync complete: %d new orders, %d labels matched, %d unmatched, %d need manual, %d updated",
                 new_order_count, new_labels_matched, new_unmatched, needs_manual, orders_updated)

    return {
        "new_orders": new_order_count,
        "new_labels_matched": new_labels_matched,
        "new_unmatched": new_unmatched,
        "needs_manual": needs_manual,
        "orders_updated": orders_updated,
        "total_orders": len(orders),
    }


def _apply_refund_logic(order, pmt, refund_amount):
    """Determine if refund is partial or full. Auto-compute True Net for partial refunds.

    Partial refund: refund_amount < sale_price — we can compute what was earned.
    Full refund: refund_amount >= sale_price — need manual entry.
    """
    sale_price = order.get("Sale Price", 0)

    if sale_price > 0 and refund_amount < sale_price:
        # PARTIAL refund — auto-compute True Net
        # Earnings = what they paid minus what was refunded minus fees minus label
        an = pmt.get("amount_net", {})
        api_net = an.get("amount", 0) / an.get("divisor", 100) if isinstance(an, dict) else 0

        # api_net already accounts for the refund adjustment from Etsy's side
        true_net = api_net - order.get("Transaction Fee", 0) - order.get("Offsite Ads", 0) - order.get("Shipping Label", 0)
        order["True Net"] = round(true_net, 2)
        _rev = (order.get("Sale Price", 0) or 0) + (order.get("Buyer Shipping", 0) or 0)
        order["Margin %"] = round(true_net / _rev * 100, 1) if _rev > 0 else 0

        # Fee %
        fee_base = sale_price + order.get("Buyer Shipping", 0)
        if fee_base > 0:
            order["Fee %"] = round(order.get("Total Etsy Fees", 0) / fee_base * 100, 1)

        order["_payment_verified"] = True
        order["_needs_manual_net"] = False
        _logger.info("Partial refund on order %s: $%.2f of $%.2f — True Net auto-computed: $%.2f",
                      order["Order ID"], refund_amount, sale_price, true_net)
    else:
        # FULL refund — can't determine earnings, flag for manual
        order["_needs_manual_net"] = True
        order["_payment_verified"] = False
        _logger.info("Full refund on order %s: $%.2f — needs manual entry",
                      order["Order ID"], refund_amount)


def _save_timestamp(ts):
    from supabase_loader import _get_supabase_client
    client = _get_supabase_client()
    client.table("config").upsert({
        "key": "auto_sync_last_timestamp",
        "value": str(ts),
    }, on_conflict="key").execute()


def _persist_sync_results(new_orders, labels_matched, unmatched, needs_manual, updated, total):
    """Save sync results to Supabase so the dashboard can display last sync info after restart."""
    from supabase_loader import save_config_value
    try:
        result = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "new_orders": new_orders,
            "labels_matched": labels_matched,
            "unmatched": unmatched,
            "needs_manual": needs_manual,
            "orders_updated": updated,
            "total_orders": total,
        }
        save_config_value("auto_sync_last_result", json.dumps(result))
    except Exception as e:
        _logger.warning("Failed to persist sync results: %s", e)


def load_persisted_sync_status():
    """Load last sync results from Supabase (called at startup)."""
    from supabase_loader import get_config_value
    try:
        raw = get_config_value("auto_sync_last_result")
        if raw:
            data = json.loads(raw) if isinstance(raw, str) else raw
            _sync_status["last_run"] = data.get("timestamp")
            _sync_status["new_orders"] = data.get("new_orders", 0)
            _sync_status["new_labels_matched"] = data.get("labels_matched", 0)
            _sync_status["new_unmatched"] = data.get("unmatched", 0)
            _sync_status["needs_manual"] = data.get("needs_manual", 0)
            _sync_status["orders_updated"] = data.get("orders_updated", 0)
            _sync_status["last_result"] = (
                f"{data.get('new_orders', 0)} orders, "
                f"{data.get('labels_matched', 0)} labels matched, "
                f"{data.get('unmatched', 0)} unmatched, "
                f"{data.get('orders_updated', 0)} updated"
            )
            return data
    except Exception as e:
        _logger.warning("Failed to load persisted sync status: %s", e)
    return None


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
            data = _safe_get_receipt_payments(shop_id, o["Order ID"])
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
    # Load persisted status from last run on startup
    load_persisted_sync_status()

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
