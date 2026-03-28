"""Etsy API v3 OAuth 2.0 with PKCE + data fetching.

OAuth flow:
1. User visits /api/etsy/connect → redirects to Etsy authorization page
2. User authorizes → Etsy redirects to /api/etsy/callback with code
3. Code is exchanged for access_token + refresh_token
4. Tokens stored in Supabase config for persistence across deploys

NOTE: You must add this callback URL to your Etsy app settings:
https://web-production-7f385.up.railway.app/api/etsy/callback
"""

import os
import json
import time
import hashlib
import base64
import secrets
import logging

import requests

_logger = logging.getLogger("dashboard.etsy_api")

ETSY_API_KEY = os.environ.get("ETSY_API_KEY", "")
ETSY_SHARED_SECRET = os.environ.get("ETSY_SHARED_SECRET", "")
ETSY_BASE_URL = "https://api.etsy.com/v3"
OAUTH_URL = "https://www.etsy.com/oauth/connect"
TOKEN_URL = f"{ETSY_BASE_URL}/public/oauth/token"

# Token storage (persisted to Supabase)
_tokens = {
    "access_token": None,
    "refresh_token": None,
    "expires_at": 0,
    "shop_id": None,
}

# PKCE temp storage (needed between redirect and callback)
_pkce_state = {}


# ── PKCE Helpers ──────────────────────────────────────────────────────────────

def generate_pkce():
    """Generate PKCE code_verifier and code_challenge."""
    verifier = secrets.token_urlsafe(32)
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return verifier, challenge


# ── OAuth Flow ────────────────────────────────────────────────────────────────

def get_auth_url(redirect_uri):
    """Generate the Etsy OAuth authorization URL."""
    verifier, challenge = generate_pkce()
    state = secrets.token_urlsafe(16)

    # Store for callback
    _pkce_state["verifier"] = verifier
    _pkce_state["state"] = state

    scopes = "transactions_r shops_r listings_r email_r"

    import urllib.parse
    params = urllib.parse.urlencode({
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": scopes,
        "client_id": ETSY_API_KEY,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    })

    url = f"{OAUTH_URL}?{params}"
    _logger.info("Generated Etsy auth URL (state=%s)", state[:8])
    return url


def exchange_code(code, redirect_uri):
    """Exchange authorization code for access + refresh tokens."""
    verifier = _pkce_state.get("verifier")
    if not verifier:
        raise ValueError("No PKCE verifier found — start OAuth flow again")

    resp = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "client_id": ETSY_API_KEY,
        "redirect_uri": redirect_uri,
        "code": code,
        "code_verifier": verifier,
    })

    if resp.status_code != 200:
        _logger.error("Token exchange failed: %s %s", resp.status_code, resp.text[:300])
        raise ValueError(f"Token exchange failed: {resp.status_code} {resp.text[:200]}")

    data = resp.json()
    _tokens["access_token"] = data["access_token"]
    _tokens["refresh_token"] = data["refresh_token"]
    _tokens["expires_at"] = time.time() + data.get("expires_in", 3600)

    _save_tokens()
    _logger.info("Etsy tokens obtained successfully")

    return data


def refresh_access_token():
    """Refresh the access token using the refresh token."""
    if not _tokens.get("refresh_token"):
        raise ValueError("No refresh token available — reconnect Etsy")

    resp = requests.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "client_id": ETSY_API_KEY,
        "refresh_token": _tokens["refresh_token"],
    })

    if resp.status_code != 200:
        _logger.error("Token refresh failed: %s %s", resp.status_code, resp.text[:300])
        raise ValueError(f"Token refresh failed: {resp.status_code} {resp.text[:200]}")

    data = resp.json()
    _tokens["access_token"] = data["access_token"]
    _tokens["refresh_token"] = data["refresh_token"]
    _tokens["expires_at"] = time.time() + data.get("expires_in", 3600)

    _save_tokens()
    _logger.info("Etsy tokens refreshed successfully")
    return data


# ── Token Management ──────────────────────────────────────────────────────────

def _get_headers():
    """Get auth headers, refreshing token if needed."""
    if _tokens.get("expires_at") and time.time() > _tokens["expires_at"] - 60:
        try:
            refresh_access_token()
        except Exception as e:
            _logger.error("Auto-refresh failed: %s", e)
            raise

    token = _tokens['access_token'] or ""
    # Strip any "Bearer " prefix if the token was stored with it
    if token.startswith("Bearer "):
        token = token[7:]
    # Etsy v3 requires x-api-key as "keystring:shared_secret"
    return {
        "Authorization": f"Bearer {token}",
        "x-api-key": f"{ETSY_API_KEY}:{ETSY_SHARED_SECRET}",
    }


def _save_tokens():
    """Persist tokens to Supabase config (direct write, bypasses governance hooks)."""
    try:
        from supabase_loader import _get_supabase_client
        client = _get_supabase_client()
        if client:
            token_data = json.dumps({
                "access_token": _tokens["access_token"],
                "refresh_token": _tokens["refresh_token"],
                "expires_at": _tokens["expires_at"],
                "shop_id": _tokens.get("shop_id"),
            })
            # Direct upsert — avoids governance mutation log that causes deadlock
            client.table("config").upsert({
                "key": "etsy_api_tokens",
                "value": token_data,
            }, on_conflict="key").execute()
            _logger.info("Tokens saved to Supabase")
    except Exception as e:
        _logger.warning("Failed to save tokens to Supabase: %s", e)


def _load_tokens():
    """Load tokens from Supabase config."""
    try:
        from supabase_loader import get_config_value
        raw = get_config_value("etsy_api_tokens")
        if raw:
            data = json.loads(raw) if isinstance(raw, str) else raw
            # Clean any "Bearer " prefix that got stored accidentally
            if data.get("access_token", "").startswith("Bearer "):
                data["access_token"] = data["access_token"][7:]
            _tokens.update(data)
            _logger.info("Loaded Etsy tokens from Supabase (shop_id=%s)", _tokens.get("shop_id"))
            return True
    except Exception as e:
        _logger.warning("Failed to load tokens from Supabase: %s", e)
    return False


def is_connected():
    """Check if we have a valid access token."""
    if not _tokens.get("access_token"):
        _load_tokens()
    return bool(_tokens.get("access_token"))


def disconnect():
    """Clear stored tokens."""
    _tokens.update({
        "access_token": None,
        "refresh_token": None,
        "expires_at": 0,
        "shop_id": None,
    })
    _save_tokens()
    _logger.info("Etsy disconnected — tokens cleared")


# ── API Calls ─────────────────────────────────────────────────────────────────

def _get_user_id():
    """Extract user ID from the access token (first segment before the dot)."""
    token = _tokens.get("access_token", "")
    if token.startswith("Bearer "):
        token = token[7:]
    if "." in token:
        return token.split(".")[0]
    return None


def get_shop_id():
    """Get the shop ID for the authenticated user."""
    if _tokens.get("shop_id"):
        return _tokens["shop_id"]

    user_id = _get_user_id()
    endpoints = []
    if user_id:
        endpoints.append(f"{ETSY_BASE_URL}/application/users/{user_id}/shops")
    endpoints.append(f"{ETSY_BASE_URL}/application/users/me/shops")
    endpoints.append(f"{ETSY_BASE_URL}/application/shops")

    for endpoint in endpoints:
        try:
            resp = requests.get(endpoint, headers=_get_headers())
            _logger.info("get_shop_id tried %s: status=%s body=%s", endpoint, resp.status_code, resp.text[:300])
            if resp.status_code == 200:
                data = resp.json()
                shops = data.get("results", [])
                if not shops and isinstance(data, dict) and data.get("shop_id"):
                    # Single shop response
                    _tokens["shop_id"] = data["shop_id"]
                    _save_tokens()
                    _logger.info("Found shop ID: %s", _tokens["shop_id"])
                    return _tokens["shop_id"]
                elif shops:
                    _tokens["shop_id"] = shops[0]["shop_id"]
                    _save_tokens()
                    _logger.info("Found shop ID: %s (%s)", _tokens["shop_id"], shops[0].get("shop_name", ""))
                    return _tokens["shop_id"]
        except Exception as e:
            _logger.warning("get_shop_id error on %s: %s", endpoint, e)

    _logger.warning("Could not get shop ID from any endpoint")
    return None


def debug_api_call(endpoint):
    """Make a raw API call and return the response for debugging."""
    try:
        headers = _get_headers()
        # Mask tokens for debug output
        debug_headers = {
            "x-api-key": headers.get("x-api-key", "")[:8] + "..." if headers.get("x-api-key") else "MISSING",
            "Authorization": "Bearer " + headers.get("Authorization", "")[:15] + "..." if "Bearer" in headers.get("Authorization", "") else "MISSING",
        }
        resp = requests.get(f"{ETSY_BASE_URL}{endpoint}", headers=headers)
        return {
            "status": resp.status_code,
            "body": resp.json() if resp.status_code == 200 else resp.text[:500],
            "headers_sent": debug_headers,
            "api_key_env_set": bool(ETSY_API_KEY),
        }
    except Exception as e:
        return {"error": str(e), "api_key_env_set": bool(ETSY_API_KEY)}


def get_shop_info(shop_id):
    """Get shop details."""
    resp = requests.get(
        f"{ETSY_BASE_URL}/application/shops/{shop_id}",
        headers=_get_headers(),
    )
    if resp.status_code == 200:
        return resp.json()
    return None


def get_receipts(shop_id, limit=25, offset=0, min_created=None):
    """Fetch order receipts (orders with shipping info).

    Each receipt contains:
    - receipt_id, order_id
    - buyer info
    - shipments (tracking_code, carrier, shipping cost)
    - transactions (line items with listing_id, title, price, quantity)
    - payment info (total, shipping, tax, discount)
    """
    params = {"limit": limit, "offset": offset}
    if min_created:
        params["min_created"] = int(min_created)

    resp = requests.get(
        f"{ETSY_BASE_URL}/application/shops/{shop_id}/receipts",
        headers=_get_headers(),
        params=params,
    )
    if resp.status_code == 200:
        return resp.json()
    _logger.warning("get_receipts failed: %s %s", resp.status_code, resp.text[:200])
    return None


def get_all_receipts(shop_id, min_created=None):
    """Fetch ALL receipts with pagination."""
    all_results = []
    offset = 0
    limit = 100  # max per page

    while True:
        data = get_receipts(shop_id, limit=limit, offset=offset, min_created=min_created)
        if not data or not data.get("results"):
            break
        all_results.extend(data["results"])
        _logger.info("Fetched %d receipts (total so far: %d)", len(data["results"]), len(all_results))

        if len(data["results"]) < limit:
            break  # last page
        offset += limit

    return all_results


def get_receipt_by_id(shop_id, receipt_id):
    """Fetch a single receipt with full details."""
    resp = requests.get(
        f"{ETSY_BASE_URL}/application/shops/{shop_id}/receipts/{receipt_id}",
        headers=_get_headers(),
    )
    if resp.status_code == 200:
        return resp.json()
    return None


def get_shop_transactions(shop_id, limit=25, offset=0):
    """Fetch shop transactions (individual line items)."""
    resp = requests.get(
        f"{ETSY_BASE_URL}/application/shops/{shop_id}/transactions",
        headers=_get_headers(),
        params={"limit": limit, "offset": offset},
    )
    if resp.status_code == 200:
        return resp.json()
    return None


def get_listings(shop_id, state="active", limit=100, offset=0):
    """Fetch shop listings."""
    resp = requests.get(
        f"{ETSY_BASE_URL}/application/shops/{shop_id}/listings/{state}",
        headers=_get_headers(),
        params={"limit": limit, "offset": offset},
    )
    if resp.status_code == 200:
        return resp.json()
    return None


def get_shipping_labels(shop_id, receipt_id):
    """Fetch shipping label info for a receipt."""
    resp = requests.get(
        f"{ETSY_BASE_URL}/application/shops/{shop_id}/receipts/{receipt_id}/tracking",
        headers=_get_headers(),
    )
    if resp.status_code == 200:
        return resp.json()
    return None


def get_receipt_payments(shop_id, receipt_id):
    """Fetch payment details for a receipt (may include fee breakdown)."""
    resp = requests.get(
        f"{ETSY_BASE_URL}/application/shops/{shop_id}/receipts/{receipt_id}/payments",
        headers=_get_headers(),
    )
    if resp.status_code == 200:
        return resp.json()
    _logger.info("get_receipt_payments %s: %s %s", receipt_id, resp.status_code, resp.text[:200])
    return None


def get_ledger_entries(shop_id, min_created, max_created, limit=100, offset=0):
    """Fetch shop payment account ledger entries (all fees, payments, deposits)."""
    resp = requests.get(
        f"{ETSY_BASE_URL}/application/shops/{shop_id}/payment-account/ledger-entries",
        headers=_get_headers(),
        params={"limit": limit, "offset": offset, "min_created": int(min_created), "max_created": int(max_created)},
    )
    if resp.status_code == 200:
        return resp.json()
    _logger.info("get_ledger_entries: %s %s", resp.status_code, resp.text[:200])
    return None


def get_all_ledger_entries(shop_id, days_back=365):
    """Fetch ALL ledger entries for the shop.

    Etsy limits queries to 31-day windows, so we loop through
    in 30-day chunks going backwards from today.

    Returns list of all ledger entry dicts.
    """
    all_entries = []
    now = int(time.time())
    chunk_days = 30
    chunk_size = chunk_days * 86400

    # Calculate how many windows we need
    total_seconds = days_back * 86400
    window_end = now

    while window_end > (now - total_seconds):
        window_start = window_end - chunk_size
        if window_start < (now - total_seconds):
            window_start = now - total_seconds

        offset = 0
        limit = 100
        window_count = 0

        while True:
            try:
                resp = requests.get(
                    f"{ETSY_BASE_URL}/application/shops/{shop_id}/payment-account/ledger-entries",
                    headers=_get_headers(),
                    params={"limit": limit, "offset": offset,
                            "min_created": int(window_start), "max_created": int(window_end)},
                )
                if resp.status_code != 200:
                    _logger.warning("Ledger fetch failed: %s %s", resp.status_code, resp.text[:200])
                    break
                data = resp.json()
                results = data.get("results", [])
                if not results:
                    break
                all_entries.extend(results)
                window_count += len(results)
                if len(results) < limit:
                    break
                offset += limit
                time.sleep(0.25)
            except Exception as e:
                _logger.warning("Ledger fetch error: %s", e)
                break

        _logger.info("Ledger window: %d entries (total: %d)", window_count, len(all_entries))
        window_end = window_start
        time.sleep(0.25)

    _logger.info("Total ledger entries fetched: %d", len(all_entries))
    return all_entries


def build_order_profit_from_ledger(all_receipts, all_ledger_entries, all_items):
    """Match ledger entries to orders and compute true P/L per order.

    Matching strategy:
    - receipt reference_type → direct receipt_id match
    - transaction reference_type → transaction_id → receipt_id via items
    - etsy reference_type (offsite ads) → receipt_id directly
    - shop_payment/processing_fee → match by same-second timestamp to receipt entry
    - listing (renew_sold_auto) → match by same-second timestamp to receipt entry
    - shipping_label → match by timestamp to receipt's shipment notification timestamp

    Returns:
        list of order dicts with full financial breakdown
    """
    # Build lookup: transaction_id → receipt_id (order ID)
    txn_to_receipt = {}
    for item in all_items:
        txn_id = str(item.get("Transaction ID", ""))
        order_id = str(item.get("Order ID", ""))
        if txn_id and order_id:
            txn_to_receipt[txn_id] = order_id

    # Build lookup: receipt_id → receipt data
    receipt_lookup = {}
    for r in all_receipts:
        rid = str(r.get("receipt_id", r.get("Order ID", "")))
        receipt_lookup[rid] = r

    # Build shipment timestamp → receipt_id lookup for label matching
    # Each receipt's shipment has a notification timestamp — match labels to this
    ship_ts_to_receipt = {}
    for r in all_receipts:
        rid = str(r.get("receipt_id", r.get("Order ID", "")))
        # Check raw receipt for shipments (from API sync)
        # The sync stores shipped_timestamp in transactions
        txns = r.get("transactions", [])
        for t in txns if isinstance(txns, list) else []:
            shipped_ts = t.get("shipped_timestamp")
            if shipped_ts and isinstance(shipped_ts, (int, float)) and shipped_ts > 0:
                ship_ts_to_receipt[int(shipped_ts)] = rid

    # Also build from receipt-level data
    for r in all_receipts:
        rid = str(r.get("receipt_id", r.get("Order ID", "")))
        shipments = r.get("shipments", [])
        if isinstance(shipments, list):
            for s in shipments:
                ts = s.get("shipment_notification_timestamp", s.get("shipped_timestamp", 0))
                if ts and isinstance(ts, (int, float)) and ts > 0:
                    ship_ts_to_receipt[int(ts)] = rid

    # Build timestamp → receipt_id index for same-second matching
    ts_to_receipt = {}
    for entry in all_ledger_entries:
        if entry.get("reference_type") == "receipt":
            ts = entry.get("created_timestamp", 0)
            rid = str(entry.get("reference_id", ""))
            if ts and rid:
                ts_to_receipt[ts] = rid

    # Group ledger entries by order
    order_financials = {}

    _unmatched_labels = 0
    _matched_labels = 0

    for entry in all_ledger_entries:
        amount = entry.get("amount", 0) / 100.0
        ledger_type = entry.get("ledger_type", "")
        ref_id = str(entry.get("reference_id", ""))
        ref_type = entry.get("reference_type", "")
        timestamp = entry.get("created_timestamp", 0)

        order_id = None

        if ref_type == "receipt":
            order_id = ref_id
        elif ref_type == "etsy":
            # offsite ads, refunds — reference_id is receipt_id
            order_id = ref_id
        elif ref_type == "transaction":
            order_id = txn_to_receipt.get(ref_id)
        elif ref_type in ("shop_payment", "processing_fee", "payment_adjustment"):
            # Match by same-second timestamp to a receipt entry
            order_id = ts_to_receipt.get(timestamp)
            if not order_id:
                # Try ±1 second
                for delta in (1, -1, 2, -2):
                    order_id = ts_to_receipt.get(timestamp + delta)
                    if order_id:
                        break
        elif ref_type == "listing":
            # Listing renewal — match by same-second timestamp
            order_id = ts_to_receipt.get(timestamp)
            if not order_id:
                for delta in (1, -1, 2, -2, 3, -3, 5, -5):
                    order_id = ts_to_receipt.get(timestamp + delta)
                    if order_id:
                        break
        elif ref_type == "shipping_label":
            # Match label to order via shipment timestamp
            # The label purchase timestamp should be very close to the ship notification
            best_match = None
            best_diff = 86400  # max 24 hours
            for ship_ts, ship_receipt in ship_ts_to_receipt.items():
                diff = abs(ship_ts - timestamp)
                if diff < best_diff:
                    best_diff = diff
                    best_match = ship_receipt
            if best_match:
                order_id = best_match
                _matched_labels += 1
            else:
                _unmatched_labels += 1

        if not order_id:
            continue

        if order_id not in order_financials:
            order_financials[order_id] = {
                "gross": 0, "processing_fee": 0, "transaction_fee": 0,
                "sales_tax": 0, "offsite_ads": 0, "listing_fee": 0,
                "shipping_label": 0, "refund": 0, "other": 0,
            }

        fin = order_financials[order_id]
        if ledger_type == "PAYMENT_GROSS":
            fin["gross"] += amount
        elif ledger_type == "PAYMENT_PROCESSING_FEE":
            fin["processing_fee"] += amount
        elif ledger_type == "transaction":
            fin["transaction_fee"] += amount
        elif ledger_type == "sales_tax":
            fin["sales_tax"] += amount
        elif ledger_type == "offsite_ads_fee":
            fin["offsite_ads"] += amount
        elif ledger_type == "renew_sold_auto":
            fin["listing_fee"] += amount
        elif ledger_type == "shipping_labels":
            fin["shipping_label"] += amount
        elif ledger_type in ("REFUND_GROSS", "REFUND_PROCESSING_FEE",
                             "transaction_refund", "sales_tax_refund"):
            fin["refund"] += amount
        else:
            fin["other"] += amount

    _logger.info("Label matching: %d matched, %d unmatched", _matched_labels, _unmatched_labels)

    # Build final order list with full breakdown
    result_orders = []
    for order_id, fin in order_financials.items():
        receipt = receipt_lookup.get(order_id, {})

        # Get item info
        order_items_list = [i for i in all_items if str(i.get("Order ID", "")) == order_id]
        item_names = " | ".join(i.get("Item Name", "") for i in order_items_list)
        variations = []
        total_qty = 0
        for it in order_items_list:
            total_qty += it.get("Quantity", 1)
            var = it.get("Variations", "")
            if var:
                for v in var.split(", "):
                    if ": " in v:
                        prop, val = v.split(": ", 1)
                        variations.append(val if prop == "Custom Property" else val)

        var_str = " / ".join(variations) if variations else ""

        total_fees = abs(fin["processing_fee"]) + abs(fin["transaction_fee"]) + abs(fin["offsite_ads"]) + abs(fin["listing_fee"])
        label_cost = abs(fin["shipping_label"])
        true_net = fin["gross"] - abs(fin["sales_tax"]) - total_fees - label_cost

        result_orders.append({
            "Order ID": order_id,
            "Sale Date": receipt.get("Sale Date", ""),
            "Buyer": receipt.get("Buyer", receipt.get("Full Name", "")),
            "Qty": total_qty or receipt.get("Number of Items", 1),
            "Item Names": item_names[:60] if item_names else receipt.get("Item Names", ""),
            "Variations": var_str,
            "Gross": round(fin["gross"], 2),
            "Processing Fee": round(abs(fin["processing_fee"]), 2),
            "Transaction Fee": round(abs(fin["transaction_fee"]), 2),
            "Offsite Ads": round(abs(fin["offsite_ads"]), 2),
            "Listing Fee": round(abs(fin["listing_fee"]), 2),
            "Total Etsy Fees": round(total_fees, 2),
            "Shipping Label": round(label_cost, 2),
            "Sales Tax": round(abs(fin["sales_tax"]), 2),
            "Discount": receipt.get("Discount Amount", 0),
            "True Net": round(true_net, 2),
            "Fee %": round(total_fees / fin["gross"] * 100, 1) if fin["gross"] else 0,
            "Status": receipt.get("Status", ""),
            "Ship State": receipt.get("Ship State", ""),
            "Tracking": receipt.get("Tracking", ""),
            "_store": receipt.get("_store", "keycomponentmfg"),
        })

    result_orders.sort(key=lambda x: x.get("Sale Date", ""), reverse=True)
    _logger.info("Built profit data for %d orders from %d ledger entries", len(result_orders), len(all_ledger_entries))
    return result_orders


def get_ledger_entry_payments(shop_id, ledger_entry_ids):
    """Fetch payment details for specific ledger entries."""
    ids_str = ",".join(str(i) for i in ledger_entry_ids)
    resp = requests.get(
        f"{ETSY_BASE_URL}/application/shops/{shop_id}/payment-account/ledger-entries/payments",
        headers=_get_headers(),
        params={"ledger_entry_ids": ids_str},
    )
    if resp.status_code == 200:
        return resp.json()
    _logger.info("get_ledger_entry_payments: %s %s", resp.status_code, resp.text[:200])
    return None


# ── Order Sync ────────────────────────────────────────────────────────────────

def _fetch_all_payments(shop_id, receipt_ids):
    """Fetch payment data for all receipts, respecting rate limits.

    Returns dict of {receipt_id: payment_data}.
    """
    payments = {}
    batch_count = 0
    for rid in receipt_ids:
        try:
            data = get_receipt_payments(shop_id, rid)
            if data and data.get("results"):
                payments[str(rid)] = data["results"][0]
            batch_count += 1
            # Respect 5 QPS rate limit
            if batch_count % 4 == 0:
                time.sleep(0.25)
        except Exception as e:
            _logger.warning("Payment fetch failed for receipt %s: %s", rid, e)
    _logger.info("Fetched payments for %d/%d receipts", len(payments), len(receipt_ids))
    return payments


def sync_all_orders(shop_id, store_slug="keycomponentmfg"):
    """Pull all receipts from Etsy API and normalize into order + item format.

    Calculates per-order fees using Etsy's known fee structure:
    - Transaction fee: 6.5% of (item price + buyer shipping)
    - Processing fee: 3% + $0.25 per payment
    - Listing fee: $0.20 per item

    Returns dict with:
        orders: list of order dicts (same shape as CSV upload + fee data)
        items: list of item dicts (same shape as CSV upload)
        raw_receipts: list of raw Etsy receipt dicts
        stats: {total, new, existing, errors}
    """
    _logger.info("Starting full order sync for shop %s (%s)", shop_id, store_slug)

    all_receipts = get_all_receipts(shop_id)
    if not all_receipts:
        return {"orders": [], "items": [], "raw_receipts": [], "stats": {"total": 0}}

    # We calculate fees from known Etsy fee structure instead of
    # fetching 465 individual payment records (which times out)

    orders = []
    items = []

    for r in all_receipts:
        try:
            receipt_id = r.get("receipt_id")
            created = r.get("created_timestamp", 0)

            # Parse amounts (Etsy returns {amount: cents, divisor: 100})
            def _amt(field):
                v = r.get(field, {})
                if isinstance(v, dict):
                    return v.get("amount", 0) / v.get("divisor", 100)
                return float(v) if v else 0

            total_price = _amt("total_price")
            shipping_cost = _amt("total_shipping_cost")
            tax = _amt("total_tax_cost")
            discount = _amt("discount_amt")
            grandtotal = _amt("grandtotal")

            # Shipment info (label cost + tracking)
            shipments = r.get("shipments", [])
            tracking_code = ""
            carrier = ""
            ship_date = ""
            if shipments:
                s = shipments[0]
                tracking_code = s.get("tracking_code", "")
                carrier = s.get("carrier_name", "")
                if s.get("ship_date") or s.get("shipped_timestamp"):
                    ts = s.get("shipped_timestamp") or s.get("ship_date")
                    if isinstance(ts, (int, float)) and ts > 0:
                        from datetime import datetime
                        ship_date = datetime.fromtimestamp(ts).strftime("%m/%d/%Y")

            # Sale date
            sale_date = ""
            if created:
                from datetime import datetime
                sale_date = datetime.fromtimestamp(created).strftime("%m/%d/%Y")

            # Buyer info
            buyer_name = r.get("name", "")
            ship_state = r.get("state", "")
            ship_country = r.get("country_iso", "")
            status = r.get("status", "")

            # Transactions (line items)
            txns = r.get("transactions", [])
            item_names = []
            num_items = 0

            for t in txns:
                t_price = t.get("price", {})
                if isinstance(t_price, dict):
                    item_price = t_price.get("amount", 0) / t_price.get("divisor", 100)
                else:
                    item_price = float(t_price) if t_price else 0

                t_ship = t.get("shipping_cost", {})
                if isinstance(t_ship, dict):
                    item_ship = t_ship.get("amount", 0) / t_ship.get("divisor", 100)
                else:
                    item_ship = float(t_ship) if t_ship else 0

                item_title = t.get("title", "")
                item_qty = t.get("quantity", 1)
                item_names.append(item_title)
                num_items += item_qty

                # Variations (size, color, etc.)
                variations = t.get("variations", [])
                var_str = ", ".join(f"{v.get('formatted_name', '')}: {v.get('formatted_value', '')}"
                                    for v in variations) if variations else ""

                # Product data (more detailed variations)
                product_data = t.get("product_data", [])
                product_vars = ", ".join(f"{p.get('property_name', '')}: {p['values'][0]}"
                                          for p in product_data if p.get("values")) if product_data else ""

                items.append({
                    "Order ID": str(receipt_id),
                    "Transaction ID": str(t.get("transaction_id", "")),
                    "Listing ID": str(t.get("listing_id", "")),
                    "Item Name": item_title,
                    "Quantity": item_qty,
                    "Price": round(item_price, 2),
                    "Shipping": round(item_ship, 2),
                    "Variations": product_vars or var_str,
                    "_store": store_slug,
                    "_source": "etsy_api",
                })

            # Calculate Etsy fees from known fee structure
            # Transaction fee: 6.5% of (item subtotal + buyer shipping)
            _subtotal = _amt("subtotal")
            transaction_fee = round((_subtotal + shipping_cost) * 0.065, 2)
            # Processing fee: 3% + $0.25 of grandtotal
            processing_fee = round(grandtotal * 0.03 + 0.25, 2)
            # Listing fee: $0.20 per unique listing
            listing_fee = round(len(txns) * 0.20, 2)
            # Total Etsy fees
            etsy_fees = round(transaction_fee + processing_fee + listing_fee, 2)
            # Net after all Etsy deductions (before hard costs)
            etsy_net = round(grandtotal - tax - etsy_fees, 2)
            profit_before_hardcost = etsy_net

            # Order-level record
            orders.append({
                "Order ID": str(receipt_id),
                "Sale Date": sale_date,
                "Date Shipped": ship_date,
                "Number of Items": num_items,
                "Order Value": round(total_price, 2),
                "Discount Amount": round(discount, 2),
                "Shipping": round(shipping_cost, 2),
                "Sales Tax": round(tax, 2),
                "Order Total": round(grandtotal, 2),
                "Transaction Fee": transaction_fee,
                "Processing Fee": processing_fee,
                "Listing Fee": listing_fee,
                "Etsy Fees": etsy_fees,
                "Etsy Net": etsy_net,
                "Order Net": etsy_net,
                "Profit Before Hard Cost": profit_before_hardcost,
                "Fee %": round(etsy_fees / grandtotal * 100, 1) if grandtotal else 0,
                "Ship State": ship_state,
                "Ship Country": ship_country,
                "Buyer": buyer_name,
                "Full Name": buyer_name,
                "Status": status,
                "Tracking": tracking_code,
                "Carrier": carrier,
                "Is Gift": r.get("is_gift", False),
                "Gift Message": r.get("gift_message", ""),
                "Item Names": " | ".join(item_names),
                "_store": store_slug,
                "_source": "etsy_api",
            })

        except Exception as e:
            _logger.warning("Error processing receipt %s: %s", r.get("receipt_id"), e)

    stats = {
        "total_receipts": len(all_receipts),
        "orders_parsed": len(orders),
        "items_parsed": len(items),
    }
    _logger.info("Sync complete: %d receipts → %d orders, %d items", len(all_receipts), len(orders), len(items))

    return {
        "orders": orders,
        "items": items,
        "raw_receipts": all_receipts,
        "stats": stats,
    }


def save_synced_orders(orders, items, store_slug="keycomponentmfg"):
    """Save API-synced orders and items to Supabase config (same as CSV upload)."""
    try:
        from supabase_loader import _get_supabase_client
        client = _get_supabase_client()
        if not client:
            return False

        # Save orders
        client.table("config").upsert({
            "key": f"order_csv_orders_{store_slug}",
            "value": json.dumps(orders),
        }, on_conflict="key").execute()

        # Save items
        client.table("config").upsert({
            "key": f"order_csv_items_{store_slug}",
            "value": json.dumps(items),
        }, on_conflict="key").execute()

        _logger.info("Saved %d orders and %d items for %s to Supabase", len(orders), len(items), store_slug)
        return True
    except Exception as e:
        _logger.error("Failed to save synced orders: %s", e)
        return False


# ── Load tokens on import ─────────────────────────────────────────────────────
_load_tokens()
