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

    scopes = "transactions_r shops_r listings_r receipts_r"

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

    return {
        "Authorization": f"Bearer {_tokens['access_token']}",
        "x-api-key": ETSY_API_KEY,
    }


def _save_tokens():
    """Persist tokens to Supabase config."""
    try:
        from supabase_loader import save_config_value
        save_config_value("etsy_api_tokens", json.dumps({
            "access_token": _tokens["access_token"],
            "refresh_token": _tokens["refresh_token"],
            "expires_at": _tokens["expires_at"],
            "shop_id": _tokens.get("shop_id"),
        }))
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

def get_shop_id():
    """Get the shop ID for the authenticated user."""
    if _tokens.get("shop_id"):
        return _tokens["shop_id"]

    resp = requests.get(
        f"{ETSY_BASE_URL}/application/shops",
        headers=_get_headers(),
    )
    if resp.status_code == 200:
        shops = resp.json().get("results", [])
        if shops:
            _tokens["shop_id"] = shops[0]["shop_id"]
            _save_tokens()
            _logger.info("Found shop ID: %s (%s)", _tokens["shop_id"], shops[0].get("shop_name", ""))
            return _tokens["shop_id"]
    _logger.warning("Could not get shop ID: %s", resp.status_code)
    return None


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


# ── Load tokens on import ─────────────────────────────────────────────────────
_load_tokens()
