"""
supabase_loader.py — Load dashboard data from Supabase (with local-file fallback).

Returns the exact same data structures that etsy_dashboard.py expects:
  DATA      — pandas DataFrame with Etsy transactions (columns match CSV headers)
  CONFIG    — dict (same shape as config.json)
  INVOICES  — list[dict] (same shape as inventory_orders.json)
  BANK_TXNS — list[dict] (same shape as bank_transactions.json → transactions[])
"""

import os
import json
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Supabase helpers ────────────────────────────────────────────────────────

_supabase_client_cache = {"client": None, "failed": False, "checked": False}


_SUPABASE_URL_DEFAULT = "https://xmypdvbfjgpymvygldkk.supabase.co"
_SUPABASE_KEY_DEFAULT = "sb_publishable_RiStZfpt5DF7UdJ1t96mqA_S5fyrr_P"


def _get_supabase_client():
    """Return a Supabase client, or None if credentials are missing or connection failed."""
    # If we already know Supabase is down, don't retry (avoids 60s+ timeout per call)
    if _supabase_client_cache["failed"]:
        return None
    if _supabase_client_cache["checked"]:
        return _supabase_client_cache["client"]

    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    url = os.environ.get("SUPABASE_URL", "") or _SUPABASE_URL_DEFAULT
    key = os.environ.get("SUPABASE_KEY", "") or _SUPABASE_KEY_DEFAULT
    if not url or not key or "YOUR_PROJECT" in url:
        _supabase_client_cache["checked"] = True
        return None

    from supabase import create_client
    client = create_client(url, key)
    _supabase_client_cache["client"] = client
    _supabase_client_cache["checked"] = True
    return client


def _mark_supabase_failed():
    """Mark Supabase as unavailable so subsequent calls skip it instantly."""
    _supabase_client_cache["failed"] = True


def _fetch_all(client, table: str, order_col: str = "id") -> list[dict]:
    """Fetch all rows from a Supabase table, paginating past the 1000-row limit."""
    rows: list[dict] = []
    page_size = 1000
    offset = 0
    while True:
        resp = (
            client.table(table)
            .select("*")
            .order(order_col)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


# ── Supabase loaders ───────────────────────────────────────────────────────

def _load_etsy_from_supabase(client) -> pd.DataFrame:
    """etsy_transactions → DataFrame matching CSV column names."""
    rows = _fetch_all(client, "etsy_transactions")
    if not rows:
        return pd.DataFrame(columns=["Date", "Type", "Title", "Info", "Currency", "Amount", "Fees & Taxes", "Net"])

    df = pd.DataFrame(rows)

    # Rename snake_case columns back to CSV headers
    df = df.rename(columns={
        "date": "Date",
        "type": "Type",
        "title": "Title",
        "info": "Info",
        "currency": "Currency",
        "amount": "Amount",
        "fees_and_taxes": "Fees & Taxes",
        "net": "Net",
        "tax_details": "Tax Details",
        "store": "Store",
    })

    # Drop Supabase-internal columns
    for col in ("id", "created_at", "statement_file"):
        if col in df.columns:
            df = df.drop(columns=[col])

    # Default store for rows that predate multi-store support
    if "Store" not in df.columns:
        df["Store"] = "keycomponentmfg"
    else:
        df["Store"] = df["Store"].fillna("keycomponentmfg").replace("", "keycomponentmfg")

    return df


def _load_config_from_supabase(client) -> dict:
    """config table (key/value JSONB) → dict matching config.json."""
    rows = _fetch_all(client, "config", order_col="key")
    if not rows:
        return {
            "etsy_balance": 0,
            "etsy_pre_capone_deposits": 0,
            "pre_capone_detail": [],
            "draw_reasons": {},
            "best_buy_cc": {"credit_limit": 0, "purchases": [], "payments": []},
        }

    config = {}
    for row in rows:
        val = row["value"]
        # supabase-py usually auto-parses JSONB, but handle string case too
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except (json.JSONDecodeError, ValueError):
                # Plain string value (not JSON) — use as-is
                pass
        config[row["key"]] = val
    return config


def _load_invoices_from_supabase(client) -> list[dict]:
    """inventory_orders + inventory_items → list[dict] matching inventory_orders.json."""
    orders = _fetch_all(client, "inventory_orders")
    items = _fetch_all(client, "inventory_items")

    # Group items by order_num
    items_by_order: dict[str, list[dict]] = {}
    for item in items:
        onum = item["order_num"]
        items_by_order.setdefault(onum, []).append({
            "name": item["name"],
            "qty": item.get("qty", 1),
            "price": float(item["price"]) if item["price"] is not None else 0.0,
            "seller": item.get("seller", "Unknown"),
            "ship_to": item.get("ship_to", ""),
            "image_url": item.get("image_url", ""),
        })

    invoices = []
    for o in orders:
        invoices.append({
            "order_num": o["order_num"],
            "date": o["date"],
            "grand_total": float(o["grand_total"]) if o["grand_total"] is not None else 0.0,
            "subtotal": float(o["subtotal"]) if o["subtotal"] is not None else 0.0,
            "tax": float(o["tax"]) if o["tax"] is not None else 0.0,
            "source": o.get("source", ""),
            "file": o.get("file", ""),
            "ship_address": o.get("ship_address", ""),
            "payment_method": o.get("payment_method", "Unknown"),
            "items": items_by_order.get(o["order_num"], []),
        })
    return invoices


def _load_bank_txns_from_supabase(client) -> list[dict]:
    """bank_transactions → list[dict] matching bank_transactions.json → transactions[]."""
    rows = _fetch_all(client, "bank_transactions")

    txns = []
    for r in rows:
        txns.append({
            "date": r["date"],
            "desc": r["description"],
            "amount": float(r["amount"]) if r["amount"] is not None else 0.0,
            "type": r["type"],
            "category": r.get("category", ""),
            "source_file": r.get("source_file", ""),
            "raw_desc": r.get("raw_description", ""),
        })
    return txns


def load_inventory_items_with_ids() -> list[dict]:
    """Fetch raw inventory_items rows (with id and image_url) for Image Manager saves."""
    client = _get_supabase_client()
    if client is None:
        return []
    return _fetch_all(client, "inventory_items")


def save_image_url(item_name: str, image_url: str) -> int:
    """Write image_url to all inventory_items rows matching the given item name.
    Returns the number of rows updated."""
    client = _get_supabase_client()
    if client is None:
        return 0
    resp = client.table("inventory_items").update({"image_url": image_url}).eq("name", item_name).execute()
    count = len(resp.data) if resp.data else 0
    # If 0 rows updated (renamed item), save as image override in config
    if count == 0:
        save_image_override(item_name, image_url)
    return count


def load_image_overrides() -> dict:
    """Load image URL overrides from config table (for renamed items).
    Returns dict of {display_name: image_url}."""
    client = _get_supabase_client()
    if client is None:
        return {}
    try:
        resp = client.table("config").select("value").eq("key", "image_overrides").execute()
        if resp.data and resp.data[0].get("value"):
            val = resp.data[0]["value"]
            if isinstance(val, str):
                return json.loads(val)
            return val
    except Exception:
        pass
    return {}


def save_image_override(item_name: str, image_url: str) -> bool:
    """Save an image URL override for a renamed item (persists in config table)."""
    client = _get_supabase_client()
    if client is None:
        return False
    try:
        # Load current overrides
        current = load_image_overrides()
        if image_url:
            current[item_name] = image_url
        else:
            current.pop(item_name, None)
        # Upsert to config table
        client.table("config").upsert(
            {"key": "image_overrides", "value": current},
            on_conflict="key",
        ).execute()
        return True
    except Exception:
        return False


# ── Location Override helpers ─────────────────────────────────────────────

def load_location_overrides() -> list[dict]:
    """Fetch all rows from inventory_location_overrides. Returns list of dicts."""
    client = _get_supabase_client()
    if client is None:
        return []
    try:
        return _fetch_all(client, "inventory_location_overrides")
    except Exception:
        return []


def save_location_override(order_num: str, item_name: str, overrides: list[dict]) -> bool:
    """Replace overrides for (order_num, item_name).

    Parameters
    ----------
    overrides : list of {"location": str, "qty": int}
        1 entry = simple reassign, 2 entries = split between locations.
    """
    client = _get_supabase_client()
    if client is None:
        return False
    # Delete existing rows for this item
    client.table("inventory_location_overrides") \
        .delete() \
        .eq("order_num", order_num) \
        .eq("item_name", item_name) \
        .execute()
    # Insert new rows
    rows = [
        {"order_num": order_num, "item_name": item_name,
         "location": o["location"], "qty": o["qty"]}
        for o in overrides
    ]
    if rows:
        client.table("inventory_location_overrides").insert(rows).execute()
    return True


def delete_location_override(order_num: str, item_name: str) -> bool:
    """Delete all override rows for (order_num, item_name), reverting to original."""
    client = _get_supabase_client()
    if client is None:
        return False
    client.table("inventory_location_overrides") \
        .delete() \
        .eq("order_num", order_num) \
        .eq("item_name", item_name) \
        .execute()
    return True


# ── Item Detail helpers (rename / categorize / true qty) ──────────────────

def load_item_details() -> list[dict]:
    """Fetch all rows from inventory_item_details. Returns list of dicts."""
    client = _get_supabase_client()
    if client is None:
        return []
    try:
        return _fetch_all(client, "inventory_item_details")
    except Exception:
        return []


def save_item_details(order_num: str, item_name: str, details: list[dict]) -> bool:
    """Replace detail rows for (order_num, item_name).

    Stores all detail entries as a JSON array in a single DB row.
    Uses UPDATE for existing _JSON_ rows to avoid duplicate key race conditions.
    """
    client = _get_supabase_client()
    if client is None:
        return False
    filtered = [d for d in details if d.get("display_name", "").strip()]

    # Find existing rows for this item (by id for safe deletion)
    try:
        existing = client.table("inventory_item_details") \
            .select("id,category") \
            .eq("order_num", order_num) \
            .eq("item_name", item_name) \
            .execute()
        existing_rows = existing.data or []
    except Exception:
        existing_rows = []

    if not filtered:
        # Delete all existing rows
        for r in existing_rows:
            try:
                client.table("inventory_item_details").delete().eq("id", r["id"]).execute()
            except Exception:
                pass
        return True

    total_qty = sum(d.get("true_qty", 1) for d in filtered)
    json_str = json.dumps(filtered)

    # Check if there's already a _JSON_ row we can UPDATE (avoids delete+insert race)
    json_row = next((r for r in existing_rows if r.get("category") == "_JSON_"), None)

    if json_row:
        # UPDATE existing JSON row in place
        client.table("inventory_item_details") \
            .update({"display_name": json_str, "true_qty": total_qty}) \
            .eq("id", json_row["id"]) \
            .execute()
        # Delete any leftover old-format rows
        for r in existing_rows:
            if r["id"] != json_row["id"]:
                try:
                    client.table("inventory_item_details").delete().eq("id", r["id"]).execute()
                except Exception:
                    pass
    else:
        # No JSON row yet — delete old rows by id, then insert new JSON row
        for r in existing_rows:
            try:
                client.table("inventory_item_details").delete().eq("id", r["id"]).execute()
            except Exception:
                pass
        row = {
            "order_num": order_num,
            "item_name": item_name,
            "display_name": json_str,
            "category": "_JSON_",
            "true_qty": total_qty,
        }
        try:
            client.table("inventory_item_details").insert(row).execute()
        except Exception:
            # Race condition or encoding mismatch — find any row and update it
            try:
                resp = client.table("inventory_item_details") \
                    .select("id") \
                    .eq("order_num", order_num) \
                    .eq("item_name", item_name) \
                    .limit(1) \
                    .execute()
                if resp.data:
                    rid = resp.data[0]["id"]
                    client.table("inventory_item_details") \
                        .update({"display_name": json_str, "category": "_JSON_", "true_qty": total_qty}) \
                        .eq("id", rid) \
                        .execute()
                else:
                    return False
            except Exception:
                return False
    return True


def save_item_details_batch(items: list[dict]) -> int:
    """Batch-save multiple item details. Returns count of successful saves.

    Each item dict: {order_num, item_name, details: [{display_name, category, true_qty, location}]}
    Reuses save_item_details() logic per item.
    """
    saved = 0
    for item in items:
        ok = save_item_details(
            item["order_num"],
            item["item_name"],
            item["details"],
        )
        if ok:
            saved += 1
    return saved


def save_new_order(order: dict) -> dict:
    """Insert a new order into Supabase (inventory_orders + inventory_items).
    Used by the receipt upload wizard to persist new orders immediately.

    Returns dict: {ok: bool, status: "created"|"duplicate"|"error", order_num: str, items: int}
    """
    client = _get_supabase_client()
    if client is None:
        print("save_new_order: no Supabase client available")
        return {"ok": False, "status": "error", "order_num": order.get("order_num", ""), "items": 0}
    try:
        # Check for existing order
        existing = (client.table("inventory_orders")
                    .select("order_num")
                    .eq("order_num", order["order_num"])
                    .execute())
        if existing.data:
            return {"ok": True, "status": "duplicate", "order_num": order["order_num"], "items": 0}

        client.table("inventory_orders").insert({
            "order_num": order["order_num"],
            "date": order["date"],
            "grand_total": order["grand_total"],
            "subtotal": order["subtotal"],
            "tax": order["tax"],
            "source": order.get("source", ""),
            "file": order.get("file", ""),
            "ship_address": order.get("ship_address", ""),
            "payment_method": order.get("payment_method", "Unknown"),
        }).execute()
        item_count = 0
        for item in order.get("items", []):
            client.table("inventory_items").insert({
                "order_num": order["order_num"],
                "name": item["name"],
                "qty": item.get("qty", 1),
                "price": item["price"],
                "seller": item.get("seller", "Unknown"),
                "ship_to": item.get("ship_to", ""),
                "image_url": item.get("image_url", ""),
            }).execute()
            item_count += 1
        return {"ok": True, "status": "created", "order_num": order["order_num"], "items": item_count}
    except Exception as e:
        print(f"save_new_order failed: {e}")
        return {"ok": False, "status": "error", "order_num": order.get("order_num", ""), "items": 0, "error": str(e)}


def delete_item_details(order_num: str, item_name: str) -> bool:
    """Delete all detail rows for (order_num, item_name)."""
    client = _get_supabase_client()
    if client is None:
        return False
    client.table("inventory_item_details") \
        .delete() \
        .eq("order_num", order_num) \
        .eq("item_name", item_name) \
        .execute()
    return True


# ── Inventory Usage helpers ────────────────────────────────────────────────

def load_usage_log() -> list[dict]:
    """Fetch all rows from inventory_usage, newest first."""
    client = _get_supabase_client()
    if client is None:
        return []
    try:
        resp = client.table("inventory_usage") \
            .select("*") \
            .order("created_at", desc=True) \
            .execute()
        return resp.data or []
    except Exception:
        return []


def save_usage(item_name: str, qty: int, note: str = "") -> dict | None:
    """Insert one usage row. Returns the inserted row or None."""
    client = _get_supabase_client()
    if client is None:
        return None
    try:
        resp = client.table("inventory_usage").insert({
            "item_name": item_name,
            "qty": qty,
            "note": note,
        }).execute()
        return resp.data[0] if resp.data else None
    except Exception:
        return None


def delete_usage(usage_id: int) -> bool:
    """Delete a usage row by id."""
    client = _get_supabase_client()
    if client is None:
        return False
    try:
        client.table("inventory_usage").delete().eq("id", usage_id).execute()
        return True
    except Exception:
        return False


# ── Quick-Add helpers ──────────────────────────────────────────────────────

def load_quick_adds() -> list[dict]:
    """Fetch all rows from inventory_quick_add, newest first."""
    client = _get_supabase_client()
    if client is None:
        return []
    try:
        resp = client.table("inventory_quick_add") \
            .select("*") \
            .order("created_at", desc=True) \
            .execute()
        return resp.data or []
    except Exception:
        return []


def save_quick_add(data: dict) -> dict | None:
    """Insert one quick-add row. Returns the inserted row or None."""
    client = _get_supabase_client()
    if client is None:
        return None
    try:
        resp = client.table("inventory_quick_add").insert(data).execute()
        return resp.data[0] if resp.data else None
    except Exception:
        return None


def delete_quick_add(qa_id: int) -> bool:
    """Delete a quick-add row by id."""
    client = _get_supabase_client()
    if client is None:
        return False
    try:
        client.table("inventory_quick_add").delete().eq("id", qa_id).execute()
        return True
    except Exception:
        return False


def save_config_value(key: str, value) -> bool:
    """Save or update a config key/value pair in Supabase config table."""
    client = _get_supabase_client()
    if client is None:
        return False
    try:
        # Upsert: insert or update if key exists
        client.table("config").upsert({"key": key, "value": value}).execute()
        try:
            from agents.governance import log_mutation
            log_mutation("config", key, value)
        except ImportError:
            pass
        return True
    except Exception as e:
        print(f"Failed to save config {key}: {e}")
        return False


def get_config_value(key: str, default=None):
    """Get a single config value from Supabase."""
    client = _get_supabase_client()
    if client is None:
        return default
    try:
        result = client.table("config").select("value").eq("key", key).execute()
        if result.data and len(result.data) > 0:
            return result.data[0]["value"]
        return default
    except Exception:
        return default


# ── Local-file loaders (fallback) ──────────────────────────────────────────

def _load_etsy_local() -> pd.DataFrame:
    _empty = pd.DataFrame(columns=["Date", "Type", "Title", "Info", "Currency", "Amount", "Fees & Taxes", "Net"])
    statements_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
    if not os.path.isdir(statements_dir):
        return _empty
    frames = []
    for f in os.listdir(statements_dir):
        if f.startswith("etsy_statement") and f.endswith(".csv"):
            frames.append(pd.read_csv(os.path.join(statements_dir, f)))
    if not frames:
        return _empty
    return pd.concat(frames, ignore_index=True)


def _load_config_local() -> dict:
    path = os.path.join(BASE_DIR, "data", "config.json")
    if not os.path.exists(path):
        return {
            "etsy_balance": 0,
            "etsy_pre_capone_deposits": 0,
            "pre_capone_detail": [],
            "draw_reasons": {},
            "best_buy_cc": {"credit_limit": 0, "purchases": [], "payments": []},
        }
    with open(path) as f:
        return json.load(f)


def _load_invoices_local() -> list[dict]:
    path = os.path.join(BASE_DIR, "data", "generated", "inventory_orders.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def _load_bank_txns_local() -> list[dict]:
    path = os.path.join(BASE_DIR, "data", "generated", "bank_transactions.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f).get("transactions", [])
    return []


# ── Shared post-processing ─────────────────────────────────────────────────

def _parse_money(val):
    """Parse monetary value. Handles both string ($1,234.56) and numeric (1234.56) inputs."""
    if pd.isna(val) or val == "--" or val == "":
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    val = str(val).replace("$", "").replace(",", "").replace('"', "")
    try:
        return float(val)
    except Exception:
        return 0.0


def _add_computed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add Amount_Clean, Net_Clean, Fees_Clean, Date_Parsed, Month, Week."""
    df["Amount_Clean"] = df["Amount"].apply(_parse_money)
    df["Net_Clean"] = df["Net"].apply(_parse_money)
    df["Fees_Clean"] = df["Fees & Taxes"].apply(_parse_money)
    df["Date_Parsed"] = pd.to_datetime(df["Date"], format="%B %d, %Y", errors="coerce")
    df["Month"] = df["Date_Parsed"].dt.to_period("M").astype(str)
    df["Week"] = df["Date_Parsed"].dt.to_period("W").apply(lambda p: p.start_time)
    return df


# ── Auto-sync after uploads ────────────────────────────────────────────────

def sync_bank_transactions(bank_txns: list[dict]) -> bool:
    """Push current bank transactions to Supabase (replaces all rows).
    Called automatically after bank statement uploads."""
    client = _get_supabase_client()
    if client is None:
        return False
    try:
        # Clear existing (paginated — Supabase limits deletes to ~1000 per call)
        while True:
            batch = client.table("bank_transactions").select("id").limit(1000).execute()
            if not batch.data:
                break
            ids = [r["id"] for r in batch.data]
            for i in range(0, len(ids), 500):
                chunk = ids[i:i+500]
                client.table("bank_transactions").delete().in_("id", chunk).execute()
        # Insert in batches
        rows = []
        for t in bank_txns:
            rows.append({
                "date": t["date"],
                "description": t["desc"],
                "amount": t["amount"],
                "type": t["type"],
                "category": t.get("category", ""),
                "source_file": t.get("source_file", ""),
                "raw_description": t.get("raw_desc", ""),
            })
        for i in range(0, len(rows), 500):
            client.table("bank_transactions").insert(rows[i:i+500]).execute()
        print(f"Synced {len(rows)} bank transactions to Supabase")
        return True
    except Exception as e:
        print(f"Bank sync to Supabase failed: {e}")
        return False


def sync_etsy_transactions(data_df) -> bool:
    """Push Etsy transactions to Supabase — only replaces rows for affected stores.
    NEVER deletes data from stores not present in data_df."""
    client = _get_supabase_client()
    if client is None:
        return False
    try:
        # Determine which stores are in this sync
        if "Store" in data_df.columns:
            stores_to_sync = list(data_df["Store"].dropna().unique())
        else:
            stores_to_sync = ["keycomponentmfg"]

        if not stores_to_sync:
            print("WARNING: No stores found in data — skipping sync to prevent data loss")
            return False

        # Safety check: count existing rows before deleting
        total_before = 0
        for store in stores_to_sync:
            count_resp = (client.table("etsy_transactions")
                         .select("id", count="exact")
                         .eq("store", store).execute())
            store_count = count_resp.count if count_resp.count is not None else 0
            total_before += store_count
            print(f"  [{store}] {store_count} existing rows in Supabase")

        # Only delete rows for the specific stores being synced
        for store in stores_to_sync:
            deleted = 0
            while True:
                batch = (client.table("etsy_transactions")
                        .select("id").eq("store", store)
                        .limit(1000).execute())
                if not batch.data:
                    break
                ids = [r["id"] for r in batch.data]
                for i in range(0, len(ids), 500):
                    chunk = ids[i:i+500]
                    client.table("etsy_transactions").delete().in_("id", chunk).execute()
                deleted += len(ids)
            if deleted:
                print(f"  [{store}] Cleared {deleted} old rows")

        # Insert new data in batches
        rows = []
        for _, r in data_df.iterrows():
            row = {
                "date": str(r.get("Date", "")),
                "type": str(r.get("Type", "")),
                "title": str(r.get("Title", "")),
                "info": str(r.get("Info", "")),
                "currency": str(r.get("Currency", "USD")),
                "amount": str(r.get("Amount", "--")),
                "fees_and_taxes": str(r.get("Fees & Taxes", "--")),
                "net": str(r.get("Net", "--")),
                "tax_details": str(r.get("Tax Details", "--")),
                "store": str(r.get("Store", "keycomponentmfg")),
            }
            rows.append(row)
        for i in range(0, len(rows), 500):
            client.table("etsy_transactions").insert(rows[i:i+500]).execute()

        # Verify: count ALL rows across ALL stores
        count_resp = client.table("etsy_transactions").select("id", count="exact").execute()
        sb_total = count_resp.count if count_resp.count is not None else len(count_resp.data)

        # Per-store verification
        for store in stores_to_sync:
            store_count = (client.table("etsy_transactions")
                          .select("id", count="exact")
                          .eq("store", store).execute())
            sc = store_count.count if store_count.count is not None else 0
            expected = len(data_df[data_df["Store"] == store]) if "Store" in data_df.columns else len(data_df)
            if sc != expected:
                print(f"WARNING: [{store}] Supabase has {sc} rows but expected {expected}")
            else:
                print(f"  [{store}] Verified: {sc} rows")

        print(f"Synced {len(rows)} rows for {stores_to_sync}. Total in Supabase: {sb_total}")
        return True
    except Exception as e:
        print(f"Etsy sync to Supabase failed: {e}")
        return False


def append_etsy_transactions(data_df) -> dict:
    """Add new Etsy rows to Supabase WITHOUT deleting existing data.
    Used by Railway uploads where only the new CSV is on disk.

    Uses rank-based dedup: counts how many times each (date,type,title,info,amount,net)
    key appears in Supabase vs the new CSV. Only inserts rows that exceed the existing
    count for that key. This preserves legitimate duplicate listing fees ($0.20 each)
    while preventing true cross-upload duplicates.

    Returns dict: {ok: bool, added: int, skipped: int, total: int}
    """
    client = _get_supabase_client()
    if client is None:
        print("append_etsy: no Supabase client")
        return {"ok": False, "added": 0, "skipped": 0, "total": 0, "error": "No Supabase client"}
    try:
        from collections import Counter
        total_new = len(data_df)
        # Fetch ALL existing rows to count duplicates per key (paginated)
        # Detect if 'store' column exists
        _has_store = True
        try:
            client.table("etsy_transactions").select("store").limit(1).execute()
        except Exception:
            _has_store = False

        existing_counts = Counter()
        _offset = 0
        _select_cols = "date,type,title,info,amount,net" + (",store" if _has_store else "")
        while True:
            batch = (client.table("etsy_transactions")
                     .select(_select_cols)
                     .order("id")
                     .range(_offset, _offset + 999)
                     .execute())
            for r in batch.data:
                key = (r.get("date", ""), r.get("type", ""), r.get("title", ""),
                       r.get("info", ""), r.get("amount", ""), r.get("net", ""),
                       r.get("store", "keycomponentmfg") if _has_store else "keycomponentmfg")
                existing_counts[key] += 1
            if len(batch.data) < 1000:
                break
            _offset += 1000
        print(f"append_etsy: found {sum(existing_counts.values())} existing rows, {len(existing_counts)} unique keys")

        # Count occurrences of each key in the new data
        new_counts = Counter()
        new_rows_by_key = {}
        for _, r in data_df.iterrows():
            key = (str(r.get("Date", "")), str(r.get("Type", "")), str(r.get("Title", "")),
                   str(r.get("Info", "")), str(r.get("Amount", "")), str(r.get("Net", "")),
                   str(r.get("Store", "keycomponentmfg")))
            new_counts[key] += 1
            if key not in new_rows_by_key:
                _row = {
                    "date": str(r.get("Date", "")),
                    "type": str(r.get("Type", "")),
                    "title": str(r.get("Title", "")),
                    "info": str(r.get("Info", "")),
                    "currency": str(r.get("Currency", "USD")),
                    "amount": str(r.get("Amount", "--")),
                    "fees_and_taxes": str(r.get("Fees & Taxes", "--")),
                    "net": str(r.get("Net", "--")),
                    "tax_details": str(r.get("Tax Details", "--")),
                }
                if _has_store:
                    _row["store"] = str(r.get("Store", "keycomponentmfg"))
                new_rows_by_key[key] = _row

        # Insert only the EXCESS rows: new_count - existing_count for each key
        rows = []
        for key, new_count in new_counts.items():
            excess = new_count - existing_counts.get(key, 0)
            if excess > 0:
                row_data = new_rows_by_key[key]
                rows.extend([row_data.copy() for _ in range(excess)])

        added = len(rows)
        skipped = total_new - added
        if rows:
            for i in range(0, len(rows), 500):
                client.table("etsy_transactions").insert(rows[i:i+500]).execute()
            print(f"Appended {added} new Etsy rows to Supabase ({skipped} duplicates skipped)")
        else:
            print("No new Etsy rows to append (all duplicates)")
        return {"ok": True, "added": added, "skipped": skipped, "total": total_new}
    except Exception as e:
        print(f"Etsy append to Supabase failed: {e}")
        return {"ok": False, "added": 0, "skipped": 0, "total": len(data_df), "error": str(e)}


def append_bank_transactions(bank_txns: list[dict]) -> dict:
    """Add new bank transactions to Supabase WITHOUT deleting existing data.
    Used by Railway uploads where only the new PDF is on disk.

    Returns dict: {ok: bool, added: int, skipped: int, total: int}
    """
    client = _get_supabase_client()
    if client is None:
        print("append_bank: no Supabase client")
        return {"ok": False, "added": 0, "skipped": 0, "total": len(bank_txns), "error": "No Supabase client"}
    try:
        total_new = len(bank_txns)
        # Fetch existing to avoid duplicates (paginated)
        existing_keys = set()
        _offset = 0
        while True:
            batch = (client.table("bank_transactions")
                     .select("date,description,amount,type")
                     .order("id")
                     .range(_offset, _offset + 999)
                     .execute())
            for r in batch.data:
                existing_keys.add((r.get("date", ""), f"{float(r.get('amount', 0)):.2f}", r.get("type", ""),
                                   r.get("description", "")))
            if len(batch.data) < 1000:
                break
            _offset += 1000

        rows = []
        for t in bank_txns:
            key = (t.get("date", ""), f"{float(t.get('amount', 0)):.2f}", t.get("type", ""), t.get("desc", ""))
            if key not in existing_keys:
                rows.append({
                    "date": t["date"],
                    "description": t["desc"],
                    "amount": t["amount"],
                    "type": t["type"],
                    "category": t.get("category", ""),
                    "source_file": t.get("source_file", ""),
                    "raw_description": t.get("raw_desc", ""),
                })
        added = len(rows)
        skipped = total_new - added
        if rows:
            for i in range(0, len(rows), 500):
                client.table("bank_transactions").insert(rows[i:i+500]).execute()
            print(f"Appended {added} new bank transactions to Supabase ({skipped} duplicates skipped)")
        else:
            print("No new bank transactions to append (all duplicates)")
        return {"ok": True, "added": added, "skipped": skipped, "total": total_new}
    except Exception as e:
        print(f"Bank append to Supabase failed: {e}")
        return {"ok": False, "added": 0, "skipped": 0, "total": len(bank_txns), "error": str(e)}


# ── Delete Functions ────────────────────────────────────────────────────────

def delete_etsy_by_month(year: int, month: int, store: str = None) -> dict:
    """Delete etsy_transactions for a specific month, optionally filtered by store.

    Returns dict: {ok: bool, deleted: int}
    """
    client = _get_supabase_client()
    if client is None:
        return {"ok": False, "deleted": 0, "error": "No Supabase client"}
    try:
        month_prefix = f"{year}-{month:02d}"
        # Etsy dates are stored as "Month DD, YYYY" strings — but some may be YYYY-MM-DD
        # Fetch all rows, filter by month (and store if given), collect IDs to delete
        ids_to_delete = []
        _offset = 0
        while True:
            _q = (client.table("etsy_transactions")
                     .select("id,date,store")
                     .order("id")
                     .range(_offset, _offset + 999))
            if store:
                _q = _q.eq("store", store)
            batch = _q.execute()
            for r in batch.data:
                d = r.get("date", "")
                # Try parsing "Month DD, YYYY" format
                try:
                    from datetime import datetime as _dt
                    parsed = _dt.strptime(d, "%B %d, %Y")
                    if parsed.year == year and parsed.month == month:
                        ids_to_delete.append(r["id"])
                        continue
                except (ValueError, TypeError):
                    pass
                # Try YYYY-MM-DD format
                if d.startswith(month_prefix):
                    ids_to_delete.append(r["id"])
            if len(batch.data) < 1000:
                break
            _offset += 1000

        if ids_to_delete:
            for i in range(0, len(ids_to_delete), 100):
                batch_ids = ids_to_delete[i:i+100]
                client.table("etsy_transactions").delete().in_("id", batch_ids).execute()
            print(f"Deleted {len(ids_to_delete)} Etsy transactions for {month_prefix}")
        return {"ok": True, "deleted": len(ids_to_delete)}
    except Exception as e:
        print(f"delete_etsy_by_month failed: {e}")
        return {"ok": False, "deleted": 0, "error": str(e)}


def delete_bank_by_month(year: int, month: int) -> dict:
    """Delete all bank_transactions for a specific month.

    Returns dict: {ok: bool, deleted: int}
    """
    client = _get_supabase_client()
    if client is None:
        return {"ok": False, "deleted": 0, "error": "No Supabase client"}
    try:
        month_prefix = f"{year}-{month:02d}"
        ids_to_delete = []
        _offset = 0
        while True:
            batch = (client.table("bank_transactions")
                     .select("id,date")
                     .order("id")
                     .range(_offset, _offset + 999)
                     .execute())
            for r in batch.data:
                d = r.get("date", "")
                if d.startswith(month_prefix):
                    ids_to_delete.append(r["id"])
            if len(batch.data) < 1000:
                break
            _offset += 1000

        if ids_to_delete:
            for i in range(0, len(ids_to_delete), 100):
                batch_ids = ids_to_delete[i:i+100]
                client.table("bank_transactions").delete().in_("id", batch_ids).execute()
            print(f"Deleted {len(ids_to_delete)} bank transactions for {month_prefix}")
        return {"ok": True, "deleted": len(ids_to_delete)}
    except Exception as e:
        print(f"delete_bank_by_month failed: {e}")
        return {"ok": False, "deleted": 0, "error": str(e)}


def delete_receipt_by_order(order_num: str) -> dict:
    """Delete an inventory order and its items by order number.

    Returns dict: {ok: bool, deleted_items: int}
    """
    client = _get_supabase_client()
    if client is None:
        return {"ok": False, "deleted_items": 0, "error": "No Supabase client"}
    try:
        # Count items first
        items = (client.table("inventory_items")
                 .select("id")
                 .eq("order_num", order_num)
                 .execute())
        item_count = len(items.data) if items.data else 0

        # Delete items, then order
        client.table("inventory_items").delete().eq("order_num", order_num).execute()
        client.table("inventory_orders").delete().eq("order_num", order_num).execute()
        # Also delete any item details
        try:
            client.table("inventory_item_details").delete().eq("order_num", order_num).execute()
        except Exception:
            pass
        print(f"Deleted order #{order_num} ({item_count} items)")
        return {"ok": True, "deleted_items": item_count}
    except Exception as e:
        print(f"delete_receipt_by_order failed: {e}")
        return {"ok": False, "deleted_items": 0, "error": str(e)}


def get_etsy_month_counts() -> list[dict]:
    """Get row counts per month for Etsy transactions.

    Returns list of {month: "YYYY-MM", count: N} sorted by month.
    """
    client = _get_supabase_client()
    if client is None:
        return []
    try:
        from collections import Counter
        month_counts = Counter()
        _offset = 0
        while True:
            batch = (client.table("etsy_transactions")
                     .select("date")
                     .order("id")
                     .range(_offset, _offset + 999)
                     .execute())
            for r in batch.data:
                d = r.get("date", "")
                try:
                    from datetime import datetime as _dt
                    parsed = _dt.strptime(d, "%B %d, %Y")
                    month_counts[f"{parsed.year}-{parsed.month:02d}"] += 1
                    continue
                except (ValueError, TypeError):
                    pass
                if len(d) >= 7:
                    month_counts[d[:7]] += 1
            if len(batch.data) < 1000:
                break
            _offset += 1000
        return [{"month": m, "count": c} for m, c in sorted(month_counts.items())]
    except Exception as e:
        print(f"get_etsy_month_counts failed: {e}")
        return []


def get_bank_month_counts() -> list[dict]:
    """Get row counts per month for bank transactions.

    Returns list of {month: "YYYY-MM", count: N} sorted by month.
    """
    client = _get_supabase_client()
    if client is None:
        return []
    try:
        from collections import Counter
        month_counts = Counter()
        _offset = 0
        while True:
            batch = (client.table("bank_transactions")
                     .select("date")
                     .order("id")
                     .range(_offset, _offset + 999)
                     .execute())
            for r in batch.data:
                d = r.get("date", "")
                if len(d) >= 7:
                    month_counts[d[:7]] += 1
            if len(batch.data) < 1000:
                break
            _offset += 1000
        return [{"month": m, "count": c} for m, c in sorted(month_counts.items())]
    except Exception as e:
        print(f"get_bank_month_counts failed: {e}")
        return []


# ── Public API ──────────────────────────────────────────────────────────────

def load_data() -> dict:
    """
    Load all dashboard data.  Tries Supabase first; falls back to local files.

    Returns
    -------
    dict with keys: DATA (DataFrame), CONFIG (dict), INVOICES (list), BANK_TXNS (list)
    """
    client = None
    try:
        client = _get_supabase_client()
    except Exception:
        pass

    # ── Try Supabase ────────────────────────────────────────────────────
    if client is not None:
        try:
            data_df = _load_etsy_from_supabase(client)
            config = _load_config_from_supabase(client)
            invoices = _load_invoices_from_supabase(client)
            bank_txns = _load_bank_txns_from_supabase(client)
            data_df = _add_computed_columns(data_df)
            print("Loaded data from Supabase")
            return {
                "DATA": data_df,
                "CONFIG": config,
                "INVOICES": invoices,
                "BANK_TXNS": bank_txns,
            }
        except Exception as e:
            print(f"Supabase load failed ({e}), falling back to local files")
            _mark_supabase_failed()  # skip Supabase for all subsequent calls

    # ── Fallback to local files ─────────────────────────────────────────
    data_df = _load_etsy_local()
    data_df = _add_computed_columns(data_df)
    config = _load_config_local()
    invoices = _load_invoices_local()
    bank_txns = _load_bank_txns_local()
    print("Loaded data from local files")
    return {
        "DATA": data_df,
        "CONFIG": config,
        "INVOICES": invoices,
        "BANK_TXNS": bank_txns,
    }
