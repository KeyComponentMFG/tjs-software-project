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
    })

    # Drop Supabase-internal columns
    for col in ("id", "created_at", "statement_file"):
        if col in df.columns:
            df = df.drop(columns=[col])

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
            val = json.loads(val)
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


def save_new_order(order: dict) -> bool:
    """Insert a new order into Supabase (inventory_orders + inventory_items).
    Used by the receipt upload wizard to persist new orders immediately."""
    client = _get_supabase_client()
    if client is None:
        print("save_new_order: no Supabase client available")
        return False
    try:
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
        return True
    except Exception as e:
        print(f"save_new_order failed: {e}")
        return False


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
    if pd.isna(val) or val == "--" or val == "":
        return 0.0
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
        # Clear existing
        client.table("bank_transactions").delete().gt("id", 0).execute()
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
    """Push current Etsy transactions to Supabase (replaces all rows).
    Called automatically after Etsy CSV uploads."""
    client = _get_supabase_client()
    if client is None:
        return False
    try:
        # Clear existing
        client.table("etsy_transactions").delete().gt("id", 0).execute()
        # Insert in batches
        rows = []
        for _, r in data_df.iterrows():
            rows.append({
                "date": str(r.get("Date", "")),
                "type": str(r.get("Type", "")),
                "title": str(r.get("Title", "")),
                "info": str(r.get("Info", "")),
                "currency": str(r.get("Currency", "USD")),
                "amount": str(r.get("Amount", "--")),
                "fees_and_taxes": str(r.get("Fees & Taxes", "--")),
                "net": str(r.get("Net", "--")),
                "tax_details": str(r.get("Tax Details", "--")),
            })
        for i in range(0, len(rows), 500):
            client.table("etsy_transactions").insert(rows[i:i+500]).execute()
        print(f"Synced {len(rows)} Etsy transactions to Supabase")
        return True
    except Exception as e:
        print(f"Etsy sync to Supabase failed: {e}")
        return False


def append_etsy_transactions(data_df) -> bool:
    """Add new Etsy rows to Supabase WITHOUT deleting existing data.
    Used by Railway uploads where only the new CSV is on disk."""
    client = _get_supabase_client()
    if client is None:
        print("append_etsy: no Supabase client")
        return False
    try:
        # Fetch existing dates to avoid duplicates
        existing = client.table("etsy_transactions").select("date,type,title,amount,net").execute()
        existing_keys = set()
        for r in existing.data:
            existing_keys.add((r.get("date", ""), r.get("type", ""), r.get("title", ""),
                               r.get("amount", ""), r.get("net", "")))

        rows = []
        for _, r in data_df.iterrows():
            key = (str(r.get("Date", "")), str(r.get("Type", "")), str(r.get("Title", "")),
                   str(r.get("Amount", "")), str(r.get("Net", "")))
            if key not in existing_keys:
                rows.append({
                    "date": str(r.get("Date", "")),
                    "type": str(r.get("Type", "")),
                    "title": str(r.get("Title", "")),
                    "info": str(r.get("Info", "")),
                    "currency": str(r.get("Currency", "USD")),
                    "amount": str(r.get("Amount", "--")),
                    "fees_and_taxes": str(r.get("Fees & Taxes", "--")),
                    "net": str(r.get("Net", "--")),
                    "tax_details": str(r.get("Tax Details", "--")),
                })
        if rows:
            for i in range(0, len(rows), 500):
                client.table("etsy_transactions").insert(rows[i:i+500]).execute()
            print(f"Appended {len(rows)} new Etsy rows to Supabase")
        else:
            print("No new Etsy rows to append (all duplicates)")
        return True
    except Exception as e:
        print(f"Etsy append to Supabase failed: {e}")
        return False


def append_bank_transactions(bank_txns: list[dict]) -> bool:
    """Add new bank transactions to Supabase WITHOUT deleting existing data.
    Used by Railway uploads where only the new PDF is on disk."""
    client = _get_supabase_client()
    if client is None:
        print("append_bank: no Supabase client")
        return False
    try:
        # Fetch existing to avoid duplicates
        existing = client.table("bank_transactions").select("date,description,amount,type").execute()
        existing_keys = set()
        for r in existing.data:
            existing_keys.add((r.get("date", ""), str(r.get("amount", 0)), r.get("type", ""),
                               r.get("description", "")))

        rows = []
        for t in bank_txns:
            key = (t.get("date", ""), str(t.get("amount", 0)), t.get("type", ""), t.get("desc", ""))
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
        if rows:
            for i in range(0, len(rows), 500):
                client.table("bank_transactions").insert(rows[i:i+500]).execute()
            print(f"Appended {len(rows)} new bank transactions to Supabase")
        else:
            print("No new bank transactions to append (all duplicates)")
        return True
    except Exception as e:
        print(f"Bank append to Supabase failed: {e}")
        return False


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
