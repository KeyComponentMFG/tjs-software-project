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

def _get_supabase_client():
    """Return a Supabase client, or None if credentials are missing."""
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key or "YOUR_PROJECT" in url:
        return None

    from supabase import create_client
    return create_client(url, key)


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
        raise ValueError("etsy_transactions table is empty")

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
        raise ValueError("config table is empty")

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


# ── Local-file loaders (fallback) ──────────────────────────────────────────

def _load_etsy_local() -> pd.DataFrame:
    frames = []
    statements_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
    for f in os.listdir(statements_dir):
        if f.startswith("etsy_statement") and f.endswith(".csv"):
            frames.append(pd.read_csv(os.path.join(statements_dir, f)))
    return pd.concat(frames, ignore_index=True)


def _load_config_local() -> dict:
    with open(os.path.join(BASE_DIR, "data", "config.json")) as f:
        return json.load(f)


def _load_invoices_local() -> list[dict]:
    path = os.path.join(BASE_DIR, "data", "generated", "inventory_orders.json")
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
