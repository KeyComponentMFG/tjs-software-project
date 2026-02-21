"""
migrate_to_supabase.py — Upload local CSV/JSON data to Supabase.

Reads the same local files the dashboard uses, then pushes them to Supabase
tables.  Idempotent: clears tables before inserting so you can re-run safely.

Usage:
    python migrate_to_supabase.py
"""

import os
import sys
import json
import csv
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY or "YOUR_PROJECT" in SUPABASE_URL:
    print("ERROR: Set SUPABASE_URL and SUPABASE_KEY in .env first.")
    sys.exit(1)

from supabase import create_client

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

BATCH_SIZE = 500


def batch_insert(table: str, rows: list[dict]):
    """Insert rows in batches of BATCH_SIZE."""
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        sb.table(table).insert(chunk).execute()
    print(f"  Inserted {len(rows)} rows into {table}")


def clear_table(table: str):
    """Delete all rows from a table."""
    # Delete where id > 0 covers BIGSERIAL tables; for config use key != ''
    try:
        sb.table(table).delete().gt("id", 0).execute()
    except Exception:
        # config table uses 'key' as PK, not id
        sb.table(table).delete().neq("key", "").execute()


# ── 1. Etsy transactions ────────────────────────────────────────────────────

def migrate_etsy_transactions():
    print("\n[1/5] Etsy Transactions")
    clear_table("etsy_transactions")

    statements_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
    rows = []
    for fname in sorted(os.listdir(statements_dir)):
        if not (fname.startswith("etsy_statement") and fname.endswith(".csv")):
            continue
        path = os.path.join(statements_dir, fname)
        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({
                    "date": r["Date"],
                    "type": r["Type"],
                    "title": r.get("Title", ""),
                    "info": r.get("Info", ""),
                    "currency": r.get("Currency", "USD"),
                    "amount": r.get("Amount", "--"),
                    "fees_and_taxes": r.get("Fees & Taxes", "--"),
                    "net": r.get("Net", "--"),
                    "tax_details": r.get("Tax Details", "--"),
                    "statement_file": fname,
                })

    batch_insert("etsy_transactions", rows)
    return len(rows)


# ── 2. Inventory orders + items ─────────────────────────────────────────────

def _save_image_urls() -> dict[str, str]:
    """Snapshot existing image_url values before clearing inventory_items."""
    try:
        rows = []
        page_size = 1000
        offset = 0
        while True:
            resp = (
                sb.table("inventory_items")
                .select("name, image_url")
                .order("id")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = resp.data
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        mapping = {}
        for r in rows:
            url = r.get("image_url") or ""
            if url:
                mapping[r["name"]] = url
        print(f"  Saved {len(mapping)} image_url mappings")
        return mapping
    except Exception as e:
        print(f"  Could not save image_urls ({e}), continuing without them")
        return {}


def _restore_image_urls(mapping: dict[str, str]):
    """Re-apply saved image_url values after re-inserting inventory_items."""
    if not mapping:
        return
    restored = 0
    for name, url in mapping.items():
        try:
            sb.table("inventory_items").update({"image_url": url}).eq("name", name).execute()
            restored += 1
        except Exception:
            pass
    print(f"  Restored {restored}/{len(mapping)} image_url mappings")


def migrate_inventory():
    print("\n[2/5] Inventory Orders")

    # Save image_url mappings before clearing
    image_urls = _save_image_urls()

    clear_table("inventory_items")  # clear child first (no FK but good practice)
    clear_table("inventory_orders")

    inv_path = os.path.join(BASE_DIR, "data", "generated", "inventory_orders.json")
    if not os.path.exists(inv_path):
        print("  WARNING: inventory_orders.json not found — skipping")
        return 0, 0

    with open(inv_path) as f:
        invoices = json.load(f)

    order_rows = []
    item_rows = []
    for inv in invoices:
        order_rows.append({
            "order_num": inv["order_num"],
            "date": inv["date"],
            "grand_total": inv["grand_total"],
            "subtotal": inv["subtotal"],
            "tax": inv["tax"],
            "source": inv["source"],
            "file": inv["file"],
            "ship_address": inv.get("ship_address", ""),
            "payment_method": inv.get("payment_method", "Unknown"),
        })
        for item in inv["items"]:
            item_rows.append({
                "order_num": inv["order_num"],
                "name": item["name"],
                "qty": item.get("qty", 1),
                "price": item["price"],
                "seller": item.get("seller", "Unknown"),
                "ship_to": item.get("ship_to", ""),
            })

    batch_insert("inventory_orders", order_rows)
    print("\n[3/5] Inventory Items")
    batch_insert("inventory_items", item_rows)

    # Restore image_url mappings
    _restore_image_urls(image_urls)

    return len(order_rows), len(item_rows)


# ── 3. Bank transactions ────────────────────────────────────────────────────

def migrate_bank_transactions():
    print("\n[4/5] Bank Transactions")
    clear_table("bank_transactions")

    bank_path = os.path.join(BASE_DIR, "data", "generated", "bank_transactions.json")
    if not os.path.exists(bank_path):
        print("  WARNING: bank_transactions.json not found — skipping")
        return 0

    with open(bank_path) as f:
        data = json.load(f)

    txns = data.get("transactions", [])
    rows = []
    for t in txns:
        rows.append({
            "date": t["date"],
            "description": t["desc"],
            "amount": t["amount"],
            "type": t["type"],
            "category": t.get("category", ""),
            "source_file": t.get("source_file", ""),
            "raw_description": t.get("raw_desc", ""),
        })

    batch_insert("bank_transactions", rows)
    return len(rows)


# ── 4. Config (key-value) ───────────────────────────────────────────────────

def migrate_config():
    print("\n[5/5] Config")
    clear_table("config")

    config_path = os.path.join(BASE_DIR, "data", "config.json")
    if not os.path.exists(config_path):
        print("  WARNING: config.json not found — skipping")
        return 0

    with open(config_path) as f:
        config = json.load(f)

    rows = []
    for key, value in config.items():
        rows.append({
            "key": key,
            "value": json.dumps(value),  # store as JSON string for JSONB column
        })

    batch_insert("config", rows)
    return len(rows)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  MIGRATE LOCAL DATA -> SUPABASE")
    print("=" * 60)

    etsy_count = migrate_etsy_transactions()
    order_count, item_count = migrate_inventory()
    bank_count = migrate_bank_transactions()
    config_count = migrate_config()

    print("\n" + "=" * 60)
    print("  MIGRATION COMPLETE")
    print("=" * 60)
    print(f"  etsy_transactions: {etsy_count} rows")
    print(f"  inventory_orders:  {order_count} rows")
    print(f"  inventory_items:   {item_count} rows")
    print(f"  bank_transactions: {bank_count} rows")
    print(f"  config:            {config_count} rows")

    # Auto-reload Railway if it's running
    railway_url = os.environ.get("RAILWAY_URL", "https://web-production-7f385.up.railway.app")
    print(f"\n  Pinging {railway_url}/api/reload to refresh Railway data...")
    try:
        import urllib.request
        req = urllib.request.Request(f"{railway_url}/api/reload", method="GET")
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
            print(f"  Railway reload: {result}")
    except Exception as e:
        print(f"  Railway reload failed (may need manual restart): {e}")


if __name__ == "__main__":
    main()
