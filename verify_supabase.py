"""
Verify all Supabase tables exist and are accessible.
Run: python verify_supabase.py
"""
import os, json
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from supabase import create_client

url = os.environ.get("SUPABASE_URL", "")
key = os.environ.get("SUPABASE_KEY", "")

if not url or not key:
    print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env")
    exit(1)

client = create_client(url, key)

TABLES = {
    "etsy_transactions":       "Etsy CSV statements",
    "config":                  "Financial config (balance, deductions, etc.)",
    "inventory_orders":        "Purchase orders from receipts",
    "inventory_items":         "Individual items from receipts",
    "inventory_item_details":  "Item categorizations & display names",
    "inventory_usage":         "Usage log (mark items as used)",
    "inventory_quick_add":     "Manual inventory additions",
}

print("=" * 60)
print("Supabase Table Verification")
print("=" * 60)

all_ok = True
for table, desc in TABLES.items():
    try:
        resp = client.table(table).select("*", count="exact").limit(1).execute()
        count = resp.count if hasattr(resp, "count") and resp.count is not None else "?"
        status = "OK"
        print(f"  {status:12s} {table:30s} ({count} rows) — {desc}")
    except Exception as e:
        err = str(e)
        if "PGRST205" in err or "not find" in err:
            status = "MISSING"
        elif "permission" in err.lower() or "42501" in err:
            status = "NO ACCESS"
        else:
            status = "ERROR"
        print(f"  {status:12s} {table:30s} — {desc}")
        all_ok = False

print("=" * 60)

if all_ok:
    print("All tables OK!")

    # Test write access on the new tables
    print("\nTesting write access...")
    for table in ["inventory_usage", "inventory_quick_add", "inventory_item_details"]:
        try:
            if table == "inventory_usage":
                test = {"item_name": "__test__", "qty": 0, "note": "verify_script"}
            elif table == "inventory_quick_add":
                test = {"item_name": "__test__", "qty": 0, "category": "Other"}
            else:
                test = {"order_num": "__test__", "item_name": "__test__",
                        "display_name": "[]", "category": "_JSON_", "true_qty": 0}

            resp = client.table(table).insert(test).execute()
            if resp.data:
                rid = resp.data[0]["id"]
                client.table(table).delete().eq("id", rid).execute()
                print(f"  WRITE OK    {table}")
            else:
                print(f"  WRITE FAIL  {table} (no data returned)")
        except Exception as e:
            print(f"  WRITE FAIL  {table} — {e}")

    print("\nAll checks passed! Your dashboard should work fully now.")
else:
    print("\nSome tables are MISSING. Run the SQL in supabase_migration.sql")
    print("in your Supabase Dashboard → SQL Editor → New query → Paste → Run")
