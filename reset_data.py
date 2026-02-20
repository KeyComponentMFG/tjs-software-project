"""
reset_data.py — Reset dashboard to zero state for testing the upload flow.

Backs up all data files into data/_backup_YYYYMMDD_HHMMSS/ (preserving folder
structure), then clears all 9 Supabase tables.  Run this, then restart the
dashboard to start with a completely empty state.

Usage:  python reset_data.py
"""

import os
import shutil
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Folders that contain user data files (relative to data/)
DATA_FOLDERS = [
    "etsy_statements",
    os.path.join("invoices", "keycomp"),
    os.path.join("invoices", "personal_amazon"),
    os.path.join("invoices", "other_receipts"),
    "bank_statements",
    "generated",
]

# All Supabase tables the dashboard uses
SUPABASE_TABLES = [
    "etsy_transactions",
    "inventory_items",        # delete items before orders (FK)
    "inventory_orders",
    "bank_transactions",
    "config",
    "inventory_item_details",
    "inventory_location_overrides",
    "inventory_usage",
    "inventory_quick_add",
]


def backup_files():
    """Move all data files to a timestamped backup folder."""
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_root = os.path.join(DATA_DIR, f"_backup_{stamp}")

    moved = 0
    for folder in DATA_FOLDERS:
        src_dir = os.path.join(DATA_DIR, folder)
        if not os.path.isdir(src_dir):
            continue
        dst_dir = os.path.join(backup_root, folder)
        for fn in os.listdir(src_dir):
            fpath = os.path.join(src_dir, fn)
            if not os.path.isfile(fpath):
                continue
            os.makedirs(dst_dir, exist_ok=True)
            shutil.move(fpath, os.path.join(dst_dir, fn))
            moved += 1

    return backup_root, moved


def clear_supabase():
    """Delete all rows from every dashboard Supabase table."""
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key or "YOUR_PROJECT" in url:
        print("  Supabase credentials not configured — skipping table clearing.")
        return 0

    from supabase import create_client
    client = create_client(url, key)

    cleared = 0
    for table in SUPABASE_TABLES:
        try:
            # Delete all rows (neq id 0 matches everything)
            client.table(table).delete().neq("id", -999999).execute()
            cleared += 1
            print(f"  Cleared {table}")
        except Exception as e:
            print(f"  Failed to clear {table}: {e}")

    return cleared


def main():
    print("=" * 50)
    print("  RESET DASHBOARD TO ZERO STATE")
    print("=" * 50)

    # 1. Backup files
    print("\n1. Backing up data files...")
    backup_path, file_count = backup_files()
    if file_count:
        print(f"   Moved {file_count} files to:\n   {backup_path}")
    else:
        print("   No data files found to back up.")

    # 2. Clear Supabase
    print("\n2. Clearing Supabase tables...")
    table_count = clear_supabase()
    print(f"   Cleared {table_count}/{len(SUPABASE_TABLES)} tables.")

    # 3. Summary
    print("\n" + "=" * 50)
    print("  DONE!")
    print(f"  Files backed up: {file_count}")
    print(f"  Tables cleared:  {table_count}")
    if file_count:
        print(f"\n  Backup location:\n  {backup_path}")
        print("\n  To restore, move files back from the backup folder.")
    print("\n  Restart the dashboard:  python etsy_dashboard.py")
    print("=" * 50)


if __name__ == "__main__":
    main()
