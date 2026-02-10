"""
import_data.py -- One-click data reprocessing for the Etsy Business Dashboard.

Usage:
    python import_data.py            # Reprocess all data
    python import_data.py --launch   # Reprocess and then open the dashboard

After adding new Etsy statements, invoices, or bank statements, just run this script.
"""

import subprocess
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def run_script(name):
    path = os.path.join(BASE_DIR, name)
    print(f"\n{'='*60}")
    print(f"  Running {name}...")
    print(f"{'='*60}\n")
    result = subprocess.run([sys.executable, path], cwd=BASE_DIR)
    if result.returncode != 0:
        print(f"\n  ERROR: {name} failed with exit code {result.returncode}")
        return False
    print(f"\n  {name} completed successfully.")
    return True


def main():
    statements_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
    invoices_dir = os.path.join(BASE_DIR, "data", "invoices")
    bank_dir = os.path.join(BASE_DIR, "data", "bank_statements")
    generated_dir = os.path.join(BASE_DIR, "data", "generated")

    for d in [statements_dir, invoices_dir, bank_dir, generated_dir]:
        os.makedirs(d, exist_ok=True)

    # Count inputs
    csv_count = len([f for f in os.listdir(statements_dir)
                     if f.startswith("etsy_statement") and f.endswith(".csv")])

    pdf_count = 0
    for sub in os.listdir(invoices_dir):
        sub_path = os.path.join(invoices_dir, sub)
        if os.path.isdir(sub_path):
            pdf_count += len([f for f in os.listdir(sub_path) if f.lower().endswith(".pdf")])

    bank_pdf_count = len([f for f in os.listdir(bank_dir) if f.lower().endswith(".pdf")])

    print(f"\n{'='*60}")
    print(f"  ETSY DASHBOARD DATA IMPORT")
    print(f"{'='*60}")
    print(f"\n  Found {csv_count} Etsy statement CSVs in data/etsy_statements/")
    print(f"  Found {pdf_count} invoice/receipt PDFs in data/invoices/")
    print(f"  Found {bank_pdf_count} bank statement PDFs in data/bank_statements/")

    if csv_count == 0:
        print("\n  WARNING: No Etsy CSVs found in data/etsy_statements/")
        print("  Drop your etsy_statement_YYYY_MM.csv files there first.")

    # Step 1: Process Etsy statements
    if not run_script("etsy_organizer.py"):
        print("\nAborting. Fix errors above and retry.")
        return

    # Step 2: Parse invoices
    if not run_script("_parse_invoices.py"):
        print("\nAborting. Fix errors above and retry.")
        return

    # Step 3: Parse bank statements
    if not run_script("_parse_bank_statements.py"):
        print("\nAborting. Fix errors above and retry.")
        return

    # Step 4: Upload to Supabase (if configured)
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    if os.environ.get("SUPABASE_URL") and "YOUR_PROJECT" not in os.environ.get("SUPABASE_URL", ""):
        if not run_script("migrate_to_supabase.py"):
            print("\n  WARNING: Supabase upload failed. Local files are fine — dashboard will use fallback.")
    else:
        print("\n  Skipping Supabase upload (SUPABASE_URL not configured in .env)")

    print(f"\n{'='*60}")
    print("  ALL DATA PROCESSED SUCCESSFULLY")
    print(f"{'='*60}")
    print(f"\n  Generated files in data/generated/:")
    for f in sorted(os.listdir(generated_dir)):
        size = os.path.getsize(os.path.join(generated_dir, f))
        print(f"    {f} ({size:,} bytes)")

    if "--launch" in sys.argv:
        print(f"\n  Launching dashboard...")
        subprocess.run([sys.executable, os.path.join(BASE_DIR, "etsy_dashboard.py")])
    else:
        print(f"\n  To view the dashboard, run:")
        print(f"    python etsy_dashboard.py")
        print(f"    Then open http://127.0.0.1:8070")


if __name__ == "__main__":
    main()
