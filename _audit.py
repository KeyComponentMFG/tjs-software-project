"""
_audit.py - Comprehensive accuracy audit: source files vs Supabase data.
"""

import os, re, csv, sys
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
sys.path.insert(0, BASE_DIR)
from supabase_loader import load_data


def parse_money(val):
    if pd.isna(val) or val == '--' or val == '' or val is None:
        return Decimal('0.00')
    val = str(val).replace('$', '').replace(',', '').replace('"', '').strip()
    try:
        return Decimal(val).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal('0.00')


def parse_deposit_from_title(title):
    if not isinstance(title, str):
        return Decimal('0.00')
    m = re.search(r"\$([\d,]+\.\d{2})", title)
    if m:
        return Decimal(m.group(1).replace(',', ''))
    return Decimal('0.00')


def d(val):
    if val is None:
        return Decimal('0.00')
    return Decimal(str(val)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def fmt(amount):
    sign = '-' if amount < 0 else ''
    return f"{sign}${abs(amount):,.2f}"


def load_etsy_csvs():
    statements_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
    frames = []
    files_found = []
    for fn in sorted(os.listdir(statements_dir)):
        if fn.startswith("etsy_statement") and fn.endswith(".csv"):
            df = pd.read_csv(os.path.join(statements_dir, fn))
            df["_source_file"] = fn
            frames.append(df)
            files_found.append(f"{fn} ({len(df)} rows)")
    combined = pd.concat(frames, ignore_index=True)
    return combined, files_found


def compute_etsy_totals(df):
    totals = {}
    counts = {}
    for _, row in df.iterrows():
        t = row.get("Type", row.get("type", "Unknown"))
        net = parse_money(row.get("Net", row.get("net", 0)))
        if t == "Deposit":
            title = row.get("Title", row.get("title", ""))
            deposit_amt = parse_deposit_from_title(title)
            net = -deposit_amt
        totals[t] = totals.get(t, Decimal('0.00')) + net
        counts[t] = counts.get(t, 0) + 1
    return totals, counts


def load_bank_csv():
    csv_path = os.path.join(BASE_DIR, "data", "bank_statements",
                            "2026-02-16_transaction_download.csv")
    transactions = []
    with open(csv_path, "r", encoding="utf-8-sig") as fh:
        lines = fh.readlines()
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue
        date_match = re.search(r',\s*"(\d{2}/\d{2}/\d{4})"\s*$', line)
        if not date_match:
            continue
        remainder = line[:date_match.start()]
        parsed = list(csv.reader([remainder]))[0]
        if len(parsed) < 4:
            continue
        credit = parsed[1].strip()
        debit = parsed[2].strip()
        desc = ",".join(parsed[3:]).strip().strip('"').strip()
        if credit:
            transactions.append({"amount": Decimal(credit.replace(",", "")),
                                 "type": "deposit", "desc": desc})
        elif debit:
            transactions.append({"amount": Decimal(debit.replace(",", "")),
                                 "type": "debit", "desc": desc})
    return transactions


def main():
    W = 72
    print("=" * W)
    print("  COMPREHENSIVE ACCURACY AUDIT: Source Files vs Supabase")
    print("=" * W)

    print("\nLoading Supabase data via supabase_loader.load_data()...")
    sb = load_data()
    sb_etsy = sb["DATA"]
    sb_bank = sb["BANK_TXNS"]
    sb_config = sb["CONFIG"]
    print(f"  Supabase etsy_transactions: {len(sb_etsy)} rows")
    print(f"  Supabase bank_transactions: {len(sb_bank)} rows")

    print("\n" + "=" * W)
    print("  SECTION 1: ETSY TRANSACTIONS - CSV vs Supabase")
    print("=" * W)

    csv_etsy, csv_files = load_etsy_csvs()
    print("\n  CSV source files loaded:")
    for fn in csv_files:
        print(f"    {fn}")
    print(f"  Total CSV rows: {len(csv_etsy)}")

    csv_totals, csv_counts = compute_etsy_totals(csv_etsy)
    sb_totals, sb_counts = compute_etsy_totals(sb_etsy)
    all_types = sorted(set(list(csv_totals.keys()) + list(sb_totals.keys())))

    hdr = f"  {'Type':<12} {'CSV Cnt':>8} {'SB Cnt':>8} {'CSV Net':>14} {'SB Net':>14} {'Diff':>14} {'Status'}" 
    print("\n" + hdr)
    s = "-"
    print(f"  {s*12} {s*8} {s*8} {s*14} {s*14} {s*14} {s*8}")

    etsy_diffs = False
    for t in all_types:
        csv_net = csv_totals.get(t, Decimal('0.00'))
        sb_net = sb_totals.get(t, Decimal('0.00'))
        csv_cnt = csv_counts.get(t, 0)
        sb_cnt = sb_counts.get(t, 0)
        diff = csv_net - sb_net
        status = "OK" if diff == 0 and csv_cnt == sb_cnt else "MISMATCH"
        if status == "MISMATCH":
            etsy_diffs = True
        print(f"  {t:<12} {csv_cnt:>8} {sb_cnt:>8} {fmt(csv_net):>14} {fmt(sb_net):>14} {fmt(diff):>14} {status}")

    csv_grand = sum(csv_totals.values(), Decimal('0.00'))
    sb_grand = sum(sb_totals.values(), Decimal('0.00'))
    grand_diff = csv_grand - sb_grand
    tc = sum(csv_counts.values())
    ts = sum(sb_counts.values())
    gstat = "OK" if grand_diff == 0 else "MISMATCH"
    print(f"  {'GRAND TOTAL':<12} {tc:>8} {ts:>8} {fmt(csv_grand):>14} {fmt(sb_grand):>14} {fmt(grand_diff):>14} {gstat}")

    if not etsy_diffs and grand_diff == 0:
        print("\n  RESULT: Etsy data PERFECTLY MATCHED between CSV and Supabase.")
    else:
        print("\n  RESULT: DISCREPANCIES FOUND in Etsy data!")

