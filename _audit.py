"""Audit raw Etsy CSV data -- exact totals to the penny.
No deduplication needed: files cover non-overlapping date ranges."""
import pandas as pd
import os, re
from decimal import Decimal

BASE = os.path.dirname(os.path.abspath(__file__))
csv_dir = os.path.join(BASE, "data", "etsy_statements")

def pm(val):
    if pd.isna(val) or str(val).strip() in ("--", ""):
        return Decimal("0")
    s = str(val).replace("$", "").replace(",", "").replace('"', "")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")

# ── Load ──
files = sorted([f for f in os.listdir(csv_dir) if f.startswith("etsy_statement") and f.endswith(".csv")])
print("=" * 70)
print("ETSY RAW CSV AUDIT")
print("=" * 70)
print("\nCSV Files (non-overlapping date ranges):")
frames = []
for f in files:
    dfi = pd.read_csv(os.path.join(csv_dir, f))
    dfi["_source"] = f
    dfi["_date"] = pd.to_datetime(dfi["Date"], format="%B %d, %Y", errors="coerce")
    print(f"  {f}: {dfi['_date'].min().date()} to {dfi['_date'].max().date()}, {len(dfi)} rows")
    frames.append(dfi)

df = pd.concat(frames, ignore_index=True)
df["_amt"] = df["Amount"].apply(pm)
df["_fees"] = df["Fees & Taxes"].apply(pm)
df["_net"] = df["Net"].apply(pm)
df["_month"] = df["_date"].dt.to_period("M")

print(f"\nTOTAL ROWS: {len(df)}")

# ── Type counts ──
print(f"\n{'TYPE':<20s} {'ROWS':>6s} {'AMOUNT':>14s} {'FEES&TAXES':>14s} {'NET':>14s}")
print("-" * 70)
for t in sorted(df["Type"].unique()):
    sub = df[df["Type"] == t]
    print(f"  {t:<18s} {len(sub):>6d} {str(sub['_amt'].sum()):>14s} {str(sub['_fees'].sum()):>14s} {str(sub['_net'].sum()):>14s}")
print("-" * 70)
print(f"  {'ALL':<18s} {len(df):>6d} {str(df['_amt'].sum()):>14s} {str(df['_fees'].sum()):>14s} {str(df['_net'].sum()):>14s}")

# ── Key metrics ──
sales = df[df["Type"] == "Sale"]
refunds = df[df["Type"] == "Refund"]
fees = df[df["Type"] == "Fee"]
shipping = df[df["Type"] == "Shipping"]
tax = df[df["Type"] == "Tax"]
mktg = df[df["Type"] == "Marketing"]
deposits = df[df["Type"] == "Deposit"]
payment = df[df["Type"] == "Payment"]
buyer_fee = df[df["Type"] == "Buyer Fee"]

pat = r"#(\d+)"
order_count = sales["Title"].str.extract(pat)[0].dropna().nunique()

print(f"\n{'=' * 70}")
print("KEY METRICS")
print(f"{'=' * 70}")
print(f"  Gross Sales (Sale Amount):    ${sales['_amt'].sum()}")
print(f"  Unique Orders (from Sales):   {order_count}")
print(f"  Refund Amount:                ${refunds['_amt'].sum()}")
print(f"  Refund Count:                 {len(refunds)}")
print(f"  Net Sales (Gross + Refunds):  ${sales['_amt'].sum() + refunds['_amt'].sum()}")

# Fee breakdown
listing = fees[fees["Title"].str.contains("Listing fee", case=False, na=False)]
txn_fee = fees[fees["Title"].str.contains("Transaction fee", case=False, na=False)]
proc_fee = fees[fees["Title"].str.contains("Processing fee", case=False, na=False)]
reg_fee = fees[fees["Title"].str.contains("Regulatory", case=False, na=False)]
used = set(listing.index) | set(txn_fee.index) | set(proc_fee.index) | set(reg_fee.index)
other_fee = fees[~fees.index.isin(used)]

print(f"\n  --- Fee Breakdown ---")
print(f"  Listing fees:                 ${listing['_net'].sum()}  ({len(listing)} rows)")
print(f"  Transaction fees:             ${txn_fee['_net'].sum()}  ({len(txn_fee)} rows)")
print(f"  Processing fees:              ${proc_fee['_net'].sum()}  ({len(proc_fee)} rows)")
print(f"  Regulatory fees:              ${reg_fee['_net'].sum()}  ({len(reg_fee)} rows)")
print(f"  Other fees (Share&Save):      ${other_fee['_net'].sum()}  ({len(other_fee)} rows)")
print(f"  TOTAL FEES (Type=Fee):        ${fees['_net'].sum()}")

print(f"\n  Shipping Labels:              ${shipping['_net'].sum()}  ({len(shipping)} labels)")
print(f"  Tax (collected by Etsy):       ${tax['_net'].sum()}  ({len(tax)} rows)")
print(f"  Marketing/Etsy Ads:           ${mktg['_net'].sum()}  ({len(mktg)} rows)")
print(f"  Buyer Fee (CO delivery):      ${buyer_fee['_net'].sum()}  ({len(buyer_fee)} rows)")

# Payment (charge for refund)
if len(payment) > 0:
    print(f"  Payment (refund charge):      ${payment['_net'].sum()}  ({len(payment)} rows)")
    for _, r in payment.iterrows():
        print(f"    -> {r['Date']}: {r['Title']}, Net={r['Net']}")

# Deposits
dep_total = Decimal("0")
for _, r in deposits.iterrows():
    m = re.search(r"\$([\d,]+\.\d{2})", str(r["Title"]))
    if m:
        dep_total += Decimal(m.group(1).replace(",", ""))
print(f"\n  Deposits to bank:             ${dep_total}  ({len(deposits)} deposits)")

# All deductions combined
all_deductions = fees['_net'].sum() + shipping['_net'].sum() + tax['_net'].sum() + mktg['_net'].sum() + buyer_fee['_net'].sum()
print(f"\n  TOTAL DEDUCTIONS:             ${all_deductions}")
print(f"  (Fees + Shipping + Tax + Ads + Buyer Fee)")

# Net
print(f"\n  NET (all rows):               ${df['_net'].sum()}")

# Cross-check
total_amt = df["_amt"].sum()
total_fees_col = df["_fees"].sum()
total_net = df["_net"].sum()
computed = total_amt + total_fees_col
print(f"\n  CROSS-CHECK: Amount({total_amt}) + Fees&Taxes({total_fees_col}) = {computed}")
print(f"  Actual Net = {total_net}")
print(f"  Difference = {total_net - computed}")

# ── Monthly ──
print(f"\n{'=' * 70}")
print("MONTHLY BREAKDOWN")
print(f"{'=' * 70}")
print(f"{'Month':>7s} {'Rows':>5s} {'Sales':>5s} {'Gross':>12s} {'Refunds':>10s} {'Fees':>10s} {'Ship':>10s} {'Ads':>10s} {'Tax':>10s} {'Net':>12s}")
print("-" * 100)
for month in sorted(df["_month"].dropna().unique()):
    mdf = df[df["_month"] == month]
    ms = mdf[mdf["Type"] == "Sale"]
    mr = mdf[mdf["Type"] == "Refund"]
    mf = mdf[mdf["Type"] == "Fee"]
    msh = mdf[mdf["Type"] == "Shipping"]
    mm = mdf[mdf["Type"] == "Marketing"]
    mt = mdf[mdf["Type"] == "Tax"]
    print(f"{str(month):>7s} {len(mdf):>5d} {len(ms):>5d} {str(ms['_amt'].sum()):>12s} {str(mr['_amt'].sum()):>10s} {str(mf['_net'].sum()):>10s} {str(msh['_net'].sum()):>10s} {str(mm['_net'].sum()):>10s} {str(mt['_net'].sum()):>10s} {str(mdf['_net'].sum()):>12s}")

# ── Supabase notes ──
print(f"\n{'=' * 70}")
print("SUPABASE LAYER ANALYSIS")
print(f"{'=' * 70}")
print("""
supabase_loader.py provides two paths to load this data:

1. SUPABASE (primary): Fetches from 'etsy_transactions' table
   - All values stored as strings (no precision loss)
   - _parse_money() converts to float (minor rounding possible vs Decimal)
   - Adds computed columns: Amount_Clean, Net_Clean, Fees_Clean, Date_Parsed, Month, Week

2. LOCAL FALLBACK: Reads these same CSV files via _load_etsy_local()
   - Concatenates all etsy_statement_*.csv files
   - Same computed columns added via _add_computed_columns()
   - NO deduplication logic (but not needed since files don't overlap)

What Supabase does NOT change:
   - Raw column values are preserved exactly
   - No rows added or removed vs CSV source
   - No aggregation or summarization in the loader
   - Config, invoices, and bank transactions are separate tables/files

Sync functions:
   - sync_etsy_transactions(): Full replace (delete all + insert all)
   - append_etsy_transactions(): Dedup by (date, type, title, info, amount, net) before insert
""")
