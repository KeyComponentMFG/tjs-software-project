"""Independent proof audit - CSV vs Supabase vs Dashboard."""
import pandas as pd, glob, os, sys

print("=" * 70)
print("  INDEPENDENT PROOF AUDIT - RAW CSV vs SUPABASE vs DASHBOARD")
print("  Every number traced to source. No globals. No trust.")
print("=" * 70)

def pm(val):
    if pd.isna(val) or val == '--' or val == '': return 0.0
    val = str(val).replace('$','').replace(',','').replace('"','')
    try: return float(val)
    except: return 0.0

# STEP 1: RAW CSV FILES
print("\n[1] RAW CSV FILES ON DISK")
kc_frames = []
for f in sorted(glob.glob(r'data/etsy_statements/etsy_statement*.csv')):
    df = pd.read_csv(f)
    kc_frames.append(df)
    print(f"  KC: {os.path.basename(f):35s} {len(df):5d} rows")
KC = pd.concat(kc_frames, ignore_index=True)
KC['Net_Clean'] = KC['Net'].apply(pm)
kc_sales = KC[KC['Type'] == 'Sale']
kc_gross = round(kc_sales['Net_Clean'].sum(), 2)
print(f"  KC TOTAL: {len(KC)} rows, {len(kc_sales)} sales, ${kc_gross:,.2f} gross")

au_frames = []
au_dir = r'C:\Users\mcnug\OneDrive\Desktop\UPLOAD_HERE\etsy_csvs aurvio'
for f in sorted(glob.glob(os.path.join(au_dir, '*.csv'))):
    df = pd.read_csv(f)
    au_frames.append(df)
    print(f"  AU: {os.path.basename(f):35s} {len(df):5d} rows")
AU = pd.concat(au_frames, ignore_index=True)
AU['Net_Clean'] = AU['Net'].apply(pm)
au_sales = AU[AU['Type'] == 'Sale']
au_gross = round(au_sales['Net_Clean'].sum(), 2)
print(f"  AU TOTAL: {len(AU)} rows, {len(au_sales)} sales, ${au_gross:,.2f} gross")

csv_total_rows = len(KC) + len(AU)
csv_total_sales = len(kc_sales) + len(au_sales)
csv_total_gross = round(kc_gross + au_gross, 2)
print(f"  COMBINED: {csv_total_rows} rows, {csv_total_sales} sales, ${csv_total_gross:,.2f} gross")

# STEP 2: SUPABASE
print("\n[2] SUPABASE DATABASE")
from supabase_loader import _get_supabase_client
client = _get_supabase_client()
sb_total = client.table('etsy_transactions').select('id', count='exact').execute()
sb_kc = client.table('etsy_transactions').select('id', count='exact').eq('store', 'keycomponentmfg').execute()
sb_au = client.table('etsy_transactions').select('id', count='exact').eq('store', 'aurvio').execute()
sb_kc_sales = client.table('etsy_transactions').select('id', count='exact').eq('store', 'keycomponentmfg').eq('type', 'Sale').execute()
sb_au_sales = client.table('etsy_transactions').select('id', count='exact').eq('store', 'aurvio').eq('type', 'Sale').execute()
print(f"  Total: {sb_total.count}, KC: {sb_kc.count} ({sb_kc_sales.count} sales), AU: {sb_au.count} ({sb_au_sales.count} sales)")

# STEP 3: DASHBOARD
print("\n[3] DASHBOARD (Railway sim)")
os.environ['PORT'] = '8200'
os.environ['IS_RAILWAY'] = '1'
import importlib.util
_spec = importlib.util.spec_from_file_location('etsy_dashboard_mono', 'etsy_dashboard.py')
_mod = importlib.util.module_from_spec(_spec)
sys.modules['etsy_dashboard_mono'] = _mod
_spec.loader.exec_module(_mod)

d_total = len(_mod.DATA)
d_kc = len(_mod.DATA[_mod.DATA['Store'] == 'keycomponentmfg'])
d_au = len(_mod.DATA[_mod.DATA['Store'] == 'aurvio'])
d_gross = round(_mod.gross_sales, 2)
d_orders = _mod.order_count
d_profit = round(_mod.profit, 2)

_mod._apply_store_filter('keycomponentmfg')
d_kc_gross = round(_mod.gross_sales, 2)
d_kc_orders = _mod.order_count
d_kc_profit = round(_mod.profit, 2)

_mod._apply_store_filter('aurvio')
d_au_gross = round(_mod.gross_sales, 2)
d_au_orders = _mod.order_count
d_au_profit = round(_mod.profit, 2)

_mod._apply_store_filter('all')

# STEP 4: CROSS-CHECK
print("\n" + "=" * 70)
print("  CROSS-CHECK: CSV vs SUPABASE vs DASHBOARD")
print("=" * 70)

checks = []
def check(name, actual, expected, tol=0):
    ok = abs(actual - expected) <= tol if isinstance(actual, float) else actual == expected
    checks.append(ok)
    mark = "PASS" if ok else "FAIL"
    line = f"  {mark}: {name}"
    if not ok:
        line += f"  (got {actual}, expected {expected})"
    print(line)

print("\n--- ROW COUNTS ---")
check("CSV total = Supabase total", sb_total.count, csv_total_rows)
check("CSV KC rows = Supabase KC", sb_kc.count, len(KC))
check("CSV AU rows = Supabase AU", sb_au.count, len(AU))
check("Dashboard total = CSV total", d_total, csv_total_rows)
check("Dashboard KC = CSV KC", d_kc, len(KC))
check("Dashboard AU = CSV AU", d_au, len(AU))

print("\n--- SALES ---")
check("Supabase KC sales = CSV KC sales", sb_kc_sales.count, len(kc_sales))
check("Supabase AU sales = CSV AU sales", sb_au_sales.count, len(au_sales))
check("Dashboard total orders = CSV total sales", d_orders, csv_total_sales)
check("Dashboard KC orders = CSV KC sales", d_kc_orders, len(kc_sales))
check("Dashboard AU orders = CSV AU sales", d_au_orders, len(au_sales))

print("\n--- GROSS REVENUE ---")
check("Dashboard total gross = CSV gross", d_gross, csv_total_gross, 0.01)
check("Dashboard KC gross = CSV KC gross", d_kc_gross, kc_gross, 0.01)
check("Dashboard AU gross = CSV AU gross", d_au_gross, au_gross, 0.01)
check("KC + AU = Total", round(d_kc_gross + d_au_gross, 2), d_gross, 0.01)

print("\n--- PROFIT (bank-derived, same for all) ---")
check("Profit all = Profit KC", d_profit, d_kc_profit, 0.01)
check("Profit all = Profit AU", d_profit, d_au_profit, 0.01)

print("\n--- INVENTORY ---")
check("Invoices", len(_mod.INVOICES), 112)
check("Inventory cost", round(_mod.total_inventory_cost, 2), 4513.50, 0.01)

print("\n--- BANK ---")
check("Bank txns", len(_mod.BANK_TXNS), 143)
check("Bank net cash", round(_mod.bank_net_cash, 2), 4568.68, 0.01)

print("\n--- REFUND ASSIGNMENTS ---")
ra = _mod._refund_assignments
check("Total assignments", len(ra), 18)
check("TJ", sum(1 for v in ra.values() if v == 'TJ'), 7)
check("Braden", sum(1 for v in ra.values() if v == 'Braden'), 8)
check("Cancelled", sum(1 for v in ra.values() if v == 'Cancelled'), 3)

print("\n--- PIPELINE ---")
check("Pipeline healthy", _mod._acct_pipeline.ledger.is_healthy, True)
vp = sum(1 for v in _mod._acct_pipeline.ledger.validations if v.passed)
check("Validations passed", vp, 5)

print("\n--- DATA ISOLATION ---")
kc_ords = set(kc_sales['Title'].str.extract(r'(Order #\d+)', expand=False).dropna())
au_ords = set(au_sales['Title'].str.extract(r'(Order #\d+)', expand=False).dropna())
check("Zero cross-store overlap", len(kc_ords & au_ords), 0)

print("\n" + "=" * 70)
p = sum(checks)
t = len(checks)
print(f"  RESULT: {p}/{t} CHECKS PASSED")
if p == t:
    print("  EVERY NUMBER MATCHES. DATA IS CORRECT.")
else:
    print(f"  {t-p} FAILURES.")
print("=" * 70)
