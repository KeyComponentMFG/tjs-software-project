"""Quick audit: compare correct vs Railway reload computations."""
import pandas as pd
import glob, os, re, sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)
sys.path.insert(0, BASE_DIR)

# Load CSVs
frames = []
for f in sorted(glob.glob('data/etsy_statements/etsy_statement*.csv')):
    frames.append(pd.read_csv(f))
DATA = pd.concat(frames, ignore_index=True)

def pm(val):
    if pd.isna(val) or val == '--' or val == '':
        return 0.0
    val = str(val).replace('$', '').replace(',', '').replace('"', '')
    try: return float(val)
    except: return 0.0

DATA['Amount_Clean'] = DATA['Amount'].apply(pm)
DATA['Net_Clean'] = DATA['Net'].apply(pm)

sales_df = DATA[DATA['Type'] == 'Sale']
fee_df = DATA[DATA['Type'] == 'Fee']

print('=== BUG 1: gross_sales column ===')
net_gs = sales_df['Net_Clean'].sum()
amt_gs = sales_df['Amount_Clean'].sum()
print(f'  Net_Clean (correct):    ${net_gs:,.2f}')
print(f'  Amount_Clean (Railway): ${amt_gs:,.2f}')
print(f'  DIFFERENCE:             ${amt_gs - net_gs:,.2f}')

print()
print('=== BUG 2: order_count ===')
print(f'  len(sales_df) (correct):  {len(sales_df)}')
order_nums = sales_df['Title'].str.extract(r'(Order #\d+)', expand=False).nunique()
print(f'  nunique orders (Railway): {order_nums}')

print()
print('=== BUG 3: buyer_fee_df ===')
buyer_fee_correct = DATA[DATA['Type'] == 'Buyer Fee']
buyer_fee_railway = fee_df[fee_df['Title'].str.contains('Regulatory operating fee|Sales tax paid', case=False, na=False)]
bfc_sum = abs(buyer_fee_correct['Net_Clean'].sum())
bfr_sum = abs(buyer_fee_railway['Net_Clean'].sum())
print(f'  Correct (Type=Buyer Fee): {len(buyer_fee_correct)} rows, ${bfc_sum:,.2f}')
print(f'  Railway (filtered fees):  {len(buyer_fee_railway)} rows, ${bfr_sum:,.2f}')

print()
print('=== BUG 4: etsy_net_earned ===')
total_fees = abs(fee_df['Net_Clean'].sum())
total_shipping = abs(DATA[DATA['Type']=='Shipping']['Net_Clean'].sum())
total_marketing = abs(DATA[DATA['Type']=='Marketing']['Net_Clean'].sum())
total_refunds = abs(DATA[DATA['Type']=='Refund']['Net_Clean'].sum())
total_taxes = abs(DATA[DATA['Type']=='Tax']['Net_Clean'].sum())
total_bf_correct = abs(buyer_fee_correct['Net_Clean'].sum())

earned_correct = net_gs - total_fees - total_shipping - total_marketing - total_refunds - total_taxes - total_bf_correct
earned_railway = DATA['Net_Clean'].sum()
print(f'  Correct (itemized):   ${earned_correct:,.2f}')
print(f'  Railway (Net sum):    ${earned_railway:,.2f}')
print(f'  DIFFERENCE:           ${earned_railway - earned_correct:,.2f}')

print()
print('=== ETSY GAP COMPARISON ===')
dep_total = 0.0
for _, dr in DATA[DATA['Type']=='Deposit'].iterrows():
    m = re.search(r'([\d,]+\.\d+)', str(dr.get('Title','')))
    if m: dep_total += float(m.group(1).replace(',',''))

etsy_pre_capone = 941.99

from _parse_bank_statements import parse_bank_pdf, parse_bank_csv, apply_overrides
bank_txns = []
covered = set()
for fn in sorted(os.listdir('data/bank_statements')):
    if fn.lower().endswith('.pdf'):
        try:
            txns, cov = parse_bank_pdf(os.path.join('data/bank_statements', fn))
            bank_txns.extend(txns)
            covered.update(cov)
        except: pass
csv_txns, csv_cov = [], set()
for fn in sorted(os.listdir('data/bank_statements')):
    if fn.lower().endswith('.csv'):
        try:
            txns, cov = parse_bank_csv(os.path.join('data/bank_statements', fn))
            csv_txns.extend(txns)
            csv_cov.update(cov)
        except: pass
if csv_txns:
    seen = {}
    for t in csv_txns:
        key = (t['date'], t['amount'], t['type'], t.get('raw_desc', t['desc']))
        seen[key] = t
    new_months = csv_cov - covered
    if new_months:
        bank_txns.extend([t for t in seen.values()
                          if "{}-{}".format(t['date'].split('/')[2], t['date'].split('/')[0]) in new_months])
        covered.update(new_months)
    bank_txns = apply_overrides(bank_txns)
bank_total_deposits = sum(t['amount'] for t in bank_txns if t['type'] == 'deposit')

etsy_total_dep = etsy_pre_capone + bank_total_deposits

# Correct path
etsy_balance_correct = max(0, round(DATA['Net_Clean'].sum() - dep_total, 2))
etsy_bal_calc = earned_correct - etsy_total_dep
etsy_gap_correct = round(etsy_bal_calc - etsy_balance_correct, 2)

# Railway reload path
etsy_balance_railway = max(0, round(earned_railway - dep_total, 2))
etsy_bal_calc_r = earned_railway - etsy_total_dep
etsy_gap_railway = round(etsy_bal_calc_r - etsy_balance_railway, 2)

print(f'  csv_deposit_total:       ${dep_total:,.2f}')
print(f'  bank_total_deposits:     ${bank_total_deposits:,.2f}')
print(f'  etsy_pre_capone:         ${etsy_pre_capone:,.2f}')
print(f'  etsy_total_deposited:    ${etsy_total_dep:,.2f}')
print()
print(f'  CORRECT PATH:')
print(f'    etsy_net_earned:       ${earned_correct:,.2f}')
print(f'    etsy_balance:          ${etsy_balance_correct:,.2f}')
print(f'    etsy_balance_calc:     ${etsy_bal_calc:,.2f}')
print(f'    etsy_csv_gap:          ${etsy_gap_correct:,.2f}')
print()
print(f'  RAILWAY RELOAD PATH:')
print(f'    etsy_net_earned:       ${earned_railway:,.2f}')
print(f'    etsy_balance:          ${etsy_balance_railway:,.2f}')
print(f'    etsy_balance_calc:     ${etsy_bal_calc_r:,.2f}')
print(f'    etsy_csv_gap:          ${etsy_gap_railway:,.2f}')

# What dashboard ACTUALLY shows
print()
print('=== WHAT DASHBOARD SHOWS ===')
print(f'  REVENUE KPI (gross_sales):')
print(f'    Correct (Net_Clean):    ${net_gs:,.2f}')
print(f'    Railway (Amount_Clean): ${amt_gs:,.2f}')
print(f'  ORDER COUNT:')
print(f'    Correct (len):          {len(sales_df)}')
print(f'    Railway (nunique):      {order_nums}')
print(f'  AVG ORDER:')
if len(sales_df) > 0:
    print(f'    Correct:                ${net_gs/len(sales_df):,.2f}')
if order_nums > 0:
    print(f'    Railway:                ${amt_gs/order_nums:,.2f}')
