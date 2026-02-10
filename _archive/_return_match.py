import pandas as pd, os

def parse_money(val):
    if pd.isna(val) or val == '--' or val == '':
        return 0.0
    val = str(val).replace('$','').replace(',','').replace('"','')
    try: return float(val)
    except: return 0.0

frames = []
for f in os.listdir('.'):
    if f.startswith('etsy_statement') and f.endswith('.csv'):
        frames.append(pd.read_csv(f))
data = pd.concat(frames, ignore_index=True)
data['Net_Clean'] = data['Net'].apply(parse_money)
data['Date_Parsed'] = pd.to_datetime(data['Date'], format='%B %d, %Y', errors='coerce')

# Return labels
returns = data[data['Title'] == 'USPS return shipping label'].sort_values('Date_Parsed')
refunds = data[data['Type'] == 'Refund'].sort_values('Date_Parsed')

print('=== RETURN LABELS ===')
for _, r in returns.iterrows():
    print(f'  {r["Date"]:25s}  {r["Info"]:30s}  Cost: ${abs(r["Net_Clean"]):.2f}')

print()
print('=== REFUNDS (with dates) ===')
for _, r in refunds.iterrows():
    order = r['Title']
    order_id = order.replace('Refund for ', '').replace('Partial refund for ', '')
    # Find what product was in this order from transaction fees
    order_fees = data[(data['Info'] == order_id) & data['Title'].str.startswith('Transaction fee:', na=False) & ~data['Title'].str.contains('Shipping', na=False)]
    product = 'Unknown'
    if len(order_fees):
        product = order_fees.iloc[0]['Title'].replace('Transaction fee: ', '')
    print(f'  {r["Date"]:25s}  {order_id:30s}  Refund: ${abs(r["Net_Clean"]):.2f}  Product: {product[:50]}')

print()
print('=== MATCHING RETURN LABELS TO REFUNDS BY DATE PROXIMITY ===')
for _, ret in returns.iterrows():
    ret_date = ret['Date_Parsed']
    ret_cost = abs(ret['Net_Clean'])
    print(f'\nReturn Label: {ret["Date"]} | {ret["Info"]} | ${ret_cost:.2f}')

    # Find refunds within +/- 7 days
    nearby_refunds = refunds[abs((refunds['Date_Parsed'] - ret_date).dt.days) <= 7]
    if len(nearby_refunds):
        for _, ref in nearby_refunds.iterrows():
            order_id = ref['Title'].replace('Refund for ', '').replace('Partial refund for ', '')
            order_fees = data[(data['Info'] == order_id) & data['Title'].str.startswith('Transaction fee:', na=False) & ~data['Title'].str.contains('Shipping', na=False)]
            product = order_fees.iloc[0]['Title'].replace('Transaction fee: ', '') if len(order_fees) else 'Unknown'
            print(f'  -> Nearby refund: {ref["Date"]} | {order_id} | ${abs(ref["Net_Clean"]):.2f} | {product[:50]}')
    else:
        print('  -> No nearby refunds found')

print()
print('=== MONTHLY NET REVENUE ===')
data['Month'] = data['Date_Parsed'].dt.to_period('M').astype(str)
# Net revenue = sum of ALL Net_Clean values (excluding deposits which are $0)
non_deposit = data[data['Type'] != 'Deposit']
monthly_net = non_deposit.groupby('Month')['Net_Clean'].sum()
monthly_sales = data[data['Type'] == 'Sale'].groupby('Month')['Net_Clean'].sum()
monthly_fees = data[data['Type'] == 'Fee'].groupby('Month')['Net_Clean'].sum()
monthly_ship = data[data['Type'] == 'Shipping'].groupby('Month')['Net_Clean'].sum()
monthly_mkt = data[data['Type'] == 'Marketing'].groupby('Month')['Net_Clean'].sum()
monthly_ref = data[data['Type'] == 'Refund'].groupby('Month')['Net_Clean'].sum()
monthly_tax = data[data['Type'] == 'Tax'].groupby('Month')['Net_Clean'].sum()

months = sorted(monthly_net.index)
print(f'{"Month":<10} {"Sales":>10} {"Fees":>10} {"Shipping":>10} {"Marketing":>10} {"Refunds":>10} {"Taxes":>10} {"NET REV":>10}')
print('-' * 85)
for m in months:
    s = monthly_sales.get(m, 0)
    f = monthly_fees.get(m, 0)
    sh = monthly_ship.get(m, 0)
    mk = monthly_mkt.get(m, 0)
    r = monthly_ref.get(m, 0)
    t = monthly_tax.get(m, 0)
    net = s + f + sh + mk + r  # exclude taxes (pass-through)
    print(f'{m:<10} ${s:>9,.2f} ${f:>9,.2f} ${sh:>9,.2f} ${mk:>9,.2f} ${r:>9,.2f} ${t:>9,.2f} ${net:>9,.2f}')

totals_net = sum(monthly_sales.get(m, 0) + monthly_fees.get(m, 0) + monthly_ship.get(m, 0) + monthly_mkt.get(m, 0) + monthly_ref.get(m, 0) for m in months)
print('-' * 85)
print(f'{"TOTAL":<10} ${monthly_sales.sum():>9,.2f} ${monthly_fees.sum():>9,.2f} ${monthly_ship.sum():>9,.2f} ${monthly_mkt.sum():>9,.2f} ${monthly_ref.sum():>9,.2f} ${monthly_tax.sum():>9,.2f} ${totals_net:>9,.2f}')
