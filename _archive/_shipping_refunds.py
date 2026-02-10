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
data['Amount_Clean'] = data['Amount'].apply(parse_money)
data['Date_Parsed'] = pd.to_datetime(data['Date'], format='%B %d, %Y', errors='coerce')

# Get refunded orders
refund_rows = data[data['Type'] == 'Refund'].copy()
refund_rows['Order'] = refund_rows['Title'].str.extract(r'(Order #\d+)')
refunded_orders = set(refund_rows['Order'].dropna())

print(f'Refunded orders: {len(refunded_orders)}')
print()

# For each refunded order, find ALL line items
for order in sorted(refunded_orders):
    order_data = data[data['Info'] == order]
    refund_data = refund_rows[refund_rows['Order'] == order]
    print(f'=== {order} ===')

    # Show all line items for this order
    for _, r in order_data.iterrows():
        print(f'  {r["Date"]:25s} {r["Type"]:12s} {r["Title"][:55]:55s} Net: {r["Net_Clean"]:8.2f}')

    # Show the refund line(s)
    for _, r in refund_data.iterrows():
        print(f'  {r["Date"]:25s} {r["Type"]:12s} {r["Title"][:55]:55s} Net: {r["Net_Clean"]:8.2f}')
    print()

# Now check: are there any shipping labels that might be return labels?
# Look at shipping labels - check the title/info for any clues
print('=== ALL SHIPPING LABEL TITLES (unique) ===')
ship_rows = data[data['Type'] == 'Shipping']
for t in ship_rows['Title'].unique():
    print(f'  {t}')

print()
print('=== SHIPPING LABEL INFO VALUES (sample) ===')
for i in ship_rows['Info'].unique()[:20]:
    print(f'  {i}')

print()

# Check for credits related to shipping on refunded orders
print('=== CREDITS ON REFUNDED ORDERS ===')
for order in sorted(refunded_orders):
    credits = data[(data['Info'] == order) & (data['Net_Clean'] > 0) & (data['Type'] == 'Fee')]
    if len(credits):
        for _, r in credits.iterrows():
            print(f'  {order}: {r["Title"][:60]} -> ${r["Net_Clean"]:.2f}')

print()

# Summary for refunded orders
print('=== REFUND SHIPPING SUMMARY ===')
total_refund_amount = 0
total_refund_ship_fee = 0
total_refund_ship_buyer = 0
total_refund_credits = 0

for order in sorted(refunded_orders):
    order_data = data[data['Info'] == order]
    refund_data = refund_rows[refund_rows['Order'] == order]

    # Refund amount
    refund_amt = abs(refund_data['Net_Clean'].sum())
    total_refund_amount += refund_amt

    # Shipping fee on this order (buyer paid shipping?)
    ship_fee = order_data[order_data['Title'].str.contains('Transaction fee: Shipping', na=False)]
    if len(ship_fee):
        fee_amt = abs(ship_fee['Net_Clean'].values[0])
        buyer_ship = fee_amt / 0.065
        total_refund_ship_fee += fee_amt
        total_refund_ship_buyer += buyer_ship

    # Credits received back
    credits = order_data[order_data['Net_Clean'] > 0]
    total_refund_credits += credits['Net_Clean'].sum()

print(f'Total refund amount: ${total_refund_amount:.2f}')
print(f'Refunded orders that charged shipping: buyer paid est ${total_refund_ship_buyer:.2f}')
print(f'Shipping tx fees on refunded orders: ${total_refund_ship_fee:.2f}')
print(f'Fee credits received back on refunds: ${total_refund_credits:.2f}')
print(f'Avg label cost: ${data[data["Type"]=="Shipping"]["Net_Clean"].abs().mean():.2f}')
print(f'Est label cost for {len(refunded_orders)} refunded orders: ${len(refunded_orders) * data[data["Type"]=="Shipping"]["Net_Clean"].abs().mean():.2f}')
