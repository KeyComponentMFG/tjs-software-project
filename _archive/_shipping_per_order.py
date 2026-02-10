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

# Get all unique orders (from Sale rows)
sale_rows = data[data['Type'] == 'Sale'].copy()
sale_rows['Order'] = sale_rows['Title'].str.extract(r'(Order #\d+)')

# Get shipping transaction fees per order
ship_fee_rows = data[data['Title'].str.contains('Transaction fee: Shipping', na=False)].copy()
ship_fee_rows['Order'] = ship_fee_rows['Info']

# Build per-order shipping info
orders_with_shipping = {}
for _, r in ship_fee_rows.iterrows():
    order = r['Order']
    fee = abs(r['Net_Clean'])
    buyer_paid = fee / 0.065
    orders_with_shipping[order] = buyer_paid

# How many orders had shipping charged?
all_orders = set(sale_rows['Order'].dropna())
orders_with_ship = set(orders_with_shipping.keys()) & all_orders
orders_free_ship = all_orders - orders_with_ship

print(f'Total orders: {len(all_orders)}')
print(f'Orders WITH shipping charged: {len(orders_with_ship)}')
print(f'Orders with FREE shipping: {len(orders_free_ship)}')
print()

total_buyer_paid = sum(orders_with_shipping.values())
print(f'Total buyers paid for shipping: ${total_buyer_paid:,.2f}')
print(f'Avg buyer shipping per charged order: ${total_buyer_paid/len(orders_with_ship) if orders_with_ship else 0:.2f}')
print()

# Label costs
total_labels = abs(data[data['Type'] == 'Shipping']['Net_Clean'].sum())
label_count = len(data[data['Type'] == 'Shipping'])
avg_label = total_labels / label_count if label_count else 0
print(f'Total label cost: ${total_labels:,.2f}')
print(f'Total labels: {label_count}')
print(f'Avg label cost: ${avg_label:.2f}')
print()

# Estimated shipping P&L for orders that charged shipping
est_label_cost_charged = len(orders_with_ship) * avg_label
print(f'--- ORDERS THAT CHARGED SHIPPING ({len(orders_with_ship)}) ---')
print(f'Buyer paid total: ${total_buyer_paid:,.2f}')
print(f'Est label cost ({len(orders_with_ship)} x ${avg_label:.2f} avg): ${est_label_cost_charged:,.2f}')
print(f'Est shipping profit: ${total_buyer_paid - est_label_cost_charged:,.2f}')
print()

est_label_cost_free = len(orders_free_ship) * avg_label
print(f'--- FREE SHIPPING ORDERS ({len(orders_free_ship)}) ---')
print(f'Buyer paid: $0.00')
print(f'Est label cost ({len(orders_free_ship)} x ${avg_label:.2f} avg): ${est_label_cost_free:,.2f}')
print(f'Pure loss on free shipping: -${est_label_cost_free:,.2f}')
print()

# Show distribution of buyer-paid shipping amounts
amounts = sorted(orders_with_shipping.values())
print(f'--- BUYER SHIPPING AMOUNTS ---')
print(f'Min: ${min(amounts):.2f}')
print(f'Max: ${max(amounts):.2f}')
print(f'Median: ${amounts[len(amounts)//2]:.2f}')
print()

# Bucket by amount
buckets = {}
for amt in amounts:
    bucket = f'${int(amt//5)*5}-${int(amt//5)*5+5}'
    buckets[bucket] = buckets.get(bucket, 0) + 1
print('Buyer shipping amount distribution:')
for b, c in sorted(buckets.items()):
    print(f'  {b}: {c} orders')

# Show per-order detail for a sample
print()
print('--- SAMPLE: Per-order shipping detail ---')
count = 0
for order in sorted(orders_with_ship)[:15]:
    buyer_paid = orders_with_shipping[order]
    # Get sale amount
    sale_row = sale_rows[sale_rows['Order'] == order]
    sale_amt = sale_row['Net_Clean'].values[0] if len(sale_row) else 0
    print(f'  {order}: Sale ${sale_amt:.2f}  |  Buyer shipping ${buyer_paid:.2f}  |  Est label ~${avg_label:.2f}  |  Est profit ${buyer_paid - avg_label:.2f}')
