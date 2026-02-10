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

# Sample a few orders to verify logic
orders_with_ship_fee = data[data['Title'].str.contains('Transaction fee: Shipping', na=False)].head(5)
for _, row in orders_with_ship_fee.iterrows():
    order = row['Info']
    print(f'=== {order} ===')
    order_rows = data[data['Info'] == order]
    for _, r in order_rows.iterrows():
        print(f'  {r["Type"]:12s} | {r["Title"][:55]:55s} | Net: {r["Net_Clean"]:8.2f}')
    ship_fee = abs(row['Net_Clean'])
    buyer_paid = ship_fee / 0.065
    print(f'  >> Buyer paid for shipping (est): ${buyer_paid:.2f}')
    print()

# Totals
ship_fees = abs(data[data['Title'].str.contains('Transaction fee: Shipping', na=False)]['Net_Clean'].sum())
ship_credits = abs(data[data['Title'].str.contains('Credit for transaction fee on shipping', na=False)]['Net_Clean'].sum())
net_ship_fees = ship_fees - ship_credits
buyer_shipping_total = net_ship_fees / 0.065
label_cost = abs(data[data['Type'] == 'Shipping']['Net_Clean'].sum())

print('=== SHIPPING TOTALS ===')
print(f'Shipping fees charged by Etsy:      ${ship_fees:.2f}')
print(f'Shipping fee credits:               ${ship_credits:.2f}')
print(f'Net shipping fees:                  ${net_ship_fees:.2f}')
print(f'Est. buyers paid for shipping:      ${buyer_shipping_total:.2f}')
print(f'Shipping labels purchased:          ${label_cost:.2f}')
print(f'Shipping profit/loss:               ${buyer_shipping_total - label_cost:.2f}')
print(f'Shipping margin:                    {(buyer_shipping_total - label_cost)/buyer_shipping_total*100:.1f}%')
