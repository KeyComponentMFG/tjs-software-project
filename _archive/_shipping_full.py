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

ship = data[data['Type'] == 'Shipping']

print('=== SHIPPING BY TITLE ===')
for title in ship['Title'].unique():
    rows = ship[ship['Title'] == title]
    total = rows['Net_Clean'].sum()
    count = len(rows)
    print(f'  {title}: {count} entries, total ${total:.2f}')

print()
print('=== RETURN LABELS ===')
returns = ship[ship['Title'].str.contains('return', case=False, na=False)]
for _, r in returns.iterrows():
    print(f'  {r["Date"]:25s} {r["Title"]:45s} {r["Info"]:30s} Net: {r["Net_Clean"]:.2f}')

print()
print('=== INSURANCE ===')
insurance = ship[ship['Title'].str.contains('insurance', case=False, na=False)]
for _, r in insurance.iterrows():
    print(f'  {r["Date"]:25s} {r["Title"]:45s} {r["Info"]:30s} Net: {r["Net_Clean"]:.2f}')

print()
print('=== ADJUSTMENTS & CREDITS ===')
adj = ship[ship['Title'].str.contains('Adjustment|Credit', case=False, na=False)]
for _, r in adj.iterrows():
    print(f'  {r["Date"]:25s} {r["Title"]:55s} Net: {r["Net_Clean"]:.2f}')

print()
print('=== ASENDIA (international?) ===')
asendia = ship[ship['Title'].str.contains('Asendia|Globe', case=False, na=False)]
for _, r in asendia.iterrows():
    print(f'  {r["Date"]:25s} {r["Title"]:55s} Net: {r["Net_Clean"]:.2f}')

print()
# Regular outbound labels only
outbound = ship[ship['Title'] == 'USPS shipping label']
print(f'=== REGULAR OUTBOUND USPS LABELS ===')
print(f'  Count: {len(outbound)}')
print(f'  Total: ${outbound["Net_Clean"].sum():.2f}')
print(f'  Avg: ${outbound["Net_Clean"].abs().mean():.2f}')
