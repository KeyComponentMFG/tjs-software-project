"""
Etsy Statement Organizer
Processes all Etsy CSV statements and creates organized summaries
"""

import pandas as pd
import os
from datetime import datetime

# Get all CSV files in the directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
folder = os.path.join(BASE_DIR, "data", "etsy_statements")
csv_files = [f for f in os.listdir(folder) if f.endswith('.csv') and f.startswith('etsy_statement')]

print("=" * 70)
print("ETSY BUSINESS SUMMARY REPORT")
print("=" * 70)
print(f"\nFiles processed: {len(csv_files)}")
for f in sorted(csv_files):
    print(f"  - {f}")

# Read and combine all CSVs
all_data = []
for csv_file in csv_files:
    df = pd.read_csv(os.path.join(folder, csv_file))
    all_data.append(df)

combined = pd.concat(all_data, ignore_index=True)

# Clean up the Amount and Net columns
def parse_money(val):
    if pd.isna(val) or val == '--':
        return 0.0
    val = str(val).replace('$', '').replace(',', '')
    try:
        return float(val)
    except:
        return 0.0

combined['Amount_Clean'] = combined['Amount'].apply(parse_money)
combined['Net_Clean'] = combined['Net'].apply(parse_money)
combined['Fees_Clean'] = combined['Fees & Taxes'].apply(parse_money)

# Calculate totals by type
print("\n" + "=" * 70)
print("BREAKDOWN BY TRANSACTION TYPE")
print("=" * 70)

type_summary = combined.groupby('Type').agg({
    'Net_Clean': 'sum',
    'Type': 'count'
}).rename(columns={'Type': 'Count', 'Net_Clean': 'Total'})

type_summary = type_summary.sort_values('Total', ascending=False)

for idx, row in type_summary.iterrows():
    print(f"\n{idx}:")
    print(f"  Count: {row['Count']:,.0f}")
    print(f"  Total: ${row['Total']:,.2f}")

# Calculate key metrics
sales = combined[combined['Type'] == 'Sale']['Net_Clean'].sum()
fees = combined[combined['Type'] == 'Fee']['Net_Clean'].sum()
shipping = combined[combined['Type'] == 'Shipping']['Net_Clean'].sum()
marketing = combined[combined['Type'] == 'Marketing']['Net_Clean'].sum()
taxes = combined[combined['Type'] == 'Tax']['Net_Clean'].sum()
refunds = combined[combined['Type'] == 'Refund']['Net_Clean'].sum()
deposits = combined[combined['Type'] == 'Deposit']['Net_Clean'].sum()

print("\n" + "=" * 70)
print("FINANCIAL SUMMARY")
print("=" * 70)

print(f"\nGROSS SALES:           ${sales:>12,.2f}")
print(f"\nDEDUCTIONS:")
print(f"  Etsy Fees:           ${fees:>12,.2f}")
print(f"  Shipping Labels:     ${shipping:>12,.2f}")
print(f"  Marketing/Ads:       ${marketing:>12,.2f}")
print(f"  Taxes Collected:     ${taxes:>12,.2f}")
print(f"  Refunds:             ${refunds:>12,.2f}")

total_deductions = fees + shipping + marketing + taxes + refunds
net_revenue = sales + total_deductions  # deductions are negative

print(f"\nTOTAL DEDUCTIONS:      ${total_deductions:>12,.2f}")
print(f"\n{'='*40}")
print(f"NET REVENUE:           ${net_revenue:>12,.2f}")
print(f"{'='*40}")

# Calculate deposits to bank
print(f"\nDEPOSITS TO BANK:      ${deposits:>12,.2f} (just tracking, not in calculation)")

# Monthly breakdown
print("\n" + "=" * 70)
print("MONTHLY BREAKDOWN")
print("=" * 70)

# Parse dates
combined['Date_Parsed'] = pd.to_datetime(combined['Date'], format='%B %d, %Y', errors='coerce')
combined['Month'] = combined['Date_Parsed'].dt.to_period('M')

monthly = combined.groupby('Month').agg({
    'Net_Clean': 'sum'
}).sort_index()

for month, row in monthly.iterrows():
    print(f"{month}: ${row['Net_Clean']:>12,.2f}")

# Top selling products
print("\n" + "=" * 70)
print("TOP SELLING PRODUCTS")
print("=" * 70)

sales_df = combined[combined['Type'] == 'Sale'].copy()
sales_df['Product'] = sales_df['Title'].str.replace('Payment for Order #', '').str.strip()

# Get product names from transaction fees
fee_df = combined[combined['Title'].str.contains('Transaction fee:', na=False)].copy()
fee_df['Product'] = fee_df['Title'].str.replace('Transaction fee: ', '').str.replace('Transaction fee: Shipping', 'Shipping').str[:50]

product_counts = fee_df[~fee_df['Product'].str.contains('Shipping', na=False)]['Product'].value_counts().head(15)

print("\nMost frequently sold items:")
for product, count in product_counts.items():
    print(f"  {count:>3}x  {product}...")

# Order count
order_count = len(sales_df)
print(f"\n\nTOTAL ORDERS: {order_count}")
print(f"AVERAGE ORDER VALUE: ${sales / order_count if order_count > 0 else 0:,.2f}")

# Fee breakdown
print("\n" + "=" * 70)
print("FEE BREAKDOWN")
print("=" * 70)

fee_types = combined[combined['Type'] == 'Fee']['Title'].apply(
    lambda x: 'Listing Fee' if 'Listing fee' in str(x)
    else 'Transaction Fee' if 'Transaction fee' in str(x)
    else 'Processing Fee' if 'Processing fee' in str(x)
    else 'Credit/Refund' if 'Credit' in str(x)
    else 'Other Fee'
)

fee_breakdown = combined[combined['Type'] == 'Fee'].copy()
fee_breakdown['Fee_Type'] = fee_types.values

fee_summary = fee_breakdown.groupby('Fee_Type')['Net_Clean'].sum().sort_values()
for fee_type, amount in fee_summary.items():
    print(f"  {fee_type:20s}: ${amount:>10,.2f}")

# Marketing breakdown
print("\n" + "=" * 70)
print("MARKETING BREAKDOWN")
print("=" * 70)

marketing_df = combined[combined['Type'] == 'Marketing'].copy()
etsy_ads = marketing_df[marketing_df['Title'].str.contains('Etsy Ads', na=False)]['Net_Clean'].sum()
offsite_ads = marketing_df[marketing_df['Title'].str.contains('Offsite Ads', na=False)]['Net_Clean'].sum()
offsite_credits = marketing_df[marketing_df['Title'].str.contains('Credit for Offsite', na=False)]['Net_Clean'].sum()

print(f"  Etsy Ads:           ${etsy_ads:>10,.2f}")
print(f"  Offsite Ads Fees:   ${offsite_ads:>10,.2f}")
print(f"  Offsite Ads Credits:${offsite_credits:>10,.2f}")

# Save summary to file
generated_dir = os.path.join(BASE_DIR, "data", "generated")
os.makedirs(generated_dir, exist_ok=True)
summary_file = os.path.join(generated_dir, "ETSY_SUMMARY_REPORT.txt")
with open(summary_file, 'w') as f:
    f.write("=" * 70 + "\n")
    f.write("ETSY BUSINESS SUMMARY REPORT\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("=" * 70 + "\n\n")

    f.write("FINANCIAL SUMMARY\n")
    f.write("-" * 40 + "\n")
    f.write(f"Gross Sales:          ${sales:>12,.2f}\n")
    f.write(f"Etsy Fees:            ${fees:>12,.2f}\n")
    f.write(f"Shipping Labels:      ${shipping:>12,.2f}\n")
    f.write(f"Marketing/Ads:        ${marketing:>12,.2f}\n")
    f.write(f"Taxes Collected:      ${taxes:>12,.2f}\n")
    f.write(f"Refunds:              ${refunds:>12,.2f}\n")
    f.write("-" * 40 + "\n")
    f.write(f"NET REVENUE:          ${net_revenue:>12,.2f}\n")
    f.write("-" * 40 + "\n\n")

    f.write(f"Total Orders: {order_count}\n")
    f.write(f"Avg Order Value: ${sales / order_count if order_count > 0 else 0:,.2f}\n\n")

    f.write("MONTHLY BREAKDOWN\n")
    f.write("-" * 40 + "\n")
    for month, row in monthly.iterrows():
        f.write(f"{month}: ${row['Net_Clean']:>12,.2f}\n")

print(f"\n\nSummary saved to: {summary_file}")

# Also create a combined CSV with all transactions
combined_file = os.path.join(generated_dir, "ALL_TRANSACTIONS_COMBINED.csv")
combined.to_csv(combined_file, index=False)
print(f"All transactions saved to: {combined_file}")

print("\n" + "=" * 70)
print("DONE!")
print("=" * 70)
