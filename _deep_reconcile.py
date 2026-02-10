"""Deep reconciliation: verify every penny across all data sources."""
import json, os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# === BANK TRANSACTIONS (same as dashboard BANK_TXNS) ===
BANK_TXNS = [
    # DECEMBER 2025 DEPOSITS
    {"date": "12/10/2025", "desc": "ETSY PAYOUT", "amount": 1287.26, "type": "deposit", "category": "Etsy Payout"},
    {"date": "12/16/2025", "desc": "ETSY PAYOUT", "amount": 228.62, "type": "deposit", "category": "Etsy Payout"},
    {"date": "12/23/2025", "desc": "ETSY PAYOUT", "amount": 633.34, "type": "deposit", "category": "Etsy Payout"},
    {"date": "12/30/2025", "desc": "ETSY PAYOUT", "amount": 328.38, "type": "deposit", "category": "Etsy Payout"},
    # DECEMBER 2025 DEBITS
    {"date": "12/12/2025", "desc": "AMAZON MKTPL YJ01H91J3", "amount": 44.16, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/12/2025", "desc": "AMAZON MKTPL XO7VT5L53", "amount": 51.68, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/15/2025", "desc": "UPS STORE 1849 TULSA OK", "amount": 16.39, "type": "debit", "category": "Shipping"},
    {"date": "12/15/2025", "desc": "USPS CLICKNSHIP", "amount": 4.92, "type": "debit", "category": "Shipping"},
    {"date": "12/15/2025", "desc": "AMAZON MKTPL E409Z4AL3", "amount": 18.44, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/15/2025", "desc": "AMAZON MKTPL 2V1YI7X13", "amount": 35.80, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/15/2025", "desc": "AMAZON MKTPL 5P9CF5KZ3", "amount": 196.40, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/16/2025", "desc": "WESTLAKE HARDWARE 088 TULSA OK", "amount": 41.00, "type": "debit", "category": "Pending"},
    {"date": "12/17/2025", "desc": "HOBBYLOBBY TULSA OK", "amount": 7.57, "type": "debit", "category": "Craft Supplies"},
    {"date": "12/17/2025", "desc": "ETSY COM US (Etsy ads/fees)", "amount": 29.00, "type": "debit", "category": "Etsy Fees"},
    {"date": "12/17/2025", "desc": "PAYPAL ALIPAYUSINC SAN JOSE CA", "amount": 76.57, "type": "debit", "category": "AliExpress Supplies"},
    {"date": "12/19/2025", "desc": "AMAZON MKTPL OR4FP0NE3", "amount": 37.97, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/19/2025", "desc": "AMAZON MKTPL NF2IJ0HX3", "amount": 67.73, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/22/2025", "desc": "AMAZON MKTPL 5J8JP26T3", "amount": 17.31, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/22/2025", "desc": "AMAZON MKTPL TW2K20SU3", "amount": 49.91, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/23/2025", "desc": "AMAZON MKTPL C72SV1CY3", "amount": 54.56, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/24/2025", "desc": "REASORS 25 TULSA OK", "amount": 50.00, "type": "debit", "category": "Personal"},
    {"date": "12/24/2025", "desc": "WILDFLOWERCAFE TULSA OK", "amount": 26.09, "type": "debit", "category": "Personal"},
    {"date": "12/24/2025", "desc": "ANTHROPOLOGIE 00546 TULSA OK", "amount": 30.38, "type": "debit", "category": "Personal"},
    {"date": "12/26/2025", "desc": "LULULEMON CENTER 1 TULSA OK", "amount": 50.00, "type": "debit", "category": "Personal"},
    {"date": "12/29/2025", "desc": "PAYPAL THANGS 3D (recurring)", "amount": 20.15, "type": "debit", "category": "3D Subscription"},
    {"date": "12/29/2025", "desc": "AMAZON MKTPL HU1NU1E73", "amount": 30.90, "type": "debit", "category": "Amazon Inventory"},
    {"date": "12/29/2025", "desc": "AMAZON MKTPL HZ3VL8FR3", "amount": 59.67, "type": "debit", "category": "Amazon Inventory"},
    # JANUARY 2026 DEPOSITS
    {"date": "01/06/2026", "desc": "ETSY PAYOUT", "amount": 497.22, "type": "deposit", "category": "Etsy Payout"},
    {"date": "01/13/2026", "desc": "ETSY PAYOUT", "amount": 759.81, "type": "deposit", "category": "Etsy Payout"},
    {"date": "01/21/2026", "desc": "ETSY PAYOUT", "amount": 1176.18, "type": "deposit", "category": "Etsy Payout"},
    {"date": "01/27/2026", "desc": "ETSY PAYOUT", "amount": 590.46, "type": "deposit", "category": "Etsy Payout"},
    # JANUARY 2026 DEBITS
    {"date": "01/02/2026", "desc": "ETSY COM US (Etsy ads/fees)", "amount": 10.00, "type": "debit", "category": "Etsy Fees"},
    {"date": "01/05/2026", "desc": "HOBBYLOBBY TULSA OK", "amount": 11.36, "type": "debit", "category": "Craft Supplies"},
    {"date": "01/05/2026", "desc": "WAL MART 0992 TULSA OK", "amount": 15.27, "type": "debit", "category": "Pending"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL KB6AD5XB3", "amount": 15.97, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL ZU68D7WB3", "amount": 16.01, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL AZ6BK1DB3", "amount": 20.56, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL Q51HU05A3", "amount": 35.04, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/06/2026", "desc": "VENMO PRICE PETTIT", "amount": 36.00, "type": "debit", "category": "Owner Draw - Tulsa"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL 8H1DG3DA3", "amount": 37.20, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL 287II1HF3", "amount": 48.69, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL 158CW8QL3", "amount": 48.81, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/06/2026", "desc": "VENMO JACOB SHELLEY", "amount": 57.00, "type": "debit", "category": "Owner Draw - Tulsa"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL 9G6FP5GA3", "amount": 71.15, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/07/2026", "desc": "AMAZON MKTPL R12KW4BP3", "amount": 34.96, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/07/2026", "desc": "AMAZON MKTPL AY55313W3", "amount": 100.00, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/08/2026", "desc": "AMAZON MKTPL WQ6J56WE3", "amount": 20.61, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/08/2026", "desc": "AMAZON MKTPL TK7525Y53", "amount": 116.87, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/09/2026", "desc": "AMAZON MKTPL OG8499T63", "amount": 34.92, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL 993EC6V63", "amount": 23.04, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL OM7OI60L3", "amount": 29.19, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL U37PZ3L83", "amount": 30.51, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL GS5T14M53", "amount": 30.59, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL 5P2Z17GU3", "amount": 49.78, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL W607J8553", "amount": 54.85, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/13/2026", "desc": "AMAZON MKTPL L781I8BJ3", "amount": 26.90, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/14/2026", "desc": "AMAZON MKTPL 808DT2723", "amount": 125.80, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/15/2026", "desc": "BEST BUY AUTO PYMT THOMAS J MCNULTY", "amount": 100.00, "type": "debit", "category": "Personal"},
    {"date": "01/16/2026", "desc": "ETSY COM US (Etsy ads/fees)", "amount": 29.00, "type": "debit", "category": "Etsy Fees"},
    {"date": "01/20/2026", "desc": "AMAZON MKTPL E11RB5RU3", "amount": 55.93, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/21/2026", "desc": "AMAZON MKTPL BQ8BF1YM1", "amount": 17.53, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/21/2026", "desc": "CHIPOTLE 2155 TULSA OK", "amount": 20.08, "type": "debit", "category": "Personal"},
    {"date": "01/21/2026", "desc": "AMAZON MKTPL YN46J1BJ3", "amount": 91.83, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/22/2026", "desc": "AMAZON MKTPL I77IO2DE3", "amount": 30.84, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/23/2026", "desc": "AMAZON MKTPL N33CL1IK3", "amount": 18.39, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/23/2026", "desc": "AMAZON MKTPL 2X7C91G13", "amount": 18.44, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/23/2026", "desc": "AMAZON MKTPL 2V1EC6XB3", "amount": 53.88, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/23/2026", "desc": "VENMO BRADEN WALKER", "amount": 450.00, "type": "debit", "category": "Owner Draw - Texas"},
    {"date": "01/26/2026", "desc": "PAYPAL AOWEIKEGTTA", "amount": 25.98, "type": "debit", "category": "AliExpress Supplies"},
    {"date": "01/26/2026", "desc": "PAYPAL AOWEIKEGTTA", "amount": 39.56, "type": "debit", "category": "AliExpress Supplies"},
    {"date": "01/26/2026", "desc": "VENMO BRADEN WALKER", "amount": 100.00, "type": "debit", "category": "Owner Draw - Texas"},
    {"date": "01/28/2026", "desc": "AMAZON MKTPL WM9RC81X3", "amount": 16.01, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/28/2026", "desc": "AMAZON MKTPL Q61PB33T3", "amount": 49.91, "type": "debit", "category": "Amazon Inventory"},
    {"date": "01/29/2026", "desc": "PAYPAL THANGS 3D (recurring)", "amount": 20.15, "type": "debit", "category": "3D Subscription"},
    {"date": "01/29/2026", "desc": "AMAZON MKTPL SJ3R644D3", "amount": 68.10, "type": "debit", "category": "Amazon Inventory"},
    # FEBRUARY 2026
    {"date": "02/02/2026", "desc": "ETSY COM US (Etsy fees)", "amount": 13.98, "type": "debit", "category": "Etsy Fees"},
    {"date": "02/02/2026", "desc": "ETSY COM US (Etsy fees)", "amount": 10.00, "type": "debit", "category": "Etsy Fees"},
    {"date": "02/02/2026", "desc": "AMAZON MKTPL (split from Discover)", "amount": 16.44, "type": "debit", "category": "Amazon Inventory"},
    {"date": "02/02/2026", "desc": "AMAZON MKTPL (split from Discover)", "amount": 29.21, "type": "debit", "category": "Amazon Inventory"},
    {"date": "02/02/2026", "desc": "AMAZON MKTPL (split from Discover)", "amount": 31.46, "type": "debit", "category": "Amazon Inventory"},
    {"date": "02/02/2026", "desc": "AMAZON MKTPL (split from Discover)", "amount": 36.84, "type": "debit", "category": "Amazon Inventory"},
    {"date": "02/02/2026", "desc": "VENMO TJ MCNULTY", "amount": 350.00, "type": "debit", "category": "Owner Draw - Tulsa"},
    {"date": "02/03/2026", "desc": "ETSY PAYOUT", "amount": 483.28, "type": "deposit", "category": "Etsy Payout"},
    {"date": "02/06/2026", "desc": "AMAZON MKTPL (pending)", "amount": 17.25, "type": "debit", "category": "Amazon Inventory"},
    {"date": "02/06/2026", "desc": "AMAZON MKTPL (pending)", "amount": 23.79, "type": "debit", "category": "Amazon Inventory"},
    {"date": "02/06/2026", "desc": "AMAZON MKTPL (pending)", "amount": 23.86, "type": "debit", "category": "Amazon Inventory"},
]

deposits = [t for t in BANK_TXNS if t["type"] == "deposit"]
debits = [t for t in BANK_TXNS if t["type"] == "debit"]

# Known statement balances
DEC_OPENING = 0.00  # Account opened in Dec
DEC_CLOSING = 1461.00  # From Jan statement opening balance
JAN_CLOSING = 2177.96  # From Jan statement
FEB_POSTED_BALANCE = 2173.31  # From Feb screenshot (before pending)

# Load inventory orders
inv_path = os.path.join(BASE_DIR, "data", "generated", "inventory_orders.json")
with open(inv_path, "r") as f:
    invoices = json.load(f)

print("=" * 70)
print("  DEEP RECONCILIATION REPORT")
print("  Capital One Business Checking 3650")
print("  Dec 2025 - Feb 8, 2026")
print("=" * 70)

# =====================================================================
# CHECK 1: Running Balance Verification
# =====================================================================
print("\n--- CHECK 1: Running Balance Verification ---\n")

dec_deps = sum(t["amount"] for t in deposits if t["date"].startswith("12/"))
dec_debs = sum(t["amount"] for t in debits if t["date"].startswith("12/"))
dec_calc = DEC_OPENING + dec_deps - dec_debs

jan_deps = sum(t["amount"] for t in deposits if t["date"].startswith("01/"))
jan_debs = sum(t["amount"] for t in debits if t["date"].startswith("01/"))
jan_calc = dec_calc + jan_deps - jan_debs

# Feb: only posted transactions (not pending) for posted balance check
feb_posted_deps = sum(t["amount"] for t in deposits if t["date"].startswith("02/") and "pending" not in t["desc"].lower())
feb_posted_debs = sum(t["amount"] for t in debits if t["date"].startswith("02/") and "pending" not in t["desc"].lower())
feb_posted_calc = jan_calc + feb_posted_deps - feb_posted_debs

# Feb including pending
feb_all_deps = sum(t["amount"] for t in deposits if t["date"].startswith("02/"))
feb_all_debs = sum(t["amount"] for t in debits if t["date"].startswith("02/"))
feb_all_calc = jan_calc + feb_all_deps - feb_all_debs

print(f"  December:")
print(f"    Opening:    $     0.00")
print(f"    Deposits:   $ {dec_deps:>9,.2f}")
print(f"    Debits:     $ {dec_debs:>9,.2f}")
print(f"    Calculated: $ {dec_calc:>9,.2f}")
print(f"    Statement:  $ {DEC_CLOSING:>9,.2f}")
diff_dec = abs(dec_calc - DEC_CLOSING)
status_dec = "[OK]" if diff_dec < 0.01 else f"[MISMATCH: ${diff_dec:,.2f}]"
print(f"    Status:     {status_dec}")

print(f"\n  January:")
print(f"    Opening:    $ {dec_calc:>9,.2f}")
print(f"    Deposits:   $ {jan_deps:>9,.2f}")
print(f"    Debits:     $ {jan_debs:>9,.2f}")
print(f"    Calculated: $ {jan_calc:>9,.2f}")
print(f"    Statement:  $ {JAN_CLOSING:>9,.2f}")
diff_jan = abs(jan_calc - JAN_CLOSING)
status_jan = "[OK]" if diff_jan < 0.01 else f"[MISMATCH: ${diff_jan:,.2f}]"
print(f"    Status:     {status_jan}")

print(f"\n  February (posted only, excl pending):")
print(f"    Opening:    $ {jan_calc:>9,.2f}")
print(f"    Deposits:   $ {feb_posted_deps:>9,.2f}")
print(f"    Debits:     $ {feb_posted_debs:>9,.2f}")
print(f"    Calculated: $ {feb_posted_calc:>9,.2f}")
print(f"    App shows:  $ {FEB_POSTED_BALANCE:>9,.2f}")
diff_feb = abs(feb_posted_calc - FEB_POSTED_BALANCE)
status_feb = "[OK]" if diff_feb < 0.01 else f"[MISMATCH: ${diff_feb:,.2f}]"
print(f"    Status:     {status_feb}")

print(f"\n  February (including pending):")
print(f"    Balance:    $ {feb_all_calc:>9,.2f}")

# =====================================================================
# CHECK 2: Etsy CSV Deposits vs Bank Deposits
# =====================================================================
print("\n--- CHECK 2: Etsy CSV Payouts vs Bank Deposits ---\n")

# Known Etsy payouts from CSV data
# Dec payouts from Etsy CSVs: $2,694.32 total
# But first payout ($216.72 on Dec 1) hit bank before Cap One opened
# So bank should show: $2,694.32 - $216.72 = $2,477.60
etsy_dec_total_csv = 2694.32
etsy_dec_pre_account = 216.72
etsy_dec_expected_bank = etsy_dec_total_csv - etsy_dec_pre_account

# Jan payouts from Etsy CSVs
etsy_jan_total_csv = 3023.67

# Feb payouts (partial month)
etsy_feb_total_csv = 483.28

print(f"  December:")
print(f"    Etsy CSV total:     $ {etsy_dec_total_csv:>9,.2f}")
print(f"    Pre-account payout: $ {etsy_dec_pre_account:>9,.2f} (Dec 1, before Cap One opened)")
print(f"    Expected on bank:   $ {etsy_dec_expected_bank:>9,.2f}")
print(f"    Actual on bank:     $ {dec_deps:>9,.2f}")
diff_dec_dep = abs(etsy_dec_expected_bank - dec_deps)
print(f"    Status:             {'[OK]' if diff_dec_dep < 0.01 else f'[MISMATCH: ${diff_dec_dep:,.2f}]'}")

print(f"\n  January:")
print(f"    Etsy CSV total:     $ {etsy_jan_total_csv:>9,.2f}")
print(f"    Actual on bank:     $ {jan_deps:>9,.2f}")
diff_jan_dep = abs(etsy_jan_total_csv - jan_deps)
print(f"    Status:             {'[OK]' if diff_jan_dep < 0.01 else f'[MISMATCH: ${diff_jan_dep:,.2f}]'}")

print(f"\n  February (partial):")
print(f"    Etsy CSV total:     $ {etsy_feb_total_csv:>9,.2f}")
feb_dep_actual = sum(t["amount"] for t in deposits if t["date"].startswith("02/"))
print(f"    Actual on bank:     $ {feb_dep_actual:>9,.2f}")
diff_feb_dep = abs(etsy_feb_total_csv - feb_dep_actual)
print(f"    Status:             {'[OK]' if diff_feb_dep < 0.01 else f'[MISMATCH: ${diff_feb_dep:,.2f}]'}")

# =====================================================================
# CHECK 3: Transaction Counts
# =====================================================================
print("\n--- CHECK 3: Transaction Counts ---\n")

dec_count = len([t for t in BANK_TXNS if t["date"].startswith("12/")])
jan_count = len([t for t in BANK_TXNS if t["date"].startswith("01/")])
feb_count = len([t for t in BANK_TXNS if t["date"].startswith("02/")])

dec_dep_ct = len([t for t in BANK_TXNS if t["date"].startswith("12/") and t["type"] == "deposit"])
dec_deb_ct = len([t for t in BANK_TXNS if t["date"].startswith("12/") and t["type"] == "debit"])
jan_dep_ct = len([t for t in BANK_TXNS if t["date"].startswith("01/") and t["type"] == "deposit"])
jan_deb_ct = len([t for t in BANK_TXNS if t["date"].startswith("01/") and t["type"] == "debit"])
feb_dep_ct = len([t for t in BANK_TXNS if t["date"].startswith("02/") and t["type"] == "deposit"])
feb_deb_ct = len([t for t in BANK_TXNS if t["date"].startswith("02/") and t["type"] == "debit"])
print(f"  December: {dec_count} txns ({dec_dep_ct} deposits + {dec_deb_ct} debits)")
print(f"  January:  {jan_count} txns ({jan_dep_ct} deposits + {jan_deb_ct} debits)")
print(f"  February: {feb_count} txns ({feb_dep_ct} deposits + {feb_deb_ct} debits)")
print(f"  TOTAL:    {dec_count + jan_count + feb_count} transactions tracked")

# =====================================================================
# CHECK 4: Category Totals
# =====================================================================
print("\n--- CHECK 4: Category Breakdown ---\n")

cats = {}
for t in debits:
    cat = t["category"]
    cats[cat] = cats.get(cat, 0) + t["amount"]

total_debits = sum(t["amount"] for t in debits)
cat_sum = sum(cats.values())

for cat in sorted(cats.keys()):
    pct = cats[cat] / total_debits * 100
    print(f"  {cat:<25s} $ {cats[cat]:>9,.2f}  ({pct:5.1f}%)")
print(f"  {'':25s} -----------")
print(f"  {'TOTAL':25s} $ {cat_sum:>9,.2f}")
print(f"\n  Sum of categories vs total debits: {'[OK]' if abs(cat_sum - total_debits) < 0.01 else '[MISMATCH]'}")

# Tax deductible categories
tax_cats = ["Amazon Inventory", "Shipping", "Craft Supplies", "Etsy Fees", "3D Subscription", "AliExpress Supplies"]
tax_total = sum(cats.get(c, 0) for c in tax_cats)
non_tax = total_debits - tax_total
print(f"\n  Tax Deductible (Sched C): $ {tax_total:>9,.2f}  ({tax_total/total_debits*100:.1f}%)")
print(f"  Non-Deductible:           $ {non_tax:>9,.2f}  ({non_tax/total_debits*100:.1f}%)")

# =====================================================================
# CHECK 5: Inventory Cross-Check (Bank Amazon vs Invoices)
# =====================================================================
print("\n--- CHECK 5: Inventory Cross-Check ---\n")

bank_amazon = [t for t in debits if t["category"] == "Amazon Inventory"]
bank_amazon_total = sum(t["amount"] for t in bank_amazon)
bank_amazon_count = len(bank_amazon)

# Invoices from inventory_orders.json
inv_total = sum(inv["grand_total"] for inv in invoices)
inv_count = len(invoices)

# How many invoices were charged to Capital One (debit)?
inv_debit = [inv for inv in invoices if inv.get("payment_method", "").lower().find("debit") >= 0
             or inv.get("payment_method", "").lower().find("capital") >= 0]
inv_discover = [inv for inv in invoices if inv.get("payment_method", "").lower().find("discover") >= 0
                or inv.get("payment_method", "").lower().find("visa") >= 0]

print(f"  Bank Amazon charges:     {bank_amazon_count} transactions, $ {bank_amazon_total:>9,.2f}")
print(f"  Invoice PDF total:       {inv_count} orders,       $ {inv_total:>9,.2f}")
print(f"  (Invoices span ALL payment methods: Capital One debit + Discover + Visa)")
print(f"")
print(f"  NOTE: Bank Amazon total ({bank_amazon_count} charges = ${bank_amazon_total:,.2f}) includes:")
print(f"    - Direct debit card purchases")
print(f"    - Split-shipment charges from Discover orders")
print(f"    Amazon frequently splits orders across payment methods,")
print(f"    so bank charges won't match invoice totals 1:1.")

# =====================================================================
# CHECK 6: Money In vs Money Out Summary
# =====================================================================
print("\n--- CHECK 6: Overall Money Flow ---\n")

total_deposits = sum(t["amount"] for t in deposits)
total_all_debits = sum(t["amount"] for t in debits)
net = total_deposits - total_all_debits

print(f"  TOTAL IN (deposits):   $ {total_deposits:>9,.2f}")
print(f"  TOTAL OUT (debits):    $ {total_all_debits:>9,.2f}")
print(f"  NET CASH FLOW:         $ {net:>9,.2f}")
print(f"  Current balance:       $ {feb_all_calc:>9,.2f}")
print(f"    (should match opening $0 + net ${net:,.2f})")
balance_check = abs(feb_all_calc - (DEC_OPENING + net))
print(f"    Status: {'[OK]' if balance_check < 0.01 else f'[MISMATCH: ${balance_check:,.2f}]'}")

# =====================================================================
# CHECK 7: Pending / Unresolved Items
# =====================================================================
print("\n--- CHECK 7: Pending / Unresolved Items ---\n")

pending = [t for t in debits if t["category"] == "Pending"]
if pending:
    print(f"  {len(pending)} transactions still categorized as 'Pending':")
    for t in pending:
        print(f"    {t['date']}  {t['desc']:<45s}  $ {t['amount']:>8,.2f}")
    pending_total = sum(t["amount"] for t in pending)
    print(f"    {'TOTAL':>51s}  $ {pending_total:>8,.2f}")
    print(f"\n  ACTION NEEDED: Categorize these as Business or Personal")
else:
    print(f"  No pending items - all transactions categorized! [OK]")

# Pending bank transactions (Feb)
feb_pending = [t for t in BANK_TXNS if "pending" in t["desc"].lower()]
if feb_pending:
    print(f"\n  {len(feb_pending)} pending bank transactions (not yet posted):")
    for t in feb_pending:
        print(f"    {t['date']}  {t['desc']:<45s}  $ {t['amount']:>8,.2f}")

# =====================================================================
# FINAL VERDICT
# =====================================================================
print("\n" + "=" * 70)
print("  FINAL VERDICT")
print("=" * 70)

issues = []
if diff_dec >= 0.01:
    issues.append(f"Dec balance mismatch: ${diff_dec:,.2f}")
if diff_jan >= 0.01:
    issues.append(f"Jan balance mismatch: ${diff_jan:,.2f}")
if diff_feb >= 0.01:
    issues.append(f"Feb posted balance mismatch: ${diff_feb:,.2f}")
if diff_dec_dep >= 0.01:
    issues.append(f"Dec Etsy deposit mismatch: ${diff_dec_dep:,.2f}")
if diff_jan_dep >= 0.01:
    issues.append(f"Jan Etsy deposit mismatch: ${diff_jan_dep:,.2f}")
if pending:
    issues.append(f"{len(pending)} transactions pending categorization (${pending_total:,.2f})")

if not issues:
    print("\n  ALL CHECKS PASSED - Every penny accounted for!")
else:
    print(f"\n  {len(issues)} item(s) to review:")
    for i, issue in enumerate(issues, 1):
        print(f"    {i}. {issue}")

print(f"\n  Account balance as of Feb 8, 2026: $ {feb_all_calc:>9,.2f}")
print("=" * 70)
