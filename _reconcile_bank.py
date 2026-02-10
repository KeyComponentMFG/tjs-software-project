"""Reconcile bank statement transactions against invoice data to find missing receipts."""
import json
from datetime import datetime, timedelta

with open(r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\data\generated\inventory_orders.json") as f:
    orders = json.load(f)

# Build list of invoice amounts with dates for matching
invoice_matches = []
for o in orders:
    try:
        dt = datetime.strptime(o["date"], "%B %d, %Y")
    except:
        dt = None
    invoice_matches.append({
        "amount": o["grand_total"],
        "date": dt,
        "order_num": o.get("order_num", ""),
        "source": o["source"],
        "items": [it["name"][:60] for it in o["items"]],
        "matched": False,
    })

# All bank transactions extracted from the PDFs
bank_txns = [
    # === DECEMBER 2025 ===
    # DEPOSITS
    {"date": "12/10/2025", "desc": "ETSY PAYOUT", "amount": 1287.26, "type": "deposit"},
    {"date": "12/16/2025", "desc": "ETSY PAYOUT", "amount": 228.62, "type": "deposit"},
    {"date": "12/23/2025", "desc": "ETSY PAYOUT", "amount": 633.34, "type": "deposit"},
    {"date": "12/30/2025", "desc": "ETSY PAYOUT", "amount": 328.38, "type": "deposit"},
    # DEBITS
    {"date": "12/12/2025", "desc": "AMAZON MKTPL YJ01H91J3", "amount": 44.16, "type": "debit"},
    {"date": "12/12/2025", "desc": "AMAZON MKTPL XO7VT5L53", "amount": 51.68, "type": "debit"},
    {"date": "12/15/2025", "desc": "UPS STORE 1849 TULSA OK", "amount": 16.39, "type": "debit"},
    {"date": "12/15/2025", "desc": "USPS CLICKNSHIP", "amount": 4.92, "type": "debit"},
    {"date": "12/15/2025", "desc": "AMAZON MKTPL E409Z4AL3", "amount": 18.44, "type": "debit"},
    {"date": "12/15/2025", "desc": "AMAZON MKTPL 2V1YI7X13", "amount": 35.80, "type": "debit"},
    {"date": "12/15/2025", "desc": "AMAZON MKTPL 5P9CF5KZ3", "amount": 196.40, "type": "debit"},
    {"date": "12/16/2025", "desc": "WESTLAKE HARDWARE 088 TULSA OK", "amount": 41.00, "type": "debit"},
    {"date": "12/17/2025", "desc": "HOBBYLOBBY TULSA OK", "amount": 7.57, "type": "debit"},
    {"date": "12/17/2025", "desc": "ETSY COM US (Etsy ads/fees)", "amount": 29.00, "type": "debit"},
    {"date": "12/17/2025", "desc": "PAYPAL ALIPAYUSINC SAN JOSE CA", "amount": 76.57, "type": "debit"},
    {"date": "12/19/2025", "desc": "AMAZON MKTPL OR4FP0NE3", "amount": 37.97, "type": "debit"},
    {"date": "12/19/2025", "desc": "AMAZON MKTPL NF2IJ0HX3", "amount": 67.73, "type": "debit"},
    {"date": "12/22/2025", "desc": "AMAZON MKTPL 5J8JP26T3", "amount": 17.31, "type": "debit"},
    {"date": "12/22/2025", "desc": "AMAZON MKTPL TW2K20SU3", "amount": 49.91, "type": "debit"},
    {"date": "12/23/2025", "desc": "AMAZON MKTPL C72SV1CY3", "amount": 54.56, "type": "debit"},
    {"date": "12/24/2025", "desc": "REASORS 25 TULSA OK", "amount": 50.00, "type": "debit"},
    {"date": "12/24/2025", "desc": "WILDFLOWERCAFE TULSA OK", "amount": 26.09, "type": "debit"},
    {"date": "12/24/2025", "desc": "ANTHROPOLOGIE 00546 TULSA OK", "amount": 30.38, "type": "debit"},
    {"date": "12/26/2025", "desc": "LULULEMON CENTER 1 TULSA OK", "amount": 50.00, "type": "debit"},
    {"date": "12/29/2025", "desc": "PAYPAL THANGS 3D (recurring)", "amount": 20.15, "type": "debit"},
    {"date": "12/29/2025", "desc": "AMAZON MKTPL HU1NU1E73", "amount": 30.90, "type": "debit"},
    {"date": "12/29/2025", "desc": "AMAZON MKTPL HZ3VL8FR3", "amount": 59.67, "type": "debit"},
    # === JANUARY 2026 ===
    # DEPOSITS
    {"date": "01/06/2026", "desc": "ETSY PAYOUT", "amount": 497.22, "type": "deposit"},
    {"date": "01/13/2026", "desc": "ETSY PAYOUT", "amount": 759.81, "type": "deposit"},
    {"date": "01/21/2026", "desc": "ETSY PAYOUT", "amount": 1176.18, "type": "deposit"},
    {"date": "01/27/2026", "desc": "ETSY PAYOUT", "amount": 590.46, "type": "deposit"},
    # DEBITS
    {"date": "01/02/2026", "desc": "ETSY COM US (Etsy ads/fees)", "amount": 10.00, "type": "debit"},
    {"date": "01/05/2026", "desc": "HOBBYLOBBY TULSA OK", "amount": 11.36, "type": "debit"},
    {"date": "01/05/2026", "desc": "WAL MART 0992 TULSA OK", "amount": 15.27, "type": "debit"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL KB6AD5XB3", "amount": 15.97, "type": "debit"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL ZU68D7WB3", "amount": 16.01, "type": "debit"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL AZ6BK1DB3", "amount": 20.56, "type": "debit"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL Q51HU05A3", "amount": 35.04, "type": "debit"},
    {"date": "01/06/2026", "desc": "VENMO PRICE PETTIT", "amount": 36.00, "type": "debit"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL 8H1DG3DA3", "amount": 37.20, "type": "debit"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL 287II1HF3", "amount": 48.69, "type": "debit"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL 158CW8QL3", "amount": 48.81, "type": "debit"},
    {"date": "01/06/2026", "desc": "VENMO JACOB SHELLEY", "amount": 57.00, "type": "debit"},
    {"date": "01/06/2026", "desc": "AMAZON MKTPL 9G6FP5GA3", "amount": 71.15, "type": "debit"},
    {"date": "01/07/2026", "desc": "AMAZON MKTPL R12KW4BP3", "amount": 34.96, "type": "debit"},
    {"date": "01/07/2026", "desc": "AMAZON MKTPL AY55313W3", "amount": 100.00, "type": "debit"},
    {"date": "01/08/2026", "desc": "AMAZON MKTPL WQ6J56WE3", "amount": 20.61, "type": "debit"},
    {"date": "01/08/2026", "desc": "AMAZON MKTPL TK7525Y53", "amount": 116.87, "type": "debit"},
    {"date": "01/09/2026", "desc": "AMAZON MKTPL OG8499T63", "amount": 34.92, "type": "debit"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL 993EC6V63", "amount": 23.04, "type": "debit"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL OM7OI60L3", "amount": 29.19, "type": "debit"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL U37PZ3L83", "amount": 30.51, "type": "debit"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL GS5T14M53", "amount": 30.59, "type": "debit"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL 5P2Z17GU3", "amount": 49.78, "type": "debit"},
    {"date": "01/12/2026", "desc": "AMAZON MKTPL W607J8553", "amount": 54.85, "type": "debit"},
    {"date": "01/13/2026", "desc": "AMAZON MKTPL L781I8BJ3", "amount": 26.90, "type": "debit"},
    {"date": "01/14/2026", "desc": "AMAZON MKTPL 808DT2723", "amount": 125.80, "type": "debit"},
    {"date": "01/15/2026", "desc": "BEST BUY AUTO PYMT THOMAS J MCNULTY", "amount": 100.00, "type": "debit"},
    {"date": "01/16/2026", "desc": "ETSY COM US (Etsy ads/fees)", "amount": 29.00, "type": "debit"},
    {"date": "01/20/2026", "desc": "AMAZON MKTPL E11RB5RU3", "amount": 55.93, "type": "debit"},
    {"date": "01/21/2026", "desc": "AMAZON MKTPL BQ8BF1YM1", "amount": 17.53, "type": "debit"},
    {"date": "01/21/2026", "desc": "CHIPOTLE 2155 TULSA OK", "amount": 20.08, "type": "debit"},
    {"date": "01/21/2026", "desc": "AMAZON MKTPL YN46J1BJ3", "amount": 91.83, "type": "debit"},
    {"date": "01/22/2026", "desc": "AMAZON MKTPL I77IO2DE3", "amount": 30.84, "type": "debit"},
    {"date": "01/23/2026", "desc": "AMAZON MKTPL N33CL1IK3", "amount": 18.39, "type": "debit"},
    {"date": "01/23/2026", "desc": "AMAZON MKTPL 2X7C91G13", "amount": 18.44, "type": "debit"},
    {"date": "01/23/2026", "desc": "AMAZON MKTPL 2V1EC6XB3", "amount": 53.88, "type": "debit"},
    {"date": "01/23/2026", "desc": "VENMO BRADEN WALKER", "amount": 450.00, "type": "debit"},
    {"date": "01/26/2026", "desc": "PAYPAL AOWEIKEGTTA", "amount": 25.98, "type": "debit"},
    {"date": "01/26/2026", "desc": "PAYPAL AOWEIKEGTTA", "amount": 39.56, "type": "debit"},
    {"date": "01/26/2026", "desc": "VENMO BRADEN WALKER", "amount": 100.00, "type": "debit"},
    {"date": "01/27/2026", "desc": "ETSY PAYOUT", "amount": 590.46, "type": "deposit"},
    {"date": "01/28/2026", "desc": "AMAZON MKTPL WM9RC81X3", "amount": 16.01, "type": "debit"},
    {"date": "01/28/2026", "desc": "AMAZON MKTPL Q61PB33T3", "amount": 49.91, "type": "debit"},
    {"date": "01/29/2026", "desc": "PAYPAL THANGS 3D (recurring)", "amount": 20.15, "type": "debit"},
    {"date": "01/29/2026", "desc": "AMAZON MKTPL SJ3R644D3", "amount": 68.10, "type": "debit"},
]

# Parse bank dates
for txn in bank_txns:
    txn["dt"] = datetime.strptime(txn["date"], "%m/%d/%Y")

# Match bank debits to invoices (by amount, within 5 day window)
matched_bank = []
unmatched_bank = []
non_amazon_debits = []

for txn in bank_txns:
    if txn["type"] == "deposit":
        continue

    is_amazon = "AMAZON" in txn["desc"]

    if not is_amazon:
        non_amazon_debits.append(txn)
        continue

    # Try to match by amount (exact match within date window)
    best_match = None
    for inv in invoice_matches:
        if inv["matched"]:
            continue
        if abs(inv["amount"] - txn["amount"]) < 0.02:  # exact amount match
            # Check date proximity (bank date can be 1-5 days after order date)
            if inv["date"]:
                diff = abs((txn["dt"] - inv["date"]).days)
                if diff <= 7:
                    best_match = inv
                    break
            else:
                best_match = inv
                break

    if best_match:
        best_match["matched"] = True
        matched_bank.append({
            "bank_date": txn["date"],
            "bank_amount": txn["amount"],
            "bank_desc": txn["desc"],
            "inv_date": best_match["date"].strftime("%B %d, %Y") if best_match["date"] else "?",
            "inv_amount": best_match["amount"],
            "inv_order": best_match["order_num"],
            "inv_items": best_match["items"],
        })
    else:
        unmatched_bank.append(txn)

# Also find invoices that weren't matched to any bank transaction
unmatched_invoices = [inv for inv in invoice_matches if not inv["matched"]
                       and inv["source"] == "Key Component Mfg"]

# ── REPORT ──
print("=" * 110)
print("  BANK RECONCILIATION: Capital One Checking 3650 vs Invoice Data")
print("  Period: December 2025 - January 2026")
print("=" * 110)

print(f"\n  AMAZON TRANSACTIONS MATCHED TO INVOICES: {len(matched_bank)}")
print(f"  {'Bank Date':<12} {'Amount':>10} {'Invoice Date':<22} {'Order #':<25} {'Items'}")
print(f"  {'-'*12} {'-'*10} {'-'*22} {'-'*25} {'-'*30}")
for m in matched_bank:
    items_str = "; ".join(m["inv_items"])[:50]
    print(f"  {m['bank_date']:<12} ${m['bank_amount']:>8.2f}  {m['inv_date']:<22} {m['inv_order']:<25} {items_str}")

print(f"\n{'='*110}")
print(f"  AMAZON TRANSACTIONS WITH NO MATCHING INVOICE (MISSING RECEIPTS): {len(unmatched_bank)}")
print(f"{'='*110}")
if unmatched_bank:
    total_missing = sum(t["amount"] for t in unmatched_bank)
    for t in unmatched_bank:
        print(f"  {t['date']:<12} ${t['amount']:>8.2f}  {t['desc']}")
    print(f"\n  TOTAL MISSING: ${total_missing:,.2f}")
else:
    print("  None! All Amazon transactions have matching invoices.")

print(f"\n{'='*110}")
print(f"  NON-AMAZON DEBITS (need separate receipts/categorization): {len(non_amazon_debits)}")
print(f"{'='*110}")
total_non_amazon = 0
for t in non_amazon_debits:
    total_non_amazon += t["amount"]
    # Check if we have a receipt
    has_receipt = False
    for inv in invoice_matches:
        if abs(inv["amount"] - t["amount"]) < 0.02:
            has_receipt = True
            break
    status = "HAS RECEIPT" if has_receipt else "NO RECEIPT"
    print(f"  {t['date']:<12} ${t['amount']:>8.2f}  {t['desc']:<50} [{status}]")
print(f"\n  TOTAL NON-AMAZON: ${total_non_amazon:,.2f}")

print(f"\n{'='*110}")
print(f"  INVOICES NOT ON THIS BANK STATEMENT: {len(unmatched_invoices)}")
print(f"  (These may be on the Discover card or other payment method)")
print(f"{'='*110}")
for inv in unmatched_invoices:
    date_str = inv["date"].strftime("%B %d, %Y") if inv["date"] else "?"
    items_str = "; ".join(inv["items"])[:60]
    print(f"  {date_str:<22} ${inv['amount']:>8.2f}  {inv['order_num']:<25} {items_str}")

# Summary
print(f"\n{'='*110}")
print(f"  SUMMARY")
print(f"{'='*110}")
deposits = sum(t["amount"] for t in bank_txns if t["type"] == "deposit")
all_debits = sum(t["amount"] for t in bank_txns if t["type"] == "debit")
amazon_debits = sum(t["amount"] for t in bank_txns if t["type"] == "debit" and "AMAZON" in t["desc"])
print(f"  Total deposits (Etsy payouts): ${deposits:,.2f}")
print(f"  Total debits: ${all_debits:,.2f}")
print(f"  Amazon debits: ${amazon_debits:,.2f} ({len([t for t in bank_txns if t['type']=='debit' and 'AMAZON' in t['desc']])} transactions)")
print(f"  Non-Amazon debits: ${total_non_amazon:,.2f} ({len(non_amazon_debits)} transactions)")
print(f"  Amazon matched to invoices: {len(matched_bank)} / {len(matched_bank) + len(unmatched_bank)}")
print(f"  Amazon MISSING receipts: {len(unmatched_bank)} (${sum(t['amount'] for t in unmatched_bank):,.2f})")
