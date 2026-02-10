"""Compute current Etsy balance, owner draw breakdown, and full ledger."""
import csv, os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
statements_dir = os.path.join(BASE_DIR, "data", "etsy_statements")

all_files = [
    ("Oct 2025", os.path.join(statements_dir, "etsy_statement_2025_10.csv")),
    ("Nov 2025", os.path.join(statements_dir, "etsy_statement_2025_11.csv")),
    ("Dec 2025", os.path.join(statements_dir, "etsy_statement_2025_12.csv")),
    ("Jan 2026", os.path.join(statements_dir, "etsy_statement_2026_1.csv")),
    ("Feb 2026", os.path.join(statements_dir, "etsy_statement_2026_2.csv")),
]

def parse_money(s):
    s = s.strip()
    if s == "--" or s == "":
        return 0.0
    neg = 1
    if s.startswith("-"):
        neg = -1
        s = s[1:]
    s = s.replace("$", "").replace(",", "")
    return neg * float(s)

print("=" * 70)
print("  COMPLETE FINANCIAL ANALYSIS")
print("  Every penny: Oct 2025 - Feb 8, 2026")
print("=" * 70)

# =====================================================================
# PART 1: ETSY ACCOUNT BALANCE
# =====================================================================
print("\n" + "=" * 70)
print("  PART 1: ETSY ACCOUNT BALANCE")
print("=" * 70)

grand_by_type = defaultdict(float)
grand_net = 0.0

for label, fpath in all_files:
    if not os.path.exists(fpath):
        continue

    with open(fpath, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    by_type = defaultdict(float)
    month_net = 0.0

    for r in rows:
        typ = r.get("Type", "").strip()
        net_val = parse_money(r.get("Net", "0"))
        by_type[typ] += net_val
        month_net += net_val

    print(f"\n  {label} ({len(rows)} transactions)")
    print(f"  {'-' * 55}")
    for typ in ["Sale", "Shipping", "Tax", "Fee", "Buyer Fee", "Marketing", "Refund", "Deposit"]:
        if typ in by_type:
            print(f"    {typ:<15s}  ${by_type[typ]:>10,.2f}")
            grand_by_type[typ] += by_type[typ]
    print(f"    {'MONTH NET':<15s}  ${month_net:>10,.2f}")
    grand_net += month_net

print(f"\n  {'=' * 55}")
print(f"  ETSY ALL-TIME TOTALS:")
for typ in ["Sale", "Shipping", "Tax", "Fee", "Buyer Fee", "Marketing", "Refund", "Deposit"]:
    if typ in grand_by_type:
        print(f"    {typ:<15s}  ${grand_by_type[typ]:>10,.2f}")
print(f"    {'-' * 40}")
print(f"    {'NET BALANCE':<15s}  ${grand_net:>10,.2f}")
print(f"\n  >>> YOUR CURRENT ETSY BALANCE: ${grand_net:,.2f}")
print(f"  >>> (This is money Etsy is holding for your next payout)")

# =====================================================================
# PART 2: OWNER DRAW BREAKDOWN
# =====================================================================
print(f"\n{'=' * 70}")
print(f"  PART 2: OWNER DRAW BREAKDOWN")
print(f"{'=' * 70}")

owner_draws = [
    ("01/06/2026", "VENMO PRICE PETTIT", 36.00, "Tulsa", "Payment to Price Pettit"),
    ("01/06/2026", "VENMO JACOB SHELLEY", 57.00, "Tulsa", "Payment to Jacob Shelley"),
    ("01/23/2026", "VENMO BRADEN WALKER", 450.00, "Texas", "CEO draw to Braden Walker"),
    ("01/26/2026", "VENMO BRADEN WALKER", 100.00, "Texas", "CEO draw to Braden Walker"),
    ("02/02/2026", "VENMO TJ MCNULTY", 350.00, "Tulsa", "CEO draw to TJ McNulty"),
]

texas_draws = [d for d in owner_draws if d[3] == "Texas"]
tulsa_draws = [d for d in owner_draws if d[3] == "Tulsa"]
texas_total = sum(d[2] for d in texas_draws)
tulsa_total = sum(d[2] for d in tulsa_draws)
total_draw = texas_total + tulsa_total

print(f"\n  Owner Draw - Texas (Braden Walker):")
for d in texas_draws:
    print(f"    {d[0]}  {d[1]:<30s}  ${d[2]:>8,.2f}  ({d[4]})")
print(f"    {'':30s}  SUBTOTAL:  ${texas_total:>8,.2f}")

print(f"\n  Owner Draw - Tulsa:")
for d in tulsa_draws:
    print(f"    {d[0]}  {d[1]:<30s}  ${d[2]:>8,.2f}  ({d[4]})")
print(f"    {'':30s}  SUBTOTAL:  ${tulsa_total:>8,.2f}")

print(f"\n  TOTAL OWNER DRAW:                    ${total_draw:>8,.2f}")
print(f"    Texas share: ${texas_total:,.2f} ({texas_total/total_draw*100:.0f}%)")
print(f"    Tulsa share: ${tulsa_total:,.2f} ({tulsa_total/total_draw*100:.0f}%)")

# =====================================================================
# PART 3: FULL BANK LEDGER - EVERY TRANSACTION
# =====================================================================
print(f"\n{'=' * 70}")
print(f"  PART 3: FULL BANK LEDGER (Capital One 3650)")
print(f"  Every transaction, running balance")
print(f"{'=' * 70}")

BANK_TXNS = [
    {"date":"12/10/2025","desc":"ETSY PAYOUT","amount":1287.26,"type":"deposit","cat":"Etsy Payout"},
    {"date":"12/16/2025","desc":"ETSY PAYOUT","amount":228.62,"type":"deposit","cat":"Etsy Payout"},
    {"date":"12/23/2025","desc":"ETSY PAYOUT","amount":633.34,"type":"deposit","cat":"Etsy Payout"},
    {"date":"12/30/2025","desc":"ETSY PAYOUT","amount":328.38,"type":"deposit","cat":"Etsy Payout"},
    {"date":"12/12/2025","desc":"AMAZON MKTPL YJ01H91J3","amount":44.16,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/12/2025","desc":"AMAZON MKTPL XO7VT5L53","amount":51.68,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/15/2025","desc":"UPS STORE 1849 TULSA OK","amount":16.39,"type":"debit","cat":"Shipping"},
    {"date":"12/15/2025","desc":"USPS CLICKNSHIP","amount":4.92,"type":"debit","cat":"Shipping"},
    {"date":"12/15/2025","desc":"AMAZON MKTPL E409Z4AL3","amount":18.44,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/15/2025","desc":"AMAZON MKTPL 2V1YI7X13","amount":35.80,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/15/2025","desc":"AMAZON MKTPL 5P9CF5KZ3","amount":196.40,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/16/2025","desc":"WESTLAKE HARDWARE 088","amount":41.00,"type":"debit","cat":"Pending"},
    {"date":"12/17/2025","desc":"HOBBYLOBBY TULSA OK","amount":7.57,"type":"debit","cat":"Craft Supplies"},
    {"date":"12/17/2025","desc":"ETSY COM US (ads/fees)","amount":29.00,"type":"debit","cat":"Etsy Fees"},
    {"date":"12/17/2025","desc":"PAYPAL ALIPAYUSINC","amount":76.57,"type":"debit","cat":"AliExpress Supplies"},
    {"date":"12/19/2025","desc":"AMAZON MKTPL OR4FP0NE3","amount":37.97,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/19/2025","desc":"AMAZON MKTPL NF2IJ0HX3","amount":67.73,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/22/2025","desc":"AMAZON MKTPL 5J8JP26T3","amount":17.31,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/22/2025","desc":"AMAZON MKTPL TW2K20SU3","amount":49.91,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/23/2025","desc":"AMAZON MKTPL C72SV1CY3","amount":54.56,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/24/2025","desc":"REASORS 25 TULSA OK","amount":50.00,"type":"debit","cat":"Personal"},
    {"date":"12/24/2025","desc":"WILDFLOWERCAFE TULSA OK","amount":26.09,"type":"debit","cat":"Personal"},
    {"date":"12/24/2025","desc":"ANTHROPOLOGIE 00546","amount":30.38,"type":"debit","cat":"Personal"},
    {"date":"12/26/2025","desc":"LULULEMON CENTER 1","amount":50.00,"type":"debit","cat":"Personal"},
    {"date":"12/29/2025","desc":"PAYPAL THANGS 3D","amount":20.15,"type":"debit","cat":"3D Subscription"},
    {"date":"12/29/2025","desc":"AMAZON MKTPL HU1NU1E73","amount":30.90,"type":"debit","cat":"Amazon Inventory"},
    {"date":"12/29/2025","desc":"AMAZON MKTPL HZ3VL8FR3","amount":59.67,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/06/2026","desc":"ETSY PAYOUT","amount":497.22,"type":"deposit","cat":"Etsy Payout"},
    {"date":"01/13/2026","desc":"ETSY PAYOUT","amount":759.81,"type":"deposit","cat":"Etsy Payout"},
    {"date":"01/21/2026","desc":"ETSY PAYOUT","amount":1176.18,"type":"deposit","cat":"Etsy Payout"},
    {"date":"01/27/2026","desc":"ETSY PAYOUT","amount":590.46,"type":"deposit","cat":"Etsy Payout"},
    {"date":"01/02/2026","desc":"ETSY COM US (ads/fees)","amount":10.00,"type":"debit","cat":"Etsy Fees"},
    {"date":"01/05/2026","desc":"HOBBYLOBBY TULSA OK","amount":11.36,"type":"debit","cat":"Craft Supplies"},
    {"date":"01/05/2026","desc":"WAL MART 0992 TULSA OK","amount":15.27,"type":"debit","cat":"Pending"},
    {"date":"01/06/2026","desc":"AMAZON MKTPL KB6AD5XB3","amount":15.97,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/06/2026","desc":"AMAZON MKTPL ZU68D7WB3","amount":16.01,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/06/2026","desc":"AMAZON MKTPL AZ6BK1DB3","amount":20.56,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/06/2026","desc":"AMAZON MKTPL Q51HU05A3","amount":35.04,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/06/2026","desc":"VENMO PRICE PETTIT","amount":36.00,"type":"debit","cat":"Owner Draw - Tulsa"},
    {"date":"01/06/2026","desc":"AMAZON MKTPL 8H1DG3DA3","amount":37.20,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/06/2026","desc":"AMAZON MKTPL 287II1HF3","amount":48.69,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/06/2026","desc":"AMAZON MKTPL 158CW8QL3","amount":48.81,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/06/2026","desc":"VENMO JACOB SHELLEY","amount":57.00,"type":"debit","cat":"Owner Draw - Tulsa"},
    {"date":"01/06/2026","desc":"AMAZON MKTPL 9G6FP5GA3","amount":71.15,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/07/2026","desc":"AMAZON MKTPL R12KW4BP3","amount":34.96,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/07/2026","desc":"AMAZON MKTPL AY55313W3","amount":100.00,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/08/2026","desc":"AMAZON MKTPL WQ6J56WE3","amount":20.61,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/08/2026","desc":"AMAZON MKTPL TK7525Y53","amount":116.87,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/09/2026","desc":"AMAZON MKTPL OG8499T63","amount":34.92,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/12/2026","desc":"AMAZON MKTPL 993EC6V63","amount":23.04,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/12/2026","desc":"AMAZON MKTPL OM7OI60L3","amount":29.19,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/12/2026","desc":"AMAZON MKTPL U37PZ3L83","amount":30.51,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/12/2026","desc":"AMAZON MKTPL GS5T14M53","amount":30.59,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/12/2026","desc":"AMAZON MKTPL 5P2Z17GU3","amount":49.78,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/12/2026","desc":"AMAZON MKTPL W607J8553","amount":54.85,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/13/2026","desc":"AMAZON MKTPL L781I8BJ3","amount":26.90,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/14/2026","desc":"AMAZON MKTPL 808DT2723","amount":125.80,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/15/2026","desc":"BEST BUY AUTO PYMT","amount":100.00,"type":"debit","cat":"Personal"},
    {"date":"01/16/2026","desc":"ETSY COM US (ads/fees)","amount":29.00,"type":"debit","cat":"Etsy Fees"},
    {"date":"01/20/2026","desc":"AMAZON MKTPL E11RB5RU3","amount":55.93,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/21/2026","desc":"AMAZON MKTPL BQ8BF1YM1","amount":17.53,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/21/2026","desc":"CHIPOTLE 2155 TULSA OK","amount":20.08,"type":"debit","cat":"Personal"},
    {"date":"01/21/2026","desc":"AMAZON MKTPL YN46J1BJ3","amount":91.83,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/22/2026","desc":"AMAZON MKTPL I77IO2DE3","amount":30.84,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/23/2026","desc":"AMAZON MKTPL N33CL1IK3","amount":18.39,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/23/2026","desc":"AMAZON MKTPL 2X7C91G13","amount":18.44,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/23/2026","desc":"AMAZON MKTPL 2V1EC6XB3","amount":53.88,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/23/2026","desc":"VENMO BRADEN WALKER","amount":450.00,"type":"debit","cat":"Owner Draw - Texas"},
    {"date":"01/26/2026","desc":"PAYPAL AOWEIKEGTTA","amount":25.98,"type":"debit","cat":"AliExpress Supplies"},
    {"date":"01/26/2026","desc":"PAYPAL AOWEIKEGTTA","amount":39.56,"type":"debit","cat":"AliExpress Supplies"},
    {"date":"01/26/2026","desc":"VENMO BRADEN WALKER","amount":100.00,"type":"debit","cat":"Owner Draw - Texas"},
    {"date":"01/28/2026","desc":"AMAZON MKTPL WM9RC81X3","amount":16.01,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/28/2026","desc":"AMAZON MKTPL Q61PB33T3","amount":49.91,"type":"debit","cat":"Amazon Inventory"},
    {"date":"01/29/2026","desc":"PAYPAL THANGS 3D","amount":20.15,"type":"debit","cat":"3D Subscription"},
    {"date":"01/29/2026","desc":"AMAZON MKTPL SJ3R644D3","amount":68.10,"type":"debit","cat":"Amazon Inventory"},
    {"date":"02/02/2026","desc":"ETSY COM US (fees)","amount":13.98,"type":"debit","cat":"Etsy Fees"},
    {"date":"02/02/2026","desc":"ETSY COM US (fees)","amount":10.00,"type":"debit","cat":"Etsy Fees"},
    {"date":"02/02/2026","desc":"AMAZON MKTPL (split)","amount":16.44,"type":"debit","cat":"Amazon Inventory"},
    {"date":"02/02/2026","desc":"AMAZON MKTPL (split)","amount":29.21,"type":"debit","cat":"Amazon Inventory"},
    {"date":"02/02/2026","desc":"AMAZON MKTPL (split)","amount":31.46,"type":"debit","cat":"Amazon Inventory"},
    {"date":"02/02/2026","desc":"AMAZON MKTPL (split)","amount":36.84,"type":"debit","cat":"Amazon Inventory"},
    {"date":"02/02/2026","desc":"VENMO TJ MCNULTY","amount":350.00,"type":"debit","cat":"Owner Draw - Tulsa"},
    {"date":"02/03/2026","desc":"ETSY PAYOUT","amount":483.28,"type":"deposit","cat":"Etsy Payout"},
    {"date":"02/06/2026","desc":"AMAZON MKTPL (pending)","amount":17.25,"type":"debit","cat":"Amazon Inventory"},
    {"date":"02/06/2026","desc":"AMAZON MKTPL (pending)","amount":23.79,"type":"debit","cat":"Amazon Inventory"},
    {"date":"02/06/2026","desc":"AMAZON MKTPL (pending)","amount":23.86,"type":"debit","cat":"Amazon Inventory"},
]

print(f"\n  {'DATE':<12s}  {'DESCRIPTION':<30s}  {'DEBIT':>9s}  {'CREDIT':>9s}  {'BALANCE':>10s}  {'CATEGORY'}")
print(f"  {'-'*12}  {'-'*30}  {'-'*9}  {'-'*9}  {'-'*10}  {'-'*20}")

balance = 0.0
for t in sorted(BANK_TXNS, key=lambda x: x["date"]):
    if t["type"] == "deposit":
        balance += t["amount"]
        print(f"  {t['date']:<12s}  {t['desc'][:30]:<30s}  {'':>9s}  ${t['amount']:>8,.2f}  ${balance:>9,.2f}  {t['cat']}")
    else:
        balance -= t["amount"]
        print(f"  {t['date']:<12s}  {t['desc'][:30]:<30s}  ${t['amount']:>8,.2f}  {'':>9s}  ${balance:>9,.2f}  {t['cat']}")

print(f"\n  ENDING BALANCE: ${balance:>9,.2f}")

# =====================================================================
# PART 4: WHERE EVERY DOLLAR WENT
# =====================================================================
print(f"\n{'=' * 70}")
print(f"  PART 4: WHERE EVERY DOLLAR WENT")
print(f"{'=' * 70}")

cats = defaultdict(float)
for t in BANK_TXNS:
    if t["type"] == "debit":
        cats[t["cat"]] += t["amount"]

total_out = sum(cats.values())
total_in = sum(t["amount"] for t in BANK_TXNS if t["type"] == "deposit")

print(f"\n  MONEY IN:")
print(f"    Etsy Payouts to Bank:                ${total_in:>9,.2f}")

print(f"\n  MONEY OUT:")
for cat in sorted(cats.keys()):
    pct = cats[cat] / total_out * 100
    print(f"    {cat:<35s}  ${cats[cat]:>9,.2f}  ({pct:4.1f}%)")
print(f"    {'-'*50}")
print(f"    {'TOTAL SPENT':<35s}  ${total_out:>9,.2f}")

print(f"\n  REMAINING IN BANK:                     ${total_in - total_out:>9,.2f}")
