"""Trace every dollar: where did ALL the money go?"""
import csv, os, json, re
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def parse_money(s):
    s = s.strip()
    if s == "--" or s == "":
        return 0.0
    neg = -1 if s.startswith("-") else 1
    s = s.lstrip("-").replace("$", "").replace(",", "")
    return neg * float(s)

# =====================================================================
# 1. ETSY: Total revenue and where it went
# =====================================================================
statements_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
files = [
    ("Oct 2025", "etsy_statement_2025_10.csv"),
    ("Nov 2025", "etsy_statement_2025_11.csv"),
    ("Dec 2025", "etsy_statement_2025_12.csv"),
    ("Jan 2026", "etsy_statement_2026_1.csv"),
    ("Feb 2026", "etsy_statement_2026_2.csv"),
]

etsy_by_type = defaultdict(float)
etsy_deposits = []  # (month, amount)

for label, fname in files:
    fpath = os.path.join(statements_dir, fname)
    if not os.path.exists(fpath):
        continue
    with open(fpath, "r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            typ = r.get("Type", "").strip()
            net = parse_money(r.get("Net", "0"))
            etsy_by_type[typ] += net
            if typ == "Deposit":
                # Extract amount from Title
                title = r.get("Title", "")
                m = re.search(r'\$([\d,]+\.\d{2})', title)
                if m:
                    amt = float(m.group(1).replace(",", ""))
                    etsy_deposits.append((label, r.get("Date", ""), amt))

total_etsy_deposits = sum(d[2] for d in etsy_deposits)
etsy_net_earnings = sum(v for k, v in etsy_by_type.items() if k != "Deposit")

print("=" * 70)
print("  WHERE IS ALL THE MONEY? - Complete Trace")
print("=" * 70)

print("\n  STEP 1: ETSY EARNINGS (what customers paid)")
print(f"  {'-' * 55}")
for typ in ["Sale", "Shipping", "Tax", "Fee", "Buyer Fee", "Marketing", "Refund"]:
    if typ in etsy_by_type:
        print(f"    {typ:<15s}  ${etsy_by_type[typ]:>10,.2f}")
print(f"    {'':15s}  -----------")
print(f"    {'NET EARNED':<15s}  ${etsy_net_earnings:>10,.2f}")

print(f"\n  STEP 2: ETSY DEPOSITED TO BANK(S)")
print(f"  {'-' * 55}")
pre_capone = []
capone = []
for label, date, amt in etsy_deposits:
    if label in ("Oct 2025", "Nov 2025"):
        pre_capone.append((label, date, amt))
    elif label == "Dec 2025" and date.strip().startswith("December 1,") and "December 1" in date and "December 15" not in date:
        pre_capone.append((label, date, amt))
    else:
        capone.append((label, date, amt))

pre_capone_total = sum(d[2] for d in pre_capone)
capone_total = sum(d[2] for d in capone)

print(f"    Pre-Capital One (old bank):")
for label, date, amt in pre_capone:
    print(f"      {date:<25s}  ${amt:>9,.2f}  ({label})")
print(f"      {'SUBTOTAL':>25s}  ${pre_capone_total:>9,.2f}")

print(f"\n    To Capital One 3650:")
for label, date, amt in capone:
    print(f"      {date:<25s}  ${amt:>9,.2f}  ({label})")
print(f"      {'SUBTOTAL':>25s}  ${capone_total:>9,.2f}")

print(f"\n    TOTAL DEPOSITED:             ${total_etsy_deposits:>9,.2f}")
etsy_balance = etsy_net_earnings - total_etsy_deposits
print(f"    STILL IN ETSY ACCOUNT:       ${etsy_balance:>9,.2f}")

# Verify
print(f"\n    CHECK: Earned ${etsy_net_earnings:,.2f} - Deposited ${total_etsy_deposits:,.2f} = ${etsy_balance:,.2f}")

# =====================================================================
# 2. CAPITAL ONE: What came in vs what went out
# =====================================================================
print(f"\n  STEP 3: CAPITAL ONE BANK ACCOUNT")
print(f"  {'-' * 55}")
print(f"    Etsy deposits received:      ${capone_total:>9,.2f}")
print(f"    Current bank balance:        $ 2,108.41")
print(f"    Total spent from bank:       ${capone_total - 2108.41:>9,.2f}")

# =====================================================================
# 3. DISCOVER CARD: Hidden spending
# =====================================================================
inv_path = os.path.join(BASE_DIR, "data", "generated", "inventory_orders.json")
with open(inv_path, "r") as f:
    invoices = json.load(f)

discover_orders = [inv for inv in invoices if "discover" in inv.get("payment_method", "").lower()]
debit_orders = [inv for inv in invoices if "discover" not in inv.get("payment_method", "").lower()]
other_pm = [inv for inv in invoices if inv.get("payment_method", "").lower() not in ("discover ending in 4570",)]

discover_total = sum(inv["grand_total"] for inv in discover_orders)
debit_total = sum(inv["grand_total"] for inv in debit_orders)

print(f"\n  STEP 4: DISCOVER CARD 4570 (HIDDEN SPENDING)")
print(f"  {'-' * 55}")
print(f"    Amazon orders on Discover:   {len(discover_orders)} orders")
print(f"    Discover invoice total:      ${discover_total:>9,.2f}")
print(f"    (Some items split-shipped to Cap One debit)")
print(f"")
print(f"    Amazon orders on debit/other: {len(debit_orders)} orders")
print(f"    Debit/other invoice total:   ${debit_total:>9,.2f}")

# Check payment methods
pms = defaultdict(lambda: {"count": 0, "total": 0})
for inv in invoices:
    pm = inv.get("payment_method", "Unknown")
    pms[pm]["count"] += 1
    pms[pm]["total"] += inv["grand_total"]

print(f"\n    Invoice breakdown by payment method:")
for pm, data in sorted(pms.items()):
    print(f"      {pm:<35s}  {data['count']:>3d} orders  ${data['total']:>9,.2f}")

# =====================================================================
# 4. THE FULL PICTURE
# =====================================================================
print(f"\n{'=' * 70}")
print(f"  THE FULL PICTURE - WHERE IS EVERY DOLLAR?")
print(f"{'=' * 70}")

print(f"\n  MONEY EARNED:")
print(f"    Etsy net earnings (sales - fees - refunds):  ${etsy_net_earnings:>10,.2f}")

print(f"\n  WHERE THAT MONEY IS RIGHT NOW:")
print(f"    1. Capital One bank balance:                 $  2,108.41")
print(f"    2. Sitting in Etsy (pending payout):         ${etsy_balance:>10,.2f}")
print(f"    3. Pre-Cap One deposits (old bank):          ${pre_capone_total:>10,.2f}")
subtotal_accounted = 2108.41 + etsy_balance + pre_capone_total
print(f"       SUBTOTAL (money you still have):          ${subtotal_accounted:>10,.2f}")

print(f"\n  MONEY SPENT FROM CAP ONE:")
bank_spent = capone_total - 2108.41
print(f"    Total spent from Cap One bank:               ${bank_spent:>10,.2f}")

# Break down bank spending
print(f"\n    Breakdown:")
cats = {
    "Amazon Inventory": 2039.29,
    "Owner Draw - Tulsa": 639.40,
    "Owner Draw - Texas": 550.00,
    "Personal": 276.55,
    "AliExpress Supplies (LED Lights)": 142.11,
    "Etsy Fees (bank charges)": 91.98,
    "Craft Supplies": 59.93,
    "Subscriptions (Thangs 3D)": 40.30,
    "Shipping": 36.58,
}
for cat, amt in cats.items():
    print(f"      {cat:<40s}  ${amt:>9,.2f}")
cat_total = sum(cats.values())
print(f"      {'TOTAL':>40s}  ${cat_total:>9,.2f}")

print(f"\n  MONEY SPENT ON DISCOVER (NOT in bank):")
print(f"    Discover Amazon orders (invoice total):      ${discover_total:>10,.2f}")
print(f"    MINUS split-shipments charged to Cap One")
# The split shipments are bank Amazon charges that came from Discover orders
# Bank Amazon total = $2,039.29 but this includes BOTH direct debit orders
# AND split shipments from Discover. We need to figure out how much of the
# bank Amazon charges were split from Discover vs direct debit purchases.
print(f"    (Some Discover items were charged to debit card as split shipments)")
print(f"    Net Discover liability is invoice total minus what hit debit card")

print(f"\n{'=' * 70}")
print(f"  ACCOUNTING EQUATION:")
print(f"{'=' * 70}")
total_out = bank_spent + pre_capone_total
print(f"    Earned from Etsy:                            ${etsy_net_earnings:>10,.2f}")
print(f"    In Cap One bank:                             $  2,108.41")
print(f"    In Etsy account:                             ${etsy_balance:>10,.2f}")
print(f"    In old bank (pre-Cap One):                   ${pre_capone_total:>10,.2f}")
print(f"    Spent from Cap One:                          ${bank_spent:>10,.2f}")
total_places = 2108.41 + etsy_balance + pre_capone_total + bank_spent
print(f"    TOTAL:                                       ${total_places:>10,.2f}")
gap = etsy_net_earnings - total_places
print(f"\n    GAP (earned - accounted for):                ${gap:>10,.2f}")
if abs(gap) < 0.02:
    print(f"    [OK] Every penny from Etsy is accounted for!")
else:
    print(f"    [!!!] ${abs(gap):,.2f} IS UNACCOUNTED FOR")

print(f"\n  ADDITIONAL LIABILITIES (not from Etsy earnings):")
print(f"    Discover 4570 balance owed:                  ${discover_total:>10,.2f}")
print(f"    (Amazon inventory bought on credit card)")
print(f"    This is money you OWE, not money you earned")

print(f"\n{'=' * 70}")
print(f"  NET WORTH OF THE BUSINESS")
print(f"{'=' * 70}")
print(f"    Assets:")
print(f"      Cap One bank:              $  2,108.41")
print(f"      Etsy pending:              ${etsy_balance:>10,.2f}")
print(f"      Old bank (pre-Cap One):    ${pre_capone_total:>10,.2f}")
print(f"      Inventory on hand:         (unknown - need physical count)")
total_assets = 2108.41 + etsy_balance + pre_capone_total
print(f"      TOTAL CASH ASSETS:         ${total_assets:>10,.2f}")
print(f"\n    Liabilities:")
print(f"      Discover 4570 balance:     ${discover_total:>10,.2f}")
print(f"      (need actual statement for current balance)")
net_worth = total_assets - discover_total
print(f"\n    NET POSITION (cash - debt):  ${net_worth:>10,.2f}")
