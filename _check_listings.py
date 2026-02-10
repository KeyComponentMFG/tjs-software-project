"""Break down listing fees vs actual products/sales."""
import csv, os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
statements_dir = os.path.join(BASE_DIR, "data", "etsy_statements")
files = [
    ("Oct 2025", "etsy_statement_2025_10.csv"),
    ("Nov 2025", "etsy_statement_2025_11.csv"),
    ("Dec 2025", "etsy_statement_2025_12.csv"),
    ("Jan 2026", "etsy_statement_2026_1.csv"),
    ("Feb 2026", "etsy_statement_2026_2.csv"),
]

def pm(s):
    s = s.strip()
    if s in ("--", ""): return 0.0
    neg = -1 if s.startswith("-") else 1
    return neg * float(s.lstrip("-").replace("$","").replace(",",""))

listing_fees_by_month = defaultdict(lambda: {"count": 0, "total": 0.0})
listing_fee_amounts = defaultdict(int)  # amount -> count
sales_by_month = defaultdict(int)
unique_products = set()
sale_count = 0

for label, fname in files:
    fpath = os.path.join(statements_dir, fname)
    if not os.path.exists(fpath): continue
    with open(fpath, "r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            typ = r.get("Type","").strip()
            title = r.get("Title","").strip()
            net = pm(r.get("Net","0"))

            if typ == "Fee" and "listing fee" in title.lower():
                listing_fees_by_month[label]["count"] += 1
                listing_fees_by_month[label]["total"] += net
                listing_fee_amounts[net] += 1

            if typ == "Fee" and title.startswith("Credit for listing fee"):
                listing_fees_by_month[label]["count"] += 1
                listing_fees_by_month[label]["total"] += net

            if typ == "Sale":
                sales_by_month[label] += 1
                sale_count += 1
                # Extract product name (before the " - " variation part)
                product = title.split(" - ")[0].strip() if " - " in title else title
                unique_products.add(product)

print("=" * 60)
print("  LISTING FEE vs SALES ANALYSIS")
print("=" * 60)

print("\nLISTING FEES BY MONTH:")
total_fees = 0
total_count = 0
for label, _ in files:
    data = listing_fees_by_month.get(label, {"count": 0, "total": 0.0})
    sales = sales_by_month.get(label, 0)
    print("  %-10s  %3d charges  $%7s  |  %3d sales" % (
        label, data["count"], "{:,.2f}".format(data["total"]), sales))
    total_fees += data["total"]
    total_count += data["count"]
print("  %-10s  %3d charges  $%7s  |  %3d sales" % (
    "TOTAL", total_count, "{:,.2f}".format(total_fees), sale_count))

print("\nLISTING FEE AMOUNTS (how much each charge was):")
for amt, cnt in sorted(listing_fee_amounts.items()):
    listings = abs(amt) / 0.20
    print("  $%s  x %3d charges  (= %d listings each)" % (
        "{:,.2f}".format(amt), cnt, int(listings)))

print("\nUNIQUE PRODUCTS SOLD: %d" % len(unique_products))
for p in sorted(unique_products):
    print("  - %s" % p[:60])

print("\nSUMMARY:")
total_listing_count = int(abs(total_fees) / 0.20)
print("  Total listing fee charges in CSVs: $%s" % "{:,.2f}".format(abs(total_fees)))
print("  At $0.20 each = ~%d listings" % total_listing_count)
print("  Total sales: %d (each sale auto-renews = $0.20)" % sale_count)
print("  Unique products: %d" % len(unique_products))
print("  Auto-renewals from sales: ~%d" % sale_count)
print("  Remaining (new listings + periodic renewals): ~%d" % (total_listing_count - sale_count))
