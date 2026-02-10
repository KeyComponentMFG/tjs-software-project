"""Count exact sales from Etsy CSVs."""
import csv, os, re
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

sales_by_month = defaultdict(int)
order_numbers = set()
all_sale_rows = []
all_types = defaultdict(int)

for label, fname in files:
    fpath = os.path.join(statements_dir, fname)
    if not os.path.exists(fpath): continue
    with open(fpath, "r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            typ = r.get("Type", "").strip()
            title = r.get("Title", "").strip()
            all_types[typ] += 1

            if typ == "Sale":
                sales_by_month[label] += 1
                # Extract order number
                m = re.search(r'#(\d+)', title)
                if m:
                    order_numbers.add(m.group(1))
                all_sale_rows.append((label, r.get("Date",""), title))

print("TRANSACTION TYPE COUNTS:")
for typ, cnt in sorted(all_types.items()):
    print("  %-15s  %d" % (typ, cnt))

print("\nSALES BY MONTH:")
total = 0
for label, _ in files:
    cnt = sales_by_month.get(label, 0)
    total += cnt
    print("  %-10s  %d sales" % (label, cnt))
print("  TOTAL:      %d sale rows" % total)

print("\nUNIQUE ORDER NUMBERS: %d" % len(order_numbers))

# Check if there are quantity > 1 orders
print("\nFIRST 10 SALE TITLES:")
for label, date, title in all_sale_rows[:10]:
    print("  %s  %s  %s" % (label, date, title[:60]))

# Check for multi-quantity in Info column
print("\nCHECKING FOR MULTI-QUANTITY...")
for label, fname in files:
    fpath = os.path.join(statements_dir, fname)
    if not os.path.exists(fpath): continue
    with open(fpath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        print("  CSV columns: %s" % ", ".join(cols))
        break
