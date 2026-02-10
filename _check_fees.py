"""Check listing fees and all fee types from Etsy CSVs."""
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

fee_breakdown = defaultdict(float)
fee_count = defaultdict(int)
listing_fees = []
all_types = defaultdict(float)

for label, fname in files:
    fpath = os.path.join(statements_dir, fname)
    if not os.path.exists(fpath): continue
    with open(fpath, "r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            typ = r.get("Type","").strip()
            title = r.get("Title","").strip()
            net = pm(r.get("Net","0"))
            all_types[typ] += net
            if typ == "Fee":
                fee_breakdown[title] += net
                fee_count[title] += 1
                if "listing" in title.lower():
                    listing_fees.append((label, r.get("Date",""), title, net))

print("=" * 60)
print("  FEE ANALYSIS")
print("=" * 60)

print("\nALL TRANSACTION TYPES:")
for typ, total in sorted(all_types.items(), key=lambda x: x[1]):
    print("  %-20s  $%10s" % (typ, "{:,.2f}".format(total)))

print("\n\nALL FEE SUBTYPES (by Title):")
for title, total in sorted(fee_breakdown.items(), key=lambda x: x[1]):
    cnt = fee_count[title]
    print("  %-50s  %4dx  $%10s" % (title[:50], cnt, "{:,.2f}".format(total)))
print("  TOTAL FEES: $%s" % "{:,.2f}".format(sum(fee_breakdown.values())))

print("\n\nLISTING FEES SPECIFICALLY: %d entries" % len(listing_fees))
listing_total = sum(x[3] for x in listing_fees)
print("  Total listing fees: $%s" % "{:,.2f}".format(listing_total))
for label, date, title, net in listing_fees[:30]:
    print("  %-10s %-20s %-40s $%s" % (label, date, title[:40], "{:,.2f}".format(net)))
if len(listing_fees) > 30:
    print("  ... and %d more" % (len(listing_fees) - 30))
