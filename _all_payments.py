"""Scan ALL PDFs for payment methods."""
import fitz, os, re
from collections import Counter

base = r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\data\invoices"
payments = Counter()
details = []

for subfolder in ["keycomp", "personal_amazon"]:
    folder = os.path.join(base, subfolder)
    for fname in sorted(os.listdir(folder)):
        if not fname.endswith(".pdf"):
            continue
        path = os.path.join(folder, fname)
        doc = fitz.open(path)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()

        # Look for card info
        found = None
        for line in text.split("\n"):
            # Match patterns like "Discover | Last digits: 4570" or "Visa ending in 5683"
            m = re.search(r'(Visa|Mastercard|Discover|Amex|American Express)\s*\|?\s*(?:Last digits:|ending in)\s*(\d+)', line, re.IGNORECASE)
            if m:
                card_type = m.group(1)
                last4 = m.group(2)
                found = f"{card_type} ending in {last4}"
                break
        if found:
            payments[found] += 1
            details.append((subfolder, fname, found))
        else:
            payments["Unknown (no card info)"] += 1

# Paper receipts - manual
paper_payments = {
    "Hobby Lobby x3": "Cash/Unknown (scanned receipt)",
    "Home Depot x1": "Cash/Unknown (scanned receipt)",
    "OK Secretary of State x1": "Credit Card (surcharge noted)",
}

print("=" * 70)
print("  PAYMENT METHODS ACROSS ALL INVOICES")
print("=" * 70)
print(f"\n  Total orders with PDFs: {sum(payments.values())}")
print()
for method, count in payments.most_common():
    print(f"  {method}: {count} orders")

print(f"\n  Paper/scanned receipts (no card info in PDF):")
for desc, method in paper_payments.items():
    print(f"    {desc}: {method}")

print(f"\n{'='*70}")
print(f"  SUMMARY")
print(f"{'='*70}")
# Group by card
card_groups = Counter()
for method, count in payments.items():
    card_groups[method] += count
for method, count in card_groups.most_common():
    print(f"  {method}: {count} orders")
