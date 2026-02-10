"""Check raw PDF text for payment method info."""
import fitz, os

base = r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\data\invoices"

# Check a few business Amazon PDFs
keycomp = os.path.join(base, "keycomp")
files = sorted(os.listdir(keycomp))[:3]
for fname in files:
    path = os.path.join(keycomp, fname)
    doc = fitz.open(path)
    text = ""
    for page in doc:
        text += page.get_text()
    doc.close()
    # Look for payment-related lines
    for line in text.split("\n"):
        ll = line.lower()
        if any(w in ll for w in ["payment", "visa", "mastercard", "amex", "discover",
                                   "credit", "debit", "card ending", "pay"]):
            print(f"[{fname}] {line.strip()}")

print("\n--- Personal Amazon ---")
pa = os.path.join(base, "personal_amazon")
for fname in sorted(os.listdir(pa))[:3]:
    if not fname.endswith(".pdf"):
        continue
    path = os.path.join(pa, fname)
    doc = fitz.open(path)
    text = ""
    for page in doc:
        text += page.get_text()
    doc.close()
    for line in text.split("\n"):
        ll = line.lower()
        if any(w in ll for w in ["payment", "visa", "mastercard", "amex", "discover",
                                   "credit", "debit", "card ending", "pay"]):
            print(f"[{fname}] {line.strip()}")

print("\n--- Other Receipts ---")
other = os.path.join(base, "other_receipts")
for fname in sorted(os.listdir(other)):
    if not fname.endswith(".pdf") and not fname.endswith(".PDF"):
        continue
    path = os.path.join(other, fname)
    try:
        doc = fitz.open(path)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        for line in text.split("\n"):
            ll = line.lower()
            if any(w in ll for w in ["payment", "visa", "mastercard", "amex", "discover",
                                       "credit", "debit", "card ending", "pay"]):
                print(f"[{fname}] {line.strip()}")
    except Exception as e:
        print(f"[{fname}] Error: {e}")
