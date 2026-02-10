"""Show all orders paid with Visa ending in 6019."""
import fitz, os, re, json

base = r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\data\invoices"
with open(r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\data\generated\inventory_orders.json") as f:
    orders = json.load(f)

# Build a map of filename -> order data
order_map = {}
for o in orders:
    order_map[o["file"]] = o

# Scan all PDFs for Visa 6019
visa6019_orders = []
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
        if "6019" in text and "Visa" in text:
            m = re.search(r'Visa.*?6019', text)
            if m:
                order = order_map.get(fname)
                if order:
                    visa6019_orders.append(order)

# Sort by date
total = 0
print("=" * 100)
print("  ALL ORDERS PAID WITH VISA ENDING IN 6019")
print("=" * 100)
for o in visa6019_orders:
    print(f"\n  Date: {o['date']}")
    print(f"  Order: {o.get('order_num', 'N/A')}")
    print(f"  Total: ${o['grand_total']:.2f}  (subtotal: ${o['subtotal']:.2f}, tax: ${o['tax']:.2f})")
    print(f"  Shipped to: {o.get('ship_address', 'N/A')}")
    total += o["grand_total"]
    for item in o["items"]:
        print(f"    - [{item['qty']}x] ${item['price']:.2f}  {item['name'][:90]}")

print(f"\n{'='*100}")
print(f"  TOTAL ON VISA 6019: ${total:,.2f} across {len(visa6019_orders)} orders")
print(f"{'='*100}")
