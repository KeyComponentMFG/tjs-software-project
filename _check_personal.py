"""Check which items are personal vs business and find uncategorized items."""
import json

with open(r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\data\generated\inventory_orders.json") as f:
    orders = json.load(f)

def categorize_item(name):
    n = name.lower()
    if any(w in n for w in ["pottery", "meat grinder", "slicer"]): return "Personal/Gift"
    if any(w in n for w in ["articles of organization", "credit card surcharge", "llc filing"]): return "Business Fees"
    if any(w in n for w in ["soldering", "3d pen"]): return "Tools"
    if any(w in n for w in ["build plate", "bed plate", "print surface"]): return "3D Printer Accessories"
    if any(w in n for w in ["earring", "jewelry"]): return "Jewelry Supplies"
    if any(w in n for w in ["pla", "filament", "3d printer filament"]): return "3D Filament"
    if any(w in n for w in ["gift box", "box", "mailer", "bubble", "wrapping", "packing", "packaging",
                             "shipping label", "label printer", "fragile sticker"]): return "Packaging & Shipping"
    if any(w in n for w in ["led", "lamp", "light", "bulb", "socket", "pendant", "lantern", " cord"]): return "Lighting Components"
    if any(w in n for w in ["screw", "bolt", "glue", "adhesive", "wire", "hook", "ring"]): return "Hardware & Fasteners"
    if any(w in n for w in ["balsa", "basswood", "wood sheet", "magnet", "clock movement", "clock mechanism", "clock kit"]): return "Crafts Supplies"
    if any(w in n for w in ["crafts", "craft"]): return "Crafts Supplies"
    if any(w in n for w in ["tape", "heavy duty"]): return "Packaging & Shipping"
    return "Other"

print("=" * 100)
print("  PERSONAL AMAZON ORDERS - ALL ITEMS")
print("=" * 100)
for o in orders:
    if o["source"] == "Personal Amazon":
        print(f"\n  Order: {o.get('order_id','')}  Date: {o['date']}  Total: ${o['grand_total']:.2f}")
        for it in o["items"]:
            cat = categorize_item(it["name"])
            flag = " *** PERSONAL ***" if cat == "Personal/Gift" else f" [{cat}]"
            print(f"    - {it['name'][:100]}")
            print(f"      qty:{it['qty']}  price:${it['price']:.2f}{flag}")

print("\n" + "=" * 100)
print("  ALL ITEMS FLAGGED AS PERSONAL/GIFT")
print("=" * 100)
for o in orders:
    for it in o["items"]:
        cat = categorize_item(it["name"])
        if cat == "Personal/Gift":
            print(f"  [{o['source']}] {it['name'][:100]}")
            print(f"    qty:{it['qty']}  price:${it['price']:.2f}  order:{o.get('order_id','')}")

print("\n" + "=" * 100)
print("  UNCATEGORIZED ITEMS (category = 'Other')")
print("=" * 100)
for o in orders:
    for it in o["items"]:
        cat = categorize_item(it["name"])
        if cat == "Other":
            print(f"  [{o['source']}] {it['name'][:100]}")
            print(f"    qty:{it['qty']}  price:${it['price']:.2f}")

print("\n" + "=" * 100)
print("  FULL ITEM LIST WITH CATEGORY + SOURCE")
print("=" * 100)
all_items = []
for o in orders:
    for it in o["items"]:
        cat = categorize_item(it["name"])
        all_items.append({
            "name": it["name"][:100],
            "cat": cat,
            "source": o["source"],
            "price": it["price"],
            "qty": it["qty"],
        })

# Sort by category
all_items.sort(key=lambda x: (x["cat"], -x["price"] * x["qty"]))
current_cat = None
for item in all_items:
    if item["cat"] != current_cat:
        current_cat = item["cat"]
        print(f"\n  --- {current_cat} ---")
    marker = " << PERSONAL" if item["source"] == "Personal Amazon" else ""
    print(f"    ${item['price']*item['qty']:>8.2f}  {item['name'][:80]}{marker}")
