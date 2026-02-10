import json

with open(r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\inventory_orders.json") as f:
    d = json.load(f)

print(f"Orders loaded: {len(d)}")
total = sum(o["grand_total"] for o in d)
subtotal = sum(o["subtotal"] for o in d)
tax = sum(o["tax"] for o in d)
items = sum(len(o["items"]) for o in d)
print(f"Total spent: ${total:,.2f} (subtotal: ${subtotal:,.2f}, tax: ${tax:,.2f})")
print(f"Total line items parsed: {items}")

print("\n--- ALL ORDERS ---")
for o in sorted(d, key=lambda x: x["date"]):
    print(f"  {o['date']:25s}  ${o['grand_total']:>8.2f}  {len(o['items']):>2} items  [{o['source']}]  Order#{o['order_num']}")
    for item in o["items"]:
        print(f"      x{item['qty']}  ${item['price']:>8.2f}  {item['name'][:75]}")

print(f"\n--- SUMMARY ---")
biz = [o for o in d if o["source"] == "Key Component Mfg"]
personal = [o for o in d if o["source"] == "Personal Amazon"]
print(f"Business orders: {len(biz)} totaling ${sum(o['grand_total'] for o in biz):,.2f}")
print(f"Personal orders: {len(personal)} totaling ${sum(o['grand_total'] for o in personal):,.2f}")
