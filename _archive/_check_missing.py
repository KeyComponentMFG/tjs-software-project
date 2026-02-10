import json

with open(r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\inventory_orders.json") as f:
    d = json.load(f)

print("=== ORDERS MISSING SHIP ADDRESS ===")
for o in d:
    addr = o.get("ship_address", "")
    if not addr:
        print(f"  {o['date']:25s}  Order#{o['order_num']}  ${o['grand_total']:>8.2f}  [{o['source']}]  File: {o['file']}")

print("\n=== ITEMS MISSING SHIP_TO ===")
for o in d:
    for item in o["items"]:
        ship = item.get("ship_to", "")
        if not ship:
            print(f"  {o['date']:25s}  ${item['price']:>8.2f}  {item['name'][:60]}  Seller: {item.get('seller','?')[:25]}  File: {o['file']}")
