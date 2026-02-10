import json
with open(r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\data\generated\inventory_orders.json") as f:
    orders = json.load(f)
for o in orders:
    if "Gigi" in str(o.get("file", "")):
        print(f"File: {o['file']}")
        print(f"Date: {o['date']}")
        print(f"Total: ${o['grand_total']:.2f}")
        print(f"Source: {o['source']}")
        for it in o["items"]:
            print(f"  - {it['name'][:80]}  qty:{it['qty']}  ${it['price']:.2f}")
        print()
