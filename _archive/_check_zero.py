import json

with open(r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\inventory_orders.json") as f:
    d = json.load(f)

print("=== ITEMS WITH $0.00 PRICE ===\n")
for o in d:
    for item in o["items"]:
        if item["price"] == 0:
            print(f"Order: {o['order_num']}")
            print(f"Date:  {o['date']}")
            print(f"File:  {o['file']}")
            print(f"Item:  {item['name']}")
            print(f"Qty:   {item['qty']}")
            print(f"Order Total: ${o['grand_total']:.2f}  |  Subtotal: ${o['subtotal']:.2f}")
            # Show all items in this order
            print(f"All items in this order:")
            for it in o["items"]:
                print(f"  x{it['qty']}  ${it['price']:>8.2f}  {it['name'][:70]}")
            print()
