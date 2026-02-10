"""Check payment methods across all orders."""
import json
from collections import Counter

with open(r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\data\generated\inventory_orders.json") as f:
    orders = json.load(f)

print("=== ALL FIELDS IN FIRST ORDER ===")
for key, val in orders[0].items():
    if key != "items":
        print(f"  {key}: {val}")
print(f"  items[0] keys: {list(orders[0]['items'][0].keys())}")

print("\n=== CHECKING FOR PAYMENT INFO ===")
payment_fields = set()
for o in orders:
    for key in o.keys():
        if "pay" in key.lower() or "card" in key.lower() or "method" in key.lower():
            payment_fields.add(key)
    for item in o["items"]:
        for key in item.keys():
            if "pay" in key.lower() or "card" in key.lower() or "method" in key.lower():
                payment_fields.add(f"item.{key}")

if payment_fields:
    print(f"  Found payment fields: {payment_fields}")
    for field in payment_fields:
        values = Counter()
        for o in orders:
            if field in o:
                values[str(o[field])] += 1
        print(f"\n  {field} values:")
        for val, count in values.most_common():
            print(f"    {val}: {count} orders")
else:
    print("  No payment fields found in JSON data.")

print("\n=== UNIQUE SOURCES ===")
sources = Counter()
for o in orders:
    sources[o.get("source", "unknown")] += 1
for src, count in sources.most_common():
    print(f"  {src}: {count} orders")

print("\n=== UNIQUE FILES (showing source variety) ===")
for o in orders:
    src = o.get("source", "")
    if src not in ("Key Component Mfg", "Personal Amazon"):
        print(f"  Source: {src}  File: {o.get('file', '')}  Total: ${o['grand_total']:.2f}")
