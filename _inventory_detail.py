"""Show full inventory detail with exact product names, colors, and per-location breakdown."""
import json
from collections import defaultdict

with open(r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\data\generated\inventory_orders.json") as f:
    orders = json.load(f)

def loc(addr):
    if not addr: return "Unknown"
    a = addr.upper()
    if "TULSA" in a or ", OK" in a: return "Tulsa"
    if "CELINA" in a or "PROSPER" in a or ", TX" in a: return "Texas"
    return "Other"

def cat(name):
    n = name.lower()
    if any(w in n for w in ["pottery", "meat grinder", "slicer"]): return "PERSONAL (not inventory)"
    if any(w in n for w in ["articles of organization", "credit card surcharge"]): return "BUSINESS FEES"
    if any(w in n for w in ["pla", "filament", "build plate", "3d pen"]): return "3D PRINTING"
    if any(w in n for w in ["led", "lamp", "light", "bulb", "socket", "pendant", "lantern", "cord", "backlight", "cabinet", "motion sensor"]): return "LIGHTING COMPONENTS"
    if any(w in n for w in ["box", "mailer", "bubble", "wrapping", "packing", "packaging", "label", "sticker", "tape"]): return "PACKAGING & SHIPPING"
    if any(w in n for w in ["screw", "bolt", "glue", "adhesive", "wire", "hook", "ring", "earring", "jewelry"]): return "HARDWARE & JEWELRY SUPPLIES"
    if any(w in n for w in ["soldering"]): return "TOOLS"
    if any(w in n for w in ["magnet", "clock", "balsa", "basswood", "craft"]): return "CRAFTS"
    return "OTHER"

products = defaultdict(lambda: {"tulsa_qty": 0, "texas_qty": 0, "tulsa_spend": 0.0, "texas_spend": 0.0})

for o in orders:
    for item in o["items"]:
        ship = item.get("ship_to", o.get("ship_address", ""))
        location = loc(ship)
        name = item["name"]
        if name.startswith("Your package was left near the front door or porch."):
            name = name.replace("Your package was left near the front door or porch.", "").strip()

        qty = item["qty"]
        total = item["price"] * qty
        if location == "Tulsa":
            products[name]["tulsa_qty"] += qty
            products[name]["tulsa_spend"] += total
        elif location == "Texas":
            products[name]["texas_qty"] += qty
            products[name]["texas_spend"] += total

categories = defaultdict(list)
for name, info in products.items():
    c = cat(name)
    categories[c].append({"name": name, **info})

for category in ["3D PRINTING", "LIGHTING COMPONENTS", "PACKAGING & SHIPPING",
                  "HARDWARE & JEWELRY SUPPLIES", "TOOLS", "CRAFTS", "BUSINESS FEES",
                  "PERSONAL (not inventory)"]:
    if category not in categories:
        continue
    items = categories[category]
    items.sort(key=lambda x: -(x["tulsa_spend"] + x["texas_spend"]))
    total = sum(i["tulsa_spend"] + i["texas_spend"] for i in items)
    print(f"\n{'='*100}")
    print(f"  {category} -- Total: ${total:,.2f}")
    print(f"{'='*100}")
    for i, item in enumerate(items, 1):
        total_spend = item["tulsa_spend"] + item["texas_spend"]
        total_qty = item["tulsa_qty"] + item["texas_qty"]
        print(f"\n  #{i}. {item['name']}")
        print(f"      Total: ${total_spend:,.2f} ({total_qty} units)")
        parts = []
        if item["tulsa_qty"] > 0:
            parts.append(f"Tulsa: {item['tulsa_qty']} units / ${item['tulsa_spend']:,.2f}")
        if item["texas_qty"] > 0:
            parts.append(f"Texas: {item['texas_qty']} units / ${item['texas_spend']:,.2f}")
        print(f"      {' | '.join(parts)}")
