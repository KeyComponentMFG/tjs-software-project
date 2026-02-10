"""Show exact items at each location (Tulsa vs Texas)."""
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
    if any(w in n for w in ["pottery", "meat grinder", "slicer"]): return "PERSONAL"
    if any(w in n for w in ["articles of organization", "credit card surcharge"]): return "BIZ FEE"
    if any(w in n for w in ["balsa", "basswood", "magnet", "clock", "quartz clock"]): return "Crafts"
    if any(w in n for w in ["soldering", "3d pen"]): return "Tools"
    if any(w in n for w in ["build plate", "bed plate"]): return "3D Accessories"
    if any(w in n for w in ["earring", "jewelry"]): return "Jewelry Supplies"
    if any(w in n for w in ["pla", "filament"]): return "3D Filament"
    if any(w in n for w in ["gift box", "box", "mailer", "bubble", "wrapping", "packing", "packaging",
                             "label", "sticker", "tape", "heavy duty"]): return "Packaging"
    if any(w in n for w in ["led", "lamp", "light", "bulb", "socket", "pendant", "lantern", "cord",
                             "backlight", "cabinet", "motion sensor"]): return "Lighting"
    if any(w in n for w in ["screw", "bolt", "glue", "adhesive", "wire", "hook", "ring"]): return "Hardware"
    if any(w in n for w in ["crafts", "craft"]): return "Crafts"
    return "Other"

# Build per-location item lists
tulsa_items = defaultdict(lambda: {"qty": 0, "spend": 0.0, "category": "", "dates": []})
texas_items = defaultdict(lambda: {"qty": 0, "spend": 0.0, "category": "", "dates": []})

for o in orders:
    for item in o["items"]:
        ship = item.get("ship_to", o.get("ship_address", ""))
        location = loc(ship)
        name = item["name"]
        if name.startswith("Your package was left near the front door or porch."):
            name = name.replace("Your package was left near the front door or porch.", "").strip()

        qty = item["qty"]
        total = item["price"] * qty
        category = cat(name)

        if location == "Tulsa":
            tulsa_items[name]["qty"] += qty
            tulsa_items[name]["spend"] += total
            tulsa_items[name]["category"] = category
            tulsa_items[name]["dates"].append(o["date"])
        elif location == "Texas":
            texas_items[name]["qty"] += qty
            texas_items[name]["spend"] += total
            texas_items[name]["category"] = category
            texas_items[name]["dates"].append(o["date"])

for label, items in [("TULSA, OK", tulsa_items), ("TEXAS (Celina/Prosper)", texas_items)]:
    total_spend = sum(i["spend"] for i in items.values())
    total_qty = sum(i["qty"] for i in items.values())
    print(f"\n{'='*110}")
    print(f"  {label} -- {len(items)} unique items, {total_qty} total units, ${total_spend:,.2f}")
    print(f"{'='*110}")

    # Group by category
    by_cat = defaultdict(list)
    for name, info in items.items():
        by_cat[info["category"]].append({"name": name, **info})

    for category in ["3D Filament", "3D Accessories", "Lighting", "Packaging", "Hardware",
                      "Jewelry Supplies", "Tools", "Crafts", "BIZ FEE", "PERSONAL", "Other"]:
        if category not in by_cat:
            continue
        cat_items = by_cat[category]
        cat_items.sort(key=lambda x: -x["spend"])
        cat_total = sum(i["spend"] for i in cat_items)
        cat_qty = sum(i["qty"] for i in cat_items)
        print(f"\n  --- {category} ({cat_qty} units, ${cat_total:,.2f}) ---")
        for i, item in enumerate(cat_items, 1):
            # Shorten name but keep color info
            short_name = item["name"][:90]
            personal_tag = " ** PERSONAL **" if category == "PERSONAL" else ""
            print(f"    {i:2d}. [{item['qty']}x] ${item['spend']:>8.2f}  {short_name}{personal_tag}")
