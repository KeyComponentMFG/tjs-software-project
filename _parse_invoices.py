"""Parse all Key Component invoices and Personal Amazon orders from PDFs."""
import fitz, os, re, json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
folder = os.path.join(BASE_DIR, "data", "invoices", "keycomp")
pa_folder = os.path.join(BASE_DIR, "data", "invoices", "personal_amazon")


def extract_payment_method(text):
    """Extract payment card type and last 4 digits from PDF text."""
    m = re.search(
        r'(Visa|Mastercard|Discover|Amex|American Express)\s*\|?\s*(?:Last digits:|ending in)\s*(\d+)',
        text, re.IGNORECASE
    )
    if m:
        return f"{m.group(1)} ending in {m.group(2)}"
    return "Unknown"


def parse_business_order(text, fname):
    order_match = re.search(r"Order #([\d-]+)", text)
    order_num = order_match.group(1) if order_match else "Unknown"

    date_match = re.search(r"Order Placed:\s+(.+?)(?:\n|Amazon)", text)
    order_date = date_match.group(1).strip() if date_match else "Unknown"

    total_match = re.search(r"Grand Total:\s*\$([\d,]+\.\d+)", text)
    if not total_match:
        total_match = re.search(r"Order Total:\s*\$([\d,]+\.\d+)", text)
    grand_total = float(total_match.group(1).replace(",", "")) if total_match else 0

    subtotal_match = re.findall(r"Item\(s\) Subtotal:\s*\$([\d,]+\.\d+)", text)
    subtotal = float(subtotal_match[-1].replace(",", "")) if subtotal_match else 0

    tax_match = re.search(r"Estimated Tax:\s*\$([\d,]+\.\d+)", text)
    tax = float(tax_match.group(1).replace(",", "")) if tax_match else 0

    # Extract items
    items = []
    # Pattern: "N of: ITEM_NAME\n...\nSold by: SELLER\n...\nCondition: New...\n$PRICE" or "Business Price\n...\n$PRICE"
    parts = re.split(r"(\d+) of: ", text)
    for i in range(1, len(parts) - 1, 2):
        qty = int(parts[i])
        rest = parts[i + 1]
        # Get item name up to "Sold by"
        name_match = re.search(r"^(.+?)(?:Sold by|$)", rest, re.DOTALL)
        name = " ".join(name_match.group(1).split())[:200] if name_match else "Unknown"

        # Get seller
        seller_match = re.search(r"Sold by:\s*(.+?)(?:\s*\(seller profile\)|\n)", rest)
        if not seller_match:
            seller_match = re.search(r"Sold by:\s*(.+)", rest)
        seller = seller_match.group(1).strip() if seller_match else "Amazon.com"

        # Get shipping address for this item's shipment
        addr_match = re.search(r"Shipping Address:\s*\n(.+?)\n(.+?)\n(.+?)\n", rest)
        ship_to = ""
        if addr_match:
            ship_name = addr_match.group(1).strip()
            ship_street = addr_match.group(2).strip()
            ship_city_state = addr_match.group(3).strip()
            ship_to = f"{ship_name}, {ship_street}, {ship_city_state}"

        # Try multiple price extraction strategies
        price = 0
        # Strategy 1: "Condition: New" (possibly with extra text) then $PRICE
        price_match = re.search(r"Condition: New[^\n]*\n\$([\d,]+\.\d+)", rest)
        if price_match:
            price = float(price_match.group(1).replace(",", ""))
        # Strategy 2: "Business Price" then optional "Condition: New..." then $PRICE
        if price == 0:
            price_match = re.search(r"Business Price\s*(?:Condition: New[^\n]*)?\n\$([\d,]+\.\d+)", rest)
            if price_match:
                price = float(price_match.group(1).replace(",", ""))
        # Strategy 3: Price appears BEFORE "Sold by" on a line by itself (multi-shipment PDFs)
        if price == 0:
            sold_by_pos = rest.find("Sold by")
            if sold_by_pos > 0:
                before_sold = rest[:sold_by_pos]
                inline_price = re.findall(r"\$([\d,]+\.\d+)", before_sold)
                if inline_price:
                    price = float(inline_price[-1].replace(",", ""))
        # Strategy 4: For single-item orders, use the shipment subtotal
        if price == 0:
            shipment_sub = re.search(r"Item\(s\) Subtotal:\s*\n?\$([\d,]+\.\d+)", rest)
            if shipment_sub:
                price = float(shipment_sub.group(1).replace(",", ""))

        items.append({"qty": qty, "name": name, "price": price, "seller": seller, "ship_to": ship_to})

    # Get primary shipping address for the order (first one found)
    primary_addr = re.search(r"Shipping Address:\s*\n(.+?)\n(.+?)\n(.+?)\n", text)
    ship_address = ""
    if primary_addr:
        ship_address = f"{primary_addr.group(1).strip()}, {primary_addr.group(2).strip()}, {primary_addr.group(3).strip()}"

    # Fill missing ship_to on items from order-level address
    for item in items:
        if not item["ship_to"] and ship_address:
            item["ship_to"] = ship_address

    return {
        "order_num": order_num,
        "date": order_date,
        "grand_total": grand_total,
        "subtotal": subtotal,
        "tax": tax,
        "items": items,
        "source": "Key Component Mfg",
        "file": fname,
        "ship_address": ship_address,
        "payment_method": extract_payment_method(text),
    }


def parse_personal_order(text, fname):
    order_match = re.search(r"Order #\s*([\d-]+)", text)
    order_num = order_match.group(1) if order_match else "Unknown"

    date_match = re.search(r"Order placed\s+(.+?)(?:\n|Order)", text)
    order_date = date_match.group(1).strip() if date_match else "Unknown"

    total_match = re.search(r"Grand Total:\s*\$([\d,]+\.\d+)", text)
    grand_total = float(total_match.group(1).replace(",", "")) if total_match else 0

    subtotal_match = re.search(r"Item\(s\) Subtotal:\s*\$([\d,]+\.\d+)", text)
    subtotal = float(subtotal_match.group(1).replace(",", "")) if subtotal_match else 0

    tax_match = re.search(r"(?:Estimated tax|Sales Tax).*?:\s*\$([\d,]+\.\d+)", text)
    tax = float(tax_match.group(1).replace(",", "")) if tax_match else 0

    promo_match = re.search(r"Promotion Applied:\s*-\$([\d,]+\.\d+)", text)
    promo = float(promo_match.group(1).replace(",", "")) if promo_match else 0

    # Extract sellers
    sellers = re.findall(r"Sold by:\s*(.+?)(?:\n|$)", text)

    # Shipping address
    addr_match = re.search(r"Ship to\s*\n(.+?)\n(.+?)\n(.+?)\n", text)
    ship_address = ""
    if addr_match:
        ship_address = f"{addr_match.group(1).strip()}, {addr_match.group(2).strip()}, {addr_match.group(3).strip()}"

    # Extract items by splitting on "Sold by:" and looking back for name, forward for price
    items = []
    sections = re.split(r"Sold by:\s*", text)
    for i in range(1, len(sections)):
        # Product name: last lines before "Sold by:" (skip delivery/status/price lines)
        prev_lines = sections[i - 1].strip().split("\n")
        name_lines = []
        for line in reversed(prev_lines):
            line = line.strip()
            if not line:
                break
            if re.match(r"(Delivered|Your package|Return window|\$[\d,]+\.\d+|Back to top|Condition|Order placed|Ship to|United States)", line):
                break
            name_lines.insert(0, line)
            if len(name_lines) >= 5:
                break
        name = " ".join(" ".join(name_lines).split())[:200]
        # Seller and price from the section after "Sold by:"
        seller_match = re.match(r"(.+?)[\n\r]", sections[i])
        seller = seller_match.group(1).strip() if seller_match else "Unknown"
        price_match = re.search(r"\$([\d,]+\.\d+)", sections[i])
        if not name or not price_match:
            continue
        price = float(price_match.group(1).replace(",", ""))
        items.append({"qty": 1, "name": name, "price": price,
                      "seller": seller, "ship_to": ship_address})

    # If no items parsed but we have sellers, add a generic item
    if not items and sellers:
        for s_idx, seller in enumerate(sellers):
            items.append({"qty": 1, "name": "Unknown item", "price": subtotal / len(sellers) if sellers else subtotal,
                          "seller": seller.strip(), "ship_to": ship_address})

    return {
        "order_num": order_num,
        "date": order_date,
        "grand_total": grand_total,
        "subtotal": subtotal,
        "tax": tax,
        "promo": promo,
        "items": items,
        "source": "Personal Amazon",
        "file": fname,
        "ship_address": ship_address,
        "payment_method": extract_payment_method(text),
    }


def parse_pdf_file(filepath):
    """Parse a single PDF invoice. Returns order dict or None."""
    doc = fitz.open(filepath)
    text = "\n".join(page.get_text() for page in doc)
    doc.close()
    fname = os.path.basename(filepath)
    # Try business format first, then personal
    if "Grand Total:" in text or "Business Price" in text or " of: " in text:
        return parse_business_order(text, fname)
    else:
        return parse_personal_order(text, fname)


if __name__ == "__main__":
    all_orders = []

    # Process main folder
    for fname in sorted(os.listdir(folder)):
        if not fname.endswith(".pdf") or fname == "Paper Receipts.pdf":
            continue
        path = os.path.join(folder, fname)
        if os.path.isdir(path):
            continue
        doc = fitz.open(path)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        order = parse_business_order(text, fname)
        all_orders.append(order)
        print(f"{fname:45s}  {order['date']:25s}  ${order['grand_total']:>8.2f}  items: {len(order['items'])}")

    # Process Personal Amazon subfolder
    for fname in sorted(os.listdir(pa_folder)):
        if not fname.endswith(".pdf"):
            continue
        path = os.path.join(pa_folder, fname)
        doc = fitz.open(path)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        order = parse_personal_order(text, fname)
        all_orders.append(order)
        print(f"Personal Amazon/{fname:35s}  {order['date']:25s}  ${order['grand_total']:>8.2f}  items: {len(order['items'])}")

    # Paper Receipts (scanned images - manually entered)
    paper_receipts = [
        {
            "order_num": "HL-20250526",
            "date": "May 26, 2025",
            "grand_total": 3.78,
            "subtotal": 3.49,
            "tax": 0.29,
            "items": [{"qty": 1, "name": "Crafts supplies", "price": 3.49,
                       "seller": "Hobby Lobby", "ship_to": "1050 S. Preston Rd, Prosper, TX 75078"}],
            "source": "Hobby Lobby",
            "file": "Paper Receipts.pdf (page 1)",
            "ship_address": "1050 S. Preston Rd, Prosper, TX 75078",
            "payment_method": "Unknown (paper receipt)",
        },
        {
            "order_num": "HD-20251231",
            "date": "December 31, 2025",
            "grand_total": 16.86,
            "subtotal": 15.54,
            "tax": 1.32,
            "items": [
                {"qty": 1, "name": "Orange Heavy Duty Bubble Wrap 12 IN. X 25 FT.", "price": 9.98,
                 "seller": "Home Depot", "ship_to": "Braden Walker, 606 W ASH ST, Celina, TX 75009"},
                {"qty": 1, "name": "HD Small Box 16x10x12", "price": 5.56,
                 "seller": "Home Depot", "ship_to": "Braden Walker, 606 W ASH ST, Celina, TX 75009"},
            ],
            "source": "Home Depot",
            "file": "Paper Receipts.pdf (page 2)",
            "ship_address": "Braden Walker, 606 W ASH ST, Celina, TX 75009",
            "payment_method": "Unknown (paper receipt)",
        },
        {
            "order_num": "HL-20251231",
            "date": "December 31, 2025",
            "grand_total": 7.57,
            "subtotal": 6.99,
            "tax": 0.58,
            "items": [
                {"qty": 1, "name": "Crafts supplies", "price": 5.58,
                 "seller": "Hobby Lobby", "ship_to": "Braden Walker, 606 W ASH ST, Celina, TX 75009"},
                {"qty": 1, "name": "Crafts supplies", "price": 1.41,
                 "seller": "Hobby Lobby", "ship_to": "Braden Walker, 606 W ASH ST, Celina, TX 75009"},
            ],
            "source": "Hobby Lobby",
            "file": "Paper Receipts.pdf (page 3)",
            "ship_address": "Braden Walker, 606 W ASH ST, Celina, TX 75009",
            "payment_method": "Unknown (paper receipt)",
        },
        {
            "order_num": "HL-20260103",
            "date": "January 3, 2026",
            "grand_total": 3.78,
            "subtotal": 3.49,
            "tax": 0.29,
            "items": [{"qty": 1, "name": "Crafts supplies", "price": 3.49,
                       "seller": "Hobby Lobby", "ship_to": "1050 S. Preston Rd, Prosper, TX 75078"}],
            "source": "Hobby Lobby",
            "file": "Paper Receipts.pdf (page 4)",
            "ship_address": "1050 S. Preston Rd, Prosper, TX 75078",
            "payment_method": "Unknown (paper receipt)",
        },
        # --- New receipts from "new reciepts" folder ---
        {
            "order_num": "111-6904647-1693853",
            "date": "October 30, 2025",
            "grand_total": 44.26,
            "subtotal": 40.79,
            "tax": 3.47,
            "items": [{"qty": 1, "name": "SUNLU PLA+2.0 3D Printer Filament Bundle, 4KG Upgrade PLA+ Filament 1.75mm, Black", "price": 40.79,
                       "seller": "SUNLU 3D Store", "ship_to": "Thomas, 2725 E 47TH ST, TULSA, OK 74105-5346"}],
            "source": "Personal Amazon",
            "file": "1B1BF75F-E70F-4A45-ABB7-FE69526DFCC8_224AE358477D.pdf",
            "ship_address": "Thomas, 2725 E 47TH ST, TULSA, OK 74105-5346",
            "payment_method": "Unknown (scanned receipt)",
        },
        {
            "order_num": "111-5998008-5094648",
            "date": "November 1, 2025",
            "grand_total": 36.34,
            "subtotal": 33.49,
            "tax": 2.85,
            "items": [
                {"qty": 1, "name": "Luminate 3D Printer Magnetic Build Plate 256x256x2, X1C/X1/P1P/P1S Compatible G10 Garolite FR4", "price": 28.50,
                 "seller": "Luminate 3D", "ship_to": "Thomas, 2725 E 47TH ST, TULSA, OK 74105-5346"},
                {"qty": 1, "name": "WOUSEDO 12 X 3g Super Glue Liquid, Clear Strong Adhesive, Fast Drying Cyanoacrylate Glue", "price": 4.99,
                 "seller": "Wusedo", "ship_to": "Thomas, 2725 E 47TH ST, TULSA, OK 74105-5346"},
            ],
            "source": "Personal Amazon",
            "file": "771DED3D-27DE-4580-8D34-B68DDC85A922_3691A3DEAEC4.pdf",
            "ship_address": "Thomas, 2725 E 47TH ST, TULSA, OK 74105-5346",
            "payment_method": "Visa ending in 3146",
        },
        {
            "order_num": "SOS-20251107",
            "date": "November 7, 2025",
            "grand_total": 104.00,
            "subtotal": 100.00,
            "tax": 0.00,
            "items": [
                {"qty": 1, "name": "Articles of Organization filing - KEY COMPONENT MANUFACTURING LLC", "price": 100.00,
                 "seller": "Oklahoma Secretary of State", "ship_to": "2725 E 47TH ST, TULSA, OK 74105"},
                {"qty": 1, "name": "Credit Card Surcharge", "price": 4.00,
                 "seller": "Oklahoma Secretary of State", "ship_to": "2725 E 47TH ST, TULSA, OK 74105"},
            ],
            "source": "Oklahoma Secretary of State",
            "file": "Articles of Organization (Reciept).PDF",
            "ship_address": "2725 E 47TH ST, TULSA, OK 74105",
            "payment_method": "Unknown (credit card surcharge noted)",
        },
        {
            "order_num": "111-1069176-7087424",
            "date": "November 10, 2025",
            "grand_total": 30.34,
            "subtotal": 27.96,
            "tax": 2.38,
            "items": [
                {"qty": 1, "name": "Balsa Wood Sheet, 5 Pack Plywood Sheets, Basswood Sheets 12X12X1/16 Inch, Unfinished Wood Boards Blanks for Laser Cutting", "price": 7.99,
                 "seller": "LNAYIAN", "ship_to": "Thomas, 2725 E 47TH ST, TULSA, OK 74105-5346"},
                {"qty": 1, "name": "DIYMAG 400pcs Small Magnets, 4 Different Sizes Tiny Mini Magnets, Multi-Use for Fridge, DIY, Office, Hobbies, Crafts", "price": 9.98,
                 "seller": "Rhinocelos Direct", "ship_to": "Thomas, 2725 E 47TH ST, TULSA, OK 74105-5346"},
                {"qty": 1, "name": "Sinji High Torque Quartz Clock Movement Mechanism Replacement Clock Kit with 12 Pairs of Hands", "price": 9.99,
                 "seller": "Clock Spine", "ship_to": "Thomas, 2725 E 47TH ST, TULSA, OK 74105-5346"},
            ],
            "source": "Personal Amazon",
            "file": "EEE20255-076A-4A0E-B152-09507DF493CD_3E772D0A95AA.pdf",
            "ship_address": "Thomas, 2725 E 47TH ST, TULSA, OK 74105-5346",
            "payment_method": "Unknown (scanned receipt)",
        },
        # --- Alibaba order ---
        {
            "order_num": "TA_CONTRACT_1770786820686",
            "date": "December 17, 2025",
            "grand_total": 75.31,
            "subtotal": 45.00,
            "tax": 5.90,
            "items": [{"qty": 1, "name": "Bambu Lab LED Lamp Kit 60mm w/ IR Remote 30 Pack (Silver Shell)", "price": 45.00,
                       "seller": "RTR LED Limited", "ship_to": "TJ McNulty, 2725 E 47TH ST, TULSA, OK 74105-5346"}],
            "source": "Alibaba",
            "file": "TA_CONTRACT_1770786820686.pdf",
            "ship_address": "TJ McNulty, 2725 E 47TH ST, TULSA, OK 74105-5346",
            "payment_method": "Discover ending in 4570 (via PayPal)",
        },
        # --- SUNLU direct store order ---
        {
            "order_num": "SL29941425",
            "date": "November 21, 2025",
            "grand_total": 69.84,
            "subtotal": 69.84,
            "tax": 0.00,
            "items": [{"qty": 1, "name": "SUNLU High Speed PLA/Matte PLA Filament 1KG 6-Pack (White, Black, Oliver, Sunny, Sky Blue, Red)", "price": 69.84,
                       "seller": "SUNLU Direct", "ship_to": "Thomas McNulty, 2725 E 47TH ST, TULSA, OK 74105"}],
            "source": "SUNLU",
            "file": "order_receipt_10028582812.pdf",
            "ship_address": "Thomas McNulty, 2725 E 47TH ST, TULSA, OK 74105",
            "payment_method": "Visa ending in 5683 (Shop Pay)",
        },
        # --- SUNLU direct orders Jan 25 2026 ---
        {
            "order_num": "SL33398726",
            "date": "January 25, 2026",
            "grand_total": 25.98,
            "subtotal": 25.98,
            "tax": 0.00,
            "items": [{"qty": 1, "name": "SUNLU High Speed Matte PLA 1KG Cherry Red 2-Pack", "price": 25.98,
                       "seller": "SUNLU Direct", "ship_to": "Braden Walker, 606 W ASH ST, Celina, TX 75009"}],
            "source": "SUNLU",
            "file": "order_receipt_10954105734.pdf",
            "ship_address": "Braden Walker, 606 W ASH ST, Celina, TX 75009",
            "payment_method": "PayPal",
        },
        {
            "order_num": "SL33398226",
            "date": "January 25, 2026",
            "grand_total": 39.56,
            "subtotal": 39.56,
            "tax": 0.00,
            "items": [
                {"qty": 1, "name": "SUNLU High Speed Matte PLA 1KG Cherry Red 2-Pack", "price": 26.78,
                 "seller": "SUNLU Direct", "ship_to": "Thomas McNulty, 2725 E 47TH ST, TULSA, OK 74105"},
                {"qty": 1, "name": "SUNLU SILK 3D Printer Filament 1KG Silk Silver", "price": 13.99,
                 "seller": "SUNLU Direct", "ship_to": "Thomas McNulty, 2725 E 47TH ST, TULSA, OK 74105"},
            ],
            "source": "SUNLU",
            "file": "order_receipt_10953944433.pdf",
            "ship_address": "Thomas McNulty, 2725 E 47TH ST, TULSA, OK 74105",
            "payment_method": "PayPal",
        },
    ]

    for pr in paper_receipts:
        all_orders.append(pr)
        print(f"{'Paper: ' + pr['source']:45s}  {pr['date']:25s}  ${pr['grand_total']:>8.2f}  items: {len(pr['items'])}")

    print()
    total = sum(o["grand_total"] for o in all_orders)
    biz_total = sum(o["grand_total"] for o in all_orders if o["source"] == "Key Component Mfg")
    personal_total = sum(o["grand_total"] for o in all_orders if o["source"] == "Personal Amazon")
    biz_subtotal = sum(o["subtotal"] for o in all_orders if o["source"] == "Key Component Mfg")
    biz_tax = sum(o["tax"] for o in all_orders if o["source"] == "Key Component Mfg")
    print(f"TOTAL ORDERS: {len(all_orders)}")
    print(f"TOTAL SPENT: ${total:,.2f}")
    print(f"  Business (Key Comp): ${biz_total:,.2f} (subtotal: ${biz_subtotal:,.2f}, tax: ${biz_tax:,.2f})")
    print(f"  Personal Amazon: ${personal_total:,.2f}")

    # Save as JSON for dashboard to consume
    out_path = os.path.join(BASE_DIR, "data", "generated", "inventory_orders.json")
    with open(out_path, "w") as f:
        json.dump(all_orders, f, indent=2)
    print(f"\nSaved {len(all_orders)} orders to inventory_orders.json")

    # Print all items
    print("\n=== ALL ITEMS ===")
    for o in all_orders:
        for item in o["items"]:
            seller = item.get("seller", "Unknown")
            ship = item.get("ship_to", o.get("ship_address", ""))
            print(f"  {o['date']:25s}  ${item['price']:>8.2f}  x{item['qty']}  {item['name'][:60]}  | Seller: {seller[:30]}  | Ship: {ship[:40]}")
