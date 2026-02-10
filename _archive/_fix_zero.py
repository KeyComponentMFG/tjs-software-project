"""Check the raw PDF text for $0 items to find the actual prices."""
import fitz, os, re

folder = r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\KeyCompInvoices\KeyCompInvoices"

zero_files = [
    "order-document (12).pdf",   # SUNLU PLA+2.0 - order total $53.88, subtotal $49.77
    "order-document (15).pdf",   # SUNLU High Speed Matte PLA 4KG - order total $141.74
    "order-document (27).pdf",   # Packaging Wholesalers boxes - order total $16.01
    "order-document (29).pdf",   # Packaging Wholesalers boxes - order total $15.97
    "order-document (30).pdf",   # SUNLU PLA Plus Black - order total $120.56
    "order-document (31).pdf",   # SUNLU PLA Plus Black - order total $137.48
    "order-document (53).pdf",   # Packaging Wholesalers boxes - order total $15.97
    "order-document (55).pdf",   # Packaging Wholesalers boxes - order total $16.01
    "order-document (6).pdf",    # Packaging Wholesalers boxes - order total $16.01
]

for fname in zero_files:
    path = os.path.join(folder, fname)
    doc = fitz.open(path)
    text = ""
    for page in doc:
        text += page.get_text()
    doc.close()

    print(f"\n{'='*80}")
    print(f"=== {fname} ===")
    print(f"{'='*80}")

    # Find all dollar amounts in the text
    prices = re.findall(r'\$(\d+\.\d+)', text)
    print(f"All dollar amounts found: {prices}")

    # Find the section around the $0 item
    # Look for "Business Price" sections
    bp_matches = list(re.finditer(r'Business Price', text))
    if bp_matches:
        for m in bp_matches:
            context = text[max(0, m.start()-200):m.end()+200]
            print(f"\nBusiness Price context:")
            print(repr(context))

    # Look for price right after item description section
    # Show relevant portion
    lines = text.split('\n')
    for i, line in enumerate(lines):
        if 'Packaging Wholesalers' in line or ('PLA' in line and ('Plus' in line or 'Matte' in line)):
            print(f"\nLines around item (line {i}):")
            for j in range(max(0, i-2), min(len(lines), i+15)):
                print(f"  [{j}] {lines[j]}")
