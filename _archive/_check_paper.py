import fitz

path = r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\KeyCompInvoices\KeyCompInvoices\Paper Receipts.pdf"
doc = fitz.open(path)
print(f"Paper Receipts.pdf: {len(doc)} pages")
for i, page in enumerate(doc):
    text = page.get_text()
    print(f"\n--- Page {i+1} ---")
    print(f"Text length: {len(text)}")
    if text.strip():
        print(f"Text: {text[:500]}")
    else:
        print("(No text - checking for images...)")
        images = page.get_images()
        print(f"Images on page: {len(images)}")
        if images:
            for img_idx, img in enumerate(images):
                xref = img[0]
                pix = fitz.Pixmap(doc, xref)
                print(f"  Image {img_idx}: {pix.width}x{pix.height}, colorspace: {pix.colorspace}")
                # Save image for inspection
                out = f"C:\\Users\\mcnug\\OneDrive\\Desktop\\etsy statments\\paper_receipt_page{i+1}.png"
                pix.save(out)
                print(f"  Saved to: {out}")
                pix = None
doc.close()
