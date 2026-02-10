import fitz, os

folder = r"C:\Users\mcnug\OneDrive\Desktop\etsy statments\new reciepts"

for fname in sorted(os.listdir(folder)):
    if not fname.lower().endswith(".pdf"):
        continue
    path = os.path.join(folder, fname)
    doc = fitz.open(path)
    print(f"\n{'='*80}")
    print(f"=== {fname} ({len(doc)} pages) ===")
    print(f"{'='*80}")
    for i, page in enumerate(doc):
        text = page.get_text()
        if text.strip():
            print(f"--- Page {i+1} (text) ---")
            print(text[:3000])
        else:
            print(f"--- Page {i+1} (image only) ---")
            images = page.get_images()
            print(f"  {len(images)} image(s)")
            for img_idx, img in enumerate(images):
                xref = img[0]
                pix = fitz.Pixmap(doc, xref)
                out = os.path.join(folder, f"{fname}_page{i+1}.png")
                pix.save(out)
                print(f"  Saved: {out} ({pix.width}x{pix.height})")
                pix = None
    doc.close()
