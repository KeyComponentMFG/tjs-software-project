"""Quick script to peek at the first 200 chars of each PDF in other_receipts."""

import os
import fitz  # PyMuPDF

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RECEIPTS_DIR = os.path.join(BASE_DIR, "data", "invoices", "other_receipts")

for fname in sorted(os.listdir(RECEIPTS_DIR)):
    if not fname.lower().endswith(".pdf"):
        continue
    path = os.path.join(RECEIPTS_DIR, fname)
    try:
        doc = fitz.open(path)
        page_count = len(doc)
        text = ""
        for page in doc:
            text += page.get_text()
            if len(text) >= 200:
                break
        doc.close()
        preview = text[:200].replace("\n", " ").strip()
        print(f"\n--- {fname} ---")
        print(f"  Pages: {page_count}")
        print(f"  Preview: {preview if preview else '(no extractable text - likely scanned image)'}")
    except Exception as e:
        print(f"\n--- {fname} ---")
        print(f"  ERROR: {e}")
