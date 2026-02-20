"""Parse Capital One bank statement PDFs and CSVs → data/generated/bank_transactions.json"""
import fitz, os, re, json, csv
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BANK_DIR = os.path.join(BASE_DIR, "data", "bank_statements")
CONFIG_PATH = os.path.join(BASE_DIR, "data", "config.json")
OUT_PATH = os.path.join(BASE_DIR, "data", "generated", "bank_transactions.json")

# ── Load config ──────────────────────────────────────────────────────────────

with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

CATEGORY_OVERRIDES = CONFIG.get("category_overrides", {})
TRANSACTION_OVERRIDES = CONFIG.get("transaction_overrides", [])
MANUAL_TRANSACTIONS = CONFIG.get("manual_transactions", [])

# ── Auto-categorization rules ────────────────────────────────────────────────

def auto_categorize(desc, txn_type):
    """Categorize a transaction based on its description."""
    d = desc.upper()

    # Check config overrides first (exact desc-prefix match)
    for pattern, category in CATEGORY_OVERRIDES.items():
        if pattern.upper() in d:
            return category

    if txn_type == "deposit":
        if "ETSY" in d:
            return "Etsy Payout"
        return "Other Deposit"

    # Debit categorization
    if "AMAZON MKTPL" in d:
        return "Amazon Inventory"
    if "UPS STORE" in d or "USPS" in d:
        return "Shipping"
    if "WAL MART" in d:
        return "Shipping"
    if "HOBBYLOBBY" in d or "HOBBY LOBBY" in d:
        return "Craft Supplies"
    if "WESTLAKE HARDWARE" in d:
        return "Craft Supplies"
    if "PAYPAL" in d and ("ALIPAY" in d or "AOWEIKE" in d):
        return "AliExpress Supplies"
    if "ETSY COM" in d:
        return "Etsy Fees"
    if "VENMO" in d:
        return "Owner Draw - Tulsa"  # default; overrides checked above
    if "BEST BUY" in d and "AUTO PYMT" in d:
        return "Business Credit Card"
    if "PAYPAL" in d and "THANGS" in d:
        return "Subscriptions"
    # Restaurants / clothing — personal spending = Owner Draw - Tulsa
    if any(w in d for w in ["REASORS", "CHIPOTLE", "WILDFLOWERCAFE",
                             "ANTHROPOLOGIE", "LULULEMON", "QT "]):
        return "Owner Draw - Tulsa"
    return "Uncategorized"


# ── PDF Parsing ──────────────────────────────────────────────────────────────

MONTH_NAMES = {"JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4, "MAY": 5,
               "JUNE": 6, "JULY": 7, "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10,
               "NOVEMBER": 11, "DECEMBER": 12}


def extract_period(text):
    """Extract the full date range from the 'FOR PERIOD' header line.
    Returns (start_year, end_year, set of YYYY-MM strings covered)."""
    m = re.search(r"FOR PERIOD\s+(\w+)\s+(\d+),?\s+(\d{4})\s*-\s*(\w+)\s+(\d+),?\s+(\d{4})", text)
    if m:
        start_month = MONTH_NAMES.get(m.group(1).upper(), 0)
        start_year = int(m.group(3))
        end_month = MONTH_NAMES.get(m.group(4).upper(), 0)
        end_year = int(m.group(6))
        # Build set of covered YYYY-MM keys
        covered = set()
        y, mo = start_year, start_month
        while (y, mo) <= (end_year, end_month):
            covered.add(f"{y}-{mo:02d}")
            mo += 1
            if mo > 12:
                mo = 1
                y += 1
        return start_year, end_year, covered
    return None, None, set()


def get_year_for_month(month_num, start_year, end_year):
    """Determine the correct year for a MM/DD date given statement period."""
    if start_year == end_year:
        return start_year
    # Cross-year statement (e.g., Dec 2025 - Jan 2026)
    # High months (Oct-Dec) → start_year, low months (Jan-Mar) → end_year
    if month_num >= 10:
        return start_year
    return end_year


def parse_bank_pdf(filepath):
    """Parse a single Capital One bank statement PDF into transactions.
    Returns (transactions_list, covered_months_set)."""
    doc = fitz.open(filepath)
    fname = os.path.basename(filepath)

    # Collect all page text, skipping page 2 (boilerplate)
    full_text = ""
    start_year, end_year, covered_months = None, None, set()
    for i, page in enumerate(doc):
        text = page.get_text()
        # Extract period from any page that has it
        if start_year is None:
            sy, ey, cov = extract_period(text)
            if sy:
                start_year, end_year, covered_months = sy, ey, cov
        # Skip page 2 (always "Important Message" boilerplate)
        if i == 1:
            continue
        full_text += text + "\n"
    doc.close()

    if start_year is None:
        print(f"  WARNING: Could not extract period from {fname}, defaulting to 2025/2026")
        start_year, end_year = 2025, 2026

    # Parse transactions using line-by-line state machine
    lines = full_text.split("\n")
    transactions = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Match a date line: exactly MM/DD
        date_match = re.match(r"^(\d{2}/\d{2})$", line)
        if not date_match:
            i += 1
            continue

        date_str = date_match.group(1)
        month_num = int(date_str.split("/")[0])
        year = get_year_for_month(month_num, start_year, end_year)
        full_date = f"{date_str}/{year}"

        # Accumulate description lines until we hit a dollar amount
        i += 1
        desc_lines = []
        deposit_amt = None
        debit_amt = None
        resulting_balance = None

        while i < len(lines):
            ln = lines[i].strip()

            # Stop if we hit the next date line, "Total" line, or end markers
            if re.match(r"^\d{2}/\d{2}$", ln):
                break
            if ln.startswith("Total"):
                break
            if ln.startswith("CONTINUED FOR PERIOD") or ln.startswith("PAGE "):
                i += 1
                continue
            # Skip header repetitions
            if ln in ("Date", "Description", "Deposits/Credits",
                      "Withdrawals/Debits", "Resulting Balance", "ACCOUNT DETAIL"):
                i += 1
                continue
            # Skip lines that are just boilerplate/headers
            if "Products and services" in ln or "Capital One" in ln:
                i += 1
                continue
            if "KEY COMPONENT MANUFACTURING" in ln:
                i += 1
                continue
            if "Speak to a dedicated" in ln or "at 1-888-755-2172" in ln:
                i += 1
                continue
            if "both your business" in ln:
                i += 1
                continue
            if re.match(r"^(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+\d", ln):
                i += 1
                continue
            if re.match(r"^\s*-\s+(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)", ln):
                i += 1
                continue

            # Try to match dollar amounts
            dollar_match = re.match(r"^\$([0-9,]+\.\d{2})$", ln)
            if dollar_match:
                amount = float(dollar_match.group(1).replace(",", ""))
                if deposit_amt is None and debit_amt is None:
                    # First dollar amount — could be deposit, debit, or balance
                    # We'll determine type after seeing all amounts
                    deposit_amt = amount  # tentatively store as first amount
                elif deposit_amt is not None and debit_amt is None and resulting_balance is None:
                    # Second dollar amount — this is the resulting balance
                    resulting_balance = amount
                    i += 1
                    break
                i += 1
                continue

            # It's a description line
            desc_lines.append(ln)
            i += 1

        if deposit_amt is None:
            continue

        # Build clean description
        raw_desc = " ".join(desc_lines)
        # Clean up common noise from description
        raw_desc = re.sub(r"\s+", " ", raw_desc).strip()

        # Determine if deposit or debit based on description keywords
        desc_upper = raw_desc.upper()
        if "ACH DEPOSIT" in desc_upper or (
            "ETSY" in desc_upper and "PAYOUT" in desc_upper and "ETSY COM" not in desc_upper
        ):
            txn_type = "deposit"
        else:
            txn_type = "debit"

        # Build a short description matching the hardcoded style
        short_desc = _build_short_desc(raw_desc)

        category = auto_categorize(short_desc, txn_type)

        transactions.append({
            "date": full_date,
            "desc": short_desc,
            "amount": deposit_amt,
            "type": txn_type,
            "category": category,
            "source_file": fname,
            "raw_desc": raw_desc,
        })

    return transactions, covered_months


def _build_short_desc(raw_desc):
    """Build a short, clean description from the raw PDF text."""
    d = raw_desc.upper()

    # Etsy payouts
    if "ACH DEPOSIT ETSY" in d or ("ETSY, INC" in d and "PAYOUT" in d):
        return "ETSY PAYOUT"

    # Amazon purchases — extract the order ID suffix
    m = re.search(r"AMAZON\s*MKTPL\s+(\w+)", d)
    if m:
        return f"AMAZON MKTPL {m.group(1)}"

    # UPS Store
    if "UPS STORE" in d:
        city_m = re.search(r"UPS STORE \d+\s+(\w+)\s+(\w{2})", d)
        if city_m:
            return f"UPS STORE {city_m.group(1)} {city_m.group(2)}"
        return "UPS STORE"

    # USPS
    if "USPS" in d and "CLICKNSHIP" in d:
        return "USPS CLICKNSHIP"

    # Hobby Lobby
    if "HOBBYLOBBY" in d:
        city_m = re.search(r"HOBBYLOBBY\s+(\w+)\s+(\w{2})", d)
        if city_m:
            return f"HOBBYLOBBY {city_m.group(1)} {city_m.group(2)}"
        return "HOBBYLOBBY"

    # Walmart
    if "WAL MART" in d:
        m2 = re.search(r"WAL MART\s+\d+\s+(\w+)\s+(\w{2})", d)
        if m2:
            return f"WAL MART {m2.group(1)} {m2.group(2)}"
        return "WAL MART"

    # Westlake Hardware
    if "WESTLAKE HARDWARE" in d:
        city_m = re.search(r"WESTLAKE\s+HARDWARE\s+\d+\s+(\w+)\s+(\w{2})", d)
        if city_m:
            return f"WESTLAKE HARDWARE {city_m.group(1)} {city_m.group(2)}"
        return "WESTLAKE HARDWARE"

    # Etsy fees
    if "ETSY COM US" in d:
        return "ETSY COM US"

    # PayPal / AliExpress
    if "PAYPAL" in d and "ALIPAYUSINC" in d:
        return "PAYPAL ALIPAYUSINC"
    if "PAYPAL" in d and "AOWEIKEGTTA" in d:
        return "PAYPAL AOWEIKEGTTA"
    if "PAYPAL" in d and "THANGS" in d:
        return "PAYPAL THANGS 3D"

    # Venmo
    if "VENMO" in d:
        m2 = re.search(r"VENMO\s+([A-Z]+(?:\s+[A-Z]+)*)", d)
        if m2:
            name = m2.group(1).strip()
            # Remove trailing phone/location noise
            name = re.sub(r"\s+\d{3}\s+\d{3}\s+\d{4}.*", "", name)
            return f"VENMO {name}"
        return "VENMO"

    # Best Buy
    if "BEST BUY" in d:
        m2 = re.search(r"(THOMAS J MCNULTY)", d)
        name_part = f" {m2.group(1)}" if m2 else ""
        return f"BEST BUY AUTO PYMT{name_part}"

    # QuikTrip
    if "QT " in d and re.search(r"QT\s+\d+", d):
        city_m = re.search(r"QT\s+\d+\s+(\w+)\s+(\w{2})", d)
        if city_m:
            return f"QT {city_m.group(1)} {city_m.group(2)}"
        return "QT"

    # Various restaurants/stores — extract merchant + city
    for merchant in ["REASORS", "WILDFLOWERCAFE", "ANTHROPOLOGIE",
                     "LULULEMON", "CHIPOTLE"]:
        if merchant in d:
            city_m = re.search(merchant + r"[^A-Z]*?(\w+)\s+(\w{2})$", d)
            if not city_m:
                city_m = re.search(merchant + r".*?(\w{4,})\s+(\w{2})", d)
            if city_m:
                return f"{merchant} {city_m.group(1)} {city_m.group(2)}"
            return merchant

    # Fallback: first 50 chars
    return raw_desc[:50].strip()


# ── CSV Parsing ──────────────────────────────────────────────────────────────

def parse_bank_csv(filepath):
    """Parse a Capital One CSV transaction download into transactions.
    CSV format: Account Number, Credit, Debit, Description, Posted Date
    Note: Capital One CSVs have double-quoted descriptions with embedded commas
    (e.g. ETSY, INC.) that confuse Python's csv module, so we parse manually.
    Returns (transactions_list, covered_months_set)."""
    fname = os.path.basename(filepath)
    transactions = []
    covered_months = set()

    with open(filepath, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    if not lines:
        return transactions, covered_months

    # Skip header
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue

        # Extract Posted Date from the end: ,"MM/DD/YYYY"
        date_match = re.search(r',\s*"(\d{2}/\d{2}/\d{4})"\s*$', line)
        if not date_match:
            continue
        posted_date = date_match.group(1)

        # Remove the date portion from the end
        remainder = line[:date_match.start()]

        # Parse first 3 fields: "AcctNum","Credit","Debit"
        # Use csv to parse just these reliable fields + description
        parsed = list(csv.reader([remainder]))[0]
        if len(parsed) < 4:
            continue

        credit = parsed[1].strip()
        debit = parsed[2].strip()
        # Description is everything from field 4 onward (may have been split by commas)
        raw_desc = ",".join(parsed[3:]).strip().strip('"').strip()

        # Parse amount and type
        if credit:
            amount = float(credit.replace(",", ""))
            txn_type = "deposit"
        elif debit:
            amount = float(debit.replace(",", ""))
            txn_type = "debit"
        else:
            continue

        date_str = posted_date
        parts = date_str.split("/")
        if len(parts) != 3:
            continue
        covered_months.add(f"{parts[2]}-{parts[0]}")

        short_desc = _build_short_desc(raw_desc)
        category = auto_categorize(short_desc, txn_type)

        transactions.append({
            "date": date_str,
            "desc": short_desc,
            "amount": amount,
            "type": txn_type,
            "category": category,
            "source_file": fname,
            "raw_desc": raw_desc,
        })

    return transactions, covered_months


# ── Apply overrides ──────────────────────────────────────────────────────────

def apply_overrides(transactions):
    """Apply transaction_overrides from config (splits, recategorizations)."""
    result = []
    for t in transactions:
        matched = False
        for override in TRANSACTION_OVERRIDES:
            match = override["match"]
            # Check if this transaction matches
            desc_match = match.get("desc_contains", "")
            date_match = match.get("date", "")
            if desc_match and desc_match.upper() not in t["desc"].upper() and desc_match.upper() not in t.get("raw_desc", "").upper():
                continue
            if date_match and not t["date"].startswith(date_match):
                continue
            amt_match = match.get("amount")
            if amt_match is not None and abs(t["amount"] - amt_match) > 0.01:
                continue

            action = override["action"]
            if action == "split":
                # Replace single transaction with multiple splits
                for split in override["splits"]:
                    result.append({
                        **t,
                        "amount": split["amount"],
                        "category": split["category"],
                        "desc": t["desc"],
                    })
                matched = True
                break
            elif action == "recategorize":
                t["category"] = override["category"]
                result.append(t)
                matched = True
                break

        if not matched:
            result.append(t)
    return result


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    pdf_files = sorted(f for f in os.listdir(BANK_DIR)
                       if f.lower().endswith(".pdf"))
    csv_files = sorted(f for f in os.listdir(BANK_DIR)
                       if f.lower().endswith(".csv"))

    all_source_files = csv_files + pdf_files

    if not all_source_files:
        print("  No bank statements found in data/bank_statements/")
        output = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "source_files": [],
                "total_deposits": 0,
                "total_debits": 0,
            },
            "transactions": [],
        }
        with open(OUT_PATH, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Wrote empty bank_transactions.json")
        return

    all_transactions = []
    all_covered_months = set()

    # Parse PDFs first — official statements are the primary source
    for fname in pdf_files:
        path = os.path.join(BANK_DIR, fname)
        print(f"  Parsing PDF: {fname}...")
        txns, covered = parse_bank_pdf(path)
        print(f"    Found {len(txns)} transactions (covers {sorted(covered)})")
        all_transactions.extend(txns)
        all_covered_months.update(covered)

    # Parse ALL CSVs, combine and dedup (newer downloads are supersets of older ones)
    csv_txns = []
    csv_covered = set()
    for fname in csv_files:
        path = os.path.join(BANK_DIR, fname)
        print(f"  Parsing CSV: {fname}...")
        txns, covered = parse_bank_csv(path)
        print(f"    Found {len(txns)} transactions (covers {sorted(covered)})")
        csv_txns.extend(txns)
        csv_covered.update(covered)

    # Dedup CSV transactions by (date, amount, type, description)
    if csv_txns:
        seen = {}
        for t in csv_txns:
            key = (t["date"], t["amount"], t["type"], t.get("raw_desc", t["desc"]))
            seen[key] = t
        csv_deduped = list(seen.values())
        print(f"    CSV dedup: {len(csv_txns)} raw -> {len(csv_deduped)} unique")

        # Only use CSV data for months NOT already covered by PDFs
        new_months = csv_covered - all_covered_months
        if new_months:
            filtered = [t for t in csv_deduped
                        if f"{t['date'].split('/')[2]}-{t['date'].split('/')[0]}" in new_months]
            print(f"    Using {len(filtered)} CSV transactions for new months {sorted(new_months)}")
            all_transactions.extend(filtered)
            all_covered_months.update(new_months)
        else:
            print(f"    All CSV months already covered by PDFs — skipping")

    # Apply overrides (splits, recategorizations)
    all_transactions = apply_overrides(all_transactions)

    # Append manual transactions from config — skip any whose month is already
    # covered by a parsed statement (prevents double-counting)
    manual_added = 0
    manual_skipped = 0
    for mt in MANUAL_TRANSACTIONS:
        parts = mt["date"].split("/")
        mt_month = f"{parts[2]}-{parts[0]}"  # YYYY-MM
        if mt_month in all_covered_months:
            manual_skipped += 1
            continue
        all_transactions.append({
            "date": mt["date"],
            "desc": mt["desc"],
            "amount": mt["amount"],
            "type": mt["type"],
            "category": mt["category"],
            "source_file": "config.json (manual)",
            "raw_desc": mt["desc"],
        })
        manual_added += 1

    if manual_added:
        print(f"  Added {manual_added} manual transactions (months not yet covered)")
    if manual_skipped:
        print(f"  Skipped {manual_skipped} manual transactions (months already covered)")

    # Compute totals
    total_deposits = sum(t["amount"] for t in all_transactions if t["type"] == "deposit")
    total_debits = sum(t["amount"] for t in all_transactions if t["type"] == "debit")

    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source_files": all_source_files,
            "total_deposits": round(total_deposits, 2),
            "total_debits": round(total_debits, 2),
        },
        "transactions": all_transactions,
    }

    with open(OUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n  Wrote {len(all_transactions)} transactions to bank_transactions.json")
    print(f"  Total deposits: ${total_deposits:,.2f}")
    print(f"  Total debits:   ${total_debits:,.2f}")
    print(f"  Net:            ${total_deposits - total_debits:,.2f}")

    # Per-source breakdown
    for fname in all_source_files:
        file_txns = [t for t in all_transactions if t.get("source_file") == fname]
        if not file_txns:
            continue
        file_deps = sum(t["amount"] for t in file_txns if t["type"] == "deposit")
        file_debs = sum(t["amount"] for t in file_txns if t["type"] == "debit")
        print(f"\n  {fname}: {len(file_txns)} txns, deposits=${file_deps:,.2f}, debits=${file_debs:,.2f}")


if __name__ == "__main__":
    main()
