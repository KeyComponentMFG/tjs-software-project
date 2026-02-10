"""
_receipt_check.py
Cross-reference bank debit transactions against inventory_orders.json
to identify which business expenses have supporting receipts/invoices.
"""

import json, os
from datetime import datetime
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Bank debits loaded from JSON sidecar file
_debits_path = os.path.join(BASE_DIR, "data", "generated", "_bank_debits.json")
with open(_debits_path, "r", encoding="utf-8") as _f:
    BANK_DEBITS = json.load(_f)

NO_RECEIPT_NEEDED = {"Etsy Fees", "Owner Draw - Tulsa", "Owner Draw - Texas",
                     "Personal", "Etsy Payout"}


def load_inventory_orders():
    path = os.path.join(BASE_DIR, "data", "generated", "inventory_orders.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_date(d):
    return datetime.strptime(d, "%m/%d/%Y")


def parse_inv_date(d):
    """Parse various date formats from inventory_orders.json."""
    for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y"]:
        try:
            return datetime.strptime(d.strip(), fmt)
        except ValueError:
            continue
    return None


def analyze_amazon_invoices(orders):
    result = []
    for order in orders:
        source = order.get("source", "").lower()
        if "amazon" in source or "keycomp" in source or "personal_amazon" in source:
            result.append(order)
    return result


def analyze_other_invoices(orders):
    result = []
    for order in orders:
        source = order.get("source", "").lower()
        if "amazon" not in source and "keycomp" not in source and "personal_amazon" not in source:
            result.append(order)
    return result


def fmt_money(val):
    sign = chr(36)
    return "{}{:,.2f}".format(sign, val)


def main():
    orders = load_inventory_orders()
    amazon_invoices = analyze_amazon_invoices(orders)
    other_invoices = analyze_other_invoices(orders)

    amazon_by_month = defaultdict(list)
    for inv in amazon_invoices:
        order_date = inv.get("order_date", inv.get("date", ""))
        if order_date:
            dt = parse_inv_date(order_date)
            if dt:
                amazon_by_month[dt.strftime("%Y-%m")].append(inv)

    no_receipt_needed = []
    has_receipt = []
    needs_receipt = []
    pending_items = []
    amazon_bank_by_month = defaultdict(list)

    for txn in BANK_DEBITS:
        cat = txn["cat"]
        dt = parse_date(txn["date"])

        if cat in NO_RECEIPT_NEEDED:
            no_receipt_needed.append(txn)
            continue
        if cat == "Pending":
            pending_items.append(txn)
            continue
        if cat == "Amazon Inventory":
            amazon_bank_by_month[dt.strftime("%Y-%m")].append(txn)
            continue
        if cat == "Shipping":
            needs_receipt.append({**txn, "receipt_status": "NEEDS RECEIPT",
                "note": "Need UPS/USPS shipping receipt or tracking confirmation"})
            continue
        if cat == "Craft Supplies":
            matched = False
            for inv in other_invoices:
                if "hobby" in inv.get("source", "").lower():
                    inv_total = float(inv.get("grand_total", inv.get("total", 0)))
                    if abs(inv_total - txn["amount"]) < 0.02:
                        has_receipt.append({**txn, "receipt_status": "HAS RECEIPT",
                            "note": "Matched: {} {}".format(inv.get("source", ""), fmt_money(inv_total))})
                        matched = True
                        break
            if not matched:
                needs_receipt.append({**txn, "receipt_status": "NEEDS RECEIPT",
                    "note": "Need Hobby Lobby receipt (check other_receipts or paper receipts)"})
            continue
        if cat == "AliExpress Supplies":
            matched = False
            for inv in other_invoices:
                if "ali" in inv.get("source", "").lower():
                    inv_total = float(inv.get("grand_total", inv.get("total", 0)))
                    if abs(inv_total - txn["amount"]) < 0.02:
                        has_receipt.append({**txn, "receipt_status": "HAS RECEIPT",
                            "note": "Matched: {} {}".format(inv.get("source", ""), fmt_money(inv_total))})
                        matched = True
                        break
            if not matched:
                needs_receipt.append({**txn, "receipt_status": "NEEDS RECEIPT",
                    "note": "Need AliExpress order confirmation or PayPal receipt"})
            continue
        if cat == "3D Subscription":
            needs_receipt.append({**txn, "receipt_status": "NEEDS RECEIPT",
                "note": "Need Thangs subscription confirmation/receipt"})
            continue
        needs_receipt.append({**txn, "receipt_status": "NEEDS RECEIPT",
            "note": "Unknown category -- need receipt"})

    # Amazon batch analysis
    all_amazon_bank = []
    for mk in sorted(set(list(amazon_bank_by_month.keys()) + list(amazon_by_month.keys()))):
        all_amazon_bank.extend(amazon_bank_by_month.get(mk, []))
    amazon_bank_total = sum(t["amount"] for t in all_amazon_bank)
    amazon_invoice_total = sum(
        float(inv.get("grand_total", inv.get("total", 0)) or 0)
        for inv in amazon_invoices
    )

    amazon_covered = []
    amazon_uncovered = []
    covered_months = set(amazon_by_month.keys())

    for mk, txns in sorted(amazon_bank_by_month.items()):
        if mk in covered_months:
            for t in txns:
                amazon_covered.append({**t, "receipt_status": "COVERED",
                    "note": "Amazon invoices exist for {} ({} invoices)".format(
                        mk, len(amazon_by_month[mk]))})
        else:
            for t in txns:
                amazon_uncovered.append({**t, "receipt_status": "NEEDS INVOICE",
                    "note": "No Amazon invoices found for {}".format(mk)})

    # ---------- PRINT RESULTS ----------
    sep = "=" * 80
    tilde = "~" * 80
    dash = "  " + "-" * 57

    print(sep)
    print("  RECEIPT CHECK REPORT")
    print("  Generated: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M")))
    print("  Total bank debits analyzed: {}".format(len(BANK_DEBITS)))
    print(sep)

    print()
    print(tilde)
    print("  NO RECEIPT NEEDED ({} transactions)".format(len(no_receipt_needed)))
    print("  These are self-documenting (Etsy fees, personal, owner draws, payouts)")
    print(tilde)
    cat_totals = defaultdict(float)
    for t in no_receipt_needed:
        cat_totals[t["cat"]] += t["amount"]
    for cat, total in sorted(cat_totals.items()):
        count = sum(1 for t in no_receipt_needed if t["cat"] == cat)
        print("  {:<25s}  {:>2d} txns  {}".format(cat, count, fmt_money(total)))
    no_receipt_total = sum(t["amount"] for t in no_receipt_needed)
    print("  {:<25s}  {:>2d} txns  {}".format("SUBTOTAL", len(no_receipt_needed), fmt_money(no_receipt_total)))

    print()
    print(tilde)
    print("  AMAZON INVENTORY -- BATCH ANALYSIS")
    print("  (Bank charges are split-shipments; will not match invoices 1:1)")
    print(tilde)
    print()
    print("  Amazon Bank Charges:")
    for mk in sorted(amazon_bank_by_month.keys()):
        txns_m = amazon_bank_by_month[mk]
        mt = sum(t["amount"] for t in txns_m)
        print("    {}:  {:>2d} charges  {:>12s}".format(mk, len(txns_m), fmt_money(mt)))
    print("    {:>7s}:  {:>2d} charges  {:>12s}".format(
        "TOTAL", len(all_amazon_bank), fmt_money(amazon_bank_total)))

    print()
    print("  Amazon Invoices on File (from inventory_orders.json):")
    for mk in sorted(amazon_by_month.keys()):
        invs = amazon_by_month[mk]
        mt = sum(float(i.get("grand_total", i.get("total", 0))) for i in invs)
        print("    {}:  {:>2d} invoices {:>12s}".format(mk, len(invs), fmt_money(mt)))
    print("    {:>7s}:  {:>2d} invoices {:>12s}".format(
        "TOTAL", len(amazon_invoices), fmt_money(amazon_invoice_total)))

    diff = amazon_bank_total - amazon_invoice_total
    pct = (amazon_invoice_total / amazon_bank_total * 100) if amazon_bank_total > 0 else 0
    print()
    print("  Coverage: {} invoiced / {} bank = {:.1f}%".format(
        fmt_money(amazon_invoice_total), fmt_money(amazon_bank_total), pct))
    if diff > 0:
        print("  Gap: {} in bank charges without matching invoice totals".format(fmt_money(diff)))
        print("  (Expected due to split-shipment charges vs whole-order invoices)")
    elif diff < 0:
        print("  Invoices exceed bank charges by {}".format(fmt_money(abs(diff))))

    missing_months = set(amazon_bank_by_month.keys()) - covered_months
    print()
    if missing_months:
        print("  WARNING: No invoices for months: {}".format(", ".join(sorted(missing_months))))
    else:
        print("  All months with Amazon bank charges have invoices on file.")

    if amazon_uncovered:
        print()
        print("  UNCOVERED Amazon charges ({}):".format(len(amazon_uncovered)))
        for t in amazon_uncovered:
            print("    {}  {:>10s}  {}".format(t["date"], fmt_money(t["amount"]), t["desc"]))
            print("             -> {}".format(t["note"]))

    print()
    print(tilde)
    print("  OTHER BUSINESS EXPENSES -- HAS RECEIPT ({} transactions)".format(len(has_receipt)))
    print(tilde)
    if has_receipt:
        for t in has_receipt:
            print("  {}  {:>10s}  {}".format(t["date"], fmt_money(t["amount"]), t["desc"]))
            print("           -> {}".format(t["note"]))
    else:
        print("  (none matched to specific invoices)")

    print()
    print(tilde)
    print("  NEEDS RECEIPT ({} transactions)".format(len(needs_receipt)))
    print(tilde)
    for t in needs_receipt:
        print("  {}  {:>10s}  {:<22s}  {}".format(
            t["date"], fmt_money(t["amount"]), t["cat"], t["desc"]))
        print("           -> {}".format(t["note"]))

    print()
    print(tilde)
    print("  *** PENDING -- NEEDS USER INPUT ({} transactions) ***".format(len(pending_items)))
    print(tilde)
    for t in pending_items:
        print("  {}  {:>10s}  {}".format(t["date"], fmt_money(t["amount"]), t["desc"]))
        print("           -> UNKNOWN CATEGORY: Need receipt to determine if business or personal")

    # Grand Summary
    print()
    print(sep)
    print("  GRAND SUMMARY")
    print(sep)

    total_all = sum(t["amount"] for t in BANK_DEBITS)
    total_amazon_covered = sum(t["amount"] for t in amazon_covered)
    total_amazon_uncovered = sum(t["amount"] for t in amazon_uncovered)
    total_has_receipt = sum(t["amount"] for t in has_receipt)
    total_needs_receipt = sum(t["amount"] for t in needs_receipt)
    total_pending = sum(t["amount"] for t in pending_items)

    total_covered = no_receipt_total + total_amazon_covered + total_has_receipt
    total_not_covered = total_amazon_uncovered + total_needs_receipt + total_pending
    count_covered = len(no_receipt_needed) + len(amazon_covered) + len(has_receipt)
    count_not_covered = len(amazon_uncovered) + len(needs_receipt) + len(pending_items)

    print()
    print("  Total transactions:     {:>4d}     {:>12s}".format(len(BANK_DEBITS), fmt_money(total_all)))
    print(dash)
    print("  No receipt needed:      {:>4d}     {:>12s}".format(len(no_receipt_needed), fmt_money(no_receipt_total)))
    print("  Amazon (invoices exist):{:>4d}     {:>12s}".format(len(amazon_covered), fmt_money(total_amazon_covered)))
    print("  Other (receipt matched):{:>4d}     {:>12s}".format(len(has_receipt), fmt_money(total_has_receipt)))
    print(dash)
    print("  COVERED:                {:>4d}     {:>12s}".format(count_covered, fmt_money(total_covered)))
    print(dash)
    print("  Amazon (NO invoices):   {:>4d}     {:>12s}".format(len(amazon_uncovered), fmt_money(total_amazon_uncovered)))
    print("  Needs receipt:          {:>4d}     {:>12s}".format(len(needs_receipt), fmt_money(total_needs_receipt)))
    print("  Pending (uncat):        {:>4d}     {:>12s}".format(len(pending_items), fmt_money(total_pending)))
    print(dash)
    print("  NOT COVERED:            {:>4d}     {:>12s}".format(count_not_covered, fmt_money(total_not_covered)))

    print()
    print("  SPECIFIC RECEIPTS STILL NEEDED:")
    print(dash)
    all_missing = needs_receipt + pending_items + amazon_uncovered
    if not all_missing:
        print("  (none -- all business expenses are covered!)")
    else:
        by_cat = defaultdict(list)
        for t in all_missing:
            by_cat[t["cat"]].append(t)
        for cat in sorted(by_cat.keys()):
            items = by_cat[cat]
            cat_total = sum(t["amount"] for t in items)
            print()
            print("  {} ({} items, {}):".format(cat, len(items), fmt_money(cat_total)))
            for t in items:
                print("    - {}  {:>10s}  {}".format(t["date"], fmt_money(t["amount"]), t["desc"]))

    print()
    print(sep)
    print("  END OF REPORT")
    print(sep)


if __name__ == "__main__":
    main()
