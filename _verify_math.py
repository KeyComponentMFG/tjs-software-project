"""Verify all dashboard math after reclassifying personal to owner draws."""

print("=" * 60)
print("  MATH VERIFICATION (after reclassification)")
print("=" * 60)

# Bank category totals (updated)
bank_by_cat = {
    "Amazon Inventory": 2039.29,
    "Shipping": 36.58,
    "Craft Supplies": 59.93,
    "Etsy Fees": 91.98,
    "Subscriptions": 40.30,
    "AliExpress Supplies": 142.11,
    "Business Credit Card": 100.00,
    # Draws now include formerly-personal items
    "Owner Draw - Tulsa": 639.40 + 50.00 + 13.05 + 30.38 + 50.00 + 20.08,  # old + Reasors + Wildflower(half) + Anthropologie + Lululemon + Chipotle
    "Owner Draw - Texas": 550.00 + 13.04,  # old + Wildflower(half)
    "Personal": 0.00,  # all moved to draws
}

bank_deposits = 5984.55
bank_debits = sum(bank_by_cat.values())
bank_balance = round(bank_deposits - bank_debits, 2)

print(f"\n  BANK:")
print(f"    Deposits:     ${bank_deposits:>10,.2f}")
print(f"    Debits:       ${bank_debits:>10,.2f}")
print(f"    Balance:      ${bank_balance:>10,.2f}")
print(f"    Expected:     $  2,108.41")
print(f"    Match: {'OK' if abs(bank_balance - 2108.41) < 0.02 else 'MISMATCH: ' + str(bank_balance)}")

print(f"\n  OWNER DRAWS:")
print(f"    Tulsa:        ${bank_by_cat['Owner Draw - Tulsa']:>10,.2f}")
print(f"    Texas:        ${bank_by_cat['Owner Draw - Texas']:>10,.2f}")
owner_draws = bank_by_cat["Owner Draw - Tulsa"] + bank_by_cat["Owner Draw - Texas"]
print(f"    Total:        ${owner_draws:>10,.2f}")

etsy_net_earned = 7997.40
etsy_balance = 1054.77
cash_on_hand = bank_balance + etsy_balance
profit = cash_on_hand + owner_draws  # no more personal
biz_expense_cats = ["Shipping", "Craft Supplies", "Etsy Fees", "Subscriptions", "AliExpress Supplies", "Business Credit Card"]
biz_expense_total = sum(bank_by_cat[c] for c in biz_expense_cats)
biz_expenses = bank_by_cat["Amazon Inventory"] + biz_expense_total
unaccounted = 941.99
etsy_gap = 16.09

print(f"\n  PROFIT:")
print(f"    Cash on hand: ${cash_on_hand:>10,.2f}")
print(f"    Owner draws:  ${owner_draws:>10,.2f}")
print(f"    PROFIT:       ${profit:>10,.2f}")

print(f"\n  ACCOUNTING EQUATION:")
print(f"    Earned:       ${etsy_net_earned:>10,.2f}")
acct_total = cash_on_hand + owner_draws + biz_expenses + unaccounted + etsy_gap
print(f"    Cash:         ${cash_on_hand:>10,.2f}")
print(f"    Draws:        ${owner_draws:>10,.2f}")
print(f"    Biz expenses: ${biz_expenses:>10,.2f}")
print(f"    Unaccounted:  ${unaccounted:>10,.2f}")
print(f"    Etsy gap:     ${etsy_gap:>10,.2f}")
print(f"    TOTAL:        ${acct_total:>10,.2f}")
gap = round(etsy_net_earned - acct_total, 2)
print(f"    GAP:          ${gap:>10,.2f}")
if abs(gap) < 0.02:
    print(f"\n  >>> EVERY PENNY ACCOUNTED FOR <<<")
else:
    print(f"\n  >>> WARNING: ${abs(gap):,.2f} UNACCOUNTED <<<")

# Verify wildflower split
wf_total = 13.05 + 13.04
print(f"\n  Wildflower split: ${13.05} + ${13.04} = ${wf_total} (was $26.09)")
print(f"  Split match: {'OK' if abs(wf_total - 26.09) < 0.02 else 'MISMATCH'}")
