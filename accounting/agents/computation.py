"""
accounting/agents/computation.py — Agent 2: Derive all metrics from Journal.

Computes every financial metric the dashboard needs from the Journal.
Each metric = MetricValue with value, confidence, and provenance.

No globals. No max(0,...). No hardcoded offsets.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from ..journal import Journal
from ..models import Confidence, MetricValue, Provenance, TxnSource, TxnType


_CONFIDENCE_ORDER = [
    Confidence.QUARANTINED,
    Confidence.NEEDS_REVIEW,
    Confidence.UNKNOWN,
    Confidence.HEURISTIC,
    Confidence.PROJECTION,
    Confidence.ESTIMATED,
    Confidence.PARTIAL,
    Confidence.DERIVED,
    Confidence.VERIFIED,
]


def _min_confidence(*confidences: Confidence) -> Confidence:
    """Return the lowest confidence among inputs."""
    min_idx = len(_CONFIDENCE_ORDER) - 1
    for c in confidences:
        try:
            idx = _CONFIDENCE_ORDER.index(c)
        except ValueError:
            idx = 0
        min_idx = min(min_idx, idx)
    return _CONFIDENCE_ORDER[min_idx]


def _ids(entries: list) -> tuple[str, ...]:
    """Extract dedup_hash tuple from a list of JournalEntry objects."""
    return tuple(e.dedup_hash for e in entries)


def _metric(name: str, value: Decimal, confidence: Confidence,
            formula: str, source_entries: int = 0,
            source_types: Optional[list[str]] = None,
            notes: str = "",
            display_format: str = "money",
            entry_ids: tuple[str, ...] = (),
            missing_inputs: tuple[str, ...] = (),
            requires_sources: tuple[str, ...] = ()) -> MetricValue:
    """Convenience constructor for MetricValue with full provenance."""
    return MetricValue(
        name=name,
        value=value,
        confidence=confidence,
        provenance=Provenance(
            formula=formula,
            source_entries=source_entries,
            source_types=source_types or [],
            notes=notes,
            source_entry_ids=entry_ids,
            missing_inputs=missing_inputs,
            requires_sources=requires_sources,
        ),
        display_format=display_format,
    )


class ComputationAgent:
    """Derives all financial metrics from a Journal."""

    def compute_all(self, journal: Journal,
                    etsy_balance: Decimal,
                    etsy_balance_confidence: Confidence,
                    pre_capone_deposits: Decimal,
                    strict_mode: bool = False) -> dict[str, MetricValue]:
        """Compute every metric from the journal.

        Parameters:
            journal: The populated Journal
            etsy_balance: Pre-computed Etsy balance (from ingestion agent)
            etsy_balance_confidence: Confidence of the balance computation
            pre_capone_deposits: Total pre-CapOne deposits from config
            strict_mode: If True, ESTIMATED metrics become UNKNOWN

        Returns:
            dict mapping metric name → MetricValue
        """
        m: dict[str, MetricValue] = {}
        self._strict_mode = strict_mode

        # ── Etsy Core Metrics ──
        sales = journal.by_type(TxnType.SALE)
        fees = journal.by_type(TxnType.FEE)
        shipping = journal.by_type(TxnType.SHIPPING)
        marketing = journal.by_type(TxnType.MARKETING)
        refunds = journal.by_type(TxnType.REFUND)
        taxes = journal.by_type(TxnType.TAX)
        deposits = journal.by_type(TxnType.DEPOSIT)
        buyer_fees = journal.by_type(TxnType.BUYER_FEE)
        payments = journal.by_type(TxnType.PAYMENT)

        gross_sales = journal.sum_amount(sales)
        # Use abs(SUM), not SUM(abs) — credits within fee rows must cancel charges
        total_refunds = abs(journal.sum_amount(refunds))
        net_sales = gross_sales - total_refunds
        total_fees = abs(journal.sum_amount(fees))
        total_shipping_cost = abs(journal.sum_amount(shipping))
        total_marketing = abs(journal.sum_amount(marketing))
        total_taxes = abs(journal.sum_amount(taxes))
        total_payments = journal.sum_amount(payments)
        total_buyer_fees = abs(journal.sum_amount(buyer_fees))
        order_count = len(sales)
        avg_order = gross_sales / order_count if order_count else Decimal("0")

        m["gross_sales"] = _metric("gross_sales", gross_sales, Confidence.VERIFIED,
                                    "SUM(Sale.Net_Clean)", len(sales), ["Sale"],
                                    entry_ids=_ids(sales), requires_sources=("ETSY_CSV",))
        m["total_refunds"] = _metric("total_refunds", total_refunds, Confidence.VERIFIED,
                                      "ABS(SUM(Refund.Net_Clean))", len(refunds), ["Refund"],
                                      entry_ids=_ids(refunds), requires_sources=("ETSY_CSV",))
        m["net_sales"] = _metric("net_sales", net_sales, Confidence.DERIVED,
                                  "gross_sales - total_refunds", len(sales) + len(refunds),
                                  ["Sale", "Refund"], entry_ids=_ids(sales) + _ids(refunds))
        m["total_fees"] = _metric("total_fees", total_fees, Confidence.VERIFIED,
                                   "ABS(SUM(Fee.Net_Clean))", len(fees), ["Fee"],
                                   entry_ids=_ids(fees), requires_sources=("ETSY_CSV",))
        m["total_shipping_cost"] = _metric("total_shipping_cost", total_shipping_cost,
                                            Confidence.VERIFIED,
                                            "ABS(SUM(Shipping.Net_Clean))", len(shipping), ["Shipping"],
                                            entry_ids=_ids(shipping), requires_sources=("ETSY_CSV",))
        m["total_marketing"] = _metric("total_marketing", total_marketing, Confidence.VERIFIED,
                                        "ABS(SUM(Marketing.Net_Clean))", len(marketing), ["Marketing"],
                                        entry_ids=_ids(marketing), requires_sources=("ETSY_CSV",))
        m["total_taxes"] = _metric("total_taxes", total_taxes, Confidence.VERIFIED,
                                    "ABS(SUM(Tax.Net_Clean))", len(taxes), ["Tax"],
                                    entry_ids=_ids(taxes), requires_sources=("ETSY_CSV",))
        m["total_payments"] = _metric("total_payments", total_payments, Confidence.VERIFIED,
                                       "SUM(Payment.Net_Clean)", len(payments), ["Payment"],
                                       entry_ids=_ids(payments), requires_sources=("ETSY_CSV",))
        m["total_buyer_fees"] = _metric("total_buyer_fees", total_buyer_fees, Confidence.VERIFIED,
                                         "ABS(SUM(BuyerFee.Net_Clean))", len(buyer_fees), ["Buyer Fee"],
                                         entry_ids=_ids(buyer_fees), requires_sources=("ETSY_CSV",))
        m["order_count"] = _metric("order_count", Decimal(str(order_count)), Confidence.VERIFIED,
                                    "COUNT(Sale)", len(sales), ["Sale"], display_format="count",
                                    entry_ids=_ids(sales))
        m["avg_order"] = _metric("avg_order", avg_order, Confidence.DERIVED,
                                  "gross_sales / order_count", len(sales), ["Sale"],
                                  entry_ids=_ids(sales))

        # ── Etsy Accounting ──
        etsy_net_earned = (gross_sales - total_fees - total_shipping_cost
                          - total_marketing - total_refunds - total_taxes
                          - total_buyer_fees + total_payments)

        m["etsy_net_earned"] = _metric("etsy_net_earned", etsy_net_earned, Confidence.DERIVED,
                                        "gross_sales - fees - shipping - marketing - refunds - taxes - buyer_fees + payments",
                                        len(journal.etsy_entries()), ["Sale", "Fee", "Shipping", "Marketing", "Refund", "Tax", "Buyer Fee", "Payment"])
        m["etsy_net"] = m["etsy_net_earned"]  # Alias

        etsy_net_margin = (etsy_net_earned / gross_sales * 100) if gross_sales else Decimal("0")
        m["etsy_net_margin"] = _metric("etsy_net_margin", etsy_net_margin, Confidence.DERIVED,
                                        "etsy_net / gross_sales * 100", 0, display_format="percent")

        m["etsy_balance"] = _metric("etsy_balance", etsy_balance, etsy_balance_confidence,
                                     "total_etsy_net - csv_deposits (auto-calculated)",
                                     len(deposits),
                                     ["Deposit"],
                                     notes="No hardcoded offset — computed from deposit titles")

        _est_conf = Confidence.UNKNOWN if strict_mode else Confidence.ESTIMATED
        _est_val = Decimal("0") if strict_mode else pre_capone_deposits
        m["etsy_pre_capone_deposits"] = _metric("etsy_pre_capone_deposits", _est_val,
                                                  _est_conf,
                                                  "SUM(config.pre_capone_detail)", 0,
                                                  notes="From config.json pre_capone_detail",
                                                  missing_inputs=("etsy_payment_account_export",) if strict_mode else ())

        # Bank deposits on Etsy's side (exclude pre-CapOne to avoid double-counting
        # since pre_capone_deposits is added separately below)
        bank_dep_entries = [e for e in journal.bank_deposits()
                            if e.source != TxnSource.CONFIG_PRE_CAPONE]
        bank_total_deposits = journal.sum_abs_amount(bank_dep_entries)

        if strict_mode:
            etsy_total_deposited = Decimal("0")
            m["etsy_total_deposited"] = _metric("etsy_total_deposited", Decimal("0"),
                                                 Confidence.UNKNOWN,
                                                 "pre_capone_deposits + bank_total_deposits",
                                                 len(bank_dep_entries),
                                                 notes="STRICT: pre-CapOne deposits not source-backed",
                                                 missing_inputs=("etsy_payment_account_export",))
            m["etsy_balance_calculated"] = _metric("etsy_balance_calculated", Decimal("0"),
                                                    Confidence.UNKNOWN,
                                                    "etsy_net_earned - etsy_total_deposited", 0,
                                                    missing_inputs=("etsy_payment_account_export",))
            m["etsy_csv_gap"] = _metric("etsy_csv_gap", Decimal("0"), Confidence.UNKNOWN,
                                         "etsy_balance_calculated - etsy_balance", 0,
                                         missing_inputs=("etsy_payment_account_export",))
        else:
            etsy_total_deposited = pre_capone_deposits + bank_total_deposits
            m["etsy_total_deposited"] = _metric("etsy_total_deposited", etsy_total_deposited,
                                                 Confidence.ESTIMATED,
                                                 "pre_capone_deposits + bank_total_deposits",
                                                 len(bank_dep_entries),
                                                 notes="Includes estimated pre-CapOne deposits")
            etsy_balance_calculated = etsy_net_earned - etsy_total_deposited
            m["etsy_balance_calculated"] = _metric("etsy_balance_calculated", etsy_balance_calculated,
                                                    Confidence.ESTIMATED,
                                                    "etsy_net_earned - etsy_total_deposited", 0)
            etsy_csv_gap = etsy_balance_calculated - etsy_balance
            m["etsy_csv_gap"] = _metric("etsy_csv_gap", etsy_csv_gap, Confidence.ESTIMATED,
                                         "etsy_balance_calculated - etsy_balance", 0,
                                         notes="Gap between formula-derived and auto-calc balance")

        # ── Bank Metrics ──
        bank_deb_entries = journal.bank_debits()
        bank_total_debits = journal.sum_abs_amount(bank_deb_entries)
        bank_net_cash = bank_total_deposits - bank_total_debits

        m["bank_total_deposits"] = _metric("bank_total_deposits", bank_total_deposits,
                                            Confidence.VERIFIED,
                                            "SUM(bank_deposit.amount)", len(bank_dep_entries),
                                            ["bank_deposit"],
                                            entry_ids=_ids(bank_dep_entries),
                                            requires_sources=("BANK_PDF", "BANK_CSV"))
        m["bank_total_debits"] = _metric("bank_total_debits", bank_total_debits,
                                          Confidence.VERIFIED,
                                          "SUM(bank_debit.amount)", len(bank_deb_entries),
                                          ["bank_debit"],
                                          entry_ids=_ids(bank_deb_entries),
                                          requires_sources=("BANK_PDF", "BANK_CSV"))
        m["bank_net_cash"] = _metric("bank_net_cash", bank_net_cash, Confidence.DERIVED,
                                      "bank_total_deposits - bank_total_debits",
                                      len(bank_dep_entries) + len(bank_deb_entries),
                                      ["bank_deposit", "bank_debit"],
                                      entry_ids=_ids(bank_dep_entries) + _ids(bank_deb_entries))

        # Bank by-category (debits only — using absolute amounts from raw_row)
        bank_by_cat: dict[str, Decimal] = {}
        for e in bank_deb_entries:
            cat = e.category
            bank_by_cat[cat] = bank_by_cat.get(cat, Decimal("0")) + abs(e.amount)
        # Sort by value descending
        bank_by_cat = dict(sorted(bank_by_cat.items(), key=lambda x: -x[1]))

        # Store the full by-cat dict as a special metric (value = total debits for reference)
        m["bank_by_cat"] = _metric("bank_by_cat", bank_total_debits, Confidence.VERIFIED,
                                    "GROUP_BY(bank_debit.category, SUM(amount))",
                                    len(bank_deb_entries), ["bank_debit"])
        # Attach the dict to provenance notes for compat
        m["bank_by_cat"].provenance.notes = str(dict(bank_by_cat))

        # Tax-deductible categories
        TAX_DEDUCTIBLE = {"Amazon Inventory", "Shipping", "Craft Supplies", "Etsy Fees",
                           "Subscriptions", "AliExpress Supplies", "Business Credit Card"}
        bank_tax_deductible = sum((v for k, v in bank_by_cat.items() if k in TAX_DEDUCTIBLE),
                                  Decimal("0"))
        m["bank_tax_deductible"] = _metric("bank_tax_deductible", bank_tax_deductible,
                                            Confidence.DERIVED,
                                            "SUM(debit.amount WHERE category IN TAX_DEDUCTIBLE)", 0)

        bank_personal = bank_by_cat.get("Personal", Decimal("0"))
        bank_pending = bank_by_cat.get("Pending", Decimal("0"))
        m["bank_personal"] = _metric("bank_personal", bank_personal, Confidence.VERIFIED,
                                      "SUM(debit.amount WHERE category='Personal')", 0)
        m["bank_pending"] = _metric("bank_pending", bank_pending, Confidence.VERIFIED,
                                     "SUM(debit.amount WHERE category='Pending')", 0)

        # Business expenses
        biz_cats = ["Shipping", "Craft Supplies", "Etsy Fees", "Subscriptions",
                     "AliExpress Supplies", "Business Credit Card"]
        bank_biz_expense_total = sum((bank_by_cat.get(c, Decimal("0")) for c in biz_cats),
                                      Decimal("0"))
        amazon_inv = bank_by_cat.get("Amazon Inventory", Decimal("0"))
        bank_all_expenses = amazon_inv + bank_biz_expense_total

        m["bank_biz_expense_total"] = _metric("bank_biz_expense_total", bank_biz_expense_total,
                                               Confidence.DERIVED,
                                               "SUM(biz_expense_categories)", 0)
        m["bank_all_expenses"] = _metric("bank_all_expenses", bank_all_expenses, Confidence.DERIVED,
                                          "amazon_inventory + biz_expenses", 0)

        # ── Real Profit (the key metric) ──
        bank_cash_on_hand = bank_net_cash + etsy_balance
        bank_owner_draw_total = sum(
            (v for k, v in bank_by_cat.items() if k.startswith("Owner Draw")),
            Decimal("0")
        )
        # Cash you HAVE + cash you TOOK = real profit
        real_profit = bank_cash_on_hand + bank_owner_draw_total
        real_profit_margin = (real_profit / gross_sales * 100) if gross_sales else Decimal("0")

        # ── Draw Settlement (computed before profit metrics that reference draws) ──
        tulsa_draws = [e for e in bank_deb_entries if e.category == "Owner Draw - Tulsa"]
        texas_draws = [e for e in bank_deb_entries if e.category == "Owner Draw - Texas"]
        tulsa_draw_total = sum((abs(e.amount) for e in tulsa_draws), Decimal("0"))
        texas_draw_total = sum((abs(e.amount) for e in texas_draws), Decimal("0"))

        profit_conf = _min_confidence(Confidence.DERIVED, etsy_balance_confidence)
        m["bank_cash_on_hand"] = _metric("bank_cash_on_hand", bank_cash_on_hand, profit_conf,
                                          "bank_net_cash + etsy_balance", 0,
                                          notes=f"bank_net_cash=${bank_net_cash:.2f} + etsy_balance=${etsy_balance:.2f}")
        all_draws = tulsa_draws + texas_draws
        m["bank_owner_draw_total"] = _metric("bank_owner_draw_total", bank_owner_draw_total,
                                              Confidence.VERIFIED,
                                              "SUM(debit.amount WHERE category LIKE 'Owner Draw%')", 0,
                                              entry_ids=_ids(all_draws))
        m["real_profit"] = _metric("real_profit", real_profit, profit_conf,
                                    "bank_cash_on_hand + bank_owner_draw_total", 0,
                                    notes=f"cash=${bank_cash_on_hand:.2f} + draws=${bank_owner_draw_total:.2f}")
        m["real_profit_margin"] = _metric("real_profit_margin", real_profit_margin, profit_conf,
                                           "real_profit / gross_sales * 100", 0, display_format="percent")
        draw_diff = abs(tulsa_draw_total - texas_draw_total)
        draw_owed_to = "Braden" if tulsa_draw_total > texas_draw_total else "TJ"

        m["tulsa_draw_total"] = _metric("tulsa_draw_total", tulsa_draw_total, Confidence.VERIFIED,
                                         "SUM(Owner Draw - Tulsa)", len(tulsa_draws),
                                         entry_ids=_ids(tulsa_draws))
        m["texas_draw_total"] = _metric("texas_draw_total", texas_draw_total, Confidence.VERIFIED,
                                         "SUM(Owner Draw - Texas)", len(texas_draws),
                                         entry_ids=_ids(texas_draws))
        m["draw_diff"] = _metric("draw_diff", draw_diff, Confidence.DERIVED,
                                  "ABS(tulsa_draw - texas_draw)", 0)

        # ── Fee Breakdown (exclude credit entries from charge totals) ──
        listing_fees = journal.sum_abs_amount(
            [e for e in journal.title_contains(TxnType.FEE, "Listing fee")
             if "credit" not in e.title.lower()])
        transaction_fees_product = journal.sum_abs_amount(
            [e for e in journal.title_contains(TxnType.FEE, "Transaction fee:")
             if "shipping" not in e.title.lower() and "credit" not in e.title.lower()])
        transaction_fees_shipping = journal.sum_abs_amount(
            [e for e in journal.title_contains(TxnType.FEE, "Transaction fee: Shipping")
             if "credit" not in e.title.lower()])
        processing_fees = journal.sum_abs_amount(
            [e for e in journal.title_contains(TxnType.FEE, "Processing fee")
             if "credit" not in e.title.lower()])

        credit_transaction = journal.sum_amount(
            journal.title_contains(TxnType.FEE, "Credit for transaction fee"))
        credit_listing = journal.sum_amount(
            journal.title_contains(TxnType.FEE, "Credit for listing fee"))
        credit_processing = journal.sum_amount(
            journal.title_contains(TxnType.FEE, "Credit for processing fee"))
        share_save = journal.sum_amount(
            journal.title_contains(TxnType.FEE, "Share & Save"))
        total_credits = credit_transaction + credit_listing + credit_processing + share_save
        total_fees_gross = listing_fees + transaction_fees_product + transaction_fees_shipping + processing_fees

        m["listing_fees"] = _metric("listing_fees", listing_fees, Confidence.VERIFIED,
                                     "ABS(SUM(Fee WHERE 'Listing fee'))", 0, ["Fee"])
        m["transaction_fees_product"] = _metric("transaction_fees_product", transaction_fees_product,
                                                 Confidence.VERIFIED,
                                                 "ABS(SUM(Fee WHERE 'Transaction fee:' NOT Shipping))", 0, ["Fee"])
        m["transaction_fees_shipping"] = _metric("transaction_fees_shipping", transaction_fees_shipping,
                                                  Confidence.VERIFIED,
                                                  "ABS(SUM(Fee WHERE 'Transaction fee: Shipping'))", 0, ["Fee"])
        m["processing_fees"] = _metric("processing_fees", processing_fees, Confidence.VERIFIED,
                                        "ABS(SUM(Fee WHERE 'Processing fee'))", 0, ["Fee"])
        m["credit_transaction"] = _metric("credit_transaction", credit_transaction, Confidence.VERIFIED,
                                           "SUM(Fee WHERE 'Credit for transaction fee')", 0, ["Fee"])
        m["credit_listing"] = _metric("credit_listing", credit_listing, Confidence.VERIFIED,
                                       "SUM(Fee WHERE 'Credit for listing fee')", 0, ["Fee"])
        m["credit_processing"] = _metric("credit_processing", credit_processing, Confidence.VERIFIED,
                                          "SUM(Fee WHERE 'Credit for processing fee')", 0, ["Fee"])
        m["share_save"] = _metric("share_save", share_save, Confidence.VERIFIED,
                                   "SUM(Fee WHERE 'Share & Save')", 0, ["Fee"])
        m["total_credits"] = _metric("total_credits", total_credits, Confidence.DERIVED,
                                      "credit_transaction + credit_listing + credit_processing + share_save", 0)
        m["total_fees_gross"] = _metric("total_fees_gross", total_fees_gross, Confidence.DERIVED,
                                         "listing + transaction_product + transaction_shipping + processing", 0)

        # ── Marketing Breakdown ──
        etsy_ads = journal.sum_abs_amount(
            journal.title_contains(TxnType.MARKETING, "Etsy Ads"))
        offsite_ads_fees = journal.sum_abs_amount(
            [e for e in journal.title_contains(TxnType.MARKETING, "Offsite Ads")
             if "credit" not in e.title.lower()])
        offsite_ads_credits = journal.sum_amount(
            journal.title_contains(TxnType.MARKETING, "Credit for Offsite"))

        m["etsy_ads"] = _metric("etsy_ads", etsy_ads, Confidence.VERIFIED,
                                 "ABS(SUM(Marketing WHERE 'Etsy Ads'))", 0, ["Marketing"])
        m["offsite_ads_fees"] = _metric("offsite_ads_fees", offsite_ads_fees, Confidence.VERIFIED,
                                         "ABS(SUM(Marketing WHERE 'Offsite Ads' NOT Credit))", 0, ["Marketing"])
        m["offsite_ads_credits"] = _metric("offsite_ads_credits", offsite_ads_credits,
                                            Confidence.VERIFIED,
                                            "SUM(Marketing WHERE 'Credit for Offsite')", 0, ["Marketing"])

        # ── Shipping Breakdown ──
        # Exact match only (not return labels)
        usps_outbound = [e for e in journal.by_type(TxnType.SHIPPING)
                         if e.title == "USPS shipping label"]
        usps_return = [e for e in journal.by_type(TxnType.SHIPPING)
                       if e.title == "USPS return shipping label"]
        asendia = journal.title_contains(TxnType.SHIPPING, "Asendia")
        ship_adj = journal.title_contains(TxnType.SHIPPING, "Adjustment")
        ship_cred = journal.title_contains(TxnType.SHIPPING, "Credit for")
        ship_ins = [e for e in journal.by_type(TxnType.SHIPPING)
                    if "insurance" in e.title.lower()]

        m["usps_outbound"] = _metric("usps_outbound", journal.sum_abs_amount(usps_outbound),
                                      Confidence.VERIFIED, "ABS(SUM(Shipping WHERE 'USPS shipping label'))",
                                      len(usps_outbound), ["Shipping"])
        m["usps_outbound_count"] = _metric("usps_outbound_count", Decimal(str(len(usps_outbound))),
                                            Confidence.VERIFIED, "COUNT(USPS outbound)", 0,
                                            display_format="count")
        m["usps_return"] = _metric("usps_return", journal.sum_abs_amount(usps_return),
                                    Confidence.VERIFIED, "ABS(SUM(USPS return labels))",
                                    len(usps_return), ["Shipping"])
        m["usps_return_count"] = _metric("usps_return_count", Decimal(str(len(usps_return))),
                                          Confidence.VERIFIED, "COUNT(USPS return)", 0,
                                          display_format="count")
        m["asendia_labels"] = _metric("asendia_labels", journal.sum_abs_amount(asendia),
                                       Confidence.VERIFIED, "ABS(SUM(Asendia))", len(asendia))
        m["asendia_count"] = _metric("asendia_count", Decimal(str(len(asendia))),
                                      Confidence.VERIFIED, "COUNT(Asendia)", 0, display_format="count")
        m["ship_adjustments"] = _metric("ship_adjustments", journal.sum_abs_amount(ship_adj),
                                         Confidence.VERIFIED, "ABS(SUM(Adjustment))", len(ship_adj))
        m["ship_adjust_count"] = _metric("ship_adjust_count", Decimal(str(len(ship_adj))),
                                          Confidence.VERIFIED, "COUNT(Adjustment)", 0, display_format="count")
        m["ship_credits"] = _metric("ship_credits", journal.sum_amount(ship_cred),
                                     Confidence.VERIFIED, "SUM(Credit for shipping)", len(ship_cred))
        m["ship_credit_count"] = _metric("ship_credit_count", Decimal(str(len(ship_cred))),
                                          Confidence.VERIFIED, "COUNT(Credit for)", 0, display_format="count")
        m["ship_insurance"] = _metric("ship_insurance", journal.sum_abs_amount(ship_ins),
                                       Confidence.VERIFIED, "ABS(SUM(insurance))", len(ship_ins))
        m["ship_insurance_count"] = _metric("ship_insurance_count", Decimal(str(len(ship_ins))),
                                             Confidence.VERIFIED, "COUNT(insurance)", 0, display_format="count")

        # Buyer-paid shipping: UNKNOWN — requires Etsy order-level CSV with
        # "Shipping charged to buyer" column, or Etsy API receipts endpoint.
        # Previous /0.065 back-solve REMOVED (violated no-estimates rule).
        m["buyer_paid_shipping"] = _metric("buyer_paid_shipping", Decimal("0"), Confidence.UNKNOWN,
                                            "REMOVED: was transaction_fees_shipping / 0.065", 0,
                                            notes="Missing data: Etsy order CSV 'Shipping charged to buyer' column")
        m["shipping_profit"] = _metric("shipping_profit", Decimal("0"), Confidence.UNKNOWN,
                                        "REMOVED: depended on buyer_paid_shipping back-solve", 0,
                                        notes="Missing data: buyer_paid_shipping from order-level data")
        m["shipping_margin"] = _metric("shipping_margin", Decimal("0"), Confidence.UNKNOWN,
                                        "REMOVED: depended on buyer_paid_shipping back-solve", 0,
                                        display_format="percent",
                                        notes="Missing data: buyer_paid_shipping from order-level data")

        # Paid vs free shipping counts
        ship_fee_entries = journal.title_contains(TxnType.FEE, "Transaction fee: Shipping")
        orders_with_paid_shipping = {e.info for e in ship_fee_entries if e.info}
        all_order_ids = set()
        for e in sales:
            import re
            match = re.search(r'(Order #\d+)', e.title)
            if match:
                all_order_ids.add(match.group(1))
        orders_free_shipping = all_order_ids - orders_with_paid_shipping
        paid_ship_count = len(orders_with_paid_shipping & all_order_ids)
        free_ship_count = len(orders_free_shipping)

        m["paid_ship_count"] = _metric("paid_ship_count", Decimal(str(paid_ship_count)),
                                        Confidence.VERIFIED, "COUNT(orders with shipping fee)", 0,
                                        display_format="count")
        m["free_ship_count"] = _metric("free_ship_count", Decimal(str(free_ship_count)),
                                        Confidence.VERIFIED, "COUNT(orders without shipping fee)", 0,
                                        display_format="count")

        usps_out_val = m["usps_outbound"].value
        usps_out_cnt = int(m["usps_outbound_count"].value)
        avg_label = usps_out_val / usps_out_cnt if usps_out_cnt else Decimal("0")
        m["avg_outbound_label"] = _metric("avg_outbound_label", avg_label, Confidence.DERIVED,
                                           "usps_outbound / usps_outbound_count", 0)

        # ── Old bank reconciliation ──
        _unacct_conf = Confidence.UNKNOWN if strict_mode else Confidence.ESTIMATED
        m["bank_unaccounted"] = _metric("bank_unaccounted", Decimal("0"), _unacct_conf,
                                         "Replaced by reconciliation agent", 0,
                                         notes="See agents/reconciliation.py for proper deposit matching")

        # Store bank_by_cat as accessible data for compat shim
        self._bank_by_cat = bank_by_cat
        self._bank_monthly = self._compute_bank_monthly(journal)
        self._draw_owed_to = draw_owed_to
        self._tulsa_draws = tulsa_draws
        self._texas_draws = texas_draws

        return m

    def _compute_bank_monthly(self, journal: Journal) -> dict[str, dict[str, Decimal]]:
        """Compute monthly bank aggregates: {month: {deposits: X, debits: Y}}."""
        monthly: dict[str, dict[str, Decimal]] = {}
        for e in journal.bank_deposits() + journal.bank_debits():
            mk = e.month
            if mk not in monthly:
                monthly[mk] = {"deposits": Decimal("0"), "debits": Decimal("0")}
            if e.txn_type == TxnType.BANK_DEPOSIT:
                monthly[mk]["deposits"] += abs(e.amount)
            else:
                monthly[mk]["debits"] += abs(e.amount)
        return monthly
