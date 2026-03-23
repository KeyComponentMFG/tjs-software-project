"""Tax Forms tab — Balance Sheet, P&L, Form 1065, K-1s, SE Tax, Est. Payments."""

from dash import html
from dashboard_utils.theme import *


def build_tab5_tax_forms():
    """Tab 5 - Tax Forms: Balance Sheet, P&L, Form 1065, K-1s, SE Tax, Est. Payments"""
    # Lazy import to avoid circular dependency during bridge phase
    import etsy_dashboard as ed

    # Pull globals from the monolith
    strict_mode = ed.strict_mode
    TAX_YEARS = ed.TAX_YEARS
    tulsa_draws = ed.tulsa_draws
    texas_draws = ed.texas_draws
    bb_cc_asset_value = ed.bb_cc_asset_value
    bb_cc_balance = ed.bb_cc_balance
    _strict_banner = ed._strict_banner
    _compute_income_tax = ed._compute_income_tax

    # ── Local helpers ──

    def yr_header(year):
        period = "Oct - Dec (partial year)" if year == 2025 else "Jan - Feb (YTD)"
        return html.H4(f"TAX YEAR {year}  —  {period}",
                        style={"color": CYAN, "margin": "16px 0 8px 0", "fontSize": "14px",
                               "borderBottom": f"1px solid {CYAN}44", "paddingBottom": "4px"})

    def bs_row(label, beg, end, indent=0, bold=False):
        """Balance-sheet row with Beginning / End columns."""
        style = {
            "display": "flex", "padding": "3px 0", "borderBottom": "1px solid #ffffff10",
            "marginLeft": f"{indent * 20}px",
        }
        if bold:
            style["fontWeight"] = "bold"
            style["borderBottom"] = "2px solid #ffffff30"
            style["padding"] = "6px 0"
        return html.Div([
            html.Span(label, style={"flex": "2", "color": WHITE, "fontSize": "13px"}),
            html.Span(money(beg), style={"flex": "1", "textAlign": "right", "fontFamily": "monospace",
                       "fontSize": "13px", "color": GREEN if beg >= 0 else RED}),
            html.Span(money(end), style={"flex": "1", "textAlign": "right", "fontFamily": "monospace",
                       "fontSize": "13px", "color": GREEN if end >= 0 else RED}),
        ], style=style)

    def form_row(line_num, label, amount, indent=0, bold=False, color=WHITE):
        """IRS form line item row."""
        style = {
            "display": "flex", "justifyContent": "space-between",
            "padding": "4px 0", "borderBottom": "1px solid #ffffff10",
            "marginLeft": f"{indent * 20}px",
        }
        if bold:
            style["fontWeight"] = "bold"
            style["borderBottom"] = "2px solid #ffffff30"
            style["padding"] = "8px 0"
        disp_color = RED if amount < 0 else color
        prefix = f"Line {line_num}: " if line_num else ""
        return html.Div([
            html.Span(f"{prefix}{label}", style={"color": color if not bold else disp_color, "fontSize": "13px"}),
            html.Span(money(amount), style={"color": disp_color, "fontFamily": "monospace", "fontSize": "13px"}),
        ], style=style)

    def col_header():
        """Column headers for balance sheet."""
        return html.Div([
            html.Div("", style={"flex": "2"}),
            html.Div("Beginning of Year", style={"flex": "1", "textAlign": "right", "color": GRAY,
                      "fontSize": "11px", "fontWeight": "bold"}),
            html.Div("End of Year", style={"flex": "1", "textAlign": "right", "color": GRAY,
                      "fontSize": "11px", "fontWeight": "bold"}),
        ], style={"display": "flex", "padding": "4px 0", "borderBottom": f"2px solid {ORANGE}44"})

    def divider(color=ORANGE):
        return html.Div(style={"borderTop": f"2px solid {color}44", "margin": "6px 0"})

    _sm = strict_mode if isinstance(strict_mode, bool) else False
    children = []

    # Strict mode banner
    if _sm:
        children.append(_strict_banner("Income tax estimates use progressive brackets and assumptions. "
                                       "Estimated Tax Summary section is hidden. SE tax (derived from net income) still shown."))

    # ── Compute per-partner totals for summary bubbles ──
    _tj_total_tax = 0
    _br_total_tax = 0
    for _yr in (2025, 2026):
        _d = TAX_YEARS[_yr]
        _gp = _d["gross_sales"] - _d["refunds"] - _d["cogs"]
        _td = (_d["net_fees"] + _d["shipping"] + _d["marketing"]
               + _d.get("bank_additional_expense", 0) + _d["taxes_collected"] + _d["buyer_fees"])
        _oi = _gp - _td
        _ps = _oi / 2
        _nse = _ps * 0.9235
        _ssb = 168600 if _yr == 2025 else 176100
        _se = min(_nse, _ssb) * 0.124 + _nse * 0.029
        _eit = _compute_income_tax(max(0, _ps - _se / 2))
        _tj_total_tax += _se + _eit
        _br_total_tax += _se + _eit

    _tj_draws_all = sum(t["amount"] for t in tulsa_draws)
    _br_draws_all = sum(t["amount"] for t in texas_draws)

    children.append(html.H3("TAX LIABILITY SUMMARY",
                            style={"color": CYAN, "margin": "0 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("ESTIMATE: Total tax owed per partner (2025 + 2026 YTD combined). "
                           "Includes self-employment tax + estimated income tax (progressive federal brackets). "
                           "Assumes single filer, no other income, standard deduction. Use actual 1040/tax software for accuracy.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))
    children.append(html.Div([
        kpi_card("TJ OWES (tax)", money(_tj_total_tax), RED, f"Draws taken: {money(_tj_draws_all)}",
                 f"TJ's estimated tax liability: self-employment tax (Social Security 12.4% + Medicare 2.9% on 92.35% of net earnings) plus estimated income tax (progressive federal brackets on partnership share minus half of SE tax). Draws taken: {money(_tj_draws_all)}. Draws are NOT taxable -- they're just advances against your partnership share."),
        kpi_card("BRADEN OWES (tax)", money(_br_total_tax), RED, f"Draws taken: {money(_br_draws_all)}",
                 f"Braden's estimated tax liability: same calculation as TJ (50/50 partnership). SE tax + income tax on his share of net income. Draws taken: {money(_br_draws_all)}. Tax is owed on partnership income regardless of draws taken."),
        kpi_card("COMBINED TAX", money(_tj_total_tax + _br_total_tax), ORANGE, "Both partners total",
                 f"Total tax owed by both partners combined across 2025 + 2026 YTD. This includes self-employment tax and estimated income tax. The partnership itself (LLC) doesn't pay taxes -- it passes through to partners via K-1 forms."),
        kpi_card("PER PARTNER", money(_tj_total_tax), CYAN, "50/50 split — identical",
                 f"Since it's a 50/50 LLC, each partner owes the same tax on their share. This should be paid quarterly via IRS Form 1040-ES to avoid underpayment penalties."),
    ], style={"display": "flex", "gap": "8px", "marginBottom": "20px", "flexWrap": "wrap"}))

    # ══════════════════════════════════════════════════════════════
    # SECTION A: BALANCE SHEET (Schedule L)
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("A: BALANCE SHEET  (Schedule L — Form 1065)",
                            style={"color": CYAN, "margin": "0 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Assets, liabilities, and partners' capital at beginning and end of each tax year.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]

        if yr == 2025:
            beg_cash = 0
            beg_equipment = 0
            beg_inventory = 0
            beg_cc_balance = 0
        else:
            p = TAX_YEARS[2025]
            beg_cash = p["bank_deposits"] - p["bank_debits"]
            beg_equipment = bb_cc_asset_value
            beg_inventory = p["inventory_cost"]
            beg_cc_balance = bb_cc_balance

        end_cash = d["bank_deposits"] - d["bank_debits"]
        if yr == 2026:
            end_cash = beg_cash + d["bank_deposits"] - d["bank_debits"]
        end_equipment = bb_cc_asset_value
        end_inventory = d["inventory_cost"]
        end_cc_balance = bb_cc_balance

        beg_total_assets = beg_cash + beg_equipment + beg_inventory
        end_total_assets = end_cash + end_equipment + end_inventory
        beg_capital = beg_total_assets - beg_cc_balance
        end_capital = end_total_assets - end_cc_balance

        children.append(yr_header(yr))
        children.append(section(f"BALANCE SHEET — {yr}", [
            col_header(),
            html.Div("ASSETS", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "8px", "marginBottom": "4px"}),
            bs_row("Cash (Bank + Etsy)", beg_cash, end_cash),
            bs_row("Equipment (3D Printers)", beg_equipment, end_equipment, indent=1),
            bs_row("Inventory", beg_inventory, end_inventory, indent=1),
            bs_row("TOTAL ASSETS", beg_total_assets, end_total_assets, bold=True),
            html.Div("LIABILITIES", style={"color": RED, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "8px", "marginBottom": "4px"}),
            bs_row("Best Buy Citi CC", beg_cc_balance, end_cc_balance),
            bs_row("TOTAL LIABILITIES", beg_cc_balance, end_cc_balance, bold=True),
            html.Div("PARTNERS' CAPITAL", style={"color": CYAN, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "8px", "marginBottom": "4px"}),
            bs_row("Total Partners' Capital", beg_capital, end_capital, bold=True),
            bs_row("  TJ (50%)", beg_capital / 2, end_capital / 2, indent=1),
            bs_row("  Braden (50%)", beg_capital / 2, end_capital / 2, indent=1),
        ], color=ORANGE))

    # ══════════════════════════════════════════════════════════════
    # SECTION B: INCOME STATEMENT / P&L
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("B: INCOME STATEMENT / P&L",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Profit & Loss per tax year. Revenue from Etsy, expenses from Etsy fees + bank statements.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        op_expenses = (d["net_fees"] + d["shipping"] + d["marketing"]
                       + d.get("bank_additional_expense", 0) + d["taxes_collected"] + d["buyer_fees"])
        net_inc = gross_profit - op_expenses

        children.append(yr_header(yr))
        children.append(section(f"INCOME STATEMENT — {yr}", [
            html.Div("REVENUE", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px",
                      "marginBottom": "4px"}),
            row_item("Gross Sales", d["gross_sales"], color=GREEN),
            row_item("Returns & Refunds", -d["refunds"], indent=1, color=GRAY),
            row_item("Net Sales", d["gross_sales"] - d["refunds"], bold=True),
            divider(),
            html.Div("COST OF GOODS SOLD", style={"color": ORANGE, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "4px", "marginBottom": "4px"}),
            row_item("Inventory (invoices)", -d["inventory_cost"], indent=1, color=GRAY),
            row_item("Additional bank Amazon (not in receipts)", -d["bank_inv_gap"], indent=1, color=GRAY) if d["bank_inv_gap"] > 0 else html.Div(),
            row_item("Total COGS", -d["cogs"], bold=True),
            divider(),
            row_item("GROSS PROFIT", gross_profit, bold=True, color=GREEN),
            divider(),
            html.Div("OPERATING EXPENSES", style={"color": RED, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "4px", "marginBottom": "4px"}),
            row_item("Etsy Fees (net of credits)", -d["net_fees"], indent=1, color=GRAY),
            row_item("Shipping Labels", -d["shipping"], indent=1, color=GRAY),
            row_item("Advertising / Marketing", -d["marketing"], indent=1, color=GRAY),
            row_item("Sales Tax Collected & Remitted", -d["taxes_collected"], indent=1, color=GRAY),
            row_item("Buyer Fees", -d["buyer_fees"], indent=1, color=GRAY),
            row_item("Bank: Shipping Supplies", -d["bank_by_cat"].get("Shipping", 0), indent=1, color=GRAY),
            row_item("Bank: Craft Supplies", -d["bank_by_cat"].get("Craft Supplies", 0), indent=1, color=GRAY),
            row_item("Bank: AliExpress Supplies", -d["bank_by_cat"].get("AliExpress Supplies", 0), indent=1, color=GRAY),
            row_item("Bank: Etsy Fees (bank-side)", -d["bank_by_cat"].get("Etsy Fees", 0), indent=1, color=GRAY),
            row_item("Bank: Subscriptions", -d["bank_by_cat"].get("Subscriptions", 0), indent=1, color=GRAY),
            row_item("Bank: Business CC Payment", -d["bank_by_cat"].get("Business Credit Card", 0), indent=1, color=GRAY),
            row_item("Total Operating Expenses", -op_expenses, bold=True),
            divider(CYAN),
            row_item("NET INCOME", net_inc, bold=True, color=GREEN if net_inc >= 0 else RED),
            html.Div([
                html.Span(f"  TJ share (50%): {money(net_inc / 2)}   |   Braden share (50%): {money(net_inc / 2)}",
                          style={"color": GRAY, "fontSize": "12px", "marginTop": "4px"}),
            ]),
        ], color=ORANGE))

    # ══════════════════════════════════════════════════════════════
    # SECTION C: FORM 1065 — Partnership Return Summary
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("C: FORM 1065 — U.S. Return of Partnership Income",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Key line items from IRS Form 1065, mapped to dashboard data.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        total_deductions = (d["net_fees"] + d["shipping"] + d["marketing"]
                            + d.get("bank_additional_expense", 0) + d["taxes_collected"] + d["buyer_fees"])
        ordinary_income = gross_profit - total_deductions

        children.append(yr_header(yr))
        children.append(section(f"FORM 1065 SUMMARY — {yr}", [
            html.Div("PAGE 1 — INCOME", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px",
                      "marginBottom": "4px"}),
            form_row("1a", "Gross receipts or sales", d["gross_sales"], color=GREEN),
            form_row("1b", "Returns and allowances", d["refunds"]),
            form_row("1c", "Balance (1a minus 1b)", d["gross_sales"] - d["refunds"]),
            form_row("2", "Cost of goods sold (Schedule A)", d["cogs"]),
            form_row("3", "Gross profit (1c minus 2)", gross_profit, bold=True),
            divider(),
            html.Div("DEDUCTIONS", style={"color": RED, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "4px", "marginBottom": "4px"}),
            form_row("10", "Guaranteed payments to partners", 0, color=GRAY),
            form_row("14", "Etsy fees + processing", d["net_fees"]),
            form_row("15", "Shipping costs", d["shipping"] + d["bank_by_cat"].get("Shipping", 0)),
            form_row("18", "Advertising (Etsy Ads)", d["marketing"]),
            form_row("20", "Other deductions (supplies, subscriptions)",
                     d["bank_by_cat"].get("Craft Supplies", 0) + d["bank_by_cat"].get("AliExpress Supplies", 0)
                     + d["bank_by_cat"].get("Subscriptions", 0) + d["bank_by_cat"].get("Business Credit Card", 0)
                     + d["taxes_collected"] + d["buyer_fees"]),
            form_row("21", "Total deductions", total_deductions, bold=True),
            divider(CYAN),
            form_row("22", "Ordinary business income (loss)", ordinary_income, bold=True,
                     color=GREEN if ordinary_income >= 0 else RED),
            html.Div([
                html.Span(f"  Each partner's 50% share: {money(ordinary_income / 2)}",
                          style={"color": CYAN, "fontSize": "12px", "fontStyle": "italic", "marginTop": "4px"}),
            ]),
        ], color=ORANGE))

    # ══════════════════════════════════════════════════════════════
    # SECTION D: SCHEDULE K-1 (side-by-side)
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("D: SCHEDULE K-1  (Partner's Share of Income)",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("One K-1 per partner — each receives 50% of partnership income.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        total_deductions = (d["net_fees"] + d["shipping"] + d["marketing"]
                            + d.get("bank_additional_expense", 0) + d["taxes_collected"] + d["buyer_fees"])
        ordinary_income = gross_profit - total_deductions
        partner_share = ordinary_income / 2

        # Capital account tracking
        if yr == 2025:
            tj_beg_capital = 0
            br_beg_capital = 0
        else:
            p25 = TAX_YEARS[2025]
            gp25 = p25["gross_sales"] - p25["refunds"] - p25["cogs"]
            td25 = (p25["net_fees"] + p25["shipping"] + p25["marketing"]
                    + p25.get("bank_additional_expense", 0) + p25["taxes_collected"] + p25["buyer_fees"])
            oi25 = gp25 - td25
            tj_beg_capital = oi25 / 2 - p25["tulsa_draws"]
            br_beg_capital = oi25 / 2 - p25["texas_draws"]

        tj_end_capital = tj_beg_capital + partner_share - d["tulsa_draws"]
        br_end_capital = br_beg_capital + partner_share - d["texas_draws"]

        def k1_card(name, beg_cap, end_cap, draws, share):
            return html.Div([
                html.Div(name, style={"color": CYAN, "fontWeight": "bold", "fontSize": "14px",
                          "marginBottom": "8px", "textAlign": "center"}),
                row_item("Ordinary business income (Box 1)", share, color=GREEN),
                row_item("Net rental real estate (Box 2)", 0, color=GRAY),
                row_item("Other net rental income (Box 3)", 0, color=GRAY),
                row_item("Guaranteed payments (Box 4)", 0, color=GRAY),
                row_item("Self-employment earnings (Box 14)", share, color=ORANGE),
                divider(),
                html.Div("CAPITAL ACCOUNT", style={"color": PURPLE, "fontWeight": "bold", "fontSize": "12px",
                          "marginTop": "4px", "marginBottom": "4px"}),
                row_item("Beginning capital", beg_cap),
                row_item("+ Capital contributed", 0, indent=1, color=GRAY),
                row_item("+ Share of income", share, indent=1, color=GREEN),
                row_item("- Distributions / draws", -draws, indent=1),
                row_item("Ending capital", end_cap, bold=True,
                         color=GREEN if end_cap >= 0 else RED),
            ], style={"flex": "1", "backgroundColor": CARD2, "padding": "12px", "borderRadius": "8px",
                      "border": f"1px solid {CYAN}33", "minWidth": "280px"})

        children.append(yr_header(yr))
        children.append(section(f"SCHEDULE K-1 — {yr}", [
            html.Div([
                k1_card("TJ  (Partner A — 50%)", tj_beg_capital, tj_end_capital, d["tulsa_draws"], partner_share),
                k1_card("Braden  (Partner B — 50%)", br_beg_capital, br_end_capital, d["texas_draws"], partner_share),
            ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}),
        ], color=PURPLE))

    # ══════════════════════════════════════════════════════════════
    # SECTION E: SCHEDULE SE — Self-Employment Tax
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("E: SCHEDULE SE  —  Self-Employment Tax",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Each partner owes SE tax on their share of partnership income. "
                           "SS tax (12.4%) applies up to $168,600 (2025) / $176,100 (2026). Medicare (2.9%) has no cap.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        total_deductions = (d["net_fees"] + d["shipping"] + d["marketing"]
                            + d.get("bank_additional_expense", 0) + d["taxes_collected"] + d["buyer_fees"])
        ordinary_income = gross_profit - total_deductions
        partner_share = ordinary_income / 2

        # SE tax calculation per IRS rules
        net_se = partner_share * 0.9235  # 92.35% of net self-employment income
        ss_wage_base = 168600 if yr == 2025 else 176100
        ss_taxable = min(net_se, ss_wage_base)
        ss_tax = ss_taxable * 0.124  # 12.4%
        medicare_tax = net_se * 0.029  # 2.9%
        total_se_tax = ss_tax + medicare_tax
        # Deductible half of SE tax
        se_deduction = total_se_tax / 2

        def se_card(name, share):
            _net = share * 0.9235
            _ss = min(_net, ss_wage_base) * 0.124
            _med = _net * 0.029
            _total = _ss + _med
            _ded = _total / 2
            return html.Div([
                html.Div(name, style={"color": ORANGE, "fontWeight": "bold", "fontSize": "14px",
                          "marginBottom": "8px", "textAlign": "center"}),
                row_item("Net earnings from K-1", share),
                row_item("x 92.35%", _net, indent=1, color=GRAY),
                row_item("Social Security (12.4%)", _ss, indent=1, color=RED),
                row_item(f"  (wage base: ${ss_wage_base:,.0f})", 0, indent=2, color=DARKGRAY),
                row_item("Medicare (2.9%)", _med, indent=1, color=RED),
                divider(),
                row_item("TOTAL SE TAX", _total, bold=True, color=RED),
                row_item("Deductible half (Sch 1)", _ded, color=GREEN),
            ], style={"flex": "1", "backgroundColor": CARD2, "padding": "12px", "borderRadius": "8px",
                      "border": f"1px solid {ORANGE}33", "minWidth": "280px"})

        children.append(yr_header(yr))
        children.append(section(f"SCHEDULE SE — {yr}", [
            html.Div([
                se_card("TJ", partner_share),
                se_card("Braden", partner_share),
            ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}),
        ], color=ORANGE))

    # ══════════════════════════════════════════════════════════════
    # SECTION F: ESTIMATED TAX SUMMARY (hidden in strict mode)
    # ══════════════════════════════════════════════════════════════
    if _sm:
        children.append(html.Div([
            html.H3("F: ESTIMATED TAX SUMMARY  (1040-ES)",
                     style={"color": GRAY, "margin": "20px 0 10px 0", "fontSize": "14px"}),
            html.P("Hidden in strict mode — income tax estimates rely on progressive bracket assumptions. "
                   "Toggle strict mode off to see quarterly payment estimates.",
                   style={"color": ORANGE, "fontSize": "12px"}),
        ], style={"backgroundColor": f"{RED}08", "border": f"1px solid {RED}22",
                  "borderRadius": "6px", "padding": "10px 14px", "marginBottom": "10px"}))

    q_dates = {
        2025: [("Q4", "Jan 15, 2026", "Oct-Dec 2025")],
        2026: [("Q1", "Apr 15, 2026", "Jan-Mar 2026"),
               ("Q2", "Jun 15, 2026", "Apr-Jun 2026"),
               ("Q3", "Sep 15, 2026", "Jul-Sep 2026"),
               ("Q4", "Jan 15, 2027", "Oct-Dec 2026")],
    }

    if not _sm:
        for yr in (2025, 2026):
            d = TAX_YEARS[yr]
            gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
            total_deductions = (d["net_fees"] + d["shipping"] + d["marketing"]
                                + d.get("bank_additional_expense", 0) + d["taxes_collected"] + d["buyer_fees"])
            ordinary_income = gross_profit - total_deductions
            partner_share = ordinary_income / 2

            net_se = partner_share * 0.9235
            ss_wage_base = 168600 if yr == 2025 else 176100
            se_tax = min(net_se, ss_wage_base) * 0.124 + net_se * 0.029
            se_deduction = se_tax / 2

            # Estimated income tax (progressive brackets)
            taxable_income = partner_share - se_deduction  # after SE deduction
            est_income_tax = _compute_income_tax(max(0, taxable_income))
            total_annual_tax = se_tax + est_income_tax
            num_quarters = len(q_dates[yr])
            quarterly_payment = total_annual_tax / num_quarters if num_quarters else 0

            children.append(yr_header(yr))

            # Quarterly schedule table
            q_rows = []
            for q_label, due_date, period in q_dates[yr]:
                q_rows.append(html.Tr([
                    html.Td(q_label, style={"color": CYAN, "padding": "6px 10px", "fontSize": "13px", "fontWeight": "bold"}),
                    html.Td(period, style={"color": GRAY, "padding": "6px 10px", "fontSize": "13px"}),
                    html.Td(due_date, style={"color": WHITE, "padding": "6px 10px", "fontSize": "13px"}),
                    html.Td(money(quarterly_payment), style={"color": RED, "padding": "6px 10px",
                              "fontSize": "13px", "fontWeight": "bold", "fontFamily": "monospace", "textAlign": "right"}),
                ], style={"borderBottom": "1px solid #ffffff10"}))

            children.append(section(f"ESTIMATED TAX — {yr} (per partner)", [
                html.Div([
                    kpi_card("SE TAX", money(se_tax), RED, "Per partner",
                             f"Self-employment tax per partner: Social Security (12.4% on first ${ss_wage_base:,.0f}) + Medicare (2.9% on all earnings). Calculated on 92.35% of net self-employment income ({money(net_se)}). This replaces the employer/employee FICA split since you're self-employed."),
                    kpi_card("EST. INCOME TAX", money(est_income_tax), ORANGE, "progressive federal brackets",
                             f"Estimated federal income tax on partnership share ({money(partner_share)}) minus half of SE tax deduction ({money(se_deduction)}). Uses progressive 2026 federal brackets (10% through 37%). Actual rate depends on total household income, filing status, and other deductions."),
                    kpi_card("TOTAL ANNUAL", money(total_annual_tax), RED, "Per partner",
                             f"SE tax ({money(se_tax)}) + income tax ({money(est_income_tax)}) = {money(total_annual_tax)} per partner per year. This is what each partner needs to set aside for taxes."),
                    kpi_card("PER QUARTER", money(quarterly_payment), CYAN,
                             f"{num_quarters} payment(s)",
                             f"Divide annual tax ({money(total_annual_tax)}) by {num_quarters} quarters. Pay via IRS Form 1040-ES by each quarter's due date to avoid underpayment penalties (currently ~8% interest)."),
                ], style={"display": "flex", "gap": "8px", "marginBottom": "12px", "flexWrap": "wrap"}),
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("Quarter", style={"color": GRAY, "padding": "6px 10px", "fontSize": "11px", "textAlign": "left"}),
                        html.Th("Period", style={"color": GRAY, "padding": "6px 10px", "fontSize": "11px", "textAlign": "left"}),
                        html.Th("Due Date", style={"color": GRAY, "padding": "6px 10px", "fontSize": "11px", "textAlign": "left"}),
                        html.Th("Amount (each)", style={"color": GRAY, "padding": "6px 10px", "fontSize": "11px", "textAlign": "right"}),
                    ], style={"borderBottom": f"2px solid {ORANGE}44"})),
                    html.Tbody(q_rows),
                ], style={"width": "100%", "borderCollapse": "collapse"}),
                html.P(f"Note: Income tax estimate uses progressive 2026 federal brackets (10% through 37%). "
                       f"Actual rate depends on each partner's total taxable income and filing status.",
                       style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "10px", "fontStyle": "italic"}),
            ], color=ORANGE))

    # ══════════════════════════════════════════════════════════════
    # SECTION G: TAX WRITE-OFFS & DEDUCTIONS
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("G: TAX WRITE-OFFS & DEDUCTIONS",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Every dollar you can deduct reduces your taxable income — which lowers both income tax AND self-employment tax. "
                           "Below is every deduction the business has already claimed, plus deductions you may be missing.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    for yr in (2025, 2026):
        d = TAX_YEARS[yr]

        # ── Build the write-off items ──
        # Etsy platform deductions
        etsy_fees_ded = d["net_fees"]
        shipping_ded = d["shipping"]
        marketing_ded = d["marketing"]
        buyer_fees_ded = d["buyer_fees"]
        taxes_collected_ded = d["taxes_collected"]

        # Bank-side deductions
        bank_shipping = d["bank_by_cat"].get("Shipping", 0)
        bank_craft = d["bank_by_cat"].get("Craft Supplies", 0)
        bank_ali = d["bank_by_cat"].get("AliExpress Supplies", 0)
        bank_subs = d["bank_by_cat"].get("Subscriptions", 0)
        bank_etsy_fees = d["bank_by_cat"].get("Etsy Fees", 0)
        bank_cc_payment = d["bank_by_cat"].get("Business Credit Card", 0)

        # COGS deductions
        inv_cost_ded = d["inventory_cost"]
        bank_inv_ded = d["bank_inv"]
        total_cogs_ded = d["cogs"]

        # SE tax deduction (deductible half)
        gross_profit = d["gross_sales"] - d["refunds"] - d["cogs"]
        total_deductions_calc = (d["net_fees"] + d["shipping"] + d["marketing"]
                                 + d.get("bank_additional_expense", 0) + d["taxes_collected"] + d["buyer_fees"])
        ordinary_income = gross_profit - total_deductions_calc
        partner_share = ordinary_income / 2
        net_se = partner_share * 0.9235
        ss_wage_base = 168600 if yr == 2025 else 176100
        se_tax = min(net_se, ss_wage_base) * 0.124 + net_se * 0.029
        se_deduction = se_tax / 2

        # Total of all claimed deductions
        total_claimed = (total_cogs_ded + etsy_fees_ded + shipping_ded + bank_shipping
                         + marketing_ded + buyer_fees_ded + taxes_collected_ded
                         + bank_craft + bank_ali + bank_subs + bank_etsy_fees)

        # Potential missed deductions — UNKNOWN without supporting documents.
        # All hardcoded guesses REMOVED. Users must provide actual documents to claim.
        home_office_est = None   # Provide: lease/mortgage statement, office measurements
        internet_est = None      # Provide: ISP bill, document business-use percentage
        phone_est = None         # Provide: phone bill, document business-use percentage
        mileage_rate = 0.70      # IRS 2025 rate (this is a fact, not an estimate)
        est_biz_miles = None     # Provide: mileage log with odometer readings
        mileage_est = None       # Provide: mileage log
        # Equipment depreciation / Section 179
        section_179_est = bb_cc_asset_value if yr == 2025 else 0  # based on actual asset data

        total_potential = (section_179_est or 0)  # only Section 179 has actual data
        total_all_deductions = total_claimed + total_potential + (se_deduction * 2)  # both partners

        # Tax savings: SE rate (fixed) + marginal income tax rate (from brackets)
        _marginal_taxable = max(0, partner_share - se_deduction)
        _marginal_rate = (_compute_income_tax(_marginal_taxable) - _compute_income_tax(max(0, _marginal_taxable - 1))) if _marginal_taxable > 0 else 0.10
        effective_ded_rate = 0.9235 * 0.153 + _marginal_rate  # SE + marginal bracket

        children.append(yr_header(yr))

        # KPI strip for this year
        children.append(html.Div([
            kpi_card("TOTAL CLAIMED", money(total_claimed), GREEN, "Already deducted",
                     f"Sum of all business deductions from Etsy fees, shipping, COGS, advertising, and bank expenses for {yr}. These reduce your taxable income dollar-for-dollar."),
            kpi_card("TAX SAVINGS", money(total_claimed * effective_ded_rate), GREEN,
                     f"~{effective_ded_rate:.0%} effective rate",
                     f"Every $1 you deduct saves ~${effective_ded_rate:.2f} in combined SE tax + income tax. Total claimed ({money(total_claimed)}) x {effective_ded_rate:.0%} = {money(total_claimed * effective_ded_rate)} in tax you DON'T pay."),
            kpi_card("POTENTIAL MISSED", money(total_potential) if total_potential else "UNKNOWN", ORANGE,
                     "Provide docs to claim",
                     f"Deductions you may qualify for: home office, internet, phone, mileage. Provide lease/ISP bill/phone bill/mileage log to claim. Section 179 equipment: {money(section_179_est) if section_179_est else 'N/A'}."),
            kpi_card("EXTRA SAVINGS", money(total_potential * effective_ded_rate) if total_potential else "UNKNOWN", ORANGE,
                     "If you claim all",
                     f"Provide supporting documents (lease, ISP bill, phone bill, mileage log) to calculate actual additional deductions and tax savings."),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "12px", "flexWrap": "wrap"}))

        # Deduction table — CLAIMED
        def ded_row(desc, amount, irs_line="", note="", is_header=False, is_missed=False):
            bg = f"{ORANGE}08" if is_missed else "transparent"
            border_style = f"2px solid {GREEN}44" if is_header else "1px solid #ffffff10"
            is_unknown = amount is None
            icon = "\u2713 " if not is_missed and not is_unknown and amount > 0 else "\u26A0 " if is_missed else ""
            amount_text = "UNKNOWN" if is_unknown else (money(amount) if amount > 0 else "\u2014")
            amount_color = RED if is_unknown else (GREEN if amount and amount > 0 and not is_missed else ORANGE if is_missed and amount and amount > 0 else DARKGRAY)
            return html.Div([
                html.Span(f"{icon}{desc}", style={"flex": "3", "color": WHITE if not is_header else GREEN,
                           "fontSize": "13px", "fontWeight": "bold" if is_header else "normal"}),
                html.Span(irs_line, style={"flex": "1", "color": GRAY, "fontSize": "11px", "textAlign": "center"}),
                html.Span(amount_text, style={
                    "flex": "1", "textAlign": "right", "fontFamily": "monospace", "fontSize": "13px",
                    "color": amount_color,
                    "fontWeight": "bold" if is_header else "normal"}),
                html.Span(note, style={"flex": "2", "color": DARKGRAY, "fontSize": "11px", "paddingLeft": "10px"}),
            ], style={"display": "flex", "alignItems": "center", "padding": "5px 0",
                      "borderBottom": border_style, "backgroundColor": bg})

        children.append(section(f"WRITE-OFFS — {yr}", [
            # Column header
            html.Div([
                html.Span("Deduction", style={"flex": "3", "color": GRAY, "fontSize": "11px", "fontWeight": "bold"}),
                html.Span("IRS Line", style={"flex": "1", "color": GRAY, "fontSize": "11px", "textAlign": "center", "fontWeight": "bold"}),
                html.Span("Amount", style={"flex": "1", "color": GRAY, "fontSize": "11px", "textAlign": "right", "fontWeight": "bold"}),
                html.Span("Notes", style={"flex": "2", "color": GRAY, "fontSize": "11px", "paddingLeft": "10px", "fontWeight": "bold"}),
            ], style={"display": "flex", "padding": "4px 0", "borderBottom": f"2px solid {CYAN}44"}),

            # COGS
            ded_row("COST OF GOODS SOLD", total_cogs_ded, "Sch A", "", True),
            ded_row("  Invoice-based inventory", inv_cost_ded, "1065 Ln 2", "Amazon Business orders, paper receipts"),
            ded_row("  Bank-categorized inventory", bank_inv_ded, "1065 Ln 2", "Amazon purchases from bank statement"),

            # Platform fees
            ded_row("PLATFORM & PROCESSING FEES", etsy_fees_ded + buyer_fees_ded + bank_etsy_fees, "Ln 14/20", "", True),
            ded_row("  Etsy fees (net of credits)", etsy_fees_ded, "1065 Ln 14", f"Transaction, listing, processing fees minus {money(d['total_credits'])} in credits"),
            ded_row("  Buyer shipping fees", buyer_fees_ded, "1065 Ln 20", "Fees charged to buyers by Etsy"),
            ded_row("  Etsy fees (bank-side)", bank_etsy_fees, "1065 Ln 20", "Additional Etsy charges seen in bank"),

            # Shipping
            ded_row("SHIPPING & POSTAGE", shipping_ded + bank_shipping, "Ln 15", "", True),
            ded_row("  Etsy shipping labels", shipping_ded, "1065 Ln 15", "Postage purchased through Etsy"),
            ded_row("  Shipping supplies (bank)", bank_shipping, "1065 Ln 15", "Boxes, mailers, tape, etc."),

            # Advertising
            ded_row("ADVERTISING", marketing_ded, "Ln 18", "", True),
            ded_row("  Etsy Ads", marketing_ded, "1065 Ln 18", "Promoted listings on Etsy"),

            # Supplies
            ded_row("SUPPLIES & MATERIALS", bank_craft + bank_ali, "Ln 20", "", True),
            ded_row("  Craft supplies (bank)", bank_craft, "1065 Ln 20", "Hobby Lobby, craft stores"),
            ded_row("  AliExpress supplies", bank_ali, "1065 Ln 20", "Bulk supplies from AliExpress"),

            # Other
            ded_row("OTHER DEDUCTIONS", bank_subs + taxes_collected_ded, "Ln 20", "", True),
            ded_row("  Software subscriptions", bank_subs, "1065 Ln 20", "Business software, tools"),
            ded_row("  Sales tax collected/remitted", taxes_collected_ded, "1065 Ln 20", "State sales tax (pass-through)"),
            ded_row("  SE tax deduction (per partner)", se_deduction, "1040 Sch 1", "Deductible half of self-employment tax"),

            # TOTAL CLAIMED
            html.Div(style={"borderTop": f"2px solid {CYAN}66", "margin": "6px 0"}),
            html.Div([
                html.Span("TOTAL CLAIMED DEDUCTIONS", style={"flex": "3", "color": CYAN, "fontSize": "14px", "fontWeight": "bold"}),
                html.Span("", style={"flex": "1"}),
                html.Span(money(total_claimed), style={"flex": "1", "textAlign": "right", "fontFamily": "monospace",
                           "fontSize": "14px", "color": CYAN, "fontWeight": "bold"}),
                html.Span("", style={"flex": "2"}),
            ], style={"display": "flex", "padding": "8px 0"}),

            # POTENTIAL MISSED
            html.Div(style={"borderTop": f"2px solid {ORANGE}66", "margin": "10px 0 4px 0"}),
            html.Div("POTENTIAL ADDITIONAL DEDUCTIONS (not yet claimed)", style={
                "color": ORANGE, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "6px"}),
            html.P("These are common small-business deductions you may qualify for. Track these expenses to claim them.",
                   style={"color": GRAY, "fontSize": "11px", "margin": "0 0 6px 0"}),
            ded_row("  Home office (simplified method)", home_office_est, "8829",
                    "PROVIDE: lease/mortgage statement + office sqft measurement", is_missed=True),
            ded_row("  Internet (business portion)", internet_est, "1065 Ln 20",
                    "PROVIDE: ISP bill + document business-use %", is_missed=True),
            ded_row("  Cell phone (business portion)", phone_est, "1065 Ln 20",
                    "PROVIDE: phone bill + document business-use %", is_missed=True),
            ded_row("  Business mileage", mileage_est, "1065 Ln 20",
                    f"PROVIDE: mileage log with odometer readings (IRS rate: ${mileage_rate}/mi)", is_missed=True),
            ded_row("  Section 179: Equipment", section_179_est, "4562",
                    "Deduct full equipment cost in year 1 (3D printers)" if section_179_est and section_179_est > 0
                    else "Equipment purchased in 2025", is_missed=True),

            html.Div(style={"borderTop": f"2px solid {ORANGE}44", "margin": "6px 0"}),
            html.Div([
                html.Span("POTENTIAL EXTRA DEDUCTIONS", style={"flex": "3", "color": ORANGE, "fontSize": "13px", "fontWeight": "bold"}),
                html.Span("", style={"flex": "1"}),
                html.Span(money(total_potential) if total_potential else "UNKNOWN \u2014 provide docs above",
                           style={"flex": "1", "textAlign": "right", "fontFamily": "monospace",
                           "fontSize": "13px", "color": ORANGE, "fontWeight": "bold"}),
                html.Span(f"\u2192 saves ~{money(total_potential * effective_ded_rate)} in tax" if total_potential
                           else "\u2192 provide documents to calculate savings",
                           style={"flex": "2", "color": ORANGE, "fontSize": "11px", "paddingLeft": "10px"}),
            ], style={"display": "flex", "padding": "6px 0"}),
        ], color=GREEN))

    # ══════════════════════════════════════════════════════════════
    # SECTION H: TAX STRATEGY & OPTIMIZATION
    # ══════════════════════════════════════════════════════════════
    children.append(html.H3("H: TAX STRATEGY & OPTIMIZATION",
                            style={"color": CYAN, "margin": "20px 0 10px 0",
                                   "borderBottom": f"2px solid {CYAN}33", "paddingBottom": "6px"}))
    children.append(html.P("Actionable strategies to legally minimize your tax bill. "
                           "Ranked by estimated impact — highest savings first.",
                           style={"color": GRAY, "margin": "0 0 10px 0", "fontSize": "13px"}))

    # Compute strategy values
    _combined_annual_income = sum(
        (TAX_YEARS[yr]["gross_sales"] - TAX_YEARS[yr]["refunds"] - TAX_YEARS[yr]["cogs"]
         - TAX_YEARS[yr]["net_fees"] - TAX_YEARS[yr]["shipping"] - TAX_YEARS[yr]["marketing"]
         - TAX_YEARS[yr].get("bank_additional_expense", 0) - TAX_YEARS[yr]["taxes_collected"] - TAX_YEARS[yr]["buyer_fees"]
         + TAX_YEARS[yr].get("payments", 0))
        for yr in (2025, 2026))
    _combined_partner_share = _combined_annual_income / 2
    _combined_se_net = _combined_partner_share * 0.9235
    _combined_se_tax = min(_combined_se_net, 176100) * 0.124 + _combined_se_net * 0.029
    _combined_income_tax = _compute_income_tax(max(0, _combined_partner_share - _combined_se_tax / 2))
    _combined_total_tax = _combined_se_tax + _combined_income_tax
    _total_draws = sum(TAX_YEARS[yr]["total_draws"] for yr in (2025, 2026))

    # S-Corp election savings estimate
    _reasonable_salary = min(_combined_partner_share * 0.6, 50000)
    _scorp_se_net = _reasonable_salary * 0.9235
    _scorp_se_tax = min(_scorp_se_net, 176100) * 0.124 + _scorp_se_net * 0.029
    _scorp_savings = max(0, _combined_se_tax - _scorp_se_tax)

    # Retirement contribution savings
    _sep_ira_limit = min(_combined_partner_share * 0.25, 69000)
    _sep_taxable = max(0, _combined_partner_share - _combined_se_tax / 2)
    _sep_tax_savings = _compute_income_tax(_sep_taxable) - _compute_income_tax(max(0, _sep_taxable - _sep_ira_limit))

    def strategy_card(title, savings, priority, status, description, action_items, color):
        pri_colors = {"HIGH": RED, "MEDIUM": ORANGE, "LOW": TEAL}
        status_colors = {"DO NOW": RED, "PLAN FOR": ORANGE, "CONSIDER": CYAN, "TRACK": GREEN}
        return html.Div([
            html.Div([
                html.Span(title, style={"color": color, "fontWeight": "bold", "fontSize": "14px", "flex": "1"}),
                html.Span(priority, style={"backgroundColor": f"{pri_colors.get(priority, GRAY)}22",
                           "color": pri_colors.get(priority, GRAY), "padding": "2px 10px", "borderRadius": "4px",
                           "fontSize": "10px", "fontWeight": "bold", "marginRight": "6px"}),
                html.Span(status, style={"backgroundColor": f"{status_colors.get(status, GRAY)}22",
                           "color": status_colors.get(status, GRAY), "padding": "2px 10px", "borderRadius": "4px",
                           "fontSize": "10px", "fontWeight": "bold"}),
            ], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}),
            html.Div([
                html.Span("Est. savings: ", style={"color": GRAY, "fontSize": "12px"}),
                html.Span(money(savings), style={"color": GREEN, "fontWeight": "bold", "fontFamily": "monospace", "fontSize": "14px"}),
                html.Span(" / year", style={"color": GRAY, "fontSize": "11px"}),
            ], style={"marginBottom": "6px"}),
            html.P(description, style={"color": GRAY, "fontSize": "12px", "margin": "0 0 8px 0", "lineHeight": "1.4"}),
            html.Div([
                html.Div(f"\u2192 {item}", style={"color": WHITE, "fontSize": "12px", "padding": "2px 0"})
                for item in action_items
            ]),
        ], style={"padding": "14px", "backgroundColor": CARD2, "borderRadius": "8px",
                  "border": f"1px solid {color}33", "borderLeft": f"4px solid {color}",
                  "marginBottom": "10px"})

    # Quarterly payment timing
    _q1_due = "Apr 15, 2026"
    _q1_amount = _combined_total_tax / 4
    _penalty_risk = _combined_total_tax > 1000

    strategies = []

    # 1. Quarterly estimated payments
    strategies.append(strategy_card(
        "Pay Quarterly Estimated Taxes", _combined_total_tax * 0.08 if _penalty_risk else 0,
        "HIGH", "DO NOW",
        f"You owe ~{money(_combined_total_tax)} per partner in total tax. If you don't pay quarterly, the IRS charges ~8% "
        f"underpayment penalty. Next payment: {_q1_due} for {money(_q1_amount)} each.",
        [f"Pay {money(_q1_amount)} per partner by {_q1_due} (Form 1040-ES)",
         "Set up IRS Direct Pay or EFTPS for auto-payments",
         "Mark calendar for Q2 (Jun 15), Q3 (Sep 15), Q4 (Jan 15)"],
        RED))

    # 2. Section 179 / Equipment deduction
    if bb_cc_asset_value > 0:
        _sec179_savings = bb_cc_asset_value * effective_ded_rate
        strategies.append(strategy_card(
            "Section 179: Deduct Equipment in Year 1", _sec179_savings,
            "HIGH", "DO NOW",
            f"You purchased {money(bb_cc_asset_value)} in 3D printing equipment. Under Section 179, you can deduct the "
            f"FULL cost in the year purchased (2025) instead of depreciating over 5-7 years. This gives you an immediate "
            f"{money(_sec179_savings)} tax reduction.",
            ["File Form 4562 with 2025 return to elect Section 179",
             f"Deduct full {money(bb_cc_asset_value)} against 2025 income",
             "Keep all Best Buy receipts \u2014 IRS requires proof of purchase",
             "Note: CC interest on business purchases is also deductible"],
            GREEN))

    # 3. Home office deduction
    _home_office_savings = 1500 * effective_ded_rate
    strategies.append(strategy_card(
        "Home Office Deduction", _home_office_savings,
        "HIGH", "DO NOW",
        "If you use a dedicated space at home exclusively for business (3D printing, packing orders), you qualify for "
        "the home office deduction. Simplified method: $5/sqft up to 300 sqft = $1,500/year. "
        "Regular method could be even higher based on your actual expenses (rent, utilities, etc).",
        ["Measure your dedicated workspace square footage",
         "Simplified: claim $5 x sqft (max 300 sqft = $1,500)",
         "Regular: calculate % of home used for biz, apply to rent/mortgage + utilities",
         "Must be used REGULARLY and EXCLUSIVELY for business"],
        GREEN))

    # 4. Track ALL business mileage
    strategies.append(strategy_card(
        "Business Mileage Deduction", mileage_rate * 500 * effective_ded_rate,
        "MEDIUM", "TRACK",
        f"Every trip to the post office, supply store, or anywhere for business purposes is deductible at "
        f"${mileage_rate:.2f}/mile (2025 IRS rate). Even 500 miles/year = {money(500 * mileage_rate)} deduction.",
        ["Download a mileage tracking app (MileIQ, Everlance, or free Stride)",
         "Log EVERY business trip: post office, Hobby Lobby, Home Depot, etc.",
         "Keep a simple spreadsheet: date, destination, miles, purpose",
         f"At ${mileage_rate:.2f}/mi, even small trips add up fast"],
        TEAL))

    # 5. Internet & phone deductions — amounts UNKNOWN without bills
    _utility_savings = 0
    strategies.append(strategy_card(
        "Internet & Phone (Business Portion)", _utility_savings,
        "MEDIUM", "TRACK",
        "You can deduct the business-use percentage of your internet and cell phone bills. "
        "If you use internet 30% for business and phone 20%, those portions are deductible.",
        ["Calculate what % of internet use is for business (Etsy shop, research, shipping)",
         "Calculate what % of phone use is for business (Etsy app, customer messages)",
         "Keep phone/internet bills as documentation",
         "Conservative estimate: 25-30% internet, 15-20% phone"],
        TEAL))

    # 6. SEP-IRA / retirement
    if _combined_partner_share > 500:
        strategies.append(strategy_card(
            "SEP-IRA Retirement Contributions", _sep_tax_savings,
            "MEDIUM", "PLAN FOR",
            f"Self-employed individuals can contribute up to 25% of net self-employment earnings to a SEP-IRA "
            f"(max $69,000 for 2025). Your max contribution: ~{money(_sep_ira_limit)} per partner. This reduces "
            f"taxable income and builds retirement savings simultaneously.",
            [f"Open a SEP-IRA at Fidelity, Vanguard, or Schwab (free)",
             f"Contribute up to {money(_sep_ira_limit)} per partner before filing deadline",
             "Contributions are tax-deductible \u2014 reduces income tax immediately",
             "Can contribute for 2025 up until Apr 15, 2026 (or Oct 15 with extension)"],
            PURPLE))

    # 7. S-Corp election (if income grows)
    if _combined_partner_share > 500:
        strategies.append(strategy_card(
            "S-Corp Election (Future)", _scorp_savings,
            "LOW", "CONSIDER",
            f"If annual profits exceed ~$40K per partner, electing S-Corp status lets you split income into "
            f"salary (subject to SE tax) and distributions (NOT subject to SE tax). With current income of "
            f"~{money(_combined_partner_share)}/partner, this could save ~{money(_scorp_savings)}/year in SE tax. "
            f"However, S-Corps have more paperwork and payroll requirements.",
            ["Only worth it when consistent profit > $40K/partner/year",
             f"Reasonable salary: ~{money(_reasonable_salary)} \u2192 SE tax only on salary portion",
             "Requires payroll (Gusto ~$40/mo), separate tax return (Form 1120-S)",
             "File Form 2553 by Mar 15 of the year you want it to take effect"],
            CYAN))

    # 8. Timing strategy
    strategies.append(strategy_card(
        "Year-End Tax Planning (Timing)", _combined_total_tax * 0.05,
        "MEDIUM", "PLAN FOR",
        "You can shift income and expenses between tax years to minimize taxes. If 2026 is looking like a high-income year, "
        "accelerate expenses into 2026 (buy supplies in Dec). If 2026 is low, defer expenses to 2027.",
        ["Buy inventory & supplies before Dec 31 to deduct in current year",
         "Prepay subscriptions or advertising if cash allows",
         "Consider major equipment purchases in high-income years for Section 179",
         "Delay invoicing or deposits if you want to push income to next year"],
        ORANGE))

    # 9. Record keeping
    strategies.append(strategy_card(
        "Bulletproof Record Keeping", 0,
        "HIGH", "DO NOW",
        "The best tax strategy is worthless without documentation. If audited, you need receipts for every deduction. "
        "Good records also make tax filing faster and cheaper.",
        ["Save ALL receipts (digital photos or apps like Dext/Shoeboxed)",
         "Separate business and personal bank accounts (you already have this!)",
         "Keep a simple log of cash expenses (post office, supply runs)",
         "Back up Etsy CSV statements and bank statements monthly",
         "This dashboard IS your documentation \u2014 export/screenshot for records"],
        WHITE))

    # Build the section
    children.append(section("TAX STRATEGY OVERVIEW", [
        # Summary KPIs
        html.Div([
            kpi_card("CURRENT TAX BILL", money(_combined_total_tax * 2), RED, "Both partners combined",
                     f"Total estimated tax liability for both partners across 2025 + 2026 YTD. This includes self-employment tax and estimated income tax using progressive federal brackets."),
            kpi_card("MAX POTENTIAL SAVINGS",
                     money(total_potential * effective_ded_rate * 2 + _scorp_savings) if total_potential else "UNKNOWN",
                     GREEN, "Provide docs to calculate",
                     f"Maximum tax savings requires providing supporting documents: lease/mortgage, ISP bill, phone bill, mileage log. Section 179 equipment deduction is based on actual asset data."),
            kpi_card("EASIEST WIN", "PROVIDE DOCS", GREEN, "Home office deduction",
                     "Provide lease/mortgage statement and office sqft measurement. IRS simplified method: $5/sqft x up to 300 sqft = max $1,500/yr deduction."),
            kpi_card("NEXT DEADLINE", _q1_due, ORANGE, f"Q1 payment: {money(_q1_amount)}/partner",
                     f"Next quarterly estimated tax payment due date. Pay {money(_q1_amount)} per partner to IRS via Form 1040-ES to avoid underpayment penalties (~8% interest)."),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "14px", "flexWrap": "wrap"}),
    ] + strategies, ORANGE))

    # Disclaimer
    children.append(html.Div([
        html.P("DISCLAIMER: These calculations are estimates based on dashboard data and standard IRS formulas. "
               "They are NOT a substitute for professional tax advice. Consult a CPA or tax professional "
               "before filing. Key Component Manufacturing LLC (EIN pending) \u2014 50/50 multi-member LLC taxed as partnership.",
               style={"color": DARKGRAY, "fontSize": "11px", "fontStyle": "italic", "margin": "10px 0",
                      "padding": "10px", "backgroundColor": CARD2, "borderRadius": "6px",
                      "border": f"1px solid {RED}33"}),
    ]))

    return html.Div(children, style={"padding": TAB_PADDING})
