"""Deep Dive / JARVIS tab — Business Intelligence Command Center."""

from dash import dcc, html
from dashboard_utils.theme import *


def build_tab2_deep_dive():
    """Tab 2 - JARVIS: Business Intelligence Command Center"""
    # Lazy import to avoid circular dependency during bridge phase
    import etsy_dashboard as ed

    # Pull globals from the monolith
    strict_mode = ed.strict_mode
    _sm = strict_mode if isinstance(strict_mode, bool) else False

    # Functions
    _compute_health_score = ed._compute_health_score
    _generate_briefing = ed._generate_briefing
    _generate_actions = ed._generate_actions
    _detect_patterns = ed._detect_patterns
    _build_jarvis_header = ed._build_jarvis_header
    _build_health_section = ed._build_health_section
    _build_actions_section = ed._build_actions_section
    _build_patterns_section = ed._build_patterns_section
    _build_goal_tracker = ed._build_goal_tracker
    _build_jarvis_auto_briefing = ed._build_jarvis_auto_briefing
    _build_kpi_pill = ed._build_kpi_pill
    chart_context = ed.chart_context
    _strict_banner = ed._strict_banner

    # Chart figures
    proj_chart = ed.proj_chart
    trend_profit_rev = ed.trend_profit_rev
    cost_ratio_fig = ed.cost_ratio_fig
    aov_fig = ed.aov_fig
    orders_day_fig = ed.orders_day_fig
    cum_fig = ed.cum_fig
    profit_rolling_fig = ed.profit_rolling_fig
    ppo_fig = ed.ppo_fig
    daily_fig = ed.daily_fig
    dow_fig = ed.dow_fig
    anomaly_fig = ed.anomaly_fig
    corr_fig = ed.corr_fig
    product_heat = ed.product_heat
    unit_wf = ed.unit_wf
    rev_inv_fig = ed.rev_inv_fig
    inv_cat_bar = ed.inv_cat_bar
    loc_fig = ed.loc_fig
    cashflow_fig = ed.cashflow_fig

    # Metrics / data globals
    analytics_insights = ed.analytics_insights
    _growth_pct = ed._growth_pct
    _r2_sales = ed._r2_sales
    _latest_month_rev = ed._latest_month_rev
    _latest_month_net = ed._latest_month_net
    _daily_rev_avg = ed._daily_rev_avg
    _daily_profit_avg = ed._daily_profit_avg
    _best_day_rev = ed._best_day_rev
    _last_fee_pct = ed._last_fee_pct
    _last_ship_pct = ed._last_ship_pct
    _last_mkt_pct = ed._last_mkt_pct
    _last_ref_pct = ed._last_ref_pct
    _last_margin_pct = ed._last_margin_pct
    avg_order = ed.avg_order
    _aov_best_week = ed._aov_best_week
    _aov_worst_week = ed._aov_worst_week
    _daily_orders_avg = ed._daily_orders_avg
    _peak_orders_day = ed._peak_orders_day
    gross_sales = ed.gross_sales
    etsy_net = ed.etsy_net
    _total_costs = ed._total_costs
    _net_margin_overall = ed._net_margin_overall
    _current_14d_profit_avg = ed._current_14d_profit_avg
    _last_ppo_val = ed._last_ppo_val
    _last_aov_val = ed._last_aov_val
    _best_dow = ed._best_dow
    _worst_dow = ed._worst_dow
    _dow_rev_vals = ed._dow_rev_vals
    _anomaly_high = ed._anomaly_high
    _anomaly_low = ed._anomaly_low
    _zero_days = ed._zero_days
    _daily_rev_mean = ed._daily_rev_mean
    _daily_rev_std = ed._daily_rev_std
    _corr_r2 = ed._corr_r2
    total_marketing = ed.total_marketing
    _top_prod_names = ed._top_prod_names
    product_revenue_est = ed.product_revenue_est
    _unit_rev = ed._unit_rev
    _unit_profit = ed._unit_profit
    _unit_margin = ed._unit_margin
    _unit_cogs = ed._unit_cogs
    _monthly_fixed = ed._monthly_fixed
    _contrib_margin_pct = ed._contrib_margin_pct
    _breakeven_monthly = ed._breakeven_monthly
    _breakeven_daily = ed._breakeven_daily
    _breakeven_orders = ed._breakeven_orders
    val_monthly_run_rate = ed.val_monthly_run_rate
    true_inventory_cost = ed.true_inventory_cost
    _val_months_operating = ed._val_months_operating
    biz_inv_by_category = ed.biz_inv_by_category
    tulsa_spend = ed.tulsa_spend
    texas_spend = ed.texas_spend
    _supplier_spend = ed._supplier_spend
    bank_total_deposits = ed.bank_total_deposits
    bank_total_debits = ed.bank_total_debits
    bank_net_cash = ed.bank_net_cash
    profit_margin = ed.profit_margin
    bank_cash_on_hand = ed.bank_cash_on_hand
    bank_owner_draw_total = ed.bank_owner_draw_total
    profit = ed.profit

    # Compute JARVIS data
    score, sub_scores, weights = _compute_health_score()
    briefing = _generate_briefing()
    actions = _generate_actions()
    patterns = _detect_patterns()

    # AI Analytics insight cards (existing)
    insight_cards = []
    for _, cat, title, detail, sev in analytics_insights:
        sc = severity_color(sev)
        insight_cards.append(html.Div([
            html.Div([
                html.Span(cat, style={
                    "color": BG, "backgroundColor": sc, "padding": "2px 8px",
                    "borderRadius": "4px", "fontSize": "11px", "fontWeight": "bold",
                }),
            ], style={"marginBottom": "6px"}),
            html.H4(title, style={"color": sc, "margin": "0 0 6px 0", "fontSize": "14px"}),
            html.P(detail, style={"color": "#cccccc", "margin": "0", "fontSize": "12px", "lineHeight": "1.5"}),
        ], style={
            "backgroundColor": "#0d1b2a", "padding": "14px", "borderRadius": "8px",
            "borderLeft": f"4px solid {sc}", "marginBottom": "8px",
        }))

    # Strict mode banner for deep dive
    _strict_banner_dd = _strict_banner() if _sm else html.Div()

    return html.Div([
        _strict_banner_dd,

        # JARVIS HEADER
        _build_jarvis_header(),

        # HEALTH SCORE + DAILY BRIEFING (hidden in strict mode — composite estimate)
        _build_health_section(score, sub_scores, weights, briefing) if not _sm else html.Div(),

        # PRIORITY ACTIONS
        _build_actions_section(actions),

        # PATTERN DETECTION
        _build_patterns_section(patterns),

        # GOAL TRACKING (hidden in strict mode — uses estimates)
        _build_goal_tracker() if not _sm else html.Div(),

        # DETAILED CHARTS (collapsible sections — all existing charts)

        html.H3("DETAILED ANALYTICS", style={
            "color": CYAN, "margin": "30px 0 6px 0", "fontSize": "14px",
            "letterSpacing": "1.5px",
            "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px",
        }),
        html.P("Expand sections below for full chart breakdowns.",
               style={"color": GRAY, "margin": "0 0 14px 0", "fontSize": "12px"}),

        # Section 1: Revenue & Trends
        html.Details([
            html.Summary("Revenue & Trends", style={
                "color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 0", "listStyle": "none",
            }),
            html.Div([
                chart_context(
                    "Linear regression forecast based on your monthly revenue trend. Solid lines = actual, dashed = projected. "
                    "The shaded band shows the confidence range \u2014 wider band means less predictable.",
                    metrics=[
                        ("Growth", f"{_growth_pct:+.1f}%/mo", GREEN if _growth_pct > 0 else RED),
                        ("R\u00b2 Fit", f"{_r2_sales:.2f}", TEAL),
                        ("Latest Month Rev", f"${_latest_month_rev:,.0f}", GREEN),
                        ("Latest Month Net", f"${_latest_month_net:,.0f}", ORANGE),
                    ],
                    look_for="Dashed lines trending up = growing business. R\u00b2 close to 1.0 = very predictable trend.",
                    simple="Solid lines = your actual sales and profit each month. Dashed lines = where a computer model predicts you're headed. If dashed lines point up, the business is growing."
                ) if not _sm else html.Div(),
                dcc.Graph(figure=proj_chart, config={"displayModeBar": False}, style={"height": "380px"}) if not _sm else html.Div(
                    "Projection chart hidden in strict mode (estimates).",
                    style={"color": GRAY, "fontSize": "12px", "fontStyle": "italic", "padding": "20px", "textAlign": "center",
                           "backgroundColor": "#ffffff06", "borderRadius": "6px", "marginBottom": "10px"}),
                html.Div(insight_cards),

                chart_context(
                    "Green bars = daily revenue, orange bars = daily profit. Dashed lines = 30-day rolling averages that smooth out noise.",
                    metrics=[
                        ("Avg Rev/Day", f"${_daily_rev_avg:,.0f}", GREEN),
                        ("Avg Profit/Day", f"${_daily_profit_avg:,.0f}", ORANGE),
                        ("Best Day", f"${_best_day_rev:,.0f}", TEAL),
                    ],
                    look_for="Rolling averages trending up together. If revenue rises but profit flattens, costs are eating your growth.",
                    simple="Each bar is one day. Green = what you earned, orange = what you kept as profit. The smooth lines filter out the noise to show the overall direction. Lines going up = business improving.",
                ),
                dcc.Graph(figure=trend_profit_rev, config={"displayModeBar": False}, style={"height": "380px"}),

                chart_context(
                    "Each line shows how many cents per dollar of sales go to that cost category. "
                    "Net Margin is what you keep after all Etsy deductions.",
                    metrics=[
                        ("Fees", f"{_last_fee_pct:.1f}%", RED),
                        ("Shipping", f"{_last_ship_pct:.1f}%", BLUE),
                        ("Marketing", f"{_last_mkt_pct:.1f}%", PURPLE),
                        ("Refunds", f"{_last_ref_pct:.1f}%", ORANGE),
                        ("Net Margin", f"{_last_margin_pct:.1f}%", TEAL),
                    ],
                    legend=[
                        (RED, "Fees %", "Etsy listing, transaction & payment processing fees combined"),
                        (BLUE, "Shipping %", "Cost of USPS/Asendia shipping labels you purchased"),
                        (PURPLE, "Marketing %", "Etsy Ads + Offsite Ads spend as % of sales"),
                        (ORANGE, "Refunds %", "Money returned to buyers \u2014 lower is better"),
                        (TEAL, "Net Margin %", "What you actually keep from each dollar of sales (dashed line)"),
                    ],
                    look_for="Lines trending down = improving efficiency. Net Margin trending up = you're keeping more per sale.",
                    simple="Each line tracks a different cost as a percentage of sales. Lines going DOWN means you're getting more efficient. The teal dashed line (Net Margin) going UP is the best sign -- it means you're keeping more of each dollar.",
                ),
                dcc.Graph(figure=cost_ratio_fig, config={"displayModeBar": False}, style={"height": "380px"}),

                html.Div([
                    html.Div([
                        chart_context(
                            "Average Order Value = total revenue \u00f7 number of orders per week. Higher AOV means customers spend more per purchase.",
                            metrics=[
                                ("Overall AOV", f"${avg_order:,.2f}", TEAL),
                                ("Best Week", f"${_aov_best_week:,.2f}", GREEN),
                                ("Worst Week", f"${_aov_worst_week:,.2f}", RED),
                            ],
                            look_for="Upward trend = customers spending more. Sudden drops may mean discounting or smaller product mix.",
                            simple="This shows how much each customer spends on average per order. Line going up = customers spending more per purchase. The gray dashed line is your overall average.",
                        ),
                        dcc.Graph(figure=aov_fig, config={"displayModeBar": False}, style={"height": "340px"}),
                    ], style={"flex": "1"}),
                    html.Div([
                        chart_context(
                            "How many orders you get each day, separated from dollar amounts. Blue bars = daily count, lines = smoothed averages.",
                            metrics=[
                                ("Avg Orders/Day", f"{_daily_orders_avg:.1f}", BLUE),
                                ("Peak Day", f"{_peak_orders_day:.0f}", GREEN),
                            ],
                            look_for="Rising trend = growing demand. Spikes often follow promotions, holidays, or viral listings.",
                            simple="Each blue bar is how many orders came in that day. The smooth lines show the trend. More bars and higher lines = more customers finding your shop.",
                        ),
                        dcc.Graph(figure=orders_day_fig, config={"displayModeBar": False}, style={"height": "340px"}),
                    ], style={"flex": "1"}),
                ], style={"display": "flex", "gap": "8px"}),

                chart_context(
                    "Running totals since your first sale. The gap between the green (revenue) and orange (profit) lines = total costs paid to date.",
                    metrics=[
                        ("Total Revenue", f"${gross_sales:,.0f}", GREEN),
                        ("Total Profit", f"${etsy_net:,.0f}", ORANGE),
                        ("Total Costs", f"${_total_costs:,.0f}", RED),
                        ("Margin", f"{_net_margin_overall:.1f}%", TEAL),
                    ],
                    look_for="Lines spreading apart = costs growing faster than revenue. Parallel lines = stable margins.",
                    simple="These lines show your total earnings since day one -- green for revenue, orange for profit. The gap between them is your total costs. Both lines should keep climbing. If the gap widens, costs are growing faster than sales.",
                ),
                dcc.Graph(figure=cum_fig, config={"displayModeBar": False}, style={"height": "380px"}),

                html.Div([
                    html.Div([
                        chart_context(
                            "Smoothed daily profit using a 14-day rolling average. Above the red zero line = making money that period.",
                            metrics=[
                                ("Avg Profit/Day", f"${_daily_profit_avg:,.2f}", ORANGE),
                                ("Current 14d Avg", f"${_current_14d_profit_avg:,.2f}", TEAL),
                            ],
                            look_for="Line staying above zero and trending up. Dips below zero = you were losing money those weeks.",
                            simple="This smooths out daily ups and downs to show if you're actually making money. Line ABOVE the red zero line = making money. Line BELOW = losing money. The higher above zero, the better.",
                        ),
                        dcc.Graph(figure=profit_rolling_fig, config={"displayModeBar": False}, style={"height": "340px"}),
                    ], style={"flex": "1"}),
                    html.Div([
                        chart_context(
                            "Green/red bars = average profit you make per order each month. Dashed line = AOV (average order value) on right axis.",
                            metrics=[
                                ("Latest Profit/Order", f"${_last_ppo_val:,.2f}", GREEN if _last_ppo_val >= 0 else RED),
                                ("Latest AOV", f"${_last_aov_val:,.2f}", TEAL),
                            ],
                            look_for="Green bars getting taller = more profit per sale. Red bars = losing money per order that month.",
                            simple="Green bars = you made money per order that month. Red bars = you lost money. The dashed line shows average order size. You want green bars getting taller over time.",
                        ),
                        dcc.Graph(figure=ppo_fig, config={"displayModeBar": False}, style={"height": "340px"}),
                    ], style={"flex": "1"}),
                ], style={"display": "flex", "gap": "8px"}),

                chart_context(
                    "Green bars = daily revenue, blue line = order count (right axis), orange line = 7-day average revenue.",
                    metrics=[
                        ("Avg Rev/Day", f"${_daily_rev_avg:,.0f}", GREEN),
                        ("Avg Orders/Day", f"{_daily_orders_avg:.1f}", BLUE),
                    ],
                    look_for="Orange trend line direction shows your momentum. Blue line diverging from green = order size changing.",
                    simple="Green bars show daily sales dollars. Blue line shows number of orders. Orange line smooths out the daily noise. All three going up = healthy growing business.",
                ),
                dcc.Graph(figure=daily_fig, config={"displayModeBar": False}, style={"height": "380px"}),
            ]),
        ], style={"marginBottom": "8px"}),

        # Section 2: Pattern Recognition
        html.Details([
            html.Summary("Pattern Recognition", style={
                "color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 0", "listStyle": "none",
            }),
            html.Div([
                html.Div([
                    html.Div([
                        chart_context(
                            f"Average performance by day of week. Best day: {_best_dow}. Worst day: {_worst_dow}.",
                            metrics=[
                                ("Best Day", f"{_best_dow} (${max(_dow_rev_vals):,.0f})", GREEN),
                                ("Worst Day", f"{_worst_dow} (${min(_dow_rev_vals):,.0f})", RED),
                            ],
                            look_for="Consistent patterns reveal when to run promotions or launch new listings.",
                            simple="This shows which days of the week you sell the most. Taller bars = better days. Use this to know when to launch new products or run promotions.",
                        ),
                        dcc.Graph(figure=dow_fig, config={"displayModeBar": False}, style={"height": "340px"}),
                    ], style={"flex": "1"}),
                    html.Div([
                        chart_context(
                            f"Revenue outliers detected with z-scores. Spikes (>2\u03c3): {len(_anomaly_high)}. "
                            f"Drops (<-1.5\u03c3): {len(_anomaly_low)}. Zero days: {len(_zero_days)}.",
                            metrics=[
                                ("Mean Revenue", f"${_daily_rev_mean:,.0f}/day", TEAL),
                                ("Std Dev", f"${_daily_rev_std:,.0f}", GRAY),
                                ("Spikes", str(len(_anomaly_high)), GREEN),
                                ("Drops", str(len(_anomaly_low)), RED),
                            ],
                            look_for="Investigate spikes (what sold?) and drops (listing issues? shipping delays?).",
                            simple="The gray line is your daily sales. Green triangles mark unusually GOOD days (big sales spikes). Red triangles mark unusually BAD days. Orange X's are days with zero sales. Investigate what happened on those days.",
                        ),
                        dcc.Graph(figure=anomaly_fig, config={"displayModeBar": False}, style={"height": "340px"}),
                    ], style={"flex": "1"}),
                ], style={"display": "flex", "gap": "8px"}),

                chart_context(
                    "Does spending more on ads actually drive more sales? Each dot is one month.",
                    metrics=[
                        ("Correlation R\u00b2", f"{_corr_r2:.2f}", GREEN if _corr_r2 > 0.5 else ORANGE),
                        ("Total Ad Spend", money(total_marketing), PURPLE),
                        ("Ad % of Sales", f"{total_marketing / gross_sales * 100:.1f}%" if gross_sales else "0%", ORANGE),
                    ],
                    look_for=f"{'Strong correlation -- ads are driving sales.' if _corr_r2 > 0.5 else 'Weak correlation -- sales may not depend much on ad spend. Test cutting ads.'}",
                    simple="Each dot is one month. If dots make a line going up-right, ads are working (spend more \u2192 earn more). If dots are scattered randomly, ads might not be helping much.",
                ),
                dcc.Graph(figure=corr_fig, config={"displayModeBar": False}, style={"height": "320px"}),

                chart_context(
                    "Product performance over time. Bright = high revenue. Dark = low/no sales. "
                    "Spot rising stars, declining products, and seasonal patterns.",
                    metrics=[
                        ("Products Tracked", str(len(_top_prod_names)), TEAL),
                        ("Top Product", f"${product_revenue_est.values[0]:,.0f}" if len(product_revenue_est) > 0 else "N/A", GREEN),
                    ],
                    look_for="Products getting brighter over time are growing. Products going dark need attention or retirement.",
                    simple="Each row is a product, each column is a month. Bright green = lots of sales. Dark purple = few sales. Black = no sales. Look for rows getting brighter (growing products) or darker (declining ones).",
                ),
                dcc.Graph(figure=product_heat, config={"displayModeBar": False}, style={"height": "360px"}),
            ]),
        ], style={"marginBottom": "8px"}),

        # Section 3: Unit Economics & Inventory
        html.Details([
            html.Summary("Unit Economics & Inventory", style={
                "color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 0", "listStyle": "none",
            }),
            html.Div([
                html.Div([
                    html.Div([
                        chart_context(
                            f"What happens to each ${avg_order:,.2f} order on average. Every dollar flows through fees, "
                            f"shipping, ads, refunds, and COGS before becoming profit.",
                            metrics=[
                                ("Avg Revenue", money(_unit_rev), GREEN),
                                ("Avg Profit", money(_unit_profit), CYAN),
                                ("Unit Margin", f"{_unit_margin:.1f}%", GREEN if _unit_margin > 20 else ORANGE),
                                ("COGS/Order", money(_unit_cogs), PURPLE),
                            ],
                            look_for="Profit bar should be at least 20% of revenue bar. If supply costs are the biggest deduction, find cheaper suppliers.",
                            simple="Start with what a customer pays (left bar). Each red bar shows where that money goes -- fees, shipping, etc. The blue bar at the end is your actual profit per order. If the blue bar is small, your costs are eating too much.",
                        ),
                        dcc.Graph(figure=unit_wf, config={"displayModeBar": False}, style={"height": "340px"}),
                    ], style={"flex": "3"}),
                    html.Div([
                        html.Div("BREAK-EVEN ANALYSIS", style={"color": CYAN, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "8px"}),
                        row_item("Monthly Fixed Costs", _monthly_fixed, color=RED),
                        row_item("Contribution Margin", _contrib_margin_pct * 100, color=GREEN),
                        html.Div(style={"borderTop": f"1px solid {CYAN}44", "margin": "6px 0"}),
                        row_item("Break-Even Revenue/Mo", _breakeven_monthly, bold=True, color=CYAN),
                        row_item("Break-Even Revenue/Day", _breakeven_daily, color=TEAL),
                        row_item("Break-Even Orders/Mo", _breakeven_orders, color=BLUE),
                        html.Div(style={"height": "10px"}),
                        html.P(f"{'You are ABOVE break-even.' if val_monthly_run_rate > _breakeven_monthly else 'WARNING: Below break-even -- you need more revenue to cover fixed costs.'}"
                               if _breakeven_monthly > 0 else "Insufficient data for break-even.",
                               style={"color": GREEN if val_monthly_run_rate > _breakeven_monthly else RED,
                                      "fontSize": "11px", "fontWeight": "bold"}),
                        html.P(f"Surplus: {money(val_monthly_run_rate - _breakeven_monthly)}/mo above break-even"
                               if val_monthly_run_rate > _breakeven_monthly and _breakeven_monthly > 0 else "",
                               style={"color": TEAL, "fontSize": "11px"}),
                    ], style={"flex": "2", "padding": "12px", "backgroundColor": CARD, "borderRadius": "8px",
                              "border": f"1px solid {CYAN}33"}),
                ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}),

                chart_context(
                    "Revenue (green) vs inventory spend (purple) each month. Orange line = COGS ratio "
                    "(% of revenue going to materials). Rising ratio = margins shrinking.",
                    metrics=[
                        ("Total Inventory", money(true_inventory_cost), PURPLE),
                        ("Supply Cost Ratio", f"{true_inventory_cost / gross_sales * 100:.1f}%" if gross_sales else "N/A", ORANGE),
                        ("Avg Monthly Spend", money(true_inventory_cost / _val_months_operating), TEAL),
                    ],
                    look_for="COGS ratio should stay flat or decrease. If it's rising, you're spending more on materials relative to sales.",
                    simple="Green bars = what you earned. Purple bars = what you spent on supplies. The orange line shows supplies as a percentage of sales. If the orange line goes up, you're spending more on materials relative to what you're earning.",
                ),
                dcc.Graph(figure=rev_inv_fig, config={"displayModeBar": False}, style={"height": "360px"}),

                html.Div([
                    html.Div([
                        chart_context(
                            "What categories of supplies you're buying. Parsed from Amazon invoice PDFs.",
                            metrics=[
                                ("Total Categories", str(len(biz_inv_by_category)), TEAL),
                                ("Biggest", f"{biz_inv_by_category.index[0] if len(biz_inv_by_category) > 0 else 'N/A'}", PURPLE),
                            ],
                            simple="Each bar shows how much you spent on a type of supply (filament, packaging, etc). The tallest bars are where most of your supply money goes. Look for categories that seem too high.",
                        ),
                        dcc.Graph(figure=inv_cat_bar, config={"displayModeBar": False}, style={"height": "340px"}),
                    ], style={"flex": "1"}),
                    html.Div([
                        chart_context(
                            "Inventory shipments split by location. Shows which partner location is ordering more supplies.",
                            metrics=[
                                ("Tulsa", money(tulsa_spend), TEAL),
                                ("Texas", money(texas_spend), ORANGE),
                            ],
                            simple="Shows where your supplies are being shipped -- Tulsa vs Texas. This helps see if one location is ordering way more than the other.",
                        ),
                        dcc.Graph(figure=loc_fig, config={"displayModeBar": False}, style={"height": "340px"}),
                    ], style={"flex": "1"}),
                ], style={"display": "flex", "gap": "8px"}),

                # Supplier analysis (top suppliers)
                *([html.Div([
                    html.Div("TOP SUPPLIERS", style={"color": PURPLE, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "8px"}),
                    *[html.Div([
                        html.Span(seller[:30], style={"color": WHITE, "fontSize": "12px", "flex": "1"}),
                        html.Span(f"{info['items']} items", style={"color": GRAY, "fontSize": "11px", "marginRight": "12px"}),
                        html.Span(f"avg ${info['avg_price']:,.2f}", style={"color": GRAY, "fontSize": "11px", "marginRight": "12px"}),
                        html.Span(money(info['total']), style={"color": PURPLE, "fontFamily": "monospace", "fontSize": "12px", "fontWeight": "bold"}),
                    ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                              "padding": "4px 0", "borderBottom": "1px solid #ffffff08"})
                      for seller, info in list(_supplier_spend.items())[:10]],
                ], style={"backgroundColor": CARD, "padding": "14px", "borderRadius": "8px",
                          "marginBottom": "14px", "borderLeft": f"3px solid {PURPLE}"})] if _supplier_spend else []),
            ]),
        ], style={"marginBottom": "8px"}),

        # Section 4: Cash Flow
        html.Details([
            html.Summary("Cash Flow & Financial Health", style={
                "color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 0", "listStyle": "none",
            }),
            html.Div([
                chart_context(
                    "Green = Etsy deposits to your bank. Red = business expenses paid. Cyan line = net cash flow each month.",
                    metrics=[
                        ("Total Deposits", money(bank_total_deposits), GREEN),
                        ("Total Expenses", money(bank_total_debits), RED),
                        ("Net Cash", money(bank_net_cash), CYAN),
                    ],
                    look_for="Cyan line should stay positive. Negative months mean you spent more than Etsy deposited.",
                    simple="Green bars = money coming IN (Etsy paying you). Red bars = money going OUT (expenses). The cyan line is the difference. Cyan line above zero = you kept more than you spent that month. Below zero = you overspent.",
                ),
                dcc.Graph(figure=cashflow_fig, config={"displayModeBar": False}, style={"height": "360px"}),

                html.Div([
                    _build_kpi_pill("\U0001f504", "INVENTORY TURNOVER",
                             f"{gross_sales / true_inventory_cost:.1f}x" if true_inventory_cost > 0 else "N/A",
                             TEAL,
                             detail=(f"How many times your inventory investment generates revenue. {gross_sales / true_inventory_cost:.1f}x means "
                                     f"every $1 of inventory generates ${gross_sales / true_inventory_cost:.2f} in revenue. "
                                     f"Higher is better. Benchmark: 4-8x for handmade goods." if true_inventory_cost > 0 else "No inventory data.")),
                    _build_kpi_pill("\U0001f4ca", "GROSS MARGIN",
                             f"{(gross_sales - true_inventory_cost) / gross_sales * 100:.1f}%" if gross_sales else "N/A",
                             GREEN, subtitle="Revenue minus COGS only",
                             detail=(f"Revenue ({money(gross_sales)}) minus cost of goods ({money(true_inventory_cost)}) = "
                                     f"{money(gross_sales - true_inventory_cost)} gross profit. This ignores Etsy fees and other expenses. "
                                     f"Benchmark: >60% for handmade, >40% for resale.")),
                    _build_kpi_pill("\U0001f4b0", "OPERATING MARGIN",
                             f"{profit_margin:.1f}%", GREEN if profit_margin > 15 else ORANGE,
                             subtitle="After ALL expenses",
                             detail=(f"Revenue minus all costs. Cash on hand ({money(bank_cash_on_hand)}) "
                                     f"+ owner draws ({money(bank_owner_draw_total)}) = {money(profit)} ({profit_margin:.1f}%). "
                                     f"All expenses flow through bank.")),
                    _build_kpi_pill("\U0001f4b5", "CASH CONVERSION",
                             f"{bank_cash_on_hand / gross_sales * 100:.1f}%" if gross_sales else "N/A",
                             CYAN, subtitle="Cash retained / Revenue",
                             detail=(f"What % of gross sales you actually retained as cash: {money(bank_cash_on_hand)} / {money(gross_sales)}. "
                                     f"The rest went to expenses, inventory, and owner draws. Higher = more efficient cash management.")),
                ], style={"display": "flex", "gap": "8px", "flexWrap": "wrap", "marginBottom": "14px"}),
            ]),
        ], style={"marginBottom": "8px"}),

        # CHATBOT (always visible)
        html.H3("DATA CHATBOT", style={"color": CYAN, "margin": "30px 0 4px 0",
                 "borderTop": f"2px solid {CYAN}33", "paddingTop": "14px"}),
        html.P("Ask any question about your Etsy store data. Try: \"How much money have I made?\" or \"What are my best sellers?\"",
               style={"color": GRAY, "margin": "0 0 14px 0", "fontSize": "13px"}),

        # Quick question buttons
        html.Div([
            html.Button(q, id={"type": "quick-q", "index": i},
                style={
                    "backgroundColor": CARD2, "color": CYAN, "border": f"1px solid {CYAN}44",
                    "borderRadius": "16px", "padding": "6px 14px", "cursor": "pointer",
                    "fontSize": "12px", "margin": "3px",
                })
            for i, q in enumerate([
                "Full summary", "Net profit", "Best sellers",
                "Shipping P/L", "Monthly breakdown", "Refunds",
                "Growth trend", "Best month", "Fees breakdown",
                "Inventory / COGS", "Unit economics",
                "Patterns", "Cash flow", "Valuation", "Debt",
            ])
        ], style={"marginBottom": "14px", "display": "flex", "flexWrap": "wrap", "gap": "4px"}),

        # Chat history (wrapped in loading indicator)
        dcc.Loading(type="dot", color=CYAN, children=[
            html.Div(id="chat-history", children=[
                html.Div([
                    html.Div([
                        html.Div(_build_jarvis_auto_briefing(),
                                 style={"fontSize": "13px", "lineHeight": "1.7", "whiteSpace": "pre-wrap"}),
                    ], style={
                        "backgroundColor": f"{CYAN}15", "border": f"1px solid {CYAN}33",
                        "borderRadius": "12px", "padding": "12px 16px", "maxWidth": "85%",
                        "color": WHITE, "fontSize": "13px", "whiteSpace": "pre-wrap",
                    }),
                ], style={"display": "flex", "justifyContent": "flex-start", "marginBottom": "10px"}),
            ], style={
                "backgroundColor": CARD, "borderRadius": "10px", "padding": "16px",
                "minHeight": "400px", "maxHeight": "600px", "overflowY": "auto",
                "marginBottom": "10px",
            }),
        ]),

        # Input area
        html.Div([
            dcc.Input(
                id="chat-input", type="text",
                placeholder="Ask a question about your data...",
                debounce=False, n_submit=0,
                style={
                    "flex": "1", "padding": "12px 16px", "backgroundColor": CARD2,
                    "color": WHITE, "border": f"1px solid {CYAN}44", "borderRadius": "8px",
                    "fontSize": "14px", "outline": "none",
                },
            ),
            html.Button("Send", id="chat-send", n_clicks=0,
                style={
                    "padding": "12px 28px", "backgroundColor": CYAN, "color": BG,
                    "border": "none", "borderRadius": "8px", "cursor": "pointer",
                    "fontSize": "14px", "fontWeight": "bold",
                }),
        ], style={"display": "flex", "gap": "8px"}),

        # Hidden store for conversation history
        dcc.Store(id="chat-store", data=[]),
    ], style={"padding": TAB_PADDING})
