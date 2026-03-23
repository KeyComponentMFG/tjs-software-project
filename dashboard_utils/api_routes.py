"""
API Routes Registry -- Documentation of all /api/ endpoints in etsy_dashboard.py.

These routes are currently defined in the monolith (etsy_dashboard.py).
They will be extracted to a Flask Blueprint in a future phase (Phase 5b)
once the globals they depend on are available via StateManager.

TODO: Extract once StateManager provides all needed data.
      Use Blueprint + getter pattern to avoid circular imports.

===============================================================================
ROUTE CATALOG  (31 routes total)
===============================================================================

--- Static File Serving ---

GET  /api/receipt/<subfolder>/<path:filename>
     Func: serve_receipt_pdf
     Serves invoice PDF files from data/invoices/<subfolder>/
     Reads: BASE_DIR (os.path)

--- Debug / Diagnostics ---

GET  /api/diagnostics
     Func: api_diagnostics
     Full diagnostic dump: Supabase connectivity, env vars, all key financial
     metrics (Etsy, bank, profit), expense receipt matching stats.
     Reads: DATA, BANK_TXNS, gross_sales, total_fees, total_shipping_cost,
            total_marketing, total_refunds, total_taxes, total_buyer_fees,
            etsy_net, etsy_balance, bank_total_deposits, bank_total_debits,
            bank_net_cash, bank_by_cat, bank_owner_draw_total,
            tulsa_draw_total, texas_draw_total, bank_cash_on_hand,
            real_profit, profit, real_profit_margin,
            expense_missing_receipts, expense_matched_count, IS_RAILWAY

GET  /api/debug-pipeline
     Func: api_debug_pipeline
     Step-by-step import test of accounting package (models, ledger, journal,
     pipeline). Tries full_rebuild and reports expense completeness.
     Reads: DATA, BANK_TXNS, CONFIG, INVOICES, _acct_pipeline

GET  /api/debug-expenses
     Func: api_debug_expenses
     Expense completeness debugging. Runs ExpenseCompletenessAgent manually
     if pipeline result is None. Reports matched/missing/gap.
     Reads: _acct_pipeline, INVOICES, BANK_TXNS, BASE_DIR (vendor_map.json)

--- Health / Briefing / Actions ---

GET  /api/health
     Func: api_health
     Business health score with sub-scores, grades, and callback error counts.
     Reads: _compute_health_score(), get_error_summary()

GET  /api/briefing
     Func: api_briefing
     AI-generated daily briefing paragraphs.
     Reads: _generate_briefing()

GET  /api/actions
     Func: api_actions
     Priority action items with impact estimates.
     Reads: _generate_actions()

--- Composite Overview ---

GET  /api/overview
     Func: api_overview
     Complete dashboard overview for React app: health score, briefing,
     top 5 actions, KPIs, last 6 months trend, expenses, bank summary.
     Reads: _compute_health_score(), _generate_briefing(), _generate_actions(),
            gross_sales, etsy_net, real_profit, real_profit_margin,
            bank_cash_on_hand, order_count, avg_order, monthly_sales,
            months_sorted, total_fees, total_shipping_cost, total_marketing,
            total_refunds, bank_owner_draw_total, bank_total_deposits,
            bank_total_debits

--- Financial Data ---

GET  /api/financials
     Func: api_financials
     Detailed financial breakdown: revenue, fees (listing/transaction/processing),
     profit, bank, owner draws, shipping (buyer_paid=None), last 12 months.
     Reads: gross_sales, total_refunds, net_sales, total_fees, listing_fees,
            transaction_fees_product, transaction_fees_shipping, processing_fees,
            total_marketing, total_shipping_cost, etsy_net, real_profit,
            real_profit_margin, bank_total_deposits, bank_total_debits,
            bank_cash_on_hand, bank_by_cat, bank_owner_draw_total,
            tulsa_draw_total, texas_draw_total, monthly_sales, monthly_fees,
            monthly_shipping, monthly_order_counts, months_sorted

GET  /api/tax
     Func: api_tax
     Tax estimates: self-employment tax, income tax, quarterly payments,
     50/50 partnership split, deductions.
     Reads: real_profit, total_shipping_cost, total_fees, total_marketing,
            bank_biz_expense_total, _compute_income_tax()

GET  /api/pnl
     Func: api_pnl
     Detailed Profit & Loss statement: revenue, Etsy fees, shipping,
     marketing (ads + offsite), bank expenses by category, owner draws.
     Reads: gross_sales, total_refunds, net_sales, listing_fees,
            transaction_fees_product, transaction_fees_shipping,
            processing_fees, total_fees, total_shipping_cost, etsy_ads,
            offsite_ads_fees, total_marketing, etsy_net, bank_by_cat,
            bank_owner_draw_total, real_profit, real_profit_margin,
            bank_cash_on_hand

--- Bank ---

GET  /api/bank/ledger
     Func: api_bank_ledger
     Last 100 bank transactions with running balance.
     Reads: bank_txns_sorted, bank_total_deposits, bank_total_debits,
            bank_cash_on_hand

GET  /api/bank/summary
     Func: api_bank_summary
     Bank summary: balance, deposits, debits, expense categories,
     owner draws (Tulsa vs Texas split + difference + owed_to).
     Reads: bank_cash_on_hand, bank_total_deposits, bank_total_debits,
            bank_by_cat, bank_owner_draw_total, tulsa_draw_total,
            texas_draw_total, draw_diff, draw_owed_to, bank_monthly

--- Inventory / Valuation ---

GET  /api/inventory/summary
     Func: api_inventory_summary
     Inventory/COGS: total items, total cost, by category, by location,
     low stock and out-of-stock lists (top 20 each).
     Reads: STOCK_SUMMARY

GET  /api/valuation
     Func: api_valuation
     Business valuation using SDE, monthly profit, and revenue multiples.
     Age-based weighting, risk factors, blended estimate (low/mid/high),
     asset/liability summary, growth guidance.
     Reads: months_sorted, gross_sales, real_profit, real_profit_margin,
            bank_owner_draw_total, bank_cash_on_hand, etsy_balance,
            true_inventory_cost, bb_cc_balance

--- Shipping / Fees ---

GET  /api/shipping
     Func: api_shipping
     Shipping analysis: label breakdown (USPS outbound/returns, Asendia),
     order counts (paid vs free shipping), avg label cost.
     buyer_paid/profit/margin all None (not in Etsy CSV).
     Reads: total_shipping_cost, usps_outbound, usps_outbound_count,
            usps_return, usps_return_count, asendia_labels, asendia_count,
            paid_ship_count, free_ship_count, avg_outbound_label

GET  /api/fees
     Func: api_fees
     Fee breakdown: listing, transaction (product + shipping), processing,
     credits (listing/transaction/processing/share_save), marketing
     (Etsy ads + offsite ads), fees as percent of sales.
     Reads: total_fees, listing_fees, transaction_fees_product,
            transaction_fees_shipping, processing_fees, credit_listing,
            credit_transaction, credit_processing, share_save, total_credits,
            etsy_ads, offsite_ads_fees, offsite_ads_credits, gross_sales

--- Configuration ---

GET/POST/OPTIONS  /api/config/credit-card
     Func: api_credit_card_config
     GET: Return current credit card config (limit, balance, purchases).
     POST: Update credit card balance/purchases, save to Supabase config.
     Mutates globals: bb_cc_balance, bb_cc_limit, bb_cc_purchases,
                      bb_cc_total_charged, bb_cc_total_paid, bb_cc_available,
                      bb_cc_asset_value, CONFIG
     Reads: bb_cc_balance, bb_cc_limit, bb_cc_total_charged, bb_cc_total_paid,
            bb_cc_available, bb_cc_purchases, _save_config_value()

--- AI Chat ---

POST/OPTIONS  /api/chat
     Func: api_chat
     Chat endpoint for React app. Accepts message + optional history,
     returns AI response via chatbot_answer().
     Reads: chatbot_answer()

--- Reconciliation ---

GET  /api/reconciliation
     Func: api_reconciliation
     Reconciliation report: compares dashboard values to raw CSV sums.
     Optional ?start=&end= date filters. Checks gross sales, fees,
     shipping, marketing, refunds, taxes, buyer fees, Etsy net, bank.
     Reads: DATA, gross_sales, total_fees, total_shipping_cost,
            total_marketing, total_refunds, total_taxes, total_buyer_fees,
            etsy_net_earned, bank_total_deposits, bank_total_debits,
            bank_deposits, etsy_pre_capone_deposits, etsy_total_deposited

--- Testing / Reload ---

GET  /api/test-upload
     Func: api_test_upload
     Re-runs _cascade_reload without file upload. Reports key metrics.
     Reads: _rebuild_etsy_derived(), _cascade_reload(), DATA, order_count,
            gross_sales, etsy_net, etsy_balance, total_fees, real_profit,
            bank_cash_on_hand

GET  /api/reload
     Func: api_reload
     Force-reload all data from Supabase. Runs full rebuild pipeline.
     Mutates globals: DATA, CONFIG, INVOICES, BANK_TXNS (and all derived)
     Reads: _load_data(), _rebuild_etsy_derived(), _rebuild_bank_derived(),
            _cascade_reload()

--- Chart Data ---

GET  /api/charts/monthly-performance
     Func: api_charts_monthly_performance
     Monthly data: sales, fees, shipping, marketing, refunds, net, orders, AOV.
     Reads: months_sorted, monthly_sales, monthly_fees, monthly_shipping,
            monthly_marketing, monthly_refunds, monthly_net_revenue,
            monthly_order_counts, monthly_aov

GET  /api/charts/daily-sales
     Func: api_charts_daily_sales
     Last 90 days: daily revenue, orders, 7-day and 30-day rolling averages.
     Reads: daily_df

GET  /api/charts/expense-breakdown
     Func: api_charts_expense_breakdown
     Expense pie/donut data: fees, shipping, marketing, refunds, COGS.
     Reads: total_fees, total_shipping_cost, total_marketing, total_refunds,
            true_inventory_cost, gross_sales

GET  /api/charts/cash-flow
     Func: api_charts_cash_flow
     Monthly cash flow: deposits, debits, net.
     Reads: bank_monthly

GET  /api/charts/products
     Func: api_charts_products
     Top 12 products by revenue.
     Reads: product_revenue_est

GET  /api/charts/health-breakdown
     Func: api_charts_health_breakdown
     Health score gauge data: profit margin, revenue trend, order velocity,
     fee efficiency, shipping economics, cash position.
     Reads: profit_margin, monthly_sales, order_count, days_active,
            total_fees, gross_sales, shipping_profit, shipping_margin,
            bank_cash_on_hand, bank_all_expenses

GET  /api/charts/projections
     Func: api_charts_projections
     Revenue/profit projections: historical data + 3-month linear projection.
     Reads: monthly_sales, monthly_net_revenue, months_sorted

--- CEO Alerts ---

GET  /api/ceo/dismiss
     Func: api_ceo_dismiss
     Dismiss a CEO alert by ?key= parameter. Saves to Supabase config.
     Redirects to /. Not JSON -- returns redirect.
     Reads: _dismissed_alerts, save_config_value()

===============================================================================
CORS: All /api/ paths get CORS headers via @server.after_request handler.
      _add_cors_headers() sets Access-Control-Allow-Origin: *
===============================================================================

GLOBALS DEPENDENCY SUMMARY (unique globals referenced across all routes):

  Data sources:      DATA, BANK_TXNS, CONFIG, INVOICES, STOCK_SUMMARY
  Revenue:           gross_sales, net_sales, etsy_net, etsy_net_earned,
                     etsy_balance, etsy_total_deposited, etsy_pre_capone_deposits
  Fees:              total_fees, listing_fees, transaction_fees_product,
                     transaction_fees_shipping, processing_fees, total_credits,
                     credit_listing, credit_transaction, credit_processing,
                     share_save, total_buyer_fees, total_taxes
  Marketing:         total_marketing, etsy_ads, offsite_ads_fees, offsite_ads_credits
  Shipping:          total_shipping_cost, usps_outbound, usps_outbound_count,
                     usps_return, usps_return_count, asendia_labels, asendia_count,
                     paid_ship_count, free_ship_count, avg_outbound_label,
                     shipping_profit, shipping_margin
  Refunds:           total_refunds
  Profit:            profit, real_profit, real_profit_margin, profit_margin
  Bank:              bank_total_deposits, bank_total_debits, bank_net_cash,
                     bank_cash_on_hand, bank_by_cat, bank_monthly,
                     bank_owner_draw_total, tulsa_draw_total, texas_draw_total,
                     draw_diff, draw_owed_to, bank_deposits, bank_txns_sorted,
                     bank_biz_expense_total, bank_all_expenses
  Orders:            order_count, avg_order, days_active
  Monthly:           months_sorted, monthly_sales, monthly_fees, monthly_shipping,
                     monthly_marketing, monthly_refunds, monthly_net_revenue,
                     monthly_order_counts, monthly_aov
  Daily:             daily_df
  Products:          product_revenue_est
  Inventory:         true_inventory_cost
  Credit card:       bb_cc_balance, bb_cc_limit, bb_cc_purchases, bb_cc_total_charged,
                     bb_cc_total_paid, bb_cc_available, bb_cc_asset_value
  Pipeline:          _acct_pipeline
  Alerts:            _dismissed_alerts
  Helpers:           _compute_health_score(), _generate_briefing(), _generate_actions(),
                     _compute_income_tax(), chatbot_answer(), get_error_summary(),
                     _save_config_value(), _load_data(), _rebuild_etsy_derived(),
                     _rebuild_bank_derived(), _cascade_reload()
  Env:               IS_RAILWAY, BASE_DIR
"""
