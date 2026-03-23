"""Tab 4 — Inventory: Business inventory, warehouse views, receipt optimizer, product library."""

from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc
from dashboard_utils.theme import *


def build_tab4_inventory():
    """Tab 4 - Inventory: Business inventory only (personal items under Owner Draws).
    Reorganized with daily-workflow-first design: collapsible sections, snapshot banner."""
    import etsy_dashboard as ed

    # Pull globals from the monolith
    strict_mode = ed.strict_mode
    BIZ_INV_DF = ed.BIZ_INV_DF
    INV_DF = ed.INV_DF
    INV_ITEMS = ed.INV_ITEMS
    _IMAGE_URLS = ed._IMAGE_URLS
    true_inventory_cost = ed.true_inventory_cost
    inv_order_count = ed.inv_order_count
    tulsa_spend = ed.tulsa_spend
    texas_spend = ed.texas_spend
    tulsa_orders = ed.tulsa_orders
    tulsa_subtotal = ed.tulsa_subtotal
    tulsa_tax = ed.tulsa_tax
    texas_orders = ed.texas_orders
    texas_subtotal = ed.texas_subtotal
    texas_tax = ed.texas_tax
    item_thumbnail = ed.item_thumbnail
    _compute_stock_kpis = ed._compute_stock_kpis
    _build_kpi_pill = ed._build_kpi_pill
    _strict_banner = ed._strict_banner
    _build_location_stats_row = ed._build_location_stats_row
    _build_warehouse_card = ed._build_warehouse_card
    _build_receipt_upload_section = ed._build_receipt_upload_section
    _build_inventory_editor = ed._build_inventory_editor
    _build_completed_receipts = ed._build_completed_receipts
    _build_receipt_gallery = ed._build_receipt_gallery
    _build_product_library = ed._build_product_library

    _sm = strict_mode if isinstance(strict_mode, bool) else False
    biz_order_count = len(BIZ_INV_DF)
    skpi = _compute_stock_kpis()

    # ── Shared collapsible header style ───────────────────────────────────
    _det_style = {
        "color": CYAN, "fontSize": "14px", "fontWeight": "bold",
        "cursor": "pointer", "padding": "10px 14px", "listStyle": "none",
        "backgroundColor": "#ffffff08", "borderRadius": "6px",
        "border": f"1px solid {CYAN}33",
    }

    def _sec_header(title, subtitle, color=CYAN, badge=""):
        parts = [
            html.Span("\u25b6 ", style={"fontSize": "12px"}),
            html.Span(title, style={"fontWeight": "bold", "color": color}),
            html.Span(f" \u2014 {subtitle}", style={"color": GRAY, "fontWeight": "normal", "fontSize": "12px"}),
        ]
        if badge:
            parts.append(html.Span(badge, style={
                "marginLeft": "10px", "backgroundColor": f"{ORANGE}22", "color": ORANGE,
                "padding": "2px 10px", "borderRadius": "10px", "fontSize": "11px",
                "fontWeight": "bold", "border": f"1px solid {ORANGE}44",
            }))
        return html.Summary(parts, style={**_det_style, "color": color})

    # ── Snapshot Banner ───────────────────────────────────────────────────
    _low_msg = f" {skpi['low']} items are running low (1-2 left)" if skpi["low"] > 0 else ""
    _oos_msg = f" and {skpi['oos']} are out of stock" if skpi["oos"] > 0 else ""
    _dot = "." if not _low_msg and not _oos_msg else ""
    snapshot_text = (
        f"You have {skpi['in_stock']} items in stock worth ${skpi['value']:,.2f} "
        f"across {skpi['unique']} unique products{_dot}."
        f"{_low_msg}{_oos_msg}{'.' if _low_msg or _oos_msg else ''} "
        f"Total supply spend: ${true_inventory_cost:,.2f} across {inv_order_count} orders."
    )

    # ── Build order & item table rows (used in All Orders / All Items) ────
    order_table_rows = []
    for _, r in INV_DF.iterrows():
        is_personal = r["source"] == "Personal Amazon" or (isinstance(r["file"], str) and "Gigi" in r["file"])
        if is_personal:
            continue
        src = r["source"]
        store_label = "Amazon" if src in ("Key Component Mfg",) else src
        ship_addr = r.get("ship_address", "")
        short_addr = ship_addr.split(",")[1].strip() + ", " + ship_addr.split(",")[2].strip().split(" ")[0] if ship_addr.count(",") >= 2 else ship_addr
        order_table_rows.append(
            html.Tr([
                html.Td(r["date"], style={"color": GRAY, "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(r["order_num"], style={"color": WHITE, "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(store_label, style={"color": TEAL, "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(short_addr, style={"color": CYAN, "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(str(r["item_count"]), style={"textAlign": "center", "color": WHITE,
                                                      "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(f"${r['subtotal']:,.2f}", style={"textAlign": "right", "color": WHITE,
                                                          "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(f"${r['tax']:,.2f}", style={"textAlign": "right", "color": GRAY,
                                                      "padding": "4px 8px", "fontSize": "12px"}),
                html.Td(f"${r['grand_total']:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                              "fontWeight": "bold", "padding": "4px 8px",
                                                              "fontSize": "12px"}),
            ], style={"borderBottom": "1px solid #ffffff10"})
        )

    item_table_rows = []
    items_sorted = INV_ITEMS.sort_values("total", ascending=False)
    for _, r in items_sorted.iterrows():
        if r.get("category", "") in ("Personal/Gift", "Business Fees"):
            continue
        item_src = r.get("source", "")
        store_name = "Amazon" if item_src in ("Key Component Mfg",) else item_src
        ship_loc = r.get("ship_to", "")
        if ship_loc.count(",") >= 2:
            parts = ship_loc.split(",")
            short_ship = parts[1].strip() + ", " + parts[2].strip().split(" ")[0]
        else:
            short_ship = ship_loc
        _img_url = _IMAGE_URLS.get(r["name"], "") or r.get("image_url", "")
        _item_orig = r.get("_orig_name", r["name"])
        _item_renamed = _item_orig != r["name"]
        _item_name_parts = [html.Span(r["name"], style={"color": WHITE})]
        if _item_renamed:
            _item_name_parts.append(html.Br())
            _item_name_parts.append(html.Span(
                _item_orig[:55], title=_item_orig,
                style={"color": DARKGRAY, "fontSize": "9px", "fontStyle": "italic"}))
        item_table_rows.append(
            html.Tr([
                html.Td(item_thumbnail(_img_url, 32), style={"padding": "4px 6px", "textAlign": "center", "width": "40px"}),
                html.Td(html.Span("INVENTORY", style={
                    "backgroundColor": "#00e67622", "color": GREEN, "padding": "2px 8px",
                    "borderRadius": "10px", "fontSize": "10px", "fontWeight": "600",
                    "letterSpacing": "0.5px"}), style={"padding": "4px 8px", "textAlign": "center"}),
                html.Td(_item_name_parts, title=f"{r['name']}\nFrom: {_item_orig}" if _item_renamed else r["name"],
                         style={"padding": "4px 8px", "fontSize": "11px",
                                "maxWidth": "350px", "overflow": "hidden",
                                "textOverflow": "ellipsis"}),
                html.Td(r.get("category", "Other"), style={"color": TEAL, "padding": "4px 8px", "fontSize": "11px"}),
                html.Td(store_name, style={"color": CYAN, "padding": "4px 8px", "fontSize": "11px"}),
                html.Td(short_ship[:30], style={"color": GRAY, "padding": "4px 8px", "fontSize": "11px"}),
                html.Td(str(r["qty"]), style={"textAlign": "center", "color": WHITE,
                                               "padding": "4px 8px", "fontSize": "11px"}),
                html.Td(f"${r['price']:,.2f}", style={"textAlign": "right", "color": WHITE,
                                                        "padding": "4px 8px", "fontSize": "11px"}),
                html.Td(f"${r['total']:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                        "fontWeight": "bold", "padding": "4px 8px",
                                                        "fontSize": "11px"}),
                html.Td(r["date"], style={"color": GRAY, "padding": "4px 8px", "fontSize": "11px"}),
            ], style={"borderBottom": "1px solid #ffffff10"})
        )

    # Build editor (returns content + unsaved count)
    _editor_result = _build_inventory_editor()
    if isinstance(_editor_result, tuple):
        _editor_content, _editor_unsaved = _editor_result
    else:
        _editor_content, _editor_unsaved = _editor_result, 0
    _unsaved_badge = f"{_editor_unsaved} unsaved" if _editor_unsaved > 0 else ""
    _editor_section = html.Details([
        _sec_header("ITEM NAMING EDITOR", "Name, categorize, and locate receipt items", color=ORANGE, badge=_unsaved_badge),
        _editor_content,
    ], open=True,
       style={"marginBottom": "14px"})

    tulsa_pct = (tulsa_spend / true_inventory_cost * 100) if true_inventory_cost else 0
    texas_pct = (texas_spend / true_inventory_cost * 100) if true_inventory_cost else 0

    return html.Div([

        # Strict mode banner
        _strict_banner("Health scores and valuation estimates are hidden. Purchase records and stock levels still shown.") if _sm else html.Div(),

        # Hidden components needed by callbacks
        dcc.Store(id="editor-save-trigger", data=0),
        html.Div(id="inv-kpi-row", style={"display": "none"}),
        html.Div(id="qa-panel", style={"display": "none"}, children=[
            html.Button(id="qa-toggle-btn", n_clicks=0, style={"display": "none"}),
        ]),
        # Hidden compat for edit inventory table callbacks
        html.Div([
            dash_table.DataTable(id="inv-qty-table", columns=[{"name": "x", "id": "x"}],
                                  data=[], style_table={"display": "none"}),
            html.Button(id="inv-qty-save-btn", n_clicks=0, style={"display": "none"}),
            html.Span(id="inv-qty-save-status", style={"display": "none"}),
            html.Button(id="inv-img-save-btn", n_clicks=0, style={"display": "none"}),
            html.Span(id="inv-img-save-status", style={"display": "none"}),
        ], style={"display": "none"}),

        # ══════════════════════════════════════════════════════════════════════
        # STATS
        # ══════════════════════════════════════════════════════════════════════
        html.Div([
            html.H3("INVENTORY", style={
                "color": CYAN, "margin": "0 0 14px 0", "fontSize": "26px",
                "fontWeight": "700", "letterSpacing": "1.5px",
                "textShadow": f"0 0 20px {CYAN}22"}),
            html.Div([
                _build_kpi_pill("#", "IN STOCK", str(skpi["in_stock"]), GREEN,
                                f"{skpi['unique']} unique"),
                _build_kpi_pill("$", "TOTAL SPEND", f"${true_inventory_cost:,.2f}", TEAL,
                                f"{inv_order_count} orders"),
                _build_kpi_pill("!", "LOW STOCK", str(skpi["low"]), ORANGE,
                                "need reorder"),
                _build_kpi_pill("\u2716", "OUT OF STOCK", str(skpi["oos"]), RED,
                                "empty"),
            ], style={"display": "flex", "gap": "10px", "marginBottom": "14px", "flexWrap": "wrap"}),
            html.Div(
                html.P(snapshot_text, style={"color": WHITE, "fontSize": "13px", "margin": "0", "lineHeight": "1.6"}),
                style={"borderLeft": f"4px solid {CYAN}", "backgroundColor": CARD,
                       "padding": "14px 18px", "borderRadius": "8px"},
            ),
        ], style={"marginBottom": "20px"}),

        # ══════════════════════════════════════════════════════════════════════
        # TULSA vs TEXAS — STATS COMPARISON
        # ══════════════════════════════════════════════════════════════════════
        html.Div([
            html.H3("TULSA vs TEXAS", style={
                "color": WHITE, "margin": "0 0 14px 0", "fontSize": "20px",
                "fontWeight": "700", "letterSpacing": "1px",
                "textShadow": "0 0 20px rgba(255,255,255,0.05)"}),
            _build_location_stats_row(),
        ], style={"marginBottom": "20px"}),

        # ══════════════════════════════════════════════════════════════════════
        # WAREHOUSE INVENTORY (side by side)
        # ══════════════════════════════════════════════════════════════════════
        html.Div([
            html.H3("WAREHOUSE INVENTORY", style={
                "color": CYAN, "margin": "0 0 14px 0", "fontSize": "20px",
                "fontWeight": "700", "letterSpacing": "1px"}),
            html.Div([
                _build_warehouse_card("TJ (Tulsa, OK)", "Tulsa", TEAL,
                                       tulsa_spend, tulsa_orders, tulsa_subtotal, tulsa_tax, tulsa_pct),
                _build_warehouse_card("BRADEN (Texas)", "Texas", ORANGE,
                                       texas_spend, texas_orders, texas_subtotal, texas_tax, texas_pct),
            ], id="location-inventory-display",
               style={"display": "flex", "gap": "14px", "flexWrap": "wrap"}),
        ], style={"marginBottom": "20px"}),

        # ══════════════════════════════════════════════════════════════════════
        # RECEIPT TO INVENTORY OPTIMIZER
        # ══════════════════════════════════════════════════════════════════════
        html.Div([
            html.H3("RECEIPT TO INVENTORY OPTIMIZER", style={
                "color": PURPLE, "margin": "0 0 14px 0", "fontSize": "20px",
                "fontWeight": "700", "letterSpacing": "1px"}),
            _build_receipt_upload_section(),
            html.Div(style={"marginTop": "14px"}),
            _editor_section,
        ]),

        # ══════════════════════════════════════════════════════════════════════
        # COMPLETED RECEIPTS
        # ══════════════════════════════════════════════════════════════════════
        _build_completed_receipts(),

        # ══════════════════════════════════════════════════════════════════════
        # RECEIPT GALLERY
        # ══════════════════════════════════════════════════════════════════════
        _build_receipt_gallery(),

        # ══════════════════════════════════════════════════════════════════════
        # PRODUCT LIBRARY
        # ══════════════════════════════════════════════════════════════════════
        _build_product_library(),

    ], style={"padding": TAB_PADDING})
