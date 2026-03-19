"""Inventory page — mirrors the Railway monolith's build_tab4_inventory() exactly.
Header + KPI pills + snapshot banner + Quick Add toggle + inventory editor +
receipt upload + warehouses + spending analytics + all orders + all items + receipt gallery."""
from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from etsy_dashboard.theme import *
from etsy_dashboard.components.kpi import kpi_pill
from etsy_dashboard.components.cards import section, row_item, make_chart
from etsy_dashboard.components.tables import stock_level_bar
from etsy_dashboard.components.thumbnail import item_thumbnail
from etsy_dashboard import data_state as ds


# ── Shared collapsible header style ──────────────────────────────────────────
_DET_STYLE = {
    "color": CYAN, "fontSize": "14px", "fontWeight": "bold",
    "cursor": "pointer", "padding": "10px 14px", "listStyle": "none",
    "backgroundColor": "#ffffff08", "borderRadius": "6px",
    "border": f"1px solid {CYAN}33",
}


def _sec_header(title, subtitle, color=CYAN, badge=""):
    """Bold title + gray subtitle for collapsible section headers."""
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
    return html.Summary(parts, style={**_DET_STYLE, "color": color})


# ══════════════════════════════════════════════════════════════════════════════
#  KPI PILL STRIP  (mirrors monolith _build_inv_kpi_row)
# ══════════════════════════════════════════════════════════════════════════════

def _build_kpi_row():
    """Premium KPI pill strip — 6 pills matching Railway layout."""
    k = ds._compute_stock_kpis()
    biz_count = len(ds.BIZ_INV_DF)
    cogs_pct = (f"{ds.true_inventory_cost / ds.gross_sales * 100:.1f}%"
                if ds.gross_sales else "N/A")
    cogs_label = ("healthy" if ds.gross_sales and
                  (ds.true_inventory_cost / ds.gross_sales * 100) < 25 else "moderate")
    return html.Div([
        kpi_pill("#", "IN STOCK", str(k["in_stock"]), GREEN,
                 f"{k['unique']} unique"),
        kpi_pill("$", "VALUE", f"${k['value']:,.2f}", TEAL,
                 "total spend"),
        kpi_pill("!", "LOW STOCK", str(k["low"]), ORANGE,
                 "need reorder"),
        kpi_pill("\u2716", "OUT OF STOCK", str(k["oos"]), RED,
                 "empty"),
        kpi_pill("%", "SUPPLY COSTS", cogs_pct, PURPLE,
                 cogs_label),
        kpi_pill("=", "ORDERS", str(biz_count), BLUE,
                 f"T:{ds.tulsa_orders}/TX:{ds.texas_orders}"),
    ], style={"display": "flex", "gap": "10px", "marginBottom": "18px", "flexWrap": "wrap"})


# ══════════════════════════════════════════════════════════════════════════════
#  SNAPSHOT BANNER
# ══════════════════════════════════════════════════════════════════════════════

def _build_snapshot_banner():
    """Dark card with left CYAN border summarizing inventory state."""
    skpi = ds._compute_stock_kpis()
    low_msg = f" {skpi['low']} items are running low (1-2 left)" if skpi["low"] > 0 else ""
    oos_msg = f" and {skpi['oos']} are out of stock" if skpi["oos"] > 0 else ""
    dot = "." if not low_msg and not oos_msg else ""

    text = (
        f"You have {skpi['in_stock']} items in stock worth ${skpi['value']:,.2f} "
        f"across {skpi['unique']} unique products{dot}."
        f"{low_msg}{oos_msg}{'.' if low_msg or oos_msg else ''} "
        f"Total supply spend: {ds.money(ds.true_inventory_cost)} across {ds.inv_order_count} orders."
    )
    return html.Div(
        html.P(text, style={"color": WHITE, "fontSize": "13px", "margin": "0", "lineHeight": "1.6"}),
        style={"borderLeft": f"4px solid {CYAN}", "backgroundColor": CARD,
               "padding": "14px 18px", "borderRadius": "8px", "marginBottom": "16px"},
    )


# ══════════════════════════════════════════════════════════════════════════════
#  QUICK ADD FORM  (mirrors monolith _build_quick_add_form)
# ══════════════════════════════════════════════════════════════════════════════

def _build_quick_add_form():
    """Quick-Add form for manual inventory entries + recent adds list."""
    cat_options = [{"label": c, "value": c} for c in CATEGORY_OPTIONS]
    loc_options = [{"label": "Tulsa, OK", "value": "Tulsa, OK"},
                   {"label": "Texas", "value": "Texas"},
                   {"label": "Other", "value": "Other"}]
    _inp = {"fontSize": "12px", "backgroundColor": "#1a1a2e", "color": WHITE,
            "border": f"1px solid {DARKGRAY}", "borderRadius": "4px", "padding": "5px 8px"}

    # Recent quick-adds list
    qa_rows = []
    for qa in ds._QUICK_ADDS[:20]:
        created = qa.get("created_at", "")[:10] if qa.get("created_at") else ""
        qa_rows.append(html.Tr([
            html.Td(created, style={"color": GRAY, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(qa.get("item_name", ""), style={"color": WHITE, "padding": "3px 8px", "fontSize": "12px"}),
            html.Td(qa.get("category", ""), style={"color": TEAL, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(str(qa.get("qty", 1)), style={"textAlign": "center", "color": WHITE,
                                                     "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(f"${float(qa.get('unit_price', 0)):,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                                        "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(qa.get("location", ""), style={"color": GRAY, "padding": "3px 6px", "fontSize": "11px"}),
            html.Td(
                html.Button("Del", id={"type": "del-qa-btn", "index": str(qa["id"])},
                            n_clicks=0,
                            style={"backgroundColor": "transparent", "color": RED,
                                   "border": f"1px solid {RED}44", "borderRadius": "4px",
                                   "padding": "2px 6px", "fontSize": "10px", "cursor": "pointer"}),
                style={"padding": "3px 4px"}),
        ], style={"borderBottom": "1px solid #ffffff08"}))

    return html.Div([
        # Form row
        html.Div([
            html.Div([
                html.Span("Name:", style={"color": GRAY, "fontSize": "11px", "marginRight": "4px"}),
                dcc.Input(id="qa-name", type="text", placeholder="Item name...",
                          style={**_inp, "width": "180px"}),
            ], style={"display": "flex", "alignItems": "center", "marginRight": "8px"}),
            html.Div([
                html.Span("Category:", style={"color": GRAY, "fontSize": "11px", "marginRight": "4px"}),
                dbc.Select(id="qa-category", options=cat_options, value="Other",
                           style={"width": "140px", "fontSize": "12px",
                                  "backgroundColor": "#1a1a2e", "color": WHITE}),
            ], style={"display": "flex", "alignItems": "center", "marginRight": "8px"}),
            html.Div([
                html.Span("Qty:", style={"color": GRAY, "fontSize": "11px", "marginRight": "4px"}),
                dcc.Input(id="qa-qty", type="number", min=1, value=1,
                          style={**_inp, "width": "55px"}),
            ], style={"display": "flex", "alignItems": "center", "marginRight": "8px"}),
            html.Div([
                html.Span("Price:", style={"color": GRAY, "fontSize": "11px", "marginRight": "4px"}),
                dcc.Input(id="qa-price", type="number", min=0, step=0.01, value=0,
                          style={**_inp, "width": "70px"}),
            ], style={"display": "flex", "alignItems": "center", "marginRight": "8px"}),
            html.Div([
                html.Span("Location:", style={"color": GRAY, "fontSize": "11px", "marginRight": "4px"}),
                dbc.Select(id="qa-location", options=loc_options, value="Tulsa, OK",
                           style={"width": "110px", "fontSize": "12px",
                                  "backgroundColor": "#1a1a2e", "color": WHITE}),
            ], style={"display": "flex", "alignItems": "center", "marginRight": "8px"}),
            html.Button("Add Item", id="qa-add-btn", n_clicks=0,
                        style={"backgroundColor": GREEN, "color": WHITE, "border": "none",
                               "borderRadius": "4px", "padding": "6px 16px", "fontSize": "12px",
                               "cursor": "pointer", "fontWeight": "bold"}),
            html.Span("", id="qa-status",
                       style={"color": GREEN, "fontSize": "11px", "marginLeft": "8px"}),
        ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap",
                  "gap": "4px", "marginBottom": "12px"}),

        # Recent quick-adds
        html.Div([
            html.Table([
                html.Thead(html.Tr([
                    html.Th("Date", style={"textAlign": "left", "padding": "4px 6px"}),
                    html.Th("Item", style={"textAlign": "left", "padding": "4px 8px"}),
                    html.Th("Category", style={"textAlign": "left", "padding": "4px 6px"}),
                    html.Th("Qty", style={"textAlign": "center", "padding": "4px 6px"}),
                    html.Th("Price", style={"textAlign": "right", "padding": "4px 6px"}),
                    html.Th("Location", style={"textAlign": "left", "padding": "4px 6px"}),
                    html.Th("", style={"width": "50px"}),
                ], style={"borderBottom": f"1px solid {GREEN}44"})),
                html.Tbody(qa_rows),
            ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
        ], id="qa-list", style={"maxHeight": "200px", "overflowY": "auto"}) if qa_rows else html.Div(id="qa-list"),
    ])


# ══════════════════════════════════════════════════════════════════════════════
#  INVENTORY EDITOR  (mirrors monolith _build_inventory_editor — simplified)
# ══════════════════════════════════════════════════════════════════════════════

def _build_inventory_editor():
    """Per-order item editor for naming, categorizing, and locating inventory items."""
    if len(ds.INV_ITEMS) == 0:
        return html.Div(id="editor-items-container")

    cat_options = [{"label": c, "value": c} for c in CATEGORY_OPTIONS]
    loc_options = [{"label": "Tulsa, OK", "value": "Tulsa, OK"},
                   {"label": "Texas", "value": "Texas"},
                   {"label": "Other", "value": "Other"}]

    order_cards = []
    saved_count = 0
    total_items = 0

    sorted_orders = sorted(ds.INVOICES, key=lambda o: o.get("date", ""), reverse=True)

    for inv in sorted_orders:
        onum = inv["order_num"]
        is_personal = (inv.get("source") == "Personal Amazon" or
                       (isinstance(inv.get("file", ""), str) and "Gigi" in inv.get("file", "")))
        source_label = ("Personal Amazon" if is_personal else
                        ("Amazon" if inv.get("source") in ("Key Component Mfg",) else inv.get("source", "")))
        ship_addr = inv.get("ship_address", "")
        orig_location = ds.classify_location(ship_addr)

        item_rows = []
        order_saved = 0
        order_total_items = 0

        for item in inv["items"]:
            item_name = item["name"]
            if item_name.startswith("Your package was left near the front door or porch."):
                item_name = item_name.replace("Your package was left near the front door or porch.", "").strip()

            total_items += 1
            order_total_items += 1
            auto_cat = ds.categorize_item(item_name)
            orig_qty = item["qty"]
            price = item["price"]

            detail_key = (onum, item["name"])
            existing = ds._ITEM_DETAILS.get(detail_key, [])
            has_details = bool(existing)
            if has_details:
                saved_count += 1
                order_saved += 1

            if existing:
                det0 = existing[0]
                det_name = det0["display_name"]
                det_cat = det0["category"]
                det_qty = sum(d.get("true_qty", 1) for d in existing)
                det_loc = det0.get("location", "") or orig_location
            else:
                det_name = item_name
                det_cat = auto_cat
                det_qty = orig_qty
                det_loc = orig_location

            status_color = GREEN if has_details else ORANGE
            status_text = "\u2713 SAVED" if has_details else "NEEDS REVIEW"
            img_url = ds._IMAGE_URLS.get(item_name, "")

            item_rows.append(html.Div([
                # Thumbnail
                item_thumbnail(item_name, img_url, 36) if img_url else html.Div(
                    "?", style={"width": "36px", "height": "36px", "display": "inline-flex",
                                "alignItems": "center", "justifyContent": "center",
                                "backgroundColor": "#ffffff10", "borderRadius": "4px",
                                "color": DARKGRAY, "fontSize": "12px", "fontWeight": "bold"}),
                # Info
                html.Div([
                    html.Div([
                        html.Span(det_name[:50], style={"color": WHITE, "fontSize": "12px", "fontWeight": "600"}),
                        html.Span(f"  {det_cat}", style={"color": CATEGORY_COLORS.get(det_cat, GRAY),
                                                           "fontSize": "10px", "marginLeft": "8px"}),
                    ]),
                    html.Div([
                        html.Span(f"Qty: {det_qty}", style={"color": GRAY, "fontSize": "11px"}),
                        html.Span(f" \u00b7 ${price:,.2f} ea", style={"color": GRAY, "fontSize": "11px",
                                                                        "marginLeft": "8px"}),
                        html.Span(f" \u00b7 {det_loc}", style={"color": CYAN, "fontSize": "11px",
                                                                  "marginLeft": "8px"}),
                    ]),
                ], style={"flex": "1", "marginLeft": "10px", "minWidth": "0"}),
                # Status
                html.Span(status_text, style={"color": status_color, "fontSize": "10px",
                                               "fontWeight": "bold", "padding": "2px 8px",
                                               "borderRadius": "8px",
                                               "backgroundColor": f"{status_color}18",
                                               "border": f"1px solid {status_color}33",
                                               "whiteSpace": "nowrap"}),
            ], style={"display": "flex", "alignItems": "center", "padding": "6px 0",
                      "borderBottom": "1px solid #ffffff08"}))

        if not item_rows:
            continue

        order_total = sum(it["price"] * it["qty"] for it in inv["items"])
        _loc_color = TEAL if "Tulsa" in orig_location else (ORANGE if "Texas" in orig_location else GRAY)
        _prog_color = GREEN if order_saved == order_total_items else ORANGE
        _order_all_done = order_saved == order_total_items and order_total_items > 0
        _order_pct = round(order_saved / order_total_items * 100) if order_total_items > 0 else 0

        # Mini progress bar
        mini_progress = html.Div([
            html.Div(style={"width": f"{max(_order_pct, 3)}%", "height": "6px",
                            "background": f"linear-gradient(90deg, {_prog_color}88, {_prog_color})",
                            "borderRadius": "3px", "transition": "width 0.3s ease"}),
        ], style={"width": "120px", "height": "6px", "backgroundColor": "#0d0d1a",
                  "borderRadius": "3px", "display": "inline-block", "verticalAlign": "middle",
                  "marginLeft": "10px", "overflow": "hidden"})

        done_pill = (html.Span("\u2713 ALL DONE", style={
            "fontSize": "12px", "fontWeight": "bold", "padding": "4px 14px",
            "borderRadius": "12px", "backgroundColor": f"{GREEN}22", "color": GREEN,
            "border": f"1px solid {GREEN}44", "marginLeft": "10px",
        }) if _order_all_done else None)

        order_card = html.Div([
            html.Div([
                html.Div([c for c in [
                    html.Span(f"ORDER #{onum}", style={"color": CYAN, "fontWeight": "bold", "fontSize": "16px"}),
                    html.Span(orig_location, style={
                        "fontSize": "11px", "padding": "3px 12px", "borderRadius": "12px",
                        "backgroundColor": f"{_loc_color}22", "color": _loc_color,
                        "border": f"1px solid {_loc_color}33", "marginLeft": "12px",
                        "fontWeight": "bold", "whiteSpace": "nowrap"}),
                    done_pill,
                ] if c is not None], style={"display": "flex", "alignItems": "center", "marginBottom": "4px"}),
                html.Div([
                    html.Span(inv.get("date", ""), style={"color": GRAY, "fontSize": "12px"}),
                    html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66", "margin": "0 6px"}),
                    html.Span(source_label, style={"color": GRAY, "fontSize": "12px"}),
                    html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66", "margin": "0 6px"}),
                    html.Span(f"${order_total:.2f}", style={"color": WHITE, "fontSize": "14px", "fontWeight": "700"}),
                    html.Span([
                        html.Span(f"{order_saved}/{order_total_items} saved",
                                  style={"color": _prog_color, "fontSize": "13px", "fontWeight": "bold"}),
                        mini_progress,
                    ], style={"marginLeft": "auto", "display": "flex", "alignItems": "center"}),
                ], style={"display": "flex", "alignItems": "center"}),
            ], style={"marginBottom": "10px", "paddingBottom": "10px",
                      "borderBottom": f"1px solid {CYAN}18"}),
            html.Div(item_rows),
        ], style={"backgroundColor": f"#2ecc7108" if _order_all_done else CARD2,
                  "padding": "16px 18px", "borderRadius": "10px", "marginBottom": "12px",
                  "border": f"1px solid {GREEN}55" if _order_all_done else f"1px solid {CYAN}18"})
        order_cards.append(order_card)

    # Progress bar
    pct = round(saved_count / total_items * 100) if total_items > 0 else 0
    _bar_color = GREEN if pct > 75 else (ORANGE if pct > 40 else TEAL)
    progress_bar = html.Div([
        html.Div([
            html.Span(f"{saved_count}", style={"color": WHITE, "fontSize": "22px", "fontWeight": "bold"}),
            html.Span(f" / {total_items} items organized", style={"color": GRAY, "fontSize": "14px",
                       "marginLeft": "4px"}),
            html.Span(f"  {pct}%", style={"color": _bar_color, "fontSize": "14px", "fontWeight": "bold",
                       "marginLeft": "10px"}),
        ], style={"marginBottom": "8px"}),
        html.Div([
            html.Div(
                f"{pct}%" if pct > 15 else "",
                style={"width": f"{max(pct, 2)}%", "height": "18px",
                        "backgroundColor": _bar_color, "borderRadius": "9px",
                        "transition": "width 0.3s", "fontSize": "10px",
                        "color": WHITE, "fontWeight": "bold", "lineHeight": "18px",
                        "textAlign": "center", "overflow": "hidden"}),
        ], style={"width": "100%", "height": "18px", "backgroundColor": "#0d0d1a",
                  "borderRadius": "9px", "overflow": "hidden"}),
    ], style={"marginBottom": "18px", "padding": "14px 16px", "backgroundColor": "#0f1225",
              "borderRadius": "8px"})

    return html.Div([
        progress_bar,
        html.Div(order_cards, id="editor-items-container"),
    ])


# ══════════════════════════════════════════════════════════════════════════════
#  RECEIPT UPLOAD  (mirrors monolith _build_receipt_upload_section)
# ══════════════════════════════════════════════════════════════════════════════

def _build_receipt_upload_section():
    """Receipt upload zone + item-by-item onboarding wizard."""
    _label = {"color": GRAY, "fontSize": "12px", "marginRight": "6px",
              "whiteSpace": "nowrap", "fontWeight": "500"}
    _inp = {"fontSize": "13px", "backgroundColor": "#0d0d1a", "color": WHITE,
            "border": f"1px solid {DARKGRAY}55", "borderRadius": "6px", "padding": "7px 12px"}
    cat_options = [{"label": c, "value": c} for c in CATEGORY_OPTIONS]
    loc_options = [{"label": "Tulsa, OK", "value": "Tulsa, OK"},
                   {"label": "Texas", "value": "Texas"},
                   {"label": "Other", "value": "Other"}]

    return html.Div([
        html.H4("UPLOAD NEW RECEIPT", style={
            "color": PURPLE, "margin": "0 0 10px 0", "fontSize": "15px"}),

        # Upload zone
        dcc.Upload(
            id="receipt-upload",
            children=html.Div([
                html.Span("Drop PDF here or ", style={"color": GRAY, "fontSize": "13px"}),
                html.A("browse files", style={
                    "color": CYAN, "textDecoration": "underline",
                    "cursor": "pointer", "fontSize": "13px"}),
            ], style={"textAlign": "center", "padding": "16px"}),
            accept=".pdf",
            style={
                "borderWidth": "2px", "borderStyle": "dashed",
                "borderColor": f"{PURPLE}55", "borderRadius": "8px",
                "backgroundColor": f"{PURPLE}08", "cursor": "pointer",
                "marginBottom": "10px",
            },
        ),

        # Upload status
        html.Div(id="receipt-upload-status", style={"marginBottom": "8px"}),

        # Wizard state store
        dcc.Store(id="receipt-wizard-state", data=None),

        # Wizard panel (hidden until a PDF is uploaded)
        html.Div([
            html.Div(id="receipt-wizard-header", style={"marginBottom": "10px"}),
            html.Div(id="receipt-wizard-orig", style={
                "backgroundColor": "#0f0f1a", "padding": "10px 14px",
                "borderRadius": "6px", "marginBottom": "12px",
                "borderLeft": f"3px solid {CYAN}",
            }),
            html.Div([
                html.Div([
                    html.Span("Display Name:", style=_label),
                    dcc.Input(id="wizard-name", type="text", value="",
                              style={**_inp, "flex": "1", "minWidth": "200px"}),
                ], style={"display": "flex", "alignItems": "center", "flex": "2"}),
                html.Div([
                    html.Span("Category:", style=_label),
                    dbc.Select(id="wizard-cat", options=cat_options, value="Other",
                               style={"width": "150px", "fontSize": "13px",
                                      "backgroundColor": "#0d0d1a", "color": WHITE}),
                ], style={"display": "flex", "alignItems": "center"}),
                html.Div([
                    html.Span("Qty:", style=_label),
                    dcc.Input(id="wizard-qty", type="number", min=1, value=1,
                              style={**_inp, "width": "65px"}),
                ], style={"display": "flex", "alignItems": "center"}),
                html.Div([
                    html.Span("Location:", style=_label),
                    dbc.Select(id="wizard-loc", options=loc_options, value="Tulsa, OK",
                               style={"width": "140px", "fontSize": "13px",
                                      "backgroundColor": "#0d0d1a", "color": WHITE}),
                ], style={"display": "flex", "alignItems": "center"}),
            ], id="wizard-form-row",
               style={"display": "flex", "alignItems": "center", "flexWrap": "wrap",
                       "gap": "12px", "marginBottom": "12px"}),
            html.Div([
                html.Button("\u2190 Back", id="wizard-back-btn", n_clicks=0, disabled=True,
                            style={"fontSize": "12px", "padding": "8px 16px",
                                   "backgroundColor": "transparent", "color": DARKGRAY,
                                   "border": f"1px solid {DARKGRAY}44", "borderRadius": "6px",
                                   "cursor": "pointer"}),
                html.Button("Skip", id="wizard-skip-btn", n_clicks=0,
                            style={"fontSize": "12px", "padding": "8px 20px",
                                   "backgroundColor": "transparent", "color": GRAY,
                                   "border": f"1px solid {DARKGRAY}55", "borderRadius": "6px",
                                   "cursor": "pointer"}),
                html.Button("Save & Next \u2192", id="wizard-save-btn", n_clicks=0,
                            style={"fontSize": "12px", "padding": "8px 24px",
                                   "backgroundColor": TEAL, "color": WHITE,
                                   "border": "none", "borderRadius": "6px",
                                   "cursor": "pointer", "fontWeight": "bold"}),
            ], id="wizard-nav-btns",
               style={"display": "flex", "gap": "10px", "alignItems": "center",
                       "marginBottom": "10px"}),
            html.Button("Done \u2014 Refresh Inventory", id="wizard-done-btn", n_clicks=0,
                        style={"display": "none", "fontSize": "12px", "padding": "8px 24px",
                               "backgroundColor": GREEN, "color": WHITE,
                               "border": "none", "borderRadius": "6px",
                               "cursor": "pointer", "fontWeight": "bold",
                               "marginBottom": "10px"}),
            html.Div(id="receipt-wizard-progress", style={"textAlign": "center"}),
        ], id="receipt-wizard-panel", style={"display": "none"}),

    ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
              "marginBottom": "14px", "border": f"1px solid {PURPLE}33",
              "borderLeft": f"4px solid {PURPLE}"})


# ══════════════════════════════════════════════════════════════════════════════
#  WAREHOUSE SECTION  (mirrors monolith _build_enhanced_location_section)
# ══════════════════════════════════════════════════════════════════════════════

def _build_warehouse_card(title, color, spend, orders, pct, by_cat):
    """Single warehouse card with spend + category breakdown."""
    cat_dots = []
    for cat in sorted(by_cat.index.tolist()) if hasattr(by_cat, 'index') else sorted(by_cat.keys()):
        amt = by_cat[cat] if hasattr(by_cat, 'index') else by_cat.get(cat, 0)
        c = CATEGORY_COLORS.get(cat, GRAY)
        cat_dots.append(html.Div([
            html.Div(style={"width": "12px", "height": "12px", "borderRadius": "50%",
                            "backgroundColor": c, "flexShrink": "0",
                            "boxShadow": f"0 0 6px {c}44"}),
            html.Span(cat, style={"color": WHITE, "fontSize": "12px", "marginLeft": "8px",
                                    "fontWeight": "500"}),
            html.Span(ds.money(amt), style={"color": WHITE, "fontSize": "11px", "marginLeft": "auto",
                                              "fontFamily": "monospace", "fontWeight": "bold",
                                              "backgroundColor": f"{c}22", "padding": "1px 8px",
                                              "borderRadius": "8px", "border": f"1px solid {c}33"}),
        ], style={"display": "flex", "alignItems": "center", "padding": "3px 0"}))

    pct_pill = html.Span(f"{pct:.1f}%", style={
        "fontSize": "12px", "fontWeight": "bold", "padding": "3px 12px",
        "borderRadius": "10px", "backgroundColor": f"{color}18", "color": color,
        "border": f"1px solid {color}33"})

    return html.Div([
        html.Div(style={"height": "5px",
                         "background": f"linear-gradient(90deg, {color}, {color}66)",
                         "borderRadius": "10px 10px 0 0",
                         "margin": "-16px -16px 14px -16px"}),
        html.Div([
            html.Span(title, style={"color": color, "fontSize": "17px", "fontWeight": "bold"}),
        ]),
        html.Div(ds.money(spend), style={"color": WHITE, "fontSize": "30px",
                                          "fontWeight": "bold", "fontFamily": "monospace",
                                          "margin": "6px 0",
                                          "textShadow": f"0 0 15px {color}22"}),
        html.Div([
            html.Span(f"{orders} orders", style={"color": GRAY, "fontSize": "12px"}),
            html.Span(" \u00b7 ", style={"color": f"{DARKGRAY}66", "margin": "0 6px"}),
            pct_pill,
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "14px",
                  "paddingBottom": "12px", "borderBottom": f"1px solid {color}22"}),
        html.Div(cat_dots, style={"marginBottom": "12px"}) if cat_dots else None,
    ], className="warehouse-card",
       style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
              "flex": "1", "border": f"1px solid {color}33", "minHeight": "200px",
              "boxShadow": "0 4px 16px rgba(0,0,0,0.3)"})


def _build_warehouse_section():
    """Warehouse cards side by side."""
    tulsa_pct = (ds.tulsa_spend / ds.true_inventory_cost * 100) if ds.true_inventory_cost else 0
    texas_pct = (ds.texas_spend / ds.true_inventory_cost * 100) if ds.true_inventory_cost else 0
    return html.Div([
        html.Div([
            html.H3("WAREHOUSES", style={"color": CYAN, "margin": "0", "fontSize": "18px",
                                          "fontWeight": "700", "letterSpacing": "1px"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "12px"}),
        html.Div([
            _build_warehouse_card("TJ (Tulsa, OK)", TEAL, ds.tulsa_spend,
                                   ds.tulsa_orders, tulsa_pct, ds.tulsa_by_cat),
            _build_warehouse_card("BRADEN (Texas)", ORANGE, ds.texas_spend,
                                   ds.texas_orders, texas_pct, ds.texas_by_cat),
        ], id="location-inventory-display",
           style={"display": "flex", "gap": "12px"}),
    ], style={"backgroundColor": CARD2, "padding": "22px", "borderRadius": "12px",
              "marginBottom": "18px", "border": f"1px solid {CYAN}33",
              "borderTop": f"5px solid {CYAN}",
              "boxShadow": "0 4px 20px rgba(0,0,0,0.3)"})


# ══════════════════════════════════════════════════════════════════════════════
#  SPENDING ANALYTICS CHARTS
# ══════════════════════════════════════════════════════════════════════════════

def _build_analytics_charts():
    """Build the 3 main inventory charts + sub-sections."""
    inv_months = sorted(ds.monthly_inv_spend.index) if len(ds.monthly_inv_spend) > 0 else []
    sales_months = sorted(ds.monthly_sales.index) if len(ds.monthly_sales) > 0 else []
    all_months = sorted(set(sales_months) | set(inv_months))

    # 1. Monthly Supply Costs
    fig1 = go.Figure()
    if inv_months:
        fig1.add_trace(go.Bar(
            name="Inventory Spend", x=inv_months,
            y=[ds.monthly_inv_spend.get(m, 0) for m in inv_months],
            marker_color=PURPLE,
            text=[f"${ds.monthly_inv_spend.get(m, 0):,.0f}" for m in inv_months],
            textposition="outside",
        ))
    make_chart(fig1, 360)
    fig1.update_layout(title="Monthly Supply Costs", yaxis_title="Amount ($)")

    # 2. Category donut
    fig2 = go.Figure()
    if len(ds.biz_inv_by_category) > 0:
        cat_colors = [BLUE, TEAL, ORANGE, RED, PURPLE, PINK, GREEN, CYAN]
        fig2.add_trace(go.Pie(
            labels=ds.biz_inv_by_category.index.tolist(),
            values=ds.biz_inv_by_category.values.tolist(),
            hole=0.45,
            marker_colors=cat_colors[:len(ds.biz_inv_by_category)],
            textinfo="label+percent", textposition="outside",
        ))
    make_chart(fig2, 380, False)
    fig2.update_layout(title="Inventory by Category (Business Only)", showlegend=False)

    # 3. Revenue vs COGS vs Profit
    fig3 = go.Figure()
    if all_months:
        fig3.add_trace(go.Bar(
            name="Revenue", x=all_months,
            y=[ds.monthly_sales.get(m, 0) for m in all_months], marker_color=GREEN))
        fig3.add_trace(go.Bar(
            name="Supplies", x=all_months,
            y=[ds.monthly_inv_spend.get(m, 0) for m in all_months], marker_color=PURPLE))
        fig3.add_trace(go.Bar(
            name="Etsy Expenses", x=all_months,
            y=[ds.monthly_fees.get(m, 0) + ds.monthly_shipping.get(m, 0)
               + ds.monthly_marketing.get(m, 0) + ds.monthly_refunds.get(m, 0)
               for m in all_months], marker_color=RED))
        true_profit = [
            ds.monthly_sales.get(m, 0) - ds.monthly_fees.get(m, 0) - ds.monthly_shipping.get(m, 0)
            - ds.monthly_marketing.get(m, 0) - ds.monthly_refunds.get(m, 0) - ds.monthly_inv_spend.get(m, 0)
            for m in all_months
        ]
        fig3.add_trace(go.Scatter(
            name="True Profit", x=all_months, y=true_profit,
            mode="lines+markers+text",
            text=[f"${v:,.0f}" for v in true_profit],
            textposition="top center", textfont=dict(color=ORANGE),
            line=dict(color=ORANGE, width=3), marker=dict(size=10)))
    make_chart(fig3, 400)
    fig3.update_layout(title="Revenue vs Supplies vs Expenses vs Profit",
                       barmode="group", yaxis_title="Amount ($)")

    # Location charts
    loc_months = sorted(set(list(ds.tulsa_monthly.index) + list(ds.texas_monthly.index)))
    fig_loc = go.Figure()
    if loc_months:
        fig_loc.add_trace(go.Bar(
            name="TJ (Tulsa)", x=loc_months,
            y=[ds.tulsa_monthly.get(m, 0) for m in loc_months],
            marker_color=TEAL,
            text=[f"${ds.tulsa_monthly.get(m, 0):,.0f}" for m in loc_months],
            textposition="outside"))
        fig_loc.add_trace(go.Bar(
            name="Braden (TX)", x=loc_months,
            y=[ds.texas_monthly.get(m, 0) for m in loc_months],
            marker_color=ORANGE,
            text=[f"${ds.texas_monthly.get(m, 0):,.0f}" for m in loc_months],
            textposition="outside"))
    make_chart(fig_loc, 380)
    fig_loc.update_layout(title="Monthly Spending by Location", barmode="group")

    # Tulsa/Texas category donuts
    tulsa_cat_fig = go.Figure()
    if len(ds.tulsa_by_cat) > 0:
        tulsa_cat_fig.add_trace(go.Pie(
            labels=ds.tulsa_by_cat.index.tolist(), values=ds.tulsa_by_cat.values.tolist(),
            hole=0.45, textinfo="label+percent", textposition="outside"))
    make_chart(tulsa_cat_fig, 320, False)
    tulsa_cat_fig.update_layout(title="TJ (Tulsa) by Category", showlegend=False)

    texas_cat_fig = go.Figure()
    if len(ds.texas_by_cat) > 0:
        texas_cat_fig.add_trace(go.Pie(
            labels=ds.texas_by_cat.index.tolist(), values=ds.texas_by_cat.values.tolist(),
            hole=0.45, textinfo="label+percent", textposition="outside"))
    make_chart(texas_cat_fig, 320, False)
    texas_cat_fig.update_layout(title="Braden (TX) by Category", showlegend=False)

    # Payment sections
    payment_cards = []
    if ds.payment_summary:
        for pm_name, pm_data in sorted(ds.payment_summary.items(), key=lambda x: -x[1]["total"]):
            item_agg = {}
            for it in pm_data.get("items", []):
                name = it.get("name", "")[:80]
                if name not in item_agg:
                    item_agg[name] = {"qty": 0, "total": 0.0, "category": it.get("category", "Other")}
                item_agg[name]["qty"] += it.get("qty", 1)
                item_agg[name]["total"] += it.get("total", 0)
            tbl_rows = []
            for iname, info in sorted(item_agg.items(), key=lambda x: -x[1]["total"])[:20]:
                tbl_rows.append(html.Tr([
                    html.Td(item_thumbnail(iname, ds._IMAGE_URLS.get(iname, ""), 24),
                             style={"padding": "2px 4px", "textAlign": "center", "width": "30px"}),
                    html.Td(str(info["qty"]), style={"textAlign": "center", "color": WHITE,
                                                       "padding": "2px 6px", "fontSize": "11px"}),
                    html.Td(iname, style={"color": WHITE, "padding": "2px 6px", "fontSize": "11px",
                                           "maxWidth": "350px", "overflow": "hidden",
                                           "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
                    html.Td(info["category"], style={"color": TEAL, "padding": "2px 6px", "fontSize": "10px"}),
                    html.Td(ds.money(info["total"]), style={"textAlign": "right", "color": ORANGE,
                                                              "fontWeight": "bold", "padding": "2px 6px",
                                                              "fontSize": "11px"}),
                ]))
            payment_cards.append(html.Div([
                html.Div([
                    html.Span(pm_name, style={"color": CYAN, "fontSize": "18px", "fontWeight": "bold"}),
                ], style={"marginBottom": "4px"}),
                html.Div([
                    html.Div([
                        html.Div("TOTAL SPENT", style={"color": GRAY, "fontSize": "10px", "fontWeight": "600"}),
                        html.Div(ds.money(pm_data["total"]), style={"color": WHITE, "fontSize": "22px",
                                                                      "fontWeight": "bold", "fontFamily": "monospace"}),
                    ], style={"textAlign": "center", "flex": "1"}),
                    html.Div([
                        html.Div("ORDERS", style={"color": GRAY, "fontSize": "10px", "fontWeight": "600"}),
                        html.Div(str(pm_data["orders"]), style={"color": WHITE, "fontSize": "22px",
                                                                   "fontWeight": "bold", "fontFamily": "monospace"}),
                    ], style={"textAlign": "center", "flex": "1"}),
                ], style={"display": "flex", "gap": "8px", "marginBottom": "12px",
                           "padding": "8px", "backgroundColor": "#ffffff06", "borderRadius": "8px"}),
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("", style={"padding": "3px 4px", "width": "30px"}),
                        html.Th("Qty", style={"textAlign": "center", "padding": "3px 6px", "fontSize": "10px"}),
                        html.Th("Item", style={"textAlign": "left", "padding": "3px 6px", "fontSize": "10px"}),
                        html.Th("Cat", style={"textAlign": "left", "padding": "3px 6px", "fontSize": "10px"}),
                        html.Th("Total", style={"textAlign": "right", "padding": "3px 6px", "fontSize": "10px"}),
                    ], style={"borderBottom": f"1px solid {CYAN}66"})),
                    html.Tbody(tbl_rows),
                ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
            ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
                      "marginBottom": "12px", "borderLeft": f"3px solid {CYAN}"}))

    return html.Div([
        # Main charts
        html.Div([
            html.Div([dcc.Graph(figure=fig1, config={"displayModeBar": False})], style={"flex": "3"}),
            html.Div([dcc.Graph(figure=fig2, config={"displayModeBar": False})], style={"flex": "2"}),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "10px"}),
        dcc.Graph(figure=fig3, config={"displayModeBar": False}),

        # Location Spending Breakdown
        html.Details([
            html.Summary("LOCATION SPENDING BREAKDOWN", style={
                "color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "8px 0"}),
            html.Div([
                dcc.Graph(figure=fig_loc, config={"displayModeBar": False}),
                html.Div([
                    html.Div([dcc.Graph(figure=tulsa_cat_fig, config={"displayModeBar": False})], style={"flex": "1"}),
                    html.Div([dcc.Graph(figure=texas_cat_fig, config={"displayModeBar": False})], style={"flex": "1"}),
                ], style={"display": "flex", "gap": "8px", "marginBottom": "10px"}),
                html.Div([
                    html.Div([
                        html.H4("TJ (Tulsa) Categories", style={"color": TEAL, "margin": "0 0 8px 0", "fontSize": "14px"}),
                    ] + [
                        html.Div([
                            html.Span(f"{cat}", style={"color": WHITE, "flex": "1"}),
                            html.Span(ds.money(amt), style={"color": TEAL, "fontFamily": "monospace", "fontWeight": "bold"}),
                        ], style={"display": "flex", "padding": "3px 8px", "borderBottom": "1px solid #ffffff10"})
                        for cat, amt in ds.tulsa_by_cat.items()
                    ] + [
                        html.Div([
                            html.Span("TOTAL", style={"color": TEAL, "flex": "1", "fontWeight": "bold"}),
                            html.Span(ds.money(ds.tulsa_by_cat.sum()), style={"color": TEAL, "fontFamily": "monospace", "fontWeight": "bold"}),
                        ], style={"display": "flex", "padding": "6px 8px", "borderTop": f"2px solid {TEAL}"}),
                    ], style={"backgroundColor": CARD, "padding": "12px", "borderRadius": "10px", "flex": "1"}),
                    html.Div([
                        html.H4("Braden (Texas) Categories", style={"color": ORANGE, "margin": "0 0 8px 0", "fontSize": "14px"}),
                    ] + [
                        html.Div([
                            html.Span(f"{cat}", style={"color": WHITE, "flex": "1"}),
                            html.Span(ds.money(amt), style={"color": ORANGE, "fontFamily": "monospace", "fontWeight": "bold"}),
                        ], style={"display": "flex", "padding": "3px 8px", "borderBottom": "1px solid #ffffff10"})
                        for cat, amt in ds.texas_by_cat.items()
                    ] + [
                        html.Div([
                            html.Span("TOTAL", style={"color": ORANGE, "flex": "1", "fontWeight": "bold"}),
                            html.Span(ds.money(ds.texas_by_cat.sum()), style={"color": ORANGE, "fontFamily": "monospace", "fontWeight": "bold"}),
                        ], style={"display": "flex", "padding": "6px 8px", "borderTop": f"2px solid {ORANGE}"}),
                    ], style={"backgroundColor": CARD, "padding": "12px", "borderRadius": "10px", "flex": "1"}),
                ], style={"display": "flex", "gap": "12px", "marginBottom": "14px"}),
            ]),
        ], open=False,
           style={"backgroundColor": CARD2, "padding": "12px 16px", "borderRadius": "10px",
                  "marginBottom": "14px", "border": f"1px solid {CYAN}33"}),

        # Payment Methods
        html.Details([
            html.Summary("PAYMENT METHODS", style={
                "color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "8px 0"}),
            html.Div([
                html.P("Breakdown by payment card.",
                       style={"color": GRAY, "margin": "0 0 12px 0", "fontSize": "13px"}),
                *payment_cards,
            ] if payment_cards else [html.P("No payment data.", style={"color": GRAY})]),
        ], open=False,
           style={"backgroundColor": CARD2, "padding": "12px 16px", "borderRadius": "10px",
                  "marginBottom": "14px", "border": f"1px solid {CYAN}33"}),

        # Spending by Category
        html.Details([
            html.Summary("SPENDING BY CATEGORY", style={
                "color": ORANGE, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "8px 0"}),
            html.Div([
                html.Div([
                    row_item(f"{cat}", amt, color=WHITE)
                    for cat, amt in ds.inv_by_category.items()
                ] + [
                    html.Div(style={"borderTop": f"2px solid {ORANGE}", "marginTop": "8px"}),
                    row_item("TOTAL (all items, subtotal only)",
                             ds.INV_ITEMS["total"].sum() if len(ds.INV_ITEMS) > 0 else 0,
                             bold=True, color=ORANGE),
                ]) if len(ds.inv_by_category) > 0 else html.P("No items found.", style={"color": GRAY}),
            ]),
        ], open=False,
           style={"backgroundColor": CARD2, "padding": "12px 16px", "borderRadius": "10px",
                  "marginBottom": "14px", "border": f"1px solid {ORANGE}33"}),
    ])


# ══════════════════════════════════════════════════════════════════════════════
#  ORDER & ITEM TABLES
# ══════════════════════════════════════════════════════════════════════════════

def _build_order_table():
    """Full orders table from BIZ_INV_DF."""
    rows = []
    for _, r in ds.INV_DF.iterrows():
        is_personal = (r.get("source") == "Personal Amazon" or
                       (isinstance(r.get("file", ""), str) and "Gigi" in r.get("file", "")))
        if is_personal:
            continue
        src = r.get("source", "")
        store_label = "Amazon" if src in ("Key Component Mfg",) else src
        ship_addr = r.get("ship_address", "")
        short_addr = (ship_addr.split(",")[1].strip() + ", " + ship_addr.split(",")[2].strip().split(" ")[0]
                      if ship_addr.count(",") >= 2 else ship_addr)
        rows.append(html.Tr([
            html.Td(r.get("date", ""), style={"color": GRAY, "padding": "4px 8px", "fontSize": "12px"}),
            html.Td(r.get("order_num", ""), style={"color": WHITE, "padding": "4px 8px", "fontSize": "12px"}),
            html.Td(store_label, style={"color": TEAL, "padding": "4px 8px", "fontSize": "12px"}),
            html.Td(short_addr, style={"color": CYAN, "padding": "4px 8px", "fontSize": "12px"}),
            html.Td(str(r.get("item_count", 0)), style={"textAlign": "center", "color": WHITE,
                                                          "padding": "4px 8px", "fontSize": "12px"}),
            html.Td(f"${r.get('subtotal', 0):,.2f}", style={"textAlign": "right", "color": WHITE,
                                                              "padding": "4px 8px", "fontSize": "12px"}),
            html.Td(f"${r.get('tax', 0):,.2f}", style={"textAlign": "right", "color": GRAY,
                                                          "padding": "4px 8px", "fontSize": "12px"}),
            html.Td(f"${r.get('grand_total', 0):,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                                  "fontWeight": "bold", "padding": "4px 8px",
                                                                  "fontSize": "12px"}),
        ], style={"borderBottom": "1px solid #ffffff10"}))

    total_items = int(ds.INV_DF["item_count"].sum()) if "item_count" in ds.INV_DF.columns else 0
    total_sub = ds.INV_DF["subtotal"].sum() if "subtotal" in ds.INV_DF.columns else 0
    total_tax = ds.INV_DF["tax"].sum() if "tax" in ds.INV_DF.columns else 0
    total_grand = ds.total_inventory_cost

    rows.append(html.Tr([
        html.Td("TOTAL", style={"color": ORANGE, "fontWeight": "bold", "padding": "6px 8px"}),
        html.Td(""), html.Td(""), html.Td(""),
        html.Td(str(total_items), style={"textAlign": "center", "color": ORANGE,
                                          "fontWeight": "bold", "padding": "6px 8px"}),
        html.Td(f"${total_sub:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                               "fontWeight": "bold", "padding": "6px 8px"}),
        html.Td(f"${total_tax:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                               "fontWeight": "bold", "padding": "6px 8px"}),
        html.Td(f"${total_grand:,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                 "fontWeight": "bold", "fontSize": "14px",
                                                 "padding": "6px 8px"}),
    ], style={"borderTop": f"3px solid {ORANGE}"}))

    return html.Table([
        html.Thead(html.Tr([
            html.Th("Date", style={"textAlign": "left", "padding": "6px 8px"}),
            html.Th("Order #", style={"textAlign": "left", "padding": "6px 8px"}),
            html.Th("Store", style={"textAlign": "left", "padding": "6px 8px"}),
            html.Th("Shipped To", style={"textAlign": "left", "padding": "6px 8px"}),
            html.Th("Items", style={"textAlign": "center", "padding": "6px 8px"}),
            html.Th("Subtotal", style={"textAlign": "right", "padding": "6px 8px"}),
            html.Th("Tax", style={"textAlign": "right", "padding": "6px 8px"}),
            html.Th("Total", style={"textAlign": "right", "padding": "6px 8px"}),
        ], style={"borderBottom": f"2px solid {PURPLE}"})),
        html.Tbody(rows),
    ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE})


def _build_item_table():
    """Full item list sorted by cost — excludes personal/biz fees."""
    rows = []
    items_sorted = ds.INV_ITEMS.sort_values("total", ascending=False)
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
        _img_url = ds._IMAGE_URLS.get(r["name"], "") or r.get("image_url", "")
        _item_orig = r.get("_orig_name", r["name"])
        _item_renamed = _item_orig != r["name"]
        _name_parts = [html.Span(r["name"], style={"color": WHITE})]
        if _item_renamed:
            _name_parts.append(html.Br())
            _name_parts.append(html.Span(
                _item_orig[:55], title=_item_orig,
                style={"color": DARKGRAY, "fontSize": "9px", "fontStyle": "italic"}))
        rows.append(html.Tr([
            html.Td(item_thumbnail(r["name"], _img_url, 32),
                     style={"padding": "4px 6px", "textAlign": "center", "width": "40px"}),
            html.Td(html.Span("INVENTORY", style={
                "backgroundColor": "#00e67622", "color": GREEN, "padding": "2px 8px",
                "borderRadius": "10px", "fontSize": "10px", "fontWeight": "600",
                "letterSpacing": "0.5px"}), style={"padding": "4px 8px", "textAlign": "center"}),
            html.Td(_name_parts,
                     title=f"{r['name']}\nFrom: {_item_orig}" if _item_renamed else r["name"],
                     style={"padding": "4px 8px", "fontSize": "11px",
                            "maxWidth": "350px", "overflow": "hidden", "textOverflow": "ellipsis"}),
            html.Td(r.get("category", "Other"), style={"color": TEAL, "padding": "4px 8px", "fontSize": "11px"}),
            html.Td(store_name, style={"color": CYAN, "padding": "4px 8px", "fontSize": "11px"}),
            html.Td(short_ship[:30], style={"color": GRAY, "padding": "4px 8px", "fontSize": "11px"}),
            html.Td(str(r.get("qty", 0)), style={"textAlign": "center", "color": WHITE,
                                                    "padding": "4px 8px", "fontSize": "11px"}),
            html.Td(f"${r.get('price', 0):,.2f}", style={"textAlign": "right", "color": WHITE,
                                                            "padding": "4px 8px", "fontSize": "11px"}),
            html.Td(f"${r.get('total', 0):,.2f}", style={"textAlign": "right", "color": ORANGE,
                                                            "fontWeight": "bold", "padding": "4px 8px",
                                                            "fontSize": "11px"}),
            html.Td(r.get("date", ""), style={"color": GRAY, "padding": "4px 8px", "fontSize": "11px"}),
        ], style={"borderBottom": "1px solid #ffffff10"}))

    return html.Div([
        html.P("Business supplies only.", style={"color": GRAY, "fontSize": "12px", "marginBottom": "8px"}),
        html.Div([
            html.Table([
                html.Thead(html.Tr([
                    html.Th("", style={"padding": "6px 4px", "width": "44px"}),
                    html.Th("Type", style={"textAlign": "center", "padding": "6px 8px", "width": "80px"}),
                    html.Th("Item Name", style={"textAlign": "left", "padding": "6px 8px"}),
                    html.Th("Category", style={"textAlign": "left", "padding": "6px 8px"}),
                    html.Th("Store", style={"textAlign": "left", "padding": "6px 8px"}),
                    html.Th("Shipped To", style={"textAlign": "left", "padding": "6px 8px"}),
                    html.Th("Qty", style={"textAlign": "center", "padding": "6px 8px"}),
                    html.Th("Unit Price", style={"textAlign": "right", "padding": "6px 8px"}),
                    html.Th("Total", style={"textAlign": "right", "padding": "6px 8px"}),
                    html.Th("Date", style={"textAlign": "left", "padding": "6px 8px"}),
                ], style={"borderBottom": f"2px solid {TEAL}"})),
                html.Tbody(rows),
            ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
        ], style={"maxHeight": "600px", "overflowY": "auto"}),
    ])


# ══════════════════════════════════════════════════════════════════════════════
#  RECEIPT GALLERY  (mirrors monolith _build_receipt_gallery — simplified)
# ══════════════════════════════════════════════════════════════════════════════

def _build_receipt_gallery():
    """Visual receipt gallery with parsed specs (no PDF viewer in local mode)."""
    import pandas as pd

    sorted_invoices = sorted(ds.INVOICES,
                             key=lambda o: o.get("date", ""), reverse=True)
    try:
        sorted_invoices = sorted(ds.INVOICES,
            key=lambda o: pd.to_datetime(o.get("date", ""), format="%B %d, %Y", errors="coerce"),
            reverse=True)
    except Exception:
        pass

    biz_cards = []
    personal_cards = []
    for inv in sorted_invoices:
        is_personal = (inv.get("source") == "Personal Amazon" or
                       (isinstance(inv.get("file", ""), str) and "Gigi" in inv.get("file", "")))
        order_num = inv.get("order_num", "N/A")
        date_str = inv.get("date", "Unknown")
        source = inv.get("source", "Unknown")
        payment = inv.get("payment_method", "Unknown")
        ship_addr = inv.get("ship_address", "")
        if ship_addr.count(",") >= 2:
            parts = ship_addr.split(",")
            short_addr = parts[1].strip() + ", " + parts[2].strip().split(" ")[0]
        else:
            short_addr = ship_addr

        accent = PINK if is_personal else CYAN
        item_rows = []
        for it in inv.get("items", []):
            item_rows.append(html.Tr([
                html.Td(it["name"][:60] + ("..." if len(it["name"]) > 60 else ""),
                         style={"color": WHITE, "fontSize": "11px", "padding": "3px 6px",
                                "maxWidth": "280px", "overflow": "hidden", "textOverflow": "ellipsis"}),
                html.Td(str(it["qty"]), style={"color": GRAY, "fontSize": "11px",
                                                "textAlign": "center", "padding": "3px 6px"}),
                html.Td(f"${it['price']:,.2f}", style={"color": WHITE, "fontSize": "11px",
                                                        "textAlign": "right", "padding": "3px 6px"}),
            ]))

        card = html.Div([
            html.Div([
                html.Span("Order #  ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(order_num, style={"color": accent, "fontSize": "13px", "fontWeight": "bold"}),
            ], style={"marginBottom": "4px"}),
            html.Div([
                html.Span(f"{date_str}  \u00b7  {source}  \u00b7  {payment}",
                           style={"color": GRAY, "fontSize": "11px"}),
            ], style={"marginBottom": "4px"}),
            html.Div([
                html.Span(f"Ship to: {short_addr}", style={"color": CYAN, "fontSize": "11px"}),
            ], style={"marginBottom": "8px"}) if short_addr else html.Div(),
            html.Table([
                html.Thead(html.Tr([
                    html.Th("Item", style={"textAlign": "left", "color": GRAY, "fontSize": "10px", "padding": "3px 6px"}),
                    html.Th("Qty", style={"textAlign": "center", "color": GRAY, "fontSize": "10px", "padding": "3px 6px"}),
                    html.Th("Price", style={"textAlign": "right", "color": GRAY, "fontSize": "10px", "padding": "3px 6px"}),
                ])),
                html.Tbody(item_rows),
            ], style={"width": "100%", "borderCollapse": "collapse", "marginBottom": "8px"}),
            html.Div([
                html.Span("Total ", style={"color": GRAY, "fontSize": "11px", "fontWeight": "bold"}),
                html.Span(f"${inv.get('grand_total', 0):,.2f}",
                          style={"color": ORANGE, "fontSize": "14px", "fontWeight": "bold"}),
            ], style={"borderTop": f"1px solid {DARKGRAY}", "paddingTop": "6px"}),
        ], style={"backgroundColor": CARD, "borderRadius": "10px", "padding": "14px",
                  "marginBottom": "10px", "border": f"1px solid {accent}22"})

        if is_personal:
            personal_cards.append(card)
        else:
            biz_cards.append(card)

    children = [
        html.H5(f"RECEIPT GALLERY  ({len(biz_cards)} business)", style={
            "color": CYAN, "fontWeight": "bold", "marginBottom": "4px", "fontSize": "15px"}),
        html.P("Every uploaded receipt with parsed specs.",
               style={"color": GRAY, "fontSize": "12px", "marginBottom": "14px"}),
    ] + biz_cards

    if personal_cards:
        children.append(html.Details([
            html.Summary(f"Personal Receipts ({len(personal_cards)})", style={
                "color": PINK, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "8px 0"}),
            html.Div(personal_cards),
        ], open=False, style={"marginTop": "14px", "backgroundColor": CARD2,
                              "padding": "12px 16px", "borderRadius": "10px",
                              "border": f"1px solid {PINK}33"}))

    return html.Div(children, style={
        "backgroundColor": CARD2, "padding": "20px", "borderRadius": "12px",
        "marginTop": "14px", "border": f"1px solid {CYAN}33",
        "borderTop": f"4px solid {CYAN}", "maxHeight": "800px", "overflowY": "auto"})


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN LAYOUT  (mirrors monolith build_tab4_inventory exactly)
# ══════════════════════════════════════════════════════════════════════════════

def layout():
    """Build the Inventory page — matches Railway monolith structure exactly."""
    biz_order_count = len(ds.BIZ_INV_DF)

    return html.Div([

        # ── Header + Quick Add button ─────────────────────────────────────
        html.Div([
            html.Div([
                html.H3("INVENTORY MANAGEMENT", style={"color": CYAN, "margin": "0", "fontSize": "26px",
                                                        "fontWeight": "700", "letterSpacing": "1.5px",
                                                        "textShadow": f"0 0 20px {CYAN}22"}),
                html.P(["Business inventory with stock tracking. ",
                        html.Span(f"{ds.unique_item_count} unique items",
                                  style={"color": GREEN, "fontWeight": "bold"}),
                        f" across {biz_order_count} orders."],
                       style={"color": GRAY, "margin": "2px 0 0 0", "fontSize": "13px"}),
            ], style={"flex": "1"}),
            html.Button("+ Quick Add", id="qa-toggle-btn", n_clicks=0,
                        className="btn-gradient-green",
                        style={"fontSize": "13px", "padding": "10px 22px",
                               "background": f"linear-gradient(135deg, {GREEN}, #27ae60)",
                               "color": WHITE, "border": "none", "borderRadius": "8px",
                               "cursor": "pointer", "fontWeight": "bold", "whiteSpace": "nowrap",
                               "alignSelf": "flex-start",
                               "boxShadow": f"0 3px 12px {GREEN}33"}),
        ], style={"display": "flex", "alignItems": "flex-start", "gap": "16px", "marginBottom": "14px"}),

        dcc.Store(id="editor-save-trigger", data=0),

        # ── KPI Pill Strip ────────────────────────────────────────────────
        html.Div(id="inv-kpi-row", children=_build_kpi_row()),

        # ── Snapshot Banner ───────────────────────────────────────────────
        _build_snapshot_banner(),

        # ── Quick Add Panel (hidden by default, toggles via button) ───────
        html.Div([
            html.H4("QUICK ADD PURCHASE", style={"color": GREEN, "margin": "0 0 8px 0", "fontSize": "15px"}),
            _build_quick_add_form(),
        ], id="qa-panel",
           style={"display": "none", "backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
                  "marginBottom": "14px", "border": f"1px solid {GREEN}33",
                  "borderLeft": f"4px solid {GREEN}"}),

        # ── Receipts to Inventory Editor ──────────────────────────────────
        _build_inventory_editor(),

        # ── Receipt Upload ────────────────────────────────────────────────
        _build_receipt_upload_section(),

        # ── Warehouses (collapsible) ──────────────────────────────────────
        html.Details([
            _sec_header("WAREHOUSES", "Who has what \u2014 inventory by location", color=CYAN),
            html.Div([_build_warehouse_section()], style={"padding": "0"}),
        ], open=False, style={"marginBottom": "14px"}),

        # ── Spending Analytics (collapsible) ──────────────────────────────
        html.Details([
            _sec_header("SPENDING ANALYTICS", "Charts and trends for inventory spending", color=PURPLE),
            html.Div([_build_analytics_charts()], style={"padding": "14px"}),
        ], open=False,
           style={"backgroundColor": CARD2, "padding": "0", "borderRadius": "12px",
                  "marginBottom": "14px", "border": f"1px solid {PURPLE}33",
                  "borderTop": f"4px solid {PURPLE}"}),

        # ── All Orders (collapsible) ──────────────────────────────────────
        html.Details([
            _sec_header("ALL ORDERS",
                        f"Every purchase order with date, store, and totals ({ds.inv_order_count})",
                        color=PURPLE),
            html.Div([_build_order_table()],
                     style={"padding": "14px", "maxHeight": "500px", "overflowY": "auto"}),
        ], open=False,
           style={"backgroundColor": CARD2, "padding": "0", "borderRadius": "10px",
                  "marginBottom": "14px", "border": f"1px solid {PURPLE}33"}),

        # ── All Items (collapsible) ───────────────────────────────────────
        html.Details([
            _sec_header("ALL ITEMS", "Every inventory item sorted by cost", color=TEAL),
            html.Div([_build_item_table()], style={"padding": "14px"}),
        ], open=False,
           style={"backgroundColor": CARD2, "padding": "0", "borderRadius": "10px",
                  "marginBottom": "14px", "border": f"1px solid {TEAL}33"}),

        # ── Receipt Gallery ───────────────────────────────────────────────
        _build_receipt_gallery(),

        # ── Hidden elements for callbacks ─────────────────────────────────
        html.Div(id="inv-save-toast"),
        html.Div([
            dbc.Input(id="inv-image-name", type="hidden"),
            dbc.Input(id="inv-image-url", type="hidden"),
            dbc.Button(id="inv-image-save-btn", style={"display": "none"}),
        ], style={"display": "none"}),
        html.Div([
            dcc.Dropdown(id="inv-use-item", style={"display": "none"}),
            dbc.Input(id="inv-use-qty", type="hidden", value=1),
            dbc.Button(id="inv-use-btn", style={"display": "none"}),
        ], style={"display": "none"}),
    ])
