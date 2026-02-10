"""
E-Commerce Financial Dashboard
Upload Etsy & Amazon statements, track where your money goes
"""

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import os
import base64
import io
from datetime import datetime

# Initialize app
app = dash.Dash(__name__)
app.title = "E-Commerce Financial Dashboard"

# Global data storage
DATA = {
    'etsy': pd.DataFrame(),
    'amazon': pd.DataFrame(),
    'combined': pd.DataFrame()
}

# Folder with existing statements
STATEMENTS_FOLDER = r"C:\Users\mcnug\OneDrive\Desktop\etsy statments"

def parse_money(val):
    """Convert money strings to float"""
    if pd.isna(val) or val == '--' or val == '':
        return 0.0
    val = str(val).replace('$', '').replace(',', '').replace('"', '')
    try:
        return float(val)
    except:
        return 0.0

def load_etsy_csv(content=None, filename=None):
    """Load and process Etsy CSV"""
    if content:
        # Uploaded file
        content_type, content_string = content.split(',')
        decoded = base64.b64decode(content_string)
        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
    else:
        return pd.DataFrame()

    df['Amount_Clean'] = df['Amount'].apply(parse_money)
    df['Net_Clean'] = df['Net'].apply(parse_money)
    df['Fees_Clean'] = df['Fees & Taxes'].apply(parse_money)
    df['Date_Parsed'] = pd.to_datetime(df['Date'], format='%B %d, %Y', errors='coerce')
    df['Month'] = df['Date_Parsed'].dt.to_period('M').astype(str)
    df['Source'] = 'Etsy'

    return df

def load_amazon_csv(content, filename):
    """Load and process Amazon CSV - handles multiple formats"""
    content_type, content_string = content.split(',')
    decoded = base64.b64decode(content_string)
    df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))

    # Amazon has different report formats, try to normalize
    # Common columns: date/time, type, amount, total
    df['Source'] = 'Amazon'

    # Try to find date column
    date_cols = [c for c in df.columns if 'date' in c.lower()]
    if date_cols:
        df['Date_Parsed'] = pd.to_datetime(df[date_cols[0]], errors='coerce')
        df['Month'] = df['Date_Parsed'].dt.to_period('M').astype(str)

    # Try to find amount/total column
    amount_cols = [c for c in df.columns if any(x in c.lower() for x in ['total', 'amount', 'price'])]
    if amount_cols:
        df['Net_Clean'] = df[amount_cols[0]].apply(parse_money)

    # Try to find type column
    type_cols = [c for c in df.columns if 'type' in c.lower()]
    if type_cols:
        df['Type'] = df[type_cols[0]]

    return df

def load_existing_etsy_files():
    """Load all existing Etsy CSVs from folder"""
    all_data = []
    csv_files = [f for f in os.listdir(STATEMENTS_FOLDER)
                 if f.startswith('etsy_statement') and f.endswith('.csv')]

    for csv_file in csv_files:
        try:
            df = pd.read_csv(os.path.join(STATEMENTS_FOLDER, csv_file))
            df['Amount_Clean'] = df['Amount'].apply(parse_money)
            df['Net_Clean'] = df['Net'].apply(parse_money)
            df['Fees_Clean'] = df['Fees & Taxes'].apply(parse_money)
            df['Date_Parsed'] = pd.to_datetime(df['Date'], format='%B %d, %Y', errors='coerce')
            df['Month'] = df['Date_Parsed'].dt.to_period('M').astype(str)
            df['Source'] = 'Etsy'
            all_data.append(df)
        except Exception as e:
            print(f"Error loading {csv_file}: {e}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

# Load existing data on startup
DATA['etsy'] = load_existing_etsy_files()
DATA['combined'] = DATA['etsy'].copy()

# App Layout
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("E-Commerce Financial Dashboard",
                style={'color': 'white', 'marginBottom': '5px'}),
        html.P("Track your Etsy & Amazon finances in one place",
               style={'color': '#aaa', 'marginTop': '0'})
    ], style={'backgroundColor': '#1a1a2e', 'padding': '20px', 'marginBottom': '20px'}),

    # Upload Section
    html.Div([
        html.Div([
            html.H3("Upload Etsy Statement"),
            dcc.Upload(
                id='upload-etsy',
                children=html.Div(['Drag & Drop or ', html.A('Select Etsy CSV')]),
                style={
                    'width': '100%', 'height': '60px', 'lineHeight': '60px',
                    'borderWidth': '2px', 'borderStyle': 'dashed', 'borderRadius': '5px',
                    'borderColor': '#f39c12', 'textAlign': 'center', 'backgroundColor': '#2d2d44'
                },
                multiple=True
            ),
        ], style={'width': '48%', 'display': 'inline-block', 'padding': '10px'}),

        html.Div([
            html.H3("Upload Amazon Statement"),
            dcc.Upload(
                id='upload-amazon',
                children=html.Div(['Drag & Drop or ', html.A('Select Amazon CSV')]),
                style={
                    'width': '100%', 'height': '60px', 'lineHeight': '60px',
                    'borderWidth': '2px', 'borderStyle': 'dashed', 'borderRadius': '5px',
                    'borderColor': '#3498db', 'textAlign': 'center', 'backgroundColor': '#2d2d44'
                },
                multiple=True
            ),
        ], style={'width': '48%', 'display': 'inline-block', 'padding': '10px'}),
    ], style={'backgroundColor': '#16213e', 'padding': '15px', 'borderRadius': '10px', 'marginBottom': '20px'}),

    html.Div(id='upload-status', style={'padding': '10px', 'color': '#2ecc71'}),

    # Key Metrics Cards
    html.Div(id='metrics-cards', style={'marginBottom': '20px'}),

    # Charts Row 1
    html.Div([
        html.Div([
            dcc.Graph(id='money-flow-chart')
        ], style={'width': '50%', 'display': 'inline-block'}),

        html.Div([
            dcc.Graph(id='expense-breakdown-chart')
        ], style={'width': '50%', 'display': 'inline-block'}),
    ]),

    # Charts Row 2
    html.Div([
        html.Div([
            dcc.Graph(id='monthly-trend-chart')
        ], style={'width': '100%'}),
    ]),

    # Balance Sheet
    html.Div([
        html.H2("Balance Sheet", style={'color': 'white', 'borderBottom': '2px solid #f39c12', 'paddingBottom': '10px'}),
        html.Div(id='balance-sheet')
    ], style={'backgroundColor': '#16213e', 'padding': '20px', 'borderRadius': '10px', 'marginTop': '20px'}),

    # Transaction Details
    html.Div([
        html.H2("Recent Transactions", style={'color': 'white', 'borderBottom': '2px solid #3498db', 'paddingBottom': '10px'}),
        html.Div(id='transactions-table')
    ], style={'backgroundColor': '#16213e', 'padding': '20px', 'borderRadius': '10px', 'marginTop': '20px'}),

    # Hidden storage
    dcc.Store(id='data-store'),

    # Auto-refresh
    dcc.Interval(id='interval-component', interval=60*1000, n_intervals=0)

], style={'backgroundColor': '#0f0f1a', 'minHeight': '100vh', 'padding': '20px', 'fontFamily': 'Arial', 'color': 'white'})


@app.callback(
    [Output('upload-status', 'children'),
     Output('data-store', 'data')],
    [Input('upload-etsy', 'contents'),
     Input('upload-amazon', 'contents')],
    [State('upload-etsy', 'filename'),
     State('upload-amazon', 'filename')]
)
def handle_uploads(etsy_contents, amazon_contents, etsy_filenames, amazon_filenames):
    global DATA

    messages = []

    # Process Etsy uploads
    if etsy_contents:
        for content, filename in zip(etsy_contents, etsy_filenames):
            try:
                new_df = load_etsy_csv(content, filename)
                DATA['etsy'] = pd.concat([DATA['etsy'], new_df], ignore_index=True)
                DATA['etsy'] = DATA['etsy'].drop_duplicates()
                messages.append(f"Loaded Etsy: {filename}")
            except Exception as e:
                messages.append(f"Error with {filename}: {str(e)}")

    # Process Amazon uploads
    if amazon_contents:
        for content, filename in zip(amazon_contents, amazon_filenames):
            try:
                new_df = load_amazon_csv(content, filename)
                DATA['amazon'] = pd.concat([DATA['amazon'], new_df], ignore_index=True)
                messages.append(f"Loaded Amazon: {filename}")
            except Exception as e:
                messages.append(f"Error with {filename}: {str(e)}")

    # Combine data
    frames = [df for df in [DATA['etsy'], DATA['amazon']] if not df.empty]
    if frames:
        DATA['combined'] = pd.concat(frames, ignore_index=True)

    status = " | ".join(messages) if messages else "Data loaded from existing files"
    return status, {'updated': datetime.now().isoformat()}


@app.callback(
    Output('metrics-cards', 'children'),
    [Input('data-store', 'data'),
     Input('interval-component', 'n_intervals')]
)
def update_metrics(data, n):
    df = DATA['combined']
    if df.empty:
        return html.Div("No data loaded", style={'color': '#e74c3c'})

    # Calculate metrics
    sales = df[df['Type'] == 'Sale']['Net_Clean'].sum() if 'Type' in df.columns else 0
    fees = abs(df[df['Type'] == 'Fee']['Net_Clean'].sum()) if 'Type' in df.columns else 0
    shipping = abs(df[df['Type'] == 'Shipping']['Net_Clean'].sum()) if 'Type' in df.columns else 0
    marketing = abs(df[df['Type'] == 'Marketing']['Net_Clean'].sum()) if 'Type' in df.columns else 0
    refunds = abs(df[df['Type'] == 'Refund']['Net_Clean'].sum()) if 'Type' in df.columns else 0

    total_expenses = fees + shipping + marketing
    net_profit = sales - total_expenses - refunds
    profit_margin = (net_profit / sales * 100) if sales > 0 else 0

    order_count = len(df[df['Type'] == 'Sale']) if 'Type' in df.columns else 0
    avg_order = sales / order_count if order_count > 0 else 0

    def metric_card(title, value, color, subtitle=""):
        return html.Div([
            html.H4(title, style={'color': '#aaa', 'margin': '0', 'fontSize': '14px'}),
            html.H2(value, style={'color': color, 'margin': '5px 0'}),
            html.P(subtitle, style={'color': '#666', 'margin': '0', 'fontSize': '12px'})
        ], style={
            'backgroundColor': '#1a1a2e', 'padding': '20px', 'borderRadius': '10px',
            'width': '15%', 'display': 'inline-block', 'margin': '5px', 'textAlign': 'center',
            'border': f'1px solid {color}'
        })

    return html.Div([
        metric_card("GROSS SALES", f"${sales:,.2f}", "#2ecc71", f"{order_count} orders"),
        metric_card("TOTAL FEES", f"${fees:,.2f}", "#e74c3c", "Platform fees"),
        metric_card("SHIPPING", f"${shipping:,.2f}", "#3498db", "Label costs"),
        metric_card("MARKETING", f"${marketing:,.2f}", "#9b59b6", "Ads spend"),
        metric_card("NET PROFIT", f"${net_profit:,.2f}", "#f39c12", f"{profit_margin:.1f}% margin"),
        metric_card("AVG ORDER", f"${avg_order:.2f}", "#1abc9c", "Per sale"),
    ])


@app.callback(
    Output('money-flow-chart', 'figure'),
    [Input('data-store', 'data'),
     Input('interval-component', 'n_intervals')]
)
def update_money_flow(data, n):
    df = DATA['combined']
    if df.empty or 'Type' not in df.columns:
        return go.Figure()

    sales = df[df['Type'] == 'Sale']['Net_Clean'].sum()
    fees = abs(df[df['Type'] == 'Fee']['Net_Clean'].sum())
    shipping = abs(df[df['Type'] == 'Shipping']['Net_Clean'].sum())
    marketing = abs(df[df['Type'] == 'Marketing']['Net_Clean'].sum())
    refunds = abs(df[df['Type'] == 'Refund']['Net_Clean'].sum())
    taxes = abs(df[df['Type'] == 'Tax']['Net_Clean'].sum())

    net = sales - fees - shipping - marketing - refunds

    fig = go.Figure(go.Waterfall(
        name="Money Flow",
        orientation="v",
        measure=["absolute", "relative", "relative", "relative", "relative", "relative", "total"],
        x=["Gross Sales", "Fees", "Shipping", "Marketing", "Refunds", "Taxes Collected", "Net Revenue"],
        y=[sales, -fees, -shipping, -marketing, -refunds, -taxes, 0],
        connector={"line": {"color": "#888"}},
        decreasing={"marker": {"color": "#e74c3c"}},
        increasing={"marker": {"color": "#2ecc71"}},
        totals={"marker": {"color": "#f39c12"}}
    ))

    fig.update_layout(
        title="Money Flow: Where Does It Go?",
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'color': 'white'}
    )

    return fig


@app.callback(
    Output('expense-breakdown-chart', 'figure'),
    [Input('data-store', 'data'),
     Input('interval-component', 'n_intervals')]
)
def update_expense_breakdown(data, n):
    df = DATA['combined']
    if df.empty or 'Type' not in df.columns:
        return go.Figure()

    # Detailed fee breakdown
    expenses = {}

    if 'Title' in df.columns:
        fee_df = df[df['Type'] == 'Fee']

        listing_fees = abs(fee_df[fee_df['Title'].str.contains('Listing fee', na=False)]['Net_Clean'].sum())
        transaction_fees = abs(fee_df[fee_df['Title'].str.contains('Transaction fee', na=False)]['Net_Clean'].sum())
        processing_fees = abs(fee_df[fee_df['Title'].str.contains('Processing fee', na=False)]['Net_Clean'].sum())

        expenses['Listing Fees'] = listing_fees
        expenses['Transaction Fees'] = transaction_fees
        expenses['Processing Fees'] = processing_fees

    expenses['Shipping Labels'] = abs(df[df['Type'] == 'Shipping']['Net_Clean'].sum())

    # Marketing breakdown
    if 'Title' in df.columns:
        marketing_df = df[df['Type'] == 'Marketing']
        etsy_ads = abs(marketing_df[marketing_df['Title'].str.contains('Etsy Ads', na=False)]['Net_Clean'].sum())
        offsite_ads = abs(marketing_df[marketing_df['Title'].str.contains('Offsite Ads', na=False) &
                                       ~marketing_df['Title'].str.contains('Credit', na=False)]['Net_Clean'].sum())
        expenses['Etsy Ads'] = etsy_ads
        expenses['Offsite Ads'] = offsite_ads

    expenses['Refunds'] = abs(df[df['Type'] == 'Refund']['Net_Clean'].sum())

    # Remove zero values
    expenses = {k: v for k, v in expenses.items() if v > 0}

    colors = ['#e74c3c', '#c0392b', '#a93226', '#3498db', '#9b59b6', '#8e44ad', '#f39c12']

    fig = go.Figure(data=[go.Pie(
        labels=list(expenses.keys()),
        values=list(expenses.values()),
        hole=.4,
        marker_colors=colors[:len(expenses)]
    )])

    fig.update_layout(
        title="Expense Breakdown",
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        font={'color': 'white'},
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2)
    )

    return fig


@app.callback(
    Output('monthly-trend-chart', 'figure'),
    [Input('data-store', 'data'),
     Input('interval-component', 'n_intervals')]
)
def update_monthly_trend(data, n):
    df = DATA['combined']
    if df.empty or 'Month' not in df.columns or 'Type' not in df.columns:
        return go.Figure()

    # Group by month
    monthly_sales = df[df['Type'] == 'Sale'].groupby('Month')['Net_Clean'].sum()
    monthly_fees = df[df['Type'] == 'Fee'].groupby('Month')['Net_Clean'].sum().abs()
    monthly_shipping = df[df['Type'] == 'Shipping'].groupby('Month')['Net_Clean'].sum().abs()
    monthly_marketing = df[df['Type'] == 'Marketing'].groupby('Month')['Net_Clean'].sum().abs()

    months = sorted(monthly_sales.index.unique())

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name='Gross Sales',
        x=months,
        y=[monthly_sales.get(m, 0) for m in months],
        marker_color='#2ecc71'
    ))

    fig.add_trace(go.Bar(
        name='Fees',
        x=months,
        y=[-monthly_fees.get(m, 0) for m in months],
        marker_color='#e74c3c'
    ))

    fig.add_trace(go.Bar(
        name='Shipping',
        x=months,
        y=[-monthly_shipping.get(m, 0) for m in months],
        marker_color='#3498db'
    ))

    fig.add_trace(go.Bar(
        name='Marketing',
        x=months,
        y=[-monthly_marketing.get(m, 0) for m in months],
        marker_color='#9b59b6'
    ))

    # Net profit line
    net_profit = []
    for m in months:
        net = monthly_sales.get(m, 0) - monthly_fees.get(m, 0) - monthly_shipping.get(m, 0) - monthly_marketing.get(m, 0)
        net_profit.append(net)

    fig.add_trace(go.Scatter(
        name='Net Profit',
        x=months,
        y=net_profit,
        mode='lines+markers',
        line=dict(color='#f39c12', width=3),
        marker=dict(size=10)
    ))

    fig.update_layout(
        title="Monthly Performance",
        barmode='relative',
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'color': 'white'},
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_title="Month",
        yaxis_title="Amount ($)"
    )

    return fig


@app.callback(
    Output('balance-sheet', 'children'),
    [Input('data-store', 'data'),
     Input('interval-component', 'n_intervals')]
)
def update_balance_sheet(data, n):
    df = DATA['combined']
    if df.empty or 'Type' not in df.columns:
        return html.Div("No data", style={'color': '#e74c3c'})

    # Revenue
    sales = df[df['Type'] == 'Sale']['Net_Clean'].sum()

    # Expenses breakdown
    fees = abs(df[df['Type'] == 'Fee']['Net_Clean'].sum())
    shipping = abs(df[df['Type'] == 'Shipping']['Net_Clean'].sum())
    marketing = abs(df[df['Type'] == 'Marketing']['Net_Clean'].sum())
    refunds = abs(df[df['Type'] == 'Refund']['Net_Clean'].sum())
    taxes = abs(df[df['Type'] == 'Tax']['Net_Clean'].sum())

    total_expenses = fees + shipping + marketing + refunds
    net_income = sales - total_expenses

    # Detailed fee breakdown
    if 'Title' in df.columns:
        fee_df = df[df['Type'] == 'Fee']
        listing_fees = abs(fee_df[fee_df['Title'].str.contains('Listing fee', na=False)]['Net_Clean'].sum())
        transaction_fees = abs(fee_df[fee_df['Title'].str.contains('Transaction fee', na=False)]['Net_Clean'].sum())
        processing_fees = abs(fee_df[fee_df['Title'].str.contains('Processing fee', na=False)]['Net_Clean'].sum())

        marketing_df = df[df['Type'] == 'Marketing']
        etsy_ads = abs(marketing_df[marketing_df['Title'].str.contains('Etsy Ads', na=False)]['Net_Clean'].sum())
        offsite_ads = abs(marketing_df[marketing_df['Title'].str.contains('Offsite Ads', na=False) &
                                       ~marketing_df['Title'].str.contains('Credit', na=False)]['Net_Clean'].sum())
    else:
        listing_fees = transaction_fees = processing_fees = etsy_ads = offsite_ads = 0

    def row(label, amount, indent=0, bold=False, color='white'):
        style = {
            'display': 'flex', 'justifyContent': 'space-between', 'padding': '8px 0',
            'borderBottom': '1px solid #333', 'marginLeft': f'{indent * 20}px'
        }
        if bold:
            style['fontWeight'] = 'bold'
            style['borderBottom'] = '2px solid #666'
        return html.Div([
            html.Span(label, style={'color': color}),
            html.Span(f"${amount:,.2f}", style={'color': color})
        ], style=style)

    return html.Div([
        html.Div([
            html.H3("REVENUE", style={'color': '#2ecc71', 'borderBottom': '1px solid #2ecc71'}),
            row("Gross Sales", sales),
            row("TOTAL REVENUE", sales, bold=True, color='#2ecc71'),
        ], style={'marginBottom': '30px'}),

        html.Div([
            html.H3("COST OF GOODS SOLD", style={'color': '#e74c3c', 'borderBottom': '1px solid #e74c3c'}),
            row("Shipping Labels", shipping),
            row("TOTAL COGS", shipping, bold=True, color='#e74c3c'),
        ], style={'marginBottom': '30px'}),

        row("GROSS PROFIT", sales - shipping, bold=True, color='#f39c12'),

        html.Div([
            html.H3("OPERATING EXPENSES", style={'color': '#e74c3c', 'borderBottom': '1px solid #e74c3c', 'marginTop': '30px'}),
            html.H4("Platform Fees", style={'color': '#aaa', 'marginLeft': '10px'}),
            row("Listing Fees", listing_fees, indent=1),
            row("Transaction Fees", transaction_fees, indent=1),
            row("Processing Fees", processing_fees, indent=1),
            row("Subtotal Fees", fees, indent=1, color='#e74c3c'),

            html.H4("Marketing", style={'color': '#aaa', 'marginLeft': '10px', 'marginTop': '15px'}),
            row("Etsy Ads", etsy_ads, indent=1),
            row("Offsite Ads", offsite_ads, indent=1),
            row("Subtotal Marketing", marketing, indent=1, color='#9b59b6'),

            html.H4("Other", style={'color': '#aaa', 'marginLeft': '10px', 'marginTop': '15px'}),
            row("Refunds Issued", refunds, indent=1),

            row("TOTAL OPERATING EXPENSES", fees + marketing + refunds, bold=True, color='#e74c3c'),
        ], style={'marginBottom': '30px'}),

        html.Div([
            html.H3("SUMMARY", style={'color': '#f39c12', 'borderBottom': '2px solid #f39c12', 'marginTop': '30px'}),
            row("Gross Sales", sales),
            row("Less: Shipping", -shipping),
            row("Less: Fees", -fees),
            row("Less: Marketing", -marketing),
            row("Less: Refunds", -refunds),
            html.Div([
                html.Span("NET INCOME", style={'color': '#f39c12', 'fontWeight': 'bold', 'fontSize': '20px'}),
                html.Span(f"${net_income:,.2f}", style={'color': '#f39c12', 'fontWeight': 'bold', 'fontSize': '20px'})
            ], style={'display': 'flex', 'justifyContent': 'space-between', 'padding': '15px 0',
                      'borderTop': '3px solid #f39c12', 'marginTop': '10px'}),

            html.Div([
                html.Span("Profit Margin"),
                html.Span(f"{(net_income/sales*100) if sales > 0 else 0:.1f}%")
            ], style={'display': 'flex', 'justifyContent': 'space-between', 'color': '#aaa'}),
        ]),

        html.Div([
            html.H4("Tax Note", style={'color': '#aaa', 'marginTop': '30px'}),
            html.P(f"Sales tax collected (pass-through): ${taxes:,.2f}", style={'color': '#666', 'fontSize': '12px'}),
            html.P("This is collected from buyers and remitted to tax authorities - not your income.",
                   style={'color': '#666', 'fontSize': '12px'})
        ])
    ])


@app.callback(
    Output('transactions-table', 'children'),
    [Input('data-store', 'data'),
     Input('interval-component', 'n_intervals')]
)
def update_transactions_table(data, n):
    df = DATA['combined']
    if df.empty:
        return html.Div("No data", style={'color': '#e74c3c'})

    # Show recent sales
    if 'Type' in df.columns and 'Date_Parsed' in df.columns:
        sales_df = df[df['Type'] == 'Sale'].copy()
        sales_df = sales_df.sort_values('Date_Parsed', ascending=False).head(20)

        display_df = sales_df[['Date', 'Title', 'Net_Clean', 'Source']].copy()
        display_df.columns = ['Date', 'Description', 'Amount', 'Platform']
        display_df['Amount'] = display_df['Amount'].apply(lambda x: f"${x:,.2f}")

        return dash_table.DataTable(
            data=display_df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in display_df.columns],
            style_header={
                'backgroundColor': '#1a1a2e',
                'color': 'white',
                'fontWeight': 'bold'
            },
            style_cell={
                'backgroundColor': '#16213e',
                'color': 'white',
                'border': '1px solid #333',
                'textAlign': 'left',
                'padding': '10px'
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'}, 'backgroundColor': '#1a1a2e'}
            ],
            page_size=10
        )

    return html.Div("No transaction data available")


if __name__ == '__main__':
    print("\n" + "="*60)
    print("E-COMMERCE FINANCIAL DASHBOARD")
    print("="*60)
    print("\nOpen your browser to: http://127.0.0.1:8050")
    print("\nFeatures:")
    print("  - Upload Etsy & Amazon CSVs")
    print("  - Visual money flow chart")
    print("  - Expense breakdown")
    print("  - Full balance sheet")
    print("  - Monthly trends")
    print("\nPress Ctrl+C to stop")
    print("="*60 + "\n")

    app.run(debug=False, host='127.0.0.1', port=8050)
