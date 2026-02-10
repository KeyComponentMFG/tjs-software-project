"""
Etsy Financial Dashboard - Simple Version
"""

import dash
from dash import dcc, html, dash_table
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os

# Load all Etsy data immediately
print("Loading Etsy data...")
folder = r"C:\Users\mcnug\OneDrive\Desktop\etsy statments"

def parse_money(val):
    if pd.isna(val) or val == '--' or val == '':
        return 0.0
    val = str(val).replace('$', '').replace(',', '').replace('"', '')
    try:
        return float(val)
    except:
        return 0.0

all_data = []
for f in os.listdir(folder):
    if f.startswith('etsy_statement') and f.endswith('.csv'):
        print(f"  Loading {f}...")
        df = pd.read_csv(os.path.join(folder, f))
        all_data.append(df)

DATA = pd.concat(all_data, ignore_index=True)
DATA['Net_Clean'] = DATA['Net'].apply(parse_money)
DATA['Date_Parsed'] = pd.to_datetime(DATA['Date'], format='%B %d, %Y', errors='coerce')
DATA['Month'] = DATA['Date_Parsed'].dt.to_period('M').astype(str)

print(f"Loaded {len(DATA)} transactions")

# Calculate all metrics
sales = DATA[DATA['Type'] == 'Sale']['Net_Clean'].sum()
fees = abs(DATA[DATA['Type'] == 'Fee']['Net_Clean'].sum())
shipping = abs(DATA[DATA['Type'] == 'Shipping']['Net_Clean'].sum())
marketing = abs(DATA[DATA['Type'] == 'Marketing']['Net_Clean'].sum())
refunds = abs(DATA[DATA['Type'] == 'Refund']['Net_Clean'].sum())
taxes = abs(DATA[DATA['Type'] == 'Tax']['Net_Clean'].sum())
order_count = len(DATA[DATA['Type'] == 'Sale'])
net_profit = sales - fees - shipping - marketing - refunds

print(f"\nSales: ${sales:,.2f}")
print(f"Orders: {order_count}")
print(f"Net Profit: ${net_profit:,.2f}")

# Fee breakdown
fee_df = DATA[DATA['Type'] == 'Fee']
listing_fees = abs(fee_df[fee_df['Title'].str.contains('Listing fee', na=False)]['Net_Clean'].sum())
transaction_fees = abs(fee_df[fee_df['Title'].str.contains('Transaction fee', na=False)]['Net_Clean'].sum())
processing_fees = abs(fee_df[fee_df['Title'].str.contains('Processing fee', na=False)]['Net_Clean'].sum())

# Marketing breakdown
marketing_df = DATA[DATA['Type'] == 'Marketing']
etsy_ads = abs(marketing_df[marketing_df['Title'].str.contains('Etsy Ads', na=False)]['Net_Clean'].sum())
offsite_ads = abs(marketing_df[marketing_df['Title'].str.contains('Offsite Ads', na=False) &
                               ~marketing_df['Title'].str.contains('Credit', na=False)]['Net_Clean'].sum())

# Monthly data
monthly = DATA.groupby(['Month', 'Type'])['Net_Clean'].sum().unstack(fill_value=0)

# App
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("ETSY STORE FINANCES", style={'textAlign': 'center', 'color': '#f39c12', 'padding': '20px'}),

    # Big numbers
    html.Div([
        html.Div([
            html.H2(f"${sales:,.2f}", style={'color': '#2ecc71', 'margin': '0', 'fontSize': '48px'}),
            html.P("Gross Sales", style={'color': '#aaa'})
        ], style={'textAlign': 'center', 'width': '25%', 'display': 'inline-block'}),

        html.Div([
            html.H2(f"{order_count}", style={'color': '#3498db', 'margin': '0', 'fontSize': '48px'}),
            html.P("Total Orders", style={'color': '#aaa'})
        ], style={'textAlign': 'center', 'width': '25%', 'display': 'inline-block'}),

        html.Div([
            html.H2(f"${net_profit:,.2f}", style={'color': '#f39c12', 'margin': '0', 'fontSize': '48px'}),
            html.P("Net Profit", style={'color': '#aaa'})
        ], style={'textAlign': 'center', 'width': '25%', 'display': 'inline-block'}),

        html.Div([
            html.H2(f"{(net_profit/sales*100):.1f}%", style={'color': '#9b59b6', 'margin': '0', 'fontSize': '48px'}),
            html.P("Profit Margin", style={'color': '#aaa'})
        ], style={'textAlign': 'center', 'width': '25%', 'display': 'inline-block'}),
    ], style={'backgroundColor': '#1a1a2e', 'padding': '30px', 'borderRadius': '10px', 'marginBottom': '20px'}),

    # Money flow chart
    html.Div([
        dcc.Graph(
            figure=go.Figure(go.Waterfall(
                orientation="v",
                measure=["absolute", "relative", "relative", "relative", "relative", "total"],
                x=["Gross Sales", "Fees", "Shipping", "Marketing", "Refunds", "NET PROFIT"],
                y=[sales, -fees, -shipping, -marketing, -refunds, 0],
                connector={"line": {"color": "#888"}},
                decreasing={"marker": {"color": "#e74c3c"}},
                increasing={"marker": {"color": "#2ecc71"}},
                totals={"marker": {"color": "#f39c12"}}
            )).update_layout(
                title="WHERE YOUR MONEY GOES",
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                height=400
            )
        )
    ], style={'width': '60%', 'display': 'inline-block', 'verticalAlign': 'top'}),

    # Expense pie
    html.Div([
        dcc.Graph(
            figure=px.pie(
                values=[listing_fees, transaction_fees, processing_fees, shipping, etsy_ads, offsite_ads, refunds],
                names=['Listing Fees', 'Transaction Fees', 'Processing Fees', 'Shipping', 'Etsy Ads', 'Offsite Ads', 'Refunds'],
                title="EXPENSE BREAKDOWN",
                color_discrete_sequence=['#e74c3c', '#c0392b', '#a93226', '#3498db', '#9b59b6', '#8e44ad', '#f39c12']
            ).update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', height=400)
        )
    ], style={'width': '40%', 'display': 'inline-block', 'verticalAlign': 'top'}),

    # Balance Sheet
    html.Div([
        html.H2("BALANCE SHEET", style={'color': '#f39c12', 'borderBottom': '2px solid #f39c12'}),
        html.Table([
            html.Tr([html.Td("REVENUE", style={'fontWeight': 'bold', 'color': '#2ecc71'}), html.Td("")]),
            html.Tr([html.Td("  Gross Sales"), html.Td(f"${sales:,.2f}", style={'textAlign': 'right'})]),
            html.Tr([html.Td(""), html.Td("")]),

            html.Tr([html.Td("EXPENSES", style={'fontWeight': 'bold', 'color': '#e74c3c'}), html.Td("")]),
            html.Tr([html.Td("  Listing Fees"), html.Td(f"-${listing_fees:,.2f}", style={'textAlign': 'right', 'color': '#e74c3c'})]),
            html.Tr([html.Td("  Transaction Fees"), html.Td(f"-${transaction_fees:,.2f}", style={'textAlign': 'right', 'color': '#e74c3c'})]),
            html.Tr([html.Td("  Processing Fees"), html.Td(f"-${processing_fees:,.2f}", style={'textAlign': 'right', 'color': '#e74c3c'})]),
            html.Tr([html.Td("  Shipping Labels"), html.Td(f"-${shipping:,.2f}", style={'textAlign': 'right', 'color': '#e74c3c'})]),
            html.Tr([html.Td("  Etsy Ads"), html.Td(f"-${etsy_ads:,.2f}", style={'textAlign': 'right', 'color': '#e74c3c'})]),
            html.Tr([html.Td("  Offsite Ads"), html.Td(f"-${offsite_ads:,.2f}", style={'textAlign': 'right', 'color': '#e74c3c'})]),
            html.Tr([html.Td("  Refunds"), html.Td(f"-${refunds:,.2f}", style={'textAlign': 'right', 'color': '#e74c3c'})]),
            html.Tr([html.Td(""), html.Td("")]),

            html.Tr([
                html.Td("NET PROFIT", style={'fontWeight': 'bold', 'color': '#f39c12', 'fontSize': '20px'}),
                html.Td(f"${net_profit:,.2f}", style={'textAlign': 'right', 'fontWeight': 'bold', 'color': '#f39c12', 'fontSize': '20px'})
            ], style={'borderTop': '2px solid #f39c12'}),

            html.Tr([html.Td(""), html.Td("")]),
            html.Tr([html.Td("Taxes Collected (pass-through)", style={'color': '#666'}),
                     html.Td(f"${taxes:,.2f}", style={'textAlign': 'right', 'color': '#666'})]),
        ], style={'width': '100%', 'color': 'white'})
    ], style={'backgroundColor': '#1a1a2e', 'padding': '20px', 'borderRadius': '10px', 'marginTop': '20px'}),

    # Monthly breakdown
    html.Div([
        html.H2("MONTHLY BREAKDOWN", style={'color': '#3498db'}),
        html.Table([
            html.Tr([
                html.Th("Month"), html.Th("Sales"), html.Th("Fees"), html.Th("Shipping"),
                html.Th("Marketing"), html.Th("Net")
            ], style={'borderBottom': '2px solid #3498db'})
        ] + [
            html.Tr([
                html.Td(month),
                html.Td(f"${monthly.loc[month].get('Sale', 0):,.2f}"),
                html.Td(f"${abs(monthly.loc[month].get('Fee', 0)):,.2f}", style={'color': '#e74c3c'}),
                html.Td(f"${abs(monthly.loc[month].get('Shipping', 0)):,.2f}", style={'color': '#e74c3c'}),
                html.Td(f"${abs(monthly.loc[month].get('Marketing', 0)):,.2f}", style={'color': '#e74c3c'}),
                html.Td(f"${monthly.loc[month].get('Sale', 0) + monthly.loc[month].get('Fee', 0) + monthly.loc[month].get('Shipping', 0) + monthly.loc[month].get('Marketing', 0):,.2f}",
                       style={'color': '#f39c12', 'fontWeight': 'bold'})
            ]) for month in sorted(monthly.index)
        ], style={'width': '100%', 'color': 'white'})
    ], style={'backgroundColor': '#1a1a2e', 'padding': '20px', 'borderRadius': '10px', 'marginTop': '20px'}),

], style={'backgroundColor': '#0f0f1a', 'minHeight': '100vh', 'padding': '20px', 'fontFamily': 'Arial'})

if __name__ == '__main__':
    print("\n" + "="*50)
    print("ETSY DASHBOARD RUNNING")
    print("Open: http://127.0.0.1:8050")
    print("="*50 + "\n")
    app.run(debug=False, port=8050)
