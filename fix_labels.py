"""Fix auto-assigned labels — unassign all, re-assign only with unique matches."""
import json, sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
os.environ['ETSY_API_KEY'] = '3tm5x73bvqc3n9k4bs1on86a'
os.environ['ETSY_SHARED_SECRET'] = 'z3wl2nw4vc'

from supabase_loader import get_config_value, load_data, _get_supabase_client
from dashboard_utils.etsy_api import _load_tokens, _tokens, get_all_ledger_entries
from datetime import datetime
_load_tokens()

d = load_data()
DATA = d['DATA']
ship = DATA[DATA['Type'] == 'Shipping']
entries = get_all_ledger_entries(_tokens['shop_id'], days_back=365)

raw_orders = get_config_value('order_profit_ledger_keycomponentmfg')
orders = json.loads(raw_orders) if isinstance(raw_orders, str) else raw_orders

raw_labels = get_config_value('unmatched_shipping_labels')
labels = json.loads(raw_labels) if isinstance(raw_labels, str) else raw_labels

# Labels manually verified by user — DO NOT TOUCH
user_verified = {
    '302115305592', '301092027914', '301822007239', '301647277494',
    '301317964247', '301318584845', '300997188090', '300567837837',
    '299489491064', '300238175355', '302899163057', '298119078973',
    '295792105746', '298236692938', '298228267051', '297588571565',
    '296068608330', '295887329040', '295792269522', '295747195128',
    '296222370585', '295636847747', '294544727643', '293281621471',
    '293269812555', '292060966236', '292837590739', '287874798352',
    '298032439237', '294346446852',
    # Insurance matched by tracking (verified)
    '301214717136', '300391024791', '296661134195', '292820316684', '293847908229',
}

# Step 1: Unassign all auto-assigned (non-user-verified) labels
unassigned_count = 0
for l in labels:
    if l.get('assigned_to') and l['label_id'] not in user_verified:
        oid = str(l['assigned_to'])
        for o in orders:
            if str(o.get('Order ID')) == oid:
                is_credit = 'credit' in l.get('type', '').lower() or 'refund' in l.get('type', '').lower()
                if is_credit:
                    o['Shipping Label'] = round(o.get('Shipping Label', 0) + l['amount'], 2)
                    o['True Net'] = round(o['True Net'] - l['amount'], 2)
                else:
                    o['Shipping Label'] = round(o.get('Shipping Label', 0) - l['amount'], 2)
                    o['True Net'] = round(o['True Net'] + l['amount'], 2)
                o['Ship P/L'] = round(o.get('Buyer Shipping', 0) - o['Shipping Label'], 2)
                break
        l['assigned_to'] = None
        unassigned_count += 1

print(f"Unassigned {unassigned_count} auto-assigned labels")

# Step 2: Build statement <-> ledger mapping (unique matches only)
stmt_entries = []
for _, s in ship.iterrows():
    info = str(s.get('Info', ''))
    if info.startswith('Label #') or info.startswith('Adjustment #'):
        num = info.replace('Label #', '').replace('Adjustment #', '').strip()
        stmt_entries.append({'num': num, 'amount': abs(s['Net_Clean']), 'date': s['Date'], 'is_adj': info.startswith('Adjustment')})

ledger_ship = []
for e in entries:
    if 'shipping' in e.get('ledger_type', ''):
        ledger_ship.append({
            'ref_id': str(e['reference_id']),
            'amount': abs(e['amount']) / 100,
            'date': datetime.fromtimestamp(e['created_timestamp']).strftime('%B %d, %Y'),
            'type': e['ledger_type'],
        })

# Map by unique amount+date pairs
stmt_to_ledger = {}
ledger_to_stmt = {}
used_ledger = set()

for se in stmt_entries:
    matches = [le for le in ledger_ship
               if abs(le['amount'] - se['amount']) < 0.01
               and le['date'] == se['date']
               and le['ref_id'] not in used_ledger]
    if len(matches) == 1:
        stmt_to_ledger[se['num']] = matches[0]['ref_id']
        ledger_to_stmt[matches[0]['ref_id']] = se['num']
        used_ledger.add(matches[0]['ref_id'])

print(f"Unique stmt <-> ledger mappings: {len(stmt_to_ledger)}")

# Step 3: Build ledger ref_id -> order_id from outbound labels
ledger_to_order = {}
for o in orders:
    lid = o.get('Label ID', '')
    if lid:
        ledger_to_order[lid] = str(o.get('Order ID', ''))

# Step 4: Re-assign adjustments using the chain:
# ledger adj ref_id -> stmt adjustment# (= original label#) -> stmt_to_ledger -> ledger_to_order
reassigned = 0
for l in labels:
    if l.get('assigned_to'):
        continue

    lid = l['label_id']
    stmt_num = ledger_to_stmt.get(lid)
    if not stmt_num:
        continue

    # For adjustments: the statement Adjustment# IS the original Label#
    # Look up original Label# in stmt_to_ledger to get the outbound label's ledger ref_id
    orig_ledger = stmt_to_ledger.get(stmt_num)
    if orig_ledger:
        order_id = ledger_to_order.get(orig_ledger)
        if order_id:
            l['assigned_to'] = order_id
            for o in orders:
                if str(o.get('Order ID')) == order_id:
                    is_credit = 'credit' in l.get('type', '').lower() or 'refund' in l.get('type', '').lower()
                    if is_credit:
                        o['Shipping Label'] = round(o.get('Shipping Label', 0) - l['amount'], 2)
                        o['True Net'] = round(o['True Net'] + l['amount'], 2)
                    else:
                        o['Shipping Label'] = round(o.get('Shipping Label', 0) + l['amount'], 2)
                        o['True Net'] = round(o['True Net'] - l['amount'], 2)
                    o['Ship P/L'] = round(o.get('Buyer Shipping', 0) - o['Shipping Label'], 2)
                    reassigned += 1
                    break

remaining = [l for l in labels if not l.get('assigned_to')]
print(f"Re-assigned (unique chain only): {reassigned}")
print(f"Still unassigned: {len(remaining)}")

for l in remaining:
    print(f"  #{l['label_id']} ${l['amount']} {l['date']} {l['type']}")

client = _get_supabase_client()
client.table('config').upsert(dict(key='order_profit_ledger_keycomponentmfg', value=json.dumps(orders)), on_conflict='key').execute()
client.table('config').upsert(dict(key='unmatched_shipping_labels', value=json.dumps(labels)), on_conflict='key').execute()
print('Saved')
