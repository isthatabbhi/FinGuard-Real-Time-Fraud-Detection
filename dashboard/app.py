"""
FinGuard - Real-Time Credit Card Fraud Detection and Streaming Monitoring Dashboard
====================================================================================
A custom enterprise web application serving real-time fraud monitoring,
Databricks Lakeflow stream inspection, and live interview simulation.
100 percent custom-built: zero third-party platform watermarks or branding.
"""

import os
import sys
import json
import re
import random
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from flask import Flask, render_template, jsonify, request

app = Flask(__name__, template_folder='templates')

BASE_DIR = Path(__file__).resolve().parent.parent

# ------------------------------------------------------------------------------
# Data Loading and Initialization
# ------------------------------------------------------------------------------
def load_watchlist():
    csv_path = BASE_DIR / 'databricks notebooks and pipelines' / 'finguard_project' / 'fraud_watchlist_file_generator' / 'fraud_watchlist.csv'
    watchlist = []
    if csv_path.exists():
        try:
            import pandas as pd
            df = pd.read_csv(csv_path)
            for _, r in df.iterrows():
                watchlist.append({
                    'entity_id': str(r.get('entity_id', '')),
                    'watchlist_id': str(r.get('watchlist_id', 'WL0001')),
                    'risk_level': str(r.get('risk_level', 'HIGH')).upper(),
                    'reason_description': str(r.get('reason_description', 'Fraudulent activity reported'))
                })
        except Exception as e:
            print(f'Error loading watchlist: {e}')
    return watchlist

def load_sample_customers():
    sql_path = BASE_DIR / 'postgres sql' / 'customers_historic.sql'
    customers = []
    if sql_path.exists():
        try:
            with open(sql_path, 'r', encoding='utf-8') as f:
                c_data = f.read()
            pattern = r"\('([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*(\d+),\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*([\d\.]+),\s*'([^']+)',\s*'([^']+)',\s*(\d+),\s*([\d\.]+),\s*([\d\.]+),\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*([\d\.]+)"
            rows = re.findall(pattern, c_data)
            for r in rows:
                customers.append({
                    'customer_id': r[0], 'first_name': r[1], 'last_name': r[2], 'gender': r[3],
                    'age': int(r[4]), 'city': r[5], 'state': r[6], 'country': r[7], 'annual_income': float(r[8]),
                    'customer_segment': r[9], 'account_open_date': r[10], 'risk_score': int(r[11]),
                    'preferred_spending_min': float(r[12]), 'preferred_spending_max': float(r[13]),
                    'preferred_city': r[14], 'preferred_country': r[15], 'trusted_device_id': r[16],
                    'card_number': r[17], 'card_type': r[18], 'email': r[19], 'transaction_limit': float(r[20])
                })
        except Exception as e:
            print(f'Error loading customers from SQL: {e}')
            
    if not customers:
        for i in range(1, 101):
            customers.append({
                'customer_id': f'CUST{i:06d}', 'first_name': f'Customer{i}', 'last_name': 'Sharma', 'gender': 'Male',
                'age': 30 + (i % 30), 'city': 'Mumbai', 'state': 'Maharashtra', 'country': 'India', 'annual_income': 800000.0,
                'customer_segment': 'Regular', 'account_open_date': '2023-01-01', 'risk_score': 25 + (i % 60),
                'preferred_spending_min': 1000.0, 'preferred_spending_max': 25000.0, 'preferred_city': 'Mumbai',
                'preferred_country': 'India', 'trusted_device_id': f'DEV{i:04d}',
                'card_number': f'500851403696{i:04d}', 'card_type': 'Visa', 'email': 'databeli14@gmail.com', 'transaction_limit': 100000.0
            })
    return customers

WATCHLIST = load_watchlist()
CUSTOMERS = load_sample_customers()

MERCHANTS = [
    {'id': 'MERC001', 'name': 'Amazon India', 'category': 'Retail', 'city': 'Bengaluru', 'country': 'India'},
    {'id': 'MERC002', 'name': 'Flipkart Online', 'category': 'Retail', 'city': 'Bengaluru', 'country': 'India'},
    {'id': 'MERC003', 'name': 'Reliance Digital', 'category': 'Electronics', 'city': 'Mumbai', 'country': 'India'},
    {'id': 'MERC004', 'name': 'Taj Hotels', 'category': 'Travel', 'city': 'Mumbai', 'country': 'India'},
    {'id': 'MERC005', 'name': 'MakeMyTrip Flights', 'category': 'Travel', 'city': 'Delhi', 'country': 'India'},
    {'id': 'MERC006', 'name': 'Swiggy Delivery', 'category': 'Food', 'city': 'Bengaluru', 'country': 'India'},
    {'id': 'MERC007', 'name': 'Zomato Dineout', 'category': 'Food', 'city': 'Delhi', 'country': 'India'},
    {'id': 'MERC008', 'name': 'Apollo Pharmacy', 'category': 'Healthcare', 'city': 'Chennai', 'country': 'India'},
    {'id': 'MERC009', 'name': 'Apple Store London', 'category': 'Electronics', 'city': 'London', 'country': 'United Kingdom'},
    {'id': 'MERC010', 'name': 'Dubai Mall Luxury', 'category': 'Retail', 'city': 'Dubai', 'country': 'UAE'},
]

# In-memory streaming state
TRANSACTIONS = []
ALERTS = []
KAFKA_OFFSET = 1000

def generate_transaction(cust, is_fraud_high=False, is_fraud_card=False):
    global KAFKA_OFFSET
    KAFKA_OFFSET += 1
    merchant = random.choice(MERCHANTS)
    txn_id = f'TXN{random.randint(100000, 999999)}'
    
    limit = float(cust.get('transaction_limit', 100000.0))
    if is_fraud_high:
        amount = round(limit + random.uniform(5000.0, 50000.0), 2)
    else:
        amount = round(random.uniform(50.0, limit * 0.4), 2)
        
    card_number = '5008514036965665' if is_fraud_card else str(cust['card_number'])
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    raw_payload = {
        'transaction_id': txn_id,
        'customer_id': cust['customer_id'],
        'card_number': card_number,
        'merchant_id': merchant['id'],
        'merchant_name': merchant['name'],
        'merchant_category': merchant['category'],
        'amount': amount,
        'currency': 'INR',
        'transaction_type': random.choice(['Purchase', 'Online', 'POS']),
        'payment_channel': random.choice(['UPI', 'Credit Card', 'Net Banking', 'POS Terminal']),
        'device_id': cust.get('trusted_device_id', 'DEV0001'),
        'city': merchant['city'],
        'country': merchant['country'],
        'transaction_timestamp': now_str,
        'is_international': merchant['country'] != 'India',
        'status': 'APPROVED'
    }
    
    kafka_event = {
        'key': txn_id,
        'value': json.dumps(raw_payload),
        'topic': 'credit_card_transactions',
        'partition': random.randint(0, 5),
        'offset': KAFKA_OFFSET,
        'timestamp': now_str,
        'ingestion_timestamp': now_str,
        **raw_payload,
        'customer_name': f"{cust['first_name']} {cust['last_name']}",
        'customer_email': cust.get('email', 'databeli14@gmail.com'),
        'customer_limit': limit,
        'risk_score': int(cust.get('risk_score', 30))
    }
    
    TRANSACTIONS.insert(0, kafka_event)
    if len(TRANSACTIONS) > 300:
        TRANSACTIONS.pop()
        
    # High-Value Alert (Stream-Static Join rule)
    if amount > limit:
        alert = {
            'alert_id': f'ALERT-HIGH-{txn_id}',
            'alert_type': 'HIGH_VALUE_TRANSACTION',
            'severity': 'CRITICAL',
            'alert_timestamp': now_str,
            'transaction_id': txn_id,
            'customer_id': cust['customer_id'],
            'customer_name': kafka_event['customer_name'],
            'customer_email': kafka_event['customer_email'],
            'amount': amount,
            'limit': limit,
            'merchant_name': merchant['name'],
            'location': f"{merchant['city']}, {merchant['country']}",
            'reason': f'Amount Rs.{amount:,.2f} exceeded customer limit of Rs.{limit:,.2f}'
        }
        ALERTS.insert(0, alert)
        
    # Watchlist Alert (Stream-Stream Join rule)
    matched_wl = [w for w in WATCHLIST if w['entity_id'] == card_number]
    if matched_wl or is_fraud_card:
        wl_info = matched_wl[0] if matched_wl else {'watchlist_id': 'WL000001', 'risk_level': 'HIGH', 'reason_description': 'Card on active fraud watchlist'}
        alert = {
            'alert_id': f"ALERT-WL-{txn_id}-{wl_info['watchlist_id']}",
            'alert_type': 'FRAUD_WATCHLIST_MATCH',
            'severity': wl_info['risk_level'],
            'alert_timestamp': now_str,
            'transaction_id': txn_id,
            'customer_id': cust['customer_id'],
            'customer_name': kafka_event['customer_name'],
            'customer_email': kafka_event['customer_email'],
            'amount': amount,
            'limit': limit,
            'merchant_name': merchant['name'],
            'location': f"{merchant['city']}, {merchant['country']}",
            'reason': f"Watchlist hit: {wl_info['reason_description']} (Card: .... {card_number[-4:]})"
        }
        ALERTS.insert(0, alert)
        
    if len(ALERTS) > 100:
        ALERTS.pop()
        
    return kafka_event

# Pre-populate 15 initial events
for _ in range(15):
    c = random.choice(CUSTOMERS)
    generate_transaction(c)

def compute_state():
    total_txns = len(TRANSACTIONS)
    total_alerts = len(ALERTS)
    total_amount = sum(t['amount'] for t in TRANSACTIONS)
    avg_amount = total_amount / total_txns if total_txns > 0 else 0.0
    high_risk_custs = sum(1 for c in CUSTOMERS if c.get('risk_score', 0) > 70)
    
    cat_counts = {}
    chan_counts = {}
    intl_counts = {'Domestic': 0, 'International': 0}
    merc_counts = {}
    
    for t in TRANSACTIONS:
        cat = t.get('merchant_category', 'Other')
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
        
        chan = t.get('payment_channel', 'Other')
        chan_counts[chan] = chan_counts.get(chan, 0) + 1
        
        if t.get('is_international', False):
            intl_counts['International'] += 1
        else:
            intl_counts['Domestic'] += 1
            
        merc = t.get('merchant_name', 'Other')
        merc_counts[merc] = merc_counts.get(merc, 0) + 1
        
    sorted_mercs = sorted(merc_counts.items(), key=lambda x: x[1], reverse=True)[:8]
    
    return {
        'kpis': {
            'total_txns': total_txns,
            'total_alerts': total_alerts,
            'total_amount': f"{total_amount:,.2f}",
            'avg_amount': f"{avg_amount:,.2f}",
            'high_risk_custs': high_risk_custs
        },
        'transactions': TRANSACTIONS[:15],
        'alerts': ALERTS[:12],
        'bronze': [
            {
                'key': t['key'],
                'topic': t['topic'],
                'partition': t['partition'],
                'offset': t['offset'],
                'timestamp': t['timestamp']
            } for t in TRANSACTIONS[:10]
        ],
        'silver': [
            {
                'transaction_id': t['transaction_id'],
                'customer_id': t['customer_id'],
                'amount': f"{t['amount']:,.2f}",
                'merchant_name': t['merchant_name'],
                'merchant_category': t['merchant_category'],
                'payment_channel': t['payment_channel'],
                'city': t['city'],
                'status': t['status']
            } for t in TRANSACTIONS[:10]
        ],
        'gold_high': [
            {
                'alert_id': a['alert_id'],
                'customer_name': a['customer_name'],
                'amount': f"{a['amount']:,.2f}",
                'limit': f"{a['limit']:,.2f}",
                'merchant_name': a['merchant_name']
            } for a in ALERTS if a['alert_type'] == 'HIGH_VALUE_TRANSACTION'
        ][:10],
        'gold_watchlist': [
            {
                'alert_id': a['alert_id'],
                'customer_name': a['customer_name'],
                'amount': f"{a['amount']:,.2f}",
                'merchant_name': a['merchant_name'],
                'reason': a['reason']
            } for a in ALERTS if a['alert_type'] == 'FRAUD_WATCHLIST_MATCH'
        ][:10],
        'analytics': {
            'categories': cat_counts,
            'channels': chan_counts,
            'intl': intl_counts,
            'merchants': {'labels': [m[0] for m in sorted_mercs], 'values': [m[1] for m in sorted_mercs]}
        }
    }

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/state', methods=['GET'])
def api_state():
    return jsonify(compute_state())

@app.route('/api/simulate/high-value', methods=['POST'])
def sim_high_value():
    reg_custs = [c for c in CUSTOMERS if c.get('customer_segment') == 'Regular']
    cust = random.choice(reg_custs) if reg_custs else random.choice(CUSTOMERS)
    evt = generate_transaction(cust, is_fraud_high=True)
    return jsonify({'success': True, 'event': evt, 'state': compute_state()})

@app.route('/api/simulate/watchlist', methods=['POST'])
def sim_watchlist():
    cust = random.choice(CUSTOMERS)
    evt = generate_transaction(cust, is_fraud_card=True)
    return jsonify({'success': True, 'event': evt, 'state': compute_state()})

@app.route('/api/simulate/normal', methods=['POST'])
def sim_normal():
    for _ in range(5):
        c = random.choice(CUSTOMERS)
        generate_transaction(c)
    return jsonify({'success': True, 'state': compute_state()})

@app.route('/api/test-email', methods=['POST'])
def test_email():
    data = request.json or {}
    user = data.get('gmail_user')
    password = data.get('gmail_pass')
    if not password:
        return jsonify({'success': True, 'simulated': True, 'message': 'Demo mode: SMTP credentials not set (simulated success).'})
    try:
        msg = MIMEMultipart()
        msg['From'] = user
        msg['To'] = user
        msg['Subject'] = 'FinGuard Security Alert'
        msg.attach(MIMEText('<h3>FinGuard Alert System Active</h3><p>Real-time SMTP connection verified.</p>', 'html'))
        with smtplib.SMTP('smtp.gmail.com', 587, timeout=10) as server:
            server.starttls()
            server.login(user, password)
            server.send_message(msg)
        return jsonify({'success': True, 'message': 'Email dispatched successfully via Gmail SMTP!'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    print(f'Starting FinGuard Dashboard on port {port}...')
    app.run(host='0.0.0.0', port=port, debug=False)
