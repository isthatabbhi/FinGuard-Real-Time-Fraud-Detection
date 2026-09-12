"""
FinGuard - Real-Time Credit Card Fraud Detection & Streaming Monitoring Dashboard
================================================================================
An interactive operational & analytics dashboard replicating Databricks Lakeview / Delta Live Tables,
designed for live interview panel demonstrations, streaming simulation, and real-time fraud alerting.
"""

import os
import sys
import time
import json
import re
import random
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Set page configuration
st.set_page_config(
    page_title="FinGuard | Real-Time Fraud Detection Platform",
    page_icon="https://img.icons8.com/color/96/shield.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0px;
    }
    .sub-header {
        font-size: 1rem;
        color: #64748B;
        margin-bottom: 20px;
    }
    .metric-card {
        background-color: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .badge-bronze { background-color: #CD7F32; color: white; padding: 3px 8px; border-radius: 5px; font-weight: 600; font-size: 0.8rem; }
    .badge-silver { background-color: #94A3B8; color: white; padding: 3px 8px; border-radius: 5px; font-weight: 600; font-size: 0.8rem; }
    .badge-gold { background-color: #EAB308; color: black; padding: 3px 8px; border-radius: 5px; font-weight: 600; font-size: 0.8rem; }
    .alert-box-high {
        background-color: #FEF2F2;
        border-left: 5px solid #EF4444;
        padding: 12px;
        margin-bottom: 10px;
        border-radius: 4px;
    }
    .alert-box-card {
        background-color: #FFFBEB;
        border-left: 5px solid #F59E0B;
        padding: 12px;
        margin-bottom: 10px;
        border-radius: 4px;
    }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# Data Loading & Initialization Helpers
# ------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

@st.cache_data
def load_watchlist():
    csv_path = BASE_DIR / "databricks notebooks and pipelines" / "finguard_project" / "fraud_watchlist_file_generator" / "fraud_watchlist.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        df['entity_id'] = df['entity_id'].astype(str)
        return df
    return pd.DataFrame()

@st.cache_data
def load_sample_customers():
    # Try parsing from SQL file
    sql_path = BASE_DIR / "postgres sql" / "customers_historic.sql"
    customers = []
    if sql_path.exists():
        try:
            with open(sql_path, "r", encoding="utf-8") as f:
                content = f.read()
            # Regex match rows in VALUES (...)
            rows = re.findall(r"\('([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*(\d+),\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*([\d\.]+),\s*'([^']+)',\s*'([^']+)',\s*(\d+),\s*([\d\.]+),\s*([\d\.]+),\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*([\d\.]+)", content)
            for r in rows:
                customers.append({
                    "customer_id": r[0], "first_name": r[1], "last_name": r[2], "gender": r[3],
                    "age": int(r[4]), "city": r[5], "state": r[6], "country": r[7], "annual_income": float(r[8]),
                    "customer_segment": r[9], "account_open_date": r[10], "risk_score": int(r[11]),
                    "preferred_spending_min": float(r[12]), "preferred_spending_max": float(r[13]),
                    "preferred_city": r[14], "preferred_country": r[15], "trusted_device_id": r[16],
                    "card_number": r[17], "card_type": r[18], "email": r[19], "transaction_limit": float(r[20])
                })
        except Exception:
            pass
            
    if not customers:
        # Fallback dummy customers
        for i in range(1, 101):
            customers.append({
                "customer_id": f"CUST{i:06d}", "first_name": f"Customer{i}", "last_name": "Sharma", "gender": "Male",
                "age": 30 + (i % 30), "city": "Mumbai", "state": "Maharashtra", "country": "India", "annual_income": 800000.0,
                "customer_segment": "Regular", "account_open_date": "2023-01-01", "risk_score": 25 + (i % 60),
                "preferred_spending_min": 1000.0, "preferred_spending_max": 25000.0, "preferred_city": "Mumbai",
                "preferred_country": "India", "trusted_device_id": f"DEV{i:04d}",
                "card_number": f"500851403696{i:04d}", "card_type": "Visa", "email": "databeli14@gmail.com", "transaction_limit": 100000.0
            })
    return pd.DataFrame(customers)

# Initialize Session State
if "transactions" not in st.session_state:
    st.session_state.transactions = []
if "alerts" not in st.session_state:
    st.session_state.alerts = []
if "kafka_offset" not in st.session_state:
    st.session_state.kafka_offset = 1000
if "is_streaming" not in st.session_state:
    st.session_state.is_streaming = False

watchlist_df = load_watchlist()
customers_df = load_sample_customers()

# Sample Merchants
MERCHANTS = [
    {"id": "MERC001", "name": "Amazon India", "category": "Retail", "city": "Bengaluru", "country": "India"},
    {"id": "MERC002", "name": "Flipkart Online", "category": "Retail", "city": "Bengaluru", "country": "India"},
    {"id": "MERC003", "name": "Reliance Digital", "category": "Electronics", "city": "Mumbai", "country": "India"},
    {"id": "MERC004", "name": "Taj Hotels", "category": "Travel", "city": "Mumbai", "country": "India"},
    {"id": "MERC005", "name": "MakeMyTrip Flights", "category": "Travel", "city": "Delhi", "country": "India"},
    {"id": "MERC006", "name": "Swiggy Delivery", "category": "Food", "city": "Bengaluru", "country": "India"},
    {"id": "MERC007", "name": "Zomato Dineout", "category": "Food", "city": "Delhi", "country": "India"},
    {"id": "MERC008", "name": "Apollo Pharmacy", "category": "Healthcare", "city": "Chennai", "country": "India"},
    {"id": "MERC009", "name": "Apple Store London", "category": "Electronics", "city": "London", "country": "United Kingdom"},
    {"id": "MERC010", "name": "Dubai Mall Luxury", "category": "Retail", "city": "Dubai", "country": "UAE"},
]

def send_real_alert_email(email_to, subject, body_html, smtp_user, smtp_pass):
    try:
        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = email_to
        msg["Subject"] = subject
        msg.attach(MIMEText(body_html, "html"))
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        return True, "Email dispatched successfully!"
    except Exception as e:
        return False, str(e)

def generate_transaction_event(cust, is_fraud_high=False, is_fraud_card=False):
    merchant = random.choice(MERCHANTS)
    st.session_state.kafka_offset += 1
    txn_id = f"TXN{random.randint(100000, 999999)}"
    
    # Amount
    if is_fraud_high:
        amount = round(float(cust["transaction_limit"]) + random.uniform(5000.0, 50000.0), 2)
    else:
        amount = round(random.uniform(50.0, float(cust["transaction_limit"]) * 0.4), 2)
        
    card_number = "5008514036965665" if is_fraud_card else str(cust["card_number"])
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    raw_payload = {
        "transaction_id": txn_id,
        "customer_id": cust["customer_id"],
        "card_number": card_number,
        "merchant_id": merchant["id"],
        "merchant_name": merchant["name"],
        "merchant_category": merchant["category"],
        "amount": amount,
        "currency": "INR",
        "transaction_type": random.choice(["Purchase", "Online", "POS"]),
        "payment_channel": random.choice(["UPI", "Credit Card", "Net Banking", "POS Terminal"]),
        "device_id": cust.get("trusted_device_id", "DEV0001"),
        "city": merchant["city"],
        "country": merchant["country"],
        "transaction_timestamp": now_str,
        "is_international": merchant["country"] != "India",
        "status": "APPROVED"
    }
    
    # Bronze metadata
    kafka_event = {
        "key": txn_id,
        "value": json.dumps(raw_payload),
        "topic": "credit_card_transactions",
        "partition": random.randint(0, 5),
        "offset": st.session_state.kafka_offset,
        "timestamp": now_str,
        "ingestion_timestamp": now_str,
        # Silver parsed fields
        **raw_payload,
        "customer_name": f"{cust['first_name']} {cust['last_name']}",
        "customer_email": cust.get("email", "databeli14@gmail.com"),
        "customer_limit": float(cust.get("transaction_limit", 100000.0)),
        "risk_score": int(cust.get("risk_score", 30))
    }
    
    st.session_state.transactions.insert(0, kafka_event)
    if len(st.session_state.transactions) > 300:
        st.session_state.transactions.pop()
        
    # Gold layer evaluation:
    # 1. High-Value Alert (Stream-Static Join: amount > transaction_limit)
    if amount > kafka_event["customer_limit"]:
        alert = {
            "alert_id": f"ALERT-HIGH-{txn_id}",
            "alert_type": "HIGH_VALUE_TRANSACTION",
            "severity": "CRITICAL",
            "alert_timestamp": now_str,
            "transaction_id": txn_id,
            "customer_id": cust["customer_id"],
            "customer_name": kafka_event["customer_name"],
            "customer_email": kafka_event["customer_email"],
            "amount": amount,
            "limit": kafka_event["customer_limit"],
            "merchant_name": merchant["name"],
            "location": f"{merchant['city']}, {merchant['country']}",
            "reason": f"Amount Rs.{amount:,.2f} exceeded customer threshold limit of Rs.{kafka_event['customer_limit']:,.2f}"
        }
        st.session_state.alerts.insert(0, alert)
        
    # 2. Watchlist Alert (Stream-Stream Join with Watermark: card_number == entity_id)
    matched_wl = watchlist_df[watchlist_df['entity_id'] == card_number] if not watchlist_df.empty else pd.DataFrame()
    if not matched_wl.empty or is_fraud_card:
        wl_row = matched_wl.iloc[0] if not matched_wl.empty else None
        reason = wl_row["reason_description"] if wl_row is not None else "Card present on active fraud watchlist"
        risk_lvl = str(wl_row["risk_level"]).upper() if wl_row is not None else "HIGH"
        wl_id = wl_row["watchlist_id"] if wl_row is not None else "WL000001"
        alert = {
            "alert_id": f"ALERT-WL-{txn_id}-{wl_id}",
            "alert_type": "FRAUD_WATCHLIST_MATCH",
            "severity": risk_lvl,
            "alert_timestamp": now_str,
            "transaction_id": txn_id,
            "customer_id": cust["customer_id"],
            "customer_name": kafka_event["customer_name"],
            "customer_email": kafka_event["customer_email"],
            "amount": amount,
            "limit": kafka_event["customer_limit"],
            "merchant_name": merchant["name"],
            "location": f"{merchant['city']}, {merchant['country']}",
            "reason": f"Watchlist hit: {reason} (Card: •••• {card_number[-4:]})"
        }
        st.session_state.alerts.insert(0, alert)
        
    if len(st.session_state.alerts) > 100:
        st.session_state.alerts.pop()
        
    return kafka_event

# Pre-populate initial traffic if empty
if len(st.session_state.transactions) == 0:
    for _ in range(15):
        c = customers_df.sample(1).iloc[0].to_dict()
        generate_transaction_event(c)

# ------------------------------------------------------------------------------
# Sidebar Controls & Live Authentication
# ------------------------------------------------------------------------------
with st.sidebar:
    st.image("https://img.icons8.com/color/96/shield.png", width=64)
    st.markdown("### **FinGuard Operations**")
    st.caption("Platform: Databricks Lakeflow & Delta Lake")
    
    st.markdown("---")
    st.subheader("Live Interview Simulation")
    
    col_sim1, col_sim2 = st.columns(2)
    with col_sim1:
        if st.button("High-Value Fraud", use_container_width=True, help="Triggers amount > limit fraud alert"):
            target_cust = customers_df[customers_df["customer_segment"] == "Regular"].iloc[0].to_dict()
            evt = generate_transaction_event(target_cust, is_fraud_high=True)
            st.toast(f"Injected High-Value Fraud: {evt['transaction_id']} (Rs.{evt['amount']:,.2f})", )
    with col_sim2:
        if st.button("Watchlist Fraud", use_container_width=True, help="Triggers fraud watchlist match alert"):
            target_cust = customers_df.sample(1).iloc[0].to_dict()
            evt = generate_transaction_event(target_cust, is_fraud_card=True)
            st.toast(f"Injected Watchlist Hit: Card 5008514036965665", )
            
    if st.button("Inject 5 Normal Txns", use_container_width=True):
        for _ in range(5):
            c = customers_df.sample(1).iloc[0].to_dict()
            generate_transaction_event(c)
        st.toast("Produced 5 transactions into stream")
        
    st.markdown("---")
    st.subheader("Cloud & Auth Credentials")
    with st.expander("Configure Free Tiers", expanded=False):
        neon_url = st.text_input("Neon Postgres URL", value=os.getenv("DATABASE_URL", ""), type="password", placeholder="postgresql://user:pass@ep-xyz.neon.tech/finguard")
        kafka_url = st.text_input("Kafka Bootstrap", value=os.getenv("BOOTSTRAP_SERVERS", ""), placeholder="confluent.cloud:9092")
        gmail_user = st.text_input("Gmail Address", value=os.getenv("EMAIL_FROM", "databeli14@gmail.com"))
        gmail_pass = st.text_input("Gmail App Password", value=os.getenv("GMAIL_APP_PASSWORD", ""), type="password", help="16-character Google App Password")
        if st.button("Test Email Dispatch"):
            if gmail_pass:
                ok, msg = send_real_alert_email(gmail_user, "FinGuard Test Alert", "<h3>FinGuard Alert System Active</h3><p>Real-time SMTP connection verified.</p>", gmail_user, gmail_pass)
                if ok:
                    st.success(msg)
                else:
                    st.error(f"Failed: {msg}")
            else:
                st.info("Simulated email dispatch: SMTP App Password not set (Demo mode active).")
                
    st.markdown("---")
    st.markdown("**Tip for Panels**: Show the live event ticker, inject fraud, and switch to the Medallion / Architecture tab to explain watermarks!")

# ------------------------------------------------------------------------------
# Main Dashboard Body
# ------------------------------------------------------------------------------
st.markdown('<div class="main-header">FinGuard Fraud Monitoring & Detection Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Real-Time Streaming Engine on Databricks Lakeflow, Apache Spark, Kafka & Delta Lake</div>', unsafe_allow_html=True)

# Top KPI Metric Counters
txns_df = pd.DataFrame(st.session_state.transactions)
alerts_df = pd.DataFrame(st.session_state.alerts)

total_txns = len(txns_df)
total_alerts = len(alerts_df)
total_amount = txns_df["amount"].sum() if not txns_df.empty else 0.0
avg_amount = txns_df["amount"].mean() if not txns_df.empty else 0.0
high_risk_custs = len(customers_df[customers_df["risk_score"] > 70])

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
with kpi1:
    st.metric("Total Stream Events", f"{total_txns:,}", delta="+ Live Feed")
with kpi2:
    st.metric("Total Fraud Alerts", f"{total_alerts:,}", delta="Immediate Flag", delta_color="inverse")
with kpi3:
    st.metric("Average Transaction", f"Rs.{avg_amount:,.2f}")
with kpi4:
    st.metric("Processed Volume", f"Rs.{total_amount:,.2f}")
with kpi5:
    st.metric("High-Risk Customers", f"{high_risk_custs:,}", help="Customers with Risk Score > 70 in Master DB")

st.markdown("---")

# Navigation Tabs
tab_live, tab_medallion, tab_charts, tab_architecture = st.tabs([
    "Real-Time Stream & Alerts",
    "Medallion Tables (Bronze/Silver/Gold)",
    "Operational Analytics & KPIs",
    "Architecture & Interview Q&A"
])

# ------------------------------------------------------------------------------
# TAB 1: Real-Time Stream & Alerts
# ------------------------------------------------------------------------------
with tab_live:
    col_alerts, col_stream = st.columns([1.1, 1.4])
    
    with col_alerts:
        st.subheader("Real-Time Fraud Alert Feed (Gold Sink)")
        if alerts_df.empty:
            st.info("No fraud alerts triggered yet. Click ' High-Value Fraud' or ' Watchlist Fraud' in the sidebar to simulate!")
        else:
            for _, alert in alerts_df.head(10).iterrows():
                is_high = alert["alert_type"] == "HIGH_VALUE_TRANSACTION"
                box_class = "alert-box-high" if is_high else "alert-box-card"
                badge = "LIMIT BREACH" if is_high else "WATCHLIST HIT"
                st.markdown(f"""
                <div class="{box_class}">
                    <div style="display:flex; justify-content:space-between;">
                        <strong>{badge}: {alert['alert_id']}</strong>
                        <span style="font-size:0.8rem; color:#64748B;">{alert['alert_timestamp']}</span>
                    </div>
                    <div style="margin-top:5px; font-size:0.95rem;">
                        <strong>Customer:</strong> {alert['customer_name']} ({alert['customer_id']})<br>
                        <strong>Amount:</strong> Rs.{alert['amount']:,.2f} | <strong>Threshold:</strong> Rs.{alert['limit']:,.2f}<br>
                        <strong>Merchant:</strong> {alert['merchant_name']} ({alert['location']})<br>
                        <span style="color:#B91C1C; font-size:0.85rem;"><strong>Rule Reason:</strong> {alert['reason']}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
    with col_stream:
        st.subheader("Live Ingestion Stream (Kafka to Spark)")
        if not txns_df.empty:
            display_cols = ["transaction_id", "amount", "merchant_name", "merchant_category", "payment_channel", "city", "status", "transaction_timestamp"]
            st.dataframe(
                txns_df[display_cols].head(12),
                use_container_width=True,
                height=420
            )
        else:
            st.info("Streaming queue empty.")

# ------------------------------------------------------------------------------
# TAB 2: Medallion Architecture Inspection
# ------------------------------------------------------------------------------
with tab_medallion:
    st.markdown("### Medallion Data Inspection (Unity Catalog: `finguard.*`)")
    
    med_choice = st.radio("Select Medallion Layer:", ["Bronze (Raw Ingestion)", "Silver (Cleaned & Standardized)", "Gold (Business & Alerts)"], horizontal=True)
    
    if "Bronze" in med_choice:
        st.markdown("<span class='badge-bronze'>BRONZE LAYER</span> `finguard.bronze.transactions` & `finguard.bronze.customers`", unsafe_allow_html=True)
        st.caption("Captures raw string JSON payload from Kafka with full message metadata, offset, and arrival timestamp.")
        bronze_view = txns_df[["key", "value", "topic", "partition", "offset", "timestamp", "ingestion_timestamp"]].head(10)
        st.dataframe(bronze_view, use_container_width=True)
        
    elif "Silver" in med_choice:
        st.markdown("<span class='badge-silver'>SILVER LAYER</span> `finguard.silver.transactions` (Parsed with Schema & Quality Expectations)", unsafe_allow_html=True)
        st.caption("Enforces schema typing, data quality expectations (@expect_or_drop), and standardizes timestamps.")
        silver_view = txns_df[["transaction_id", "customer_id", "card_number", "amount", "merchant_name", "merchant_category", "payment_channel", "city", "country", "is_international", "status"]].head(10)
        st.dataframe(silver_view, use_container_width=True)
        
        st.markdown("#### Applied Declarative Data Quality Expectations:")
        st.code("""
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number", "card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")  # Warn only
        """, language="python")
        
    else:
        st.markdown("<span class='badge-gold'>GOLD LAYER</span> Business-ready aggregations and alerts", unsafe_allow_html=True)
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            st.markdown("#### `finguard.gold.high_value_transactions_alert`")
            high_alerts = [a for a in st.session_state.alerts if a["alert_type"] == "HIGH_VALUE_TRANSACTION"]
            if high_alerts:
                st.dataframe(pd.DataFrame(high_alerts)[["alert_id", "customer_name", "amount", "limit", "merchant_name"]], use_container_width=True)
            else:
                st.info("No high value alerts.")
        with gcol2:
            st.markdown("#### `finguard.gold.fraud_card_alert`")
            card_alerts = [a for a in st.session_state.alerts if a["alert_type"] == "FRAUD_WATCHLIST_MATCH"]
            if card_alerts:
                st.dataframe(pd.DataFrame(card_alerts)[["alert_id", "customer_name", "amount", "merchant_name", "reason"]], use_container_width=True)
            else:
                st.info("No watchlist card alerts.")

# ------------------------------------------------------------------------------
# TAB 3: Operational Analytics & KPIs (Replicating Databricks Lakeview Dashboard)
# ------------------------------------------------------------------------------
with tab_charts:
    st.markdown("### Operational Lakeview Visualizations (1-Minute Refresh)")
    
    c1, c2 = st.columns(2)
    with c1:
        # Merchant category distribution
        cat_counts = txns_df["merchant_category"].value_counts().reset_index()
        cat_counts.columns = ["Category", "Count"]
        fig_cat = px.bar(cat_counts, x="Category", y="Count", title="Transactions by Merchant Category", color="Category", template="plotly_white")
        fig_cat.update_layout(showlegend=False, height=320)
        st.plotly_chart(fig_cat, use_container_width=True)
        
    with c2:
        # Payment channel distribution
        chan_counts = txns_df["payment_channel"].value_counts().reset_index()
        chan_counts.columns = ["Channel", "Count"]
        fig_chan = px.pie(chan_counts, names="Channel", values="Count", title="Payment Channel Distribution", hole=0.4, template="plotly_white")
        fig_chan.update_layout(height=320)
        st.plotly_chart(fig_chan, use_container_width=True)
        
    c3, c4 = st.columns(2)
    with c3:
        # International vs Domestic
        intl_counts = txns_df["is_international"].map({True: "International", False: "Domestic"}).value_counts().reset_index()
        intl_counts.columns = ["Type", "Count"]
        fig_intl = px.pie(intl_counts, names="Type", values="Count", title="Domestic vs. International Transactions", color_discrete_sequence=["#10B981", "#F59E0B"], template="plotly_white")
        fig_intl.update_layout(height=320)
        st.plotly_chart(fig_intl, use_container_width=True)
        
    with c4:
        # Top 10 Merchants by volume
        merc_counts = txns_df["merchant_name"].value_counts().head(8).reset_index()
        merc_counts.columns = ["Merchant", "Transactions"]
        fig_merc = px.bar(merc_counts, x="Transactions", y="Merchant", orientation="h", title="Top Merchants by Transaction Volume", color="Transactions", color_continuous_scale="Blues", template="plotly_white")
        fig_merc.update_layout(height=320)
        st.plotly_chart(fig_merc, use_container_width=True)

# ------------------------------------------------------------------------------
# TAB 4: Architecture & Interview Q&A
# ------------------------------------------------------------------------------
with tab_architecture:
    st.markdown("### Complete FinGuard Architecture & Technical Interview Talking Points")
    
    st.markdown("""
    ```mermaid
    flowchart LR
        subgraph Ingestion ["Source Ingestion"]
            K["Confluent / Upstash Kafka<br>(credit_card_transactions)"]
            W["Auto Loader<br>(JSON Watchlist)"]
            P["Neon PostgreSQL<br>(Customer Master CDC)"]
        end

        subgraph Bronze ["Bronze Layer (Raw)"]
            B1[("finguard.bronze.transactions")]
            B2[("finguard.bronze.fraud_watchlist")]
            B3[("finguard.bronze.customers")]
        end

        subgraph Silver ["Silver Layer (Cleaned & Validated)"]
            S1[("finguard.silver.transactions<br>DQ: expect_or_drop")]
            S2[("finguard.silver.fraud_watchlist")]
            S3[("finguard.silver.customers")]
        end

        subgraph Gold ["Gold Layer (Analytics & Fraud Sinks)"]
            G1["High-Value Alert<br>(Stream-Static Join)"]
            G2["Fraud Card Alert<br>(Stream-Stream Join + 5m Watermark)"]
            G3["1-Min Tumbling Window<br>5-Min Sliding Window"]
        end

        subgraph Sinks ["Alerting & Dashboards"]
            E["Gmail SMTP Notifier<br>(foreach_batch_sink)"]
            D["Lakeview Dashboard<br>(1-Min Near Real-Time)"]
        end

        K --> B1 --> S1
        W --> B2 --> S2
        P --> B3 --> S3
        S1 & S3 --> G1
        S1 & S2 & S3 --> G2
        S1 --> G3
        G1 & G2 --> E
        G1 & G2 & G3 --> D
    ```
    """)
    
    st.markdown("---")
    st.subheader("Key Interview Questions & Defenses for Candidates")
    
    with st.expander("Q1: Why Spark Structured Streaming over legacy Spark Streaming (DStreams)?"):
        st.markdown("""
        - **Event-Time Processing & Watermarking**: Structured Streaming evaluates timestamps based on when the transaction occurred at the terminal, not arrival time at the cluster.
        - **Unified API**: Ingestion uses standard DataFrames/SQL (`spark.readStream` instead of `spark.read`), simplifying maintenance.
        - **Native Delta Lake Integration**: Direct streaming reads/writes with ACID transactions and schema enforcement.
        """)
        
    with st.expander("Q2: Why did you set a 5-minute watermark on stream-stream joins?"):
        st.markdown("""
        - In a stream-stream join (transactions $\\leftrightarrow$ fraud watchlist), Spark must retain events in state memory waiting for potential matches.
        - Without a watermark, the state store expands infinitely leading to Out-Of-Memory (OOM) errors.
        - `transactions.withWatermark("transaction_timestamp", "5 minutes")` bounds the state: any watchlist entry arriving more than 5 minutes late relative to the latest event time is dropped from state memory.
        """)

    with st.expander("Q3: Explain the difference between your Stream-Static join and Stream-Stream join."):
        st.markdown("""
        - **Stream-Static Join** (`high_value_transactions_alert`): Silver transactions stream is joined with static customer master data (`customers`). Spark performs a left outer join against the current snapshot of customer limits. No watermarking is needed because static data does not require streaming state management.
        - **Stream-Stream Join** (`fraud_card_alert`): Joins streaming transactions with streaming watchlist files. Requires watermarks on both sides and an inner join on `card_number == entity_id`.
        """)

    with st.expander("Q4: How did you handle credential security and pipeline orchestration?"):
        st.markdown("""
        - **Databricks Secret Scopes**: Secured Kafka API keys and Gmail App Passwords inside `finguard-scope` (`dbutils.secrets.get()`), ensuring zero secrets in git code.
        - **Unity Catalog**: Role-based access control (RBAC), data lineage, and unified governance across all Bronze/Silver/Gold schemas.
        - **Lakeflow Spark Declarative Pipelines**: Automated pipeline orchestration with declarative quality rules (`@expect_or_drop`).
        """)

st.markdown("---")
st.caption("FinGuard Data Engineering Portfolio Project | Built with Apache Spark, Delta Lake, Kafka & Streamlit")
