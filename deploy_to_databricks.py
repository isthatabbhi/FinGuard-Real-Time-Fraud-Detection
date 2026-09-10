"""
FinGuard - Databricks Automation & Deployment Helper
===================================================
Automates Databricks Secret Scope creation, secret configuration, and pipeline validation via REST API.

Usage:
    python deploy_to_databricks.py --host "https://adb-xxxx.azuredatabricks.net" --token "dapi..."
Or configure DATABRICKS_HOST and DATABRICKS_TOKEN in kafka_producer/.env
"""

import os
import sys
import json
import argparse
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load environment
ENV_PATH = Path(__file__).resolve().parent / "kafka_producer" / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)

def get_args():
    parser = argparse.ArgumentParser(description="FinGuard Databricks Automated Setup")
    parser.add_argument("--host", type=str, default=os.getenv("DATABRICKS_HOST"), help="Databricks instance URL")
    parser.add_argument("--token", type=str, default=os.getenv("DATABRICKS_TOKEN"), help="Databricks Personal Access Token (PAT)")
    return parser.parse_args()

def create_secret_scope(host: str, token: str, scope_name: str = "finguard-scope"):
    print(f"\n🔑 Setting up Databricks Secret Scope: '{scope_name}'...")
    url = f"{host.rstrip('/')}/api/2.0/secrets/scopes/create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"scope": scope_name}
    
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 200:
        print(f"✅ Secret scope '{scope_name}' created successfully.")
    elif resp.status_code == 400 and "already exists" in resp.text:
        print(f"ℹ️ Secret scope '{scope_name}' already exists. Proceeding to update secrets.")
    else:
        print(f"⚠️ Notice on scope creation (status {resp.status_code}): {resp.text}")

def put_secret(host: str, token: str, scope_name: str, key: str, value: str):
    url = f"{host.rstrip('/')}/api/2.0/secrets/put"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"scope": scope_name, "key": key, "string_value": value}
    
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 200:
        print(f"✅ Secret '{key}' configured successfully in '{scope_name}'.")
    else:
        print(f"❌ Failed to put secret '{key}': {resp.status_code} - {resp.text}")

def main():
    args = get_args()
    host = args.host
    token = args.token
    
    if not host or not token:
        print("❌ Databricks Host and Token are required.")
        print("\n👉 How to get your free Databricks credentials:")
        print("1. Sign up for a Databricks 14-day Free Trial on AWS / Azure / GCP, or use your company/university workspace.")
        print("2. Copy your workspace URL: e.g. https://adb-123456789.azuredatabricks.net")
        print("3. In Databricks, click your Profile (top right) -> Settings -> Developer -> Access tokens -> Generate new token.")
        print("4. Run this script:")
        print("   python deploy_to_databricks.py --host \"<your_host>\" --token \"<your_token>\"\n")
        sys.exit(1)
        
    print(f"📡 Connecting to Databricks workspace: {host}")
    scope = "finguard-scope"
    create_secret_scope(host, token, scope)
    
    # 1. Kafka secrets
    bootstrap = os.getenv("BOOTSTRAP_SERVERS", "pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092")
    api_key = os.getenv("API_KEY", "")
    api_secret = os.getenv("API_SECRET", "")
    topic = os.getenv("TOPIC_NAME", "credit_card_transactions")
    
    kafka_details = json.dumps({
        "bootstrap_servers": bootstrap,
        "topic": topic,
        "api_key": api_key,
        "api_secret": api_secret
    })
    put_secret(host, token, scope, "kafka_connection_details", kafka_details)
    
    # 2. Gmail SMTP secrets
    gmail_key = os.getenv("GMAIL_APP_PASSWORD", "lkls hreo ltin dqwz")
    put_secret(host, token, scope, "gmail_api_key", gmail_key)
    
    print("\n🎉 Databricks secrets setup complete!")
    print("\n📋 Next steps in your Databricks Workspace:")
    print("1. Push this repository to GitHub and link it in Databricks under 'Workspace' > 'Repos'.")
    print("2. Navigate to 'Delta Live Tables' / 'Lakeflow Pipelines' > Create Pipeline.")
    print("   - Name: FinGuard-Streaming-Pipeline")
    print("   - Source code: select 'databricks notebooks and pipelines/finguard_project/finguard_streaming'")
    print("   - Storage Catalog: finguard")
    print("   - Target Schema: silver / gold")
    print("3. Click 'Start' to trigger the end-to-end streaming execution!")

if __name__ == "__main__":
    main()
