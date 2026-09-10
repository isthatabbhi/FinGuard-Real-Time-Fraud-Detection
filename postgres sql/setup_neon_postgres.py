"""
FinGuard - Neon PostgreSQL Setup and Data Ingestion Script
===========================================================
This script initializes the customer master data in your free Neon.tech PostgreSQL database.

Usage:
    python setup_neon_postgres.py --connection-string "postgresql://user:password@ep-xyz.neon.tech/finguard?sslmode=require"
Or set DATABASE_URL in your .env file.
"""

import os
import sys
import argparse
import psycopg2
from pathlib import Path

def get_connection_string(cli_arg: str = None) -> str:
    if cli_arg:
        return cli_arg
    
    # Try .env
    env_file = Path(__file__).resolve().parent.parent / "kafka_producer" / ".env"
    if env_file.exists():
        with open(env_file, "r") as f:
            for line in f:
                if line.startswith("DATABASE_URL="):
                    return line.strip().split("=", 1)[1].strip('"').strip("'")
                if line.startswith("POSTGRES_URL="):
                    return line.strip().split("=", 1)[1].strip('"').strip("'")
                    
    db_url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
    if db_url:
        return db_url
        
    print("❌ No PostgreSQL connection string provided.")
    print("\n👉 How to get your FREE Neon PostgreSQL Database in 2 minutes:")
    print("1. Go to https://neon.tech and sign up for free (No credit card needed).")
    print("2. Create a new project named 'finguard'.")
    print("3. Copy the connection string displayed on the Neon dashboard.")
    print("4. Re-run this script with:")
    print("   python setup_neon_postgres.py --connection-string \"<your_connection_string>\"\n")
    return None

def run_migration(connection_string: str):
    sql_file = Path(__file__).resolve().parent / "customers_historic.sql"
    if not sql_file.exists():
        print(f"❌ Could not find SQL file at {sql_file}")
        sys.exit(1)
        
    print(f"📡 Connecting to Neon PostgreSQL...")
    try:
        conn = psycopg2.connect(connection_string)
        conn.autocommit = False
        cursor = conn.cursor()
        print(" Connected successfully to database!")
        
        print(f"📂 Reading SQL statements from {sql_file.name}...")
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()
            
        print(" Executing customer table creation and data ingestion...")
        cursor.execute(sql_content)
        conn.commit()
        print(" Data ingestion committed successfully!")
        
        # Verify
        cursor.execute("SELECT count(*) FROM customers;")
        count = cursor.fetchone()[0]
        print(f"🎉 Verification passed: {count} customer records now available in 'customers' table!")
        
        # Sample preview
        cursor.execute("SELECT customer_id, first_name, last_name, email, transaction_limit FROM customers LIMIT 3;")
        rows = cursor.fetchall()
        print("\nSample Data:")
        for r in rows:
            print(f" - {r[0]}: {r[1]} {r[2]} | Email: {r[3]} | Limit: ₹{r[4]:,.2f}")
            
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error during database execution: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="FinGuard Neon PostgreSQL Migration Tool")
    parser.add_argument("--connection-string", type=str, help="PostgreSQL connection string")
    args = parser.parse_args()
    
    conn_str = get_connection_string(args.connection_string)
    if not conn_str:
        try:
            val = input("Paste your Neon Connection String here (or press Enter to exit): ").strip()
            if val:
                conn_str = val
            else:
                sys.exit(1)
        except EOFError:
            sys.exit(1)
            
    run_migration(conn_str)

if __name__ == "__main__":
    main()
