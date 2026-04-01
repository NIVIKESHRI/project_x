#!/usr/bin/env python3
"""
nifty_derivatives.py – Fetch Nifty futures & options from Angel One master file and store in MySQL.
"""

import os
import json
import requests
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# MySQL config
MYSQL_HOST = os.getenv("DB_HOST", "localhost")
MYSQL_PORT = int(os.getenv("DB_PORT", 3306))
MYSQL_USER = os.getenv("DB_USER", "root")
MYSQL_PASSWORD = os.getenv("DB_PASSWORD", "")
MYSQL_DATABASE = os.getenv("DB_NAME", "stock_db")

MASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"

def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        autocommit=False
    )

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS derivative_symbols (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            token INT NOT NULL UNIQUE,
            exchange VARCHAR(10) DEFAULT 'NFO',
            instrument_type ENUM('FUT', 'CE', 'PE') NOT NULL,
            expiry DATE,
            strike DECIMAL(12,2),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    conn.commit()
    cursor.close()
    conn.close()

def parse_expiry(expiry_str):
    """Convert expiry string like '24APR2025' to date."""
    try:
        return datetime.strptime(expiry_str, "%d%b%Y").date()
    except:
        return None

def fetch_and_store():
    print("Downloading master file...")
    resp = requests.get(MASTER_URL)
    resp.raise_for_status()
    data = resp.json()

    conn = get_db_connection()
    cursor = conn.cursor()

    # Mark all existing as inactive; we'll reactivate the ones we find
    cursor.execute("UPDATE derivative_symbols SET is_active = FALSE")
    conn.commit()

    inserted = 0
    for item in data:
        # Filter for NFO segment (derivatives)
        if item.get('exch_seg') != 'NFO':
            continue
        # Filter for instrument type: FUTIDX or OPTIDX (or any containing FUT/OPT)
        inst_type_raw = item.get('instrumenttype', '')
        if 'FUT' in inst_type_raw:
            inst_type = 'FUT'
        elif 'OPT' in inst_type_raw:
            # Determine if CE or PE from symbol or strike sign
            # Option symbol often includes 'CE' or 'PE'
            symbol = item.get('symbol', '')
            if 'CE' in symbol:
                inst_type = 'CE'
            elif 'PE' in symbol:
                inst_type = 'PE'
            else:
                continue
        else:
            continue

        # Only Nifty derivatives
        symbol = item.get('symbol', '')
        if not symbol.startswith('NIFTY'):
            continue

        # Extract expiry
        expiry_str = item.get('expiry', '')
        expiry = parse_expiry(expiry_str) if expiry_str else None

        # Strike for options
        strike = None
        if inst_type in ('CE', 'PE'):
            strike = item.get('strike')

        token = item.get('token')
        if not token:
            continue

        try:
            cursor.execute("""
                INSERT INTO derivative_symbols (symbol, token, exchange, instrument_type, expiry, strike, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                ON DUPLICATE KEY UPDATE
                    expiry = VALUES(expiry),
                    strike = VALUES(strike),
                    is_active = TRUE
            """, (symbol, token, 'NFO', inst_type, expiry, strike))
            inserted += 1
        except Exception as e:
            print(f"Error inserting {symbol}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Stored {inserted} active Nifty derivative contracts")

if __name__ == "__main__":
    create_tables()
    fetch_and_store()