#!/usr/bin/env python3
"""
fno_list.py – Extract F&O symbols from CSV, fetch Angel One tokens, store in MySQL.
"""

import os
import sys
import csv
import time
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import pyotp
from SmartApi import SmartConnect

# ============================================================
# Load credentials from .env
# ============================================================
load_dotenv()

# Angel One (exactly as in your .env)
API_KEY = os.getenv("ANGEL_API_KEY")
CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
PASSWORD = os.getenv("ANGEL_PASSWORD")
TOTP_SECRET = os.getenv("ANGEL_TOTP")          # note: variable name is ANGEL_TOTP

# MySQL
MYSQL_HOST = os.getenv("DB_HOST", "localhost")
MYSQL_PORT = int(os.getenv("DB_PORT", 3306))
MYSQL_USER = os.getenv("DB_USER", "root")
MYSQL_PASSWORD = os.getenv("DB_PASSWORD", "")
MYSQL_DATABASE = os.getenv("DB_NAME", "stock_db")

CSV_FILE = "fo_mktlots.csv"

# ============================================================
# Database helpers (MySQL)
# ============================================================
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            autocommit=False
        )
        return conn
    except Error as e:
        print(f"❌ MySQL connection error: {e}")
        sys.exit(1)

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS symbols (
            symbol VARCHAR(50) PRIMARY KEY,
            token VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ MySQL table 'symbols' ready")

def save_token(symbol, token):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO symbols (symbol, token) VALUES (%s, %s) ON DUPLICATE KEY UPDATE token = VALUES(token)",
        (symbol, token)
    )
    conn.commit()
    cursor.close()
    conn.close()

def get_cached_token(symbol):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT token FROM symbols WHERE symbol = %s", (symbol,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None

# ============================================================
# Angel One API
# ============================================================
def login():
    try:
        totp = pyotp.TOTP(TOTP_SECRET).now()
        obj = SmartConnect(api_key=API_KEY)
        resp = obj.generateSession(CLIENT_ID, PASSWORD, totp)
        if resp.get('status'):
            print("✅ Login successful")
            return obj
        else:
            print(f"❌ Login failed: {resp.get('message', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"❌ Login exception: {e}")
        return None

def get_token(obj, symbol):
    try:
        resp = obj.searchScrip("NSE", symbol)
        if resp and resp.get('data'):
            data = resp['data']
            best_token = None
            for item in data:
                trading_symbol = item.get('tradingsymbol', '')
                token = item.get('symboltoken')
                if trading_symbol == symbol.upper():
                    return token
                if trading_symbol == f"{symbol.upper()}-EQ":
                    return token
                if best_token is None and trading_symbol.endswith('-EQ'):
                    best_token = token
            if best_token:
                return best_token
            if data:
                return data[0].get('symboltoken')
        return None
    except Exception as e:
        print(f"   ⚠️ Error searching {symbol}: {e}")
        return None

# ============================================================
# CSV parsing
# ============================================================
def read_symbols_from_csv():
    symbols = []
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            for row in reader:
                if len(row) >= 2:
                    sym = row[1].strip()
                    if sym and not sym.startswith('Derivatives'):
                        symbols.append(sym)
    except FileNotFoundError:
        print(f"❌ CSV file '{CSV_FILE}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        sys.exit(1)

    # Remove duplicates preserving order
    seen = set()
    unique = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    return unique

# ============================================================
# Main
# ============================================================
def main():
    print("\n" + "="*60)
    print("F&O SYMBOL TOKEN EXTRACTOR (MySQL)")
    print("="*60)

    symbols = read_symbols_from_csv()
    if not symbols:
        print("No symbols found. Exiting.")
        return
    print(f"📊 Found {len(symbols)} unique symbols.")

    init_db()

    obj = login()
    if not obj:
        return

    print("\n🔍 Fetching tokens...\n")
    new_tokens = 0
    cached_used = 0
    failed = []

    for i, sym in enumerate(symbols, 1):
        print(f"[{i:3d}/{len(symbols)}] {sym}")

        token = get_cached_token(sym)
        if token:
            print(f"   🔑 Using cached token: {token}")
            cached_used += 1
            continue

        token = get_token(obj, sym)
        if token:
            save_token(sym, token)
            print(f"   ✅ New token: {token}")
            new_tokens += 1
        else:
            print(f"   ❌ No token found")
            failed.append(sym)

        time.sleep(1.0)

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total symbols:        {len(symbols)}")
    print(f"Used cached tokens:   {cached_used}")
    print(f"New tokens fetched:   {new_tokens}")
    print(f"Failed (no token):    {len(failed)}")
    if failed:
        print("\nFailed symbols:")
        for f in failed[:20]:
            print(f"  - {f}")
        if len(failed) > 20:
            print(f"  ... and {len(failed)-20} more")

if __name__ == "__main__":
    main()