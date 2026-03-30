#!/usr/bin/env python3
"""
fno_list.py – Extract F&O symbols from CSV, fetch Angel One tokens (any token), store in SQLite.
"""

import os
import sys
import csv
import time
import sqlite3
from dotenv import load_dotenv
import pyotp
from SmartApi import SmartConnect

# Load credentials from .env file
load_dotenv()

API_KEY = os.getenv("ANGEL_API_KEY")
CLIENT_CODE = os.getenv("ANGEL_CLIENT_ID")
PASSWORD = os.getenv("ANGEL_PASSWORD")
TOTP_SECRET = os.getenv("ANGEL_TOTP")

DB_FILE = "fno_symbols.db"
CSV_FILE = "fo_mktlots.csv"

# ------------------------------------------------------------
# Database helpers
# ------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS symbols (
            symbol TEXT PRIMARY KEY,
            token TEXT
        )
    """)
    conn.commit()
    return conn

def save_token(conn, symbol, token):
    conn.execute("INSERT OR REPLACE INTO symbols (symbol, token) VALUES (?, ?)",
                 (symbol, token))
    conn.commit()

def get_cached_token(conn, symbol):
    cur = conn.execute("SELECT token FROM symbols WHERE symbol = ?", (symbol,))
    row = cur.fetchone()
    return row[0] if row else None

# ------------------------------------------------------------
# Angel One login and token fetch
# ------------------------------------------------------------
def login():
    try:
        totp = pyotp.TOTP(TOTP_SECRET).now()
        obj = SmartConnect(api_key=API_KEY)
        resp = obj.generateSession(CLIENT_CODE, PASSWORD, totp)
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
    """
    Search for the symbol and return the correct token.
    Checks for exact match OR `-EQ` suffix.
    """
    try:
        resp = obj.searchScrip("NSE", symbol)
        if resp and resp.get('data'):
            data = resp['data']
            
            # The API returns keys: 'tradingsymbol' and 'symboltoken'
            # First, try to find an exact match (for indices) or -EQ (for stocks)
            best_token = None
            for item in data:
                trading_symbol = item.get('tradingsymbol', '')
                
                # Check for exact match (e.g. NIFTY) or standard EQ match (e.g. HCLTECH-EQ)
                if trading_symbol == symbol.upper() or trading_symbol == f"{symbol.upper()}-EQ":
                    return item.get('symboltoken')
                
                # If neither exact nor -EQ, save the first one ending in -EQ as a fallback
                if best_token is None and trading_symbol.endswith('-EQ'):
                    best_token = item.get('symboltoken')
            
            if best_token:
                return best_token
            
            # If all else fails, return the first token from the list
            if data:
                return data[0].get('symboltoken')
        return None
    except Exception as e:
        print(f"   ⚠️ Error searching {symbol}: {e}")
        return None

# ------------------------------------------------------------
# CSV parsing
# ------------------------------------------------------------
def read_symbols_from_csv():
    """
    Read the CSV file and extract unique SYMBOL values.
    The CSV has a header row; the second column is SYMBOL.
    """
    symbols = []
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                print("❌ CSV file is empty")
                return []
            for row in reader:
                if len(row) >= 2:
                    sym = row[1].strip()
                    if sym and sym not in ("SYMBOL", "Symbol", "symbol") and not sym.startswith('Derivatives'):
                        symbols.append(sym)
    except FileNotFoundError:
        print(f"❌ CSV file '{CSV_FILE}' not found. Please place it in the same directory.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        sys.exit(1)

    # Remove duplicates while preserving order
    seen = set()
    unique_symbols = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            unique_symbols.append(s)
    return unique_symbols

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    print("\n" + "="*60)
    print("F&O SYMBOL LIST EXTRACTOR (No -EQ requirement)")
    print("="*60)

    # 1. Read symbols from CSV
    symbols = read_symbols_from_csv()
    if not symbols:
        print("No symbols found in CSV. Exiting.")
        return
    print(f"📊 Found {len(symbols)} unique symbols in CSV.")

    # 2. Connect to DB
    conn = init_db()
    print(f"📁 Database ready: {DB_FILE}")

    # 3. Login to Angel One
    obj = login()
    if not obj:
        print("Cannot proceed without login. Exiting.")
        conn.close()
        return

    # 4. Process each symbol
    print("\n🔍 Fetching tokens...")
    new_tokens = 0
    cached_used = 0
    failed = []

    for i, sym in enumerate(symbols, 1):
        print(f"[{i:3d}/{len(symbols)}] {sym}")

        # Check cache first
        token = get_cached_token(conn, sym)
        if token:
            print(f"   🔑 Using cached token: {token}")
            cached_used += 1
            continue

        # Fetch fresh token (any token, not necessarily -EQ)
        token = get_token(obj, sym)
        if token:
            save_token(conn, sym, token)
            print(f"   ✅ New token: {token}")
            new_tokens += 1
        else:
            print(f"   ❌ No token found")
            failed.append(sym)

        time.sleep(1.0)   # be gentle with API (rate limit is strict so we need 1s)

    # 5. Summary
    conn.close()
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
    print(f"\nDatabase saved to: {DB_FILE}")

if __name__ == "__main__":
    main()