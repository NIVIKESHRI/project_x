#!/usr/bin/env python3
"""
fetch_historical.py – Fetch historical OHLCV data from Angel One SmartAPI.
Stores data in MySQL with support for multiple timeframes.
"""

import os
import sys
import time
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import pyotp
from SmartApi import SmartConnect

# ============================================================
# Load configuration from .env
# ============================================================
load_dotenv()

# Angel One
API_KEY = os.getenv("ANGEL_API_KEY")
CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
PASSWORD = os.getenv("ANGEL_PASSWORD")
TOTP_SECRET = os.getenv("ANGEL_TOTP")

# MySQL
MYSQL_HOST = os.getenv("DB_HOST", "localhost")
MYSQL_PORT = int(os.getenv("DB_PORT", 3306))
MYSQL_USER = os.getenv("DB_USER", "root")
MYSQL_PASSWORD = os.getenv("DB_PASSWORD", "")
MYSQL_DATABASE = os.getenv("DB_NAME", "stock_db")

# ============================================================
# Mapping from user‑friendly interval to Angel One interval & max days
# ============================================================
INTERVAL_MAP = {
    "1min":   ("ONE_MINUTE", 30),
    "5min":   ("FIVE_MINUTE", 100),
    "10min":  ("TEN_MINUTE", 100),
    "15min":  ("FIFTEEN_MINUTE", 200),
    "30min":  ("THIRTY_MINUTE", 200),
    "1hour":  ("ONE_HOUR", 400),
    "1day":   ("ONE_DAY", 2000),
    "1week":  ("ONE_DAY", 2000),      # week will be handled by fetching daily and aggregating later if needed
    "1month": ("ONE_DAY", 2000)
}

# ============================================================
# Database helpers
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

def init_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            `interval` VARCHAR(20) NOT NULL,
            `datetime` DATETIME NOT NULL,
            open DECIMAL(12,2),
            high DECIMAL(12,2),
            low DECIMAL(12,2),
            close DECIMAL(12,2),
            volume BIGINT,
            UNIQUE KEY unique_record (symbol, `interval`, `datetime`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fetch_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            `interval` VARCHAR(20) NOT NULL,
            start_date DATE,
            end_date DATE,
            status VARCHAR(20),
            records_fetched INT,
            error_message TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Database tables ready")

def save_price_data(symbol, interval, data):
    """Insert or update price data."""
    if not data:
        return 0
    conn = get_db_connection()
    cursor = conn.cursor()
    count = 0
    for row in data:
        try:
            cursor.execute("""
                INSERT INTO price_data (symbol, `interval`, `datetime`, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    high = VALUES(high),
                    low = VALUES(low),
                    close = VALUES(close),
                    volume = VALUES(volume)
            """, (symbol, interval, row['datetime'], row['open'], row['high'], row['low'], row['close'], row['volume']))
            count += 1
        except Exception as e:
            print(f"   ⚠️ Insert error for {symbol} at {row['datetime']}: {e}")
    conn.commit()
    cursor.close()
    conn.close()
    return count

def log_fetch(symbol, interval, start_date, end_date, status, records, error=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO fetch_logs (symbol, `interval`, start_date, end_date, status, records_fetched, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (symbol, interval, start_date, end_date, status, records, error))
    conn.commit()
    cursor.close()
    conn.close()

def get_all_symbols():
    """Return list of all symbols from the symbols table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM symbols ORDER BY symbol")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [row[0] for row in rows]

def get_symbol_token(symbol):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT token FROM symbols WHERE symbol = %s", (symbol,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None

# ============================================================
# Angel One API helpers
# ============================================================
def login():
    try:
        totp = pyotp.TOTP(TOTP_SECRET).now()
        obj = SmartConnect(api_key=API_KEY)
        resp = obj.generateSession(CLIENT_ID, PASSWORD, totp)
        if resp.get('status'):
            print("✅ Angel One login successful")
            return obj
        else:
            print(f"❌ Login failed: {resp.get('message', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"❌ Login exception: {e}")
        return None

def fetch_candles(obj, token, symbol, interval, from_date, to_date):
    params = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": interval,
        "fromdate": from_date.strftime("%Y-%m-%d 09:15"),
        "todate": to_date.strftime("%Y-%m-%d 15:30")
    }
    try:
        resp = obj.getCandleData(params)
        if resp and resp.get('status') and resp.get('data'):
            data = []
            for candle in resp['data']:
                # candle[0] is date string in format 'YYYY-MM-DDTHH:MM:SS+05:30'
                date_str = candle[0]
                # Remove timezone offset if present (keep only the part before '+')
                if '+' in date_str:
                    date_str = date_str.split('+')[0]
                dt = datetime.fromisoformat(date_str)
                data.append({
                    'datetime': dt,
                    'open': float(candle[1]),
                    'high': float(candle[2]),
                    'low': float(candle[3]),
                    'close': float(candle[4]),
                    'volume': int(candle[5])
                })
            return data
        else:
            print(f"   ⚠️ No data for {symbol} from {from_date.date()} to {to_date.date()}")
            return []
    except Exception as e:
        print(f"   ❌ API error for {symbol}: {e}")
        return []

def fetch_historical_chunked(obj, token, symbol, interval, start_date, end_date, max_days_per_chunk):
    """
    Split date range into chunks and fetch all candles.
    Returns combined list of candles.
    """
    all_data = []
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=max_days_per_chunk), end_date)
        print(f"   Fetching chunk: {current_start.date()} to {current_end.date()}")
        chunk = fetch_candles(obj, token, symbol, interval, current_start, current_end)
        if chunk:
            all_data.extend(chunk)
        else:
            print(f"   ⚠️ Empty chunk, stopping early.")
            break
        current_start = current_end
        time.sleep(0.5)   # be kind to API
    return all_data

# ============================================================
# User interaction
# ============================================================
def get_date_range():
    print("\nDate range options:")
    print("1. Last N days (from today)")
    print("2. Specific start and end date")
    choice = input("Choose (1/2): ").strip()
    if choice == '1':
        days = int(input("Number of days back: "))
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
    else:
        start_str = input("Start date (YYYY-MM-DD): ")
        end_str = input("End date (YYYY-MM-DD): ")
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
    return start_date, end_date

def select_symbols(all_symbols):
    print("\nSymbol selection:")
    print("1. All symbols")
    print("2. Specific symbol(s) (comma separated)")
    choice = input("Choose (1/2): ").strip()
    if choice == '1':
        return all_symbols
    else:
        symbols_input = input("Enter symbol names (comma separated, e.g. RELIANCE,TCS): ").strip().upper()
        selected = [s.strip() for s in symbols_input.split(',')]
        # validate against existing symbols
        valid = [s for s in selected if s in all_symbols]
        invalid = [s for s in selected if s not in all_symbols]
        if invalid:
            print(f"⚠️ Unknown symbols ignored: {invalid}")
        if not valid:
            print("❌ No valid symbols selected. Exiting.")
            sys.exit(1)
        return valid

# ============================================================
# Main
# ============================================================
def main():
    print("\n" + "="*60)
    print("HISTORICAL OHLCV DATA FETCHER")
    print("="*60)

    # 1. Database setup
    init_tables()

    # 2. Get all symbols and tokens from DB
    all_symbols = get_all_symbols()
    if not all_symbols:
        print("❌ No symbols found in database. Run fno_list.py first.")
        return
    print(f"📊 Found {len(all_symbols)} symbols in database.")

    # 3. Ask for symbols
    symbols_to_fetch = select_symbols(all_symbols)

    # 4. Ask for timeframe
    print("\nAvailable intervals:")
    for key in INTERVAL_MAP.keys():
        print(f"  - {key}")
    interval_key = input("Enter interval (e.g., 1day, 1hour, 5min): ").strip().lower()
    if interval_key not in INTERVAL_MAP:
        print(f"❌ Invalid interval. Choose from: {list(INTERVAL_MAP.keys())}")
        return
    angel_interval, max_days = INTERVAL_MAP[interval_key]

    # 5. Ask for date range
    start_date, end_date = get_date_range()
    if start_date >= end_date:
        print("❌ Start date must be before end date.")
        return

    # 6. Login to Angel One
    obj = login()
    if not obj:
        return

    # 7. Fetch data for each symbol
    print(f"\n🚀 Fetching {interval_key} data from {start_date.date()} to {end_date.date()}\n")
    total_records = 0
    for sym in symbols_to_fetch:
        print(f"\n📈 Processing {sym}...")
        token = get_symbol_token(sym)
        if not token:
            print(f"   ❌ No token for {sym}. Skipping.")
            log_fetch(sym, interval_key, start_date.date(), end_date.date(), "FAILED", 0, "Token missing")
            continue

        data = fetch_historical_chunked(obj, token, sym, angel_interval, start_date, end_date, max_days)
        if data:
            saved = save_price_data(sym, interval_key, data)
            print(f"   ✅ Saved {saved} records")
            total_records += saved
            log_fetch(sym, interval_key, start_date.date(), end_date.date(), "SUCCESS", saved)
        else:
            print(f"   ⚠️ No data fetched")
            log_fetch(sym, interval_key, start_date.date(), end_date.date(), "FAILED", 0, "No data from API")

        time.sleep(1)   # rate limit between symbol

    print(f"\n✅ Done! Total records saved: {total_records}")

if __name__ == "__main__":
    main()