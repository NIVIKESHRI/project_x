#!/usr/bin/env python3
"""
nifty_realtime.py – Real‑time tick collector for Nifty derivatives.
Fetches current Nifty price via Angel One LTP API to dynamically set strike range.
"""

import os
import sys
import time
import json
import mysql.connector
from datetime import datetime, time as dt_time
from dotenv import load_dotenv
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

load_dotenv()

# ============================================================
# Configuration
# ============================================================
API_KEY = os.getenv("ANGEL_API_KEY")
CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
PASSWORD = os.getenv("ANGEL_PASSWORD")
TOTP_SECRET = os.getenv("ANGEL_TOTP")

MYSQL_HOST = os.getenv("DB_HOST", "localhost")
MYSQL_PORT = int(os.getenv("DB_PORT", 3306))
MYSQL_USER = os.getenv("DB_USER", "root")
MYSQL_PASSWORD = os.getenv("DB_PASSWORD", "")
MYSQL_DATABASE = os.getenv("DB_NAME", "stock_db")

MARKET_START = dt_time(9, 15)
MARKET_END = dt_time(15, 30)

SUBSCRIPTION_MODE = 3   # SnapQuote (full data)
BATCH_SIZE = 100

batch = []               # list of tuples for insertion
token_to_symbol = {}

# ============================================================
# Database helpers
# ============================================================
def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        autocommit=False
    )

def get_active_tokens(nifty_price):
    """
    Fetch a limited set of Nifty derivative tokens.
    Uses the current Nifty price to select options within a ±500 strike range.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    strike_range = 500   # width around current Nifty
    expiry_window = 30   # days

    query = f"""
        SELECT token, symbol
        FROM derivative_symbols
        WHERE is_active = TRUE
          AND (
              -- Futures: only the nearest expiry
              (instrument_type = 'FUT' AND expiry = (SELECT MIN(expiry) FROM derivative_symbols WHERE instrument_type = 'FUT' AND expiry >= CURDATE()))
              OR
              -- Options: expiry within next {expiry_window} days, strike within range
              (instrument_type IN ('CE','PE')
               AND expiry BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL {expiry_window} DAY)
               AND strike BETWEEN {nifty_price - strike_range} AND {nifty_price + strike_range})
          )
        LIMIT 800   # safety limit
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {int(row[0]): row[1] for row in rows}

def flush_batch():
    global batch
    if not batch:
        return
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany("""
            INSERT INTO derivative_ticks (
                symbol, token, exchange_timestamp,
                last_traded_price, last_traded_quantity, average_traded_price,
                volume_trade_for_the_day, total_buy_quantity, total_sell_quantity,
                open_price_of_the_day, high_price_of_the_day, low_price_of_the_day, closed_price,
                upper_circuit_limit, lower_circuit_limit,
                open_interest, open_interest_change_percentage,
                best_5_buy_data, best_5_sell_data
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        conn.commit()
        print(f"✅ Inserted {len(batch)} ticks")
    except Exception as e:
        print(f"⚠️ Batch insert error: {e}")
    finally:
        cursor.close()
        conn.close()
        batch = []

def add_to_batch(tick):
    global batch
    batch.append(tick)
    if len(batch) >= BATCH_SIZE:
        flush_batch()

# ============================================================
# Angel One helpers
# ============================================================
def login():
    obj = SmartConnect(api_key=API_KEY)
    totp = pyotp.TOTP(TOTP_SECRET).now()
    resp = obj.generateSession(CLIENT_ID, PASSWORD, totp)
    if resp.get('status'):
        print("✅ Angel One login successful")
        feed_token = obj.getfeedToken()
        return obj, feed_token
    else:
        print(f"❌ Login failed: {resp}")
        return None, None

def get_current_nifty_price(obj):
    """Fetch the last traded price of Nifty 50 index using Angel One's LTP API."""
    try:
        # Nifty index token (from the master file)
        nifty_token = "99926000"
        response = obj.ltpData("NSE", nifty_token, "NIFTY")
        if response and response.get('status'):
            return float(response['data']['ltp'])
        else:
            print(f"⚠️ Could not fetch Nifty price: {response}")
            return None
    except Exception as e:
        print(f"⚠️ Error fetching Nifty price: {e}")
        return None

# ============================================================
# WebSocket callbacks
# ============================================================
def on_data(wsapp, message):
    try:
        data = json.loads(message)
        token = data.get('token')
        symbol = token_to_symbol.get(token)
        if not symbol:
            return

        # Convert timestamp (milliseconds) to datetime
        ts = data.get('exchange_timestamp')
        if ts:
            dt = datetime.fromtimestamp(ts / 1000.0)
        else:
            dt = datetime.now()

        tick = (
            symbol,
            token,
            dt,
            data.get('last_traded_price'),
            data.get('last_traded_quantity'),
            data.get('average_traded_price'),
            data.get('volume_trade_for_the_day'),
            data.get('total_buy_quantity'),
            data.get('total_sell_quantity'),
            data.get('open_price_of_the_day'),
            data.get('high_price_of_the_day'),
            data.get('low_price_of_the_day'),
            data.get('closed_price'),
            data.get('upper_circuit_limit'),
            data.get('lower_circuit_limit'),
            data.get('open_interest'),
            data.get('open_interest_change_percentage'),
            json.dumps(data.get('best_5_buy_data', [])),
            json.dumps(data.get('best_5_sell_data', []))
        )
        add_to_batch(tick)
    except Exception as e:
        print(f"Error processing tick: {e}")

def on_open(wsapp):
    print("WebSocket opened")

def on_error(wsapp, error):
    print(f"WebSocket error: {error}")

def on_close(wsapp):
    print("WebSocket closed")

# ============================================================
# Market hours check
# ============================================================
def is_market_hours():
    now = datetime.now().time()
    return MARKET_START <= now <= MARKET_END

# ============================================================
# Main
# ============================================================
def main():
    print("\n" + "="*60)
    print("📈 NIFTY DERIVATIVES REAL‑TIME TICK COLLECTOR")
    print("="*60)

    # 1. Login to Angel One
    obj, feed_token = login()
    if not obj:
        return

    # 2. Fetch current Nifty price to determine strike range
    nifty_price = get_current_nifty_price(obj)
    if nifty_price is None:
        print("❌ Could not fetch Nifty price. Using default 18000.")
        nifty_price = 18000  # fallback
    else:
        print(f"📊 Current Nifty price: {nifty_price}")

    # 3. Load filtered tokens from database
    global token_to_symbol
    token_to_symbol = get_active_tokens(nifty_price)
    if not token_to_symbol:
        print("No active tokens found. Check derivative_symbols or adjust filter.")
        return
    print(f"📊 Loaded {len(token_to_symbol)} derivative contracts (filtered)")

    # 4. Prepare subscription list (token, exchange, mode)
    subscription_list = [(token, "NFO", SUBSCRIPTION_MODE) for token in token_to_symbol.keys()]

    # 5. Create WebSocket connection
    ws = SmartWebSocketV2(
        api_key=API_KEY,
        client_code=CLIENT_ID,
        feed_token=feed_token,
        auth_token=obj.auth_token
    )
    ws.on_open = on_open
    ws.on_data = on_data
    ws.on_error = on_error
    ws.on_close = on_close

    ws.connect()
    ws.subscribe(subscription_list)
    print("✅ Subscribed to Nifty derivatives")

    print(f"🕒 Market hours: {MARKET_START} – {MARKET_END}")
    print("Collecting ticks... Press Ctrl+C to stop.\n")

    try:
        while True:
            if not is_market_hours():
                print("Market closed. Disconnecting.")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        flush_batch()
        ws.close()
        print("WebSocket closed.")

if __name__ == "__main__":
    main()