import logging
import sys
import pandas as pd
from datetime import datetime
from db import Database
from angel_api import AngelOneAPI
import config

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def sync_master_contract(api, db):
    """
    Fetch the latest master contract and update the local database.
    This ensures tokens are always accurate.
    """
    logger.info("Syncing master contract with database...")
    df = api.get_master_contract()
    if df.empty:
        logger.error("Could not fetch master contract.")
        return

    # Filter for NSE Equity (Standard setup)
    # Angel symbols for equity usually have symboltype='SYMBOL' or segment='NSE'
    eq_df = df[(df['exch_seg'] == 'NSE') & (df['instrumenttype'] == 'SYMBOL')]
    
    # Optional: Save to CSV for the manual lookup fallback we wrote earlier
    eq_df.to_csv("master_contract_NSE.csv", index=False)

    # Note: We don't bulk insert all 50,000+ stocks to avoid bloat.
    # We will insert stocks on-demand during the historical fetch.
    logger.info(f"Master contract synced. {len(eq_df)} symbols available.")
    return eq_df

def fetch_and_store_historical(api, db, master_df):
    """
    Handles the user input, data fetching, and high-speed bulk insertion.
    """
    symbol = input("\nEnter stock symbol (e.g., RELIANCE): ").strip().upper()
    
    # 1. Get Token from Master DF
    # We append '-EQ' because Angel One uses that suffix for NSE Cash symbols
    lookup_symbol = f"{symbol}-EQ"
    match = master_df[master_df['symbol'] == lookup_symbol]
    
    if match.empty:
        # Try without the -EQ suffix just in case
        match = master_df[master_df['symbol'] == symbol]
    
    if match.empty:
        logger.error(f"Symbol {symbol} not found in master contract.")
        return

    token = str(match.iloc[0]['token'])
    name = match.iloc[0]['name']
    logger.info(f"Found Token: {token} for {symbol}")

    # 2. Get Date Range
    from_date = input("Enter start date (YYYY-MM-DD): ").strip()
    to_date = input("Enter end date (YYYY-MM-DD): ").strip()
    
    try:
        # Simple validation
        datetime.strptime(from_date, "%Y-%m-%d")
        datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD.")
        return

    # 3. Fetch Data from API
    logger.info(f"Fetching {config.DEFAULT_INTERVAL} data for {symbol}...")
    df = api.get_historical_data(
        symbol=symbol,
        token=token,
        exchange="NSE",
        from_date=from_date,
        to_date=to_date,
        interval=config.DEFAULT_INTERVAL
    )

    if df.empty:
        logger.warning("No data returned from API.")
        return

    # 4. Prepare for Database
    # Ensure the stock exists in the 'stocks' table and get its internal ID
    stock_id = db.insert_stock(symbol, token, name=name)

    # Convert DataFrame to list of tuples for bulk insertion
    # Structure: (stock_id, date, open, high, low, close, volume)
    records = []
    for _, row in df.iterrows():
        records.append((
            stock_id,
            row['date'].to_pydatetime(), # Convert pandas Timestamp to python datetime
            float(row['open']),
            float(row['high']),
            float(row['low']),
            float(row['close']),
            int(row['volume'])
        ))

    # 5. Execute Bulk Insert
    db.insert_prices_bulk(records)
    logger.info(f"Successfully processed {len(records)} candles for {symbol}.")

def main():
    logger.info("--- Starting Stock Data Pipeline ---")
    db = None
    api = None
    
    try:
        # Initialize Database
        db = Database()
        db.create_tables()

        # Initialize API (handles automated TOTP login)
        api = AngelOneAPI()

        # Step 1: Update/Sync Master Data
        master_df = sync_master_contract(api, db)

        # Step 2: User loop for fetching data
        while True:
            fetch_and_store_historical(api, db, master_df)
            cont = input("\nFetch another stock? (y/n): ").lower()
            if cont != 'y':
                break

    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user.")
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
    finally:
        if db:
            db.close()
        if api:
            api.logout()
        logger.info("--- Pipeline Finished ---")

if __name__ == "__main__":
    main()