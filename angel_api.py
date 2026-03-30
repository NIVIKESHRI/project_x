import logging
import pandas as pd
from SmartApi import SmartConnect
import pyotp
from config import ANGEL_CREDENTIALS

logger = logging.getLogger(__name__)

class AngelOneAPI:
    def __init__(self):
        self.smart_api = None
        self.auth_token = None
        self.login()

    def login(self):
        """Authenticates with Angel One using credentials and TOTP."""
        try:
            self.smart_api = SmartConnect(api_key=ANGEL_CREDENTIALS["api_key"])
            
            # Generate TOTP
            totp_code = pyotp.TOTP(ANGEL_CREDENTIALS["totp"]).now()
            
            data = self.smart_api.generateSession(
                ANGEL_CREDENTIALS["client_id"], 
                ANGEL_CREDENTIALS["password"], 
                totp_code
            )

            if data['status']:
                self.auth_token = data['data']['jwtToken']
                logger.info("Angel One login successful")
            else:
                logger.error(f"Login failed: {data.get('message')}")
                raise Exception(f"Login failed: {data.get('message')}")
                
        except Exception as e:
            logger.error(f"Error during Angel One login: {e}")
            raise

    def get_master_contract(self):
        """
        Fetches the full master contract from Angel One.
        Note: This is a large JSON file containing all tradable instruments.
        """
        try:
            logger.info("Downloading master contract...")
            # This returns a list of dictionaries
            import requests
            url = "https://margincalculator.angelbroking.com/OpenAPI_Standard/token/OpenAPIScripMaster.json"
            response = requests.get(url)
            data = response.json()
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Failed to fetch master contract: {e}")
            return pd.DataFrame()

    def get_historical_data(self, symbol, token, exchange, from_date, to_date, interval):
        """
        Fetches historical OHLCV data.
        Dates should be in 'YYYY-MM-DD HH:MM' format for the API.
        """
        try:
            # Ensure date format has time for the API (defaulting to market open/close)
            if len(from_date) == 10: from_date += " 09:15"
            if len(to_date) == 10: to_date += " 15:30"

            params = {
                "exchange": exchange,
                "symboltoken": token,
                "interval": interval,
                "fromdate": from_date,
                "todate": to_date
            }

            response = self.smart_api.getCandleData(params)

            if response['status'] and response['data']:
                # Angel returns a list of lists: [time, open, high, low, close, volume]
                df = pd.DataFrame(response['data'], columns=['date', 'open', 'high', 'low', 'close', 'volume'])
                
                # Convert date string to datetime objects
                df['date'] = pd.to_datetime(df['date'])
                return df
            else:
                logger.warning(f"No data for {symbol}: {response.get('message')}")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {e}")
            return pd.DataFrame()

    def logout(self):
        """Terminates the session."""
        try:
            if self.smart_api:
                self.smart_api.terminateSession(ANGEL_CREDENTIALS["client_id"])
                logger.info("Angel One session terminated")
        except Exception as e:
            logger.error(f"Error during logout: {e}")