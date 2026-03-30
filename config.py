import os
from dotenv import load_dotenv

load_dotenv()  # Loads variables from .env file

# MySQL configuration
MYSQL_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

# Angel One credentials
ANGEL_CREDENTIALS = {
    "client_id": os.getenv("ANGEL_CLIENT_ID"),
    "password": os.getenv("ANGEL_PASSWORD"),
    "totp": os.getenv("ANGEL_TOTP"),
    "api_key": os.getenv("ANGEL_API_KEY")
}

# Other settings
LOG_FILE = "logs/app.log"
LOG_LEVEL = "INFO"
DEFAULT_INTERVAL = "ONE_DAY"