import mysql.connector
from mysql.connector import Error
import logging
from config import MYSQL_CONFIG

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.connection = None
        self.connect()

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**MYSQL_CONFIG)
            logger.info("MySQL connection established")
        except Error as e:
            logger.error(f"MySQL connection failed: {e}")
            raise

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL connection closed")

    def create_tables(self):
        """Create tables if they don't exist."""
        cursor = self.connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                token VARCHAR(20) NOT NULL,
                name VARCHAR(100),
                is_fno BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY (symbol, token)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_id INT NOT NULL,
                date DATE NOT NULL,
                open DECIMAL(12,4),
                high DECIMAL(12,4),
                low DECIMAL(12,4),
                close DECIMAL(12,4),
                volume BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE,
                UNIQUE KEY (stock_id, date)
            )
        """)
        self.connection.commit()
        cursor.close()
        logger.info("Tables created/verified")

    def insert_stock(self, symbol, token, name="", is_fno=True):
        """Insert or update a stock record."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO stocks (symbol, token, name, is_fno)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name), is_fno = VALUES(is_fno)
            """, (symbol, token, name, is_fno))
            self.connection.commit()
            return cursor.lastrowid
        except Error as e:
            logger.error(f"Failed to insert stock {symbol}: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def get_stock_id(self, symbol, token):
        """Return stock ID for given symbol and token."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT id FROM stocks WHERE symbol = %s AND token = %s", (symbol, token))
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row else None

    def insert_price(self, stock_id, date, open_, high, low, close, volume):
        """Insert or update daily OHLCV data."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open = VALUES(open), high = VALUES(high), low = VALUES(low),
                close = VALUES(close), volume = VALUES(volume)
            """, (stock_id, date, open_, high, low, close, volume))
            self.connection.commit()
        except Error as e:
            logger.error(f"Failed to insert price for stock {stock_id} on {date}: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def stock_exists(self, symbol, token):
        """Check if stock already exists."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT id FROM stocks WHERE symbol = %s AND token = %s", (symbol, token))
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists
