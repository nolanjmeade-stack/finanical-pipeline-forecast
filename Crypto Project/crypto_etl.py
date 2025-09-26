import requests
import pandas as pd
import os
import psycopg2
import psycopg2.extras  # Added missing import
from psycopg2 import sql
from datetime import datetime
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

# Load environment variables - specify 'env' file
load_dotenv('env')

# Setup logging
log_file = "crypto_etl.log"
logger = logging.getLogger("crypto_etl")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(log_file, maxBytes=1000000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler())

# PostgreSQL config
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

TABLE_NAME = "crypto_prices"
DATA_DIR = "crypto_data"
os.makedirs(DATA_DIR, exist_ok=True)

# Target coins
COINS = ["bitcoin", "ethereum", "binancecoin", "cardano"]

def fetch_crypto_data():
    """Fetch cryptocurrency data from CoinGecko API"""
    logger.info("Fetching data from CoinGecko API...")
    data = []
    for coin in COINS:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}"
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            j = res.json()
            mkt = j["market_data"]
            
            # Handle datetime parsing for ath_date and atl_date
            ath_date = None
            atl_date = None
            try:
                if mkt.get("ath_date") and mkt["ath_date"].get("usd"):
                    ath_date = pd.to_datetime(mkt["ath_date"]["usd"])
            except:
                logger.warning(f"Could not parse ath_date for {coin}")
                
            try:
                if mkt.get("atl_date") and mkt["atl_date"].get("usd"):
                    atl_date = pd.to_datetime(mkt["atl_date"]["usd"])
            except:
                logger.warning(f"Could not parse atl_date for {coin}")
            
            row = {
                "timestamp": datetime.now(),
                "coin_id": j["id"],
                "symbol": j["symbol"],
                "name": j["name"],
                "current_price_usd": mkt["current_price"]["usd"],
                "market_cap_usd": mkt["market_cap"]["usd"],
                "total_volume_usd": mkt["total_volume"]["usd"],
                "price_change_24h": mkt["price_change_24h"],
                "price_change_percentage_24h": mkt["price_change_percentage_24h"],
                "circulating_supply": mkt["circulating_supply"],
                "total_supply": mkt["total_supply"],
                "ath": mkt["ath"]["usd"],
                "ath_date": ath_date,
                "atl": mkt["atl"]["usd"],
                "atl_date": atl_date,
            }
            data.append(row)
            logger.info(f"Successfully fetched data for {coin}")
        except Exception as e:
            logger.error(f"Error fetching {coin}: {e}")
    
    df = pd.DataFrame(data)
    logger.info(f"Fetched data for {len(df)} coins")
    return df

def save_files(df):
    """Save dataframe to CSV and Parquet files"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(DATA_DIR, f"crypto_{timestamp}.csv")
    pq_path = os.path.join(DATA_DIR, f"crypto_{timestamp}.parquet")
    
    try:
        df.to_csv(csv_path, index=False)
        df.to_parquet(pq_path, index=False)
        logger.info(f"Saved data to {csv_path} and {pq_path}")
    except Exception as e:
        logger.error(f"Error saving files: {e}")

def connect_postgres():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def create_table_if_not_exists(conn):
    """Create or recreate the crypto_prices table with correct schema"""
    try:
        with conn.cursor() as cur:
            # Check if table exists and get its columns
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = 'public';
            """, (TABLE_NAME,))
            existing_columns = [row[0] for row in cur.fetchall()]
            
            # Expected columns
            expected_columns = [
                'id', 'timestamp', 'coin_id', 'symbol', 'name', 'current_price_usd',
                'market_cap_usd', 'total_volume_usd', 'price_change_24h', 
                'price_change_percentage_24h', 'circulating_supply', 'total_supply',
                'ath', 'ath_date', 'atl', 'atl_date', 'created_at'
            ]
            
            # If table doesn't exist or has wrong schema, recreate it
            if not existing_columns or not all(col in existing_columns for col in expected_columns):
                logger.info(f"Table schema mismatch. Recreating {TABLE_NAME} table...")
                
                # Drop existing table
                cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
                logger.info(f"Dropped existing {TABLE_NAME} table")
                
                # Create new table with correct schema
                create_sql = f"""
                CREATE TABLE {TABLE_NAME} (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL,
                    coin_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    name TEXT NOT NULL,
                    current_price_usd NUMERIC(20,8),
                    market_cap_usd NUMERIC(30,2),
                    total_volume_usd NUMERIC(30,2),
                    price_change_24h NUMERIC(20,8),
                    price_change_percentage_24h NUMERIC(10,4),
                    circulating_supply NUMERIC(30,2),
                    total_supply NUMERIC(30,2),
                    ath NUMERIC(20,8),
                    ath_date TIMESTAMP,
                    atl NUMERIC(20,8),
                    atl_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (coin_id, timestamp)
                );
                """
                cur.execute(create_sql)
                logger.info(f"Created new {TABLE_NAME} table with correct schema")
            else:
                logger.info(f"Table '{TABLE_NAME}' already exists with correct schema")
            
            conn.commit()
            
    except Exception as e:
        logger.error(f"Error managing table: {e}")
        conn.rollback()
        raise

def insert_data(conn, df):
    """Insert dataframe data into PostgreSQL table"""
    if df.empty:
        logger.warning("No data to insert")
        return
    
    try:
        # Prepare the column names (excluding 'id' since it's auto-generated)
        cols = [col for col in df.columns]
        cols_str = ",".join(cols)
        
        # Prepare values
        values = []
        for _, row in df.iterrows():
            # Handle None values and convert to tuple
            row_values = []
            for val in row:
                if pd.isna(val):
                    row_values.append(None)
                else:
                    row_values.append(val)
            values.append(tuple(row_values))
        
        # Create placeholders for the INSERT statement
        placeholders = ",".join(["%s"] * len(cols))
        insert_sql = f"""
            INSERT INTO {TABLE_NAME} ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT (coin_id, timestamp) DO NOTHING;
        """
        
        with conn.cursor() as cur:
            cur.executemany(insert_sql, values)
            rows_affected = cur.rowcount
            conn.commit()
            logger.info(f"Inserted {rows_affected} new rows into '{TABLE_NAME}'")
            
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        conn.rollback()
        raise

def summarize_db(conn):
    """Display recent records from the database"""
    try:
        with conn.cursor() as cur:
            # Get total count
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
            total_count = cur.fetchone()[0]
            logger.info(f"Total records in {TABLE_NAME}: {total_count}")
            
            # Get recent records
            cur.execute(f"""
                SELECT coin_id, symbol, current_price_usd, timestamp 
                FROM {TABLE_NAME} 
                ORDER BY timestamp DESC 
                LIMIT 10;
            """)
            rows = cur.fetchall()
            
            print("\n" + "="*60)
            print("RECENT CRYPTO PRICE DATA")
            print("="*60)
            for row in rows:
                coin_id, symbol, price, timestamp = row
                print(f"{symbol.upper():>6} | ${price:>10.2f} | {timestamp}")
            print("="*60)
            
    except Exception as e:
        logger.error(f"Error querying database: {e}")

def main():
    """Main ETL process"""
    logger.info("Starting crypto ETL process...")
    
    try:
        # Extract
        df = fetch_crypto_data()
        if df.empty:
            logger.warning("No data fetched. Exiting.")
            return
        
        # Transform & Save files
        save_files(df)
        
        # Load to database
        conn = connect_postgres()
        if not conn:
            logger.error("Could not connect to database. Exiting.")
            return
            
        try:
            create_table_if_not_exists(conn)
            insert_data(conn, df)
            summarize_db(conn)
            logger.info("ETL process completed successfully!")
            
        finally:
            conn.close()
            logger.info("Database connection closed")
            
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    main()