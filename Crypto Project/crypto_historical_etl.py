import requests
import pandas as pd
import os
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import time

# Load environment variables
load_dotenv('env')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create data directory (same as ETL script)
DATA_DIR = "crypto_data"
os.makedirs(DATA_DIR, exist_ok=True)

# PostgreSQL config
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

TABLE_NAME = "crypto_prices"
COIN_ID = "bitcoin"

def fetch_historical_data(days=30):
    """Fetch historical Bitcoin data from CoinGecko API"""
    logger.info(f"Fetching {days} days of historical data for {COIN_ID}...")
    
    url = f"https://api.coingecko.com/api/v3/coins/{COIN_ID}/market_chart"
    params = {
        'vs_currency': 'usd',
        'days': days,
        'interval': 'daily'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract price data
        prices = data['prices']
        market_caps = data['market_caps']
        volumes = data['total_volumes']
        
        historical_data = []
        
        for i, (timestamp_ms, price) in enumerate(prices):
            # Convert timestamp from milliseconds to datetime
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
            
            # Get corresponding market cap and volume
            market_cap = market_caps[i][1] if i < len(market_caps) else None
            volume = volumes[i][1] if i < len(volumes) else None
            
            row = {
                "timestamp": timestamp,
                "coin_id": COIN_ID,
                "symbol": "btc",
                "name": "Bitcoin",
                "current_price_usd": price,
                "market_cap_usd": market_cap,
                "total_volume_usd": volume,
                "price_change_24h": None,  # Not available in historical data
                "price_change_percentage_24h": None,
                "circulating_supply": None,
                "total_supply": None,
                "ath": None,
                "ath_date": None,
                "atl": None,
                "atl_date": None,
            }
            historical_data.append(row)
        
        df = pd.DataFrame(historical_data)
        logger.info(f"Successfully fetched {len(df)} historical records")
        
        # Save historical data to crypto_data folder
        save_historical_files(df, days)
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching historical data: {e}")
        return pd.DataFrame()

def save_historical_files(df, days):
    """Save historical dataframe to CSV and Parquet files in crypto_data folder"""
    if df.empty:
        return
        
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(DATA_DIR, f"bitcoin_historical_{days}days_{timestamp}.csv")
        pq_path = os.path.join(DATA_DIR, f"bitcoin_historical_{days}days_{timestamp}.parquet")
        
        df.to_csv(csv_path, index=False)
        df.to_parquet(pq_path, index=False)
        logger.info(f"Saved historical data to {csv_path} and {pq_path}")
        
    except Exception as e:
        logger.error(f"Error saving historical files: {e}")

def connect_postgres():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def insert_historical_data(conn, df):
    """Insert historical dataframe data into PostgreSQL table"""
    if df.empty:
        logger.warning("No data to insert")
        return
    
    try:
        # Prepare the column names
        cols = [col for col in df.columns]
        cols_str = ",".join(cols)
        
        # Prepare values
        values = []
        for _, row in df.iterrows():
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
            logger.info(f"Inserted {rows_affected} new historical records into '{TABLE_NAME}'")
            
    except Exception as e:
        logger.error(f"Error inserting historical data: {e}")
        conn.rollback()
        raise

def check_current_data(conn):
    """Check how much Bitcoin data we currently have"""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
                FROM {TABLE_NAME} 
                WHERE coin_id = %s
            """, (COIN_ID,))
            
            count, min_date, max_date = cur.fetchone()
            logger.info(f"Current Bitcoin records: {count}")
            if count > 0:
                logger.info(f"Date range: {min_date} to {max_date}")
            return count
            
    except Exception as e:
        logger.error(f"Error checking current data: {e}")
        return 0

def main():
    """Main process to backfill historical data"""
    logger.info("Starting historical data backfill...")
    logger.info(f"Data files will be saved to: {DATA_DIR}")
    
    # Connect to database
    conn = connect_postgres()
    if not conn:
        logger.error("Could not connect to database. Exiting.")
        return
    
    try:
        # Check current data
        current_count = check_current_data(conn)
        
        # Determine how many days of historical data to fetch
        days_to_fetch = 30 if current_count < 30 else 7
        logger.info(f"Will fetch {days_to_fetch} days of historical data")
        
        # Fetch historical data (this also saves files)
        df = fetch_historical_data(days=days_to_fetch)
        if df.empty:
            logger.error("No historical data fetched. Exiting.")
            return
        
        # Insert data
        insert_historical_data(conn, df)
        
        # Check final count
        final_count = check_current_data(conn)
        logger.info(f"Historical data backfill complete! Total Bitcoin records: {final_count}")
        
        if final_count >= 10:
            logger.info("You now have enough data to run Prophet forecasting!")
        else:
            logger.warning(f"You have {final_count} records. Prophet works better with 10+ data points.")
        
        # List files in crypto_data directory
        try:
            files = os.listdir(DATA_DIR)
            data_files = [f for f in files if f.startswith(('crypto_', 'bitcoin_'))]
            if data_files:
                logger.info(f"Files in {DATA_DIR}: {len(data_files)} total data files")
        except Exception as e:
            logger.warning(f"Could not list directory contents: {e}")
        
    except Exception as e:
        logger.error(f"Historical data backfill failed: {e}")
        
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()