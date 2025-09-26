from prophet import Prophet
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from 'env' file (not '.env')
load_dotenv('env')

# Create data directory
DATA_DIR = "crypto_data"
os.makedirs(DATA_DIR, exist_ok=True)

# Debug: Check if environment variables are loaded
logger.info("Checking environment variables...")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT") 
dbname = os.getenv("POSTGRES_DB")
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")

logger.info(f"Host: {host}")
logger.info(f"Port: {port}")
logger.info(f"Database: {dbname}")
logger.info(f"User: {user}")
logger.info(f"Password: {'*' * len(password) if password else 'None'}")

# Validate environment variables
if not all([host, port, dbname, user, password]):
    logger.error("Missing environment variables!")
    logger.error(f"Missing: {[k for k, v in {'host': host, 'port': port, 'dbname': dbname, 'user': user, 'password': password}.items() if not v]}")
    exit(1)

# Database credentials
db_params = {
    "host": host,
    "port": port,
    "dbname": dbname,
    "user": user,
    "password": password
}

def ensure_table_exists(engine):
    """Check if crypto_predictions table exists (should already exist)"""
    try:
        # Just verify the table exists by querying it
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM crypto_predictions LIMIT 1"))
            logger.info("✅ Using existing crypto_predictions table")
    except Exception as e:
        logger.error(f"crypto_predictions table not found: {e}")
        raise

def save_forecast_to_db(engine, forecast_df, training_info):
    """Save forecast results to existing crypto_predictions table"""
    try:
        # Prepare data for your existing table structure
        db_records = []
        for _, row in forecast_df.iterrows():
            record = {
                'ds': row['date'],  # predicted_date -> ds
                'yhat': row['predicted_price'],  # predicted_price -> yhat
                'yhat_lower': row['lower_bound'],  # lower_bound -> yhat_lower
                'yhat_upper': row['upper_bound'],  # upper_bound -> yhat_upper
                'symbol': 'BTC',  # symbol for Bitcoin
                'created_at': datetime.now()  # when this forecast was made
            }
            db_records.append(record)
        
        # Convert to DataFrame for easy database insertion
        db_df = pd.DataFrame(db_records)
        
        # Insert into your existing crypto_predictions table
        db_df.to_sql('crypto_predictions', engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"Saved {len(db_records)} forecast records to crypto_predictions table")
        
    except Exception as e:
        logger.error(f"Error saving forecast to database: {e}")
        raise

def get_recent_forecasts(engine, limit=5):
    """Retrieve recent forecasts from crypto_predictions table"""
    try:
        from sqlalchemy import text
        query = text("""
        SELECT 
            created_at,
            ds,
            yhat,
            yhat_lower,
            yhat_upper,
            symbol
        FROM crypto_predictions 
        WHERE symbol = 'BTC'
        ORDER BY created_at DESC, ds ASC 
        LIMIT :limit_val
        """)
        
        df = pd.read_sql(query, engine, params={"limit_val": limit})
        return df
        
    except Exception as e:
        logger.error(f"Error retrieving recent forecasts: {e}")
        return pd.DataFrame()

def cleanup_old_forecasts(engine, days_to_keep=30):
    """Remove forecasts older than specified days from crypto_predictions"""
    try:
        from sqlalchemy import text
        cleanup_sql = text("""
        DELETE FROM crypto_predictions 
        WHERE symbol = 'BTC' AND created_at < NOW() - INTERVAL ':days days'
        """)
        
        with engine.connect() as conn:
            result = conn.execute(cleanup_sql, {"days": days_to_keep})
            deleted_count = result.rowcount
            conn.commit()
            
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old Bitcoin forecast records")
            
    except Exception as e:
        logger.error(f"Error cleaning up old forecasts: {e}")

try:
    # URL encode the password to handle special characters
    from urllib.parse import quote_plus
    encoded_password = quote_plus(db_params['password'])
    
    # Create connection string for SQLAlchemy
    connection_string = f"postgresql+psycopg2://{db_params['user']}:{encoded_password}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    logger.info("Creating database engine...")
    engine = create_engine(connection_string)
    
    # Test connection
    logger.info("Testing database connection...")
    with engine.connect() as conn:
        logger.info("✅ Database connection successful!")
    
    # Create forecast table
    ensure_table_exists(engine)
    
    # Read crypto prices table from PostgreSQL
    logger.info("Fetching Bitcoin data from database...")
    query = """
    SELECT timestamp as ds, current_price_usd as y 
    FROM crypto_prices 
    WHERE coin_id = 'bitcoin' 
    ORDER BY timestamp
    """
    
    df = pd.read_sql(query, engine)
    logger.info(f"Fetched {len(df)} Bitcoin price records")
    
    if df.empty:
        logger.error("No Bitcoin data found in database!")
        logger.info("Make sure you've run the crypto_etl.py script first to populate the database")
        exit(1)
    
    # Check data quality
    logger.info(f"Date range: {df['ds'].min()} to {df['ds'].max()}")
    logger.info(f"Price range: ${df['y'].min():.2f} to ${df['y'].max():.2f}")
    
    # Store training info for database
    training_info = {
        'start_date': df['ds'].min(),
        'end_date': df['ds'].max(),
        'data_points': len(df)
    }
    
    # Prepare DataFrame for Prophet (already renamed in the query)
    df_prophet = df[["ds", "y"]].copy()
    
    # Remove any null values
    df_prophet = df_prophet.dropna()
    logger.info(f"Using {len(df_prophet)} clean records for forecasting")
    
    # Initialize and fit model
    logger.info("Training Prophet model...")
    model = Prophet(
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=True,
        changepoint_prior_scale=0.05  # Controls flexibility of automatic changepoint detection
    )
    model.fit(df_prophet)
    logger.info("✅ Model training complete!")
    
    # Forecast next 30 days
    logger.info("Generating 30-day forecast...")
    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)
    
    # Get just the future predictions (last 30 rows)
    future_forecast = forecast.tail(30)[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
    future_forecast.columns = ['date', 'predicted_price', 'lower_bound', 'upper_bound']
    
    # Save prediction outputs to files in crypto_data folder
    forecast_full_path = os.path.join(DATA_DIR, "bitcoin_forecast_full.csv")
    forecast_30days_path = os.path.join(DATA_DIR, "bitcoin_forecast_30days.csv")
    
    forecast.to_csv(forecast_full_path, index=False)
    future_forecast.to_csv(forecast_30days_path, index=False)
    
    # Save forecast to database
    logger.info("Saving forecast to database...")
    save_forecast_to_db(engine, future_forecast, training_info)
    
    # Cleanup old forecasts
    cleanup_old_forecasts(engine, days_to_keep=30)
    
    # Display summary
    logger.info("\n" + "="*60)
    logger.info("BITCOIN PRICE FORECAST SUMMARY")
    logger.info("="*60)
    logger.info(f"Training period: {df['ds'].min()} to {df['ds'].max()}")
    logger.info(f"Current price: ${df['y'].iloc[-1]:.2f}")
    logger.info(f"30-day forecast range: ${future_forecast['lower_bound'].mean():.2f} - ${future_forecast['upper_bound'].mean():.2f}")
    logger.info(f"Average predicted price: ${future_forecast['predicted_price'].mean():.2f}")
    logger.info("="*60)
    
    print("\n📊 Forecast complete!")
    print(f"📁 Full forecast saved to: {forecast_full_path}")
    print(f"📁 30-day forecast saved to: {forecast_30days_path}")
    print(f"💾 Forecast data saved to database table: crypto_predictions")
    
    # Display first few predictions
    print("\nNext 5 days forecast:")
    print(future_forecast.head()[['date', 'predicted_price', 'lower_bound', 'upper_bound']].to_string(index=False))
    
    # Show recent database records
    recent_forecasts = get_recent_forecasts(engine, limit=3)
    if not recent_forecasts.empty:
        print(f"\n📊 Recent forecasts in crypto_predictions table:")
        recent_forecasts.columns = ['Forecast Made', 'Predicted Date', 'Price', 'Lower', 'Upper', 'Symbol']
        print(recent_forecasts.to_string(index=False))
    
    # Show forecast statistics
    with engine.connect() as conn:
        stats_query = """
        SELECT 
            COUNT(*) as total_forecasts,
            COUNT(DISTINCT DATE(created_at)) as unique_forecast_days,
            MIN(created_at) as first_forecast,
            MAX(created_at) as latest_forecast
        FROM crypto_predictions
        WHERE symbol = 'BTC'
        """
        stats_df = pd.read_sql(stats_query, conn)
        if not stats_df.empty:
            stats = stats_df.iloc[0]
            print(f"\n📈 Bitcoin forecast statistics in database:")
            print(f"   Total forecast records: {stats['total_forecasts']}")
            print(f"   Unique forecast days: {stats['unique_forecast_days']}")
            print(f"   First forecast: {stats['first_forecast']}")
            print(f"   Latest forecast: {stats['latest_forecast']}")
    
except Exception as e:
    logger.error(f"Error occurred: {e}")
    logger.error("Make sure:")
    logger.error("1. Your 'env' file has the correct database credentials")
    logger.error("2. Your database is running")
    logger.error("3. You've run crypto_etl.py to populate the database with Bitcoin data")
    logger.error("4. You have the required packages: pip install prophet sqlalchemy psycopg2-binary pandas")
    exit(1)