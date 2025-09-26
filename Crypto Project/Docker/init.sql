-- Create crypto_prices table
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    price_usd NUMERIC,
    market_cap NUMERIC,
    volume NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create crypto_predictions table
CREATE TABLE IF NOT EXISTS crypto_predictions (
    ds DATE NOT NULL,
    yhat NUMERIC,
    yhat_lower NUMERIC,
    yhat_upper NUMERIC,
    symbol VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);