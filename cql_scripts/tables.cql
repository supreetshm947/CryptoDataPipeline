CREATE KEYSPACE IF NOT EXISTS crypto_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE crypto_data;

CREATE TABLE IF NOT EXISTS crypto_price_data (
    coin_id text,
    read_timestamp timestamp,
    date date,
    hour int,
    price decimal,
    volume_24h decimal,
    market_cap decimal,
    PRIMARY KEY (coin_id, read_timestamp)
);
