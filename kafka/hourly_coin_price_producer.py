from kafka import KafkaProducer
import json
import time

from coin_utils import fetch_coin_tickers_data
from constants import KAFKA_HOST, KAFKA_PORT, POSTGRES_TABLE_META_DATA, POSTGRES_TABLE_META_DATA_ID
from spark import postgres

KAFKA_TOPIC = 'hourly_coin_prices'
KAFKA_SERVER = f'{KAFKA_HOST}:{KAFKA_PORT}'  # or your Docker container's IP

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Serialize data to JSON
)


def produce_coin_data():
    session = postgres.get_session()
    coin_ids = postgres.get_all_ids(session, POSTGRES_TABLE_META_DATA, POSTGRES_TABLE_META_DATA_ID)
    postgres.close_session(session)

    for coin_id in coin_ids:
        coin_data = fetch_coin_tickers_data(coin_id)
        producer.send(KAFKA_TOPIC, value=coin_data)


if __name__ == "__main__":
    while True:
        produce_coin_data()
        time.sleep(3600)  # Run every hour
