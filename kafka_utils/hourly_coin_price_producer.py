from kafka import KafkaProducer
import json
import time
import signal

from coin_utils import fetch_coin_tickers_data, get_all_active_coin_ids
from constants import KAFKA_HOST, KAFKA_PORT
import sys

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_SERVER = f'{KAFKA_HOST}:{KAFKA_PORT}'  # or your Docker container's IP


class CoinPriceProducer:
    def __init__(self, kafka_host, kafka_port, topic, interval, **kwargs):
        self.kafka_server = f"{kafka_host}:{kafka_port}"
        self.topic = topic
        self.interval = interval
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_server],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Serialize data to JSON
        )
        # gracefully shutdown the producer in case it is interrupted
        self.session = kwargs["session"]
        signal.signal(signal.SIGINT, self.shutdown_producer)

    def produce_messages(self):
        while True:
            try:
                # # test code
                # with open("sample_json/tickers_USDZ.json") as f:
                #     sample_yr_historic = json.load(f)
                # from constants import KAFKA_TOPIC_HOURLY_PRICE
                # self.producer.send(KAFKA_TOPIC_HOURLY_PRICE, value=sample_yr_historic)

                # Get all coin ids
                coin_ids = get_all_active_coin_ids(self.session)

                for coin_id in coin_ids:
                    try:
                        coin_data = fetch_coin_tickers_data(coin_id)
                        self.producer.send(self.topic, value=coin_data)
                    except Exception as e:
                        logger.error(f"Error fetching data for {coin_id}: {e}")

                self.producer.flush()
                logger.info(f"Hourly Price Messages successfully generated.")
            except Exception as e:
                logger.error(f"Error in producer loop: {e}")

            time.sleep(self.interval)

    def shutdown_producer(self):
        logger.info('Shutting down CoinPriceProducer...')
        self.producer.flush()
        self.producer.close()
        sys.exit(0)

    def start(self):
        logger.info(f"Starting CoinPriceProducer for topic {self.topic}")
        self.produce_messages()
