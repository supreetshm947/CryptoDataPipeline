from kafka import KafkaConsumer
import json
import signal
import logging
import sys
import traceback
from coin_utils import insert_coin_price_in_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CoinPriceConsumer:
    def __init__(self, kafka_host, kafka_port, topic, **kwargs):
        self.kafka_server = f"{kafka_host}:{kafka_port}"
        self.topic = topic
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_server,
            auto_offset_reset="earliest",  # Start reading at the earliest message
            enable_auto_commit=True,
            group_id="coin-price-group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        self.running = True
        self.session = kwargs["session"]
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown_consumer)

    def shutdown_consumer(self, signum, frame):
        logger.info("Shutting down CoinPriceConsumer...")
        self.running = False
        self.consumer.close()
        sys.exit(0)

    def start(self):
        logger.info(f"Starting CoinPriceConsumer for topic {self.topic}")
        for message in self.consumer:
            if not self.running:
                break
            self.process_message(message.value)

    def process_message(self, price_date):
        try:
            coin_id = price_date["id"]
            logger.info(f"Processing message for {coin_id}.")
            insert_coin_price_in_db(self.session, coin_price_data=price_date)
            in_time = price_date["last_updated"]
            logger.info(f"Successfully inserted price data for {coin_id} for {in_time}.")
        except Exception as e:
            coin_id = price_date.get("id", None)
            logger.error(f"Something went wrong while processing and persisting price data for {coin_id}:{e}")
            logger.error(traceback.format_exc())
            raise e
