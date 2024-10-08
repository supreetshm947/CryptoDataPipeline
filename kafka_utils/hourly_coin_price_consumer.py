import signal
import traceback
from coin_utils import insert_coin_price_in_db
from kafka_utils.consumer import BaseConsumer
from mylogger import get_logger

logger = get_logger()


class CoinPriceConsumer(BaseConsumer):
    def __init__(self, kafka_host, kafka_port, kafka_group, kafka_topic, **kwargs):
        super().__init__(kafka_host, kafka_port, kafka_group, kafka_topic)
        self.session = kwargs["session"]

    def process_message(self, message):
        try:
            coin_id = message["id"]
            logger.info(f"Processing message for {coin_id}.")
            insert_coin_price_in_db(self.session, coin_price_data=message)
            in_time = message["last_updated"]
            logger.info(f"Successfully inserted price data for {coin_id} for {in_time}.")
        except Exception as e:
            coin_id = message.get("id", None)
            logger.error(f"Something went wrong in {self.name} while processing {coin_id}:{e}")
            logger.error(traceback.format_exc())
