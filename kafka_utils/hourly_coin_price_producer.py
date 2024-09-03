import time

from coin_utils import fetch_coin_tickers_data, get_all_active_coin_ids

from kafka_utils.producer import BaseProducer
from mylogger import get_logger

logger = get_logger()


class CoinPriceProducer(BaseProducer):
    def __init__(self, kafka_host, kafka_port, kafka_topic, interval, **kwargs):
        super().__init__(kafka_host, kafka_port, kafka_topic, interval)
        self.session = kwargs["session"]

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
                        logger.error(f"Error in {self.name}, while fetching data for {coin_id}: {e}")

                self.producer.flush()
                logger.info(f"Hourly Price Messages successfully generated.")
                self.sleep()
            except Exception as e:
                logger.error(f"Error in {self.name} loop: {e}")
