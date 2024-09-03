import signal
import traceback
from coin_utils import insert_coin_price_in_db
from kafka_utils.consumer import BaseConsumer
from mylogger import get_logger
from sentiment_data import insert_reddit_submission_in_db

logger = get_logger()


class RedditSubmissionConsumer(BaseConsumer):
    def __init__(self, kafka_host, kafka_port, kafka_group, kafka_topic, **kwargs):
        super().__init__(kafka_host, kafka_port, kafka_group, kafka_topic)
        self.session = kwargs["session"]

    def process_message(self, message):
        try:
            insert_reddit_submission_in_db(self.session, message)
            logger.info(f"Successfully inserted reddit submissions.")
        except Exception as e:
            logger.error(f"Something went wrong in {self.name}: {e}")
            logger.error(traceback.format_exc())
