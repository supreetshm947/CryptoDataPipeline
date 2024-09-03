import time
from mylogger import get_logger

from constants import POSTGRES_TABLE_META_DATA, POSTGRES_TABLE_META_DATA_ID, POSTGRES_TABLE_META_DATA_NAME, \
    POSTGRES_TABLE_META_DATA_SYMBOL, POSTGRES_TABLE_META_DATA_IS_MONITORED, NUMBER_OF_REDDIT_SUBMISSION_TO_FETCH
from kafka_utils.producer import BaseProducer
from reddit_client import search_within_subreddit
from spark_connector import postgres

logger = get_logger()


class RedditSubmissionProducer(BaseProducer):
    def __init__(self, kafka_host, kafka_port, kafka_topic, interval, **kwargs):
        super().__init__(kafka_host, kafka_port, kafka_topic, interval)

        self.session = kwargs["session"]
        self.reddit = kwargs["reddit"]

    def produce_messages(self):
        while True:
            try:

                clauses = {POSTGRES_TABLE_META_DATA_IS_MONITORED: True}
                coins = postgres.get_data(self.session, POSTGRES_TABLE_META_DATA,
                                          [POSTGRES_TABLE_META_DATA_ID, POSTGRES_TABLE_META_DATA_NAME,
                                           POSTGRES_TABLE_META_DATA_SYMBOL], clauses)

                for coin in coins:
                    try:
                        coin_id = coin[POSTGRES_TABLE_META_DATA_ID]
                        coin_name = coin[POSTGRES_TABLE_META_DATA_NAME]
                        coin_symbol = coin[POSTGRES_TABLE_META_DATA_SYMBOL]
                        keywords = [coin_name, coin_symbol]  # filtering retweets
                        reddit_submission = search_within_subreddit(self.reddit, coin_id, keywords,
                                                                    limit=int(NUMBER_OF_REDDIT_SUBMISSION_TO_FETCH))
                        self.producer.send(self.topic, value=reddit_submission)
                    except Exception as e:
                        coin_id = coin.get(POSTGRES_TABLE_META_DATA_ID, None)
                        logger.error(f"Error in {self.name}, while fetching data for {coin_id}: {e}")

                self.producer.flush()
                logger.info(f"Reddit Submission successfully produced.")

                self.sleep()
            except Exception as e:
                logger.error(f"Error in {self.name} loop: {e}")

