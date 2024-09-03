# Load Coin Metadata
from threading import Thread
from constants import KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_HOURLY_PRICE, KAFKA_TOPIC_REDDIT_SUBMISSIONS
from kafka_utils.hourly_coin_price_consumer import CoinPriceConsumer
from kafka_utils.hourly_coin_price_producer import CoinPriceProducer
from kafka_utils.reddit_submission_consumer import RedditSubmissionConsumer
from kafka_utils.reddit_submission_producer import RedditSubmissionProducer
from reddit_client import get_reddit_client
from spark_connector.elastic import create_reddit_index, get_elastic_session
from spark_connector.session_utils import get_spark_session

# load_coin_metadata()

# loading historic data for all coins in database
# from datetime import datetime, timedelta
#
# start_date = str((datetime.now() - timedelta(days=364)).date())
# load_coin_historic_data(start_date)

KAFKA_PRODUCERS = [CoinPriceProducer, RedditSubmissionProducer]
KAFKA_PRODUCER_ARGUMENT = dict()
KAFKA_PRODUCER_INTERVALS = [3600, 43200]
KAFKA_GROUP_ID = "crypto-group"
KAFKA_TOPICS = [KAFKA_TOPIC_HOURLY_PRICE, KAFKA_TOPIC_REDDIT_SUBMISSIONS]
KAFKA_CONSUMERS = [CoinPriceConsumer, RedditSubmissionConsumer]
KAFKA_CONSUMER_ARGUMENT = dict()


def instantiate_kafka_producers():
    return [producer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPICS[i], KAFKA_PRODUCER_INTERVALS[i], **KAFKA_PRODUCER_ARGUMENT)
            for i, producer in enumerate(KAFKA_PRODUCERS)]


def start_kafka_producer(producer):
    producer.start()


def instantiate_kafka_consumers():
    return [consumer(KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP_ID, KAFKA_TOPICS[i], **KAFKA_CONSUMER_ARGUMENT) for i, consumer
            in
            enumerate(KAFKA_CONSUMERS)]


def start_kafka_consumer(consumer):
    consumer.start()


def main():
    session = get_spark_session()
    reddit = get_reddit_client()

    # adding index to Elasticsearch
    es = get_elastic_session()
    create_reddit_index(es)

    KAFKA_PRODUCER_ARGUMENT.update(
        {
            "session": session,
            "reddit": reddit
        }
    )
    KAFKA_CONSUMER_ARGUMENT.update(
        {
            "session": session
        }
    )

    producers = instantiate_kafka_producers()
    consumers = instantiate_kafka_consumers()

    producer_threads = [Thread(target=start_kafka_producer, args=(producer,)) for producer in producers]
    consumer_threads = [Thread(target=start_kafka_consumer, args=(consumer,)) for consumer in consumers]

    [thread.start() for thread in producer_threads + consumer_threads]
    # all the rest of the functionality
    [thread.join() for thread in producer_threads + consumer_threads]

    session.stop()
    es.close()

if __name__ == "__main__":
    main()
