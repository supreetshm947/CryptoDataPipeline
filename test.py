from coin_utils import get_all_active_coin_ids
from constants import POSTGRES_TABLE_META_DATA, POSTGRES_TABLE_META_DATA_ID, TWITTER_BEARER_TOKEN
from spark_connector.session_utils import get_spark_session
from spark_connector import postgres

# session = postgres.get_session()
# coin_ids = postgres.get_all_ids(session, POSTGRES_TABLE_META_DATA, POSTGRES_TABLE_META_DATA_ID)
# postgres.close_session(session)
# session = get_spark_session()
# coin_ids = get_all_active_coin_ids(session)
# print(coin_ids)

import tweepy
import pandas as pd,  json

client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN)

# Fetch recent tweets using the v2 endpoint
response = client.search_recent_tweets(query="Bitcoin", max_results=10)

for tweet in response.data:
    print(tweet.text)
