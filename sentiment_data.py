from constants import POSTGRES_TABLE_META_DATA_IS_MONITORED, POSTGRES_TABLE_META_DATA, \
    POSTGRES_TABLE_META_DATA_NAME, POSTGRES_TABLE_META_DATA_SYMBOL, POSTGRES_TABLE_META_DATA_ID, \
    ELASTIC_REDDIT_INDEX_NAME
from reddit_client import search_within_subreddit, get_reddit_client
from spark_connector.elastic import insert_df
from spark_connector.session_utils import get_spark_session
from tweepy_client import get_twitter_api, get_tweets
from spark_connector import postgres

def insert_tweets_in_db(session, tweets):
    tweet_data_list = []
    for tweet in tweets:
        tweet_data = {
            "tweet_id": tweet.id_str,
            "created_at": tweet.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            "text": tweet.full_text,
            "user": tweet.user.screen_name,
            "user_id": tweet.user.id_str,  # User ID to track tweets by the same user
            "user_followers_count": tweet.user.followers_count,  # Influence measure
            "user_verified": tweet.user.verified,  # Whether the user is verified
            "retweet_count": tweet.retweet_count,  # Measure of engagement
            "favorite_count": tweet.favorite_count,  # Measure of engagement
            "hashtags": [hashtag["text"] for hashtag in tweet.entities["hashtags"]],
            "coin": tweet.coin,
            "symbol": tweet.symbol,
            "tweet_url": f"https://twitter.com/{tweet.user.screen_name}/status/{tweet.id_str}",  # Direct URL to the tweet
            "lang": tweet.lang,  # Language of the tweet
            "is_retweet": hasattr(tweet, 'retweeted_status')  # Check if it's a retweet
        }
        tweet_data_list.append(tweet_data)
    df = session.createDataFrame(tweet_data_list)
    insert_df(df)

def insert_reddit_submission_in_db(session, submissions):
    df = session.createDataFrame(submissions)
    insert_df(df, id_col="id", index=f"{ELASTIC_REDDIT_INDEX_NAME}")

def load_reddit_submission_for_coins_in_db(session, since_date=None, until_date=None, limit=500):
    # Get coin meta-data from database
    clauses = {POSTGRES_TABLE_META_DATA_IS_MONITORED: True}
    coins = postgres.get_data(session, POSTGRES_TABLE_META_DATA,
                             [POSTGRES_TABLE_META_DATA_ID, POSTGRES_TABLE_META_DATA_NAME, POSTGRES_TABLE_META_DATA_SYMBOL], clauses)

    for coin in coins:
        coin_id = coin[POSTGRES_TABLE_META_DATA_ID]
        coin_name = coin[POSTGRES_TABLE_META_DATA_NAME]
        coin_symbol = coin[POSTGRES_TABLE_META_DATA_SYMBOL]
        keywords = [coin_name, coin_symbol]    # filtering retweets
        reddit = get_reddit_client()
        reddit_submission = search_within_subreddit(reddit, coin_id, keywords, limit=limit)
        insert_reddit_submission_in_db(session, reddit_submission)


session = get_spark_session()
load_reddit_submission_for_coins_in_db(session, limit=1)