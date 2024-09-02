import tweepy
from tweepy import API
from constants import TWITTER_API_KEY, TWITTER_API_KEY_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_twitter_api():
    auth = tweepy.OAuth1UserHandler(TWITTER_API_KEY, TWITTER_API_KEY_SECRET,TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)

    # wait_on_rate_limit allows api to wait if code hits rate limit and for the rate limit window to reset before continuing.
    api = tweepy.API(auth, wait_on_rate_limit=True)
    return api


def get_tweets(api: API, query, lang="en", since_date=None, until_date=None, number=1):
    logger.info(f"Getting tweets related to: {query}")
    try:
        args = {
            "q":query,
            "lang":lang,
            "tweet_mode":"extended"
        }

        if since_date:
            args["since"]=since_date
        if until_date:
            args["until"]=until_date


        # tweet_mode extended allows to get full tweet not truncated and for free tier of API we can do 180 requests per 15 min and only a maximum of 100 tweets per request.
        tweets = tweepy.Cursor(api.search_tweets, **args).items(number)
        return tweets
    except tweepy.errors.TweepyException as e:
        logger.error(f"Error fetching tweets for {query}: {str(e)}")

