import praw

from constants import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET
import requests

from utils import exception_logger, convert_seconds_to_datetime

SORT_HOT="hot"
SORT_TOP="top"
SORT_RELEVANCE="relevance"

def convert_submission_to_dict(submission, coin_id, sort_type):
    post_dict = {
        'coin_id':coin_id,
        'id': submission.id,
        'title': submission.title or "",
        'score': submission.score,
        'url': submission.url or "",
        'num_comments': submission.num_comments,
        'created_utc':  convert_seconds_to_datetime(submission.created_utc),
        'selftext': submission.selftext or "",
        'subreddit': submission.subreddit.display_name,
        'author': str(submission.author),
        'sort_type': sort_type,
        'flair': submission.link_flair_text or ""
    }
    return post_dict

@exception_logger("reddit_client.get_reddit_client")
def get_reddit_client():
    reddit = praw.Reddit(client_id=REDDIT_CLIENT_ID,
                         client_secret=REDDIT_CLIENT_SECRET,
                         user_agent="gathering recent post related to cypto currencies to do sentiment analysis.")
    return reddit

@exception_logger("reddit_client.search_within_subreddit")
def search_within_subreddit(reddit, coin_id, keywords, subreddit='all', sort_type=SORT_HOT, limit=500):
    subreddit = reddit.subreddit(subreddit)
    posts = []
    for keyword in keywords:
        for submission in subreddit.search(keyword, sort=sort_type, limit=limit):
            post_dict = convert_submission_to_dict(submission, coin_id, sort_type)
            posts.append(post_dict)
    return posts


#TODO work on bearer token and all, before this can be implemented
def search_with_push_shift_api(reddit, coin_id, query, sort_type=SORT_HOT, days_after="7d", start_time=None, end_time=None, limit=500):
    url = f"https://api.pushshift.io/reddit/search/submission"
    params = {
        "q": query,
        "size": limit,  # Number of results to return
        "sort_type": sort_type,  # Sort by the score of the post
        "after": days_after  # Only get posts from the last days_after
    }
    if start_time:
        params.update({"after": start_time})
    if end_time:
        params.update({"before": end_time})

    response = requests.get(url, params=params)
    data = response.json()

    posts = []
    for submission_data in data['data']:
        post_id = submission_data['id']

        # Use PRAW to get the full Reddit post object
        submission = reddit.submission(id=post_id)

        post_dict =  convert_submission_to_dict(submission, coin_id, sort_type)
        posts.append(post_dict)

    return posts