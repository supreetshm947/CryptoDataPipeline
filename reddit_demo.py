import praw

reddit = praw.Reddit(client_id="FZTkFfnAS1f4zhWqvG6Fng",
    client_secret="ezSv5BKK6-1-5eoj4o4zkeFTBcMGhA",
    user_agent="gathering recent post related to cypto currencies to do sentiment analysis.")

subreddit = reddit.subreddit('cryptocurrency')
for post in subreddit.search('Bitcoin', limit=10):
    print(post.title, post.selftext)