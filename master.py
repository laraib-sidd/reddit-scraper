import praw
from dotenv import load_dotenv
import os 
load_dotenv()

reddit = praw.Reddit(
    client_id=os.environ.get('client_id'),
    client_secret=os.environ.get('client_secret'),
    password=os.environ.get('password'),
    user_agent=os.environ.get('user_agent'),
    username=os.environ.get('username'))

subreddit = reddit.subreddit('forhire')
for post in subreddit.new(limit=10):
    # post_check = check_post(post.id)
    # if (post.link_flair_text == 'Hiring'):
    print(post.link_flair_text, post.title)