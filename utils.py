import praw
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from datetime import timedelta,datetime as dt
import sys
import pandas as pd
from prawcore import NotFound

def get_mongo_client():
    url = "mongodb://localhost:27017"
    client = MongoClient(url)
    return client

def validate_date(date):
    format = "%Y-%m-%d"
    try:
        res = bool(dt.strptime(date, format))
    except ValueError:
        res = False
    return res


def get_reddit_client():
    load_dotenv()
    reddit = praw.Reddit(
    client_id=os.environ.get('client_id'),
    client_secret=os.environ.get('client_secret'),
    password=os.environ.get('password'),
    user_agent=os.environ.get('user_agent'),
    username=os.environ.get('username'))
    return reddit

def sub_exists(args):
    reddit = get_reddit_client()
    sub_name = args.sub_name
    try:
        reddit.subreddits.search_by_name(sub_name, exact=True)
    except NotFound:
        sys.exit("Subreddit does not exist")
    return None



def validate_args(args):
    sub_name = args.sub_name
    start_date = args.start_date
    end_date = args.end_date
    sub_name = sub_name.lower().strip()

    start_date_check = validate_date(start_date)
    end_date_check = validate_date(end_date)

    if len(sub_name) < 1:
        sys.exit('Sub-name must be greater than 0')
         
    if not start_date_check:
        sys.exit('Start date format wrong, please use proper format YYYY-MM-DD')
    if not end_date_check:
        sys.exit('End date format wrong, please use proper format YYYY')

def get_date_list(args):
    start_date = args.start_date
    end_date = args.end_date
    sd= dt.strptime(start_date, '%Y-%m-%d')
    ed = dt.strptime(end_date, '%Y-%m-%d')
    s = pd.date_range(sd,ed+timedelta(days=1),freq='d')
    li = []
    for i in s:
        li.append(i.strftime('%Y-%m-%d'))
    return li

