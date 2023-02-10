import praw
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from datetime import timedelta, datetime as dt
import sys
import pandas as pd
from prawcore import NotFound
from pmaw import PushshiftAPI
import json
import urllib.request


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
    sd = dt.strptime(start_date, '%Y-%m-%d')
    ed = dt.strptime(end_date, '%Y-%m-%d')
    s = pd.date_range(sd, ed+timedelta(days=1), freq='d')
    li = []
    for i in s:
        li.append(i.strftime('%Y-%m-%d'))
    return li


def load_csv_posts_pmaw(date_range, args):
    sub_name = args.sub_name
    api = PushshiftAPI()
    for i in range(0, len(date_range)-1):
        s = dt.strptime(date_range[i], '%Y-%m-%d')
        ddate = s.strftime('%Y-%m-%d')
        month = s.strftime('%B')

        sub_path = f"test/data/posts/{sub_name}"
        if not os.path.exists(sub_path):
            os.makedirs(sub_path)

        date_path = f"test/data/posts/{sub_name}/{month}"
        if not os.path.exists(date_path):
            os.makedirs(date_path)

        sd = int(dt.strptime(date_range[i], '%Y-%m-%d').timestamp())
        ed = int(dt.strptime(date_range[i+1], '%Y-%m-%d').timestamp())

        posts = api.search_submissions(
            subreddit=sub_name, limit=3, before=ed, after=sd)
        print(
            f'Retrieved {len(posts)} comments from Pushshift for {month} and {ddate}')
        li = []
        for i in posts:
            posts_dict = dict()
            posts_dict['subreddit'] = i['subreddit']
            posts_dict['selftext'] = "".join(i['selftext'].splitlines())
            posts_dict['author_fullname'] = i['author_fullname']
            posts_dict['title'] = i['title']
            posts_dict['total_awards_received'] = i['total_awards_received']
            posts_dict['score'] = i['score']
            posts_dict['removed_by_category'] = i['removed_by_category']
            posts_dict['over_18'] = i['over_18']
            posts_dict['media_only'] = i['media_only']
            posts_dict['locked'] = i['locked']
            posts_dict['subreddit_id'] = i['subreddit_id']
            posts_dict['post_id'] = i['id']
            posts_dict['author'] = i['author']
            posts_dict['num_comments'] = i['num_comments']
            posts_dict['post_url'] = f"https://www.reddit.com/{i['permalink']}"
            posts_dict['num_crossposts'] = i['num_crossposts']
            posts_dict['post_date'] = i['utc_datetime_str']
            posts_dict['created_utc'] = i['created_utc']
            # print(d)
            li.append(posts_dict)

        df = pd.DataFrame.from_dict(li, orient='columns')
        df.to_csv(
            f"test/data/posts/{sub_name}/{month}/{ddate}_posts.csv", index=False)


def load_mongo_posts_pmaw(date_range, client, args):
    api = PushshiftAPI()
    sub_name = args.sub_name

    mydb = client["test"]
    mycol = mydb["posts"]

    for i in range(0, len(date_range)-1):
        s = dt.strptime(date_range[i], '%Y-%m-%d')
        ddate = s.strftime('%Y-%m-%d')
        month = s.strftime('%B')

        sd = int(dt.strptime(date_range[i], '%Y-%m-%d').timestamp())
        ed = int(dt.strptime(date_range[i+1], '%Y-%m-%d').timestamp())

        posts = api.search_submissions(
            subreddit=sub_name, limit=3, before=ed, after=sd)
        print(
            f'Retrieved {len(posts)} comments from Pushshift for {month} and {ddate}')
        li = []
        for i in posts:
            posts_dict = dict()
            posts_dict['subreddit'] = i['subreddit']
            posts_dict['selftext'] = "".join(i['selftext'].splitlines())
            posts_dict['author_fullname'] = i['author_fullname']
            posts_dict['title'] = i['title']
            posts_dict['total_awards_received'] = i['total_awards_received']
            posts_dict['score'] = i['score']
            posts_dict['removed_by_category'] = i['removed_by_category']
            posts_dict['over_18'] = i['over_18']
            posts_dict['media_only'] = i['media_only']
            posts_dict['locked'] = i['locked']
            posts_dict['subreddit_id'] = i['subreddit_id']
            posts_dict['post_id'] = i['id']
            posts_dict['author'] = i['author']
            posts_dict['num_comments'] = i['num_comments']
            posts_dict['post_url'] = f"https://www.reddit.com/{i['permalink']}"
            posts_dict['num_crossposts'] = i['num_crossposts']
            posts_dict['post_date'] = i['utc_datetime_str']
            posts_dict['created_utc'] = i['created_utc']
            # print(d)
            li.append(posts_dict)
        mycol.insert_many(li)


def load_mongo_posts_api(date_range, client, args):
    api = PushshiftAPI()
    sub_name = args.sub_name

    mydb = client["test"]
    mycol = mydb["posts"]

    for i in range(0, len(date_range)-1):
        s = dt.strptime(date_range[i], '%Y-%m-%d')
        ddate = s.strftime('%Y-%m-%d')
        month = s.strftime('%B')

        sd = int(dt.strptime(date_range[i], '%Y-%m-%d').timestamp())
        ed = int(dt.strptime(date_range[i+1], '%Y-%m-%d').timestamp())

        url = f"https://api.pushshift.io/reddit/search/submission/?subreddit=bitcoin&metadata=true&limit=3&since={sd}&until={ed}"
        response = urllib.request.urlopen(url)
        text = response.read()
        req_data = json.loads(text.decode('utf-8'))

        posts = req_data['data']
        print(
            f'Retrieved {len(posts)} comments from Pushshift for {month} and {ddate}')
        li = []
        for i in posts:
            posts_dict = dict()
            posts_dict['subreddit'] = i['subreddit']
            posts_dict['selftext'] = "".join(i['selftext'].splitlines())
            posts_dict['author_fullname'] = i['author_fullname']
            posts_dict['title'] = i['title']
            posts_dict['total_awards_received'] = i['total_awards_received']
            posts_dict['score'] = i['score']
            posts_dict['removed_by_category'] = i['removed_by_category']
            posts_dict['over_18'] = i['over_18']
            posts_dict['media_only'] = i['media_only']
            posts_dict['locked'] = i['locked']
            posts_dict['subreddit_id'] = i['subreddit_id']
            posts_dict['post_id'] = i['id']
            posts_dict['author'] = i['author']
            posts_dict['num_comments'] = i['num_comments']
            posts_dict['post_url'] = f"https://www.reddit.com/{i['permalink']}"
            posts_dict['num_crossposts'] = i['num_crossposts']
            posts_dict['post_date'] = i['utc_datetime_str']
            posts_dict['created_utc'] = i['created_utc']
            # print(d)
            li.append(posts_dict)
        mycol.insert_many(li)

