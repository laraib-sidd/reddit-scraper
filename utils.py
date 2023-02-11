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
        client_id=os.environ.get("client_id"),
        client_secret=os.environ.get("client_secret"),
        password=os.environ.get("password"),
        user_agent=os.environ.get("user_agent"),
        username=os.environ.get("username"),
    )
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
        sys.exit("Sub-name must be greater than 0")

    if not start_date_check:
        sys.exit("Start date format wrong, please use proper format YYYY-MM-DD")
    if not end_date_check:
        sys.exit("End date format wrong, please use proper format YYYY")


def get_date_list(args):
    start_date = args.start_date
    end_date = args.end_date
    sd = dt.strptime(start_date, "%Y-%m-%d")
    ed = dt.strptime(end_date, "%Y-%m-%d")
    s = pd.date_range(sd, ed + timedelta(days=1), freq="d")
    li = []
    for i in s:
        li.append(i.strftime("%Y-%m-%d"))
    return li


def load_csv_posts_pmaw(date_range, args):
    sub_name = args.sub_name
    api = PushshiftAPI()
    for i in range(0, len(date_range) - 1):
        s = dt.strptime(date_range[i], "%Y-%m-%d")
        ddate = s.strftime("%Y-%m-%d")
        month = s.strftime("%B")

        sub_path = f"test/data/posts/{sub_name}"
        if not os.path.exists(sub_path):
            os.makedirs(sub_path)

        date_path = f"test/data/posts/{sub_name}/{month}"
        if not os.path.exists(date_path):
            os.makedirs(date_path)

        sd = int(dt.strptime(date_range[i], "%Y-%m-%d").timestamp())
        ed = int(dt.strptime(date_range[i + 1], "%Y-%m-%d").timestamp())

        posts = api.search_submissions(subreddit=sub_name, limit=3, before=ed, after=sd)
        print(f"Retrieved {len(posts)} Posts from Pushshift for {month} and {ddate}")
        li = []
        for post in posts:
            post["selftext"] = "".join(post["selftext"].splitlines())
            post["post_url"] = f"https://www.reddit.com{post['permalink']}"
            li.append(post)

        df = pd.DataFrame.from_dict(li, orient="columns")
        df.to_csv(f"test/data/posts/{sub_name}/{month}/{ddate}_posts.csv", index=False)


def load_mongo_posts_pmaw(date_range, client, args):
    api = PushshiftAPI()
    sub_name = args.sub_name

    mydb = client["test"]
    mycol = mydb["posts"]

    for i in range(0, len(date_range) - 1):
        s = dt.strptime(date_range[i], "%Y-%m-%d")
        ddate = s.strftime("%Y-%m-%d")
        month = s.strftime("%B")

        sd = int(dt.strptime(date_range[i], "%Y-%m-%d").timestamp())
        ed = int(dt.strptime(date_range[i + 1], "%Y-%m-%d").timestamp())

        posts = api.search_submissions(subreddit=sub_name, limit=3, before=ed, after=sd)
        print(f"Retrieved {len(posts)} posts from Pushshift for {month} and {ddate}")
        li = []
        for post in posts:
            post["selftext"] = "".join(post["selftext"].splitlines())
            post["post_url"] = f"https://www.reddit.com{post['permalink']}"
            li.append(post)
        mycol.insert_many(li)


def load_mongo_posts_api(date_range, client, args):
    sub_name = args.sub_name

    mydb = client["test"]
    mycol = mydb["posts"]

    for i in range(0, len(date_range) - 1):
        s = dt.strptime(date_range[i], "%Y-%m-%d")
        ddate = s.strftime("%Y-%m-%d")
        month = s.strftime("%B")

        sd = int(dt.strptime(date_range[i], "%Y-%m-%d").timestamp())
        ed = int(dt.strptime(date_range[i + 1], "%Y-%m-%d").timestamp())

        url = f"https://api.pushshift.io/reddit/search/submission/?subreddit={sub_name}&metadata=true&limit=3&since={sd}&until={ed}"
        response = urllib.request.urlopen(url)
        text = response.read()
        req_data = json.loads(text.decode("utf-8"))

        posts = req_data["data"]
        print(f"Retrieved {len(posts)} posts from Pushshift for {month} and {ddate}")
        li = []
        for post in posts:
            post["selftext"] = "".join(post["selftext"].splitlines())
            post["post_url"] = f"https://www.reddit.com{post['permalink']}"
            li.append(post)
            load_comments_mongo(post["id"], ddate, month, client)
        mycol.insert_many(li)


def load_csv_posts_api(date_range, args):
    sub_name = args.sub_name
    for i in range(0, len(date_range) - 1):
        s = dt.strptime(date_range[i], "%Y-%m-%d")
        ddate = s.strftime("%Y-%m-%d")
        month = s.strftime("%B")

        sub_path = f"test/data/posts/{sub_name}"
        if not os.path.exists(sub_path):
            os.makedirs(sub_path)

        date_path = f"test/data/posts/{sub_name}/{month}"
        if not os.path.exists(date_path):
            os.makedirs(date_path)

        sd = int(dt.strptime(date_range[i], "%Y-%m-%d").timestamp())
        ed = int(dt.strptime(date_range[i + 1], "%Y-%m-%d").timestamp())

        url = f"https://api.pushshift.io/reddit/search/submission/?subreddit={sub_name}&metadata=true&limit=3&since={sd}&until={ed}"
        response = urllib.request.urlopen(url)
        text = response.read()
        req_data = json.loads(text.decode("utf-8"))

        posts = req_data["data"]
        print(f"Retrieved {len(posts)} posts from Pushshift for {month} and {ddate}")
        li = []
        for post in posts:
            post["selftext"] = "".join(post["selftext"].splitlines())
            post["post_url"] = f"https://www.reddit.com{post['permalink']}"
            load_comments_csv(post["id"], sub_name, ddate, month)
            li.append(post)

        df = pd.DataFrame.from_dict(li, orient="columns")
        df.to_csv(f"test/data/posts/{sub_name}/{month}/{ddate}_posts.csv", index=False)


def load_comments_csv(pid, sub_name, ddate, month):

    sub_path = f"test/data/comments/{sub_name}"
    if not os.path.exists(sub_path):
        os.makedirs(sub_path)

    date_path = f"test/data/comments/{sub_name}/{month}"
    if not os.path.exists(date_path):
        os.makedirs(date_path)
    p_id_path = f"test/data/comments/{sub_name}/{month}/{ddate}/{pid}"
    if not os.path.exists(p_id_path):
        os.makedirs(p_id_path)

    base_10_id = int(pid, 36)
    url = f"https://api.pushshift.io/reddit/search/comment?link_id={base_10_id}&limit=3"
    response = urllib.request.urlopen(url)
    text = response.read()
    req_data = json.loads(text.decode("utf-8"))

    comments = req_data["data"]
    print(
        f"Retrieved {len(comments)} comments from Pushshift for {month} and {ddate} and{pid}"
    )
    if len(comments) == 0:
        return
    li = []
    for comment in comments:
        comment["post_id"] = pid
        comment["body"] = "".join(comment["body"].splitlines())
        comment["comment_url"] = f"https://www.reddit.com{comment['permalink']}"
        li.append(comment)

    df = pd.DataFrame.from_dict(li, orient="columns")
    df.to_csv(
        f"test/data/comments/{sub_name}/{month}/{ddate}/{pid}/comments.csv", index=False
    )


def load_comments_mongo(pid, ddate, month, client):
    mydb = client["test"]
    mycol = mydb["comments"]

    base_10_id = int(pid, 36)
    url = f"https://api.pushshift.io/reddit/search/comment?link_id={base_10_id}&limit=1000"
    response = urllib.request.urlopen(url)
    text = response.read()
    req_data = json.loads(text.decode("utf-8"))

    comments = req_data["data"]
    print(
        f"Retrieved {len(comments)} comments from Pushshift for {month} and {ddate} and{pid}"
    )
    if len(comments) == 0:
        return
    li = []
    for comment in comments:
        comment["post_id"] = pid
        comment["body"] = "".join(comment["body"].splitlines())
        comment["comment_url"] = f"https://www.reddit.com{comment['permalink']}"
        li.append(comment)

    mycol.insert_many(li)
