import time
from datetime import datetime as dt
import argparse
import os
from utils import * 

import sys
import json
import urllib.request


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument("-n",
    #                     "--name",
    #                     type=str,
    #                     help="subredit_name",
    #                     dest="sub_name",
    #                     required=True)
    # parser.add_argument("-sd", "--start-date", type=str,
    #                     help="start_date format YYYY/MM/DD example : 2021-02-01", dest="start_date", required=True)
    # parser.add_argument("-ed", "--end-date", type=str,
    #                     help="end_date format YYYY/MM/DD example : 2022-04-01", dest="end_date", required=True)
    # parser.add_argument("-o", "--output", type=str,help="output file, options 1)mongo 2)csv 3)dual", dest="output", required=True)
    # args = parser.parse_args()
    
    # validate_args(args)

    # sub_exists(args)
    class Object(object):
        pass

    temp_args = Object()
    temp_args.sub_name = "btc"
    temp_args.start_date = "2022-09-01"
    temp_args.end_date = "2022-09-06"
    temp_args.output = "csv"
    date_list = get_date_list(temp_args)
    for i in range(0,len(date_list)-1):
        print(date_list[i],date_list[i+1])



import datetime
start_time = datetime.datetime.now()

# Validate Arguments

before = int(dt(2022, 11, 5, 0, 0).timestamp())
after = int(dt(2022, 11, 4, 0, 0).timestamp())


url = f"https://api.pushshift.io/reddit/search/submission/?subreddit=bitcoin&metadata=true&limit=3&since={after}&until={before}"
response = urllib.request.urlopen(url)
text = response.read()
req_data = json.loads(text.decode('utf-8'))

posts = req_data['data']
li = []
for i in posts:
    d = dict()
    d['subreddit'] = i['subreddit']
    d['selftext'] = "".join(i['selftext'].splitlines())
    d['author_fullname'] = i['author_fullname']
    d['title'] = i['title']
    d['total_awards_received'] = i['total_awards_received']
    d['score'] = i['score']
    d['removed_by_category'] = i['removed_by_category']
    d['over_18'] = i['over_18']
    d['media_only'] = i['media_only']
    d['locked'] = i['locked']
    d['subreddit_id'] = i['subreddit_id']
    d['post_id'] = i['id']
    d['author'] = i['author']
    d['num_comments'] = i['num_comments']
    d['post_url'] = f"https://www.reddit.com/{i['permalink']}"
    d['num_crossposts'] = i['num_crossposts']
    d['post_date'] = i['utc_datetime_str'] 
    d['created_utc'] = i['created_utc']
    # print(d)
    li.append(d)
print(len(li))
end_time = datetime.datetime.now()
print(end_time - start_time)

# print(req_data['metadata']['total'])
# for i in range(len(posts)):
#     posts_list.append(posts[i]['title'])
#     date_list.append(posts[i]['utc_datetime_str'])


if __name__ == "__main__":
    pass
    # main()