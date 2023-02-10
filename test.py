import pandas as pd
from pmaw import PushshiftAPI
from datetime import datetime
import sys
import os

api = PushshiftAPI()

# mydb = client["test"]
# mycol = mydb["posts"]
before = int(datetime(2022, 11, 5, 0, 0).timestamp())
after = int(datetime(2022, 11, 4, 0, 0).timestamp())
sub_name = 'bitcoin'


import datetime
start_time = datetime.datetime.now()

posts = api.search_submissions(subreddit=sub_name, limit=3,before=before, after=after)
print(f'Retrieved {len(posts)} comments from Pushshift')
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

# print(li)
    # sys.exit(0)
# mycol.insert_many(li)

# df = pd.DataFrame.from_dict(li, orient='columns')
# df.to_csv('results.csv',index=False)

# # Check subredit path
# folder_path = f"test/data/{sub_name}"
# if not os.path.exists(folder_path):
#     os.makedirs(folder_path)
    


# start_date = '2022-11-05'
# end_date = '2022-11-10'
# a = get_date_list(start_date,end_date)

# for i in range(0,len(a)-1):
#     # print(a[i],a[i+1])
#     s = datetime.strptime(a[i], '%Y-%m-%d')
#     d = s.strftime('%Y-%m-%d')
#     month = s.strftime('%B') 
#     date_path = f"test/data/{sub_name}/{month}"
#     if not os.path.exists(date_path):
#         os.makedirs(date_path)
#     sd = int(datetime.strptime(a[i],'%Y-%m-%d').timestamp())
#     ed = int(datetime.strptime(a[i+1],'%Y-%m-%d').timestamp())
#     posts = api.search_submissions(subreddit="bitcoin", limit=3,before=ed, after=sd)
#     print(f'Retrieved {len(posts)} comments from Pushshift for {month} and {d}')
#     li = []
#     for i in posts:
#         li.append(i)
#         # sys.exit(0)
#     df = pd.DataFrame.from_dict(li, orient='columns')
#     df.to_csv(f"test/data/{sub_name}/{month}/{d}_posts.csv",index=False)

    
