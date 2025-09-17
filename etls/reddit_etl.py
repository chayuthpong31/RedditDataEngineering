from praw import Reddit
import sys
from utils.constants import POST_FILEDS
import pandas as pd
import numpy as np

def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        print("Connected to Reddit")
        return reddit
    except Exception as e:
        print(f"Failed to connect to Reddit: {e}")
        sys.exit(1)

def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter:str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit) # Subreddit = Community
    posts = subreddit.top(time_filter=time_filter, limit=limit) # Submission = Post

    post_lists = []

    for post in posts:
        # tramsformation post object to dict
        post_dict = vars(post)
        # filter only required fields
        post = {key: post_dict[key] for key in POST_FILEDS}
        post_lists.append(post)

    return post_lists
    
def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s') # convert unix timestamp to utc 
    post_df['over_18'] = post_df['over_18'].astype(bool) # check bool 
    post_df['author'] = post_df['author'].astype(str)

    # if post is edited, it will have the timestamp of the edit converted to True, otherwise it will be False by default
    def convert_edited(value):
        if value is False:
            return False
        return True

    post_df['edited'] = post_df['edited'].apply(convert_edited)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['upvote_ratio'] = post_df['upvote_ratio'].astype(float)
    post_df['selftext'] = post_df['selftext'].astype(str)
    post_df['selftext'] = post_df['selftext'].replace('', np.nan) # replace empty string with NaN
    post_df['title'] = post_df['title'].astype(str)

    return post_df

def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)