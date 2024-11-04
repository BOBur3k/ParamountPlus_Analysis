# collect_tv_mentions.py

import sys
import os
import praw
import pandas as pd
import logging

# Add the project root directory to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

import config  # Now this should work

# Ensure the logs directory exists
logs_dir = os.path.join(project_root, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    filename=os.path.join(logs_dir, 'reddit_collection.log'),
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def read_tv_show_list(csv_file):
    df = pd.read_csv(csv_file)
    tv_show_names = df['name'].dropna().unique().tolist()
    return tv_show_names

def search_reddit_for_tv_show(reddit, tv_show_name, subreddit_list, limit=1000):
    collected_posts = []
    query = f'"{tv_show_name}"'
    for subreddit_name in subreddit_list:
        subreddit = reddit.subreddit(subreddit_name)
        try:
            for submission in subreddit.search(query, limit=limit, syntax='lucene'):
                post_data = {
                    'id': submission.id,
                    'title': submission.title,
                    'selftext': submission.selftext,
                    'created_utc': submission.created_utc,
                    'subreddit': submission.subreddit.display_name,
                    'author': submission.author.name if submission.author else '[deleted]',
                    'score': submission.score,
                    'num_comments': submission.num_comments,
                    'url': submission.url,
                    'tv_show_name': tv_show_name
                }
                collected_posts.append(post_data)
        except praw.exceptions.APIException as e:
            logging.error(f"Reddit API exception for '{tv_show_name}' in r/{subreddit_name}: {e}")
        except Exception as e:
            logging.error(f"Error searching for '{tv_show_name}' in r/{subreddit_name}: {e}")
    return collected_posts

def main():
    # Paths
    tv_show_csv = os.path.join(project_root, 'data', 'raw', 'paramount_plus_tv_shows.csv')
    output_csv = os.path.join(project_root, 'data', 'raw', 'reddit_tv_show_mentions.csv')
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    
    # Subreddits to search
    subreddit_list = ['television', 'tvshows', 'netflix', 'Hulu', 'AmazonPrimeVideo', 'ParamountPlus']
    
    # Read TV show names
    tv_show_names = read_tv_show_list(tv_show_csv)
    logging.info(f"Total TV shows to search for: {len(tv_show_names)}")
    
    # Initialize Reddit instance
    reddit = praw.Reddit(
        client_id=config.CLIENT_ID,
        client_secret=config.CLIENT_SECRET,
        user_agent=config.USER_AGENT
    )
    
    all_collected_posts = []
    
    for index, tv_show_name in enumerate(tv_show_names):
        logging.info(f"Searching for '{tv_show_name}' ({index + 1}/{len(tv_show_names)})")
        collected_posts = search_reddit_for_tv_show(reddit, tv_show_name, subreddit_list)
        all_collected_posts.extend(collected_posts)
        logging.info(f"Collected {len(collected_posts)} posts for '{tv_show_name}'")
    
    if all_collected_posts:
        df = pd.DataFrame(all_collected_posts)
        df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
        df.to_csv(output_csv, index=False, encoding='utf-8')
        logging.info(f"Data saved to {output_csv}")
    else:
        logging.info("No posts collected.")

if __name__ == '__main__':
    main()
