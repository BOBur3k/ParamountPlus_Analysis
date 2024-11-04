# collect_tv_comments.py

import sys
import os
import praw
import prawcore
import pandas as pd
import csv
import time
import random
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
    filename=os.path.join(logs_dir, 'reddit_comments.log'),
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def read_submission_ids(submissions_csv):
    df_submissions = pd.read_csv(submissions_csv)
    submission_ids = df_submissions['id'].unique().tolist()
    return submission_ids

def read_existing_comments(comments_csv):
    if os.path.exists(comments_csv):
        df_comments = pd.read_csv(comments_csv)
        fetched_submission_ids = df_comments['submission_id'].unique().tolist()
    else:
        df_comments = pd.DataFrame()
        fetched_submission_ids = []
    return df_comments, fetched_submission_ids

def identify_missing_submissions(all_submission_ids, fetched_submission_ids):
    missing_submission_ids = list(set(all_submission_ids) - set(fetched_submission_ids))
    return missing_submission_ids

def wait_if_needed(request_tracker):
    current_time = time.time()
    elapsed_time = current_time - request_tracker['start_time']

    # Reset the tracker every 60 seconds
    if elapsed_time > 60:
        request_tracker['count'] = 0
        request_tracker['start_time'] = current_time
        logging.info("Request tracker reset.")
    elif request_tracker['count'] >= request_tracker['threshold']:
        wait_time = 60 - elapsed_time + random.uniform(5, 10)  # Adding random buffer
        logging.info(f"Approaching rate limit. Waiting for {wait_time:.2f} seconds...")
        time.sleep(wait_time)
        request_tracker['count'] = 0
        request_tracker['start_time'] = time.time()

def exponential_backoff(retries):
    max_sleep = min(600, (2 ** retries) + random.uniform(0, 1))
    logging.warning(f"Waiting for {max_sleep:.2f} seconds before retrying...")
    time.sleep(max_sleep)

def fetch_comments_for_submission(reddit, submission_id, request_tracker):
    comments_data = []
    retries = 0
    max_retries = 5
    while retries < max_retries:
        try:
            # Rate limit handling
            wait_if_needed(request_tracker)

            submission = reddit.submission(id=submission_id)
            request_tracker['count'] += 1

            # Fetch all comments (including 'more comments')
            submission.comments.replace_more(limit=None)
            request_tracker['count'] += 1  # Account for the replace_more call

            for comment in submission.comments.list():
                comment_data = process_comment(comment, submission_id)
                comments_data.append(comment_data)

            logging.info(f"Successfully fetched comments for submission ID {submission_id}")
            break  # Exit the retry loop if successful
        except prawcore.exceptions.TooManyRequests as e:
            logging.error(f"Received 429 Too Many Requests for submission ID {submission_id}: {e}")
            retry_after = int(e.response.headers.get('retry-after', 60))
            logging.info(f"Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            # Reset the request tracker
            request_tracker['count'] = 0
            request_tracker['start_time'] = time.time()
            retries += 1
        except Exception as e:
            logging.error(f"Error fetching comments for submission ID {submission_id}: {e}")
            retries += 1
            exponential_backoff(retries)
    else:
        # If maximum retries reached, log the submission ID
        failed_submissions_file = os.path.join(logs_dir, 'failed_submissions.csv')
        with open(failed_submissions_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([submission_id, "Max retries exceeded"])
        logging.error(f"Max retries exceeded for submission ID {submission_id}")
    return comments_data

def process_comment(comment, submission_id):
    return {
        'submission_id': submission_id,
        'comment_id': comment.id,
        'parent_id': comment.parent_id,
        'body': comment.body,
        'author': comment.author.name if comment.author else '[deleted]',
        'created_utc': comment.created_utc,
        'score': comment.score,
        'is_submitter': comment.is_submitter
    }

def main():
    # Paths to your CSV files
    submissions_csv = os.path.join(project_root, 'data', 'raw', 'reddit_tv_show_mentions.csv')
    comments_csv = os.path.join(project_root, 'data', 'raw', 'reddit_comments.csv')
    os.makedirs(os.path.dirname(comments_csv), exist_ok=True)

    # Ensure logs directory exists
    os.makedirs(logs_dir, exist_ok=True)
    failed_submissions_file = os.path.join(logs_dir, 'failed_submissions.csv')

    # Read submission IDs
    all_submission_ids = read_submission_ids(submissions_csv)
    logging.info(f"Total submissions: {len(all_submission_ids)}")

    # Read existing comments data
    df_comments, fetched_submission_ids = read_existing_comments(comments_csv)
    logging.info(f"Submissions with comments already fetched: {len(fetched_submission_ids)}")

    # Identify missing submissions
    missing_submission_ids = identify_missing_submissions(all_submission_ids, fetched_submission_ids)
    logging.info(f"Submissions missing comments: {len(missing_submission_ids)}")

    if not missing_submission_ids:
        logging.info("No missing submissions found. All comments have been fetched.")
        print("No missing submissions found. All comments have been fetched.")
        return

    # Initialize Reddit instance
    reddit = praw.Reddit(
        client_id=config.CLIENT_ID,
        client_secret=config.CLIENT_SECRET,
        user_agent=config.USER_AGENT
    )

    # Initialize a request tracker for rate limiting
    request_tracker = {
        'count': 0,
        'start_time': time.time(),
        'threshold': 30  # Adjust the threshold as needed
    }

    # Initialize a list to store new comments
    new_comments = []

    # Iterate over missing submission IDs
    for index, submission_id in enumerate(missing_submission_ids):
        logging.info(f"Processing submission ID {submission_id} ({index + 1}/{len(missing_submission_ids)})")
        print(f"Fetching comments for submission ID {submission_id} ({index + 1}/{len(missing_submission_ids)})")
        comments = fetch_comments_for_submission(reddit, submission_id, request_tracker)
        new_comments.extend(comments)
        logging.info(f"Collected {len(comments)} comments from submission ID {submission_id}")
        print(f"Collected {len(comments)} comments from submission ID {submission_id}")
        # Respect Reddit's rate limits by adding a randomized delay
        sleep_time = random.uniform(5, 10)
        logging.info(f"Sleeping for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

    # Save the new comments to the comments CSV file
    if new_comments:
        df_new_comments = pd.DataFrame(new_comments)
        df_new_comments['created_utc'] = pd.to_datetime(df_new_comments['created_utc'], unit='s')
        # Append new comments to existing comments CSV
        if os.path.exists(comments_csv):
            df_new_comments.to_csv(comments_csv, mode='a', header=False, index=False, encoding='utf-8')
        else:
            df_new_comments.to_csv(comments_csv, index=False, encoding='utf-8')
        logging.info(f"New comments data saved to {comments_csv}")
        print(f"New comments data saved to {comments_csv}")
    else:
        logging.info("No new comments collected.")
        print("No new comments collected.")

if __name__ == '__main__':
    main()
