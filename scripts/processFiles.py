import sys
import logging
import json
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")
import os
import time
import random
from collections import defaultdict
from typing import Any, Iterable
from google.cloud import storage
from supplements_list import supplements, supplement_aliases
from fileStreams import getFileJsonStream
from thefuzz import fuzz
from logging.handlers import RotatingFileHandler


filePath = "/Users/ronit/Desktop/projects/arctic_shift/raw_reddit_dumps_Supplements_submissions.zst"
recursive = False

# cloud storage config
bucket_name = 'supplements_app_storage'
last_processed_filename = 'filtered_raw_posts/last_processed.txt'

# logger config
logger = logging.getLogger('processFiles')
logger.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler('processingPosts.log', maxBytes=1024*1024*5, backupCount=5)
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def exponential_backoff(retries=5, base_sleep_time=0.1, max_sleep_time=5):
    """Utility function to retry operations with exponential backoff."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    sleep_time = min(max_sleep_time, base_sleep_time * 2 ** i)
                    time.sleep(sleep_time + random.uniform(0, 0.1))  # Adding jitter
                    if i == retries - 1:
                        raise
                    logger.error(f"Retry {i + 1} for function {func.__name__} due to error: {e}")
        return wrapper
    return decorator

def find_top_match(title, selftext, threshold=80):
    texts = [title, selftext]
    top_match = None
    best_ratio = 0

    for text in texts:
        if text:
            words = text.split()
            for word in words:
                for supplement in supplements:
                    supplement_names_to_match = [supplement] + supplement_aliases.get(supplement, [])
                    for supplement_name_to_match in supplement_names_to_match:
                        ratio = fuzz.ratio(word, supplement_name_to_match)
                        if ratio >= threshold and ratio >= best_ratio: 
                            top_match = supplement
                            best_ratio = ratio
    return top_match # Returns a list of matching supplements

def processRow(row: dict[str, Any], i: int):
    logger.debug(f"Processing row {i}")
    row_data = {}
    title = row.get("title")
    if title is not None:
        row_data["title"] = title

    selftext = row.get("selftext")
    if selftext is not None:
        row_data["selftext"] = selftext

    found_supplement = find_top_match(title, selftext)

    if not found_supplement:
        logger.debug(f"No supplement match found for row {i}")
        return

    num_comments = row.get("num_comments")
    if num_comments is not None and num_comments <= 1:
        logger.debug(f"Skipping row {i} due to insufficient comments")
        return

    author = row.get("author")
    if author is not None:
        row_data["author"] = author

    created_utc = row.get("created_utc")
    if created_utc is not None:
        row_data["created_utc"] = created_utc

    post_id = row.get("id")
    if post_id is not None:
        row_data["id"] = post_id

    permalink = row.get("permalink")
    if permalink is not None:
        row_data["permalink"] = permalink

    subreddit_name_prefixed = row.get("subreddit_name_prefixed")
    if subreddit_name_prefixed is not None:
        row_data["subreddit_name_prefixed"] = subreddit_name_prefixed

    subreddit_id = row.get("subreddit_id")
    if subreddit_id is not None:
        row_data["subreddit_id"] = subreddit_id

    write_to_gcs_bucket(subreddit_id, found_supplement, post_id, row_data, i)
    logger.debug(f"Row {i} data written to cloud storage for supplement {found_supplement}")

@exponential_backoff()
def write_to_gcs_bucket(subreddit_id, supplement, post_id, data, last_processed_row):
    storage_client = storage.Client()
    folder_name = f"{subreddit_id}-{supplement}-posts"
    file_name = f"{post_id}.json"
    file_path = f"filtered_raw_posts/{folder_name}/{file_name}"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    # Upload the data as a JSON string
    blob.upload_from_string(json.dumps(data) + "\n", content_type="application/json")
    logger.debug(f"Data for post {post_id} written to {file_path} in bucket {bucket_name}")
    
	# Write the last processed row
    if last_processed_row % 500:
        last_processed_blob = bucket.blob(last_processed_filename)
        last_processed_blob.upload_from_string(str(last_processed_row), content_type='text/plain')
        logger.debug(f"Updated last processed row to {last_processed_row}")

@exponential_backoff()
def load_latest_processed_row_if_exists():
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    last_processed_blob = bucket.blob(last_processed_filename)

    if last_processed_blob.exists():
        contents = last_processed_blob.download_as_string().decode('utf-8')
        try:
            number = int(contents)
            logger.debug(f"Loaded last processed row: {number}")
            return number
        except ValueError:
            logger.error(f"Error: Contents of {last_processed_filename} are not a valid integer.")
            return None
    else:
        logger.debug("No last processed row found, starting from the beginning.")
        return None

def processFile(path: str):
    jsonStream = getFileJsonStream(path)
    if jsonStream is None:
        logger.error(f"Skipping unknown file {path}")
        return

    last_processed_row = load_latest_processed_row_if_exists()
    for i, (lineLength, row) in enumerate(jsonStream):
        if last_processed_row is not None and i <= last_processed_row:
            logger.debug(f"Skipping row {i} as it has already been processed")
            continue
        logger.info(f"Processing row {i}")
        processRow(row, i)
        time.sleep(0.1)
        if i % 100 == 0:
            logger.info(f"Processed up to row {i}")

def main():
    global filePath
    # get filename from command line parameter
    if len(sys.argv) < 1:
        logger.error("No file path provided")
        sys.exit(1)       
    filePath = sys.argv[1]
    
    if os.path.isdir(filePath):
        logger.error("This script should operate on a .zst file, not a directory")
        return
    else:
        logger.info("Starting file processing")
        processFile(filePath)
        logger.info("File processing completed")
        
        
if __name__ == "__main__":
    main()

