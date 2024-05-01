import sys
import logging
import json
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")
import os
from collections import defaultdict
from typing import Any, Iterable
from google.cloud import storage
from supplements_list import supplements, supplement_aliases
from fileStreams import getFileJsonStream
from thefuzz import fuzz

filePath = "/Users/ronit/Desktop/projects/arctic_shift/raw_reddit_dumps_Supplements_submissions.zst"
recursive = False

# cloud storage config
bucket_name = 'supplements_app_storage'
last_processed_filename = 'misc_tests/posts/last_processed.txt'

# logger config
logger = logging.getLogger('processFiles')
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('processFiles.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def find_top_matches(title, selftext, threshold=80):
    texts = [title, selftext]
    matches = set()

    for text in texts:
        if text:
            words = text.split()
            for word in words:
                for supplement in supplements:
                    supplement_names_to_match = [supplement] + supplement_aliases[supplement]
                    for supplement_name_to_match in supplement_names_to_match:
                        ratio = fuzz.ratio(word, supplement_name_to_match)
                        if ratio >= threshold:
                            matches.add(supplement)
    return list(matches)  # Returns a list of matching supplements

def processRow(row: dict[str, Any], i: int):
    logger.debug(f"Processing row {i}")
    row_data = {}
    title = row.get("title")
    if title is not None:
        row_data["title"] = title

    selftext = row.get("selftext")
    if selftext is not None:
        row_data["selftext"] = selftext

    supplements = find_top_matches(title, selftext)

    if not supplements:
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

    submission_id = row.get("id")
    if submission_id is not None:
        row_data["id"] = submission_id

    permalink = row.get("permalink")
    if permalink is not None:
        row_data["permalink"] = permalink

    subreddit_name_prefixed = row.get("subreddit_name_prefixed")
    if subreddit_name_prefixed is not None:
        row_data["subreddit_name_prefixed"] = subreddit_name_prefixed

    subreddit_id = row.get("subreddit_id")
    if subreddit_id is not None:
        row_data["subreddit_id"] = subreddit_id

    for supplement in supplements:
        write_to_gcs_bucket(f"{subreddit_id}-{supplement}-posts.jsonl", i, row_data)
        logger.debug(f"Row {i} data written to cloud storage for supplement {supplement}")

def write_to_gcs_bucket(filename, row_num, data):
    storage_client = storage.Client()
    file_path = f"misc_tests/posts/{filename}"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    if blob.exists():
        with blob.open(mode="a") as f:
            f.write(json.dumps(data).encode("utf-8"))
    else:
        blob.upload_from_string(json.dumps(data) + "\n", content_type="application/json")
    logger.debug(f"Data for row {row_num} written to {file_path} in bucket {bucket_name}")
    
    last_processed_blob = bucket.blob(last_processed_filename)
    last_processed_blob.upload_from_string(str(row_num), content_type='text/plain')
    logger.debug(f"Updated last processed row to {row_num}")

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
        if i % 100 == 0:
            logger.info(f"Processed up to row {i}")

def main():
    if os.path.isdir(filePath):
        logger.error("This script should operate on a .zst file, not a directory")
        return
    else:
        logger.info("Starting file processing")
        processFile(filePath)
        logger.info("File processing completed")
        
        
if __name__ == "__main__":
    main()

