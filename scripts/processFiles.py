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
from supplements_list import supplements
from fileStreams import getFileJsonStream
from thefuzz import fuzz

filePath = "/Users/ronit/Desktop/projects/arctic_shift/raw_reddit_dumps_Supplements_submissions.zst"
recursive = False

# cloud storage config
bucket_name = 'supplements_app_storage'
output_blob_name = 'misc_tests/posts/output.jsonl'

# logger config
logger = logging.getLogger('processFiles')
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('processFiles.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def find_top_match(title, threshold=80):
    words = title.split()
    best_match = None
    best_ratio = 0

    for word in words:
        for supplement in supplements:
            ratio = fuzz.ratio(word, supplement)
            if ratio >= threshold and ratio > best_ratio:
                best_match = supplement
                best_ratio = ratio

    return best_match  # Can return None if no match found 

def write_to_jsonl_file(filename, key, data):
    if not os.path.exists(filename):
        open(filename, "w").close()  # Create an empty file if it doesn't exist

    with open(filename, "a") as f:
        f.write(json.dumps({key: data}) + "\n")

def processRow(row: dict[str, Any], i: int):
    # Initialize a dictionary to store the desired fields
    row_data = {}
    
	# skip if post is not about a supplement we know or there are no comments
    title = row.get("title")
    if title is not None:
        row_data["title"] = title
          
    supplement = find_top_match(title)
    if not supplement:
        return
    
    num_comments = row.get("num_comments")
    if num_comments is not None and num_comments == 0:
        return

    # Extract desired fields and add them to the dictionary if they exist
    author = row.get("author")
    if author is not None:
        row_data["author"] = author

    created_utc = row.get("created_utc")
    if created_utc is not None:
        row_data["created_utc"] = created_utc

    selftext = row.get("selftext")
    if selftext is not None:
        row_data["selftext"] = selftext

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

    # Write the row data to the JSONL file in cloud storage
    write_to_gcs_bucket(f"{supplement}-posts.jsonl", submission_id, row_data)
	
def write_to_gcs_bucket(filename, key, data):
    # Authenticate with Google Cloud Storage
    storage_client = storage.Client()

    # Set the bucket name and file path
    file_path = f"misc_tests/posts/{filename}"

    # Get the bucket and create a new blob (file) object
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    # Append the data to the file in the bucket
    blob.upload_from_string(json.dumps({key: data}) + "\n", content_type="application/json")
    print(f"Data written to {file_path} in bucket {bucket_name}")

def processFile(path: str):
	jsonStream = getFileJsonStream(path)
	if jsonStream is None:
		logger.error(f"Skipping unknown file {path}")
		print(f"Skipping unknown file {path}")
		return
	for i, (lineLength, row) in enumerate(jsonStream):
		if i > 5:
			break
		processRow(row, i)

def main():
	if os.path.isdir(filePath):
		logger.error("This script should operate on a .zst file, not a directory")
	else:
		processFile(filePath)
	
	print("Done :>")

if __name__ == "__main__":
	main()
