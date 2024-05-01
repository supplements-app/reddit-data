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

# Unused
def write_to_jsonl_file(filename, key, data):
    if not os.path.exists(filename):
        open(filename, "w").close()  # Create an empty file if it doesn't exist

    with open(filename, "a") as f:
        f.write(json.dumps({key: data}) + "\n")
# --

def find_top_match(title, threshold=80):
    words = title.split()
    best_match = None
    best_ratio = 0

    for word in words:
        for supplement in supplements:
            supplement_names_to_match = [supplement] + supplement_aliases[supplement]
            for supplement_name_to_match in supplement_names_to_match:
            	ratio = fuzz.ratio(word, supplement_name_to_match)
                if ratio >= threshold and ratio > best_ratio:
                    best_match = supplement
                    best_ratio = ratio

    return best_match  # Can return None if no match found 

def processRow(row: dict[str, Any], i: int):
    # Initialize a dictionary to store the desired fields
    row_data = {}
    
	# skip if post is not about a supplement we know or there are no comments
    title = row.get("title")
    if title is not None:
        row_data["title"] = title
          
	# todo: store in any supplement that exceed ratio
    supplement = find_top_match(title)
    if not supplement:
        return
    
    num_comments = row.get("num_comments")
    if num_comments is not None and num_comments <= 1:
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
    write_to_gcs_bucket(f"{supplement}-posts.jsonl", i, row_data+"\n")
	
def write_to_gcs_bucket(filename, row_num, data):
    # Authenticate with Google Cloud Storage
    storage_client = storage.Client()

    file_path = f"misc_tests/posts/{filename}"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    if blob.exists():
        # Append to existing supplement file
        with blob.open(mode="a") as f:
            f.write(data.encode("utf-8"))
    else:
    	# Write new supplement file
        blob.upload_from_string(json.dumps({data}) + "\n", content_type="application/json")
    print(f"Data written to {file_path} in bucket {bucket_name}")
    
    last_processed_blob = bucket.blob(last_processed_filename)
    last_processed_row = str(row_num)
    last_processed_blob.upload_from_string(last_processed_row, content_type='text/plain')

def load_latest_processed_row_if_exists():
    # Authenticate with Google Cloud Storage
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    last_processed_blob = bucket.blob(last_processed_filename)

    if last_processed_blob.exists():
        contents = last_processed_blob.download_as_string().decode('utf-8')
        try:
            number = int(contents)  # Convert the downloaded string to an integer
            return number
        except ValueError:
            print(f"Error: Contents of {last_processed_filename} are not a valid integer.")
            return None

def processFile(path: str):
	jsonStream = getFileJsonStream(path)
	if jsonStream is None:
		logger.error(f"Skipping unknown file {path}")
		print(f"Skipping unknown file {path}")
		return
     
	last_processed_row = load_latest_processed_row_if_exists()
	for i, (lineLength, row) in enumerate(jsonStream):
		if i > 5:
			break
		if last_processed_row >= i:
			continue
		processRow(row, i)

def main():
	if os.path.isdir(filePath):
		logger.error("This script should operate on a .zst file, not a directory")
	else:
		processFile(filePath)
	
	print("Done :>")

if __name__ == "__main__":
	main()
