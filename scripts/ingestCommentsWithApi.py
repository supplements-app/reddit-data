from http.client import RemoteDisconnected
import json
import requests
import logging
from google.cloud import storage
import time
from requests.exceptions import RequestException
from logging.handlers import RotatingFileHandler


# Setup logging
logger = logging.getLogger('ingestCommentsWithApi')
logger.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler('ingestingComments.log', maxBytes=1024*1024*5, backupCount=5)
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

BASE_URL = 'https://arctic-shift.photon-reddit.com'

def fetch_comments(link_id):
    """Fetch comments from the API with retries and exponential backoff."""
    url = f"{BASE_URL}/api/comments/tree?link_id={link_id}&limit=9999"
    retries = 3
    backoff_factor = 2
    delay = 1  # Initial delay of 1 second

    for attempt in range(retries):
        try:
            time.sleep(delay)  # Delay between retries
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP error responses
            return response.json()
        except RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            delay *= backoff_factor
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON from response.")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            break

    logger.error("API request failed after retries.")
    return None  # Return None or handle it as per your application's error handling policy

def extract_relevant_data(comment):
    """Extract relevant fields from a comment."""
    fields = ['author', 'id', 'body', 'created_utc', 'subreddit_id', 'subreddit', 'controversiality', 'link_id', 'score']
    return {field: comment[field] for field in fields if field in comment}

def process_comments(comments):
    """Recursively process comments to extract relevant data and replies."""
    processed = []
    for comment in comments:
        processed_comment = extract_relevant_data(comment['data'])
        if processed_comment['body'] == '[deleted]':
            continue
        processed.append(processed_comment)
    return processed

def write_to_jsonl(bucket_name, output_folder, post_id, data):
    """Write processed data to a JSONL file in GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"filtered_raw_comments-v1/{output_folder}/{post_id}.jsonl")
    output_lines = [json.dumps(comment) for comment in data]
    blob.upload_from_string("\n".join(output_lines))
    logger.info(f"Data written to /filtered_raw_comments-v1{output_folder}/{post_id}.jsonl")


def write_to_jsonl(bucket_name, output_folder, post_id, data):
    """Write processed data to a JSONL file in GCS with retries."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"filtered_raw_comments-v1/{output_folder}/{post_id}.jsonl")
    output_lines = [json.dumps(comment) for comment in data]

    retries = 3
    backoff_factor = 2
    delay = 1  # Initial delay

    for attempt in range(retries):
        try: 
            blob.upload_from_string("\n".join(output_lines), content_type='text/plain')
            logger.info(f"Data written to /filtered_raw_comments-v1/{output_folder}/{post_id}.jsonl")
            break  # Success!
        except (ConnectionError, RemoteDisconnected) as e:  # Catch relevant errors 
            logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:  # Only retry if attempts remain
                delay *= backoff_factor
                time.sleep(delay)             
            else:
                logger.error(f"Upload failed after retries: /filtered_raw_comments-v1/{output_folder}/{post_id}.jsonl")



def process_folder(bucket_name, folder_name, processed_file_path, cur_folder_num, total_folders):
    """Process each JSON file in the folder."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=folder_name))
    folder_name_end = folder_name.split('/')[-1]
    output_folder = folder_name_end.replace('-posts', '-aggregated-comments')
    num_blobs = len(blobs)
    for index, blob in enumerate(blobs):
        if blob.name.endswith('.json'):
            post_id = blob.name.split('/')[-1].split('.')[0]
            logger.info(f"Processing post ID: {post_id}")
            comments = fetch_comments(post_id)
            if comments is None:
                logger.error(f"Failed to fetch comments for post ID: {post_id}")
                continue
            processed_comments = process_comments(comments['data'])
            write_to_jsonl(bucket_name, output_folder, post_id, processed_comments)
            logger.info(f"Processed {index} of {num_blobs} posts in folder {cur_folder_num} of {total_folders}")

def main(bucket_name, base_folder):
    """Main function to process all subfolders in the base folder."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=base_folder)
    folders = set()
    for blob in blobs:
        parts = blob.name.split('/')
        if len(parts) > 1 and '-posts' in parts[-2]:
            folders.add('/'.join(parts[:-1]))
    num_folders = len(folders)
    processed_file_path = 'filtered_raw_comments-v1/processed.txt'
    processed_blob = bucket.blob(processed_file_path)
    try:
        processed_folders = {line.decode('utf-8').strip() for line in processed_blob.download_as_bytes().splitlines()}
    except:
        processed_blob.upload_from_string("")
        processed_folders = set()

    for index, folder_name in enumerate(folders):
        if folder_name not in processed_folders:
            logger.info(f"Processing folder {index}: {folder_name}")
            process_folder(bucket_name, folder_name, processed_file_path, index, num_folders)
            logger.info(f"Processed {index} of {num_folders} folders")
            # Append the processed folder name to the processed.txt in the bucket
            processed_folders.add(folder_name)
            processed_blob.upload_from_string("\n".join(processed_folders), content_type='text/plain')
        else:
            logger.info(f"Skipping already processed folder: {folder_name}")


input_bucket_name = 'supplements_app_storage'
base_folder = 'filtered_raw_posts/'
main(input_bucket_name, base_folder)