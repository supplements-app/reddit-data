import json
import logging
from logging.handlers import RotatingFileHandler
import time
from google.cloud import storage
import weaviate
import os
import re

# Initialize Weaviate client
from dotenv import load_dotenv
from weaviate.classes.config import Configure, Property, DataType

load_dotenv()  # Load environment variables

weaviate_client = weaviate.connect_to_wcs(
    cluster_url=os.getenv("WEAVIATE_CLUSTER_URL"),  # WCS URL from .env file
    auth_credentials=weaviate.auth.AuthApiKey(os.getenv("WEAVIATE_AUTH_KEY")),  # WCS key from .env file
    headers={'X-VoyageAI-Api-Key': os.getenv("VOYAGE_API_KEY")}  # OpenAI API key from .env file
)
# Initialize Google Cloud Storage client
storage_client = storage.Client()
bucket = storage_client.bucket("supplements_app_storage")

# subreddit ids
subreddit_ids = ["t5_2qhb8", "t5_2r81c"]

# logger config
logger = logging.getLogger('embed_post_data')
logger.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler('embeddingPostData.log', maxBytes=1024*1024*5, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

from supplements_list import supplements

def exponential_backoff(func):
    """Decorator to retry function with exponential backoff."""
    def wrapper(*args, **kwargs):
        retries = 5
        backoff_factor = 2
        delay = 0.5  # Initial delay in seconds
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                    delay *= backoff_factor
                else:
                    logger.error(f"Failed after {retries} attempts")
                    raise
    return wrapper

@exponential_backoff
def read_checkpoint():
    checkpoint_blob = bucket.blob("last_processed.txt")
    if not checkpoint_blob.exists():
        return 0, 0  # Default starting indexes if file does not exist
    checkpoint_data = checkpoint_blob.download_as_text()
    supplement_index, post_index = map(int, checkpoint_data.split(','))
    return supplement_index, post_index

@exponential_backoff
def update_checkpoint(supplement_index, post_index, force_update=False):
    if force_update or post_index % 10 == 0:  # Update checkpoint every 10 posts or on force
        checkpoint_blob = bucket.blob("last_processed.txt")
        checkpoint_data = f"{supplement_index},{post_index}"
        if not checkpoint_blob.exists():
            checkpoint_blob.upload_from_string("0,0")  # Initialize if not exists
        checkpoint_blob.upload_from_string(checkpoint_data)

def create_post_data_collection_if_not_exists():
    if not weaviate_client.collections.exists("Post_Data"): 
        weaviate_client.collections.create(
            "Post_Data",
            vectorizer_config=Configure.Vectorizer.text2vec_voyageai(
                vectorize_collection_name=False
            ),
            properties=[
                Property(name="title", data_type=DataType.TEXT, description="The title of the post"),
                Property(name="body_chunk", data_type=DataType.TEXT, description="A chunk (or the whole) of the post body"),
                Property(name="comments", data_type=DataType.TEXT, skip_vectorization=True, description="A JSON array of comments for the post"),
                Property(name="author", data_type=DataType.TEXT, skip_vectorization=True, description="The author of the comment"),
                Property(name="body", data_type=DataType.TEXT, skip_vectorization=True, description="The body of the comment"),
                Property(name="supplement", data_type=DataType.TEXT, skip_vectorization=True, description="The supplement discussed in the comment"),
                Property(name="created_utc", data_type=DataType.TEXT, skip_vectorization=True, description="The UTC timestamp of the comment creation"),
                Property(name="subreddit_id", data_type=DataType.TEXT, skip_vectorization=True, description="The ID of the subreddit"),
                Property(name="link_id", data_type=DataType.TEXT, skip_vectorization=True, description="The link ID associated with the comment"),
                Property(name="score", data_type=DataType.TEXT, skip_vectorization=True, description="The score of the comment")
            ]
        )

@exponential_backoff
def download_with_exponential_backoff(blob):
    return blob.download_as_string()

def chunk_text(text, max_words=100):
    """Chunk text into segments of approximately max_words, ending at the last complete sentence."""
    words = text.split()
    chunks = []
    current_chunk = []
    word_count = 0

    for word in words:
        current_chunk.append(word)
        word_count += 1
        if word_count >= max_words:
            # Join the current chunk and check for the last period to end the sentence
            chunk_str = ' '.join(current_chunk)
            last_period_index = chunk_str.rfind('.')
            if last_period_index != -1:
                # End the chunk at the last complete sentence
                chunks.append(chunk_str[:last_period_index + 1])
                current_chunk = current_chunk[chunk_str[:last_period_index + 1].count(' '):]
            else:
                chunks.append(chunk_str)
                current_chunk = []
            word_count = len(current_chunk)
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    return chunks

def process_posts(): 
    supplement_index_checkpoint, post_index_checkpoint = read_checkpoint()
    with weaviate_client.batch.rate_limit(requests_per_minute=295) as batch:
        for subreddit_id in subreddit_ids:
            for supplement_index, supplement in enumerate(supplements):
                
                if supplement_index < supplement_index_checkpoint:
                    continue

                # Adjusted prefix to point to posts instead of comments
                post_prefix = f"filtered_raw_posts/{subreddit_id}-{supplement}-posts/"
                post_blobs = list(bucket.list_blobs(prefix=post_prefix))  # Convert to list to check length
                if not post_blobs:
                    logger.debug(f"No post files found for supplement: {supplement}")
                    continue  # Skip to the next supplement if no post blobs are found

                for post_index, post_blob in enumerate(post_blobs):
                    if supplement_index == supplement_index_checkpoint and post_index < post_index_checkpoint:
                        continue

                    post_content = download_with_exponential_backoff(post_blob)
                    if post_content:  # Ensure the post content is not empty
                        post_data = json.loads(post_content)  # Load the entire post content as a JSON object
                        post_id = post_data.get("id")  # Get the post ID

                        # Construct the path for the corresponding comments file using the post ID
                        comments_prefix = f"filtered_raw_comments-v1/{subreddit_id}-{supplement}-aggregated-comments/{post_id}.jsonl"
                        comments_blob = bucket.blob(comments_prefix)
                        comments_content = download_with_exponential_backoff(comments_blob)
                        if comments_content:
                            # Split the content by lines and convert each line to a JSON object
                            comments = [json.loads(line) for line in comments_content.splitlines()]
                        else:
                            comments = []
                            logger.error(f"Empty or invalid content in comments blob: {comments_blob.name}")

                        # Stub: Process post_data and comments
                        serialized_comments = json.dumps(comments)  # Serialize comments into a JSON string
                        body_chunks = chunk_text(post_data.get("selftext", ""))
                        for body_chunk in body_chunks:
                            batch.add_object(collection="Post_Data", properties={
                                "title": post_data.get("title"),
                                "body_chunk": body_chunk,
                                "comments": serialized_comments,
                                "author": post_data.get("author"),
                                "supplement": supplement,
                                "body": post_data.get("selftext"),
                                "created_utc": str(post_data.get("created_utc")),
                                "subreddit_id": post_data.get("subreddit_id"),
                                "link_id": post_data.get("link_id"),
                                "score": str(post_data.get("score"))
                            })
                            logger.info(f"Added post data for supplement {supplement} to batch.")

                        # update checkpoint in last_processed.txt
                        update_checkpoint(supplement_index, post_index, force_update=(post_index % 10 == 9))
                    else:
                        logger.error(f"Empty or invalid content in post blob: {post_blob.name}")
    logger.info(f"Batch of post data added to Weaviate. Failed Objects: %s", weaviate_client.batch.failed_objects)

if __name__ == "__main__":
    try:
        create_post_data_collection_if_not_exists()
        process_posts()
    finally:
        weaviate_client.close()

