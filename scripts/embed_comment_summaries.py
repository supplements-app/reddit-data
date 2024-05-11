import json
import logging
from logging.handlers import RotatingFileHandler
from google.cloud import storage
import weaviate
import os

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
subredd_ids = ["t5_2qhb8", "t5_2r81c"]

# logger config
logger = logging.getLogger('embed_comment_summaries')
logger.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler('embeddingCommentSummaries.log', maxBytes=1024*1024*5, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

from supplements_list import supplements

def create_comment_summaries_collection_if_not_exists():
    if not weaviate_client.collections.exists("Comment_Summaries"): 
        weaviate_client.collections.create(
            "Comment_Summaries",
            vectorizer_config=Configure.Vectorizer.text2vec_voyageai(
                model="voyage-lite-02-instruct",
                vectorize_collection_name=False
            ),
            properties=[  # vectorize every field except supplement and summary
                Property(name="author", data_type=DataType.TEXT, skip_vectorization=True, description="The author of the comment"),
                Property(name="body", data_type=DataType.TEXT, skip_vectorization=True, description="The body of the comment"),
                Property(name="supplement", data_type=DataType.TEXT, description="The supplement discussed in the comment"),
                Property(name="summary", data_type=DataType.TEXT, description="The summary of the comment"),
                Property(name="created_utc", data_type=DataType.INT, skip_vectorization=True, description="The UTC timestamp of the comment creation"),
                Property(name="subreddit_id", data_type=DataType.TEXT, skip_vectorization=True, description="The ID of the subreddit"),
                Property(name="link_id", data_type=DataType.TEXT, skip_vectorization=True, description="The link ID associated with the comment"),
                Property(name="score", data_type=DataType.INT, skip_vectorization=True, description="The score of the comment")
            ]
        )

def create_raw_comments_collection_if_not_exists():
    if not weaviate_client.collections.exists("Raw_Comments"): 
        weaviate_client.collections.create(
            "Raw_Comments",
            vectorizer_config=Configure.Vectorizer.text2vec_voyageai(
                model="voyage-lite-02-instruct",
                vectorize_collection_name=False
            ),
            properties=[  # vectorize every field except supplement and summary
                Property(name="author", data_type=DataType.TEXT, skip_vectorization=True, description="The author of the comment"),
                Property(name="body", data_type=DataType.TEXT, description="The body of the comment"),
                Property(name="supplement", data_type=DataType.TEXT, description="The supplement discussed in the comment"),
                Property(name="created_utc", data_type=DataType.INT, skip_vectorization=True, description="The UTC timestamp of the comment creation"),
                Property(name="subreddit_id", data_type=DataType.TEXT, skip_vectorization=True, description="The ID of the subreddit"),
                Property(name="link_id", data_type=DataType.TEXT, skip_vectorization=True, description="The link ID associated with the comment"),
                Property(name="score", data_type=DataType.INT, skip_vectorization=True, description="The score of the comment")
            ]
        )

def process_comments(): 
    # Uses dynamic batching; weaviate handles how many batches to create
    with weaviate_client.batch.rate_limit(requests_per_minute=250) as batch:
        for subreddit_id in ["t5_2qhb8"]:
            for supplement in supplements:
                if supplement == "Alcohol":
                    break

                prefix = f"filtered_raw_comments/{subreddit_id}-{supplement}-aggregated-comments/"
                blobs = list(bucket.list_blobs(prefix=prefix))  # Convert to list to check length
                if not blobs:
                    logger.debug(f"No files found for supplement: {supplement}")
                    continue  # Skip to the next supplement if no blobs are found

                for blob in blobs:
                    blob_content = blob.download_as_string()
                    if blob_content:  # Ensure the comment summary is not empty
                        for line in blob_content.splitlines():
                            comment_json_data = json.loads(line)
                            comment_object_properties = {
                                "author": comment_json_data.get("author", "deleted"),
                                "body": comment_json_data.get("body"),
                                "supplement": supplement,
                                "created_utc": comment_json_data.get("created_utc"),
                                "subreddit_id": comment_json_data.get("subreddit_id"),
                                "link_id": comment_json_data.get("link_id"),
                                "score": comment_json_data.get("score")
                            }
                            batch.add_object(collection="Raw_Comments", properties=comment_object_properties)
                            logger.debug(f"Added raw comment for supplement {supplement} to batch.")
                    else:
                        logger.error(f"Empty or invalid content in blob: {blob.name}")
    
    # batch submitted
    logger.info("Batch of comment summaries added to Weaviate. Failed Objects: %s", weaviate_client.batch.failed_objects)

if __name__ == "__main__":
    try:
        create_raw_comments_collection_if_not_exists()
        process_comments()
    finally:
        weaviate_client.close()

