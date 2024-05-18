from dataclasses import dataclass
import json

@dataclass
class PostData:
    title: str
    body_chunk: str
    comments: str
    author: str
    body: str
    supplement: str
    created_utc: str
    subreddit_id: str
    link_id: str
    result_score: float

def create_prompt(query, sorted_results):
    posts_data = []
    for index, post in enumerate(sorted_results):
        comments = json.loads(post.comments)
        comments_str = "\n".join([f"Comment ID: {comment['id']}, Body: {comment['body']}" for comment in comments])
        
        post_data = {
            "index": index,
            "title": post.title,
            "body": post.body,
            "comments": comments_str
        }
        posts_data.append(post_data)
    
    # Construct the prompt
    prompt = f"This is a user query: {query}\n\n"
    prompt += "Here are some posts and their comments to be used as context to answer the query:\n\n"
    for post_data in posts_data:
        prompt += f"Post {post_data['index']}:\n"
        prompt += f"Title: {post_data['title']}\n"
        prompt += f"Body: {post_data['body']}\n"
        prompt += f"Comments: {post_data['comments']}\n\n"
    
    prompt += (
        "Based on the above posts, please provide the following in JSON format:\n"
        "{\n"
        "  \"summary\": \"Provide a descriptive passage here to the user query based on the context in the provided posts and comments. Bias towards answering based on general supplements or nootropics rather than specific brands. You can include knowledge you've been trained on. This should be a self-contained answer so that a user could read this answer without having to look at the recommendations or sources. You can make this verbose.\",\n"
        "  \"supplements\": [\n"
        "    // Order these supplements descending by how confident you are that it would help the user."
        "    {\n"
        "      \"name\": \"Name of the supplement\",\n"
        "      \"description\": \"Provide a passge explaining what the supplement is and explain why it is recommended based on the user query.\"\n"
        "    }\n"
        "    // More supplements in this format\n"
        "  ],\n"
        "  \"sources\": [\n"
        "    // Please provide a list of sources (posts or comments) from the provided posts ranked in descending order of helpfulness to answering the user query. If the source is about diet or lifestyle rather than a supplement, don't favor it.\n"
        "    // Format for each source: {\"source_type\": \"post\" or \"comment\", \"comment_id\": \"ID of the comment if source is a comment, empty if source is a post\", \"index\": index_of_post}\n"
        "  ]\n"
        "}\n"
    )

    return prompt

def parse_rag_response(response_json, sorted_results):
    response_dict = json.loads(response_json)

    print(response_dict)
    
    summary = response_dict['summary']
    
    supplements = [
        {
            "supplementName": supp['name'],
            "description": supp['description'],
            "purchaseUrl": "#"
        }
        for supp in response_dict['supplements']
    ]
    

    subreddit_ids = {"t5_2qhb8": "r/Supplements", "t5_2r81c": "r/Nootropics"}

    sources = []
    used_sources = set()
    for src in response_dict['sources']:
        index = src['index']
        post = sorted_results[index]
        if src['source_type'] == "post":
            source_key = f"https://www.reddit.com/{post.link_id}"
            sources.append({
                "sourceName": "reddit",
                "sourceUrl": source_key,
                "sourceType": "post",
                "sourceContent": post.body,
                "sourceAuthor": post.author,
                "subredditName": subreddit_ids[post.subreddit_id]
            })
            used_sources.add(source_key)
        elif src['source_type'] == "comment":
            comment = next((c for c in json.loads(post.comments) if c['id'] == src['comment_id']), None)
            source_key = f"https://www.reddit.com/r/{comment['subreddit']}/comments/{comment['link_id']}/comment/{comment['id']}"
            if comment:
                sources.append({
                    "sourceName": "reddit",
                    "sourceUrl": source_key,
                    "sourceType": "comment",
                    "sourceUpvotes": comment.get('score', None),
                    "sourceContent": comment['body'],
                    "sourceAuthor": comment['author'],
                    "subredditName": subreddit_ids[comment['subreddit_id']]
                })
            used_sources.add(source_key)

    # Augment sources with all similarity search data
    for post in sorted_results:
        source_key = f"https://www.reddit.com/{post.link_id}"
        if source_key not in used_sources:
            sources.append({
                "sourceName": "reddit",
                "sourceUrl": source_key,
                "sourceType": "post",
                "sourceContent": post.body,
                "sourceAuthor": post.author,
                "subredditName": subreddit_ids[post.subreddit_id]
            })
            used_sources.add(source_key)


        comments = json.loads(post.comments)
        for comment in comments:
            source_key = f"https://www.reddit.com/r/{comment['subreddit']}/comments/{comment['link_id']}/comment/{comment['id']}"
            if source_key not in used_sources:
                sources.append({
                    "sourceName": "reddit",
                    "sourceUrl": source_key,
                    "sourceType": "comment",
                    "sourceUpvotes": comment.get('score', None),
                    "sourceContent": comment['body'],
                    "sourceAuthor": comment['author'],
                    "subredditName": subreddit_ids[comment['subreddit_id']]
                })
                used_sources.add(source_key)

    print(sources)
    return {
        "aiAnswer": summary,
        "supplementRecommendations": supplements,
        "sources": sources
    }

