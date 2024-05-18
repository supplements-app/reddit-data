from collections import namedtuple
from dataclasses import dataclass
import functions_framework
from openai import OpenAI
from flask import Flask, request, jsonify
import weaviate
import json
import voyageai


@functions_framework.http
def process_query(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    
    # Env variables
    WEAVIATE_CLUSTER_URL = ""
    WEAVIATE_AUTH_KEY = ""
    VOYAGE_API_KEY=""
    OPENAI_API_KEY=""

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'query' in request_json:
        query = request_json.get('query')
    else:
        return jsonify({'error': 'Query parameter is missing.'}), 400
    
    # Construct the prompt
    # We could do a fuzzy search here instead to see if the query contains a supplement we know; this 
    # just allows to match supplements that we don't know about but have embedded
    prompt = f"""Given the query: '{query}', generate a short passage that could help answer this query. 
    I only want you to respond with an answer and nothing else. Leave out any disclaimers about physical 
    exercise or balance. I only want you to focus on supplements and nootropics that can help. If the query 
    is about a specific supplement, make sure your response is only about that supplement. I also want you 
    to include a boolean for whether the query is about a specific supplement. The format returned should 
    be JSON: {{'answer': '...', 'is_named_query': True/False}}"""


    # Call the GPT-4 API with the prompt
    gptClient = OpenAI(api_key=OPENAI_API_KEY)
    completion = gptClient.chat.completions.create(
        model="gpt-4o",
        response_format={ "type": "json_object" },
        messages=[
            {"role": "system", "content": "You are a helpful assistant to answer queries about supplements and nootropics."},
            {"role": "user", "content": prompt}
        ],
        n=3
    )
    
    # Parse the hypothetical documents
    ResponseTuple = namedtuple('ResponseTuple', ['similarity_query', 'is_named_query'])
    responses = []
    for choice in completion.choices:
        try:
            response_data = json.loads(choice.message.content)
            print(response_data)
            responses.append(ResponseTuple(similarity_query=response_data['answer'], is_named_query=response_data['is_named_query']))
        except json.JSONDecodeError:
            responses.append(ResponseTuple(similarity_query=choice.message.content, is_named_query=False))

    weaviate_client = weaviate.connect_to_wcs(
        cluster_url=WEAVIATE_CLUSTER_URL,
        auth_credentials=weaviate.auth.AuthApiKey(WEAVIATE_AUTH_KEY),
        headers={'X-VoyageAI-Api-Key': VOYAGE_API_KEY}
    )
    post_data_collection = weaviate_client.collections.get("Post_Data")

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

    # Query Weaviate and deduplicate across post ids based on unique 'id' property
    unique_results = {}
    for response in responses:
        alpha_value = 0.25 if response.is_named_query else 0.75
        query_response = post_data_collection.query.hybrid(
            query=response.similarity_query,
            alpha=alpha_value,
            limit=3
        )
        for result in query_response.objects:
            result_id = result.properties.get('id')
            if result_id not in unique_results:
                comments_json = json.loads(result.properties.get('comments', '[]'))  # Default to empty list if comments are missing
                link_id = comments_json[0].get('link_id') if comments_json else None  # Safely get link_id from the first comment if available

                unique_results[result_id] = PostData(
                    title=result.properties.get('title'),
                    body_chunk=result.properties.get('body_chunk'),
                    comments=result.properties.get('comments'),
                    author=result.properties.get('author'),
                    body=result.properties.get('body'),
                    supplement=result.properties.get('supplement'),
                    created_utc=result.properties.get('created_utc'),
                    subreddit_id=result.properties.get('subreddit_id'),
                    link_id=link_id,  # Use the extracted link_id
                    result_score=result.metadata.score
                )
    
    sorted_results = sorted(unique_results.values(), key=lambda x: x.result_score)

    # re-rank weaviate results based on original query
    # Extract the concatenation of title and body fields for each element in sorted_results
    # post_titles_and_bodies = [f"{result.title} {result.body}" for result in sorted_results]
    # vo = voyageai.Client(api_key=VOYAGE_API_KEY)

    # reranking = vo.rerank(query, post_titles_and_bodies, model="rerank-lite-1")
    # for r in reranking.results:
    #     print(f"Document: {r.document}")
    #     print(f"Relevance Score: {r.relevance_score}")

    rag_prompt = create_prompt(sorted_results)
    completion = gptClient.chat.completions.create(
        model="gpt-4o",
        response_format={ "type": "json_object" },
        messages=[
            {"role": "system", "content": "You are a helpful assistant to answer queries about supplements and nootropics."},
            {"role": "user", "content": rag_prompt}
        ],
    )
    rag_response = parse_rag_response(completion.choices[0].message)

    return jsonify({'hydes': sorted_results})

def create_prompt(sorted_results):
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
    prompt = "Here are some posts and their comments:\n\n"
    for post_data in posts_data:
        prompt += f"Post {post_data['index']}:\n"
        prompt += f"Title: {post_data['title']}\n"
        prompt += f"Body: {post_data['body']}\n"
        prompt += f"Comments: {post_data['comments']}\n\n"
    
    prompt += (
        "Based on the above posts, please provide the following in JSON format:\n"
        "{\n"
        "  \"summary\": \"A general summary answer based on the context in sorted_results.\",\n"
        "  \"supplements\": [\n"
        "    {\n"
        "      \"name\": \"Name of the supplement\",\n"
        "      \"description\": \"Short description of the supplement and why it is recommended\"\n"
        "    }\n"
        "    // More supplements in this format\n"
        "  ],\n"
        "  \"sources\": [\n"
        "    {\n"
        "      \"source_type\": \"post\" or \"comment\",\n"
        "      \"comment_id\": \"ID of the comment if source is a comment, empty if source is a post\",\n"
        "      \"index\": index_of_post\n"
        "    }\n"
        "    // More sources in this format\n"
        "  ]\n"
        "}\n"
    )

    return prompt

def parse_rag_response(response_json):
    @dataclass
    class SupplementRecommendation:
        name: str
        description: str

    @dataclass
    class Source:
        source_type: str
        comment_id: str
        index: int

    @dataclass
    class RAGResponse:
        summary: str
        supplements: list[SupplementRecommendation]
        sources: list[Source]

    response_dict = json.loads(response_json)
    
    summary = response_dict['summary']
    
    supplements = [
        SupplementRecommendation(name=supp['name'], description=supp['description'])
        for supp in response_dict['supplements']
    ]
    
    sources = [
        Source(source_type=src['source_type'], comment_id=src['comment_id'], index=src['index'])
        for src in response_dict['sources']
    ]
    
    return RAGResponse(summary=summary, supplements=supplements, sources=sources)