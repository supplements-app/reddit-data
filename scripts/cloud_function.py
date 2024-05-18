from collections import namedtuple
from dataclasses import dataclass
import functions_framework
from openai import OpenAI
from flask import Flask, request, jsonify
from helpers import create_prompt, parse_rag_response, PostData
import weaviate
import json
import logging

logging.basicConfig(level=logging.INFO)

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
    VOYAGE_API_KEY= ""
    OPENAI_API_KEY= ""
    ALLLOWED_BEARER_TOKEN = ""

    request_json = request.get_json(silent=True)
    request_args = request.args

    auth_header = request.headers.get('Authorization')[7:]
    if (auth_header != ALLLOWED_BEARER_TOKEN):
        return jsonify({"error": "invalid bearer token"}), 401

    if request_json and "query" in request_json:
        query = request_json.get("query")
    else:
        logging.error(f"An error occurred parsing the request body: {request.data}")
        return jsonify({'error': 'Query parameter is missing.', 'request_body': request_json}), 400
    
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
        temperature=1,
        messages=[
            {"role": "system", "content": "You are a helpful assistant to answer queries about supplements and nootropics."},
            {"role": "user", "content": prompt}
        ],
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
    responses.append(ResponseTuple(query, False))

    logging.info("responses to query from weaviate", responses)

    weaviate_client = weaviate.connect_to_wcs(
        cluster_url=WEAVIATE_CLUSTER_URL,
        auth_credentials=weaviate.auth.AuthApiKey(WEAVIATE_AUTH_KEY),
        headers={'X-VoyageAI-Api-Key': VOYAGE_API_KEY}
    )
    post_data_collection = weaviate_client.collections.get("Post_Data")

    # Query Weaviate and deduplicate across post ids based on unique 'id' property
    unique_results = {}
    for response in responses:
        alpha_value = 0.25 if response.is_named_query else 0.75
        query_response = post_data_collection.query.hybrid(
            query=response.similarity_query,
            alpha=alpha_value,
            limit=10
        )
        for result in query_response.objects:
            logging.info("result from weavaite", result)
            comments_json = json.loads(result.properties.get('comments', '[]'))  # Default to empty list if comments are missing
            link_id = comments_json[0].get('link_id')[3:] if comments_json and 'link_id' in comments_json[0] else None  # Safely get link_id from the first comment if available and chop off the first 3 characters if it exists
            if link_id not in unique_results:
                unique_results[link_id] = PostData(
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
            else:
                logging.info("Filtered out the same post returned by weaviate")

    logging.info("unique results from vector search", len(unique_results.values()))
    sorted_results = sorted(unique_results.values(), key=lambda x: x.result_score)

    # re-rank weaviate results based on original query
    # Extract the concatenation of title and body fields for each element in sorted_results
    # post_titles_and_bodies = [f"{result.title} {result.body}" for result in sorted_results]
    # vo = voyageai.Client(api_key=VOYAGE_API_KEY)

    # reranking = vo.rerank(query, post_titles_and_bodies, model="rerank-lite-1")
    # for r in reranking.results:
    #     print(f"Document: {r.document}")
    #     print(f"Relevance Score: {r.relevance_score}")

    rag_prompt = create_prompt(query, sorted_results)
    logging.info("prompt created", rag_prompt)
    
    completion = gptClient.chat.completions.create(
        model="gpt-4o",
        response_format={ "type": "json_object" },
        seed=123,
        temperature=0,
        messages=[
            {"role": "system", "content": "You are a helpful assistant to answer queries about supplements and nootropics."},
            {"role": "user", "content": rag_prompt}
        ],
    )
    parsed_response = parse_rag_response(completion.choices[0].message.content, sorted_results)

    return json.dumps(parsed_response)