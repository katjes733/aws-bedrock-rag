import json
import os
import logging
import time
from typing import Optional
import boto3
import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

levels = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warn': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}
logger = logging.getLogger()
try:
    logger.setLevel(levels.get(os.getenv('LOG_LEVEL', 'info').lower()))
except KeyError:
    logger.setLevel(logging.INFO)

app = FastAPI()

bedrock = boto3.client('bedrock-runtime')

class Story(BaseModel):
    """
    The story class.

    Args:
        BaseModel (_type_): _description_
    """
    topic: Optional[str] = None


@app.post("/")
def api_story(story: Story) -> StreamingResponse:
    """
    API endpoint to get a story.

    Args:
        story (Story): The story object.

    Returns:
        StreamingResponse: The streaming response
    """
    if story.topic is None or story.topic == "":
        return None

    return StreamingResponse(bedrock_stream(story.topic), media_type="text/html")


async def bedrock_stream(topic: str):
    """
    Stream the response from the bedrock model based on the given topic.

    Args:
        topic (str): The topic.
    """
    instruction = f"""
    You are a world class writer. Please write a sweet bedtime story about {topic}.
    Always end with "The End".
    Always be polite and friendly.
    Never add any copyright related information to your answer.
    Do not ask for feedback and do not ask for further requests.
    """
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "user",
                "content": instruction,
            }
        ],
    })

    logger.debug("Invoking model with response stream...")
    start_time = time.time()
    response = bedrock.invoke_model_with_response_stream(
        modelId='anthropic.claude-3-haiku-20240307-v1:0',
        body=body
    )
    end_time = time.time()
    duration = end_time - start_time
    logger.info("Invoked model with response stream in %.3f seconds", duration)

    stream = response.get('body')
    count = 1
    if stream:
        logger.debug("Streaming...")
        start_time = time.time()
        for event in stream:
            chunk = event.get('chunk')
            if chunk:
                message = json.loads(chunk.get("bytes").decode())
                if message['type'] == "content_block_delta":
                    logger.debug("Chunk %d: %s", count, message['delta']['text'])
                    yield message['delta']['text'] or ""
                elif message['type'] == "message_stop":
                    end_time = time.time()
                    duration = end_time - start_time
                    logger.info("Streaming completed in %.3f seconds", duration)
                    yield "\n"


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
