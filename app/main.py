import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from uuid import uuid4

from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from azure.storage.blob import BlobServiceClient, StandardBlobTier
from azure.storage.queue import QueueClient
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from openai import OpenAI, RateLimitError
from pydantic import BaseModel, Field

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BLOB_CONTAINER = os.getenv("AZURE_BLOB_CONTAINER", "documents")
QUEUE_NAME = os.getenv("AZURE_QUEUE_NAME", "document-jobs")
FAILED_QUEUE_NAME = os.getenv("AZURE_FAILED_QUEUE_NAME", "failed-documents")
COSMOS_DATABASE = os.getenv("COSMOS_DATABASE", "document-pipeline")
COSMOS_CONTAINER = os.getenv("COSMOS_CONTAINER", "documents")
AI_MODEL = os.getenv("AI_MODEL", "gpt-4.1-mini")

NUM_SHARDS = 64
MAX_DEQUEUE_ATTEMPTS = 3


@lru_cache(maxsize=1)
def get_clients():
    storage_url = os.environ["AZURE_STORAGE_ACCOUNT_URL"]
    queue_url = os.environ["AZURE_QUEUE_ACCOUNT_URL"]
    cosmos_endpoint = os.environ["COSMOS_ENDPOINT"]
    ai_endpoint = os.environ["AI_ENDPOINT"]

    credential = DefaultAzureCredential()

    blob_service = BlobServiceClient(account_url=storage_url, credential=credential)
    container_client = blob_service.get_container_client(BLOB_CONTAINER)

    jobs_queue = QueueClient(
        account_url=queue_url, queue_name=QUEUE_NAME, credential=credential
    )
    failed_queue = QueueClient(
        account_url=queue_url, queue_name=FAILED_QUEUE_NAME, credential=credential
    )

    cosmos = CosmosClient(url=cosmos_endpoint, credential=credential)
    db = cosmos.get_database_client(COSMOS_DATABASE)
    cosmos_container = db.get_container_client(COSMOS_CONTAINER)


    token_provider = get_bearer_token_provider(
        credential,
        "https://ai.azure.com/.default",
    )

    ai_client = OpenAI(
        base_url=ai_endpoint,
        api_key=token_provider,
        max_retries=2,
    )


    return container_client, jobs_queue, failed_queue, cosmos_container, ai_client


app = FastAPI(title="CC Assignment 4 — Document Processing Pipeline")



class DocumentUploadRequest(BaseModel):
    filename: str = Field(..., min_length=1, max_length=255)
    content: str = Field(..., min_length=1)


class ExtractedDocumentFields(BaseModel):
    category: str
    summary: str
    keywords: list[str] = Field(..., min_length=3, max_length=5)


def compute_shard_id(document_id: str) -> str:
    digest = hashlib.sha256(document_id.encode("utf-8")).hexdigest()
    shard_num = int(digest, 16) % NUM_SHARDS
    return f"shard-{shard_num:02d}"


def extract_document_fields(ai_client: OpenAI, text: str) -> dict:
    prompt = (
        "Analyze the following document and respond ONLY with a valid JSON object "
        "containing exactly three keys:\n"
        '1. "category": a short topic category string (e.g., "Renewable Energy", "Finance", "Technology")\n'
        '2. "summary": a concise 2-sentence summary string\n'
        '3. "keywords": a list of 3 to 5 lowercase keyword strings\n\n'
        f"Document:\n{text}"
    )

    response = ai_client.responses.create(
        model=AI_MODEL,
        input=prompt,
    )

    if getattr(response, "usage", None):
        logger.info(
            "AI tokens: input=%s output=%s total=%s",
            response.usage.input_tokens,
            response.usage.output_tokens,
            response.usage.total_tokens,
        )

    raw_output = response.output_text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw_output, flags=re.DOTALL).strip()

    parsed = json.loads(cleaned)

    validated = ExtractedDocumentFields.model_validate(parsed)

    return validated.model_dump()



@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "document-processing-pipeline",
    }


@app.post("/documents")
def upload_document(payload: DocumentUploadRequest):
    container_client, jobs_queue, _, _, _ = get_clients()
    document_id = str(uuid4())
    shard_id = compute_shard_id(document_id)
    created_at = datetime.now(timezone.utc).isoformat()
    blob_name = f"{document_id}/{payload.filename}"

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(
        payload.content.encode("utf-8"),
        overwrite=True,
        standard_blob_tier=StandardBlobTier.HOT,
    )

    job_message = json.dumps(
        {
            "id": document_id,
            "shardId": shard_id,
            "filename": payload.filename,
            "blobName": blob_name,
            "createdAt": created_at,
        }
    )
    jobs_queue.send_message(job_message)

    return {
        "id": document_id,
        "shardId": shard_id,
        "filename": payload.filename,
        "status": "queued",
        "message": "Document uploaded successfully.",
    }


@app.post("/process-next")
def process_next_document():
    container_client, jobs_queue, failed_queue, cosmos_container, ai_client = get_clients()
    messages = list(jobs_queue.receive_messages(messages_per_page=1, visibility_timeout=60))
    if not messages:
        return {
            "status": "empty",
            "message": "No queued documents waiting for processing.",
        }

    msg = messages[0]
    job = json.loads(msg.content)
    document_id = job["id"]
    shard_id = job["shardId"]
    blob_name = job["blobName"]

    try:
        blob_client = container_client.get_blob_client(blob_name)
        text_content = blob_client.download_blob().readall().decode("utf-8")

        extracted = extract_document_fields(ai_client, text_content)

        record = {
            "id": document_id,
            "shardId": shard_id,
            "filename": job["filename"],
            "blobName": blob_name,
            "category": extracted["category"],
            "summary": extracted["summary"],
            "keywords": extracted["keywords"],
            "createdAt": job["createdAt"],
            "status": "processed",
        }

        cosmos_container.upsert_item(body=record)
        jobs_queue.delete_message(msg.id, msg.pop_receipt)

        return {
            "id": document_id,
            "shardId": shard_id,
            "status": "processed",
            "result": {
                "category": record["category"],
                "summary": record["summary"],
                "keywords": record["keywords"],
                "status": record["status"],
            },
        }

    except RateLimitError as exc:
        retry_delay = 30
        if getattr(exc, "response", None) is not None:
            retry_ms = exc.response.headers.get("retry-after-ms")
            retry_sec = exc.response.headers.get("Retry-After")
            if retry_ms and retry_ms.isdigit():
                retry_delay = max(1, int(int(retry_ms) / 1000))
            elif retry_sec and retry_sec.isdigit():
                retry_delay = max(1, int(retry_sec))

        logger.warning("AI rate limit hit for %s; delaying retry by %ss", document_id, retry_delay)
        if msg.dequeue_count >= MAX_DEQUEUE_ATTEMPTS:
            failed_queue.send_message(msg.content)
            jobs_queue.delete_message(msg.id, msg.pop_receipt)
            raise HTTPException(status_code=429, detail="Max retries exceeded; moved job to failed-documents queue.")

        jobs_queue.update_message(msg.id, msg.pop_receipt, visibility_timeout=retry_delay)
        raise HTTPException(status_code=429, detail=f"AI rate limit exceeded; retry scheduled in {retry_delay}s.")


    except Exception:
        logger.exception(
            "Processing failed for document %s (attempt %s)",
            document_id,
            msg.dequeue_count,
        )

        if msg.dequeue_count >= MAX_DEQUEUE_ATTEMPTS:
            failed_queue.send_message(msg.content)
            jobs_queue.delete_message(msg.id, msg.pop_receipt)

            raise HTTPException(
                status_code=502,
                detail="Processing failed permanently. The job was moved to the failed-documents queue.",
            )

        jobs_queue.update_message(
            msg.id,
            msg.pop_receipt,
            visibility_timeout=15,
        )

        raise HTTPException(
            status_code=502,
            detail="Document processing failed. The job has been scheduled for retry.",
        )



@app.get("/documents/{document_id}")
def get_document(document_id: str):
    _, _, _, cosmos_container, _ = get_clients()
    shard_id = compute_shard_id(document_id)
    try:
        item = cosmos_container.read_item(item=document_id, partition_key=shard_id)
        return item
    except CosmosResourceNotFoundError:
        raise HTTPException(status_code=404, detail="Document not found or still processing.")
