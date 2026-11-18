# Assignment 4 — Data Pipeline and Scale

**Name:** K. Montarroso · **Campus:** Madrid  
**Course:** Cloud Computing · **Repository:** `cc-assignment-4`  
**Cloud Provider:** Microsoft Azure (`swedencentral` & `norwayeast`) · **Stack:** FastAPI + Azure Blob/Queue Storage + Cosmos DB + Azure AI Foundry

---

## 1. Overview

This project implements a document-processing pipeline using **Azure Blob Storage**, **Azure Queue Storage**, **Azure Cosmos DB for NoSQL**, **Azure AI Foundry**, and a **Python FastAPI** application hosted on **Azure App Service**.

The system accepts text documents via `POST /documents`, stores the original files in Blob Storage (`Hot` access tier), queues processing jobs in Azure Queue Storage, extracts structured fields (`category`, `summary`, and `keywords`) asynchronously via `POST /process-next` using an Azure AI Foundry `gpt-4.1-mini` deployment, and persists the structured results in Cosmos DB. A query endpoint (`GET /documents/{document_id}`) retrieves processed records using an O(1) point read.

All service-to-service authentication uses **Microsoft Entra ID** (`DefaultAzureCredential` with Azure CLI locally and a System-Assigned Managed Identity in Azure)—with **zero account keys or secrets** stored in code or configuration.

---

## 2. Architecture

```mermaid
flowchart TD
    A[User uploads document<br/>POST /documents] --> B[FastAPI Ingestion Endpoint]
    B --> C[Azure Blob Storage<br/>Hot Access Tier]
    B --> D[Azure Queue Storage<br/>document-jobs]
    D --> E[Processing Worker<br/>POST /process-next]
    E --> C
    E --> F[Azure AI Foundry<br/>gpt-4.1-mini]
    F --> E
    E -- Max Retries Exceeded --> G[Dead-Letter Queue<br/>failed-documents]
    E --> H[Azure Cosmos DB<br/>Partition Key: /shardId]
    I[User query<br/>GET /documents/id] --> J[FastAPI Query Endpoint]
    J --> H
    H --> J
    J --> K[Document Result JSON]
```

### Azure Services & Responsibilities

| Component | Azure Service | Responsibility |
| :--- | :--- | :--- |
| **Application** | Azure App Service (`B1` Linux, Python 3.12) | Hosts the FastAPI ingestion, worker, and query API |
| **File Storage** | Azure Blob Storage (`StorageV2`, `Hot` tier) | Stores original uploaded text documents |
| **Queue** | Azure Queue Storage (`document-jobs` & `failed-documents`) | Decouples ingestion from AI latency/rate limits and captures poison messages |
| **Database** | Azure Cosmos DB for NoSQL (Serverless demo, `/shardId`) | Stores extracted metadata, summaries, and keywords |
| **AI Service** | Azure AI Foundry (`gpt-4.1-mini`, `10,000` TPM) | Performs document summarisation, classification, and keyword extraction |
| **Authentication** | Microsoft Entra ID (`DefaultAzureCredential`) | Keyless RBAC authentication via App Service Managed Identity |

---

## 3. Authentication & Storage Decisions

### Keyless Authentication
The application uses `DefaultAzureCredential` across all four Azure SDK clients (`BlobServiceClient`, `QueueClient`, `CosmosClient`, and `OpenAI` via `get_bearer_token_provider` for `https://azure.com`).
* **Local development**: Authenticates via the developer's `az login` Entra ID session.
* **Azure deployment**: Uses the App Service **System-Assigned Managed Identity**, granted least-privilege RBAC roles (`Storage Blob Data Contributor`, `Storage Queue Data Contributor`, `Cognitive Services OpenAI User`, and `Cosmos DB Built-in Data Contributor`). No storage keys, Cosmos keys, or Foundry API keys exist in the repository.

### Storage Access Tier (`Hot`)
The Blob Storage account and uploaded document blobs explicitly use the **`Hot` access tier**. Newly uploaded documents are read immediately by the processing worker (`POST /process-next`), making frequent initial access guaranteed. While `Cool` or `Cold` tiers offer lower per-GB storage rates, they carry higher read operation costs and a 30-day (`Cool`) or 90-day (`Cold`) minimum retention penalty—making `Hot` the most cost-effective tier for active ingestion pipelines (with lifecycle management rules suitable for moving aged documents to `Cool` after 30 days).

### AI Rate Limits, Latency & Failure Handling
* **Latency decoupling**: `POST /documents` returns immediately after persisting the blob and enqueueing a lightweight job message, isolating upload clients from multi-second LLM inference latency.
* **Rate limits (`HTTP 429`)**: The OpenAI client retries transient errors (`max_retries=2`), and explicit `RateLimitError` exceptions inspect `retry-after-ms` / `Retry-After` headers to delay queue message visibility (`update_message(..., visibility_timeout=retry_delay)`) rather than hammering the endpoint.
* **At-least-once persistence & dead-lettering**: The queue message is deleted **only after** `cosmos_container.upsert_item(...)` succeeds. If a job fails `3` times (`dequeue_count >= 3`), it is moved to the `failed-documents` queue for inspection instead of being silently dropped.

---

## 4. Part C — Design for Scale

### 4.1 Partitioning
The Cosmos DB container uses **`/shardId`** as its partition key. When a document is created, the application hashes its UUID with SHA-256 and computes `int(sha256(id), 16) % 64`, producing 64 evenly distributed logical partition values (`shard-00` through `shard-63`).

The primary query pattern is retrieving a single processed document by ID:
```http
GET /documents/{document_id}
```
Because `shardId` is deterministically derived from `document_id`, the API executes a direct **point read** (`read_item(item=document_id, partition_key=shard_id)`), routing directly to the target partition without fan-out.

At **500 GB** of total data, an even hash distribution across 64 shards yields:
$$\frac{500\text{ GB}}{64\text{ shards}} \approx 7.81\text{ GB per shard}$$
This stays comfortably below Cosmos DB's **20 GB per logical partition** storage limit (leaving headroom for indexing overhead and minor skew).

**Which query this choice makes slow:** Analytical or filtered queries that do not specify `document_id`—such as *"find all documents where `category == 'Renewable Energy'`"* or *"list the 50 most recent documents across all users"*—cannot compute a single `shardId`. They must perform a **cross-partition fan-out query** across all 64 shards, increasing Request Unit (RU) consumption and tail latency.

### 4.2 The Bottleneck
At scale, the ingestion path and the read path have distinct bottlenecks, with **Azure AI Foundry saturating first during ingestion**:

* **Ingestion bottleneck (Azure AI Foundry TPM quota):**
  * **Measured quota**: My `gpt-4.1-mini` deployment in `swedencentral` has an allocated quota of **`10,000` tokens per minute (TPM)** (`sku.capacity = 10`).
  * **Measured token usage**: Processing `example.txt` logged **`270` total tokens** (`181` input tokens + `89` output tokens).
  * **Maximum sustainable ingestion throughput**:
    $$\frac{10{,}000\text{ tokens/min}}{270\text{ tokens/document}} \approx 37.04\text{ documents/minute} \approx 0.62\text{ documents/second}$$
  * Any sustained upload rate above **37 documents/minute** saturates the AI model quota, causing HTTP `429` throttling and growing backlog depth in `document-jobs`.

* **Read bottleneck (Cosmos DB RU/s at 50,000 QPS):**
  * Because `GET /documents/{document_id}` is decoupled from AI inference and executes a point read on a ~1 KB JSON item, each read costs approximately **1 RU**.
  * Sustaining **50,000 point reads/second** requires at least **50,000 RU/s** of provisioned (or autoscale) throughput—which the demonstration's Serverless Cosmos DB account (capped at 5,000 RU/s per container) and single `B1` App Service instance could not sustain without scaling out both compute instances and provisioned RU/s.

### 4.3 A Pattern That Helps: Event-Driven Queue
The pipeline uses **Azure Queue Storage** (`document-jobs`) between `POST /documents` and `POST /process-next`.

* **What it fixes:**
  1. **Absorbs traffic spikes**: Sudden upload bursts write quickly to Blob Storage and Queue Storage without overwhelming the `10,000` TPM AI deployment.
  2. **Isolates latency**: Upload callers receive an immediate `200 OK` (`status: "queued"`) instead of blocking on LLM generation.
  3. **Enables safe retries**: Transient AI failures or `429` rate limits leave the message in the queue for delayed retry rather than failing the user's upload.
* **What it costs:**
  1. **Eventual consistency & processing latency**: Immediately after `POST /documents`, `GET /documents/{id}` returns `404` until the worker finishes processing the queued job.
  2. **Operational complexity**: Requires managing queue visibility timeouts, idempotent database upserts, poison-message detection, and monitoring the `failed-documents` dead-letter queue.
  3. **Additional infrastructure & cost**: Introduces an extra managed resource and storage transactions per message enqueue/dequeue cycle.

### 4.4 Placement
Referring to **Session 2 (Cloud Regions vs. Edge)**, **no part of this pipeline belongs at the edge; the entire pipeline should remain inside a cloud region** (ideally co-located in the same region, such as `swedencentral`).

* **Justification**: Every stage relies on stateful, centralized regional services—durable Blob Storage, Queue Storage, Cosmos DB, and heavy GPU-backed LLM inference in Azure AI Foundry. Placing compute at the edge would introduce wide-area network round-trips back to the regional database and AI model without reducing end-to-end processing time.
While an edge CDN/API gateway could terminate TLS closer to globally distributed uploaders, the payloads are tiny text documents processed asynchronously, so regional hosting minimizes inter-service latency, simplifies Entra ID RBAC, and avoids edge complexity.

---

## 5. Running Locally & End-to-End Test

### Prerequisites
* Python 3.12+, Azure CLI (`az`), and an Azure subscription with RBAC permissions assigned to your Entra ID user (`Storage Blob Data Contributor`, `Storage Queue Data Contributor`, `Cognitive Services OpenAI User`, and `Cosmos DB Built-in Data Contributor`).

### Local Setup
```bash
git clone https://github.com
cd cc-assignment-4
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

az login
cp .env.example .env   # Populate with your Storage, Cosmos DB, and AI Foundry endpoints
uvicorn app.main:app --reload
```

### End-to-End Pipeline Flow
1. **Health check**: `GET /health`
2. **Upload document**: `POST /documents` with `{"filename": "example.txt", "content": "..."}` → returns `id` and `shardId`.
3. **Process queued job**: `POST /process-next` → reads blob, calls Azure AI Foundry (`gpt-4.1-mini`), upserts structured JSON to Cosmos DB, and deletes the queue message.
4. **Query processed document**: `GET /documents/{id}` → performs a partition-key point read from Cosmos DB.

---

## 6. Evidence

1. **Document Uploaded (`POST /documents`)**  
   ![Document Upload](evidence/01-document-upload.png)

2. **Original Document in Azure Blob Storage (`Hot` Tier)**  
   ![Blob Storage Hot Tier](evidence/02-blob-storage.png)

3. **AI Processing Completed (`POST /process-next`)**  
   ![AI Processing](evidence/03-ai-processing.png)

4. **Saved Record in Cosmos DB (`Data Explorer` with `/shardId`)**  
   ![Cosmos DB Record](evidence/04-cosmos-record.png)

5. **Query Endpoint Returning Processed Record (`GET /documents/{id}`)**  
   ![Query Endpoint](evidence/05-query-endpoint.png)

6. **App Service System-Assigned Managed Identity & RBAC Role Assignments**  
   ![Managed Identity](evidence/06-managed-identity.png)

---

## 7. Cleanup
After capturing all evidence, the entire Azure resource group (`cc-assignment-4-rg`) was deleted (`az group delete --name cc-assignment-4-rg --yes --no-wait`) and verified via `az group exists --name cc-assignment-4-rg` returning `false` so zero ongoing costs remain.

---

## AI Use Disclosure
I acknowledge the use of OpenAI ChatGPT (https://chatgpt.com) and Google Gemini (https://google.com) in completing this assignment. ChatGPT was used to generate the initial project guide and architecture structure, and Google Gemini was used to assist with implementing the FastAPI endpoints, configuring keyless Entra ID and Managed Identity RBAC permissions, troubleshooting Azure deployment and model quotas, verifying resource cleanup, and structuring the Part C scale calculations. All Azure deployments, API executions, token measurements, and screenshots were performed and verified directly in my own environment.