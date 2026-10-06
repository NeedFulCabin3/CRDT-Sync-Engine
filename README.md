# CRDT Sync Engine

> Real-time concurrent document collaboration backed by FastAPI WebSockets and Redis Pub/Sub.

## Overview
Collaborative document platforms require immediate convergence when multiple users execute operations on identical text indices simultaneously. Without synchronization mechanics, race conditions distort document state, cause index drift, and trigger network disconnects.

`crdt-sync-engine` addresses this operational friction by decoupling state mutation from network dispatch. It executes position-based operational transformations across persistent WebSocket connections, validating incoming edit frames with Pydantic schemas and broadcasting state updates across cluster instances using Redis Pub/Sub.

## How It Works
The system handles client connections, state synchronization, and horizontal distribution through a non-blocking asynchronous pipeline:

1. **Client Connection & Initial Snapshot:** When a client joins a document session via `/ws/{doc_id}`, the server initializes connection tracking in `DocumentHub` and dispatches a JSON snapshot containing full text state and existing vector clock counters.
2. **Local Operational Transformation:** Incoming client edits pass through schema validation (`TextOp`). `CRDTDocument` verifies the operation against local site vector clocks (`self.vector_clocks`). Operations with equal or lesser clock values are discarded as redundant or out of order. Valid operations mutate the in-memory `content` list.
3. **Local Broadcast:** Mutated operations immediately stream to all other clients connected to the current process instance via active WebSocket handles.
4. **Multi-Node Sync Loop:** If Redis is available, the host node publishes the frame to `doc:{doc_id}`. Background tasks (`_redis_subscribe_loop`) running on secondary node instances ingest the channel payload, mutate local document memory, and forward changes to their connected clients.

## Key Features
* **Asynchronous WebSockets:** Built on FastAPI for handling high-concurrency client socket loops with minimal overhead.
* **Vector Clock Deduplication:** Prevents stale or duplicated frame processing using site identifiers and sequence counters.
* **Graceful Degradation:** Automatically falls back to single-process in-memory synchronization if the Redis infrastructure becomes unavailable.
* **Data Validation:** Enforces strict payload structure (`TextOp` and `SyncPayload`) through Pydantic field validators.
* **Embedded Test Suite UI:** Includes an embedded single-page application route (`/`) featuring a light diff-matching engine for browser testing.

## Tech Stack & Core Dependencies Breakdown
* **Python Target:** Python 3.10+
* **FastAPI:** Core web framework providing non-blocking ASGI WebSocket routes and HTTP endpoints.
* **Pydantic (v2):** Enforces data model constraints, typed JSON parsing, and range checks on incoming operation indexes.
* **redis-py (`redis.asyncio`):** Manages asynchronous publish/subscribe messaging across horizontal service instances.
* **Uvicorn:** Production-grade ASGI server implementation running the event loop.

## Environment & Web-Based Quick Start

### Running in GitHub Codespaces
1. Open the repository in GitHub Codespaces.
2. Start an isolated Redis container (optional, fallback supported):
   ```bash
   docker run -d -p 6379:6379 redis:alpine
   ```
3. Initialize the Python environment and launch the engine:
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install fastapi pydantic redis uvicorn
    uvicorn main:app --host 0.0.0.0 --port 8000
    ```
4. Access the embedded client by navigating to http://localhost:8000/.

### Virtual Environment Configration 

```bash
# Clone or create project workspace
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install fastapi pydantic redis uvicorn

# Execute server
python main:app
```

## Repository Structure

```bash
.
├── .github/
│   └── workflows/
│       └── ci.yml               # Automated code health checks and syntax validation
├── .gitignore                   # Stack-specific file and environment exclusions
├── LICENSE                      # MIT license file
├── README.md                    # System architecture and execution documentation
└── main.py                      # Core engine, CRDT models, Redis manager, and FastAPI app
```

## Roadmap

**[ ] Async I/O Persistence:** Migrate document state flushing from process memory to an asynchronous PostgreSQL or Redis key-value persistent store.

**[ ] Yjs / LWW-Element-Graph Integration:** Replace basic positional list mutation with a formal conflict-free replicated data structure (such as Yjs or standard LWW-element sequence graphs) to handle multi-character block insertions seamlessly.

**[ ] Structured Telemetry:** Integrate Prometheus metrics tracking active socket counts, latency spikes during payload parsing, and Redis Pub/Sub frame transport times.
