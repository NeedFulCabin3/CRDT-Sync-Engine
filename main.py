import asyncio
import json
import logging
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CRDT_Engine")

REDIS_URL = "redis://localhost:6379"

# -----------------------------------------------------------------------------
# 1. CRDT / OT Data Models & Data Structures
# -----------------------------------------------------------------------------

class TextOp(BaseModel):
    """
    Operation model representing an edit frame.
    Types: 'insert', 'delete'
    """
    op_type: str = Field(..., pattern="^(insert|delete)$")
    position: int = Field(..., ge=0)
    character: Optional[str] = ""
    site_id: str
    clock: int

class SyncPayload(BaseModel):
    doc_id: str
    site_id: str
    ops: List[TextOp]

@dataclass
class CRDTDocument:
    doc_id: str
    content: List[str] = field(default_factory=list)
    vector_clocks: Dict[str, int] = field(default_factory=dict)
    
    def apply_op(self, op: TextOp) -> bool:
        """
        Applies an operation using basic position-based OT with site-id/clock deduplication.
        Returns True if state changed, False if redundant/out-of-order op ignored.
        """
        last_clock = self.vector_clocks.get(op.site_id, -1)
        if op.clock <= last_clock:
            return False  # Ignore duplicate/stale operation

        if op.op_type == "insert":
            pos = min(op.position, len(self.content))
            self.content.insert(pos, op.character or "")
        elif op.op_type == "delete":
            if 0 <= op.position < len(self.content):
                self.content.pop(op.position)
        
        self.vector_clocks[op.site_id] = op.clock
        return True

    def get_text(self) -> str:
        return "".join(self.content)


# -----------------------------------------------------------------------------
# 2. Redis Pub/Sub & Connection Manager
# -----------------------------------------------------------------------------

class DocumentHub:
    """
    Manages active WebSocket connections and integrates Redis Pub/Sub for horizontal scaling.
    """
    def __init__(self):
        # doc_id -> Set[WebSocket]
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        # doc_id -> CRDTDocument
        self.documents: Dict[str, CRDTDocument] = {}
        self.redis: Optional[aioredis.Redis] = None
        self.pubsub_tasks: Dict[str, asyncio.Task] = {}

    async def initialize(self):
        try:
            self.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
            await self.redis.ping()
            logger.info("Connected to Redis Pub/Sub layer successfully.")
        except Exception as e:
            logger.warning(f"Redis unavailable ({e}). Fallback to local process memory.")
            self.redis = None

    def get_or_create_doc(self, doc_id: str) -> CRDTDocument:
        if doc_id not in self.documents:
            self.documents[doc_id] = CRDTDocument(doc_id=doc_id)
        return self.documents[doc_id]

    async def connect(self, websocket: WebSocket, doc_id: str):
        await websocket.accept()
        if doc_id not in self.active_connections:
            self.active_connections[doc_id] = set()
            if self.redis:
                # Spawn background task to listen to Redis channel for this document
                self.pubsub_tasks[doc_id] = asyncio.create_task(self._redis_subscribe_loop(doc_id))
                
        self.active_connections[doc_id].add(websocket)
        
        # Send initial document snapshot to the joining client
        doc = self.get_or_create_doc(doc_id)
        snapshot = {
            "type": "snapshot",
            "content": doc.get_text(),
            "vector_clocks": doc.vector_clocks
        }
        await websocket.send_text(json.dumps(snapshot))

    def disconnect(self, websocket: WebSocket, doc_id: str):
        if doc_id in self.active_connections:
            self.active_connections[doc_id].discard(websocket)
            if not self.active_connections[doc_id]:
                del self.active_connections[doc_id]
                # Cleanup Redis subscription task if channel empty locally
                if doc_id in self.pubsub_tasks:
                    self.pubsub_tasks[doc_id].cancel()
                    del self.pubsub_tasks[doc_id]

    async def broadcast_local(self, doc_id: str, message: str, sender: Optional[WebSocket] = None):
        """Broadcasts payload to connected sockets on this server node."""
        if doc_id in self.active_connections:
            for connection in self.active_connections[doc_id]:
                if connection != sender:
                    try:
                        await connection.send_text(message)
                    except Exception as e:
                        logger.error(f"Error sending to client: {e}")

    async def process_remote_op(self, doc_id: str, raw_payload: str, sender: Optional[WebSocket] = None):
        """Applies mutation to local state and publishes out to cluster via Redis."""
        try:
            data = json.loads(raw_payload)
            if data.get("type") == "op":
                op = TextOp(**data["op"])
                doc = self.get_or_create_doc(doc_id)
                if doc.apply_op(op):
                    # Broadcast to other local sockets
                    await self.broadcast_local(doc_id, raw_payload, sender=sender)
                    # Publish to Redis channel for multi-node sync
                    if self.redis:
                        await self.redis.publish(f"doc:{doc_id}", raw_payload)
        except Exception as e:
            logger.error(f"Malformed operation frame received: {e}")

    async def _redis_subscribe_loop(self, doc_id: str):
        """Listens for remote updates published from other nodes running via Redis."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(f"doc:{doc_id}")
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    payload = message["data"]
                    # Apply edit frame locally without re-broadcasting back to Redis
                    data = json.loads(payload)
                    if data.get("type") == "op":
                        op = TextOp(**data["op"])
                        doc = self.get_or_create_doc(doc_id)
                        if doc.apply_op(op):
                            await self.broadcast_local(doc_id, payload)
        except asyncio.CancelledError:
            await pubsub.unsubscribe(f"doc:{doc_id}")


hub = DocumentHub()
app = FastAPI(title="Real-Time Collaborative CRDT Engine")


@app.on_event("startup")
async def startup_event():
    await hub.initialize()


# -----------------------------------------------------------------------------
# 3. HTTP & WebSocket Endpoints
# -----------------------------------------------------------------------------

@app.get("/doc/{doc_id}")
async def get_document(doc_id: str):
    """REST retrieval for initial state checks or integration testing."""
    doc = hub.get_or_create_doc(doc_id)
    return {
        "doc_id": doc_id,
        "content": doc.get_text(),
        "length": len(doc.content),
        "vector_clocks": doc.vector_clocks
    }


@app.websocket("/ws/{doc_id}")
async def websocket_endpoint(websocket: WebSocket, doc_id: str):
    await hub.connect(websocket, doc_id)
    try:
        while True:
            data = await websocket.receive_text()
            await hub.process_remote_op(doc_id, data, sender=websocket)
    except WebSocketDisconnect:
        hub.disconnect(websocket, doc_id)
    except Exception as e:
        logger.error(f"Unexpected WebSocket failure: {e}")
        hub.disconnect(websocket, doc_id)


# -----------------------------------------------------------------------------
# 4. Embedded Front-End Client (Web Browser UI)
# -----------------------------------------------------------------------------

@app.get("/")
async def client_interface():
    html_content = """
    <!DOCTYPE html>
    <html>
        <head>
            <title>Collaborative Editor</title>
            <style>
                body { font-family: sans-serif; margin: 2rem; background: #f4f4f9; }
                #editor { width: 100%; height: 300px; font-size: 16px; padding: 10px; }
                .status { margin-bottom: 10px; font-weight: bold; color: #333; }
            </style>
        </head>
        <body>
            <h2>Real-Time Collaborative Editor</h2>
            <div class="status">Status: <span id="status">Connecting...</span> | Site ID: <span id="siteId"></span></div>
            <textarea id="editor" placeholder="Type here to collaborate..."></textarea>

            <script>
                const docId = "default-doc";
                const siteId = "site-" + Math.random().toString(36).substring(2, 9);
                document.getElementById("siteId").innerText = siteId;

                const ws = new WebSocket(`ws://${location.host}/ws/${docId}`);
                const editor = document.getElementById("editor");
                const status = document.getElementById("status");

                let clock = 0;
                let lastContent = "";

                ws.onopen = () => { status.innerText = "Connected"; };
                ws.onclose = () => { status.innerText = "Disconnected"; };

                ws.onmessage = (event) => {
                    const data = JSON.parse(event.data);
                    if (data.type === "snapshot") {
                        editor.value = data.content;
                        lastContent = data.content;
                    } else if (data.type === "op") {
                        applyRemoteOp(data.op);
                    }
                };

                function applyRemoteOp(op) {
                    let text = editor.value;
                    if (op.op_type === "insert") {
                        text = text.slice(0, op.position) + op.character + text.slice(op.position);
                    } else if (op.op_type === "delete") {
                        text = text.slice(0, op.position) + text.slice(op.position + 1);
                    }
                    editor.value = text;
                    lastContent = text;
                }

                editor.addEventListener("input", (e) => {
                    const currentText = editor.value;
                    
                    // Simple Diff Engine to detect operation
                    if (currentText.length > lastContent.length) {
                        // Insertion
                        let pos = editor.selectionStart - 1;
                        let char = currentText[pos];
                        clock++;
                        sendOp("insert", pos, char);
                    } else if (currentText.length < lastContent.length) {
                        // Deletion
                        let pos = editor.selectionStart;
                        clock++;
                        sendOp("delete", pos, "");
                    }
                    lastContent = currentText;
                });

                function sendOp(type, position, character) {
                    const payload = {
                        type: "op",
                        op: {
                            op_type: type,
                            position: position,
                            character: character,
                            site_id: siteId,
                            clock: clock
                        }
                    };
                    ws.send(JSON.stringify(payload));
                }
            </script>
        </body>
    </html>
    """
    return HTMLResponse(html_content)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)