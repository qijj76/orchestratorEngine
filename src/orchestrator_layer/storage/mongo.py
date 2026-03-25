"""MongoDB storage implementations using the motor async driver."""

from __future__ import annotations

import time
from dataclasses import asdict, is_dataclass
from typing import Any, TypeVar

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from orchestrator_layer.storage.audit_log import AuditEntry, AuditLog
from orchestrator_layer.storage.checkpoint_store import CheckpointStore
from orchestrator_layer.storage.pending_queue import PendingItem, PendingQueue
from orchestrator_layer.storage.token_store import (
    ApprovalToken,
    TokenStatus,
    TokenStore,
)
from orchestrator_layer.storage.workflow_state import (
    WorkflowInstanceState,
    WorkflowStateStore,
)

T = TypeVar("T")


def _doc_to_workflow_state(doc: dict[str, Any]) -> WorkflowInstanceState:
    return WorkflowInstanceState(
        instance_id=doc["instance_id"],
        workflow_id=doc["workflow_id"],
        status=doc["status"],
        context=doc.get("context") or {},
        current_step=doc.get("current_step"),
        version=doc.get("version", 0),
    )


def _workflow_state_to_doc(s: WorkflowInstanceState) -> dict[str, Any]:
    return {
        "instance_id": s.instance_id,
        "workflow_id": s.workflow_id,
        "status": s.status,
        "context": s.context,
        "current_step": s.current_step,
        "version": s.version,
    }


def _doc_to_token(doc: dict[str, Any]) -> ApprovalToken:
    return ApprovalToken(
        token_id=doc["token_id"],
        instance_id=doc["instance_id"],
        step_id=doc["step_id"],
        status=TokenStatus(doc["status"]),
        payload=doc.get("payload"),
        expires_at=doc.get("expires_at"),
    )


def _token_to_doc(t: ApprovalToken) -> dict[str, Any]:
    return {
        "token_id": t.token_id,
        "instance_id": t.instance_id,
        "step_id": t.step_id,
        "status": t.status.value,
        "payload": t.payload,
        "expires_at": t.expires_at,
    }


def _doc_to_audit_entry(doc: dict[str, Any]) -> AuditEntry:
    return AuditEntry(
        entry_id=doc["entry_id"],
        instance_id=doc["instance_id"],
        event_type=doc["event_type"],
        payload=doc["payload"],
        timestamp=doc["timestamp"],
    )


def _payload_to_store(payload: Any) -> dict[str, Any]:
    if is_dataclass(payload) and not isinstance(payload, type):
        return asdict(payload)
    if isinstance(payload, dict):
        return payload
    return {"value": payload}


class MongoWorkflowStateStore(WorkflowStateStore):
    """MongoDB-backed workflow state store (async, motor)."""

    def __init__(
        self,
        database: AsyncIOMotorDatabase,
        *,
        collection_name: str = "workflow_state",
    ) -> None:
        self._coll = database[collection_name]

    async def get(self, instance_id: str) -> WorkflowInstanceState | None:
        doc = await self._coll.find_one({"instance_id": instance_id})
        if doc is None:
            return None
        return _doc_to_workflow_state(doc)

    async def put(self, state: WorkflowInstanceState) -> None:
        await self._coll.replace_one(
            {"instance_id": state.instance_id},
            _workflow_state_to_doc(state),
            upsert=True,
        )

    async def delete(self, instance_id: str) -> bool:
        r = await self._coll.delete_one({"instance_id": instance_id})
        return r.deleted_count > 0

    async def list_by_workflow(self, workflow_id: str) -> list[WorkflowInstanceState]:
        cursor = self._coll.find({"workflow_id": workflow_id})
        return [_doc_to_workflow_state(d) async for d in cursor]

    async def list_all(self) -> list[WorkflowInstanceState]:
        cursor = self._coll.find({})
        return [_doc_to_workflow_state(d) async for d in cursor]

    async def ensure_indexes(self) -> None:
        """Create recommended indexes. Call once at startup."""
        await self._coll.create_index("instance_id", unique=True)
        await self._coll.create_index("workflow_id")


class MongoTokenStore(TokenStore):
    """MongoDB-backed token store (async, motor)."""

    def __init__(
        self,
        database: AsyncIOMotorDatabase,
        *,
        collection_name: str = "tokens",
    ) -> None:
        self._coll = database[collection_name]

    async def get(self, token_id: str) -> ApprovalToken | None:
        doc = await self._coll.find_one({"token_id": token_id})
        if doc is None:
            return None
        return _doc_to_token(doc)

    async def put(self, token: ApprovalToken) -> None:
        await self._coll.replace_one(
            {"token_id": token.token_id},
            _token_to_doc(token),
            upsert=True,
        )

    async def list_pending_for_instance(self, instance_id: str) -> list[ApprovalToken]:
        cursor = self._coll.find(
            {"instance_id": instance_id, "status": TokenStatus.PENDING.value}
        )
        return [_doc_to_token(d) async for d in cursor]

    async def list_pending(self) -> list[ApprovalToken]:
        """List pending tokens across all workflow instances."""
        cursor = self._coll.find({"status": TokenStatus.PENDING.value})
        return [_doc_to_token(d) async for d in cursor]

    async def list_by_step(self, instance_id: str, step_id: str) -> list[ApprovalToken]:
        cursor = self._coll.find({"instance_id": instance_id, "step_id": step_id})
        return [_doc_to_token(d) async for d in cursor]

    async def ensure_indexes(self) -> None:
        await self._coll.create_index("token_id", unique=True)
        await self._coll.create_index([("instance_id", 1), ("status", 1)])
        await self._coll.create_index([("instance_id", 1), ("step_id", 1)])


class MongoAuditLog(AuditLog):
    """MongoDB-backed append-only audit log (async, motor)."""

    def __init__(
        self,
        database: AsyncIOMotorDatabase,
        *,
        collection_name: str = "audit_log",
    ) -> None:
        self._coll = database[collection_name]

    async def append(self, entry: AuditEntry) -> None:
        await self._coll.insert_one({
            "entry_id": entry.entry_id,
            "instance_id": entry.instance_id,
            "event_type": entry.event_type,
            "payload": entry.payload,
            "timestamp": entry.timestamp,
        })

    async def list_by_instance(self, instance_id: str, limit: int = 100) -> list[AuditEntry]:
        cursor = (
            self._coll.find({"instance_id": instance_id})
            .sort("timestamp", -1)
            .limit(limit)
        )
        return [_doc_to_audit_entry(d) async for d in cursor]

    async def list_by_event_type(self, event_type: str, limit: int = 100) -> list[AuditEntry]:
        cursor = (
            self._coll.find({"event_type": event_type})
            .sort("timestamp", -1)
            .limit(limit)
        )
        return [_doc_to_audit_entry(d) async for d in cursor]

    async def ensure_indexes(self) -> None:
        await self._coll.create_index("instance_id")
        await self._coll.create_index("event_type")
        await self._coll.create_index("timestamp")


class MongoPendingQueue(PendingQueue[dict[str, Any]]):
    """
    MongoDB-backed pending queue (async, motor).
    Payload is stored and returned as a dict (BSON-serializable).
    For dataclass payloads (e.g. CallbackPayload), serialize before enqueue
    or reconstruct from the returned dict after dequeue.
    """

    def __init__(
        self,
        database: AsyncIOMotorDatabase,
        *,
        collection_name: str = "pending_queue",
    ) -> None:
        self._coll = database[collection_name]

    async def enqueue(self, item: PendingItem[Any]) -> None:
        created = item.created_at if item.created_at > 0 else time.time()
        payload = _payload_to_store(item.payload)
        await self._coll.insert_one({
            "item_id": item.item_id,
            "payload": payload,
            "priority": item.priority,
            "created_at": created,
        })

    async def dequeue(self) -> PendingItem[dict[str, Any]] | None:
        doc = await self._coll.find_one_and_delete(
            {},
            sort=[("priority", -1), ("created_at", 1)],
        )
        if doc is None:
            return None
        return PendingItem(
            item_id=doc["item_id"],
            payload=doc["payload"],
            priority=doc.get("priority", 0),
            created_at=doc.get("created_at", 0),
        )

    async def peek(self) -> PendingItem[dict[str, Any]] | None:
        doc = await self._coll.find_one(
            {},
            sort=[("priority", -1), ("created_at", 1)],
        )
        if doc is None:
            return None
        return PendingItem(
            item_id=doc["item_id"],
            payload=doc["payload"],
            priority=doc.get("priority", 0),
            created_at=doc.get("created_at", 0),
        )

    async def size(self) -> int:
        return await self._coll.count_documents({})

    async def ensure_indexes(self) -> None:
        await self._coll.create_index([("priority", -1), ("created_at", 1)])


class MongoCheckpointStore(CheckpointStore):
    """MongoDB-backed checkpoint store (async, motor). One document per instance (latest data); metadata history in separate collection."""

    def __init__(
        self,
        database: AsyncIOMotorDatabase,
        *,
        collection_name: str = "checkpoints",
        meta_collection_name: str = "checkpoint_meta",
    ) -> None:
        self._coll = database[collection_name]
        self._meta_coll = database[meta_collection_name]

    async def save(self, instance_id: str, data: dict[str, Any], metadata: dict[str, Any] | None = None) -> None:
        meta = dict(metadata) if metadata else {}
        ts = meta.get("timestamp", time.time())
        meta.setdefault("instance_id", instance_id)
        meta.setdefault("timestamp", ts)
        meta.setdefault("current_step_id", data.get("current_step_id"))
        await self._coll.replace_one(
            {"instance_id": instance_id},
            {"instance_id": instance_id, "data": data, "timestamp": ts, "current_step_id": data.get("current_step_id")},
            upsert=True,
        )
        await self._meta_coll.insert_one(meta)

    async def load(self, instance_id: str) -> dict[str, Any] | None:
        doc = await self._coll.find_one({"instance_id": instance_id})
        if doc is None:
            return None
        return doc.get("data")

    async def list_metadata(self, instance_id: str) -> list[dict[str, Any]]:
        cursor = (
            self._meta_coll.find({"instance_id": instance_id})
            .sort("timestamp", -1)
            .limit(1000)
        )
        return [d async for d in cursor]

    async def ensure_indexes(self) -> None:
        await self._coll.create_index("instance_id", unique=True)
        await self._meta_coll.create_index("instance_id")
        await self._meta_coll.create_index("timestamp")


def create_mongo_stores(
    uri: str = "mongodb://localhost:27017",
    database_name: str = "orchestrator",
    *,
    workflow_state_collection: str = "workflow_state",
    tokens_collection: str = "tokens",
    audit_log_collection: str = "audit_log",
    pending_queue_collection: str = "pending_queue",
    checkpoints_collection: str = "checkpoints",
    checkpoint_meta_collection: str = "checkpoint_meta",
) -> tuple[
    MongoWorkflowStateStore,
    MongoTokenStore,
    MongoAuditLog,
    MongoPendingQueue,
    MongoCheckpointStore,
]:
    """
    Create MongoDB-backed stores.
    Returns (workflow_state_store, token_store, audit_log, pending_queue, checkpoint_store).
    """
    client = AsyncIOMotorClient(uri)
    db = client[database_name]
    workflow_store = MongoWorkflowStateStore(db, collection_name=workflow_state_collection)
    token_store = MongoTokenStore(db, collection_name=tokens_collection)
    audit_log = MongoAuditLog(db, collection_name=audit_log_collection)
    pending_queue = MongoPendingQueue(db, collection_name=pending_queue_collection)
    checkpoint_store = MongoCheckpointStore(
        db,
        collection_name=checkpoints_collection,
        meta_collection_name=checkpoint_meta_collection,
    )
    return workflow_store, token_store, audit_log, pending_queue, checkpoint_store


async def ensure_mongo_indexes(
    workflow_store: MongoWorkflowStateStore,
    token_store: MongoTokenStore,
    audit_log: MongoAuditLog,
    pending_queue: MongoPendingQueue,
    checkpoint_store: MongoCheckpointStore,
) -> None:
    """Create recommended indexes on all Mongo collections. Call once at startup."""
    await workflow_store.ensure_indexes()
    await token_store.ensure_indexes()
    await audit_log.ensure_indexes()
    await pending_queue.ensure_indexes()
    await checkpoint_store.ensure_indexes()
