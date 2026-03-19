"""Checkpointing: save and restore execution state for recovery via a checkpoint store."""

from __future__ import annotations

import time
from typing import Any

from orchestrator_layer.state_manager.execution_context import ExecutionContext
from orchestrator_layer.storage.checkpoint_store import CheckpointStore, InMemoryCheckpointStore


class Checkpointing:
    """Saves and loads ExecutionContext using a CheckpointStore (storage layer)."""

    def __init__(self, store: CheckpointStore) -> None:
        self._store = store

    async def save_checkpoint(self, instance_id: str, context: ExecutionContext) -> None:
        """Save execution context checkpoint."""
        data = context.to_dict()
        metadata = {
            "instance_id": instance_id,
            "timestamp": time.time(),
            "current_step_id": context.current_step_id,
        }
        await self._store.save(instance_id, data, metadata)

    async def load_checkpoint(self, instance_id: str) -> ExecutionContext | None:
        """Load latest checkpoint for an instance."""
        data = await self._store.load(instance_id)
        if data is None:
            return None
        return ExecutionContext.from_dict(data)

    async def list_checkpoints(self, instance_id: str) -> list[dict[str, Any]]:
        """List checkpoint metadata (e.g. for replay)."""
        return await self._store.list_metadata(instance_id)


def InMemoryCheckpointing() -> Checkpointing:
    """Convenience: Checkpointing backed by InMemoryCheckpointStore (backward compatible)."""
    return Checkpointing(InMemoryCheckpointStore())
