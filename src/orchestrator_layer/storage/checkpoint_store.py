"""Checkpoint store: persistence for execution context (serialized as dict)."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any


class CheckpointStore(ABC):
    """Abstract store for checkpoint data (instance_id -> serialized context + metadata)."""

    @abstractmethod
    async def save(self, instance_id: str, data: dict[str, Any], metadata: dict[str, Any] | None = None) -> None:
        """Save checkpoint data for an instance. Overwrites previous for that instance."""
        ...

    @abstractmethod
    async def load(self, instance_id: str) -> dict[str, Any] | None:
        """Load latest checkpoint data for an instance. Returns None if not found."""
        ...

    @abstractmethod
    async def list_metadata(self, instance_id: str) -> list[dict[str, Any]]:
        """List checkpoint metadata for an instance (e.g. for replay), newest first."""
        ...


class InMemoryCheckpointStore(CheckpointStore):
    """In-memory checkpoint store (single checkpoint per instance)."""

    def __init__(self) -> None:
        self._data: dict[str, dict[str, Any]] = {}
        self._meta: dict[str, list[dict[str, Any]]] = {}

    async def save(self, instance_id: str, data: dict[str, Any], metadata: dict[str, Any] | None = None) -> None:
        self._data[instance_id] = data
        meta = dict(metadata) if metadata else {}
        meta.setdefault("instance_id", instance_id)
        meta.setdefault("timestamp", time.time())
        meta.setdefault("current_step_id", data.get("current_step_id"))
        self._meta.setdefault(instance_id, []).append(meta)

    async def load(self, instance_id: str) -> dict[str, Any] | None:
        return self._data.get(instance_id)

    async def list_metadata(self, instance_id: str) -> list[dict[str, Any]]:
        return list(reversed(self._meta.get(instance_id, [])))
