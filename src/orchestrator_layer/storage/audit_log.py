"""Audit Log: append-only log of workflow and approval events."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class AuditEntry:
    """Single audit log entry."""

    entry_id: str
    instance_id: str
    event_type: str  # e.g. WORKFLOW_STARTED, STEP_COMPLETED, APPROVAL_REQUESTED
    payload: dict[str, Any]
    timestamp: float  # epoch


class AuditLog(ABC):
    """Abstract append-only audit log (async)."""

    @abstractmethod
    async def append(self, entry: AuditEntry) -> None:
        """Append an audit entry."""
        ...

    @abstractmethod
    async def list_by_instance(self, instance_id: str, limit: int = 100) -> list[AuditEntry]:
        """List audit entries for a workflow instance, newest first."""
        ...

    @abstractmethod
    async def list_by_event_type(self, event_type: str, limit: int = 100) -> list[AuditEntry]:
        """List audit entries by event type."""
        ...


class InMemoryAuditLog(AuditLog):
    """In-memory implementation of audit log."""

    def __init__(self) -> None:
        self._entries: list[AuditEntry] = []
        self._by_instance: dict[str, list[AuditEntry]] = {}
        self._by_type: dict[str, list[AuditEntry]] = {}

    async def append(self, entry: AuditEntry) -> None:
        self._entries.append(entry)
        self._by_instance.setdefault(entry.instance_id, []).append(entry)
        self._by_type.setdefault(entry.event_type, []).append(entry)

    async def list_by_instance(self, instance_id: str, limit: int = 100) -> list[AuditEntry]:
        entries = self._by_instance.get(instance_id, [])
        return list(reversed(entries[-limit:]))

    async def list_by_event_type(self, event_type: str, limit: int = 100) -> list[AuditEntry]:
        entries = self._by_type.get(event_type, [])
        return list(reversed(entries[-limit:]))
