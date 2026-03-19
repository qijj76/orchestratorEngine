"""Workflow State store: persistence for workflow instance state."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class WorkflowInstanceState:
    """Snapshot of a workflow instance."""

    instance_id: str
    workflow_id: str
    status: str  # e.g. RUNNING, WAITING_APPROVAL, COMPLETED, FAILED
    context: dict[str, Any] = field(default_factory=dict)
    current_step: str | None = None
    version: int = 0


class WorkflowStateStore(ABC):
    """Abstract store for workflow instance state (async)."""

    @abstractmethod
    async def get(self, instance_id: str) -> WorkflowInstanceState | None:
        """Load workflow instance by id."""
        ...

    @abstractmethod
    async def put(self, state: WorkflowInstanceState) -> None:
        """Save or update workflow instance state."""
        ...

    @abstractmethod
    async def delete(self, instance_id: str) -> bool:
        """Remove workflow instance. Returns True if existed."""
        ...

    @abstractmethod
    async def list_by_workflow(self, workflow_id: str) -> list[WorkflowInstanceState]:
        """List all instances of a workflow."""
        ...


class InMemoryWorkflowStateStore(WorkflowStateStore):
    """In-memory implementation of workflow state store."""

    def __init__(self) -> None:
        self._store: dict[str, WorkflowInstanceState] = {}

    async def get(self, instance_id: str) -> WorkflowInstanceState | None:
        return self._store.get(instance_id)

    async def put(self, state: WorkflowInstanceState) -> None:
        self._store[state.instance_id] = state

    async def delete(self, instance_id: str) -> bool:
        if instance_id in self._store:
            del self._store[instance_id]
            return True
        return False

    async def list_by_workflow(self, workflow_id: str) -> list[WorkflowInstanceState]:
        return [s for s in self._store.values() if s.workflow_id == workflow_id]
