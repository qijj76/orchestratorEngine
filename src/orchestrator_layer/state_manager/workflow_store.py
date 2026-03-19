"""Workflow Instance Store: facade over workflow state storage + instance lifecycle."""

from __future__ import annotations

import uuid
from typing import Any

from orchestrator_layer.storage.workflow_state import (
    WorkflowStateStore,
    WorkflowInstanceState,
)


class WorkflowInstanceStore:
    """Manages workflow instance lifecycle using the workflow state store."""

    def __init__(self, state_store: WorkflowStateStore) -> None:
        self._store = state_store

    async def create(self, workflow_id: str, initial_context: dict[str, Any] | None = None) -> WorkflowInstanceState:
        """Create a new workflow instance."""
        instance_id = str(uuid.uuid4())
        state = WorkflowInstanceState(
            instance_id=instance_id,
            workflow_id=workflow_id,
            status="RUNNING",
            context=initial_context or {},
            current_step=None,
            version=0,
        )
        await self._store.put(state)
        return state

    async def get(self, instance_id: str) -> WorkflowInstanceState | None:
        """Load workflow instance by id."""
        return await self._store.get(instance_id)

    async def update(
        self,
        instance_id: str,
        *,
        status: str | None = None,
        current_step: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> WorkflowInstanceState | None:
        """Update instance fields. Merges context if provided."""
        state = await self._store.get(instance_id)
        if not state:
            return None
        if status is not None:
            state.status = status
        if current_step is not None:
            state.current_step = current_step
        if context is not None:
            state.context.update(context)
        state.version += 1
        await self._store.put(state)
        return state

    async def list_by_workflow(self, workflow_id: str) -> list[WorkflowInstanceState]:
        """List all instances of a workflow."""
        return await self._store.list_by_workflow(workflow_id)

    async def delete(self, instance_id: str) -> bool:
        """Remove workflow instance."""
        return await self._store.delete(instance_id)
