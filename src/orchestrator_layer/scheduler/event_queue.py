"""Event Queue: queue of workflow events for the dispatcher."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any


class EventKind(str, Enum):
    STEP_READY = "step_ready"
    APPROVAL_RESOLVED = "approval_resolved"
    TIMER_FIRED = "timer_fired"
    WORKFLOW_START = "workflow_start"
    WORKFLOW_CANCEL = "workflow_cancel"


@dataclass
class WorkflowEvent:
    """Event to be processed by the orchestration engine."""

    event_id: str
    kind: EventKind
    instance_id: str
    step_id: str | None = None
    payload: dict[str, Any] | None = None


class EventQueue(ABC):
    """Abstract queue for workflow events (async-first)."""

    @abstractmethod
    async def put(self, event: WorkflowEvent) -> None:
        """Add event to the queue (async)."""
        ...

    @abstractmethod
    async def get(self) -> WorkflowEvent:
        """Remove and return next event; blocks until available (async)."""
        ...

    @abstractmethod
    def size(self) -> int:
        """Current queue size."""
        ...


class InMemoryEventQueue(EventQueue):
    """In-memory FIFO event queue (async put/get)."""

    def __init__(self) -> None:
        self._events: list[WorkflowEvent] = []
        self._cond = asyncio.Condition()

    async def put(self, event: WorkflowEvent) -> None:
        async with self._cond:
            self._events.append(event)
            self._cond.notify()

    async def get(self) -> WorkflowEvent:
        async with self._cond:
            while not self._events:
                await self._cond.wait()
            return self._events.pop(0)

    def size(self) -> int:
        return len(self._events)


class AsyncEventQueue(EventQueue):
    """Event queue backed by asyncio.Queue for high-concurrency I/O."""

    def __init__(self, maxsize: int = 0) -> None:
        self._queue: asyncio.Queue[WorkflowEvent] = asyncio.Queue(maxsize=maxsize)

    async def put(self, event: WorkflowEvent) -> None:
        await self._queue.put(event)

    async def get(self) -> WorkflowEvent:
        return await self._queue.get()

    def size(self) -> int:
        return self._queue.qsize()


DEFAULT_GROUP = "default"


class EventQueueRouter:
    """
    Routes workflow events to a specific EventQueue per group.
    Use when each group of workflows should have its own queue (and optionally workers).
    """

    def __init__(
        self,
        workflow_groups: dict[str, str],
        queues_by_group: dict[str, EventQueue],
        default_group: str = DEFAULT_GROUP,
    ) -> None:
        """
        Args:
            workflow_groups: workflow_id -> group_id. Workflows not in this map use default_group.
            queues_by_group: group_id -> EventQueue. Must contain default_group if any workflow may use it.
            default_group: group_id used when workflow_id is not in workflow_groups.
        """
        self._workflow_groups = dict(workflow_groups)
        self._queues_by_group = dict(queues_by_group)
        self._default_group = default_group

    def get_group(self, workflow_id: str) -> str:
        """Return the group_id for the given workflow_id."""
        return self._workflow_groups.get(workflow_id, self._default_group)

    def get_queue(self, group_id: str) -> EventQueue | None:
        """Return the EventQueue for the given group_id, or None."""
        return self._queues_by_group.get(group_id)

    def get_queue_for_workflow(self, workflow_id: str) -> EventQueue | None:
        """Return the EventQueue for the workflow's group, or None."""
        return self.get_queue(self.get_group(workflow_id))

    async def put(self, workflow_id: str, event: WorkflowEvent) -> None:
        """Route the event to the queue for the workflow's group."""
        group_id = self.get_group(workflow_id)
        queue = self._queues_by_group.get(group_id)
        if queue is None:
            raise KeyError(f"No queue registered for group {group_id!r} (workflow_id={workflow_id!r})")
        await queue.put(event)