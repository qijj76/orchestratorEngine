"""Central workflow runner: enqueues next steps from definitions after each step."""

from __future__ import annotations

import uuid
from typing import Any, Awaitable, Callable

from orchestrator_layer.scheduler.event_queue import EventKind, EventQueue, WorkflowEvent
from orchestrator_layer.scheduler.worker_pool import StepResult
from orchestrator_layer.workflow.definition import WorkflowRegistry

PutEventFn = Callable[[str, WorkflowEvent], Awaitable[None]]


class WorkflowRunner:
    """
    Uses a WorkflowRegistry to determine the next step(s) after a step completes
    and enqueues corresponding events. Also handles WORKFLOW_START by enqueueing
    the entry step.
    Supports a single shared EventQueue or a put_event(workflow_id, event) for
    routing to a queue per workflow group.
    """

    def __init__(
        self,
        registry: WorkflowRegistry,
        event_queue: EventQueue | None = None,
        put_event: PutEventFn | None = None,
    ) -> None:
        self._registry = registry
        if put_event is not None:
            self._put_event = put_event
        elif event_queue is not None:
            self._put_event = lambda wf_id, ev: event_queue.put(ev)
        else:
            raise ValueError("Provide either event_queue or put_event")

    async def handle_start(
        self,
        instance_id: str,
        workflow_id: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """
        Handle WORKFLOW_START: enqueue the entry step for this workflow.
        Call this from the engine when dispatching a WORKFLOW_START event.
        """
        entry_step_id = self._registry.get_entry_step(workflow_id)
        if not entry_step_id:
            return
        await self._put_event(
            workflow_id,
            WorkflowEvent(
                event_id=str(uuid.uuid4()),
                kind=EventKind.STEP_READY,
                instance_id=instance_id,
                step_id=entry_step_id,
                payload=payload,
            ),
        )

    async def enqueue_next(
        self,
        instance_id: str,
        workflow_id: str,
        current_step_id: str,
        step_result: StepResult,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """
        After a step completes, get next step(s) from the registry (or from
        step_result.next_step_id) and enqueue one STEP_READY event per next step.
        """
        next_step_ids = self._registry.get_next_steps(
            workflow_id,
            current_step_id,
            step_result.next_step_id if step_result.success else None,
        )
        for step_id in next_step_ids:
            await self._put_event(
                workflow_id,
                WorkflowEvent(
                    event_id=str(uuid.uuid4()),
                    kind=EventKind.STEP_READY,
                    instance_id=instance_id,
                    step_id=step_id,
                    payload=payload,
                ),
            )
