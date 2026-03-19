"""Orchestration Engine: async facade for State Manager, Scheduler, Approval, and Storage."""

from __future__ import annotations

import time
import uuid
from typing import Any

from orchestrator_layer.approval.callback_receiver import CallbackReceiver, CallbackPayload
from orchestrator_layer.approval.notification_manager import NoOpNotificationManager, NotificationManager
from orchestrator_layer.approval.token_manager import TokenManager
from orchestrator_layer.scheduler.event_queue import (
    AsyncEventQueue,
    DEFAULT_GROUP,
    EventKind,
    EventQueue,
    EventQueueRouter,
    WorkflowEvent,
)
from orchestrator_layer.scheduler.timer_service import AsyncTimerService
from orchestrator_layer.scheduler.worker_pool import AsyncStepExecutor, AsyncWorkerPool, StepResult, WorkerPool
from orchestrator_layer.state_manager.checkpointing import Checkpointing, InMemoryCheckpointing
from orchestrator_layer.workflow.definition import WorkflowRegistry
from orchestrator_layer.workflow.runner import WorkflowRunner
from orchestrator_layer.state_manager.execution_context import ExecutionContext
from orchestrator_layer.state_manager.workflow_store import WorkflowInstanceStore
from orchestrator_layer.storage.audit_log import AuditLog, AuditEntry, InMemoryAuditLog
from orchestrator_layer.storage.checkpoint_store import CheckpointStore, InMemoryCheckpointStore
from orchestrator_layer.storage.pending_queue import InMemoryPendingQueue, PendingQueue
from orchestrator_layer.storage.token_store import InMemoryTokenStore, TokenStore
from orchestrator_layer.storage.workflow_state import (
    InMemoryWorkflowStateStore,
    WorkflowStateStore,
)


class OrchestrationEngine:
    """
    Orchestration Engine: coordinates State Manager, Scheduler/Dispatcher,
    Approval Subsystem, and Storage Layer.
    """

    def __init__(
        self,
        *,
        workflow_state_store: WorkflowStateStore | None = None,
        token_store: TokenStore | None = None,
        audit_log: AuditLog | None = None,
        pending_queue: PendingQueue[CallbackPayload] | None = None,
        event_queue: EventQueue | None = None,
        grouped_queues: dict[str, EventQueue] | None = None,
        workflow_groups: dict[str, str] | None = None,
        checkpointing: Checkpointing | None = None,
        checkpoint_store: CheckpointStore | None = None,
        notification_manager: NotificationManager | None = None,
        worker_pool: WorkerPool | None = None,
        workflow_runner: WorkflowRunner | None = None,
        workflow_registry: WorkflowRegistry | None = None,
    ) -> None:
        # Storage layer (all async)
        self._workflow_state = workflow_state_store or InMemoryWorkflowStateStore()
        self._token_store = token_store or InMemoryTokenStore()
        self._audit_log = audit_log or InMemoryAuditLog()
        self._pending_queue = pending_queue or InMemoryPendingQueue[CallbackPayload]()

        # State manager
        self._workflow_store = WorkflowInstanceStore(self._workflow_state)
        self._checkpointing = checkpointing or (
            Checkpointing(checkpoint_store) if checkpoint_store else InMemoryCheckpointing()
        )

        # Scheduler / dispatcher (async): single queue or per-group queues
        self._event_queue_router: EventQueueRouter | None = None
        self._worker_pools: dict[str, AsyncWorkerPool] = {}

        if grouped_queues and workflow_groups is not None:
            self._event_queue = None
            self._event_queue_router = EventQueueRouter(
                workflow_groups,
                grouped_queues,
                default_group=DEFAULT_GROUP,
            )
            default_queue = grouped_queues.get(DEFAULT_GROUP) or next(iter(grouped_queues.values()))
            self._timer_service = AsyncTimerService(default_queue.put)
            for gid, q in grouped_queues.items():
                self._worker_pools[gid] = AsyncWorkerPool(q)
            self._worker_pool = None
            _put_event = self._event_queue_router.put
        else:
            self._event_queue = event_queue or AsyncEventQueue()
            self._timer_service = AsyncTimerService(self._event_queue.put)
            self._worker_pool = worker_pool or AsyncWorkerPool(self._event_queue)
            _put_event = lambda wf_id, ev: self._event_queue.put(ev)

        self._put_event = _put_event

        # Approval subsystem
        self._token_manager = TokenManager(self._token_store)
        self._notification_manager = notification_manager or NoOpNotificationManager()
        self._callback_receiver = CallbackReceiver(
            self._token_manager,
            self._audit_log,
            self._pending_queue,
        )
        self._workflow_runner = workflow_runner or (
            WorkflowRunner(workflow_registry, put_event=self._put_event)
            if self._event_queue_router and workflow_registry
            else (WorkflowRunner(workflow_registry, event_queue=self._event_queue) if workflow_registry else None)
        )

    async def _dispatch(
        self,
        event: WorkflowEvent,
        step_executor: AsyncStepExecutor,
    ) -> None:
        """Async dispatch: load context, run step executor (await), checkpoint, audit, then enqueue next steps via workflow runner."""
        state = await self._workflow_store.get(event.instance_id)
        if not state:
            return

        # WORKFLOW_START: no step to run; let workflow runner enqueue entry step
        if event.kind == EventKind.WORKFLOW_START:
            if self._workflow_runner:
                await self._workflow_runner.handle_start(
                    event.instance_id,
                    state.workflow_id,
                    event.payload,
                )
            return

        ctx = await self._checkpointing.load_checkpoint(event.instance_id) or ExecutionContext(
            instance_id=state.instance_id,
            workflow_id=state.workflow_id,
            variables=state.context,
        )
        ctx.current_step_id = event.step_id
        await self._checkpointing.save_checkpoint(event.instance_id, ctx)
        await self._workflow_store.update(event.instance_id, current_step=event.step_id)
        step_result: StepResult = StepResult(success=False)
        try:
            step_ctx = (event.payload or {}).get("context", {})
            step_result = await step_executor(event.instance_id, event.step_id or "", step_ctx)
            if not isinstance(step_result, StepResult):
                step_result = StepResult(success=True, output=step_result if isinstance(step_result, dict) else None)
        except Exception as e:
            step_result = StepResult(success=False, error=str(e))
        finally:
            await self._audit_log.append(
                AuditEntry(
                    entry_id=str(uuid.uuid4()),
                    instance_id=event.instance_id,
                    event_type="STEP_EXECUTED",
                    payload={"step_id": event.step_id},
                    timestamp=time.time(),
                )
            )
        # Central workflow runner: enqueue next step(s) from definition or result.next_step_id
        if self._workflow_runner and event.step_id:
            await self._workflow_runner.enqueue_next(
                event.instance_id,
                state.workflow_id,
                event.step_id,
                step_result,
                event.payload,
            )

    # --- Public API ---

    @property
    def state_manager(self) -> dict[str, Any]:
        """Access to state manager components."""
        return {
            "workflow_store": self._workflow_store,
            "checkpointing": self._checkpointing,
        }

    @property
    def scheduler(self) -> dict[str, Any]:
        """Access to scheduler components."""
        return {
            "event_queue": self._event_queue,
            "event_queue_router": self._event_queue_router,
            "timer_service": self._timer_service,
            "worker_pool": self._worker_pool,
            "worker_pools": self._worker_pools if self._worker_pools else None,
            "workflow_runner": self._workflow_runner,
        }

    @property
    def approval(self) -> dict[str, Any]:
        """Access to approval subsystem."""
        return {
            "token_manager": self._token_manager,
            "notification_manager": self._notification_manager,
            "callback_receiver": self._callback_receiver,
        }

    @property
    def storage(self) -> dict[str, Any]:
        """Access to storage layer."""
        return {
            "workflow_state": self._workflow_state,
            "token_store": self._token_store,
            "audit_log": self._audit_log,
            "pending_queue": self._pending_queue,
        }

    def run_workers(
        self,
        step_executor: AsyncStepExecutor,
        num_workers: int = 4,
        num_workers_per_group: int | None = None,
    ) -> None:
        """
        Start async workers that consume from the event queue (or from each group's queue).
        Call from an async context (e.g. asyncio.run() or FastAPI).
        When using grouped queues, num_workers_per_group workers are started per group
        (defaults to num_workers if not set).
        """
        n = num_workers_per_group if num_workers_per_group is not None else num_workers
        if self._worker_pools:
            for pool in self._worker_pools.values():
                if isinstance(pool, AsyncWorkerPool):
                    pool.start_workers(
                        lambda e, ex: self._dispatch(e, ex),
                        step_executor,
                        n,
                    )
        elif isinstance(self._worker_pool, AsyncWorkerPool):
            self._worker_pool.start_workers(
                lambda e, ex: self._dispatch(e, ex),
                step_executor,
                num_workers,
            )

    async def start_workflow(self, workflow_id: str, initial_context: dict[str, Any] | None = None) -> str:
        """Start a new workflow instance; returns instance_id (async)."""
        state = await self._workflow_store.create(workflow_id, initial_context)
        ctx = ExecutionContext(
            instance_id=state.instance_id,
            workflow_id=state.workflow_id,
            variables=dict(state.context),
        )
        await self._checkpointing.save_checkpoint(state.instance_id, ctx)
        start_event = WorkflowEvent(
            event_id=str(uuid.uuid4()),
            kind=EventKind.WORKFLOW_START,
            instance_id=state.instance_id,
            payload={"context": initial_context or {}},
        )
        if self._event_queue_router:
            await self._event_queue_router.put(workflow_id, start_event)
        else:
            await self._event_queue.put(start_event)
        await self._audit_log.append(
            AuditEntry(
                entry_id=str(uuid.uuid4()),
                instance_id=state.instance_id,
                event_type="WORKFLOW_STARTED",
                payload={"workflow_id": workflow_id},
                timestamp=time.time(),
            )
        )
        return state.instance_id

    async def receive_approval_callback(self, payload: CallbackPayload) -> tuple[bool, str]:
        """Process an approval callback (e.g. from webhook) (async)."""
        return await self._callback_receiver.receive(payload)

    def shutdown(self) -> None:
        """Shutdown worker pool(s) and timers."""
        if self._worker_pools:
            for pool in self._worker_pools.values():
                pool.shutdown()
        if self._worker_pool:
            self._worker_pool.shutdown()
