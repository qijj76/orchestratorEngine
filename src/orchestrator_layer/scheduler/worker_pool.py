"""Worker Pool: execute workflow steps (dispatcher)."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from orchestrator_layer.scheduler.event_queue import EventQueue, WorkflowEvent


@dataclass
class StepResult:
    """Result of executing a step."""

    success: bool
    next_step_id: str | None = None
    wait_approval: bool = False
    error: str | None = None
    output: dict[str, Any] | None = None


StepExecutor = Callable[[str, str, dict[str, Any]], StepResult | Awaitable[StepResult]]

# Async-only step executor: use with AsyncWorkerPool for I/O-bound steps.
AsyncStepExecutor = Callable[[str, str, dict[str, Any]], Awaitable[StepResult]]


class WorkerPool(ABC):
    """Abstract worker pool for executing workflow steps."""

    @abstractmethod
    def submit(self, event: WorkflowEvent, executor: StepExecutor) -> None:
        """Submit event for execution. Execution may be async."""
        ...

    @abstractmethod
    def shutdown(self) -> None:
        """Shutdown the pool."""
        ...


class ThreadPoolWorkerPool(WorkerPool):
    """Thread-pool based worker pool; runs step executor in a thread."""

    def __init__(self, max_workers: int = 4) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._executor_fn: StepExecutor | None = None

    def set_executor(self, fn: StepExecutor) -> None:
        """Set the step executor function."""
        self._executor_fn = fn

    def submit(self, event: WorkflowEvent, executor: StepExecutor | None = None) -> None:
        fn = executor or self._executor_fn
        if not fn:
            return
        self._executor.submit(self._run, event, fn)

    def _run(self, event: WorkflowEvent, fn: StepExecutor) -> None:
        try:
            ctx = (event.payload or {}).get("context", {})
            result = fn(event.instance_id, event.step_id or "", ctx)
            if hasattr(result, "__await__"):
                import asyncio
                asyncio.get_event_loop().run_until_complete(result)
        except Exception:
            pass

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)


class AsyncWorkerPool(WorkerPool):
    """
    Async worker pool: N asyncio tasks pull from an event queue and await
    the step executor. No threads; use for high concurrency of I/O-bound steps.
    """

    def __init__(self, event_queue: EventQueue) -> None:
        self._event_queue = event_queue
        self._tasks: list[asyncio.Task[None]] = []
        self._shutdown = asyncio.Event()

    def start_workers(
        self,
        dispatch_fn: Callable[
            [WorkflowEvent, AsyncStepExecutor],
            Awaitable[None],
        ],
        step_executor: AsyncStepExecutor,
        num_workers: int = 4,
    ) -> None:
        """Start num_workers async tasks that consume events and await dispatch."""
        for _ in range(num_workers):
            t = asyncio.create_task(
                self._worker_loop(dispatch_fn, step_executor),
            )
            self._tasks.append(t)

    async def _worker_loop(
        self,
        dispatch_fn: Callable[
            [WorkflowEvent, AsyncStepExecutor],
            Awaitable[None],
        ],
        step_executor: AsyncStepExecutor,
    ) -> None:
        while not self._shutdown.is_set():
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0,
                )
            except asyncio.TimeoutError:
                continue
            try:
                await dispatch_fn(event, step_executor)
            except Exception:
                pass

    def submit(self, event: WorkflowEvent, executor: StepExecutor | None = None) -> None:
        """No-op for async pool; events are consumed by worker tasks."""
        pass

    def shutdown(self) -> None:
        """Signal workers to stop and cancel tasks."""
        self._shutdown.set()
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()
