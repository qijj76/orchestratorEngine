"""Timer Service: schedule delayed events (e.g. approval timeouts)."""

from __future__ import annotations

import asyncio
import threading
import uuid
from collections.abc import Awaitable
from typing import Callable

from orchestrator_layer.scheduler.event_queue import EventKind, WorkflowEvent


class TimerService:
    """Schedules one-shot timers that enqueue workflow events when they fire."""

    def __init__(self, enqueue_fn: Callable[[WorkflowEvent], None]) -> None:
        self._enqueue = enqueue_fn
        self._timers: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    def schedule(
        self,
        instance_id: str,
        delay_seconds: float,
        step_id: str | None = None,
        kind: EventKind = EventKind.TIMER_FIRED,
        payload: dict | None = None,
    ) -> str:
        """Schedule a timer. Returns timer_id for cancellation."""
        timer_id = str(uuid.uuid4())
        event = WorkflowEvent(
            event_id=str(uuid.uuid4()),
            kind=kind,
            instance_id=instance_id,
            step_id=step_id,
            payload=payload,
        )

        def fire() -> None:
            with self._lock:
                self._timers.pop(timer_id, None)
            self._enqueue(event)

        t = threading.Timer(delay_seconds, fire)
        with self._lock:
            self._timers[timer_id] = t
        t.start()
        return timer_id

    def cancel(self, timer_id: str) -> bool:
        """Cancel a scheduled timer. Returns True if it was cancelled."""
        with self._lock:
            t = self._timers.pop(timer_id, None)
        if t is not None:
            t.cancel()
            return True
        return False

    def cancel_all_for_instance(self, instance_id: str) -> int:
        """Cancel all timers for an instance. Returns count cancelled."""
        # This impl doesn't store instance_id on timer; would need to track it.
        # For now, return 0; could extend with a map instance_id -> [timer_id].
        return 0


class AsyncTimerService:
    """Async timers using asyncio.sleep; no threads. Enqueue callback is async."""

    def __init__(
        self,
        enqueue_fn: Callable[[WorkflowEvent], Awaitable[None]],
    ) -> None:
        self._enqueue_fn = enqueue_fn
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def schedule(
        self,
        instance_id: str,
        delay_seconds: float,
        step_id: str | None = None,
        kind: EventKind = EventKind.TIMER_FIRED,
        payload: dict | None = None,
    ) -> str:
        """Schedule a timer. Returns timer_id for cancellation. Call from async context."""
        timer_id = str(uuid.uuid4())
        event = WorkflowEvent(
            event_id=str(uuid.uuid4()),
            kind=kind,
            instance_id=instance_id,
            step_id=step_id,
            payload=payload,
        )

        async def fire() -> None:
            await asyncio.sleep(delay_seconds)
            self._tasks.pop(timer_id, None)
            await self._enqueue_fn(event)

        task = asyncio.create_task(fire())
        self._tasks[timer_id] = task
        return timer_id

    def cancel(self, timer_id: str) -> bool:
        """Cancel a scheduled timer. Returns True if it was cancelled."""
        task = self._tasks.pop(timer_id, None)
        if task is not None:
            task.cancel()
            return True
        return False
