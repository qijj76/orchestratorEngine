"""Pending Queue: queue of items awaiting processing (e.g. approval callbacks)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, TypeVar


T = TypeVar("T")


@dataclass
class PendingItem(Generic[T]):
    """Item in the pending queue with optional priority."""

    item_id: str
    payload: T
    priority: int = 0  # higher = sooner
    created_at: float = 0.0  # epoch, set by implementation


class PendingQueue(ABC, Generic[T]):
    """Abstract queue for pending items (async)."""

    @abstractmethod
    async def enqueue(self, item: PendingItem[T]) -> None:
        """Add item to the queue."""
        ...

    @abstractmethod
    async def dequeue(self) -> PendingItem[T] | None:
        """Remove and return next item (by priority, then FIFO)."""
        ...

    @abstractmethod
    async def peek(self) -> PendingItem[T] | None:
        """Return next item without removing."""
        ...

    @abstractmethod
    async def size(self) -> int:
        """Current queue size."""
        ...


class InMemoryPendingQueue(PendingQueue[T]):
    """In-memory implementation of pending queue."""

    def __init__(self) -> None:
        self._items: list[PendingItem[T]] = []
        self._counter = 0

    async def enqueue(self, item: PendingItem[T]) -> None:
        import time

        if not hasattr(item, "created_at") or item.created_at == 0.0:
            item.created_at = time.time()
        self._items.append(item)
        self._items.sort(key=lambda x: (-x.priority, x.created_at))

    async def dequeue(self) -> PendingItem[T] | None:
        if not self._items:
            return None
        return self._items.pop(0)

    async def peek(self) -> PendingItem[T] | None:
        if not self._items:
            return None
        return self._items[0]

    async def size(self) -> int:
        return len(self._items)
