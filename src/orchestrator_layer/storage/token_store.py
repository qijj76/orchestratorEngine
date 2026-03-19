"""Token Store: persistence for approval tokens."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any


class TokenStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


@dataclass
class ApprovalToken:
    """Approval token for a workflow step."""

    token_id: str
    instance_id: str
    step_id: str
    status: TokenStatus = TokenStatus.PENDING
    payload: dict[str, Any] | None = None
    expires_at: float | None = None  # epoch timestamp


class TokenStore(ABC):
    """Abstract store for approval tokens (async)."""

    @abstractmethod
    async def get(self, token_id: str) -> ApprovalToken | None:
        """Get token by id."""
        ...

    @abstractmethod
    async def put(self, token: ApprovalToken) -> None:
        """Save or update token."""
        ...

    @abstractmethod
    async def list_pending_for_instance(self, instance_id: str) -> list[ApprovalToken]:
        """List pending tokens for a workflow instance."""
        ...

    @abstractmethod
    async def list_by_step(self, instance_id: str, step_id: str) -> list[ApprovalToken]:
        """List tokens for a specific step."""
        ...


class InMemoryTokenStore(TokenStore):
    """In-memory implementation of token store."""

    def __init__(self) -> None:
        self._store: dict[str, ApprovalToken] = {}

    async def get(self, token_id: str) -> ApprovalToken | None:
        return self._store.get(token_id)

    async def put(self, token: ApprovalToken) -> None:
        self._store[token.token_id] = token

    async def list_pending_for_instance(self, instance_id: str) -> list[ApprovalToken]:
        return [
            t
            for t in self._store.values()
            if t.instance_id == instance_id and t.status == TokenStatus.PENDING
        ]

    async def list_by_step(self, instance_id: str, step_id: str) -> list[ApprovalToken]:
        return [
            t
            for t in self._store.values()
            if t.instance_id == instance_id and t.step_id == step_id
        ]
