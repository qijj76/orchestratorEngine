"""Token Manager: create and manage approval tokens."""

from __future__ import annotations

import time
import uuid
from typing import Any

from orchestrator_layer.storage.token_store import (
    ApprovalToken,
    TokenStatus,
    TokenStore,
)


class TokenManager:
    """Creates and validates approval tokens; delegates persistence to TokenStore."""

    def __init__(self, token_store: TokenStore) -> None:
        self._store = token_store

    async def create_token(
        self,
        instance_id: str,
        step_id: str,
        payload: dict[str, Any] | None = None,
        ttl_seconds: float | None = None,
    ) -> ApprovalToken:
        """Create a new approval token for a step."""
        token_id = str(uuid.uuid4())
        expires_at = (time.time() + ttl_seconds) if ttl_seconds else None
        token = ApprovalToken(
            token_id=token_id,
            instance_id=instance_id,
            step_id=step_id,
            status=TokenStatus.PENDING,
            payload=payload,
            expires_at=expires_at,
        )
        await self._store.put(token)
        return token

    async def get_token(self, token_id: str) -> ApprovalToken | None:
        """Retrieve token by id."""
        return await self._store.get(token_id)

    async def approve(self, token_id: str) -> bool:
        """Mark token as approved. Returns False if not found or not pending."""
        token = await self._store.get(token_id)
        if not token or token.status != TokenStatus.PENDING:
            return False
        if token.expires_at and time.time() > token.expires_at:
            token.status = TokenStatus.EXPIRED
            await self._store.put(token)
            return False
        token.status = TokenStatus.APPROVED
        await self._store.put(token)
        return True

    async def reject(self, token_id: str) -> bool:
        """Mark token as rejected. Returns False if not found or not pending."""
        token = await self._store.get(token_id)
        if not token or token.status != TokenStatus.PENDING:
            return False
        token.status = TokenStatus.REJECTED
        await self._store.put(token)
        return True

    async def list_pending(self, instance_id: str) -> list[ApprovalToken]:
        """List pending tokens for an instance."""
        return await self._store.list_pending_for_instance(instance_id)
