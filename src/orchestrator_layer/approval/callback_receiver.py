"""Callback Receiver: receive and route approval callbacks (e.g. webhook)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Awaitable

import time
import uuid

from orchestrator_layer.approval.token_manager import TokenManager
from orchestrator_layer.storage.audit_log import AuditLog, AuditEntry
from orchestrator_layer.storage.pending_queue import PendingItem, PendingQueue
from orchestrator_layer.storage.token_store import TokenStatus


@dataclass
class CallbackPayload:
    """Payload of an approval callback."""

    token_id: str
    approved: bool
    comment: str | None = None
    metadata: dict[str, Any] | None = None


CallbackHandler = Callable[[str, CallbackPayload], Awaitable[None] | None]


class CallbackReceiver:
    """Receives approval callbacks (e.g. webhook), validates token, and triggers handling."""

    def __init__(
        self,
        token_manager: TokenManager,
        audit_log: AuditLog,
        pending_queue: PendingQueue[CallbackPayload] | None = None,
    ) -> None:
        self._tokens = token_manager
        self._audit = audit_log
        self._pending = pending_queue
        self._handlers: list[CallbackHandler] = []

    def register_handler(self, handler: CallbackHandler) -> None:
        """Register a handler to be invoked when a callback is processed."""
        self._handlers.append(handler)

    async def receive(self, payload: CallbackPayload) -> tuple[bool, str]:
        """
        Process an approval callback. Returns (success, message).
        If pending_queue is set, enqueues for async processing; otherwise applies immediately.
        """
        token = await self._tokens.get_token(payload.token_id)
        if not token:
            return False, "token_not_found"
        if token.status != TokenStatus.PENDING:
            return False, "token_already_processed"

        if self._pending is not None:
            item = PendingItem(
                item_id=str(uuid.uuid4()),
                payload=payload,
                priority=1,
            )
            await self._pending.enqueue(item)
            await self._audit.append(
                AuditEntry(
                    entry_id=str(uuid.uuid4()),
                    instance_id=token.instance_id,
                    event_type="APPROVAL_CALLBACK_QUEUED",
                    payload={
                        "token_id": payload.token_id,
                        "approved": payload.approved,
                    },
                    timestamp=time.time(),
                )
            )
            return True, "queued"

        return await self._apply_callback(payload)

    async def _apply_callback(self, payload: CallbackPayload) -> tuple[bool, str]:
        """Apply callback: update token and run handlers."""
        token = await self._tokens.get_token(payload.token_id)
        if not token:
            return False, "token_not_found"
        if payload.approved:
            ok = await self._tokens.approve(payload.token_id)
        else:
            ok = await self._tokens.reject(payload.token_id)
        if not ok:
            return False, "token_invalid_or_expired"
        token = await self._tokens.get_token(payload.token_id)
        await self._audit.append(
            AuditEntry(
                entry_id=str(uuid.uuid4()),
                instance_id=token.instance_id,
                event_type="APPROVAL_CALLBACK_APPLIED",
                payload={
                    "token_id": payload.token_id,
                    "approved": payload.approved,
                    "comment": payload.comment,
                },
                timestamp=time.time(),
            )
        )
        for h in self._handlers:
            try:
                out = h(token.instance_id, payload)
                if hasattr(out, "__await__"):
                    await out
            except Exception:
                pass
        return True, "processed"
