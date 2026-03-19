"""Notification Manager: send approval requests and status notifications."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from orchestrator_layer.storage.token_store import ApprovalToken


@dataclass
class NotificationTarget:
    """Target for a notification (e.g. user, channel, webhook)."""

    channel: str  # e.g. "email", "webhook", "slack"
    address: str
    extra: dict[str, Any] | None = None


class NotificationManager(ABC):
    """Sends approval requests and notifications; abstract for pluggable backends."""

    @abstractmethod
    def send_approval_request(
        self,
        token: ApprovalToken,
        targets: list[NotificationTarget],
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Notify targets that an approval is requested."""
        ...

    @abstractmethod
    def send_approval_result(
        self,
        token: ApprovalToken,
        targets: list[NotificationTarget],
        approved: bool,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Notify targets of approval/rejection result."""
        ...


class NoOpNotificationManager(NotificationManager):
    """No-op implementation; does not send any notifications."""

    def send_approval_request(
        self,
        token: ApprovalToken,
        targets: list[NotificationTarget],
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        pass

    def send_approval_result(
        self,
        token: ApprovalToken,
        targets: list[NotificationTarget],
        approved: bool,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        pass
