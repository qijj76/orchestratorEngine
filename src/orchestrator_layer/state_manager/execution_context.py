"""Execution Context: in-memory context for the current execution (variables, step data)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExecutionContext:
    """Mutable execution context for a workflow run (step inputs/outputs, variables)."""

    instance_id: str
    workflow_id: str
    variables: dict[str, Any] = field(default_factory=dict)
    step_outputs: dict[str, Any] = field(default_factory=dict)  # step_id -> output
    current_step_id: str | None = None

    def get_variable(self, key: str, default: Any = None) -> Any:
        return self.variables.get(key, default)

    def set_variable(self, key: str, value: Any) -> None:
        self.variables[key] = value

    def set_step_output(self, step_id: str, output: Any) -> None:
        self.step_outputs[step_id] = output

    def get_step_output(self, step_id: str) -> Any:
        return self.step_outputs.get(step_id)

    def to_dict(self) -> dict[str, Any]:
        """Serialize for checkpointing/storage."""
        return {
            "instance_id": self.instance_id,
            "workflow_id": self.workflow_id,
            "variables": dict(self.variables),
            "step_outputs": dict(self.step_outputs),
            "current_step_id": self.current_step_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExecutionContext:
        """Restore from checkpoint/storage."""
        return cls(
            instance_id=data["instance_id"],
            workflow_id=data["workflow_id"],
            variables=data.get("variables", {}),
            step_outputs=data.get("step_outputs", {}),
            current_step_id=data.get("current_step_id"),
        )
