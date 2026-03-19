"""Workflow definition: DAG of steps per workflow."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class WorkflowDef:
    """
    Definition of a workflow: entry step and transitions (current_step -> next step ids).
    Use transitions for linear or DAG flows; StepResult.next_step_id can override for branching.
    """

    workflow_id: str
    entry_step_id: str
    transitions: dict[str, list[str]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_next_steps(self, current_step_id: str, result_next_step_id: str | None = None) -> list[str]:
        """
        Return list of next step ids after current_step_id.
        If result_next_step_id is set (from StepResult), use that single step if non-empty.
        Otherwise use transitions[current_step_id]; if missing, return [] (workflow end).
        """
        if result_next_step_id:
            return [result_next_step_id]
        return self.transitions.get(current_step_id, [])


class WorkflowRegistry:
    """Registry of workflow definitions by workflow_id."""

    def __init__(self) -> None:
        self._workflows: dict[str, WorkflowDef] = {}

    def register(self, defn: WorkflowDef) -> None:
        """Register a workflow definition."""
        self._workflows[defn.workflow_id] = defn

    def get(self, workflow_id: str) -> WorkflowDef | None:
        """Get workflow definition by id."""
        return self._workflows.get(workflow_id)

    def get_next_steps(
        self,
        workflow_id: str,
        current_step_id: str,
        result_next_step_id: str | None = None,
    ) -> list[str]:
        """
        Get next step ids for a workflow instance.
        Returns empty list if workflow unknown or no next steps.
        """
        defn = self._workflows.get(workflow_id)
        if not defn:
            return []
        return defn.get_next_steps(current_step_id, result_next_step_id)

    def get_entry_step(self, workflow_id: str) -> str | None:
        """Get the entry (first) step id for a workflow. None if not registered."""
        defn = self._workflows.get(workflow_id)
        return defn.entry_step_id if defn else None
