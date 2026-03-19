"""
Integration tests: full engine + workflow registry + step execution.
"""

import asyncio
from typing import Any

import pytest

from orchestrator_layer import OrchestrationEngine, WorkflowDef, WorkflowRegistry
from orchestrator_layer.approval.callback_receiver import CallbackPayload
from orchestrator_layer.scheduler.event_queue import AsyncEventQueue, DEFAULT_GROUP
from orchestrator_layer.scheduler.worker_pool import AsyncStepExecutor, StepResult


@pytest.fixture
def workflow_registry():
    """Linear workflow: validate -> approve -> notify -> end."""
    registry = WorkflowRegistry()
    registry.register(
        WorkflowDef(
            workflow_id="order_approval",
            entry_step_id="validate",
            transitions={
                "validate": ["approve"],
                "approve": ["notify"],
                "notify": [],
            },
        )
    )
    return registry


@pytest.fixture
def executed_steps():
    """Shared list to record step execution order and context."""
    return []


@pytest.fixture
def engine(workflow_registry):
    """Engine with in-memory stores and workflow registry."""
    return OrchestrationEngine(workflow_registry=workflow_registry)


@pytest.mark.asyncio
async def test_workflow_runs_steps_in_order(engine, workflow_registry, executed_steps):
    """Start workflow and assert steps run in definition order: validate -> approve -> notify."""
    async def step_executor(instance_id: str, step_id: str, context: dict[str, Any]) -> StepResult:
        executed_steps.append((instance_id, step_id, dict(context)))
        return StepResult(success=True)

    engine.run_workers(step_executor, num_workers=2)
    instance_id = await engine.start_workflow("order_approval", {"order_id": "ORD-1"})

    # Allow workers to process all events
    await asyncio.sleep(0.3)
    engine.shutdown()

    step_ids = [s[1] for s in executed_steps]
    assert step_ids == ["validate", "approve", "notify"]
    assert all(s[0] == instance_id for s in executed_steps)
    assert executed_steps[0][2].get("order_id") == "ORD-1"


@pytest.mark.asyncio
async def test_workflow_state_and_audit(engine, workflow_registry, executed_steps):
    """After run, instance state is updated and audit log contains step events."""
    async def step_executor(instance_id: str, step_id: str, context: dict[str, Any]) -> StepResult:
        executed_steps.append(step_id)
        return StepResult(success=True)

    engine.run_workers(step_executor, num_workers=2)
    instance_id = await engine.start_workflow("order_approval", {})
    await asyncio.sleep(0.3)
    engine.shutdown()

    state = await engine.state_manager["workflow_store"].get(instance_id)
    assert state is not None
    assert state.workflow_id == "order_approval"
    assert state.current_step == "notify"

    entries = await engine.storage["audit_log"].list_by_instance(instance_id, limit=20)
    event_types = [e.event_type for e in entries]
    assert "WORKFLOW_STARTED" in event_types
    assert event_types.count("STEP_EXECUTED") == 3


@pytest.mark.asyncio
async def test_branching_via_step_result_next_step_id(engine, workflow_registry, executed_steps):
    """StepResult.next_step_id overrides definition for branching."""
    registry = WorkflowRegistry()
    registry.register(
        WorkflowDef(
            workflow_id="branch_wf",
            entry_step_id="decide",
            transitions={
                "decide": ["default_path"],
                "default_path": ["end"],
                "other_path": ["end"],
                "end": [],
            },
        )
    )
    engine_branch = OrchestrationEngine(workflow_registry=registry)

    async def step_executor(instance_id: str, step_id: str, context: dict[str, Any]) -> StepResult:
        executed_steps.append(step_id)
        if step_id == "decide":
            return StepResult(success=True, next_step_id="other_path")
        return StepResult(success=True)

    engine_branch.run_workers(step_executor, num_workers=2)
    await engine_branch.start_workflow("branch_wf", {})
    await asyncio.sleep(0.2)
    engine_branch.shutdown()

    assert executed_steps == ["decide", "other_path", "end"]


@pytest.mark.asyncio
async def test_approval_callback_received(engine):
    """Receive approval callback returns token_not_found for unknown token."""
    ok, msg = await engine.receive_approval_callback(
        CallbackPayload(token_id="nonexistent", approved=True)
    )
    assert ok is False
    assert "token" in msg.lower()


@pytest.mark.asyncio
async def test_unknown_workflow_id_no_crash(workflow_registry, executed_steps):
    """Starting a workflow not in registry does not enqueue steps; no crash."""
    engine = OrchestrationEngine(workflow_registry=workflow_registry)
    async def step_executor(instance_id: str, step_id: str, context: dict[str, Any]) -> StepResult:
        executed_steps.append(step_id)
        return StepResult(success=True)

    engine.run_workers(step_executor, num_workers=2)
    instance_id = await engine.start_workflow("unknown_workflow", {"x": 1})
    await asyncio.sleep(0.15)
    engine.shutdown()

    # No steps run because no workflow runner definition for unknown_workflow
    assert len(executed_steps) == 0
    state = await engine.state_manager["workflow_store"].get(instance_id)
    assert state is not None
    assert state.workflow_id == "unknown_workflow"


@pytest.mark.asyncio
async def test_grouped_queues_each_group_has_own_queue(executed_steps):
    """With grouped_queues and workflow_groups, events go to the queue for the workflow's group."""
    registry = WorkflowRegistry()
    registry.register(
        WorkflowDef(
            workflow_id="wf_default",
            entry_step_id="step_a",
            transitions={"step_a": ["step_b"], "step_b": []},
        )
    )
    registry.register(
        WorkflowDef(
            workflow_id="wf_priority",
            entry_step_id="step_x",
            transitions={"step_x": ["step_y"], "step_y": []},
        )
    )
    queue_default = AsyncEventQueue()
    queue_priority = AsyncEventQueue()
    grouped_queues = {DEFAULT_GROUP: queue_default, "priority": queue_priority}
    workflow_groups = {"wf_default": DEFAULT_GROUP, "wf_priority": "priority"}

    engine = OrchestrationEngine(
        workflow_registry=registry,
        grouped_queues=grouped_queues,
        workflow_groups=workflow_groups,
    )
    steps_by_instance: list[tuple[str, str]] = []

    async def step_executor(instance_id: str, step_id: str, context: dict[str, Any]) -> StepResult:
        steps_by_instance.append((instance_id, step_id))
        return StepResult(success=True)

    engine.run_workers(step_executor, num_workers=2, num_workers_per_group=1)
    id_default = await engine.start_workflow("wf_default", {})
    id_priority = await engine.start_workflow("wf_priority", {})
    await asyncio.sleep(0.4)
    engine.shutdown()

    steps_default = [s for i, s in steps_by_instance if i == id_default]
    steps_priority = [s for i, s in steps_by_instance if i == id_priority]
    assert steps_default == ["step_a", "step_b"]
    assert steps_priority == ["step_x", "step_y"]
