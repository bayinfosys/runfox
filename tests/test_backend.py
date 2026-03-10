"""
test_backend.py -- Backend named operations.

Tests cover the Backend lifecycle methods directly, without running a
full workflow. All tests use the default InMemoryStore + InProcessRunner
construction via Backend(executor=...).
"""

import pytest
from conftest import FIB_SPEC, fib_executor

import runfox as rfx
from runfox.backend import Backend
from runfox.status import StepStatus, WorkflowStatus


class TestBackend:

    def test_create_returns_id(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        assert isinstance(wf_id, str) and len(wf_id) > 0

    def test_load_returns_isolated_copy(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        record1 = backend.load(wf_id)
        record1.status = WorkflowStatus.IN_PROGRESS
        assert backend.load(wf_id).status == WorkflowStatus.PENDING

    def test_initial_step_statuses_all_ready(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        for step in backend.load(wf_id).steps.values():
            assert step.status == StepStatus.READY

    def test_mark_in_progress(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.mark_in_progress(wf_id, "f0")
        assert backend.load(wf_id).steps["f0"].status == StepStatus.IN_PROGRESS

    def test_write_step_output_and_mark_complete(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.write_step_output(wf_id, "f0", {"value": 0})
        backend.mark_complete(wf_id, "f0")
        record = backend.load(wf_id)
        assert record.steps["f0"].status == StepStatus.COMPLETE
        assert record.steps["f0"].output == {"value": 0}

    def test_unknown_workflow_raises(self):
        backend = Backend(executor=fib_executor)
        with pytest.raises(KeyError):
            backend.load("nonexistent")

    def test_merge_workflow_state(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.merge_workflow_state(wf_id, {"x": 1})
        backend.merge_workflow_state(wf_id, {"y": 2})
        assert backend.load(wf_id).state == {"x": 1, "y": 2}

    def test_reset_step_increments_run_id(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.reset_step(wf_id, "f0")
        record = backend.load(wf_id)
        assert record.steps["f0"].run_id == 1
        assert record.steps["f0"].status == StepStatus.READY

    def test_reset_for_retry_sets_retry_status(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.reset_for_retry(wf_id, "f0")
        record = backend.load(wf_id)
        assert record.steps["f0"].status == StepStatus.RETRY
        assert record.steps["f0"].run_id == 1

    def test_write_workflow_outcome_marks_complete(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.write_workflow_outcome(wf_id, {"result": 42})
        record = backend.load(wf_id)
        assert record.status == WorkflowStatus.COMPLETE
        assert record.outcome == {"result": 42}

    def test_write_workflow_outcome_preserves_halted(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.mark_halted(wf_id, "f0")
        backend.write_workflow_outcome(wf_id, {"reason": "stopped"})
        assert backend.load(wf_id).status == WorkflowStatus.HALTED

    def test_on_state_change_callback_fires(self):
        calls = []

        def callback(wf_exec_id, previous, current, event):
            calls.append((previous.copy(), current.copy()))

        backend = Backend(executor=fib_executor, on_state_change=callback)
        wf_id = backend.create(FIB_SPEC)
        backend.merge_workflow_state(wf_id, {"x": 1})
        backend.merge_workflow_state(wf_id, {"y": 2})
        assert len(calls) == 2
        assert calls[0] == ({}, {"x": 1})
        assert calls[1] == ({"x": 1}, {"x": 1, "y": 2})

    def test_on_state_change_not_called_on_empty_output(self):
        calls = []

        def callback(wf_exec_id, previous, current):
            calls.append(current)

        backend = Backend(executor=fib_executor, on_state_change=callback)
        wf_id = backend.create(FIB_SPEC)
        backend.merge_workflow_state(wf_id, {})
        backend.merge_workflow_state(wf_id, None)
        assert calls == []
