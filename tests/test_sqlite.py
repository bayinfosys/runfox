"""
test_sqlite.py -- Backend and Workflow behaviour against SqliteStore.

Exercises the full serialisation/deserialisation round-trip via
SqliteStore + InProcessRunner + InProcessWorker. The executor is
decoupled from both; InProcessWorker drives execution locally,
mirroring what an external worker does against the tasks table.

Each test gets a fresh temporary database file, deleted on teardown.
"""

import os

import pytest
from conftest import (FIB6_YAML, FIB_SPEC, HALT_SPEC, SET_LOOP_YAML,
                      fib_executor, halting_executor, make_sqlite_backend)

import runfox as rfx
from runfox import Complete, Halt
from runfox.backend import Backend, InProcessRunner, InProcessWorker
from runfox.backend.sqlite_store import SqliteStore
from runfox.status import StepStatus, WorkflowStatus
from runfox.results import DispatchJob

import tempfile

from runfox.backend.sqlite_runner import SqliteRunner

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _SqliteTest:
    """
    Base for test classes that need a SQLite backend.

    Subclasses call self._make(executor) to get (backend, worker).
    The database file is deleted in teardown regardless of test outcome.
    """

    def setup_method(self, method):
        self._db_paths = []

    def teardown_method(self, method):
        for path in self._db_paths:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    def _make(self, executor):
        backend, worker, db_path = make_sqlite_backend(executor)
        self._db_paths.append(db_path)
        return backend, worker


# ---------------------------------------------------------------------------
# Store round-trip
# ---------------------------------------------------------------------------


class TestSqliteStore(_SqliteTest):
    """
    SqliteStore serialisation/deserialisation.

    These tests exercise the store layer directly, without running a
    full workflow, to verify that every field survives a write/load
    round-trip through SQLite.
    """

    def test_create_and_load(self):
        backend, _ = self._make(fib_executor)
        wf_id = backend.create(FIB_SPEC)
        record = backend.load(wf_id)
        assert record.status == WorkflowStatus.PENDING
        assert set(record.steps.keys()) == {"f0", "f1", "f2", "f3", "f4"}

    def test_load_returns_isolated_copy(self):
        backend, _ = self._make(fib_executor)
        wf_id = backend.create(FIB_SPEC)
        record = backend.load(wf_id)
        record.status = WorkflowStatus.IN_PROGRESS
        assert backend.load(wf_id).status == WorkflowStatus.PENDING

    def test_step_status_survives_round_trip(self):
        backend, _ = self._make(fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.mark_in_progress(wf_id, "f0")
        assert backend.load(wf_id).steps["f0"].status == StepStatus.IN_PROGRESS

    def test_step_output_survives_round_trip(self):
        backend, _ = self._make(fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.write_step_output(wf_id, "f0", {"value": 99})
        assert backend.load(wf_id).steps["f0"].output == {"value": 99}

    def test_state_survives_round_trip(self):
        backend, _ = self._make(fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.merge_workflow_state(wf_id, {"x": 1, "y": [2, 3]})
        assert backend.load(wf_id).state == {"x": 1, "y": [2, 3]}

    def test_outcome_survives_round_trip(self):
        backend, _ = self._make(fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.write_workflow_outcome(wf_id, {"result": 42})
        record = backend.load(wf_id)
        assert record.outcome == {"result": 42}
        assert record.status == WorkflowStatus.COMPLETE

    def test_run_id_survives_round_trip(self):
        backend, _ = self._make(fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.reset_step(wf_id, "f0")
        backend.reset_step(wf_id, "f0")
        assert backend.load(wf_id).steps["f0"].run_id == 2

    def test_retry_status_survives_round_trip(self):
        backend, _ = self._make(fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.reset_for_retry(wf_id, "f0")
        record = backend.load(wf_id)
        assert record.steps["f0"].status == StepStatus.RETRY
        assert record.steps["f0"].run_id == 1

    def test_unknown_id_raises(self):
        backend, _ = self._make(fib_executor)
        with pytest.raises(KeyError):
            backend.load("nonexistent")


# ---------------------------------------------------------------------------
# Workflow execution against SQLite
# ---------------------------------------------------------------------------


class TestSqliteExecution(_SqliteTest):
    """
    Full workflow runs against SqliteStore + InProcessWorker.

    Verifies that the execution logic (dependency resolution, branch
    evaluation, set-action loops) produces the same results as
    InMemoryStore, confirming that store substitution is transparent.
    """

    def test_fib_dict_spec(self):
        backend, worker = self._make(fib_executor)
        result = rfx.Workflow.from_dict(FIB_SPEC, backend).run(worker=worker)
        assert isinstance(result, Complete)
        assert result.outcome["result"] == 3

    def test_fib_yaml_spec(self):
        backend, worker = self._make(fib_executor)
        result = rfx.Workflow.from_yaml(FIB6_YAML, backend).run(worker=worker)
        assert isinstance(result, Complete)
        assert result.outcome["result"] == 8

    def test_all_steps_complete_after_run(self):
        backend, worker = self._make(fib_executor)
        wf = rfx.Workflow.from_dict(FIB_SPEC, backend)
        wf.run(worker=worker)
        for status in wf.step_statuses.values():
            assert status == StepStatus.COMPLETE

    def test_halt_fires(self):
        backend, worker = self._make(halting_executor)
        result = rfx.Workflow.from_dict(HALT_SPEC, backend).run(worker=worker)
        assert isinstance(result, Halt)
        assert result.result == {"status": "rejected", "reason": "unsafe"}

    def test_status_halted_after_halt(self):
        backend, worker = self._make(halting_executor)
        wf = rfx.Workflow.from_dict(HALT_SPEC, backend)
        wf.run(worker=worker)
        assert wf.status == WorkflowStatus.HALTED

    def test_set_loop_runs_to_completion(self):
        backend, worker = self._make(
            lambda op, inputs: {"n": (inputs.get("n") or 0) + 1}
        )
        result = rfx.Workflow.from_yaml(SET_LOOP_YAML, backend).run(worker=worker)
        assert isinstance(result, Complete)
        assert result.outcome["result"] == 5

    def test_set_loop_run_id_increments(self):
        backend, worker = self._make(
            lambda op, inputs: {"n": (inputs.get("n") or 0) + 1}
        )
        wf = rfx.Workflow.from_yaml(SET_LOOP_YAML, backend)
        wf.run(worker=worker)
        assert backend.load(wf.id).steps["count"].run_id == 4

    def test_resume_after_create(self):
        backend, worker = self._make(fib_executor)
        wf_id = backend.create(FIB_SPEC)
        wf = rfx.Workflow.resume(wf_id, backend)
        result = wf.run(worker=worker)
        assert isinstance(result, Complete)
        assert result.outcome["result"] == 3


class TestSqliteRunner(_SqliteTest):
    """
    SqliteRunner dispatch/gather/take cycle.
    Verifies the tasks table protocol independently of full workflow runs.
    """

    def _make_job(self, op="s1", inputs=None, run_id=0):
        return DispatchJob(
            workflow_execution_id="wf#exec",
            op=op,
            inputs=inputs or {},
            run_id=run_id,
        )

    def _make_runner(self):
        db_fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        self._db_paths.append(db_path)
        return SqliteRunner(db_path), db_path

    def test_dispatch_then_list_pending(self):
        runner, _ = self._make_runner()
        job = self._make_job()
        runner.dispatch("wf#exec", [job])
        pending = runner.list_pending_jobs()
        assert len(pending) == 1
        assert pending[0].op == "s1"
        assert pending[0].workflow_execution_id == "wf#exec"

    def test_list_pending_is_nondestructive(self):
        runner, _ = self._make_runner()
        runner.dispatch("wf#exec", [self._make_job()])
        runner.list_pending_jobs()
        runner.list_pending_jobs()
        assert len(runner.list_pending_jobs()) == 1

    def test_take_pending_marks_started(self):
        runner, db_path = self._make_runner()
        runner.dispatch("wf#exec", [self._make_job()])
        taken = runner.take_pending_jobs()
        assert len(taken) == 1
        # second take should return nothing -- row is now STARTED
        assert runner.take_pending_jobs() == []

    def test_take_pending_returns_dispatchjob(self):
        runner, _ = self._make_runner()
        runner.dispatch("wf#exec", [self._make_job(run_id=3)])
        taken = runner.take_pending_jobs()
        assert isinstance(taken[0], DispatchJob)
        assert taken[0].run_id == 3

    def test_submit_work_result_then_gather(self):
        runner, _ = self._make_runner()
        runner.dispatch("wf#exec", [self._make_job()])
        runner.take_pending_jobs()
        runner.submit_work_result("wf#exec", "s1", {"value": 42})
        pairs = runner.gather("wf#exec")
        assert pairs == [("s1", {"value": 42})]

    def test_gather_marks_processed(self):
        runner, _ = self._make_runner()
        runner.dispatch("wf#exec", [self._make_job()])
        runner.take_pending_jobs()
        runner.submit_work_result("wf#exec", "s1", {"v": 1})
        runner.gather("wf#exec")
        # second gather should return nothing
        assert runner.gather("wf#exec") == []

    def test_gather_returns_empty_when_nothing_complete(self):
        runner, _ = self._make_runner()
        runner.dispatch("wf#exec", [self._make_job()])
        assert runner.gather("wf#exec") == []

    def test_multiple_jobs_dispatched_and_taken(self):
        runner, _ = self._make_runner()
        jobs = [self._make_job(op=f"s{i}", run_id=i) for i in range(3)]
        runner.dispatch("wf#exec", jobs)
        taken = runner.take_pending_jobs()
        assert len(taken) == 3
        assert {j.op for j in taken} == {"s0", "s1", "s2"}

    def test_run_id_recovered_from_task_key(self):
        runner, _ = self._make_runner()
        runner.dispatch("wf#exec", [self._make_job(run_id=7)])
        taken = runner.take_pending_jobs()
        assert taken[0].run_id == 7
