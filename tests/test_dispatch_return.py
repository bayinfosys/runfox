"""
test_dispatch_return.py -- dispatch() return value contract.

Verifies that InProcessRunner.dispatch(), SqliteRunner.dispatch(), and
Backend.dispatch() all return an empty list, and that Backend.dispatch()
passes the runner's return value through unchanged.
"""

import os
import tempfile

import pytest

from runfox.backend import Backend, InMemoryStore, InProcessRunner
from runfox.backend.sqlite_runner import SqliteRunner
from runfox.results import DispatchJob


class TestInProcessRunnerDispatchReturn:

    def test_returns_empty_list(self):
        runner = InProcessRunner()
        job = DispatchJob(
            workflow_execution_id="wf#exec",
            op="step1",
            inputs={},
            run_id=0,
        )
        result = runner.dispatch("wf#exec", [job])
        assert result == []

    def test_returns_empty_list_for_multiple_jobs(self):
        runner = InProcessRunner()
        jobs = [
            DispatchJob(workflow_execution_id="wf#exec", op=f"s{i}", inputs={}, run_id=0)
            for i in range(3)
        ]
        result = runner.dispatch("wf#exec", jobs)
        assert result == []

    def test_returns_empty_list_for_no_jobs(self):
        runner = InProcessRunner()
        result = runner.dispatch("wf#exec", [])
        assert result == []


class TestSqliteRunnerDispatchReturn:

    def test_returns_empty_list(self):
        db_fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        try:
            runner = SqliteRunner(db_path)
            job = DispatchJob(
                workflow_execution_id="wf#exec",
                op="step1",
                inputs={},
                run_id=0,
            )
            result = runner.dispatch("wf#exec", [job])
            assert result == []
        finally:
            os.unlink(db_path)


class TestBackendDispatchPassthrough:

    def test_passes_runner_return_value_through(self):
        runner = InProcessRunner()
        backend = Backend(store=InMemoryStore(), runner=runner)
        wf_id = backend.create({"name": "test", "steps": [{"op": "s1"}]})
        backend.mark_in_progress(wf_id, "s1")
        job = DispatchJob(
            workflow_execution_id=wf_id,
            op="s1",
            inputs={},
            run_id=0,
        )
        result = backend.dispatch(wf_id, [job])
        assert result == []

    def test_return_type_is_list(self):
        runner = InProcessRunner()
        backend = Backend(store=InMemoryStore(), runner=runner)
        wf_id = backend.create({"name": "test", "steps": [{"op": "s1"}]})
        result = backend.dispatch(wf_id, [])
        assert isinstance(result, list)
