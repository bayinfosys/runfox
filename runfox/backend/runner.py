"""
runner.py -- Runner

Two primitives:

  dispatch(workflow_execution_id, jobs) -> None
  gather(workflow_execution_id) -> list[tuple[str, dict]]

gather() always returns immediately. Returns an empty list if no results
are ready. The runner never calls on_step_result; Workflow.run() does that.

The runner is a job queue. dispatch() enqueues; gather() dequeues results.
The caller drives execution between those two calls -- a local function, a
thread, a Lambda, an SQS consumer. The executor (fn, inputs -> dict) has
no runfox dependency regardless of which runner is used.

InProcessRunner -- dict-backed queue. Semantically identical to SqliteRunner;
                   the dict is the tasks table. Use InProcessWorker to drive
                   local execution against it.

SqliteRunner    -- SQLite tasks-table queue. An external worker owns execution.
                   See worker protocol in class docstring.

InProcessWorker -- local worker harness for InProcessRunner. Mirrors the
                   SqliteRunner worker protocol. The executor remains a plain
                   callable with no runfox dependency.
"""

import datetime
import json
import sqlite3
from typing import Callable

from ..results import DispatchJob


class Runner:

    def dispatch(self, workflow_execution_id: str, jobs: list) -> None:
        """Enqueue jobs. jobs is a list of DispatchJob."""
        raise NotImplementedError

    def gather(self, workflow_execution_id: str) -> list:
        """
        Return completed (op, output) pairs. Always returns immediately.
        Returns an empty list if no results are ready.
        """
        raise NotImplementedError

    def list_pending_jobs(self) -> list:
        """
        Non-destructive snapshot of all pending jobs across all workflows.
        Safe to call for diagnostics; does not affect queue state.
        """
        raise NotImplementedError

    def take_pending_jobs(self) -> list:
        """
        Consume and return all pending jobs across all workflows.
        Called by worker harnesses. Each returned job will not be
        returned again by a subsequent call.
        """
        raise NotImplementedError

    def submit_work_result(
        self, workflow_execution_id: str, op: str, output: dict
    ) -> None:
        """Write a result back from a worker."""
        raise NotImplementedError
