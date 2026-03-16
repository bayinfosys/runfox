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
from .runner import Runner


class InProcessRunner(Runner):
    """
    Dict-backed job queue.

    Semantically identical to SqliteRunner: dispatch() writes to _pending
    (equivalent to INSERT PENDING), gather() reads from _results (equivalent
    to SELECT COMPLETE and mark PROCESSED). InProcessWorker drives execution
    between those two calls, mirroring what an external worker does against
    the SQLite tasks table.
    """

    def __init__(self):
        self._pending: dict[str, list] = {}
        self._results: dict[str, list] = {}

    def dispatch(self, workflow_execution_id: str, jobs: list) -> None:
        existing = self._pending.get(workflow_execution_id, [])
        self._pending[workflow_execution_id] = existing + list(jobs)

    def gather(self, workflow_execution_id: str) -> list:
        return self._results.pop(workflow_execution_id, [])

    def list_pending_jobs(self) -> list:
        """Non-destructive snapshot. Does not alter queue state."""
        return [job for jobs in self._pending.values() for job in jobs]

    def take_pending_jobs(self) -> list:
        """Consume all pending jobs. Clears the queue."""
        jobs = [job for jobs in self._pending.values() for job in jobs]
        self._pending.clear()
        return jobs

    def submit_work_result(
        self, workflow_execution_id: str, op: str, output: dict
    ) -> None:
        existing = self._results.get(workflow_execution_id, [])
        self._results[workflow_execution_id] = existing + [(op, output)]
