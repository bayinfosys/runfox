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

from .inprocess_runner import InProcessRunner


class InProcessWorker:
    """
    Local worker harness for InProcessRunner.

    Mirrors the SqliteRunner worker protocol using the runner's internal
    dicts in place of the tasks table. The executor remains a plain callable
    with no runfox dependency.

    Equivalent remote pattern
    -------------------------
    This harness:
        for job in runner.pending(wf_exec_id):
            output = executor(job.fn, job.inputs)
            runner.submit_work_result(wf_exec_id, job.step_id, output)

    SQS/Lambda equivalent:
        message = sqs.receive()
        output  = executor(message.fn, message.inputs)
        dynamodb.put(task_key, output, status="COMPLETE")
    """

    def __init__(self, runner: InProcessRunner, executor: Callable[[str, dict], dict]):
        self._runner = runner
        self._executor = executor

    def run(self, workflow_execution_id: str) -> None:
        for job in self._runner.take_pending_jobs():
            try:
                output = self._executor(job.fn, job.inputs)
            except Exception as exc:
                output = {"error": str(exc), "ok": False}
            self._runner.submit_work_result(workflow_execution_id, job.step_id, output)
