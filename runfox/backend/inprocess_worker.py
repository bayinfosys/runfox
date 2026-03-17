"""
inprocess_worker.py -- InProcessWorker

Local worker harness for InProcessRunner. Mirrors the SqliteRunner worker
protocol using the runner's internal dicts in place of the tasks table.
The executor remains a plain callable with no runfox dependency.

Equivalent remote pattern
-------------------------
This harness:
    for job in runner.take_pending_jobs():
        output = executor(job.op, job.inputs)
        runner.submit_work_result(job.workflow_execution_id, job.op, output)

SQS/Lambda equivalent:
    message = sqs.receive()
    output  = executor(message.op, message.inputs)
    dynamodb.update(task_key, output, status="COMPLETE")
"""

from typing import Callable

from .inprocess_runner import InProcessRunner


class InProcessWorker:
    """
    Local worker harness for InProcessRunner.

    Mirrors the SqliteRunner worker protocol using the runner's internal
    dicts in place of the tasks table. The executor remains a plain callable
    with no runfox dependency.
    """

    def __init__(self, runner: InProcessRunner, executor: Callable[[str, dict], dict]):
        self._runner = runner
        self._executor = executor

    def run(self, workflow_execution_id: str) -> None:
        for job in self._runner.take_pending_jobs():
            try:
                output = self._executor(job.op, job.inputs)
            except Exception as exc:
                output = {"error": str(exc), "ok": False}
            self._runner.submit_work_result(workflow_execution_id, job.op, output)
