"""
inprocess_runner.py -- InProcessRunner

Dict-backed job queue for local and test use. Semantically identical to
SqliteRunner: dispatch() writes to _pending (equivalent to INSERT PENDING),
gather() reads from _results (equivalent to SELECT COMPLETE and mark
PROCESSED). InProcessWorker drives execution between those two calls,
mirroring what an external worker does against the SQLite tasks table.

Runner pattern
--------------
InProcessRunner owns two dicts as its tasks table: _pending keyed by
workflow_execution_id, and _results keyed by workflow_execution_id.
These are short-term; they are not persisted across process restarts.
"""

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

    def dispatch(self, workflow_execution_id: str, jobs: list) -> list:
        existing = self._pending.get(workflow_execution_id, [])
        self._pending[workflow_execution_id] = existing + list(jobs)
        return []

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
