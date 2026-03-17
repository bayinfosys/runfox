"""
runner.py -- Runner base class.

Runner pattern
--------------
Runner owns the tasks table -- short-term, one row per dispatched step,
holds status and output until gathered. dispatch() enqueues jobs.
submit_work_result() writes results. gather() dequeues results.

There is no requirement that Store and Runner use the same physical
storage. SqliteStore and SqliteRunner share a .db file by convention
(separate tables within it). DynamoDBStore and SQSRunner use separate
DynamoDB tables. InMemoryStore and InProcessRunner use separate dicts.

For runners that dispatch to an external queue (SQS, etc.) there is no
in-band return channel. gather() reads from the runner's own tasks table,
which workers write to via submit_work_result(). The tasks table is
runner-owned short-term state; the workflow table is store-owned
long-term state. They are always logically distinct even when the same
storage technology is used for both.
"""


class Runner:

    def dispatch(self, workflow_execution_id: str, jobs: list) -> list:
        """
        Enqueue jobs. jobs is a list of DispatchJob.

        Returns a list of (op, output) pairs for any jobs executed
        locally by this runner rather than submitted to an external queue.
        Returns an empty list if all jobs were enqueued externally.

        Callers must feed any returned pairs to on_step_result() before
        calling advance().
        """
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
