"""
sqlite_runner.py -- SqliteRunner

SQLite tasks-table runner. dispatch() inserts PENDING rows. An external
worker claims and executes them, writing COMPLETE or ERROR back. gather()
reads COMPLETE/ERROR rows, marks them PROCESSED, and returns (op, output)
pairs.

Runner pattern
--------------
SqliteRunner owns the tasks table -- short-term, one row per dispatched
step. SqliteStore owns the workflows table -- long-term, one record per
workflow_execution_id. They may share the same .db file; they use
separate tables and do not call each other.

Worker protocol
---------------
1. SELECT * FROM tasks WHERE status = 'PENDING' LIMIT 1
2. UPDATE tasks SET status = 'STARTED', updated_at = ?
   WHERE task_key = ? AND status = 'PENDING'
3. Execute op(inputs).
4. UPDATE tasks SET status = 'COMPLETE', output = ?, updated_at = ?
   WHERE task_key = ?
   -- or ERROR on failure
"""

import datetime
import json
import sqlite3

from ..results import DispatchJob
from .runner import Runner


class SqliteRunner(Runner):

    def __init__(self, db_path: str, poll_interval: float = 0.1):
        self._db_path = db_path
        self.poll_interval = poll_interval
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_key               TEXT PRIMARY KEY,
                    workflow_execution_id  TEXT NOT NULL,
                    op                     TEXT NOT NULL,
                    inputs                 TEXT NOT NULL,
                    output                 TEXT,
                    status                 TEXT NOT NULL DEFAULT 'PENDING',
                    created_at             TEXT NOT NULL,
                    updated_at             TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_workflow_execution_status
                    ON tasks (workflow_execution_id, status);
            """)

    def _now_iso(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    def _row_to_job(self, row) -> "DispatchJob":
        return DispatchJob(
            workflow_execution_id=row["workflow_execution_id"],
            op=row["op"],
            inputs=json.loads(row["inputs"]),
            run_id=int(row["task_key"].rsplit("#", 1)[-1]),
        )

    def dispatch(self, workflow_execution_id: str, jobs: list) -> list:
        now = self._now_iso()
        with self._connect() as conn:
            for job in jobs:
                task_key = f"{workflow_execution_id}#{job.op}#{job.run_id}"
                conn.execute(
                    "INSERT INTO tasks "
                    "(task_key, workflow_execution_id, op, inputs, status, created_at) "
                    "VALUES (?, ?, ?, ?, 'PENDING', ?)",
                    [
                        task_key,
                        workflow_execution_id,
                        job.op,
                        json.dumps(job.inputs),
                        now,
                    ],
                )
        return []

    def gather(self, workflow_execution_id: str) -> list:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks "
                "WHERE workflow_execution_id = ? AND status IN ('COMPLETE', 'ERROR')",
                [workflow_execution_id],
            ).fetchall()
            pairs = []
            for row in rows:
                output = json.loads(row["output"])
                pairs.append((row["op"], output))
                conn.execute(
                    "UPDATE tasks SET status = 'PROCESSED' WHERE task_key = ?",
                    [row["task_key"]],
                )
        return pairs

    def list_pending_jobs(self) -> list:
        """Non-destructive snapshot of all PENDING rows."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status = 'PENDING'"
            ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def take_pending_jobs(self) -> list:
        """Consume all PENDING rows by marking them STARTED."""
        now = self._now_iso()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status = 'PENDING'"
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE tasks SET status = 'STARTED', updated_at = ? "
                    "WHERE task_key = ? AND status = 'PENDING'",
                    [now, row["task_key"]],
                )
        return [self._row_to_job(row) for row in rows]

    def submit_work_result(
        self, workflow_execution_id: str, op: str, output: dict
    ) -> None:
        now = self._now_iso()
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET status = 'COMPLETE', output = ?, updated_at = ? "
                "WHERE workflow_execution_id = ? AND op = ? AND status = 'STARTED'",
                [json.dumps(output), now, workflow_execution_id, op],
            )
