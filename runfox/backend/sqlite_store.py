"""
sqlite_store.py -- SqliteStore

Manages the workflows table. The tasks table belongs to SqliteRunner.
SqliteStore and SqliteRunner may share the same db_path; they do not
call each other.

Store pattern
-------------
Store owns the workflow table -- long-term, one record per
workflow_execution_id, holds the full serialised WorkflowRecord.
Serialisation delegates to WorkflowRecord.to_dict() / from_dict().
"""

import json
import sqlite3

from runfox.status import WorkflowStatus

from .models import StepRecord, WorkflowRecord
from .store import Store


class SqliteStore(Store):
    """
    SQLite-backed store. Manages the workflows table only.

    SqliteStore and SqliteRunner may share the same db_path; they do not
    call each other.
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
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
                CREATE TABLE IF NOT EXISTS workflows (
                    id           TEXT PRIMARY KEY,
                    workflow_id  TEXT NOT NULL,
                    execution_id TEXT NOT NULL,
                    spec         TEXT NOT NULL,
                    inputs       TEXT NOT NULL,
                    state        TEXT NOT NULL,
                    steps        TEXT NOT NULL,
                    status       TEXT NOT NULL,
                    outcome      TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_workflows_workflow_id
                    ON workflows (workflow_id);
            """)

    def _row_to_record(self, row: sqlite3.Row) -> WorkflowRecord:
        raw_steps = json.loads(row["steps"])
        steps = {op: StepRecord.from_dict(s) for op, s in raw_steps.items()}
        return WorkflowRecord(
            workflow_id=row["workflow_id"],
            execution_id=row["execution_id"],
            spec=json.loads(row["spec"]),
            inputs=json.loads(row["inputs"]),
            state=json.loads(row["state"]),
            steps=steps,
            status=WorkflowStatus(row["status"]),
            outcome=json.loads(row["outcome"]) if row["outcome"] else None,
        )

    def load(self, workflow_execution_id: str) -> WorkflowRecord:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM workflows WHERE id = ?", [workflow_execution_id]
            ).fetchone()
        if row is None:
            raise KeyError(workflow_execution_id)
        return self._row_to_record(row)

    def write(self, record: WorkflowRecord) -> None:
        key = f"{record.workflow_id}#{record.execution_id}"
        d = record.to_dict()
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO workflows "
                "(id, workflow_id, execution_id, spec, inputs, state, steps, status, outcome) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    key,
                    d["workflow_id"],
                    d["execution_id"],
                    json.dumps(d["spec"]),
                    json.dumps(d["inputs"]),
                    json.dumps(d["state"]),
                    json.dumps(d["steps"]),
                    d["status"],
                    json.dumps(d["outcome"]) if d["outcome"] is not None else None,
                ],
            )
