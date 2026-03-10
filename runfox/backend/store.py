"""
store.py -- Store

Two primitives:

  load(workflow_execution_id) -> WorkflowRecord
  write(record) -> None

Implementations: InMemoryStore, SqliteStore.
SqliteStore manages the workflows table only. The tasks table belongs to SqliteRunner.
"""

import copy
import dataclasses
import json
import sqlite3

from runfox.status import StepStatus, WorkflowStatus

from .base import StepRecord, WorkflowRecord


class Store:

    def load(self, workflow_execution_id: str) -> WorkflowRecord:
        """Return a value isolated from the backing store. Raises KeyError if not found."""
        raise NotImplementedError

    def write(self, record: WorkflowRecord) -> None:
        """Persist a value isolated from the caller."""
        raise NotImplementedError
