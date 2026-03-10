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

from .models import StepRecord, WorkflowRecord
from .store import Store


class InMemoryStore(Store):

    def __init__(self):
        self._store: dict[str, WorkflowRecord] = {}

    def load(self, workflow_execution_id: str) -> WorkflowRecord:
        if workflow_execution_id not in self._store:
            raise KeyError(workflow_execution_id)
        return copy.deepcopy(self._store[workflow_execution_id])

    def write(self, record: WorkflowRecord) -> None:
        key = f"{record.workflow_id}#{record.execution_id}"
        self._store[key] = copy.deepcopy(record)
