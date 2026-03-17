"""
inmemory_store.py -- InMemoryStore

Dict-backed store for local and test use. load() returns a deep copy
isolated from the backing dict; write() stores a deep copy isolated from
the caller. No persistence across process restarts.

Runner pattern
--------------
InMemoryStore owns a single dict as its workflow table, keyed by
workflow_execution_id. This is the long-term authoritative store for
workflow state within a process.
"""

import copy

from .models import WorkflowRecord
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
