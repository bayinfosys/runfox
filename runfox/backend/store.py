"""
store.py -- Store base class.

Store pattern
-------------
Store owns the workflow table -- long-term, one record per
workflow_execution_id, holds the full serialised WorkflowRecord.

Implementations: InMemoryStore, SqliteStore, DynamoDBStore.
"""


class Store:

    def load(self, workflow_execution_id: str):
        """Return a value isolated from the backing store. Raises KeyError if not found."""
        raise NotImplementedError

    def write(self, record) -> None:
        """Persist a value isolated from the caller."""
        raise NotImplementedError
