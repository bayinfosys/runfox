"""
aws.py -- DynamoDBStore and SQSRunner

AWS backend implementations. Install with:
    pip install runfox[aws]

boto3, dynawrap, and numpy are imported inside this module only. Plain
runfox installs that never import this module are unaffected.

DynamoDBStore
-------------
Implements Store. Owns the workflows DynamoDB table -- long-term,
one item per workflow_execution_id. Serialises via WorkflowRecord.to_dict()
/ from_dict().

SQSRunner
---------
Implements Runner. Owns a tasks DynamoDB table -- short-term, one item
per (workflow_execution_id, op, run_id). dispatch() inserts PENDING task
items and sends SQS messages. Workers call submit_work_result() which
updates the task item to COMPLETE. gather() queries COMPLETE items and
marks them PROCESSED.

Both classes accept optional pre-constructed client objects for their
respective AWS services. Pass a fake or stub in tests; pass a boto3
client with custom credentials or endpoint in production. When not
provided, a boto3 client is constructed with default credential resolution.

Storage pattern
---------------
DynamoDBStore and SQSRunner use separate DynamoDB tables. The workflows
table is long-term state owned by the Store. The tasks table is short-term
state owned by the Runner. They may be in the same AWS account and region
but are logically and physically distinct.

Known limitations
-----------------
DynamoDBStore.write() is a full item replacement (PutItem). Two concurrent
Lambda invocations processing parallel step results for the same workflow
may produce a lost update on merge_workflow_state. For workflows with low
parallelism this is low probability. Override write_step_output() and
merge_workflow_state() on a DynamoDBStore subclass with targeted UpdateItem
calls when needed. The Store interface supports this without changes.

SQSRunner.list_pending_jobs() and take_pending_jobs() return empty lists.
In the SQS pattern, workers receive jobs from SQS directly; the tasks
table is not the authoritative pending-job queue.

mask-diff and kmeans compute steps are not yet implemented.
"""

import dataclasses
import datetime
import json
import logging
from typing import ClassVar

import boto3
from dynawrap import DBItem

from runfox.backend.models import WorkflowRecord
from runfox.backend.runner import Runner
from runfox.backend.store import Store
from runfox.results import DispatchJob

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DynamoDB item types
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class WorkflowStateItem(DBItem):
    """One item per workflow_execution_id in the workflows table."""

    pk_pattern: ClassVar[str] = "WORKFLOW#{workflow_execution_id}"
    sk_pattern: ClassVar[str] = "STATE"

    schema_version: str = ""
    workflow_execution_id: str = ""
    record_json: str = ""


@dataclasses.dataclass
class WorkflowTaskItem(DBItem):
    """
    One item per (workflow_execution_id, op, run_id) in the tasks table.

    PK: TASK#{workflow_execution_id}
    SK: {op}#{run_id}

    Using run_id in the SK means retried and reset tasks create new items
    without overwriting prior ones, matching the SqliteRunner safety property.
    """

    pk_pattern: ClassVar[str] = "TASK#{workflow_execution_id}"
    sk_pattern: ClassVar[str] = "{op}#{run_id}"

    workflow_execution_id: str = ""
    op: str = ""
    run_id: int = 0
    inputs_json: str = ""
    output_json: str = ""
    status: str = "PENDING"
    created_at: str = ""
    updated_at: str = ""


# ---------------------------------------------------------------------------
# DynamoDBStore
# ---------------------------------------------------------------------------


class DynamoDBStore(Store):
    """
    DynamoDB-backed store. Manages the workflows table only.

    Store pattern: owns the workflow table (long-term, one item per
    workflow_execution_id). Serialises via WorkflowRecord.to_dict() /
    from_dict().

    client: optional pre-constructed boto3 DynamoDB client. When not
    provided, boto3.client("dynamodb") is called with default credential
    resolution.
    """

    def __init__(self, table: str, client=None):
        self._table = table
        self._client = client or boto3.client("dynamodb")

    def load(self, workflow_execution_id: str) -> WorkflowRecord:
        key = WorkflowStateItem.create_item_key(
            workflow_execution_id=workflow_execution_id
        )
        response = self._client.get_item(
            TableName=self._table,
            Key=key,
        )
        if "Item" not in response:
            raise KeyError(workflow_execution_id)
        item = WorkflowStateItem.from_dynamo_item(response["Item"])
        return WorkflowRecord.from_dict(json.loads(item.record_json))

    def write(self, record: WorkflowRecord) -> None:
        wf_exec_id = f"{record.workflow_id}#{record.execution_id}"
        item = WorkflowStateItem(
            workflow_execution_id=wf_exec_id,
            record_json=json.dumps(record.to_dict()),
        )
        self._client.put_item(
            TableName=self._table,
            Item=item.to_dynamo_item(),
        )


# ---------------------------------------------------------------------------
# SQSRunner
# ---------------------------------------------------------------------------


class SQSRunner(Runner):
    """
    SQS-dispatch runner with a DynamoDB tasks table.

    Owns the tasks table: one item per dispatched step, written at
    dispatch time and updated to COMPLETE when the result arrives.

    queue_url is either a fixed string (single-queue deployments) or a
    callable that receives a DispatchJob and returns a queue URL string
    (multi-queue deployments). The callable form allows routing logic to
    be supplied by the caller without subclassing.

    message_body_fn, if provided, receives a DispatchJob and a
    workflow_execution_id string and returns a JSON-serialisable dict.
    When not provided, job.asdict() is used. Override get_message_body()
    in a subclass for more structured control.

    Both queue resolution and body construction are wrapped with exception
    logging. JSON serialisation failure is caught and logged separately.
    Binary input values (bytes, bytearray) are rejected before any AWS
    call; callers must pass binary data as S3 reference dicts.
    """

    def __init__(
        self,
        tasks_table: str,
        queue_url,  # str or callable
        sqs_client=None,
        ddb_client=None,
        message_body_fn=None,
    ):
        self._tasks_table = tasks_table
        self._sqs = sqs_client or boto3.client("sqs")
        self._ddb = ddb_client or boto3.client("dynamodb")
        self._queue_url = queue_url
        self._message_body_fn = message_body_fn

    def _now_iso(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    def get_message_body(self, job: DispatchJob, workflow_execution_id: str) -> dict:
        """create a message for sqs using the job context"""
        if self._message_body_fn is not None:
            return self._message_body_fn(job, workflow_execution_id)
        return dataclasses.asdict(job)

    def _submit_to_sqs(self, job: DispatchJob, workflow_execution_id: str) -> None:
        for key, value in job.inputs.items():
            if isinstance(value, (bytes, bytearray)):
                raise NotImplementedError(
                    f"Binary input {key!r} in step {job.op!r} is not supported. "
                    "Pass binary data as a reference dict."
                )

        try:
            queue_url = (
                self._queue_url
                if isinstance(self._queue_url, str)
                else self._queue_url(job)
            )
        except Exception:
            logger.exception("get_queue_url failed for op=%s", job.op)
            raise

        try:
            body = self.get_message_body(job, workflow_execution_id)
        except Exception:
            logger.exception("get_message_body failed for op=%s", job.op)
            raise

        try:
            message_body = json.dumps(body)
        except (TypeError, ValueError):
            logger.exception("message body is not JSON-serialisable for op=%s", job.op)
            raise

        self._sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
        )

    def dispatch(self, workflow_execution_id: str, jobs: list) -> list:
        for job in jobs:
            for key, value in job.inputs.items():
                if isinstance(value, (bytes, bytearray)):
                    raise NotImplementedError(
                        f"Binary input {key!r} in step {job.op!r} is not supported. "
                        "Pass binary data as a reference dict."
                    )

        now = self._now_iso()
        for job in jobs:
            item = WorkflowTaskItem(
                workflow_execution_id=workflow_execution_id,
                op=job.op,
                run_id=job.run_id,
                inputs_json=json.dumps(job.inputs),
                status="PENDING",  # TODO: replace with a constant
                created_at=now,
            )
            self._ddb.put_item(
                TableName=self._tasks_table,
                Item=item.to_dynamo_item(),
            )
            self._submit_to_sqs(job, workflow_execution_id)
        return []
