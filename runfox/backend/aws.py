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
from typing import ClassVar

import boto3
from dynawrap import DBItem

from runfox.backend.models import WorkflowRecord
from runfox.backend.runner import Runner
from runfox.backend.store import Store
from runfox.results import DispatchJob


# ---------------------------------------------------------------------------
# DynamoDB item types
# ---------------------------------------------------------------------------

import dataclasses

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

    Runner pattern: owns the tasks table (short-term, one item per
    dispatched step). dispatch() inserts PENDING task items and sends
    SQS messages. submit_work_result() updates task items to COMPLETE.
    gather() returns COMPLETE items and marks them PROCESSED.

    queue_map:   {model_type: sqs_queue_url}
    tasks_table: DynamoDB table name for the runner-owned tasks table.
    sqs_client:  optional pre-constructed boto3 SQS client.
    ddb_client:  optional pre-constructed boto3 DynamoDB client.
    """

    def __init__(
        self,
        queue_map: dict,
        tasks_table: str,
        sqs_client=None,
        ddb_client=None,
    ):
        self._queue_map = queue_map
        self._tasks_table = tasks_table
        self._sqs = sqs_client or boto3.client("sqs")
        self._ddb = ddb_client or boto3.client("dynamodb")

    def _now_iso(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Runner interface
    # ------------------------------------------------------------------

    def dispatch(self, workflow_execution_id: str, jobs: list) -> list:
        """
        Write a PENDING task item to the tasks table and send an SQS
        message for each job. Returns an empty list; all dispatch is
        external.
        """
        for job in jobs:
            for key, value in job.inputs.items():
                if isinstance(value, (bytes, bytearray)):
                    raise NotImplementedError(
                        f"Binary input {key!r} in step {job.op!r} requires "
                        "S3 staging, which is not yet implemented. "
                        "Pass binary data as S3 reference dicts between steps."
                    )
        now = self._now_iso()
        for job in jobs:
            item = WorkflowTaskItem(
                workflow_execution_id=workflow_execution_id,
                op=job.op,
                run_id=job.run_id,
                inputs_json=json.dumps(job.inputs),
                status="PENDING",
                created_at=now,
            )
            self._ddb.put_item(
                TableName=self._tasks_table,
                Item=item.to_dynamo_item(),
            )
            self._submit_to_sqs(job, workflow_execution_id)
        return []

    def gather(self, workflow_execution_id: str) -> list:
        """
        Query the tasks table for COMPLETE or ERROR items. Marks each
        PROCESSED and returns (op, output) pairs.
        """
        response = self._ddb.query(
            TableName=self._tasks_table,
            KeyConditionExpression="pk = :pk",
            FilterExpression="#s IN (:c, :e)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":pk": {"S": f"TASK#{workflow_execution_id}"},
                ":c":  {"S": "COMPLETE"},
                ":e":  {"S": "ERROR"},
            },
        )
        pairs = []
        for raw in response.get("Items", []):
            task = WorkflowTaskItem.from_dynamo_item(raw)
            output = json.loads(task.output_json) if task.output_json else {}
            pairs.append((task.op, output))
            key = WorkflowTaskItem.create_item_key(
                workflow_execution_id=workflow_execution_id,
                op=task.op,
                run_id=task.run_id,
            )
            self._ddb.update_item(
                TableName=self._tasks_table,
                Key=key,
                UpdateExpression="SET #s = :p, updated_at = :t",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":p": {"S": "PROCESSED"},
                    ":t": {"S": self._now_iso()},
                },
            )
        return pairs

    def list_pending_jobs(self) -> list:
        # Workers receive jobs from SQS directly. The tasks table is not
        # the authoritative pending-job queue.
        return []

    def take_pending_jobs(self) -> list:
        # Workers receive jobs from SQS directly. See list_pending_jobs.
        return []

    def submit_work_result(
        self, workflow_execution_id: str, op: str, output: dict
    ) -> None:
        """
        Update the PENDING task item for (workflow_execution_id, op) to
        COMPLETE with the given output.

        Queries with begins_with(sk, op+"#") since the base class
        signature carries no run_id. There is at most one non-PROCESSED
        task per (wf_exec_id, op) at any time.
        """
        response = self._ddb.query(
            TableName=self._tasks_table,
            KeyConditionExpression="pk = :pk AND begins_with(sk, :op_prefix)",
            FilterExpression="#s = :pending",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":pk":        {"S": f"TASK#{workflow_execution_id}"},
                ":op_prefix": {"S": f"{op}#"},
                ":pending":   {"S": "PENDING"},
            },
        )
        items = response.get("Items", [])
        if not items:
            return
        task = WorkflowTaskItem.from_dynamo_item(items[0])
        key = WorkflowTaskItem.create_item_key(
            workflow_execution_id=workflow_execution_id,
            op=task.op,
            run_id=task.run_id,
        )
        self._ddb.update_item(
            TableName=self._tasks_table,
            Key=key,
            UpdateExpression="SET #s = :c, output_json = :o, updated_at = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":c": {"S": "COMPLETE"},
                ":o": {"S": json.dumps(output)},
                ":t": {"S": self._now_iso()},
            },
        )

    # ------------------------------------------------------------------
    # SQS submission
    # ------------------------------------------------------------------

    def _submit_to_sqs(self, job: DispatchJob, workflow_execution_id: str) -> None:
        inputs = dict(job.inputs)

        model_type = inputs.pop("model_type")
        model_name = inputs.pop("model_name")
        prompt_template = inputs.pop("prompt_template", None)

        if prompt_template is not None:
            template_vars = {
                k: v for k, v in inputs.items()
                if "{" + k + "}" in prompt_template
            }
            inputs["prompt"] = prompt_template.format(**template_vars)

        queue_url = self._queue_map.get(model_type)
        if queue_url is None:
            raise ValueError(
                f"No SQS queue configured for model_type {model_type!r}. "
                f"Available types: {list(self._queue_map)}"
            )

        message_id = f"WORKFLOW#{workflow_execution_id}#{job.op}#{job.run_id}"

        body = {
            "model_name": model_name,
            "message_id": message_id,
            **inputs,
        }

        self._sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(body),
        )
