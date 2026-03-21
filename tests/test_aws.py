"""
test_aws.py -- SQSRunner and DynamoDBStore functional tests.

No AWS services are contacted. Injectable client parameters accept
simple fakes that record calls.

Tests cover:
  - SQSRunner input guards (binary, unknown model_type): raise before
    any client call, so no fake required.
  - SQSRunner message body construction (prompt template, message_id
    format): verified via a recording SQS fake.
  - SQSRunner dispatch returns [].
  - WorkflowRecord / StepRecord serialisation round-trip: the contract
    DynamoDBStore depends on. Covered in test_models.py; not repeated here.
  - DynamoDBStore load/write: depend on dynawrap item format internals
    that cannot be verified without a real or moto-backed DynamoDB table.
    Not tested here; covered by integration tests run against a real table
    or localstack when available.
"""

import json

import pytest

from runfox.backend.aws import SQSRunner
from runfox.results import DispatchJob


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeSQSClient:
    """Records send_message calls."""

    def __init__(self):
        self.sent = []

    def send_message(self, QueueUrl, MessageBody):
        self.sent.append({"QueueUrl": QueueUrl, "MessageBody": MessageBody})


class FakeDDBClient:
    """
    Records put_item, update_item, and query calls.
    query() returns an empty Items list by default; override
    _query_response to return specific items.
    """

    def __init__(self):
        self.put_items = []
        self.update_items = []
        self.queries = []
        self._query_response = {"Items": []}

    def put_item(self, TableName, Item):
        self.put_items.append({"TableName": TableName, "Item": Item})

    def update_item(self, TableName, Key, **kwargs):
        self.update_items.append({"TableName": TableName, "Key": Key, **kwargs})

    def query(self, TableName, **kwargs):
        self.queries.append({"TableName": TableName, **kwargs})
        return self._query_response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_runner(sqs=None, ddb=None, queue_url="https://sqs.example.com/test-queue", message_body_fn=None):
    return SQSRunner(
        tasks_table="test-tasks",
        queue_url=queue_url,
        sqs_client=sqs or FakeSQSClient(),
        ddb_client=ddb or FakeDDBClient(),
        message_body_fn=message_body_fn,
    )


def make_job(op="step1", run_id=0, extra_inputs=None):
    inputs = {"model_type": "instruct", "model_name": "some-model"}
    if extra_inputs:
        inputs.update(extra_inputs)
    return DispatchJob(
        workflow_execution_id="abc#exec1",
        op=op,
        inputs=inputs,
        run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Input guards
# ---------------------------------------------------------------------------


class TestSQSRunnerGuards:

    def test_binary_input_raises_before_sqs_call(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        job = make_job(extra_inputs={"image": b"binary"})
        with pytest.raises(NotImplementedError):
            runner.dispatch("abc#exec1", [job])
        assert sqs.sent == []


# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------


class TestSQSRunnerDispatch:

    def test_dispatch_returns_empty_list(self):
        runner = make_runner()
        result = runner.dispatch("abc#exec1", [make_job()])
        assert result == []

    def test_dispatch_sends_one_sqs_message_per_job(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        runner.dispatch("abc#exec1", [make_job("s1"), make_job("s2")])
        assert len(sqs.sent) == 2

    def test_dispatch_writes_pending_task_to_ddb(self):
        ddb = FakeDDBClient()
        runner = make_runner(ddb=ddb)
        runner.dispatch("abc#exec1", [make_job()])
        assert len(ddb.put_items) == 1
        assert ddb.put_items[0]["TableName"] == "test-tasks"


# ---------------------------------------------------------------------------
# queue_url -- str and callable forms
# ---------------------------------------------------------------------------


class TestSQSRunnerQueueUrl:

    def test_string_queue_url_used_directly(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs, queue_url="https://sqs.example.com/fixed-queue")
        runner.dispatch("abc#exec1", [make_job()])
        assert sqs.sent[0]["QueueUrl"] == "https://sqs.example.com/fixed-queue"

    def test_callable_queue_url_receives_job(self):
        sqs = FakeSQSClient()
        received = []

        def queue_url_fn(job):
            received.append(job)
            return "https://sqs.example.com/dynamic-queue"

        runner = make_runner(sqs=sqs, queue_url=queue_url_fn)
        job = make_job()
        runner.dispatch("abc#exec1", [job])
        assert len(received) == 1
        assert received[0] is job
        assert sqs.sent[0]["QueueUrl"] == "https://sqs.example.com/dynamic-queue"

    def test_callable_queue_url_exception_propagates(self):
        runner = make_runner(queue_url=lambda job: (_ for _ in ()).throw(RuntimeError("routing error")))
        with pytest.raises(RuntimeError, match="routing error"):
            runner.dispatch("abc#exec1", [make_job()])


# ---------------------------------------------------------------------------
# message body
# ---------------------------------------------------------------------------


class TestSQSRunnerMessageBody:

    def test_default_body_uses_job_as_dict(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        runner.dispatch("abc#exec1", [make_job(op="step1", run_id=3)])
        body = json.loads(sqs.sent[0]["MessageBody"])
        # job.as_dict() should include at minimum op and run_id
        assert body["op"] == "step1"
        assert body["run_id"] == 3

    def test_message_body_fn_used_when_provided(self):
        sqs = FakeSQSClient()

        def custom_body(job, workflow_execution_id):
            return {"custom": True, "op": job.op}

        runner = make_runner(sqs=sqs, message_body_fn=custom_body)
        runner.dispatch("abc#exec1", [make_job(op="mystep")])
        body = json.loads(sqs.sent[0]["MessageBody"])
        assert body == {"custom": True, "op": "mystep"}

    def test_message_body_fn_exception_propagates(self):
        def bad_body(job, wf_id):
            raise ValueError("bad body")

        runner = make_runner(message_body_fn=bad_body)
        with pytest.raises(ValueError, match="bad body"):
            runner.dispatch("abc#exec1", [make_job()])

    def test_non_serialisable_body_raises(self):
        runner = make_runner(message_body_fn=lambda job, wf_id: {"data": object()})
        with pytest.raises((TypeError, ValueError)):
            runner.dispatch("abc#exec1", [make_job()])
