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


def make_runner(sqs=None, ddb=None):
    return SQSRunner(
        queue_map={"instruct": "https://sqs.example.com/test-queue"},
        tasks_table="test-tasks",
        sqs_client=sqs or FakeSQSClient(),
        ddb_client=ddb or FakeDDBClient(),
    )


def make_job(op="step1", model_type="instruct", run_id=0, extra_inputs=None):
    inputs = {"model_type": model_type, "model_name": "gpt-4"}
    if extra_inputs:
        inputs.update(extra_inputs)
    return DispatchJob(
        workflow_execution_id="abc#exec1",
        op=op,
        inputs=inputs,
        run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Input guards -- no client calls needed
# ---------------------------------------------------------------------------


class TestSQSRunnerGuards:

    def test_binary_input_raises_before_sqs_call(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        job = make_job(extra_inputs={"image": b"binary"})
        with pytest.raises(NotImplementedError, match="S3 staging"):
            runner.dispatch("abc#exec1", [job])
        assert sqs.sent == []

    def test_unknown_model_type_raises_before_sqs_call(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        job = make_job(model_type="unknown-type")
        with pytest.raises(ValueError, match="No SQS queue configured"):
            runner.dispatch("abc#exec1", [job])
        assert sqs.sent == []


# ---------------------------------------------------------------------------
# dispatch -- return value and SQS message body
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

    def test_message_body_contains_model_name(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        runner.dispatch("abc#exec1", [make_job()])
        body = json.loads(sqs.sent[0]["MessageBody"])
        assert body["model_name"] == "gpt-4"

    def test_message_id_format(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        runner.dispatch("abc#exec1", [make_job(op="step1", run_id=2)])
        body = json.loads(sqs.sent[0]["MessageBody"])
        assert body["message_id"] == "WORKFLOW#abc#exec1#step1#2"

    def test_message_sent_to_correct_queue(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        runner.dispatch("abc#exec1", [make_job(model_type="instruct")])
        assert sqs.sent[0]["QueueUrl"] == "https://sqs.example.com/test-queue"

    def test_model_type_not_in_message_body(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        runner.dispatch("abc#exec1", [make_job()])
        body = json.loads(sqs.sent[0]["MessageBody"])
        assert "model_type" not in body

    def test_dispatch_writes_pending_task_to_ddb(self):
        ddb = FakeDDBClient()
        runner = make_runner(ddb=ddb)
        runner.dispatch("abc#exec1", [make_job()])
        assert len(ddb.put_items) == 1
        assert ddb.put_items[0]["TableName"] == "test-tasks"


# ---------------------------------------------------------------------------
# Prompt template
# ---------------------------------------------------------------------------


class TestSQSRunnerPromptTemplate:

    def test_prompt_template_rendered_into_prompt_field(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        job = make_job(extra_inputs={
            "prompt_template": "Summarise: {text}",
            "text": "hello world",
        })
        runner.dispatch("abc#exec1", [job])
        body = json.loads(sqs.sent[0]["MessageBody"])
        assert body["prompt"] == "Summarise: hello world"

    def test_prompt_template_key_removed_from_body(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        job = make_job(extra_inputs={
            "prompt_template": "Say: {text}",
            "text": "hi",
        })
        runner.dispatch("abc#exec1", [job])
        body = json.loads(sqs.sent[0]["MessageBody"])
        assert "prompt_template" not in body

    def test_no_prompt_template_sends_inputs_directly(self):
        sqs = FakeSQSClient()
        runner = make_runner(sqs=sqs)
        job = make_job(extra_inputs={"text": "hello"})
        runner.dispatch("abc#exec1", [job])
        body = json.loads(sqs.sent[0]["MessageBody"])
        assert "prompt" not in body
        assert body["text"] == "hello"
