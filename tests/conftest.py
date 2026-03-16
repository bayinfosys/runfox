"""
conftest.py -- shared executors, specs, fixtures, and record construction
for the runfox test suite.
"""

import os
import tempfile

import pytest

from runfox.backend import (
    Backend,
    InMemoryStore,
    InProcessRunner,
    InProcessWorker,
    SqliteStore,
)
from runfox.backend.base import StepRecord, WorkflowRecord
from runfox.status import StepStatus, WorkflowStatus

# ---------------------------------------------------------------------------
# Executors
# ---------------------------------------------------------------------------


def fib_executor(op, inputs):
    if op in ("f0", "f1"):
        return {"value": inputs["value"]}
    if op in ("f2", "f3", "f4", "f5", "f6"):
        return {"value": inputs["a"] + inputs["b"]}
    raise ValueError(op)


def counting_executor(op, inputs):
    if op == "count":
        return {"count": inputs["text"].lower().count("r")}
    if op == "identity":
        return inputs
    raise ValueError(op)


def halting_executor(op, inputs):
    if op == "check":
        return inputs
    if op == "process":
        return inputs
    raise ValueError(op)


# ---------------------------------------------------------------------------
# Spec dicts
# ---------------------------------------------------------------------------

FIB_SPEC = {
    "name": "fibonacci",
    "steps": [
        {"op": "f0", "input": {"value": 0}},
        {"op": "f1", "input": {"value": 1}},
        {
            "op": "f2",
            "depends_on": ["f0", "f1"],
            "input": {
                "a": {"var": "steps.f0.output.value"},
                "b": {"var": "steps.f1.output.value"},
            },
        },
        {
            "op": "f3",
            "depends_on": ["f1", "f2"],
            "input": {
                "a": {"var": "steps.f1.output.value"},
                "b": {"var": "steps.f2.output.value"},
            },
        },
        {
            "op": "f4",
            "depends_on": ["f2", "f3"],
            "input": {
                "a": {"var": "steps.f2.output.value"},
                "b": {"var": "steps.f3.output.value"},
            },
        },
    ],
    "outputs": {"result": {"var": "steps.f4.output.value"}},
}

HALT_SPEC = {
    "name": "halt-test",
    "steps": [
        {
            "op": "check",
            "input": {"unsafe": 0.9},
            "branch": [
                {
                    "condition": {">=": [{"var": "unsafe"}, 0.7]},
                    "action": "halt",
                    "result": {"status": "rejected", "reason": "unsafe"},
                }
            ],
        },
        {
            "op": "process",
            "depends_on": ["check"],
            "input": {"text": "hello"},
        },
    ],
    "outputs": {"text": {"var": "steps.process.output.text"}},
}

BRANCH_PASS_SPEC = {
    "name": "branch-pass",
    "steps": [
        {
            "op": "check",
            "input": {"unsafe": 0.1},
            "branch": [
                {
                    "condition": {">=": [{"var": "unsafe"}, 0.7]},
                    "action": "halt",
                    "result": {"status": "rejected", "reason": "unsafe"},
                }
            ],
        },
        {
            "op": "process",
            "depends_on": ["check"],
            "input": {"text": "hello"},
        },
    ],
    "outputs": {"text": {"var": "steps.process.output.text"}},
}


# ---------------------------------------------------------------------------
# Spec YAML strings
# ---------------------------------------------------------------------------

FIB6_YAML = """
name: fibonacci_6
steps:
  - op: f0
    input:
      value: 0
  - op: f1
    input:
      value: 1
  - op: f2
    depends_on: [f0, f1]
    input:
      a: {"var": "steps.f0.output.value"}
      b: {"var": "steps.f1.output.value"}
  - op: f3
    depends_on: [f1, f2]
    input:
      a: {"var": "steps.f1.output.value"}
      b: {"var": "steps.f2.output.value"}
  - op: f4
    depends_on: [f2, f3]
    input:
      a: {"var": "steps.f2.output.value"}
      b: {"var": "steps.f3.output.value"}
  - op: f5
    depends_on: [f3, f4]
    input:
      a: {"var": "steps.f3.output.value"}
      b: {"var": "steps.f4.output.value"}
  - op: f6
    depends_on: [f4, f5]
    input:
      a: {"var": "steps.f4.output.value"}
      b: {"var": "steps.f5.output.value"}
outputs:
  result: {"var": "steps.f6.output.value"}
"""

SET_LOOP_YAML = """
name: set-loop
steps:
  - op: count
    input:
      n: {"var": "state.n"}
    branch:
      - condition: {"<": [{"var": "n"}, 5]}
        action: {set: "steps.count.status", value: ready}
      - condition: {">=": [{"var": "n"}, 5]}
        action: complete
outputs:
  result: {"var": "state.n"}
"""


# ---------------------------------------------------------------------------
# WorkflowRecord construction helper
# ---------------------------------------------------------------------------


def make_record(
    steps,
    deps=None,
    state=None,
    inputs=None,
    step_inputs=None,
    step_outputs=None,
    spec_outputs=None,
    branches=None,
):
    """
    Build a minimal WorkflowRecord for pure function tests.

    steps       -- dict[str, StepStatus] mapping op to its current status
    deps        -- dict[str, list[str]] depends_on per step
    state       -- shared state accumulator dict
    inputs      -- workflow-level inputs dict
    step_inputs -- dict[str, dict] of spec-level input expressions per step
    step_outputs -- dict[str, dict] of output written to each step record
    spec_outputs -- dict of workflow-level output expressions
    branches    -- dict[str, list] of branch specs per step
    """
    deps = deps or {}
    state = state or {}
    inputs = inputs or {}
    step_inputs = step_inputs or {}
    step_outputs = step_outputs or {}
    spec_outputs = spec_outputs or {}
    branches = branches or {}

    spec_steps = []
    for op in steps:
        step_spec = {"op": op}
        if op in deps:
            step_spec["depends_on"] = deps[op]
        if op in step_inputs:
            step_spec["input"] = step_inputs[op]
        if op in branches:
            step_spec["branch"] = branches[op]
        spec_steps.append(step_spec)

    spec = {"name": "test", "steps": spec_steps, "outputs": spec_outputs}

    step_records = {
        op: StepRecord(
            op=op,
            status=status,
            output=step_outputs.get(op),
        )
        for op, status in steps.items()
    }

    return WorkflowRecord(
        workflow_id="test",
        execution_id="test",
        spec=spec,
        inputs=inputs,
        state=state,
        steps=step_records,
        status=WorkflowStatus.PENDING,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_backend(request):
    """
    Yields (backend, worker) backed by a temporary SQLite file.
    Deletes the database file on teardown regardless of test outcome.
    """
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(db_fd)
    executor = getattr(request, "param", fib_executor)
    runner = InProcessRunner()
    worker = InProcessWorker(runner, executor)
    backend = Backend(store=SqliteStore(db_path), runner=runner)
    yield backend, worker
    os.unlink(db_path)


def make_sqlite_backend(executor):
    """
    Return (backend, worker, db_path) for tests that need direct control
    over teardown or multiple backends in one test.
    Caller is responsible for calling os.unlink(db_path) after the test.
    """
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(db_fd)
    runner = InProcessRunner()
    worker = InProcessWorker(runner, executor)
    backend = Backend(store=SqliteStore(db_path), runner=runner)
    return backend, worker, db_path
