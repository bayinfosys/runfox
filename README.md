# runfox

`import runfox as rfx`

A workflow orchestration library. A **Backend** owns all workflow state.
A **Workflow** is a stateless handle rehydrated from the backend on every
operation. The backend is swappable -- the same workflow definition runs
in-process, locally against SQLite, or distributed across SQS and
DynamoDB, with no changes to the workflow code.

---

## Installation
```bash
pip install runfox
```

For AWS backends:
```bash
pip install runfox[aws]
```

---

## Quickstart
```python
import runfox as rfx
from runfox.backend import InMemoryStore, InProcessRunner, InProcessWorker

SPEC = """
name: example
steps:
  - op: make_greeting
    input:
      name: {"var": "input.name"}

  - op: shout
    depends_on: [make_greeting]
    input:
      text: {"var": "steps.make_greeting.output.text"}

outputs:
  message: {"var": "steps.shout.output.text"}
"""

def execute(op, inputs):
    if op == "make_greeting":
        return {"text": f"hello, {inputs['name']}"}
    if op == "shout":
        return {"text": inputs["text"].upper()}

runner  = InProcessRunner()
worker  = InProcessWorker(runner, execute)
backend = rfx.Backend(store=InMemoryStore(), runner=runner)
wf      = rfx.Workflow.from_yaml(SPEC, backend, inputs={"name": "world"})
result  = wf.run(worker=worker)

print(result.outcome)
# {"message": "HELLO, WORLD"}
```

`from_yaml()` takes a YAML string. To load from a file, read it first:
```python
with open("example.yaml") as f:
    spec = f.read()
wf = rfx.Workflow.from_yaml(spec, backend, inputs={"name": "world"})
```

---

## Workflow definition

### Steps

Each step declares:

- `op` -- unique identifier within the workflow and the dispatch token
  passed to the executor.
- `label` -- optional human description. Never read by runfox.
- `input` -- input values; literals or JSON Logic expressions.
- `depends_on` -- step ops that must complete before this step runs.
- `branch` -- conditional exits evaluated after the step completes.
- `max_attempts` -- error-recovery retry budget (default: 1).

### Input references
```yaml
input:
  threshold: 0.7                                       # literal
  text:      {"var": "steps.caption.output.text"}      # prior step output
  image:     {"var": "input.image"}                    # workflow input
  total:     {"var": "state.running_total"}             # shared accumulator
  scores:    {"vars": "steps[*].output.score"}          # multi-value
```

### Branch conditions
```yaml
branch:
  - condition: {">=": [{"var": "scores.unsafe"}, 0.7]}
    action: halt
    result: {status: rejected, reason: unsafe}
```

Three actions: `halt`, `complete`, `{set: "steps.X.status", value: ready}`.

`halt` terminates the workflow immediately; the `result` payload becomes
the outcome.

`complete` marks the step complete immediately, bypassing the
`max_attempts` retry budget. Use it to exit a loop or skip retry logic
when a specific condition is met.

When a `set` action fires, the named steps and all steps that
transitively depend on them are reset to ready. This clears stale
outputs from any downstream steps before the scheduler re-dispatches.

### Data-driven loops
```yaml
steps:
  - op: seed

  - op: iterate
    depends_on: [seed]
    input:
      a: {"var": "state.fib_n"}
      b: {"var": "state.fib_n1"}
    branch:
      - condition: {"<": [{"var": "state.fib_n"}, 100]}
        action: {set: "steps.iterate.status", value: ready}
      - condition: {">=": [{"var": "state.fib_n"}, 100]}
        action: complete
```

Every step with a `set` branch must also declare a `complete` or `halt`
branch. `max_attempts` is an error-recovery budget, not a loop counter.

### Outputs
```yaml
outputs:
  embedding: {"var": "steps.embed.output.vector"}
  total:     {"var": "state.running_total"}
  status:    accepted
```

---

## Execution modes

### Local (synchronous)

`InProcessRunner` and `InProcessWorker` run jobs synchronously in-process.
The full workflow completes within a single `wf.run()` call.
```python
runner  = InProcessRunner()
worker  = InProcessWorker(runner, execute)
backend = rfx.Backend(store=InMemoryStore(), runner=runner)
wf      = rfx.Workflow.from_yaml(SPEC, backend)
result  = wf.run(worker=worker)
```

### Local with SQLite persistence

`SqliteStore` persists workflow records across process restarts.
`InProcessWorker` still drives execution locally.
```python
from runfox.backend import SqliteStore, InProcessRunner, InProcessWorker

runner  = InProcessRunner()
worker  = InProcessWorker(runner, execute)
backend = rfx.Backend(store=SqliteStore("workflow.db"), runner=runner)
wf      = rfx.Workflow.from_yaml(SPEC, backend)
result  = wf.run(worker=worker)
```

### Async local (SQLite worker)

`SqliteRunner` writes PENDING task rows. A separate worker thread or
process polls them, executes, and writes results back. `Workflow.run()`
polls `gather()` until results appear.
```python
from runfox.backend import SqliteStore, SqliteRunner

backend = rfx.Backend(
    store=SqliteStore("workflow.db"),
    runner=SqliteRunner("workflow.db"),
)
wf = rfx.Workflow.from_yaml(SPEC, backend)

# external worker polls backend.pending_tasks() and calls
# backend.submit_result() -- see runner.py worker protocol

result = wf.run()  # no worker= argument; polls gather() internally
```

### Event-driven (distributed)

`advance()` claims all ready steps and returns a `Dispatch`. The caller
places each job on whatever queue it uses. When a result arrives the
caller routes it to `on_step_result()` then calls `advance()` again.
```python
def _dispatch(wf, result):
    if not isinstance(result, rfx.Dispatch):
        return
    # dispatch() returns (op, output) pairs for any locally-executed steps.
    # Feed them to on_step_result() before the next advance().
    pairs = backend.dispatch(wf.id, result.jobs)
    for op, output in pairs:
        wf.on_step_result(op, output)

def start_workflow(spec, inputs):
    wf     = rfx.Workflow.from_yaml(spec, backend, inputs=inputs)
    result = wf.advance()
    _dispatch(wf, result)

def handle_result(workflow_execution_id, op, output):
    wf          = rfx.Workflow.resume(workflow_execution_id, backend)
    step_result = wf.on_step_result(op, output)
    if isinstance(step_result, rfx.Halt):
        return
    result = wf.advance()
    _dispatch(wf, result)
```

### Distributed (DynamoDB + SQS)

`DynamoDBStore` persists workflow records in DynamoDB. `SQSRunner`
dispatches steps to SQS queues keyed by `model_type`. Workers consume
SQS messages, execute, and call `backend.submit_result()`. `gather()`
polls the runner's DynamoDB tasks table for completed rows.
```python
from runfox.backend.aws import DynamoDBStore, SQSRunner

store = DynamoDBStore(table="workflows")
runner = SQSRunner(
    queue_map={
        "instruct":        "https://sqs.eu-west-2.amazonaws.com/.../instruct",
        "image-embedding": "https://sqs.eu-west-2.amazonaws.com/.../image-embedding",
        "compute":         "https://sqs.eu-west-2.amazonaws.com/.../compute",
    },
    tasks_table="tasks",
)
backend = rfx.Backend(store=store, runner=runner)
wf = rfx.Workflow.from_yaml(SPEC, backend, inputs={"name": "world"})

# Use the event-driven pattern above. Workflow.run() is not used
# in the distributed pattern.
result = wf.advance()
if isinstance(result, rfx.Dispatch):
    backend.dispatch(wf.id, result.jobs)
```

`DynamoDBStore` and `SQSRunner` own separate DynamoDB tables: the
workflows table (Store, long-term) and the tasks table (Runner,
short-term). See `runfox/backend/aws.py` for table schemas.

---

## Concepts

### Backend

`Backend` composes a `Store` and a `Runner`. It is the only mutation
point; all state transitions go through Backend methods.
```python
backend = rfx.Backend(store=InMemoryStore(), runner=runner)
```

Pass-throughs for external worker use:

  `backend.pending_tasks()` -- calls `runner.list_pending_jobs()`
  `backend.take_tasks()`    -- calls `runner.take_pending_jobs()`
  `backend.submit_result(workflow_execution_id, op, output)` -- calls `runner.submit_work_result()`

### Store

Owns durable persistence of `WorkflowRecord`. One record per
`workflow_execution_id` -- long-term state.

  `load(workflow_execution_id)` -- returns a copy isolated from the
  backing store. Raises `KeyError` if not found.
  `write(record)` -- persists a copy isolated from the caller.

Implementations: `InMemoryStore`, `SqliteStore`, `DynamoDBStore`.

### Runner

Owns the job queue. One row per dispatched step -- short-term state.

  `dispatch(workflow_execution_id, jobs)` -- enqueues jobs. Returns a
  list of `(op, output)` pairs for any jobs executed locally; empty
  list if all jobs were submitted externally.
  `gather(workflow_execution_id)` -- returns completed `(op, output)`
  pairs. Always returns immediately; empty list if no results ready.
  `list_pending_jobs()` -- non-destructive snapshot of all pending jobs.
  `take_pending_jobs()` -- consume and return all pending jobs.
  `submit_work_result(workflow_execution_id, op, output)` -- writes a
  result back from a worker.

Implementations: `InProcessRunner`, `SqliteRunner`, `SQSRunner`.

### Storage pattern

Store owns the **workflow table** (long-term, one record per execution).
Runner owns the **tasks table** (short-term, one row per dispatched step).
There is no requirement they use the same physical storage.
`SqliteStore` and `SqliteRunner` share a `.db` file by convention.
`DynamoDBStore` and `SQSRunner` use separate DynamoDB tables. Either
axis can be swapped independently.

### Worker harness

Sits between the runner and the executor. Calls `take_pending_jobs()`,
calls the executor, calls `submit_work_result()`.

`InProcessWorker` is provided for local and test use:
```python
worker = InProcessWorker(runner, execute)
result = wf.run(worker=worker)
```

For `SqliteRunner` the worker is an external process. See worker
protocol in `sqlite_runner.py`.

### Workflow

Holds only a `workflow_execution_id` and a backend reference. Every
method call loads current state from the backend, operates on it, and
writes back. Multiple instances with the same
`(workflow_execution_id, backend)` are interchangeable.

### Executor

A plain callable with no runfox dependency:
```python
def execute(op: str, inputs: dict) -> dict:
    ...
```

Receives the step dispatch token and a resolved input dict. Returns an
output dict. Deployable and testable independently of the rest of the
stack.

---

## Result types

| Type | Meaning |
|---|---|
| `Complete(outcome)` | All steps finished; `outcome` contains resolved outputs |
| `Halt(result)` | A branch condition fired; `result` is the branch payload |
| `Dispatch(jobs)` | Steps claimed; `jobs` is a list of `DispatchJob` |
| `Pending()` | In-progress steps exist; nothing new is ready yet |

Each `DispatchJob` carries `workflow_execution_id`, `op`, `inputs`, and
`run_id`. `op` is the dispatch token passed to the executor.

`wf.on_step_result()` returns `Halt` if a branch fires, `Pending` if
the step is scheduled for retry, or `None` on normal completion.

---

## Observing state changes

`Backend` accepts an optional `on_state_change` callback fired after
every `merge_workflow_state` call:
```python
def on_state_change(workflow_execution_id, previous_state, new_state, event):
    if event is not None and event.op == "score":
        print(new_state["score"])

backend = rfx.Backend(
    store=InMemoryStore(),
    runner=runner,
    on_state_change=on_state_change,
)
```

The callback receives `(workflow_execution_id, previous_state,
new_state, event)`. `event` is a `StateChangeEvent` with a single field
`op` identifying the step that triggered the merge. The callback must
be pure: no side effects, no exceptions, no calls back into the backend.
See NOTES.md for the full purity contract.

---

## Executor contract and error handling

The executor contract is `(op: str, inputs: dict) -> dict`.

The recommended pattern catches errors and returns a structured output
so branch conditions can act on them:
```python
def execute(op, inputs):
    try:
        return run_step(op, inputs)
    except Exception as e:
        return {"error": str(e), "ok": False}
```
```yaml
branch:
  - condition: {"==": [{"var": "ok"}, false]}
    action: halt
    result: {status: error}
```

A worker that crashes without writing any result leaves the workflow
waiting indefinitely. The executor should not raise.

---

## Project layout
```
runfox/
  __init__.py         -- public API: Backend, Workflow,
                         Complete, Dispatch, Halt, Pending,
                         StepStatus, WorkflowStatus
  results.py          -- Complete, Halt, Dispatch, DispatchJob, Pending,
                         StateChangeEvent
  status.py           -- StepStatus, WorkflowStatus
  workflow.py         -- Workflow; pure graph functions: advance(),
                         on_step_result(), dependency walk, input
                         resolution, branch evaluation,
                         _find_transitive_dependents
  backend/
    __init__.py       -- Backend, Store, Runner, InMemoryStore,
                         SqliteStore, InProcessRunner, InProcessWorker,
                         SqliteRunner
    base.py           -- Backend; ID generation; composite key accessors
    models.py         -- WorkflowRecord, StepRecord dataclasses
    store.py          -- Store base class
    inmemory_store.py -- InMemoryStore
    sqlite_store.py   -- SqliteStore
    runner.py         -- Runner base class
    inprocess_runner.py -- InProcessRunner
    inprocess_worker.py -- InProcessWorker
    sqlite_runner.py  -- SqliteRunner
    aws.py            -- DynamoDBStore, SQSRunner, WorkflowStateItem,
                         WorkflowTaskItem (requires runfox[aws])

tests/
  conftest.py         -- shared executors, specs, fixtures
  test_backend.py     -- Backend lifecycle operations
  test_workflow.py    -- Workflow construction, execution, cascade reset
  test_sqlite.py      -- SqliteStore and SqliteRunner
  test_status.py      -- StepStatus and WorkflowStatus enum value guards
  test_state_change_event.py -- on_state_change callback behaviour
  test_inprocess.py   -- InProcessRunner and InProcessWorker
  test_models.py      -- WorkflowRecord and StepRecord serialisation
  test_dispatch_return.py -- dispatch() return value contract
  test_aws.py         -- SQSRunner functional tests

examples/
  ops/                -- abstract patterns: single_step, accumulation,
                         fan_out, fan_in, branch_halt, retry
  uses/               -- worked examples: fibonacci, fibonacci_async,
                         document_parser, multisource, validation,
                         stack_machine, conways_game_of_life
```

---

## Dependencies

- [json-logic-path](https://pypi.org/project/json-logic-path/) --
  input reference resolution and branch condition evaluation
- [PyYAML](https://pypi.org/project/PyYAML/) -- workflow definition
  parsing
- [boto3](https://pypi.org/project/boto3/) -- AWS SDK (`runfox[aws]`)
- [dynawrap](https://pypi.org/project/dynawrap/) -- DynamoDB item
  abstractions (`runfox[aws]`)
