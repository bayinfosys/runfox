# runfox

`import runfox as rfx`

A workflow orchestration library. A **Backend** owns all workflow state.
A **Workflow** is a stateless handle rehydrated from the backend on every
operation. The backend is swappable -- the same workflow definition runs
in-process, locally against SQLite, or distributed across SQS and
DynamoDB, with no changes to the workflow code.

---

## Architecture

runfox is organised in five layers:

**Application** -- `Workflow`, `advance()`, `on_step_result()`, `run()`.
Graph logic as pure functions over `WorkflowRecord`. No I/O.

**Orchestration** -- `Backend`. Composes a Store and a Runner. Owns
all workflow lifecycle operations. No knowledge of storage or transport
implementations.

**Storage / Queue** -- `Store` (`load`, `write`) and `Runner`
(`dispatch`, `gather`, `list_pending_jobs`, `submit_work_result`).
Implementations: `InMemoryStore`, `SqliteStore` / `InProcessRunner`,
`SqliteRunner`.

**Transport** -- worker harnesses. `InProcessWorker` for local use.
External scripts, Lambda handlers, or SQS consumers for remote use.
Calls the executor; calls `submit_work_result`. The only layer aware
of both sides.

**Executor** -- plain `(op, inputs) -> dict` callables. No runfox
imports. Business logic only.

The executor never crosses upward into the transport layer. `Backend`
never constructs SQL or touches dicts directly.

---

## Concepts

### Backend

`Backend` composes a `Store` and a `Runner`.
```python
from runfox import Backend
from runfox.backend import InMemoryStore, InProcessRunner, InProcessWorker

runner  = InProcessRunner()
worker  = InProcessWorker(runner, execute)
backend = Backend(store=InMemoryStore(), runner=runner)
```

For SQLite persistence with a local worker:
```python
from runfox.backend import SqliteStore, SqliteRunner

backend = Backend(
    store=SqliteStore("workflow.db"),
    runner=SqliteRunner("workflow.db"),
)
```

For DynamoDB storage with SQS dispatch (caller implements):
```python
backend = Backend(
    store=DynamoDBStore(table="workflows"),
    runner=SQSRunner(queue_url="https://..."),
)
```

`Workflow` takes a single `Backend`. No change to its constructor or
public API regardless of which store and runner are used.

### Store

Owns durable persistence of `WorkflowRecord`.

  `load(workflow_execution_id)` -- returns a copy isolated from the
  backing store. Raises `KeyError` if not found.

  `write(record)` -- persists a copy isolated from the caller.

Implementations: `InMemoryStore`, `SqliteStore`.

### Runner

Owns the job queue interface.

  `dispatch(workflow_execution_id, jobs)` -- enqueues jobs.
  `gather(workflow_execution_id)` -- returns completed
  `(op, output)` pairs. Always returns immediately; empty list if no results ready.
  `list_pending_jobs()` -- non-destructive snapshot of all pending jobs. Safe for diagnostics; does not alter queue state.
  `take_pending_jobs()` -- consume and return all pending jobs. Called by worker harnesses; returned jobs will not be returned by a subsequent call.
  `submit_work_result(workflow_execution_id, op, output)` -- writes a result back from a worker.

Implementations: `InProcessRunner`, `SqliteRunner`.

### Worker harness

Sits between the runner and the executor. Calls `take_pending_jobs()`,
calls the executor, calls `submit_work_result()`.

`InProcessWorker` is provided for local and test use:
```python
worker = InProcessWorker(runner, execute)
result = wf.run(worker=worker)
```

For `SqliteRunner` the worker is an external process. See worker
protocol in `runner.py`.

### Workflow

The `Workflow` object holds only a `workflow_execution_id` and a
backend reference. It has no local state. Every method call loads
current state from the backend, operates on it, and writes back.
Constructing a new `Workflow` instance with the same
`workflow_execution_id` and backend is equivalent to using the
original -- they are interchangeable.

### Executor

A plain callable with no runfox dependency:
```python
def execute(op: str, inputs: dict) -> dict:
    ...
```

Receives the step dispatch token and a resolved input dict. Returns an
output dict. The dispatch token is the `op` field from the workflow spec;
the executor switches on it to determine what work to perform.
Deployable and testable independently of the rest of the stack.

### Workflow and execution identifiers

Every workflow record carries two identifiers:

- `workflow_id` -- MD5 of the canonical spec JSON. Stable across all
  executions of the same definition.
- `execution_id` -- timestamp plus short suffix, assigned at `create()`
  time. Identifies one specific run.

The store key is `workflow_execution_id`:
`{workflow_id}#{execution_id}`. This is `wf.id` and what
`Workflow.resume()` accepts.
```python
backend.workflow_execution_id(record)           # store key
backend.step_key(wf_exec_id, op)                # step within execution
backend.step_run_key(wf_exec_id, op, run_id)    # one dispatch of a step
```

---

## Installation
```bash
pip install runfox
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

`from_yaml()` takes a YAML string, not a file path. To load from a
file, read it first:
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
  passed to the executor. The executor switches on this value to
  determine what work to perform.
- `label` -- optional human description of the step, analogous to a
  docstring. Never read by runfox.
- `input` -- input values; literals or JSON Logic expressions
- `depends_on` -- step ops that must complete before this step runs
- `branch` -- conditional exits evaluated after the step completes
- `max_attempts` -- error-recovery retry budget (default: 1)

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
the step has remaining retry attempts, or `None` on normal completion.

---

## Execution modes

### Local (synchronous)

`InProcessRunner` and `InProcessWorker` run jobs synchronously
in-process. The full workflow completes within a single `wf.run()` call.
```python
runner  = InProcessRunner()
worker  = InProcessWorker(runner, execute)
backend = rfx.Backend(store=InMemoryStore(), runner=runner)
wf      = rfx.Workflow.from_yaml(SPEC, backend)
result  = wf.run(worker=worker)
```

### Local with SQLite persistence

`SqliteStore` persists workflow records across process restarts.
`InProcessWorker` still drives execution locally. Use this for
development environments that need durable state.
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

# external worker polls backend.list_pending_jobs() and calls
# backend.submit_work_result() -- see runner.py worker protocol

result = wf.run()  # no worker= argument; polls gather() internally
```

### Event-driven (distributed)

`advance()` claims all ready steps and returns a `Dispatch`. The caller
places each job on whatever queue it uses. When a result arrives the
caller routes it to `on_step_result()` then calls `advance()` again.
```python
def start_workflow(spec, inputs):
    wf     = rfx.Workflow.from_yaml(spec, backend, inputs=inputs)
    result = wf.advance()
    if isinstance(result, rfx.Dispatch):
        backend.dispatch(wf.id, result.jobs)
        for job in backend.take_pending_jobs():
            my_queue.send(job)

def handle_result(workflow_execution_id, op, output):
    wf          = rfx.Workflow.resume(workflow_execution_id, backend)
    step_result = wf.on_step_result(op, output)
    if isinstance(step_result, rfx.Halt):
        return
    result = wf.advance()
    if isinstance(result, rfx.Dispatch):
        backend.dispatch(wf.id, result.jobs)
        for job in backend.take_pending_jobs():
            my_queue.send(job)
```

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
`op` identifying the step dispatch token that triggered the merge.
The callback must be pure: no side effects, no exceptions, no calls
back into the backend. See NOTES.md for the full purity contract.

---

## Executor contract and error handling

The executor contract is `(op: str, inputs: dict) -> dict`.

`op` is the dispatch token from the workflow spec. The executor switches
on it to determine what work to perform. Because `op` is the unique step
identity within the workflow, the executor can use it to look up any
step-specific configuration it needs.

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
  __init__.py       -- public API: Backend, Workflow,
                       Complete, Dispatch, Halt, Pending,
                       StepStatus, WorkflowStatus
  results.py        -- Complete, Halt, Dispatch, DispatchJob, Pending,
                       StateChangeEvent
  status.py         -- StepStatus, WorkflowStatus
  workflow.py       -- Workflow; pure graph functions: advance(),
                       on_step_result(), dependency walk, input
                       resolution, branch evaluation,
                       _find_transitive_dependents
  backend/
    __init__.py     -- Backend, Store, Runner, InMemoryStore,
                       SqliteStore, InProcessRunner, InProcessWorker,
                       SqliteRunner
    base.py         -- Backend, WorkflowRecord, StepRecord,
                       ID generation, composite key accessors
    models.py       -- WorkflowRecord, StepRecord dataclasses
    store.py        -- Store base class
    inmemory_store.py -- InMemoryStore
    sqlite_store.py -- SqliteStore
    runner.py       -- Runner base class
    inprocess_runner.py -- InProcessRunner
    inprocess_worker.py -- InProcessWorker
    sqlite_runner.py -- SqliteRunner

tests/
  conftest.py       -- shared executors, specs, fixtures
  test_backend.py   -- Backend lifecycle operations
  test_workflow.py  -- Workflow construction, execution, cascade reset
  test_sqlite.py    -- SqliteStore and SqliteRunner
  test_status.py    -- StepStatus and WorkflowStatus enum value guards
  test_state_change_event.py -- on_state_change callback behaviour
  test_inprocess.py -- InProcessRunner and InProcessWorker

examples/
  ops/              -- abstract patterns: single_step, accumulation,
                       fan_out, fan_in, branch_halt, retry
  uses/             -- worked examples: fibonacci, fibonacci_async,
                       document_parser, multisource, validation,
                       stack_machine, conways_game_of_life
```

---

## Design notes

**State is always written before dispatch.** `mark_in_progress` is
called for all ready steps before any are submitted. This prevents
double-submission if two workers complete parallel steps and both
trigger `advance()`.

**The executor knows nothing about runfox.** It receives `(op, inputs)`
and returns a dict. The coupling between executor and orchestrator is
the `op` naming convention -- the dispatch token that identifies what
work the step performs. The executor codebase has no runfox dependency
and can be deployed independently.

**`op` is both step identity and dispatch token.** A single field serves
both roles. The executor switches on `op` to determine what to do.
`label` is an optional YAML-only field for human description; runfox
never reads it.

**The Workflow object has no trusted local state.** Every property and
method loads from the backend. Multiple instances with the same
`(workflow_execution_id, backend)` are interchangeable. Correct for any
system where more than one process may operate on the same workflow.

**Branch conditions are serialisable data.** JSON Logic expressions
stored in the workflow definition. Evaluable by any process with backend
access without importing application code.

**Set-branch cascade resets transitive dependents.** When a set-branch
fires, all steps that transitively depend on the reset targets are also
reset to ready. This prevents stale outputs from downstream steps
persisting across loop iterations. A stale result guard in
`on_step_result` discards any result arriving for a step that is no
longer IN_PROGRESS.

**on_step_result and advance are deliberately decoupled.**
on_step_result processes one step result and returns. advance finds and
submits all ready steps and returns. The caller drives the loop. Call
stack stays flat regardless of workflow depth.

**Backend is the only mutation point.** All state transitions go through
Backend methods. Workflow and the graph functions are pure
transformations over WorkflowRecord. No mutation happens outside the
backend.

**Store and Runner are separate objects.** Storage backends and
execution mechanisms have different extension axes. Either can be
swapped independently. A team may want DynamoDB storage with SQS
dispatch, or SQLite storage with in-process dispatch for local testing.

---

## Dependencies

- [json-logic-path](https://pypi.org/project/json-logic-path/) --
  input reference resolution and branch condition evaluation
- [PyYAML](https://pypi.org/project/PyYAML/) -- workflow definition
  parsing
