"""
fibonacci_iter.py -- iterative fibonacci via SqliteBackend with a worker thread.

Demonstrates the event-driven execution pattern using SqliteBackend.

The main thread drives the workflow loop. A worker thread polls the tasks
table independently, executes each step, and writes the result back as
COMPLETE or ERROR. The two threads coordinate only through the database.

This is the local analogue of a distributed deployment: replace the worker
thread with a separate process (or container) and replace SQLite with
DynamoDB, and the structure is identical. The worker has no runfox imports
and no knowledge of workflow structure.

A seed step writes the initial pair into the shared state accumulator. The
iterate step loops via a set branch until fib_n >= 100, then fires the
complete branch. Termination is data-driven; no fixed iteration count.

Usage
-----
    python3 fibonacci_iter.py
"""

import os
import tempfile
import threading
import time

import runfox as rfx
from runfox.backend import Backend, SqliteStore

SPEC = """
name: fibonacci_iterative
steps:
  - op: seed
    label: initial response for 0,1

  - op: iterate
    label: fibonacci step function
    depends_on: [seed]
    input:
      a: {"var": "state.fib_n"}
      b: {"var": "state.fib_n1"}
    branch:
      - condition: {"<": [{"var": "fib_n"}, 100]}
        action: {set: "steps.iterate.status", value: ready}
      - condition: {">=": [{"var": "fib_n"}, 100]}
        action: complete

outputs:
  fib_n:  {"var": "state.fib_n"}
  fib_n1: {"var": "state.fib_n1"}
"""


# ---------------------------------------------------------------------------
# Executor
#
# Plain function. No runfox imports. In a real deployment this runs in a
# separate process or container. The worker thread calls it directly here
# to keep the example self-contained.
# ---------------------------------------------------------------------------


def execute(op: str, inputs: dict) -> dict:
    if op == "seed":
        return {"fib_n": 0, "fib_n1": 1}
    if op == "step":
        a, b = inputs["a"], inputs["b"]
        return {"fib_n": b, "fib_n1": a + b}
    raise ValueError(f"Unknown op: {op!r}")


# ---------------------------------------------------------------------------
# Worker
#
# Polls the tasks table for PENDING rows, claims each one, executes it, and
# writes COMPLETE or ERROR back to the same row. Coupled to the orchestrator
# only through the database and the task_key convention.
# ---------------------------------------------------------------------------


def worker(backend: Backend, stop: threading.Event) -> None:
    while not stop.is_set():
        jobs = backend.take_tasks()
        if not jobs:
            time.sleep(0.05)
            continue
        for job in jobs:
            try:
                output = execute(job.op, job.inputs)
            except Exception as exc:
                output = {"error": str(exc), "ok": False}
            backend.submit_result(job.workflow_execution_id, job.op, output)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(db_fd)

    try:
        backend = Backend(store=SqliteStore(db_path))
        wf = rfx.Workflow.from_yaml(SPEC, backend)

        print(f"Workflow {wf.id!r} created\n")

        stop = threading.Event()
        t = threading.Thread(target=worker, args=(backend, stop), daemon=True)
        t.start()

        result = wf.run()

        stop.set()
        t.join(timeout=2)

        print(f"\nResult:   {result.outcome}")
        print(f"Elapsed:  {wf.elapsed:.3f}s")
        print(f"Progress: {wf.progress}")
    finally:
        os.unlink(db_path)


if __name__ == "__main__":
    main()
