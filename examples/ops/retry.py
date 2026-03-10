"""
abstract_retry -- a step runs multiple times using max_attempts.

A seed step writes an initial value into the shared state accumulator.
The iterate step then runs max_attempts times. On each attempt it reads
the current value from state (via {"var": "state.*"}) and writes back
an updated value. The accumulator carries state across attempts; the
step has no other memory.

This is the iterative loop pattern: the step spec is fixed, but
the inputs it receives change on every attempt because the accumulator
is updated by merge_state after each run.

Demonstrates: max_attempts, reset_for_retry, state as loop memory.
"""

import runfox as rfx

SPEC = """
name: retry

steps:
  - id: seed
    fn: seed

  - id: iterate
    fn: iterate
    depends_on: [seed]
    max_attempts: 5
    input:
      current: {"var": "state.total"}

outputs:
  total: {"var": "state.total"}
"""


def execute(fn, inputs):
    if fn == "seed":
        return {"total": 0}
    if fn == "iterate":
        return {"total": inputs["current"] + 10}


backend = rfx.Backend(executor=execute)
wf = rfx.Workflow.from_yaml(SPEC, backend)
result = wf.run()

print(result.outcome)
# {'total': 50}  -- 5 attempts * 10
