"""
abstract_accumulation -- steps writing into a shared state accumulator.

Each step runs in sequence and writes one field into the shared state
dict via merge_state. Later steps read from state rather than from
named step outputs. The workflow outcome is assembled from state.

This pattern suits pipelines that build a record incrementally where
the identity of the contributing step is less important than the
field it produced.

Demonstrates: merge_state, {"var": "state.*"} input resolution,
              accumulator as workflow result.
"""

import runfox as rfx

SPEC = """
name: accumulation

steps:
  - id: step_a
    fn: produce_a

  - id: step_b
    fn: produce_b
    depends_on: [step_a]

  - id: step_c
    fn: produce_c
    depends_on: [step_b]
    input:
      a: {"var": "state.a"}
      b: {"var": "state.b"}

outputs:
  a: {"var": "state.a"}
  b: {"var": "state.b"}
  c: {"var": "state.c"}
"""


def execute(fn, inputs):
    if fn == "produce_a":
        return {"a": 1}
    if fn == "produce_b":
        return {"b": 2}
    if fn == "produce_c":
        # step_c reads prior accumulator values and derives a third
        return {"c": inputs["a"] + inputs["b"]}


backend = rfx.Backend(executor=execute)
result = rfx.Workflow.from_yaml(SPEC, backend).run()

print(result.outcome)
# {'a': 1, 'b': 2, 'c': 3}
