"""
abstract_fan_in -- multiple parallel steps feed a single final step.

Three steps have no dependencies and are submitted together on the
first advance() call. The final step declares all three as depends_on
and is only submitted once all of them are complete. It receives each
of their outputs as resolved inputs.

Demonstrates: multi-dependency fan-in, parallel root steps,
              input resolution from multiple prior steps.
"""

import runfox as rfx

SPEC = """
name: fan_in

steps:
  - op: source_a
    input:
      key: a

  - op: source_b
    input:
      key: b

  - op: source_c
    input:
      key: c

  - op: merge
    depends_on: [source_a, source_b, source_c]
    input:
      a: {"var": "steps.source_a.output.value"}
      b: {"var": "steps.source_b.output.value"}
      c: {"var": "steps.source_c.output.value"}

outputs:
  total: {"var": "steps.merge.output.total"}
"""


def execute(op, inputs):
    if op == "source":
        return {"value": len(inputs["key"])}  # trivial: length of key name
    if op == "merge":
        return {"total": inputs["a"] + inputs["b"] + inputs["c"]}


backend = rfx.Backend(executor=execute)
wf = rfx.Workflow.from_yaml(SPEC, backend)
result = wf.run()

print(result.outcome)
# {'total': 3}  -- 1 + 1 + 1
