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
  - id: source_a
    fn: source
    input:
      key: a

  - id: source_b
    fn: source
    input:
      key: b

  - id: source_c
    fn: source
    input:
      key: c

  - id: merge
    fn: merge
    depends_on: [source_a, source_b, source_c]
    input:
      a: {"var": "steps.source_a.output.value"}
      b: {"var": "steps.source_b.output.value"}
      c: {"var": "steps.source_c.output.value"}

outputs:
  total: {"var": "steps.merge.output.total"}
"""


def execute(fn, inputs):
    if fn == "source":
        return {"value": len(inputs["key"])}  # trivial: length of key name
    if fn == "merge":
        return {"total": inputs["a"] + inputs["b"] + inputs["c"]}


backend = rfx.Backend(executor=execute)
wf = rfx.Workflow.from_yaml(SPEC, backend)
result = wf.run()

print(result.outcome)
# {'total': 3}  -- 1 + 1 + 1
