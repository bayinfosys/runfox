"""
abstract_fan_out -- one step dispatches multiple parallel steps.

A single root step completes, producing a value. All steps that depend
only on the root are found ready on the same advance() call and
submitted together. They execute in parallel (or in fast sequence in
InMemoryBackend, which is synchronous).

Demonstrates: parallel dispatch, single-dependency fan-out.
"""

import runfox as rfx

SPEC = """
name: fan_out

steps:
  - op: root

  - op: branch_a
    depends_on: [root]
    input:
      value: {"var": "steps.root.output.value"}
      label: a

  - op: branch_b
    depends_on: [root]
    input:
      value: {"var": "steps.root.output.value"}
      label: b

  - op: branch_c
    depends_on: [root]
    input:
      value: {"var": "steps.root.output.value"}
      label: c

outputs:
  a: {"var": "steps.branch_a.output.result"}
  b: {"var": "steps.branch_b.output.result"}
  c: {"var": "steps.branch_c.output.result"}
"""


def execute(op, inputs):
    if op == "root":
        return {"value": 10}
    if op == "branch_c":
        return {"result": inputs["value"] * 2}


backend = rfx.Backend(executor=execute)
wf = rfx.Workflow.from_yaml(SPEC, backend)
result = wf.run()

print(result.outcome)
# {'a': 20, 'b': 20, 'c': 20}
