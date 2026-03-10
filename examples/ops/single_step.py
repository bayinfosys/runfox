"""
abstract_single_step -- the minimal runfox workflow.

One step. One executor. One output.

Demonstrates: step definition, fn dispatch, literal inputs, outputs.
"""

import runfox as rfx

SPEC = """
name: single_step

steps:
  - id: greet
    fn: greet
    input:
      name: world

outputs:
  message: {"var": "steps.greet.output.message"}
"""


def execute(fn, inputs):
    if fn == "greet":
        return {"message": f"hello, {inputs['name']}"}


backend = rfx.Backend(executor=execute)
wf = rfx.Workflow.from_yaml(SPEC, backend)
result = wf.run()

print(result.outcome)
# {'message': 'hello, world'}
