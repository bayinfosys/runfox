"""
abstract_branch_halt -- a step evaluates its output and halts the workflow.

The branch declaration on a step specifies a JSON Logic condition
evaluated against the step's output fields. If the condition is met
the workflow is marked halted, downstream steps are not executed, and
the branch result payload becomes the workflow outcome.

Two runs are shown: one that halts, one that continues to completion.

Demonstrates: branch, condition, halt action, result payload,
              downstream step suppression, workflow-level inputs.
"""

import runfox as rfx

SPEC = """
name: branch_halt

steps:
  - id: score
    fn: score
    input:
      value: {"var": "input.value"}
    branch:
      - condition: {">=": [{"var": "score"}, 0.7]}
        action: halt
        result:
          status: rejected
          reason: score too high

  - id: accept
    fn: accept
    depends_on: [score]
    input:
      score: {"var": "steps.score.output.score"}

outputs:
  status: {"var": "steps.accept.output.status"}
"""


def execute(fn, inputs):
    if fn == "score":
        return {"score": inputs["value"]}
    if fn == "accept":
        return {"status": "accepted"}


print("-- run 1: value=0.9, expect Halt --")
backend = rfx.Backend(executor=execute)
result = rfx.Workflow.from_yaml(SPEC, backend, inputs={"value": 0.9}).run()
print(type(result).__name__, result)

print()
print("-- run 2: value=0.3, expect Complete --")
backend = rfx.Backend(executor=execute)
result = rfx.Workflow.from_yaml(SPEC, backend, inputs={"value": 0.3}).run()
print(type(result).__name__, result)
