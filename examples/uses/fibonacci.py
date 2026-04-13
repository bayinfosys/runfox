"""
Fibonacci workflow -- local in-memory execution.

Demonstrates the full runfox pattern:
  - executor is a plain function with no runfox imports
  - InMemoryBackend wires executor -> on_result -> advance
  - Workflow is a stateless handle; run() drives to completion
"""

import runfox as rfx

SPEC = """
name: fibonacci
description: >
  Compute the Fibonacci sequence up to index 6.
  f0=0, f1=1, f2=1, f3=2, f4=3, f5=5, f6=8.

  f0 and f1 are seed values with no dependencies -- they are submitted
  in parallel on the first advance(). Each subsequent step depends on
  the two preceding values and is submitted as soon as both are complete.

steps:
  - op: f0
    input:
      value: 0

  - op: f1
    input:
      value: 1

  - op: f2
    depends_on: [f0, f1]
    input:
      a: {"var": "steps.f0.output.value"}
      b: {"var": "steps.f1.output.value"}

  - op: f3
    depends_on: [f1, f2]
    input:
      a: {"var": "steps.f1.output.value"}
      b: {"var": "steps.f2.output.value"}

  - op: f4
    depends_on: [f2, f3]
    input:
      a: {"var": "steps.f2.output.value"}
      b: {"var": "steps.f3.output.value"}

  - op: f5
    depends_on: [f3, f4]
    input:
      a: {"var": "steps.f3.output.value"}
      b: {"var": "steps.f4.output.value"}

  - op: f6
    depends_on: [f4, f5]
    input:
      a: {"var": "steps.f4.output.value"}
      b: {"var": "steps.f5.output.value"}

outputs:
  result: {"var": "steps.f6.output.value"}
"""

# ---------------------------------------------------------------------------
# Executor
#
# Receives (op, inputs), returns output dict.
# No runfox imports. No knowledge of workflow structure.
# On AWS this would be an ECS task writing to DynamoDB.
# ---------------------------------------------------------------------------


def execute(op: str, inputs: dict) -> dict:
    print(f"  execute: op={op!r}  inputs={inputs}")
    if "a" in inputs and "b" in inputs:
        return {"value": inputs["a"] + inputs["b"]}
    return {"value": inputs["value"]}


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def main():
    backend = rfx.Backend(executor=execute)
    wf = rfx.Workflow.from_yaml(SPEC, backend)

    print(f"\nWorkflow {wf.id!r} created")
    result = wf.run()

    print(f"\nResult : {result}")
    print(f"Outcome: {result.outcome}")
    print("\nStep statuses:")
    for step_id, status in wf.step_statuses.items():
        print(f"  {step_id}: {status}")


if __name__ == "__main__":
    main()
