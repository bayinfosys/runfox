"""
stack_machine.py -- a stack-based RPN calculator via runfox.

A stack is maintained in the shared state accumulator as a list under
the key "stack". An operations list is loaded from workflow inputs.

Three steps run in sequence on each pass:

  fetch   -- pops the next operation from the ops queue, writes it and
             its operands (if any) to state.

  execute -- reads the current op and stack from state, applies the
             operation, writes back the updated stack and result.

  check   -- reads the ops queue length from state:
               complete  if the queue is empty
               set fetch otherwise (resets fetch and execute to pending,
                                    loops back for the next operation)

This demonstrates:
  - set targeting multiple steps: check resets both fetch and execute
  - a processing queue drained by a loop
  - state accumulator as the working memory of the machine

Supported operations: push <n>, add, sub, mul, div, dup, drop, swap.

Usage
-----
    python3 stack_machine.py
    python3 stack_machine.py --ops "push 3, push 4, add, push 2, mul"
    python3 stack_machine.py --ops "push 10, push 3, dup, mul, sub"
"""

import argparse

import yaml

from runfox import Backend, Workflow
from runfox.backend import InMemoryStore, InProcessRunner, InProcessWorker

SPEC = """
name: stack_machine
steps:
  - id: init
    fn: init
    input:
      ops: {"var": "input.ops"}

  - id: fetch
    fn: fetch
    depends_on: [init]
    input:
      ops_queue: {"var": "state.ops_queue"}

  - id: execute
    fn: execute_op
    depends_on: [fetch]
    input:
      op:    {"var": "state.current_op"}
      stack: {"var": "state.stack"}

  - id: check
    fn: check
    depends_on: [execute]
    input:
      ops_queue: {"var": "state.ops_queue"}
    branch:
      - condition: {"==": [{"var": "ops_remaining"}, 0]}
        action: complete
      - condition: {">": [{"var": "ops_remaining"}, 0]}
        action: {set: ["steps.fetch.status", "steps.execute.status"], value: ready}

outputs:
  stack:  {"var": "state.stack"}
  result: {"var": "state.result"}
"""


def parse_ops(ops_str):
    """Parse a comma-separated ops string into a list of op dicts."""
    ops = []
    for token in ops_str.split(","):
        token = token.strip()
        if token.startswith("push "):
            ops.append({"op": "push", "val": float(token[5:])})
        else:
            ops.append({"op": token})
    return ops


def apply_op(op_dict, stack):
    op = op_dict["op"]
    stack = list(stack)

    if op == "push":
        stack.append(op_dict["val"])
    elif op == "add":
        b, a = stack.pop(), stack.pop()
        stack.append(a + b)
    elif op == "sub":
        b, a = stack.pop(), stack.pop()
        stack.append(a - b)
    elif op == "mul":
        b, a = stack.pop(), stack.pop()
        stack.append(a * b)
    elif op == "div":
        b, a = stack.pop(), stack.pop()
        stack.append(a / b)
    elif op == "dup":
        stack.append(stack[-1])
    elif op == "drop":
        stack.pop()
    elif op == "swap":
        stack[-1], stack[-2] = stack[-2], stack[-1]
    else:
        raise ValueError(f"Unknown op: {op!r}")

    return stack


def execute(fn, inputs):
    if fn == "init":
        ops = (
            parse_ops(inputs["ops"])
            if isinstance(inputs["ops"], str)
            else inputs["ops"]
        )
        return {"stack": [], "ops_queue": ops, "current_op": None, "result": None}

    if fn == "fetch":
        queue = list(inputs["ops_queue"])
        op = queue.pop(0)
        return {"current_op": op, "ops_queue": queue}

    if fn == "execute_op":
        stack = apply_op(inputs["op"], inputs["stack"])
        result = stack[-1] if stack else None
        return {"stack": stack, "result": result}

    if fn == "check":
        queue = inputs["ops_queue"]
        return {"ops_remaining": len(queue)}

    raise ValueError(f"Unknown fn: {fn!r}")


def on_state_change(workflow_id, previous, current, event):
    if event is None or event.fn != "execute_op":
        return
    if "stack" not in current:
        return
    op = current.get("current_op")
    if op is None:
        return
    stack = current.get("stack", [])
    remaining = len(current.get("ops_queue", []))
    op_str = f"push {op['val']}" if op.get("val") is not None else op["op"]
    print(
        f"  {op_str:<12}  stack: {[round(v, 4) for v in stack]}"
        f"  ({remaining} remaining)"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ops",
        default="push 3, push 4, add, push 2, mul",
        help="Comma-separated RPN operations",
    )
    args = parser.parse_args()

    spec = yaml.safe_load(SPEC)
    runner = InProcessRunner()
    worker = InProcessWorker(runner, execute)
    backend = Backend(
        store=InMemoryStore(), runner=runner, on_state_change=on_state_change
    )

    wf = Workflow.from_dict(spec, backend, inputs={"ops": args.ops})

    print(f"Workflow {wf.id!r}")
    print(f"Program : {args.ops}")
    print(f"{'Op':<12}  Stack")
    print("-" * 40)

    result = wf.run(worker=worker)

    print("-" * 40)
    print(f"Result  : {result.outcome['result']}")
    print(f"Stack   : {result.outcome['stack']}")
    print(f"Elapsed : {wf.elapsed:.3f}s")


if __name__ == "__main__":
    main()
