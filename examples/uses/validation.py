"""
worked_validation -- validate a data record against parallel rules.

An input record is checked against three independent rules in parallel.
Each step writes its result into the shared state accumulator under a
distinct top-level key (name_ok, age_ok, email_ok). Because merge_state
is a shallow dict.update, parallel steps must not share a key -- each
step owns its own namespace in the accumulator.

A branch on the age check halts immediately if the value is out of
range. If all checks pass a final report step reads the accumulated
flags and produces a validation summary.

This pattern is common in ETL pipelines where a record must satisfy
several constraints before it is accepted for downstream processing.

Demonstrates: parallel validation steps, per-step state keys,
              branch/halt on first failure, fan-in assembly step.
"""

import pprint

import runfox as rfx

SPEC = """
name: validation

steps:
  - id: check_name
    fn: check_name
    input:
      value: {"var": "input.name"}

  - id: check_age
    fn: check_age
    input:
      value: {"var": "input.age"}
    branch:
      - condition: {"!": [{"var": "age_ok"}]}
        action: halt
        result:
          status: rejected
          field:  age
          reason: must be 18 or over

  - id: check_email
    fn: check_email
    input:
      value: {"var": "input.email"}

  - id: report
    fn: report
    depends_on: [check_name, check_age, check_email]
    input:
      name_ok:  {"var": "state.name_ok"}
      age_ok:   {"var": "state.age_ok"}
      email_ok: {"var": "state.email_ok"}

outputs:
  report: {"var": "steps.report.output.report"}
"""


def execute(fn, inputs):
    if fn == "check_name":
        ok = bool(inputs["value"]) and len(inputs["value"]) >= 2
        return {"name_ok": ok, "name_reason": "" if ok else "name too short"}

    if fn == "check_age":
        ok = int(inputs["value"]) >= 18
        return {"age_ok": ok, "age_reason": "" if ok else "must be 18 or over"}

    if fn == "check_email":
        ok = "@" in str(inputs["value"])
        return {"email_ok": ok, "email_reason": "" if ok else "invalid email"}

    if fn == "report":
        all_passed = inputs["name_ok"] and inputs["age_ok"] and inputs["email_ok"]
        return {
            "report": {
                "status": "accepted" if all_passed else "failed",
                "name": inputs["name_ok"],
                "age": inputs["age_ok"],
                "email": inputs["email_ok"],
            }
        }


def run(label, record):
    print(f"-- {label} --")
    backend = rfx.Backend(executor=execute)
    wf = rfx.Workflow.from_yaml(SPEC, backend, inputs=record)
    result = wf.run()
    print(f"  {type(result).__name__}")
    pprint.pprint(result.result if hasattr(result, "result") else result.outcome)
    print()


run("valid record", {"name": "Alice", "age": 30, "email": "alice@example.com"})
run("underage: expect Halt", {"name": "Bob", "age": 17, "email": "bob@example.com"})
run("bad email: expect failed", {"name": "Carol", "age": 25, "email": "notanemail"})
