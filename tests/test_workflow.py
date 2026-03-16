"""
test_workflow.py -- Workflow construction and execution.

Covers: Workflow construction, fibonacci execution, branch conditions,
input resolution, set-action loops, and max_attempts retry.

All tests use the default InMemoryStore + InProcessRunner via
Backend(executor=...).
"""

import pytest
from conftest import (BRANCH_PASS_SPEC, FIB6_YAML, FIB_SPEC, HALT_SPEC,
                      SET_LOOP_YAML, counting_executor, fib_executor,
                      halting_executor)

import runfox as rfx
from runfox import Complete, Dispatch, Halt, Pending
from runfox.backend import Backend
from runfox.status import StepStatus, WorkflowStatus

# ---------------------------------------------------------------------------
# Workflow construction
# ---------------------------------------------------------------------------


class TestWorkflowConstruction:

    def test_from_dict(self):
        backend = Backend(executor=fib_executor)
        wf = rfx.Workflow.from_dict(FIB_SPEC, backend)
        assert wf.status == WorkflowStatus.PENDING

    def test_from_yaml(self):
        backend = Backend(executor=fib_executor)
        wf = rfx.Workflow.from_yaml(FIB6_YAML, backend)
        assert wf.status == WorkflowStatus.PENDING

    def test_resume(self):
        backend = Backend(executor=fib_executor)
        wf_id = backend.create(FIB_SPEC)
        assert rfx.Workflow.resume(wf_id, backend).id == wf_id

    def test_resume_unknown_raises(self):
        backend = Backend(executor=fib_executor)
        with pytest.raises(KeyError):
            rfx.Workflow.resume("bad-id", backend)

    def test_repr(self):
        backend = Backend(executor=fib_executor)
        wf = rfx.Workflow.from_dict(FIB_SPEC, backend)
        assert "Workflow" in repr(wf) and wf.id in repr(wf)


# ---------------------------------------------------------------------------
# Fibonacci execution
# ---------------------------------------------------------------------------


class TestFibonacci:

    def test_fib_6_equals_8(self):
        backend = Backend(executor=fib_executor)
        result = rfx.Workflow.from_yaml(FIB6_YAML, backend).run()
        assert isinstance(result, Complete)
        assert result.outcome["result"] == 8

    def test_fib_4_equals_3(self):
        backend = Backend(executor=fib_executor)
        result = rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        assert isinstance(result, Complete)
        assert result.outcome["result"] == 3

    def test_status_complete_after_run(self):
        backend = Backend(executor=fib_executor)
        wf = rfx.Workflow.from_dict(FIB_SPEC, backend)
        wf.run()
        assert wf.status == WorkflowStatus.COMPLETE

    def test_all_steps_complete_after_run(self):
        backend = Backend(executor=fib_executor)
        wf = rfx.Workflow.from_dict(FIB_SPEC, backend)
        wf.run()
        for status in wf.step_statuses.values():
            assert status == StepStatus.COMPLETE

    def test_outcome_accessible_via_property(self):
        backend = Backend(executor=fib_executor)
        wf = rfx.Workflow.from_dict(FIB_SPEC, backend)
        wf.run()
        assert wf.outcome["result"] == 3

    def test_no_duplicate_executions(self):
        calls = []

        def counting_fib(fn, inputs):
            calls.append(fn)
            return fib_executor(fn, inputs)

        backend = Backend(executor=counting_fib)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        assert len(calls) == 5


# ---------------------------------------------------------------------------
# Branch conditions
# ---------------------------------------------------------------------------


class TestBranches:

    def test_halt_fires_when_condition_met(self):
        backend = Backend(executor=halting_executor)
        result = rfx.Workflow.from_dict(HALT_SPEC, backend).run()
        assert isinstance(result, Halt)
        assert result.result == {"status": "rejected", "reason": "unsafe"}

    def test_downstream_step_not_executed_after_halt(self):
        calls = []

        def tracking(fn, inputs):
            calls.append(fn)
            return halting_executor(fn, inputs)

        backend = Backend(executor=tracking)
        rfx.Workflow.from_dict(HALT_SPEC, backend).run()
        assert "identity" not in calls

    def test_no_halt_when_condition_not_met(self):
        backend = Backend(executor=halting_executor)
        result = rfx.Workflow.from_dict(BRANCH_PASS_SPEC, backend).run()
        assert isinstance(result, Complete)

    def test_status_halted_after_halt(self):
        backend = Backend(executor=halting_executor)
        wf = rfx.Workflow.from_dict(HALT_SPEC, backend)
        wf.run()
        assert wf.status == WorkflowStatus.HALTED


# ---------------------------------------------------------------------------
# Input resolution
# ---------------------------------------------------------------------------


class TestInputResolution:

    def test_var_resolves_prior_step_output(self):
        backend = Backend(executor=fib_executor)
        wf = rfx.Workflow.from_dict(FIB_SPEC, backend)
        wf.run()
        assert backend.load(wf.id).steps["f2"].output["value"] == 1

    def test_plain_literal_input_passed_through(self):
        backend = Backend(executor=counting_executor)
        spec = {
            "name": "literal-test",
            "steps": [{"op": "count", "input": {"text": "strawberry"}}],
            "outputs": {"count": {"var": "steps.count.output.count"}},
        }
        result = rfx.Workflow.from_dict(spec, backend).run()
        assert result.outcome["count"] == 3

    def test_state_var_resolves_accumulator(self):
        def executor(fn, inputs):
            if fn == "write":
                return {"total": 42}
            if fn == "read":
                return {"got": inputs["value"]}
            raise ValueError(fn)

        spec = {
            "name": "state-resolution",
            "steps": [
                {"op": "write"},
                {
                    "op": "read",
                    "depends_on": ["write"],
                    "input": {"value": {"var": "state.total"}},
                },
            ],
            "outputs": {"got": {"var": "steps.read.output.got"}},
        }
        result = rfx.Workflow.from_dict(spec, Backend(executor=executor)).run()
        assert result.outcome["got"] == 42

    def test_workflow_input_var_resolves(self):
        def executor(fn, inputs):
            if fn == "echo":
                return {"out": inputs["val"]}
            raise ValueError(fn)

        spec = {
            "name": "input-resolution",
            "steps": [
                {"op": "echo", "input": {"val": {"var": "input.name"}}},
            ],
            "outputs": {"out": {"var": "steps.echo.output.out"}},
        }
        result = rfx.Workflow.from_dict(
            spec, Backend(executor=executor), inputs={"name": "alice"}
        ).run()
        assert result.outcome["out"] == "alice"


# ---------------------------------------------------------------------------
# Set action and loops
# ---------------------------------------------------------------------------


class TestSetAction:

    def test_set_loop_runs_to_completion(self):
        def increment(fn, inputs):
            return {"n": (inputs.get("n") or 0) + 1}

        backend = Backend(executor=increment)
        result = rfx.Workflow.from_yaml(SET_LOOP_YAML, backend).run()
        assert isinstance(result, Complete)
        assert result.outcome["result"] == 5

    def test_set_loop_correct_iteration_count(self):
        calls = []

        def increment(fn, inputs):
            calls.append(inputs.get("n") or 0)
            return {"n": (inputs.get("n") or 0) + 1}

        backend = Backend(executor=increment)
        rfx.Workflow.from_yaml(SET_LOOP_YAML, backend).run()
        assert calls == [0, 1, 2, 3, 4]

    def test_set_resets_run_id(self):
        def increment(fn, inputs):
            return {"n": (inputs.get("n") or 0) + 1}

        backend = Backend(executor=increment)
        wf = rfx.Workflow.from_yaml(SET_LOOP_YAML, backend)
        wf.run()
        # 5 dispatches: run_id starts at 0, incremented on each reset
        assert backend.load(wf.id).steps["count"].run_id == 4


# ---------------------------------------------------------------------------
# max_attempts retry
# ---------------------------------------------------------------------------


class TestRetry:

    def test_retry_runs_max_attempts_times(self):
        RETRY_SPEC = {
            "name": "retry-test",
            "steps": [{"op": "flaky", "max_attempts": 3}],
            "outputs": {"run_id": {"var": "steps.flaky.output.run_id"}},
        }
        run_ids = []
        backend = Backend(executor=lambda fn, inputs: None)
        wf = rfx.Workflow.from_dict(RETRY_SPEC, backend)

        def flaky(fn, inputs):
            r = backend.load(wf.id).steps["flaky"].run_id
            run_ids.append(r)
            return {"run_id": r}

        backend._worker._executor = flaky
        wf.run()
        assert run_ids == [0, 1, 2]

    def test_retry_final_output_is_last_attempt(self):
        RETRY_SPEC = {
            "name": "retry-final",
            "steps": [{"op": "s", "max_attempts": 2}],
            "outputs": {"v": {"var": "steps.s.output.v"}},
        }
        backend = Backend(executor=lambda fn, inputs: None)
        wf = rfx.Workflow.from_dict(RETRY_SPEC, backend)

        def f(fn, inputs):
            r = backend.load(wf.id).steps["s"].run_id
            return {"v": r}

        backend._worker._executor = f
        result = wf.run()
        assert result.outcome["v"] == 1


class TestTiming:
    def test_elapsed_is_zero_before_run(self):
        backend = Backend(executor=fib_executor)
        wf = rfx.Workflow.from_dict(FIB_SPEC, backend)
        assert wf.elapsed == 0.0

    def test_elapsed_is_positive_after_run(self):
        backend = Backend(executor=fib_executor)
        wf = rfx.Workflow.from_dict(FIB_SPEC, backend)
        wf.run()
        assert wf.elapsed > 0.0

    def test_step_durations_after_run(self):
        backend = Backend(executor=fib_executor)
        wf = rfx.Workflow.from_dict(FIB_SPEC, backend)
        wf.run()
        durations = wf.step_durations
        assert set(durations.keys()) == {"f0", "f1", "f2", "f3", "f4"}
        for v in durations.values():
            assert v >= 0.0


# ---------------------------------------------------------------------------
# Cascade reset and stale result guard
# ---------------------------------------------------------------------------


class TestCascadeReset:

    def test_cascade_resets_linear_chain(self):
        SPEC = """
name: test
steps:
  - op: a
  - op: b
    depends_on: [a]
  - op: c
    depends_on: [b]
"""
        backend = Backend()
        wf = rfx.Workflow.from_yaml(SPEC, backend)

        for op in ["a", "b", "c"]:
            backend.mark_in_progress(wf.id, op)
            backend.mark_complete(wf.id, op)

        from runfox.workflow import _find_transitive_dependents
        record = backend.load(wf.id)
        dependents = _find_transitive_dependents(record, ["a"])
        for op in ["a"] + dependents:
            backend.reset_step(wf.id, op)

        statuses = wf.step_statuses
        assert statuses["a"] == StepStatus.READY
        assert statuses["b"] == StepStatus.READY
        assert statuses["c"] == StepStatus.READY

    def test_cascade_resets_diamond(self):
        SPEC = """
name: test
steps:
  - op: a
  - op: b
    depends_on: [a]
  - op: c
    depends_on: [a]
  - op: d
    depends_on: [b, c]
"""
        backend = Backend()
        wf = rfx.Workflow.from_yaml(SPEC, backend)

        for op in ["a", "b", "c", "d"]:
            backend.mark_in_progress(wf.id, op)
            backend.mark_complete(wf.id, op)

        from runfox.workflow import _find_transitive_dependents
        record = backend.load(wf.id)
        dependents = _find_transitive_dependents(record, ["a"])
        for op in ["a"] + dependents:
            backend.reset_step(wf.id, op)

        statuses = wf.step_statuses
        assert statuses["a"] == StepStatus.READY
        assert statuses["b"] == StepStatus.READY
        assert statuses["c"] == StepStatus.READY
        assert statuses["d"] == StepStatus.READY

    def test_cascade_does_not_reset_unrelated_steps(self):
        SPEC = """
name: test
steps:
  - op: a
  - op: b
  - op: c
    depends_on: [a, b]
"""
        backend = Backend()
        wf = rfx.Workflow.from_yaml(SPEC, backend)

        for op in ["a", "b", "c"]:
            backend.mark_in_progress(wf.id, op)
            backend.mark_complete(wf.id, op)

        from runfox.workflow import _find_transitive_dependents
        record = backend.load(wf.id)
        dependents = _find_transitive_dependents(record, ["a"])
        for op in ["a"] + dependents:
            backend.reset_step(wf.id, op)

        statuses = wf.step_statuses
        assert statuses["a"] == StepStatus.READY
        assert statuses["b"] == StepStatus.COMPLETE
        assert statuses["c"] == StepStatus.READY

    def test_stale_result_discarded_after_reset(self):
        SPEC = """
name: test
steps:
  - op: a
  - op: b
    depends_on: [a]
"""
        backend = Backend()
        wf = rfx.Workflow.from_yaml(SPEC, backend)

        backend.mark_in_progress(wf.id, "a")
        backend.mark_complete(wf.id, "a")
        backend.mark_in_progress(wf.id, "b")

        backend.reset_step(wf.id, "b")
        assert wf.step_statuses["b"] == StepStatus.READY

        result = wf.on_step_result("b", {"value": 99})
        assert result is None
        assert wf.step_statuses["b"] == StepStatus.READY

    def test_cascade_end_to_end(self):
        SPEC = """
name: test
steps:
  - op: seed

  - op: loop
    depends_on: [seed]
    input:
      n: {"var": "state.n"}
    branch:
      - condition: {">=": [{"var": "n"}, 3]}
        action: complete
      - condition: {"<": [{"var": "n"}, 3]}
        action: {set: "steps.loop.status"}

  - op: downstream
    depends_on: [loop]
    input:
      n: {"var": "state.n"}

outputs:
  n: {"var": "state.n"}
  downstream_n: {"var": "steps.downstream.output.seen"}
"""
        call_log = []

        def execute(op, inputs):
            call_log.append(op)
            if op == "seed":
                return {"n": 0}
            if op == "loop":
                return {"n": inputs["n"] + 1}
            if op == "downstream":
                return {"seen": inputs["n"]}

        backend = Backend(executor=execute)
        wf = rfx.Workflow.from_yaml(SPEC, backend)
        result = wf.run()

        assert isinstance(result, Complete)
        assert result.outcome["n"] == 3
        assert result.outcome["downstream_n"] == 3
        assert call_log.count("downstream") == 1
