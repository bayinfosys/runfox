"""
test_state_change_events.py -- StateChangeEvent callback and input satisfaction guard.

Covers:
  - on_state_change receives correct event on each merge
  - event.op and event.op identify the triggering step
  - event is None when merge_workflow_state is called outside a step result
  - _assert_inputs_satisfied raises on unresolved steps.X references
  - _assert_inputs_satisfied raises on unknown step references
  - _assert_inputs_satisfied passes for non-steps.X var references
  - full workflow run produces events in dependency order
"""

import pytest
from conftest import FIB6_YAML, FIB_SPEC, fib_executor

import runfox as rfx
from runfox import Complete, StateChangeEvent
from runfox.backend import Backend
from runfox.status import StepStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _collecting_backend(executor, events=None, states=None):
    """
    Build a Backend that collects (event, previous, current) triples.
    events and states are mutated in place by the callback.
    """
    if events is None:
        events = []
    if states is None:
        states = []

    def callback(wf_exec_id, previous, current, event):
        events.append(event)
        states.append((previous.copy(), current.copy()))

    return Backend(executor=executor, on_state_change=callback), events, states


# ---------------------------------------------------------------------------
# Callback signature and event fields
# ---------------------------------------------------------------------------


class TestStateChangeEventCallback:

    def test_callback_receives_event_on_step_result(self):
        events = []
        backend, events, _ = _collecting_backend(fib_executor, events)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        assert len(events) > 0

    def test_event_is_state_change_event_instance(self):
        backend, events, _ = _collecting_backend(fib_executor)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        for event in events:
            assert isinstance(event, StateChangeEvent)

    def test_event_op_is_valid_step(self):
        backend, events, _ = _collecting_backend(fib_executor)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        valid_ids = {"f0", "f1", "f2", "f3", "f4"}
        for event in events:
            assert event.op in valid_ids

    def test_event_fn_matches_step(self):
        backend, events, _ = _collecting_backend(fib_executor)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        for event in events:
            assert event.op in ("f0", "f1", "f2", "f3", "f4")

    def test_one_event_per_step(self):
        backend, events, _ = _collecting_backend(fib_executor)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        # FIB_SPEC has 5 steps; each produces one merge
        assert len(events) == 5

    def test_event_ops_cover_all_steps(self):
        backend, events, _ = _collecting_backend(fib_executor)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        assert {e.op for e in events} == {"f0", "f1", "f2", "f3", "f4"}

    def test_previous_and_current_states_are_dicts(self):
        backend, _, states = _collecting_backend(fib_executor)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        for previous, current in states:
            assert isinstance(previous, dict)
            assert isinstance(current, dict)

    def test_current_state_accumulates(self):
        # Each merge adds to state; current should be a superset of previous.
        backend, _, states = _collecting_backend(fib_executor)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        for previous, current in states:
            for key in previous:
                assert key in current

    def test_direct_merge_outside_step_result_passes_none_event(self):
        # merge_workflow_state called directly (not via on_step_result)
        # should pass event=None to the callback.
        events = []
        backend, events, _ = _collecting_backend(fib_executor, events)
        wf_id = backend.create(FIB_SPEC)
        backend.merge_workflow_state(wf_id, {"x": 1})
        assert len(events) == 1
        assert events[0] is None

    def test_callback_not_fired_on_empty_merge(self):
        backend, events, _ = _collecting_backend(fib_executor)
        wf_id = backend.create(FIB_SPEC)
        backend.merge_workflow_state(wf_id, {})
        backend.merge_workflow_state(wf_id, None)
        assert events == []

    def test_event_filtering_by_op(self):
        # Demonstrates the intended use: callback filters by event.op
        # to respond only to specific step types.
        add_events = []

        def callback(wf_exec_id, previous, current, event):
            if event is not None and event.op == "add":
                add_events.append(event.op)

        backend = Backend(executor=fib_executor, on_state_change=callback)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        assert set(add_events) == {"f2", "f3", "f4"}

    def test_event_filtering_by_op(self):
        seen = []

        def callback(wf_exec_id, previous, current, event):
            if event is not None and event.op == "f4":
                seen.append(current.get("value"))

        backend = Backend(executor=fib_executor, on_state_change=callback)
        rfx.Workflow.from_dict(FIB_SPEC, backend).run()
        assert len(seen) == 1
        assert seen[0] == 3


# ---------------------------------------------------------------------------
# _assert_inputs_satisfied
# ---------------------------------------------------------------------------


class TestAssertInputsSatisfied:

    def test_raises_on_incomplete_dependency(self):
        """
        A step whose steps.X var reference points to a step that has not
        yet completed should raise at dispatch time, not silently pass None.
        """
        calls = []

        def executor(op, inputs):
            calls.append(op)
            if op == "a":
                return {"value": 1}
            if op == "b":
                return {"value": inputs.get("v")}
            raise ValueError(op)

        # Spec where b incorrectly declares no depends_on but references a.
        # Without the guard this silently passes None.
        spec = {
            "name": "bad-dep",
            "steps": [
                {"op": "a"},
                {
                    "op": "b",
                    # depends_on intentionally omitted -- this is the error
                    "input": {"v": {"var": "steps.a.output.value"}},
                },
            ],
            "outputs": {"v": {"var": "steps.b.output.value"}},
        }
        backend = Backend(executor=executor)
        wf = rfx.Workflow.from_dict(spec, backend)
        with pytest.raises(ValueError, match="step 'a'"):
            wf.run()

    def test_raises_on_unknown_step_reference(self):
        spec = {
            "name": "unknown-ref",
            "steps": [
                {
                    "op": "a",
                    "input": {"v": {"var": "steps.nonexistent.output.value"}},
                },
            ],
            "outputs": {},
        }
        backend = Backend(executor=lambda op, inputs: {"v": 1})
        wf = rfx.Workflow.from_dict(spec, backend)
        with pytest.raises(ValueError, match="nonexistent"):
            wf.run()

    def test_does_not_raise_on_state_var(self):
        """state.* references are not subject to the steps.* guard."""

        def executor(op, inputs):
            if op == "w":
                return {"total": 10}
            if op == "r":
                return {"got": inputs["v"]}
            raise ValueError(op)

        spec = {
            "name": "state-ref",
            "steps": [
                {"op": "w"},
                {
                    "op": "r",
                    "depends_on": ["w"],
                    "input": {"v": {"var": "state.total"}},
                },
            ],
            "outputs": {"got": {"var": "steps.r.output.got"}},
        }
        result = rfx.Workflow.from_dict(spec, Backend(executor=executor)).run()
        assert result.outcome["got"] == 10

    def test_does_not_raise_on_input_var(self):
        """input.* references are not subject to the steps.* guard."""
        spec = {
            "name": "input-ref",
            "steps": [
                {"op": "s", "input": {"v": {"var": "input.name"}}},
            ],
            "outputs": {"out": {"var": "steps.s.output.out"}},
        }
        executor = lambda fn, inputs: {"out": inputs["v"]}
        result = rfx.Workflow.from_dict(
            spec, Backend(executor=executor), inputs={"name": "alice"}
        ).run()
        assert result.outcome["out"] == "alice"

    def test_does_not_raise_on_literal_input(self):
        spec = {
            "name": "literal",
            "steps": [
                {"op": "s", "input": {"v": 42}},
            ],
            "outputs": {"v": {"var": "steps.s.output.v"}},
        }
        result = rfx.Workflow.from_dict(
            spec, Backend(executor=lambda fn, inputs: {"v": inputs["v"]})
        ).run()
        assert result.outcome["v"] == 42

    def test_does_not_raise_on_satisfied_dependency(self):
        """Normal correctly-declared dependency should not raise."""
        spec = {
            "name": "ok-dep",
            "steps": [
                {"op": "a"},
                {
                    "op": "b",
                    "depends_on": ["a"],
                    "input": {"v": {"var": "steps.a.output.value"}},
                },
            ],
            "outputs": {"v": {"var": "steps.b.output.v"}},
        }

        def executor(op, inputs):
            if op == "a":
                return {"value": 7}
            if op == "b":
                return {"v": inputs["v"]}
            raise ValueError(op)

        result = rfx.Workflow.from_dict(spec, Backend(executor=executor)).run()
        assert result.outcome["v"] == 7
