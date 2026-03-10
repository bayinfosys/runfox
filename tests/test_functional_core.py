"""
test_functional_core.py -- pure function tests for the workflow graph logic.

All functions under test are module-level pure functions over WorkflowRecord.
No Backend, no I/O, no threading. Tests construct records directly via
make_record and assert on return values only.

Functions covered:
  _find_dispatchable_steps
  _all_terminal
  _resolve_inputs
  _resolve_outputs
  _evaluate_branches
  _assert_inputs_satisfied
"""

import pytest
from conftest import make_record

from runfox.status import StepStatus, WorkflowStatus
from runfox.workflow import (_all_terminal, _assert_inputs_satisfied,
                             _evaluate_branches, _find_dispatchable_steps,
                             _resolve_inputs, _resolve_outputs, _parse_set_targets, _get_step_spec)

READY = StepStatus.READY
IN_PROGRESS = StepStatus.IN_PROGRESS
COMPLETE = StepStatus.COMPLETE
HALTED = StepStatus.HALTED
RETRY = StepStatus.RETRY


# ---------------------------------------------------------------------------
# _find_dispatchable_steps
# ---------------------------------------------------------------------------


class TestFindDispatchableSteps:

    def test_single_ready_step_no_deps(self):
        record = make_record({"a": READY})
        result = _find_dispatchable_steps(record)
        assert [s["id"] for s in result] == ["a"]

    def test_multiple_ready_steps_no_deps(self):
        record = make_record({"a": READY, "b": READY, "c": READY})
        ids = {s["id"] for s in _find_dispatchable_steps(record)}
        assert ids == {"a", "b", "c"}

    def test_step_with_incomplete_dep_not_dispatchable(self):
        record = make_record(
            {"a": READY, "b": READY},
            deps={"b": ["a"]},
        )
        ids = {s["id"] for s in _find_dispatchable_steps(record)}
        assert ids == {"a"}
        assert "b" not in ids

    def test_step_with_complete_dep_is_dispatchable(self):
        record = make_record(
            {"a": COMPLETE, "b": READY},
            deps={"b": ["a"]},
        )
        ids = {s["id"] for s in _find_dispatchable_steps(record)}
        assert ids == {"b"}

    def test_in_progress_step_not_dispatchable(self):
        record = make_record({"a": IN_PROGRESS})
        assert _find_dispatchable_steps(record) == []

    def test_complete_step_not_dispatchable(self):
        record = make_record({"a": COMPLETE})
        assert _find_dispatchable_steps(record) == []

    def test_halted_step_not_dispatchable(self):
        record = make_record({"a": HALTED})
        assert _find_dispatchable_steps(record) == []

    def test_retry_step_is_dispatchable(self):
        record = make_record({"a": RETRY})
        ids = {s["id"] for s in _find_dispatchable_steps(record)}
        assert ids == {"a"}

    def test_retry_step_with_incomplete_dep_not_dispatchable(self):
        record = make_record(
            {"a": READY, "b": RETRY},
            deps={"b": ["a"]},
        )
        ids = {s["id"] for s in _find_dispatchable_steps(record)}
        assert "b" not in ids

    def test_multiple_deps_all_must_be_complete(self):
        record = make_record(
            {"a": COMPLETE, "b": READY, "c": READY},
            deps={"c": ["a", "b"]},
        )
        ids = {s["id"] for s in _find_dispatchable_steps(record)}
        assert "c" not in ids
        assert "b" in ids

    def test_chain_only_first_dispatchable(self):
        record = make_record(
            {"a": READY, "b": READY, "c": READY},
            deps={"b": ["a"], "c": ["b"]},
        )
        ids = {s["id"] for s in _find_dispatchable_steps(record)}
        assert ids == {"a"}

    def test_empty_steps(self):
        record = make_record({})
        assert _find_dispatchable_steps(record) == []


# ---------------------------------------------------------------------------
# _all_terminal
# ---------------------------------------------------------------------------


class TestAllTerminal:

    def test_all_complete(self):
        assert _all_terminal(make_record({"a": COMPLETE, "b": COMPLETE}))

    def test_all_halted(self):
        assert _all_terminal(make_record({"a": HALTED, "b": HALTED}))

    def test_mixed_complete_and_halted(self):
        assert _all_terminal(make_record({"a": COMPLETE, "b": HALTED}))

    def test_one_ready(self):
        assert not _all_terminal(make_record({"a": COMPLETE, "b": READY}))

    def test_one_in_progress(self):
        assert not _all_terminal(make_record({"a": COMPLETE, "b": IN_PROGRESS}))

    def test_one_retry(self):
        assert not _all_terminal(make_record({"a": COMPLETE, "b": RETRY}))

    def test_empty_steps(self):
        assert _all_terminal(make_record({}))


# ---------------------------------------------------------------------------
# _resolve_inputs
# ---------------------------------------------------------------------------


class TestResolveInputs:

    def test_literal_string(self):
        record = make_record({"a": READY}, step_inputs={"a": {"key": "hello"}})
        step_spec = record.spec["steps"][0]
        assert _resolve_inputs(step_spec, record) == {"key": "hello"}

    def test_literal_number(self):
        record = make_record({"a": READY}, step_inputs={"a": {"n": 42}})
        step_spec = record.spec["steps"][0]
        assert _resolve_inputs(step_spec, record) == {"n": 42}

    def test_steps_var(self):
        record = make_record(
            {"a": COMPLETE, "b": READY},
            step_outputs={"a": {"value": 7}},
            step_inputs={"b": {"v": {"var": "steps.a.output.value"}}},
            deps={"b": ["a"]},
        )
        step_spec = next(s for s in record.spec["steps"] if s["id"] == "b")
        assert _resolve_inputs(step_spec, record) == {"v": 7}

    def test_state_var(self):
        record = make_record(
            {"a": READY},
            state={"total": 99},
            step_inputs={"a": {"v": {"var": "state.total"}}},
        )
        step_spec = record.spec["steps"][0]
        assert _resolve_inputs(step_spec, record) == {"v": 99}

    def test_input_var(self):
        record = make_record(
            {"a": READY},
            inputs={"name": "alice"},
            step_inputs={"a": {"v": {"var": "input.name"}}},
        )
        step_spec = record.spec["steps"][0]
        assert _resolve_inputs(step_spec, record) == {"v": "alice"}

    def test_no_inputs(self):
        record = make_record({"a": READY})
        step_spec = record.spec["steps"][0]
        assert _resolve_inputs(step_spec, record) == {}

    def test_mixed_literal_and_var(self):
        record = make_record(
            {"a": READY},
            state={"x": 5},
            step_inputs={"a": {"lit": 1, "from_state": {"var": "state.x"}}},
        )
        step_spec = record.spec["steps"][0]
        result = _resolve_inputs(step_spec, record)
        assert result == {"lit": 1, "from_state": 5}


# ---------------------------------------------------------------------------
# _resolve_outputs
# ---------------------------------------------------------------------------


class TestResolveOutputs:

    def test_step_output_var(self):
        record = make_record(
            {"a": COMPLETE},
            step_outputs={"a": {"value": 3}},
            spec_outputs={"result": {"var": "steps.a.output.value"}},
        )
        assert _resolve_outputs(record) == {"result": 3}

    def test_state_var(self):
        record = make_record(
            {"a": COMPLETE},
            state={"total": 10},
            spec_outputs={"out": {"var": "state.total"}},
        )
        assert _resolve_outputs(record) == {"out": 10}

    def test_literal_output(self):
        record = make_record(
            {"a": COMPLETE},
            spec_outputs={"status": "ok"},
        )
        assert _resolve_outputs(record) == {"status": "ok"}

    def test_empty_outputs(self):
        record = make_record({"a": COMPLETE})
        assert _resolve_outputs(record) == {}

    def test_multiple_outputs(self):
        record = make_record(
            {"a": COMPLETE, "b": COMPLETE},
            step_outputs={"a": {"x": 1}, "b": {"y": 2}},
            spec_outputs={
                "out_a": {"var": "steps.a.output.x"},
                "out_b": {"var": "steps.b.output.y"},
            },
        )
        assert _resolve_outputs(record) == {"out_a": 1, "out_b": 2}


# ---------------------------------------------------------------------------
# _evaluate_branches
# ---------------------------------------------------------------------------


class TestEvaluateBranches:

    def test_no_branches_returns_none_none(self):
        record = make_record({"a": READY})
        step_spec = record.spec["steps"][0]
        assert _evaluate_branches(step_spec, {}, record) == (None, None)

    def test_halt_branch_fires(self):
        record = make_record(
            {"a": READY},
            branches={
                "a": [
                    {
                        "condition": {">=": [{"var": "score"}, 0.7]},
                        "action": "halt",
                        "result": {"status": "rejected"},
                    }
                ]
            },
        )
        step_spec = record.spec["steps"][0]
        action, result = _evaluate_branches(step_spec, {"score": 0.9}, record)
        assert action == "halt"
        assert result == {"status": "rejected"}

    def test_halt_branch_does_not_fire_when_condition_false(self):
        record = make_record(
            {"a": READY},
            branches={
                "a": [
                    {
                        "condition": {">=": [{"var": "score"}, 0.7]},
                        "action": "halt",
                        "result": {"status": "rejected"},
                    }
                ]
            },
        )
        step_spec = record.spec["steps"][0]
        action, result = _evaluate_branches(step_spec, {"score": 0.3}, record)
        assert action is None
        assert result is None

    def test_complete_branch_fires(self):
        record = make_record(
            {"a": READY},
            branches={
                "a": [
                    {
                        "condition": {">=": [{"var": "n"}, 5]},
                        "action": "complete",
                    }
                ]
            },
        )
        step_spec = record.spec["steps"][0]
        action, result = _evaluate_branches(step_spec, {"n": 5}, record)
        assert action == "complete"

    def test_set_branch_fires(self):
        record = make_record(
            {"a": READY},
            branches={
                "a": [
                    {
                        "condition": {"<": [{"var": "n"}, 5]},
                        "action": {"set": "steps.a.status", "value": "ready"},
                    }
                ]
            },
        )
        step_spec = record.spec["steps"][0]
        action, _ = _evaluate_branches(step_spec, {"n": 3}, record)
        assert isinstance(action, dict)
        assert "set" in action

    def test_first_matching_branch_wins(self):
        record = make_record(
            {"a": READY},
            branches={
                "a": [
                    {
                        "condition": {">=": [{"var": "n"}, 0]},
                        "action": "complete",
                    },
                    {
                        "condition": {">=": [{"var": "n"}, 0]},
                        "action": "halt",
                        "result": {},
                    },
                ]
            },
        )
        step_spec = record.spec["steps"][0]
        action, _ = _evaluate_branches(step_spec, {"n": 1}, record)
        assert action == "complete"

    def test_branch_evaluates_against_step_output(self):
        # Branch condition vars resolve against step output, not state.
        record = make_record(
            {"a": READY},
            state={"n": 10},
            branches={
                "a": [
                    {
                        "condition": {">=": [{"var": "n"}, 5]},
                        "action": "halt",
                        "result": {},
                    }
                ]
            },
        )
        step_spec = record.spec["steps"][0]
        # step output overrides state in branch context
        action, _ = _evaluate_branches(step_spec, {"n": 1}, record)
        assert action is None


# ---------------------------------------------------------------------------
# _assert_inputs_satisfied
# ---------------------------------------------------------------------------


class TestAssertInputsSatisfied:

    def test_passes_for_complete_dep(self):
        record = make_record(
            {"a": COMPLETE, "b": READY},
            step_inputs={"b": {"v": {"var": "steps.a.output.value"}}},
            deps={"b": ["a"]},
        )
        step_spec = next(s for s in record.spec["steps"] if s["id"] == "b")
        _assert_inputs_satisfied(step_spec, record)  # must not raise

    def test_raises_for_ready_dep(self):
        record = make_record(
            {"a": READY, "b": READY},
            step_inputs={"b": {"v": {"var": "steps.a.output.value"}}},
        )
        step_spec = next(s for s in record.spec["steps"] if s["id"] == "b")
        with pytest.raises(ValueError, match="step 'a'"):
            _assert_inputs_satisfied(step_spec, record)

    def test_raises_for_in_progress_dep(self):
        record = make_record(
            {"a": IN_PROGRESS, "b": READY},
            step_inputs={"b": {"v": {"var": "steps.a.output.value"}}},
        )
        step_spec = next(s for s in record.spec["steps"] if s["id"] == "b")
        with pytest.raises(ValueError, match="step 'a'"):
            _assert_inputs_satisfied(step_spec, record)

    def test_raises_for_unknown_step_reference(self):
        record = make_record(
            {"a": READY},
            step_inputs={"a": {"v": {"var": "steps.ghost.output.value"}}},
        )
        step_spec = record.spec["steps"][0]
        with pytest.raises(ValueError, match="ghost"):
            _assert_inputs_satisfied(step_spec, record)

    def test_passes_for_state_var(self):
        record = make_record(
            {"a": READY},
            state={"x": 1},
            step_inputs={"a": {"v": {"var": "state.x"}}},
        )
        step_spec = record.spec["steps"][0]
        _assert_inputs_satisfied(step_spec, record)  # must not raise

    def test_passes_for_input_var(self):
        record = make_record(
            {"a": READY},
            inputs={"name": "alice"},
            step_inputs={"a": {"v": {"var": "input.name"}}},
        )
        step_spec = record.spec["steps"][0]
        _assert_inputs_satisfied(step_spec, record)  # must not raise

    def test_passes_for_literal(self):
        record = make_record(
            {"a": READY},
            step_inputs={"a": {"v": 42}},
        )
        step_spec = record.spec["steps"][0]
        _assert_inputs_satisfied(step_spec, record)  # must not raise

    def test_passes_for_no_inputs(self):
        record = make_record({"a": READY})
        step_spec = record.spec["steps"][0]
        _assert_inputs_satisfied(step_spec, record)  # must not raise


class TestAssertInputsSatisified:

    def test_assert_inputs_satisfied_raises_with_step_name(self):
        record = make_record(
            {"a": StepStatus.READY, "b": StepStatus.READY},
            step_inputs={"b": {"x": {"var": "steps.a.output.v"}}},
        )
        step_spec = _get_step_spec(record, "b")
        with pytest.raises(ValueError, match="a"):
            _assert_inputs_satisfied(step_spec, record)

    def test_assert_inputs_satisfied_passes_when_dep_complete(self):
        record = make_record(
            {"a": StepStatus.COMPLETE, "b": StepStatus.READY},
            step_inputs={"b": {"x": {"var": "steps.a.output.v"}}},
        )
        step_spec = _get_step_spec(record, "b")
        _assert_inputs_satisfied(step_spec, record)

    def test_assert_inputs_satisfied_passes_for_state_ref(self):
        record = make_record(
            {"a": StepStatus.READY},
            step_inputs={"a": {"x": {"var": "state.total"}}},
        )
        step_spec = _get_step_spec(record, "a")
        _assert_inputs_satisfied(step_spec, record)

    def test_assert_inputs_satisfied_passes_for_input_ref(self):
        record = make_record(
            {"a": StepStatus.READY},
            step_inputs={"a": {"x": {"var": "input.name"}}},
        )
        step_spec = _get_step_spec(record, "a")
        _assert_inputs_satisfied(step_spec, record)

class TestParseSetTargets:

    def test_parse_set_targets_dotpath(self):
        record = make_record({"a": StepStatus.READY, "b": StepStatus.READY})
        action = {"set": "steps.a.status", "value": "ready"}
        assert _parse_set_targets(action, record) == ["a"]

    def test_parse_set_targets_bare_string(self):
        record = make_record({"a": StepStatus.READY})
        action = {"set": "a", "value": "ready"}
        assert _parse_set_targets(action, record) == ["a"]

    def test_parse_set_targets_list(self):
        record = make_record({"a": StepStatus.READY, "b": StepStatus.READY})
        action = {"set": ["steps.a.status", "steps.b.status"], "value": "ready"}
        assert _parse_set_targets(action, record) == ["a", "b"]

    def test_parse_set_targets_mixed_list(self):
        record = make_record({"a": StepStatus.READY, "b": StepStatus.READY})
        action = {"set": ["steps.a.status", "b"], "value": "ready"}
        assert _parse_set_targets(action, record) == ["a", "b"]
