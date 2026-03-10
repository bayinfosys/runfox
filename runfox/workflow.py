"""
workflow.py -- Workflow

The Workflow object is a stateless handle over backend state. It holds
only a workflow_execution_id and a backend reference. Every method loads
current state from the backend at call time.

Module-level functions
----------------------
The graph operations -- finding dispatchable steps, resolving inputs,
evaluating branch conditions, etc. -- are pure functions over WorkflowRecord.
They take a record (and in some cases a step spec or action dict) and return
a value; they never mutate the record or call the backend. They are defined
at module level so they can be called without a Workflow instance.

Execution model
---------------
advance() claims all currently dispatchable steps (marking them in_progress)
and returns a Dispatch carrying the full job specs. It does not execute
anything.

run() drives the loop for synchronous and polled backends:

    while True:
        result = advance()              -- claim ready steps, return Dispatch
        if terminal: return result
        backend.execute(id, dispatch)   -- run or enqueue the claimed jobs
        backend.process_results(id)     -- wait for results, call on_step_result()

For InMemoryBackend: execute() runs jobs synchronously and calls
on_step_result() for each; process_results() is a no-op; the next advance()
returns Complete or Halt.

For SqliteBackend: execute() writes task rows; process_results() blocks
until at least one COMPLETE row appears and calls on_step_result(); loop
continues until terminal.

For distributed backends (DynamoDB, EventBridge, SNS, etc.): run() is
not used. The caller gets a Dispatch from advance(), iterates the jobs,
places each on its queue, and calls on_step_result() then advance() when
each result arrives.
"""

import copy
import datetime
import time  # add alongside existing imports
from typing import Any

import yaml
from json_logic_path import is_logic, jsonLogic

from .backend.base import WorkflowRecord
from .results import (Complete, Dispatch, DispatchJob, Halt, Pending,
                      StateChangeEvent)
from .status import StepStatus, WorkflowStatus

# ---------------------------------------------------------------------------
# Pure functions over WorkflowRecord
# ---------------------------------------------------------------------------


def _parse_iso(ts: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(ts)


def _get_step_spec(record: WorkflowRecord, step_id: str) -> dict:
    for step in record.spec["steps"]:
        if step["id"] == step_id:
            return step
    raise KeyError(f"Step {step_id!r} not found in spec")


def _find_dispatchable_steps(record: WorkflowRecord) -> list[dict]:
    """
    Return step specs that are ready to be dispatched.

    A step is dispatchable when its status is READY or RETRY and all of
    its declared dependencies have status COMPLETE.

    READY means the step has not yet run. RETRY means it has previously
    failed and is waiting to be re-dispatched. Both statuses are dispatched
    immediately when dependencies are satisfied; no backoff is implemented.
    """
    dispatchable = []
    for step_spec in record.spec["steps"]:
        sid = step_spec["id"]
        if record.steps[sid].status not in (StepStatus.READY, StepStatus.RETRY):
            continue
        deps = step_spec.get("depends_on", [])
        if all(record.steps[d].status == StepStatus.COMPLETE for d in deps):
            dispatchable.append(step_spec)
    return dispatchable


def _all_terminal(record: WorkflowRecord) -> bool:
    """True when every step has reached a terminal status."""
    return all(
        s.status in (StepStatus.COMPLETE, StepStatus.HALTED)
        for s in record.steps.values()
    )


def _make_context(record: WorkflowRecord) -> dict:
    """
    Build the JSON Logic evaluation context.

    ``steps.<id>.output.<field>``  -- completed step outputs
    ``input.<field>``              -- immutable workflow-level inputs
    ``state.<field>``              -- mutable shared accumulator
    """
    return {
        "steps": {
            sid: {"output": s.output}
            for sid, s in record.steps.items()
            if s.output is not None
        },
        "input": record.inputs,
        "state": record.state,
    }


def _resolve_value(value: Any, context: dict) -> Any:
    if isinstance(value, dict) and is_logic(value):
        return jsonLogic(value, copy.deepcopy(context))
    return value


def _resolve_inputs(step_spec: dict, record: WorkflowRecord) -> dict:
    spec_inputs = step_spec.get("input") or {}
    context = _make_context(record)
    return {key: _resolve_value(value, context) for key, value in spec_inputs.items()}


def _resolve_outputs(record: WorkflowRecord) -> dict:
    spec_outputs = record.spec.get("outputs") or {}
    context = _make_context(record)
    return {key: _resolve_value(value, context) for key, value in spec_outputs.items()}


def _assert_inputs_satisfied(step_spec: dict, record: WorkflowRecord) -> None:
    """
    Assert that every steps.X var reference in the step's input spec
    corresponds to a step with status COMPLETE.

    Raises ValueError naming the offending step and input key if any
    referenced step is not complete. Unreachable if dependency derivation
    is correct, but catches spec errors at dispatch time rather than
    silently passing None to the executor.
    """
    spec_inputs = step_spec.get("input") or {}
    for key, value in spec_inputs.items():
        if not isinstance(value, dict):
            continue
        var = value.get("var", "")
        if not isinstance(var, str) or not var.startswith("steps."):
            continue
        parts = var.split(".")
        if len(parts) < 2:
            continue
        ref_id = parts[1]
        if ref_id not in record.steps:
            raise ValueError(
                f"Step {step_spec['id']!r} input {key!r} references "
                f"unknown step {ref_id!r}"
            )
        if record.steps[ref_id].status != StepStatus.COMPLETE:
            raise ValueError(
                f"Step {step_spec['id']!r} input {key!r} references "
                f"step {ref_id!r} which has status "
                f"{record.steps[ref_id].status!r}, expected complete"
            )


def _parse_set_targets(action: dict, record: WorkflowRecord) -> list[str]:
    """
    Extract a list of step IDs from a set branch action dict.

    The ``set`` key accepts the following forms:

      "steps.X.status"              -- dot-path; middle segment is step ID
      "X"                           -- bare step ID string
      ["steps.X.status", "Y", ...]  -- list of either of the above
      JSON Logic expression          -- must resolve to a string or list
                                       in one of the forms above
    """
    raw = action["set"]
    context = _make_context(record)

    if isinstance(raw, dict) and is_logic(raw):
        raw = jsonLogic(raw, copy.deepcopy(context))

    if not isinstance(raw, list):
        raw = [raw]

    step_ids = []
    for item in raw:
        if isinstance(item, str) and "." in item:
            step_ids.append(item.split(".")[1])
        else:
            step_ids.append(item)

    return step_ids


def _evaluate_branches(
    step_spec: dict, step_output: dict, record: WorkflowRecord
) -> tuple[Any, Any]:
    """
    Evaluate branch conditions after a step completes.

    Returns ``(action, result)`` for the first branch whose condition
    is truthy, or ``(None, None)`` if no branch fires.

    ``action`` may be ``"halt"``, ``"complete"``, or a dict with a
    ``"set"`` key.
    """
    if not step_spec.get("branch"):
        return None, None

    context = {
        **_make_context(record),
        **(step_output or {}),
    }

    for branch in step_spec["branch"]:
        if jsonLogic(branch["condition"], copy.deepcopy(context)):
            return branch["action"], branch.get("result", {})

    return None, None


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------


class Workflow:
    """
    Stateless handle over a workflow stored in a Backend.

    The only local state is the workflow_execution_id and the backend
    reference. All workflow data is loaded from the backend on each
    operation.
    """

    def __init__(self, workflow_execution_id: str, backend):
        self.id = workflow_execution_id  # TODO: I would prefer not to use `id` as a field; should be `workflow_execution_id`
        self.backend = backend

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_dict(cls, spec: dict, backend, inputs: dict = None) -> "Workflow":
        """Create a new workflow from a spec dict."""
        wf_exec_id = backend.create(spec, inputs or {})
        return cls(wf_exec_id, backend)

    @classmethod
    def from_yaml(cls, text: str, backend, inputs: dict = None) -> "Workflow":
        """Create a new workflow from a YAML string."""
        spec = yaml.safe_load(text)
        return cls.from_dict(spec, backend, inputs)

    @classmethod
    def resume(cls, workflow_execution_id: str, backend) -> "Workflow":
        """
        Resume an existing workflow by execution ID.
        Raises KeyError if the workflow does not exist in the backend.
        """
        backend.load(workflow_execution_id)
        return cls(workflow_execution_id, backend)

    # ------------------------------------------------------------------
    # State machine operations -- public API
    # ------------------------------------------------------------------

    def advance(self) -> Any:
        """
        Evaluate the step graph and claim all dispatchable steps.

        Returns one of: Dispatch | Pending | Complete | Halt.
        Does not recurse or block.
        """
        record = self.backend.load(self.id)

        if record.status == WorkflowStatus.COMPLETE:
            return Complete(record.outcome)
        if record.status == WorkflowStatus.HALTED:
            return Halt(record.outcome)

        dispatchable = _find_dispatchable_steps(record)

        if not dispatchable:
            if _all_terminal(record):
                outcome = _resolve_outputs(record)
                self.backend.write_workflow_outcome(self.id, outcome)
                return Complete(outcome)
            return Pending()

        # Mark ALL dispatchable steps in_progress before returning.
        # This is the claim: no concurrent caller can select the same
        # steps regardless of when they call advance().
        jobs = []
        for step_spec in dispatchable:
            _assert_inputs_satisfied(step_spec, record)  # raises if deps not satisfied
            resolved = _resolve_inputs(step_spec, record)
            self.backend.mark_in_progress(self.id, step_spec["id"])
            fn = step_spec.get("fn") or step_spec.get("type")
            step = record.steps[step_spec["id"]]
            jobs.append(
                DispatchJob(
                    workflow_execution_id=self.id,
                    step_id=step_spec["id"],
                    fn=fn,
                    inputs=resolved,
                    run_id=step.run_id,
                )
            )

        return Dispatch(jobs=jobs)

    def on_step_result(self, step_id: str, output: dict) -> Any:
        """
        Process a completed step result.

        1. Write the step output to the backend.
        2. Merge the step output into the shared state accumulator.
        3. Evaluate branch conditions.
        4. Dispatch on the fired action:

           halt     -- mark step halted, write workflow outcome, return Halt.
           complete -- mark step complete, return None.
           {set}    -- reset each named target step and the current step
                       to READY; return None.
           (none)   -- if current run_id + 1 < max_attempts, reset for
                       retry and return Pending(); otherwise mark complete
                       and return None.

        Output is written and state is merged unconditionally before branch
        evaluation, so the accumulator reflects this step's output
        regardless of which branch fires.

        Does not call advance(). The caller is responsible for calling
        advance() after on_step_result() to submit the next batch of
        ready steps.

        In the event-driven pattern:
            wf.on_step_result(step_id, output)
            wf.advance()
        """
        record = self.backend.load(self.id)
        step_spec = _get_step_spec(record, step_id)
        action, result = _evaluate_branches(step_spec, output, record)

        self.backend.write_step_output(self.id, step_id, output)
        event = StateChangeEvent(
            step_id=step_id,
            fn=step_spec.get("fn") or step_spec.get("type"),
        )
        self.backend.merge_workflow_state(self.id, output, event)

        if action == "halt":
            self.backend.mark_halted(self.id, step_id)
            self.backend.write_workflow_outcome(self.id, result)
            return Halt(result)

        if action == "complete":
            self.backend.mark_complete(self.id, step_id)
            return None

        if isinstance(action, dict) and "set" in action:
            targets = _parse_set_targets(action, record)
            # Include the current step and deduplicate. reset_step increments
            # run_id, so calling it more than once per step per branch
            # evaluation would over-increment.
            all_targets = dict.fromkeys(targets + [step_id])
            for target in all_targets:
                self.backend.reset_step(self.id, target)
            return None

        # No branch fired. Use max_attempts as the retry budget.
        max_attempts = step_spec.get("max_attempts", 1)
        current_run_id = record.steps[step_id].run_id

        if current_run_id + 1 < max_attempts:
            self.backend.reset_for_retry(self.id, step_id)
            return Pending()

        self.backend.mark_complete(self.id, step_id)
        return None

    def run(self, worker=None) -> Any:
        """
        Drive the workflow to completion.

        Calls advance() to claim ready steps, dispatches jobs via
        backend.dispatch(), runs the worker if provided, then polls
        backend.gather() for results and feeds each into on_step_result().

        worker may be any object with a run(workflow_execution_id) method.
        If not provided, backend._worker is used if present. For SqliteRunner
        and other remote runners no worker is passed; an external process owns
        execution and Workflow.run() polls gather() until results appear.

        Not used for distributed backends in event-driven deployments. Call
        advance() directly and dispatch jobs from the returned Dispatch object.
        """
        _worker = worker or getattr(self.backend, "_worker", None)
        while True:
            result = self.advance()
            if isinstance(result, (Complete, Halt)):
                return result
            if isinstance(result, Dispatch):
                self.backend.dispatch(self.id, result.jobs)
            if _worker:
                _worker.run(self.id)
            pairs = self.backend.gather(self.id)
            if not pairs:
                time.sleep(self.backend.poll_interval)
                continue
            for step_id, output in pairs:
                self.on_step_result(step_id, output)

    # ------------------------------------------------------------------
    # Status inspection
    # ------------------------------------------------------------------

    @property
    def status(self) -> WorkflowStatus:
        """Current workflow status."""
        return self.backend.load(self.id).status

    @property
    def outcome(self) -> Any:
        """Resolved workflow outcome, or None if not yet terminal."""
        return self.backend.load(self.id).outcome

    @property
    def step_statuses(self) -> dict[str, StepStatus]:
        """Dict of {step_id: StepStatus} for all steps."""
        return {sid: s.status for sid, s in self.backend.load(self.id).steps.items()}

    # ------------------------------------------------------------------
    # Timing and progress
    # ------------------------------------------------------------------

    @property
    def progress(self) -> dict[str, int]:
        """Count of steps in each status, plus a 'total' key."""
        record = self.backend.load(self.id)
        counts: dict[str, int] = {
            StepStatus.READY: 0,
            StepStatus.IN_PROGRESS: 0,
            StepStatus.COMPLETE: 0,
            StepStatus.HALTED: 0,
            StepStatus.RETRY: 0,
        }
        for s in record.steps.values():
            counts[s.status] = counts.get(s.status, 0) + 1
        counts["total"] = len(record.steps)
        return counts

    @property
    def step_durations(self) -> dict[str, float]:
        """
        Wall-clock duration in seconds for each step that has a start_time.

        Steps not yet started are omitted. Steps started but not finished
        are measured against the current time.
        """
        record = self.backend.load(self.id)
        now = datetime.datetime.now(datetime.timezone.utc)
        durations: dict[str, float] = {}
        for sid, s in record.steps.items():
            if s.start_time is None:
                continue
            start = _parse_iso(s.start_time)
            end = _parse_iso(s.end_time) if s.end_time else now
            durations[sid] = (end - start).total_seconds()
        return durations

    @property
    def elapsed(self) -> float:
        """
        Wall-clock seconds from the earliest step start_time to the
        latest step end_time, or to now if the workflow is still running.

        Returns 0.0 if no step has started.
        """
        record = self.backend.load(self.id)
        now = datetime.datetime.now(datetime.timezone.utc)

        start_times = [
            _parse_iso(s.start_time) for s in record.steps.values() if s.start_time
        ]
        if not start_times:
            return 0.0

        end_times = [
            _parse_iso(s.end_time) for s in record.steps.values() if s.end_time
        ]

        earliest = min(start_times)
        latest = max(end_times) if len(end_times) == len(start_times) else now
        return (latest - earliest).total_seconds()

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"Workflow(id={self.id!r}, status={self.status!r})"
