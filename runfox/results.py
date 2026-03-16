"""
Result types returned by advance() and on_step_result().

These are the only values the caller needs to inspect. The workflow
internals (step statuses, dependency graph, output accumulation) are
all managed inside the backend and are not exposed here.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class DispatchJob:
    """A single step ready for execution.

    Carries everything the caller needs to place the job on a queue or
    run it directly, without requiring a further backend load.

    run_id is included so the caller can construct a unique, stable
    message ID via backend.step_run_key(wf_exec_id, step_id, run_id).
    It increments on every dispatch of the step, whether due to error
    retry or set-branch loop reset.
    """

    workflow_execution_id: str
    op: str  # step execution token - unique for each step definition
    inputs: dict
    run_id: int


@dataclass
class Dispatch:
    """One or more steps are ready for execution.

    jobs contains one DispatchJob per ready step. The caller is
    responsible for dispatching each job to whatever execution mechanism
    it uses. runfox has already marked every job in_progress before
    returning this result.
    """

    jobs: list[DispatchJob]


@dataclass
class Pending:
    """In-progress steps exist but nothing new is ready to submit.

    This occurs when parallel steps are in flight and the workflow is
    waiting for them to complete before it can advance further.
    """

    pass


@dataclass
class Complete:
    """The workflow has finished successfully.

    outcome contains the resolved workflow outputs as defined in
    the outputs section of the spec.
    """

    outcome: Any


@dataclass
class Halt:
    """A branch condition triggered a halt before normal completion.

    result is the payload declared on the branch that fired.
    """

    result: Any


@dataclass
class StateChangeEvent:
    """
    Passed to the on_state_change callback to identify what triggered
    the state merge.

    op -- op token of the step which produced the output.
    """

    op: str
