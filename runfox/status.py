"""
status.py -- status enumerations

Two enumerations:

  StepStatus    -- the lifecycle of a single step execution
  WorkflowStatus -- the lifecycle of the workflow record

Values are stored as strings in the backend so that records are readable
without importing this module. Comparisons throughout the codebase use
the enum members, not raw strings.

StepStatus values
-----------------
  READY       -- initial state; deps satisfied but not yet dispatched.
                 Replaces the former "pending" string. "pending" is kept
                 as a stored value only in the workflow-level status (see
                 WorkflowStatus) where it means "no steps have started".

  IN_PROGRESS -- claimed by advance(), dispatched to executor.

  COMPLETE    -- executor returned a result; step output written.

  HALTED      -- a halt branch fired; this step caused workflow termination.

  RETRY       -- executor returned a result but max_attempts not yet
                 reached; step will be re-dispatched after backoff.
                 Distinct from READY so that step_statuses consumers can
                 distinguish a fresh step from one that is recovering.

WorkflowStatus values
---------------------
  PENDING     -- workflow record created; no steps have started.

  IN_PROGRESS -- at least one step is in_progress.

  COMPLETE    -- all steps reached terminal status; outcome resolved.

  HALTED      -- a halt branch fired; outcome is the branch result payload.
"""

from enum import StrEnum


class StepStatus(StrEnum):
    READY = "ready"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    HALTED = "halted"
    RETRY = "retry"


class WorkflowStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    HALTED = "halted"
