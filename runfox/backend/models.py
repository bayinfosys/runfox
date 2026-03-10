"""
base.py -- Backend

Composes a Store and a Runner. All workflow lifecycle operations are
implemented here in terms of self._store.load() and self._store.write().
dispatch() and gather() delegate to self._runner.

Construction
------------
  Backend(store=s, runner=r)  -- explicit store and runner

Composite key accessors
-----------------------
workflow_execution_id(record)
step_key(wf_exec_id, step_id)
step_run_key(wf_exec_id, step_id, run_id)
"""

import dataclasses
import datetime
import hashlib
import json
import random
import socket
import string
from typing import Any

from runfox.status import StepStatus, WorkflowStatus


@dataclasses.dataclass
class StepRecord:
    """
    Runtime state of a single step.

    id         -- step identifier
    status     -- current lifecycle status
    output     -- written by executor on completion; None until then
    start_time -- ISO-8601 UTC; set when claimed
    end_time   -- ISO-8601 UTC; set on terminal status
    host       -- hostname of the claiming process
    run_id     -- incremented on every dispatch (retry or set-branch reset)
    """

    id: str
    status: StepStatus = StepStatus.READY
    output: dict | None = None
    start_time: str | None = None
    end_time: str | None = None
    host: str | None = None
    run_id: int = 0


@dataclasses.dataclass
class WorkflowRecord:
    """
    Runtime state of a workflow execution as returned by store.load().

    workflow_id  -- MD5 of canonical spec JSON
    execution_id -- timestamp + short suffix; identifies one run
    spec         -- parsed workflow definition (immutable after create)
    inputs       -- workflow-level inputs (immutable after create)
    state        -- mutable shared accumulator
    steps        -- dict[step_id -> StepRecord]
    status       -- current workflow lifecycle status
    outcome      -- resolved outputs on completion, branch payload on halt
    """

    workflow_id: str
    execution_id: str
    spec: dict
    inputs: dict
    state: dict
    steps: dict
    status: WorkflowStatus
    outcome: Any = None
