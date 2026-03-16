"""
models.py -- WorkflowRecord and StepRecord dataclasses.

Runtime state types used throughout the backend. These are plain data
containers with no behaviour.
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

    op         -- step identifier
    status     -- current lifecycle status
    output     -- written by executor on completion; None until then
    start_time -- ISO-8601 UTC; set when claimed
    end_time   -- ISO-8601 UTC; set on terminal status
    host       -- hostname of the claiming process
    run_id     -- incremented on every dispatch (retry or set-branch reset)
    """

    op: str
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
    steps        -- dict[op -> StepRecord]
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
