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
step_key(wf_exec_id, op)
step_run_key(wf_exec_id, op, run_id)
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

from .models import StepRecord, WorkflowRecord
from .inprocess_runner import InProcessRunner
from .inprocess_worker import InProcessWorker
from .inmemory_store import InMemoryStore

class Backend:

    def __init__(
        self,
        executor=None,
        store=None,
        runner=None,
        poll_interval: float = 0.1,
        on_state_change=None,
    ):
        """
        on_state_change: optional callback fired after every state merge.
            Signature: (workflow_execution_id, previous_state, new_state) -> None.
            Must be pure: no side effects, no exceptions, no backend calls.
            The callback fires inside a write cycle; any mutation of backend
            state from within it will produce inconsistent records.
        """
        if store is None:
            store = InMemoryStore()
        if runner is None:
            runner = InProcessRunner()
        self._store = store
        self._runner = runner
        self._worker = InProcessWorker(runner, executor) if executor else None
        self.poll_interval = poll_interval
        self._on_state_change = on_state_change

    # ------------------------------------------------------------------
    # Private ID generation
    # ------------------------------------------------------------------

    def _make_workflow_id(self, spec: dict) -> str:
        canonical = json.dumps(spec, sort_keys=True, separators=(",", ":"))
        return hashlib.md5(canonical.encode()).hexdigest()

    def _make_execution_id(self) -> str:
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
        suffix = "".join(random.choices(string.hexdigits[:16], k=4)).lower()
        return f"{ts}-{suffix}"

    # ------------------------------------------------------------------
    # Public composite key accessors
    # ------------------------------------------------------------------

    def workflow_execution_id(self, record: WorkflowRecord) -> str:
        return f"{record.workflow_id}#{record.execution_id}"

    def step_key(self, wf_exec_id: str, op: str) -> str:
        return f"{wf_exec_id}#{op}"

    def step_run_key(self, wf_exec_id: str, op: str, run_id: int) -> str:
        return f"{wf_exec_id}#{op}#{run_id}"

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _now_iso(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    def _make_step_record(self, op: str) -> StepRecord:
        return StepRecord(op=op)

    # ------------------------------------------------------------------
    # Store pass-throughs (used by Workflow and tests)
    # ------------------------------------------------------------------

    def load(self, workflow_execution_id: str) -> WorkflowRecord:
        return self._store.load(workflow_execution_id)

    def write(self, record: WorkflowRecord) -> None:
        self._store.write(record)

    # ------------------------------------------------------------------
    # create
    # ------------------------------------------------------------------

    def create(self, spec: dict, inputs: dict = None) -> str:
        record = WorkflowRecord(
            workflow_id=self._make_workflow_id(spec),
            execution_id=self._make_execution_id(),
            spec=spec,
            inputs=inputs or {},
            state={},
            steps={
                step["op"]: self._make_step_record(step["op"]) for step in spec["steps"]
            },
            status=WorkflowStatus.PENDING,
        )
        self._store.write(record)
        return self.workflow_execution_id(record)

    # ------------------------------------------------------------------
    # Named step operations
    # ------------------------------------------------------------------

    def mark_in_progress(self, workflow_execution_id: str, op: str) -> None:
        record = self._store.load(workflow_execution_id)
        new_step = dataclasses.replace(
            record.steps[op],
            status=StepStatus.IN_PROGRESS,
            start_time=self._now_iso(),
            host=socket.gethostname(),
        )
        self._store.write(
            dataclasses.replace(
                record,
                status=WorkflowStatus.IN_PROGRESS,
                steps={**record.steps, op: new_step},
            )
        )

    def mark_complete(self, workflow_execution_id: str, op: str) -> None:
        record = self._store.load(workflow_execution_id)
        new_step = dataclasses.replace(
            record.steps[op],
            status=StepStatus.COMPLETE,
            end_time=self._now_iso(),
        )
        self._store.write(
            dataclasses.replace(
                record,
                steps={**record.steps, op: new_step},
            )
        )

    def mark_halted(self, workflow_execution_id: str, op: str) -> None:
        record = self._store.load(workflow_execution_id)
        new_step = dataclasses.replace(
            record.steps[op],
            status=StepStatus.HALTED,
            end_time=self._now_iso(),
        )
        self._store.write(
            dataclasses.replace(
                record,
                status=WorkflowStatus.HALTED,
                steps={**record.steps, op: new_step},
            )
        )

    def write_step_output(
        self, workflow_execution_id: str, op: str, output: dict
    ) -> None:
        record = self._store.load(workflow_execution_id)
        new_step = dataclasses.replace(record.steps[op], output=output)
        self._store.write(
            dataclasses.replace(
                record,
                steps={**record.steps, op: new_step},
            )
        )

    def merge_workflow_state(self, workflow_execution_id, output, event=None):
        """
        Update the internal workflow state with a new output.
        Latest-wins update of keys.

        on_state_change is called with
        (workflow_execution_id, prev_state, new_state, event) if set.
        event is a StateChangeEvent identifying the step that triggered
        the merge, or None if called outside a step result context.
        """
        if not output:
            return
        record = self._store.load(workflow_execution_id)
        new_state = {**record.state, **output}
        if self._on_state_change:
            # Callback must be pure. Side effects here are undefined behaviour --
            # the store write has completed but the workflow has not advanced.
            self._on_state_change(workflow_execution_id, record.state, new_state, event)
        self._store.write(dataclasses.replace(record, state=new_state))

    def write_workflow_outcome(self, workflow_execution_id: str, outcome: Any) -> None:
        """send the result of the workflow to the store for writing"""
        record = self._store.load(workflow_execution_id)
        status = (
            WorkflowStatus.HALTED
            if record.status == WorkflowStatus.HALTED
            else WorkflowStatus.COMPLETE
        )
        self._store.write(dataclasses.replace(record, outcome=outcome, status=status))

    def reset_step(self, workflow_execution_id: str, op: str) -> None:
        record = self._store.load(workflow_execution_id)
        new_step = dataclasses.replace(
            record.steps[op],
            status=StepStatus.READY,
            output=None,
            start_time=None,
            end_time=None,
            run_id=record.steps[op].run_id + 1,
        )
        self._store.write(
            dataclasses.replace(
                record,
                steps={**record.steps, op: new_step},
            )
        )

    def reset_for_retry(self, workflow_execution_id: str, op: str) -> None:
        record = self._store.load(workflow_execution_id)
        new_step = dataclasses.replace(
            record.steps[op],
            status=StepStatus.RETRY,
            run_id=record.steps[op].run_id + 1,
            start_time=None,
            end_time=None,
        )
        self._store.write(
            dataclasses.replace(
                record,
                steps={**record.steps, op: new_step},
            )
        )

    # ------------------------------------------------------------------
    # Runner pass-throughs
    # ------------------------------------------------------------------

    def dispatch(self, workflow_execution_id: str, jobs: list) -> None:
        self._runner.dispatch(workflow_execution_id, jobs)

    def gather(self, workflow_execution_id: str) -> list:
        return self._runner.gather(workflow_execution_id)

    def pending_tasks(self) -> list:
        return self._runner.list_pending_jobs()

    def take_tasks(self) -> list:
        return self._runner.take_pending_jobs()

    def submit_result(self, workflow_execution_id, op, output):
        self._runner.submit_work_result(workflow_execution_id, op, output)
