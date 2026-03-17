import dataclasses
from typing import Any

from runfox.status import StepStatus, WorkflowStatus


@dataclasses.dataclass
class StepRecord:
    op: str
    status: StepStatus = StepStatus.READY
    output: dict | None = None
    start_time: str | None = None
    end_time: str | None = None
    host: str | None = None
    run_id: int = 0

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "StepRecord":
        return cls(
            op=d["op"],
            status=StepStatus(d["status"]),
            output=d.get("output"),
            start_time=d.get("start_time"),
            end_time=d.get("end_time"),
            host=d.get("host"),
            run_id=d.get("run_id", 0),
        )


@dataclasses.dataclass
class WorkflowRecord:
    workflow_id: str
    execution_id: str
    spec: dict
    inputs: dict
    state: dict
    steps: dict
    status: WorkflowStatus
    outcome: Any = None

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "WorkflowRecord":
        return cls(
            workflow_id=d["workflow_id"],
            execution_id=d["execution_id"],
            spec=d["spec"],
            inputs=d["inputs"],
            state=d["state"],
            steps={op: StepRecord.from_dict(s) for op, s in d["steps"].items()},
            status=WorkflowStatus(d["status"]),
            outcome=d.get("outcome"),
        )
