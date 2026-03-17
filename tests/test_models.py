"""
test_models.py -- StepRecord and WorkflowRecord serialisation.

Tests that to_dict() / from_dict() round-trip correctly for both types,
that status enums are serialised as strings and deserialised back to
enum members, and that None fields survive the round trip.
"""

import pytest

from runfox.backend.models import StepRecord, WorkflowRecord
from runfox.status import StepStatus, WorkflowStatus


class TestStepRecordSerialisation:

    def test_round_trip_defaults(self):
        record = StepRecord(op="step1")
        assert StepRecord.from_dict(record.to_dict()) == record

    def test_round_trip_all_fields(self):
        record = StepRecord(
            op="step1",
            status=StepStatus.COMPLETE,
            output={"value": 42},
            start_time="2024-01-01T00:00:00+00:00",
            end_time="2024-01-01T00:01:00+00:00",
            host="worker-1",
            run_id=3,
        )
        assert StepRecord.from_dict(record.to_dict()) == record

    def test_status_serialised_as_string(self):
        record = StepRecord(op="step1", status=StepStatus.IN_PROGRESS)
        d = record.to_dict()
        assert d["status"] == "in_progress"
        assert isinstance(d["status"], str)

    def test_from_dict_restores_status_enum(self):
        record = StepRecord.from_dict({"op": "step1", "status": "complete"})
        assert record.status == StepStatus.COMPLETE
        assert isinstance(record.status, StepStatus)

    def test_none_fields_survive_round_trip(self):
        record = StepRecord(op="step1")
        d = record.to_dict()
        assert d["output"] is None
        assert d["start_time"] is None
        assert d["end_time"] is None
        assert d["host"] is None
        restored = StepRecord.from_dict(d)
        assert restored.output is None
        assert restored.host is None

    def test_run_id_default_zero(self):
        record = StepRecord.from_dict({"op": "step1", "status": "ready"})
        assert record.run_id == 0

    def test_run_id_preserved(self):
        record = StepRecord(op="step1", run_id=5)
        assert StepRecord.from_dict(record.to_dict()).run_id == 5


class TestWorkflowRecordSerialisation:

    def _make_record(self, status=WorkflowStatus.PENDING, outcome=None):
        return WorkflowRecord(
            workflow_id="abc123",
            execution_id="20240101T000000-abcd",
            spec={"name": "test", "steps": [{"op": "s1"}]},
            inputs={"x": 1},
            state={"n": 5},
            steps={
                "s1": StepRecord(
                    op="s1",
                    status=StepStatus.COMPLETE,
                    output={"value": 1},
                    run_id=1,
                )
            },
            status=status,
            outcome=outcome,
        )

    def test_round_trip(self):
        record = self._make_record()
        loaded = WorkflowRecord.from_dict(record.to_dict())
        assert loaded.workflow_id == record.workflow_id
        assert loaded.execution_id == record.execution_id
        assert loaded.inputs == record.inputs
        assert loaded.state == record.state
        assert loaded.status == record.status

    def test_steps_round_trip(self):
        record = self._make_record()
        loaded = WorkflowRecord.from_dict(record.to_dict())
        assert loaded.steps["s1"].status == StepStatus.COMPLETE
        assert loaded.steps["s1"].output == {"value": 1}
        assert loaded.steps["s1"].run_id == 1

    def test_status_serialised_as_string(self):
        record = self._make_record(status=WorkflowStatus.IN_PROGRESS)
        d = record.to_dict()
        assert d["status"] == "in_progress"
        assert isinstance(d["status"], str)

    def test_from_dict_restores_status_enum(self):
        record = self._make_record()
        loaded = WorkflowRecord.from_dict(record.to_dict())
        assert isinstance(loaded.status, WorkflowStatus)

    def test_outcome_none_survives_round_trip(self):
        record = self._make_record(outcome=None)
        loaded = WorkflowRecord.from_dict(record.to_dict())
        assert loaded.outcome is None

    def test_outcome_dict_survives_round_trip(self):
        record = self._make_record(
            status=WorkflowStatus.COMPLETE,
            outcome={"result": 42},
        )
        loaded = WorkflowRecord.from_dict(record.to_dict())
        assert loaded.outcome == {"result": 42}

    def test_spec_preserved(self):
        record = self._make_record()
        loaded = WorkflowRecord.from_dict(record.to_dict())
        assert loaded.spec == record.spec
