"""
test_status.py -- guard tests for StepStatus and WorkflowStatus enum values.

These tests exist to prevent accidental renaming of enum members or
alteration of their string values. The stored values appear in backend
records (SQLite rows, DynamoDB items, in-memory dicts) and in any
serialised workflow state. Changing them without a migration breaks all
existing records.

If you need to rename a value, add a migration path first and update
these tests last.
"""

from runfox.status import StepStatus, WorkflowStatus


class TestStepStatusValues:
    def test_ready(self):
        assert StepStatus.READY == "ready"

    def test_in_progress(self):
        assert StepStatus.IN_PROGRESS == "in_progress"

    def test_complete(self):
        assert StepStatus.COMPLETE == "complete"

    def test_halted(self):
        assert StepStatus.HALTED == "halted"

    def test_retry(self):
        assert StepStatus.RETRY == "retry"

    def test_no_unexpected_members(self):
        expected = {"ready", "in_progress", "complete", "halted", "retry"}
        actual = {m.value for m in StepStatus}
        assert actual == expected


class TestWorkflowStatusValues:
    def test_pending(self):
        assert WorkflowStatus.PENDING == "pending"

    def test_in_progress(self):
        assert WorkflowStatus.IN_PROGRESS == "in_progress"

    def test_complete(self):
        assert WorkflowStatus.COMPLETE == "complete"

    def test_halted(self):
        assert WorkflowStatus.HALTED == "halted"

    def test_no_unexpected_members(self):
        expected = {"pending", "in_progress", "complete", "halted"}
        actual = {m.value for m in WorkflowStatus}
        assert actual == expected


class TestStrEnumBehaviour:
    def test_step_status_compares_equal_to_string(self):
        # str, Enum members compare equal to their value string.
        # This is the property that makes them drop-in replacements for
        # bare strings in backend record comparisons.
        assert StepStatus.READY == "ready"
        assert StepStatus.IN_PROGRESS == "in_progress"
        assert StepStatus.COMPLETE == "complete"
        assert StepStatus.HALTED == "halted"
        assert StepStatus.RETRY == "retry"

    def test_workflow_status_compares_equal_to_string(self):
        assert WorkflowStatus.PENDING == "pending"
        assert WorkflowStatus.IN_PROGRESS == "in_progress"
        assert WorkflowStatus.COMPLETE == "complete"
        assert WorkflowStatus.HALTED == "halted"

    def test_step_status_str_is_value(self):
        # str() on a str enum member returns the value, not "StepStatus.READY".
        # json.dumps(default=str) relies on this for serialisation.
        assert str(StepStatus.READY) == "ready"
        assert str(StepStatus.RETRY) == "retry"

    def test_workflow_status_str_is_value(self):
        assert str(WorkflowStatus.PENDING) == "pending", str(WorkflowStatus.PENDING)
        assert str(WorkflowStatus.COMPLETE) == "complete", str(WorkflowStatus.COMPLETE)
