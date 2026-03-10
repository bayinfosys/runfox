from runfox.backend.inprocess_runner import InProcessRunner
from runfox.backend.inprocess_worker import InProcessWorker
from runfox.results import DispatchJob

def _make_job(step_id="s1", fn="f", inputs=None, run_id=0):
    return DispatchJob(
        workflow_execution_id="wf#exec",
        step_id=step_id,
        fn=fn,
        inputs=inputs or {},
        run_id=run_id,
    )

class TestInProcessWorker:
    def _make_pair(self, executor):
        runner = InProcessRunner()
        worker = InProcessWorker(runner, executor)
        return runner, worker

    def test_run_executes_pending_jobs(self):
        runner, worker = self._make_pair(lambda fn, inputs: {"value": 1})
        runner.dispatch("wf#exec", [_make_job()])
        worker.run("wf#exec")
        results = runner.gather("wf#exec")
        assert results == [("s1", {"value": 1})]

    def test_run_passes_fn_and_inputs_to_executor(self):
        received = []
        def executor(fn, inputs):
            received.append((fn, inputs))
            return {}
        runner, worker = self._make_pair(executor)
        runner.dispatch("wf#exec", [_make_job(fn="myfn", inputs={"x": 42})])
        worker.run("wf#exec")
        assert received == [("myfn", {"x": 42})]

    def test_run_on_empty_queue_does_nothing(self):
        runner, worker = self._make_pair(lambda fn, inputs: {})
        worker.run("wf#exec")
        assert runner.gather("wf#exec") == []

    def test_executor_exception_produces_error_output(self):
        def bad(fn, inputs):
            raise ValueError("boom")
        runner, worker = self._make_pair(bad)
        runner.dispatch("wf#exec", [_make_job()])
        worker.run("wf#exec")
        results = runner.gather("wf#exec")
        assert results[0][1] == {"error": "boom", "ok": False}

    def test_multiple_jobs_all_executed(self):
        runner, worker = self._make_pair(lambda fn, inputs: {"done": fn})
        jobs = [_make_job(step_id=f"s{i}", fn=f"f{i}") for i in range(3)]
        runner.dispatch("wf#exec", jobs)
        worker.run("wf#exec")
        results = runner.gather("wf#exec")
        assert len(results) == 3
        assert {sid for sid, _ in results} == {"s0", "s1", "s2"}
