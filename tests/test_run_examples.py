"""
test_examples.py -- integration tests that run each example as a subprocess.

Each example is executed with its default arguments and expected to exit
cleanly with return code 0. These tests verify end-to-end composition:
executor, backend, workflow graph, branch evaluation, and output resolution
all working together.

This mirrors the manual check:
    for i in examples/ops/*.py; do python3 $i; done

Failures here indicate a regression in the composed system, not in any
individual component. Component failures are caught by test_functional_core.py,
test_backend.py, test_workflow.py, and test_sqlite.py.
"""

import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES_ROOT = Path(__file__).parent.parent / "examples"

OPS_EXAMPLES = [
    "ops/single_step.py",
    "ops/accumulation.py",
    "ops/fan_out.py",
    "ops/fan_in.py",
    "ops/branch_halt.py",
    "ops/retry.py",
    "ops/stack_machine.py",
]

USES_EXAMPLES = [
    "uses/fibonacci.py",
    "uses/fibonacci_async.py",
    "uses/document_parser.py",
    "uses/multisource.py",
    "uses/validation.py",
    "uses/conways_game_of_life.py",
]


def run_example(relative_path):
    """Run an example script and return the CompletedProcess."""
    path = EXAMPLES_ROOT / relative_path
    return subprocess.run(
        [sys.executable, str(path)],
        capture_output=True,
        text=True,
        timeout=30,
    )


def _example_id(relative_path):
    return Path(relative_path).stem


# ---------------------------------------------------------------------------
# Ops examples -- abstract patterns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("example", OPS_EXAMPLES, ids=_example_id)
def test_ops_example(example):
    result = run_example(example)
    assert result.returncode == 0, (
        f"{example} exited with code {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


# ---------------------------------------------------------------------------
# Uses examples -- worked scenarios
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("example", USES_EXAMPLES, ids=_example_id)
def test_uses_example(example):
    result = run_example(example)
    assert result.returncode == 0, (
        f"{example} exited with code {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
