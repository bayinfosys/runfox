"""
conways_game_of_life.py -- Conway's Game of Life via runfox.

The board is a flat list of integers (0 dead, 1 alive) stored in the
shared state accumulator. Width, height, and target generation count are
also written to state by the init step so the evolve step is self-contained.

Two steps:

  init_board  -- creates the initial board from a named pattern and
                 writes board, width, height, generation, and target
                 into the shared state accumulator.

  evolve      -- reads the current board from state, applies Conway's
                 rules, writes back the next generation, then branches:
                   complete  if generation >= target
                   set self  otherwise (resets to pending, loops again)

The termination condition is data-driven. No iteration count is baked
into max_attempts or the executor. The spec can be serialised and the
same behaviour reproduced on any backend.

Patterns: glider, blinker, toad, beacon.

Usage
-----
    python3 conways_game_of_life.py
    python3 conways_game_of_life.py --pattern blinker --generations 6
    python3 conways_game_of_life.py --pattern toad --width 12 --height 12
"""

import argparse

import yaml

from runfox import Workflow
from runfox.backend import Backend

PATTERNS = {
    "glider": [(1, 0), (2, 1), (0, 2), (1, 2), (2, 2)],
    "blinker": [(1, 0), (1, 1), (1, 2)],
    "toad": [(1, 1), (2, 1), (3, 1), (0, 2), (1, 2), (2, 2)],
    "beacon": [(0, 0), (1, 0), (0, 1), (3, 2), (2, 3), (3, 3)],
}


def make_board(width, height, cells):
    board = [0] * (width * height)
    for x, y in cells:
        if 0 <= x < width and 0 <= y < height:
            board[y * width + x] = 1
    return board


def evolve_board(board, width, height):
    result = []
    for y in range(height):
        for x in range(width):
            cell = board[y * width + x]
            n = sum(
                board[((y + dy) % height) * width + (x + dx) % width]
                for dy in (-1, 0, 1)
                for dx in (-1, 0, 1)
                if not (dy == 0 and dx == 0)
            )
            result.append(1 if (cell and n in (2, 3)) or (not cell and n == 3) else 0)
    return result


def render_board(board, width, height):
    return "\n".join(
        "".join("#" if board[y * width + x] else "." for x in range(width))
        for y in range(height)
    )


def execute(op, inputs):
    if op == "init":
        w, h = inputs["width"], inputs["height"]
        target = inputs["target_generation"]
        cells = PATTERNS.get(inputs["pattern"], PATTERNS["glider"])
        board = make_board(w, h, cells)
        return {
            "board": board,
            "width": w,
            "height": h,
            "generation": 0,
            "target_generation": target,
        }

    if op == "evolve":
        w, h = inputs["width"], inputs["height"]
        gen = inputs["generation"]
        target = inputs["target_generation"]
        next_b = evolve_board(inputs["board"], w, h)
        return {
            "board": next_b,
            "width": w,
            "height": h,
            "generation": gen + 1,
            "target_generation": target,
            "live_cells": sum(next_b),
        }

    raise ValueError(f"Unknown op: {op!r}")


def on_state_change(workflow_execution_id, previous, current, event):
    if "board" not in current:
        return
    gen = current["generation"]
    live = current.get("live_cells", sum(current["board"]))
    print(f"\n-- generation {gen}  live: {live} --")
    print(render_board(current["board"], current["width"], current["height"]))


def make_spec(pattern, width, height, generations):
    return f"""
name: conways_game_of_life
steps:
  - op: init
    label: initialise the board
    input:
      pattern:          {pattern}
      width:            {width}
      height:           {height}
      target_generation: {generations}

  - op: evolve
    label: evolve the board one timestep
    depends_on: [init]
    input:
      board:             {{"var": "state.board"}}
      width:             {{"var": "state.width"}}
      height:            {{"var": "state.height"}}
      generation:        {{"var": "state.generation"}}
      target_generation: {{"var": "state.target_generation"}}
    branch:
      - condition: {{">=": [{{"var": "generation"}}, {{"var": "target_generation"}}]}}
        action: complete
      - condition: {{"<": [{{"var": "generation"}}, {{"var": "target_generation"}}]}}
        action: {{set: "steps.evolve.status", value: ready}}

outputs:
  generation: {{"var": "state.generation"}}
  live_cells: {{"var": "state.live_cells"}}
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", default="glider", choices=list(PATTERNS))
    parser.add_argument("--generations", default=20, type=int)
    parser.add_argument("--width", default=20, type=int)
    parser.add_argument("--height", default=20, type=int)
    args = parser.parse_args()

    spec = make_spec(args.pattern, args.width, args.height, args.generations)
    backend = Backend(executor=execute, on_state_change=on_state_change)
    wf = Workflow.from_dict(yaml.safe_load(spec), backend)

    print(
        f"Workflow {wf.id!r}  pattern={args.pattern}  "
        f"{args.width}x{args.height}  target={args.generations} generations"
    )

    result = wf.run()

    print("\n-- complete --")
    print(f"Final generation : {result.outcome['generation']}")
    print(f"Live cells       : {result.outcome['live_cells']}")
    print(f"Elapsed          : {wf.elapsed:.3f}s")


if __name__ == "__main__":
    main()
