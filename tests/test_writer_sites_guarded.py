"""Every background writer routes its project write-back through publish_if_current.

Seven functions save the project folder and then republish the project. Any one of
them that publishes directly can resurrect a project the user just deleted, and
its auto-save re-creates the deleted folder. The tiles also contain a dozen
*legitimate* render-thread ``project.set(...)`` calls, so this checks the writer
functions specifically rather than grepping whole files.
"""

import inspect
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TASK_TILES = [
    ("gui/tile/variables_tile.py", "download_task"),
    ("gui/tile/process_tile.py", "process_task"),
    ("gui/tile/postprocess_tile.py", "change_task"),
]


def _task_body(src: str, name: str) -> str:
    """Source of a nested `async def <name>()` — from its line to the next dedent."""
    lines = src.splitlines()
    start = next(i for i, line in enumerate(lines) if f"async def {name}(" in line)
    indent = len(lines[start]) - len(lines[start].lstrip())
    end = start + 1
    while end < len(lines):
        line = lines[end]
        stripped = line.strip()
        if stripped and (len(line) - len(line.lstrip())) <= indent:
            break
        end += 1
    return "\n".join(lines[start:end])


def test_thread_workers_do_not_publish_the_project_directly():
    from gui.tile.evaluation_tile import _run_evaluation
    from gui.tile.inference_tile import _run_import
    from gui.tile.sampling_tile import _run_sampling
    from gui.tile.train_tile import _run_training

    for fn in (_run_sampling, _run_training, _run_import, _run_evaluation):
        src = inspect.getsource(fn)
        assert "publish_if_current(" in src, f"{fn.__name__} publishes without the guard"
        assert "project_reactive.set(" not in src, f"{fn.__name__} still publishes directly"
        assert "project.set(" not in src, f"{fn.__name__} still publishes directly"
        assert "writing(" in src, f"{fn.__name__} does not mark the project as being written"


def test_inference_marks_the_project_as_being_written():
    """_run_inference cannot join the loop above — it never republishes the project
    (run_inference saves it itself), so the publish_if_current assert would fail it.
    It still has to hold the writer mark: it is the longest-running writer here, and
    run_inference mkdir(exist_ok=True)s the output folder, so deleting the project
    mid-run without the mark would let it re-create the folder that was just erased.
    """
    from gui.tile.inference_tile import _run_inference

    src = inspect.getsource(_run_inference)
    assert "writing(" in src, "_run_inference does not mark the project as being written"


def test_use_task_tiles_do_not_publish_the_project_directly():
    for rel, task in TASK_TILES:
        body = _task_body((ROOT / rel).read_text(), task)
        assert "publish_if_current(" in body, f"{rel}:{task} publishes without the guard"
        assert "project.set(" not in body, f"{rel}:{task} still publishes directly"
        assert "writing(" in body, f"{rel}:{task} does not mark the project as being written"
