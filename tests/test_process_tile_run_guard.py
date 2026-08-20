"""Regression: the Run-processing button must not be re-entrant.

``processing`` is only set INSIDE process_task's coroutine, and ``disabled=`` is
a render-time prop that reaches the browser a round-trip after the task is
invoked — so neither actually stops a fast double-click. A second click
re-invokes process_task, and ``TaskAsyncio.__call__`` cancels the in-flight run:
cancellation unwinds the ``with writing(...)`` block (dropping the writer mark)
while the orphaned ``asyncio.to_thread`` executor thread keeps writing rasters
and calling ``project.save()`` — uncancellable. The button must therefore go
through the same synchronous, handler-side ``pending`` guard
``ProjectPanel.confirm_delete`` uses, not straight to the task.
"""

import inspect

from gui.tile.process_tile import ProcessTile


def test_run_processing_button_is_not_re_entrant():
    src = inspect.getsource(ProcessTile)
    lines = src.splitlines()

    def guard_follows(signature: str, within: int = 12) -> bool:
        """True when `signature`'s def line is followed, within a few lines (to
        allow for a leading docstring), by the synchronous pending guard."""
        for i, line in enumerate(lines):
            if signature in line:
                window = "\n".join(lines[i + 1 : i + 1 + within])
                return "if process_task.pending:" in window
        return False

    assert guard_follows("def run_processing():"), (
        "run_processing must bail out on process_task.pending before re-invoking it"
    )
    assert "on_click=run_processing" in src           # not on_click=lambda: process_task()
    assert "on_click=lambda: process_task()" not in src
