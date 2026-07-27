"""Bridge the `spatial_risk` logger to pysepal notification TaskTrackers.

Each background job wraps its work in ``tracked_job(notifier, title)``, which
opens a pysepal ``notifier.track(...)`` task. While the job body runs, the
``TaskStepLogHandler`` attached to the ``spatial_risk`` logger forwards every
INFO+ record *emitted on the job's own thread* as a ``task.step(...)``
milestone — so granular library progress (``log_progress``'s
"Downloading layer 2/5", processing stage lines, …) surfaces in the official
notification pill without ``spatialrisk/`` knowing about the GUI.

Thread-scoping is what keeps concurrent jobs honest: two trainings running at
once each see only their own log lines. Completion / failure semantics come
from pysepal's tracker context manager (auto-complete on exit, mark FAILED +
error toast + re-raise on exception).

Threads without a Solara kernel context (``asyncio.to_thread`` pool threads)
cannot publish reactives to the browser — run those job bodies via
``solara_threads.to_thread_in_context`` so the tracker's bus updates land.
"""

import logging
import threading
from contextlib import contextmanager

_LOGGER_NAME = "spatial_risk"

# thread ident -> TaskTracker of the tracked job running on that thread.
_trackers = {}
_install_lock = threading.Lock()


class TaskStepLogHandler(logging.Handler):
    """Forward INFO+ records from tracked-job threads as task milestones.

    Records from threads with no registered tracker are ignored (file/console
    handlers still see them). The handler itself reads no reactive state, so a
    component that logs during its render cannot be auto-subscribed by it
    (``tracker.step`` does read the bus, but only job threads are registered,
    never a rendering thread).
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Step the emitting thread's tracker (if any) with the record message."""
        try:
            tracker = _trackers.get(threading.get_ident())
            if tracker is not None:
                tracker.step(record.getMessage())
        except Exception:  # a logging handler must never raise into caller code
            pass


def install_task_log_handler(level: int = logging.INFO) -> TaskStepLogHandler:
    """Attach a single ``TaskStepLogHandler`` to the ``spatial_risk`` logger.

    Idempotent: returns the existing handler if one is already attached (guards
    Solara hot-reload / repeated imports). Sets the handler level to ``INFO``
    but does NOT change the logger's own level (file/console keep DEBUG detail).
    """
    logger = logging.getLogger(_LOGGER_NAME)
    with _install_lock:
        for h in logger.handlers:
            if isinstance(h, TaskStepLogHandler):
                return h
        handler = TaskStepLogHandler()
        handler.setLevel(level)
        logger.addHandler(handler)
        return handler


@contextmanager
def tracked_job(notifier, title: str, total_steps=None):
    """Track a job as a pysepal notification task, echoing its log milestones.

    Must be entered on the thread that runs the job body — the log→milestone
    forwarding is keyed on that thread's ident. Nesting restores the previous
    tracker on exit. Yields the ``TaskTracker`` for explicit ``step()`` /
    ``set_progress()`` calls where log lines aren't enough. ``notifier=None``
    falls back to a no-op tracker so workers stay callable from plain tests.
    """
    if notifier is None:
        from pysepal.solara.notifications.notifier import NoopNotifier

        notifier = NoopNotifier()
    install_task_log_handler()
    with notifier.track(title, total_steps=total_steps) as task:
        tid = threading.get_ident()
        previous = _trackers.get(tid)
        _trackers[tid] = task
        try:
            yield task
        finally:
            if previous is not None:
                _trackers[tid] = previous
            else:
                _trackers.pop(tid, None)
