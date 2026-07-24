"""notify_bridge: spatial_risk log milestones -> pysepal notification tasks.

The bridge replaces the old on-map LogConsole: instead of rendering raw log
records in a custom widget, each background job opens a pysepal TaskTracker
(``tracked_job``) and the ``TaskStepLogHandler`` forwards INFO+ records emitted
on the job's own thread as task milestones, so granular library progress
(e.g. ``log_progress``'s "Downloading layer 2/5") surfaces in the official
notification pill.
"""

import logging
import threading

import pytest

from pysepal.solara.notifications.bus import NotificationBus
from pysepal.solara.notifications.notifier import Notifier
from pysepal.solara.notifications.state import TaskStatus, ToastType

from gui.scripts import notify_bridge


def _fresh():
    """Real spatial_risk logger with the bridge handler + a private bus."""
    notify_bridge.install_task_log_handler()
    logger = logging.getLogger("spatial_risk")
    logger.setLevel(logging.DEBUG)
    bus = NotificationBus()
    return logger, bus, Notifier(bus)


def _milestones(bus, title):
    task = next(t for t in bus.tasks.value if t.title == title)
    return [m.message for m in task.milestones]


def _status(bus, title):
    return next(t for t in bus.tasks.value if t.title == title).status


def test_job_log_lines_become_milestones_and_task_completes():
    logger, bus, notifier = _fresh()
    with notify_bridge.tracked_job(notifier, "My job"):
        logger.info("Step one")
        logger.info("Step %d", 2)
    assert _milestones(bus, "My job") == ["Step one", "Step 2"]
    assert _status(bus, "My job") == TaskStatus.COMPLETED


def test_debug_lines_are_not_forwarded():
    logger, bus, notifier = _fresh()
    with notify_bridge.tracked_job(notifier, "Quiet job"):
        logger.debug("hidden debug")
    assert _milestones(bus, "Quiet job") == []


def test_lines_from_unrelated_threads_are_not_forwarded():
    """Only the job's own thread feeds its tracker — a concurrent log line
    from elsewhere (another session, an unrelated worker) must not pollute
    the task's milestone list."""
    logger, bus, notifier = _fresh()
    with notify_bridge.tracked_job(notifier, "Mine"):
        other = threading.Thread(target=lambda: logger.info("not mine"))
        other.start()
        other.join()
        logger.info("mine")
    assert _milestones(bus, "Mine") == ["mine"]


def test_exception_marks_task_failed_with_error_toast_and_reraises():
    logger, bus, notifier = _fresh()
    with pytest.raises(ValueError, match="boom"):
        with notify_bridge.tracked_job(notifier, "Doomed job"):
            logger.info("about to fail")
            raise ValueError("boom")
    task = next(t for t in bus.tasks.value if t.title == "Doomed job")
    assert task.status == TaskStatus.FAILED
    assert task.error_message == "boom"
    assert any(
        t.type == ToastType.ERROR and "boom" in t.message for t in bus.toasts.value
    )
    # The failed thread's registration must not leak into later log calls.
    logger.info("after the failure")
    assert "after the failure" not in _milestones(bus, "Doomed job")


def test_no_active_job_is_a_noop():
    logger, bus, _notifier = _fresh()
    logger.info("nobody is tracking this")
    assert bus.tasks.value == []


def test_install_is_idempotent():
    notify_bridge.install_task_log_handler()
    notify_bridge.install_task_log_handler()
    handlers = [
        h
        for h in logging.getLogger("spatial_risk").handlers
        if isinstance(h, notify_bridge.TaskStepLogHandler)
    ]
    assert len(handlers) == 1


def test_concurrent_jobs_keep_milestones_separate():
    logger, bus, notifier = _fresh()
    barrier = threading.Barrier(2, timeout=5)

    def job(title, line):
        with notify_bridge.tracked_job(notifier, title):
            barrier.wait()  # both jobs are tracking before either logs
            logger.info(line)
            barrier.wait()  # neither exits before both have logged

    t1 = threading.Thread(target=job, args=("Job A", "line A"))
    t2 = threading.Thread(target=job, args=("Job B", "line B"))
    t1.start(), t2.start()
    t1.join(), t2.join()
    assert _milestones(bus, "Job A") == ["line A"]
    assert _milestones(bus, "Job B") == ["line B"]


def test_to_thread_in_context_attaches_caller_context_to_pool_thread(monkeypatch):
    """Job bodies tracked inside ``asyncio.to_thread`` publish bus updates from
    a pool thread — without the caller's kernel context attached first, those
    updates never reach the browser (same reason spawn_in_context exists)."""
    import asyncio

    from solara.server import kernel_context

    from gui.scripts.solara_threads import to_thread_in_context

    sentinel = object()
    attached = []
    monkeypatch.setattr(kernel_context, "get_current_context", lambda: sentinel)
    monkeypatch.setattr(kernel_context, "has_current_context", lambda: False)
    monkeypatch.setattr(
        kernel_context,
        "set_context_for_thread",
        lambda ctx, thread: attached.append((ctx, thread)),
    )

    caller = threading.current_thread()

    def work(x):
        return (threading.current_thread(), x * 2)

    worker, result = asyncio.run(to_thread_in_context(work, 21))
    assert result == 42
    assert worker is not caller
    assert attached and attached[0] == (sentinel, worker)


def test_to_thread_in_context_without_context_still_runs(monkeypatch):
    import asyncio

    from solara.server import kernel_context

    from gui.scripts.solara_threads import to_thread_in_context

    def _raise():
        raise RuntimeError("no context")

    monkeypatch.setattr(kernel_context, "get_current_context", _raise)
    assert asyncio.run(to_thread_in_context(lambda: "ok")) == "ok"


def test_nested_tracked_jobs_restore_the_outer_tracker():
    logger, bus, notifier = _fresh()
    with notify_bridge.tracked_job(notifier, "Outer"):
        with notify_bridge.tracked_job(notifier, "Inner"):
            logger.info("inner line")
        logger.info("outer line")
    assert _milestones(bus, "Inner") == ["inner line"]
    assert _milestones(bus, "Outer") == ["outer line"]
