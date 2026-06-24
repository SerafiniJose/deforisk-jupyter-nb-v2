"""Bridge the `spatial_risk` logger to a Solara reactive for the on-map LogConsole.

A ``ReactiveLogHandler`` attached to the logger pushes each INFO+ record into the
capped, immutable ``log_records`` tuple; the ``LogConsole`` widget renders it.
Background jobs log from worker threads, so ``emit`` attaches the kernel context
the console binds on mount (mirroring ``spawn_in_context``) — otherwise
``reactive.set`` from an ``asyncio.to_thread`` pool thread never reaches the browser.
"""

import logging
import threading
import time

import solara

MAX_RECORDS = 200
_LOGGER_NAME = "spatial_risk"

# Module-level, session-global (single-user app, like train_jobs / vars_on_map).
# Immutable tuple so every .set() is a new identity — Solara never short-circuits
# on equality (same reasoning as update_job's fresh-dict rule).
log_records = solara.reactive(tuple())


def _record_to_dict(record: logging.LogRecord) -> dict:
    return {
        "time": time.strftime("%H:%M:%S", time.localtime(record.created)),
        "level": record.levelname,
        "name": record.name,
        "msg": record.getMessage(),
    }


class ReactiveLogHandler(logging.Handler):
    """Append each emitted record to ``log_records`` and publish to the browser."""

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.RLock()
        self._ctx = None  # kernel context bound by LogConsole on mount

    def bind_context(self, ctx) -> None:
        """Remember the live kernel context so worker-thread emits can attach it."""
        self._ctx = ctx

    def emit(self, record: logging.LogRecord) -> None:
        try:
            item = _record_to_dict(record)
            # Hold the lock across _publish, not just the buffer build: the
            # read-modify-publish must be atomic, or two concurrent emits could
            # each snapshot the same buffer and publish one missing the other's
            # record. Do NOT narrow this back to only the buffer build.
            with self._lock:
                buf = (log_records.value + (item,))[-MAX_RECORDS:]
                self._publish(buf)
        except Exception:  # a logging handler must never raise into caller code
            pass

    def _publish(self, buf: tuple) -> None:
        from solara.server import kernel_context

        # reactive.set only reaches the browser from a context-bearing thread.
        # to_thread pool threads may lack one — attach the bound context.
        if not kernel_context.has_current_context() and self._ctx is not None:
            kernel_context.set_context_for_thread(self._ctx, threading.current_thread())
        log_records.set(buf)


def install_log_console_handler(level: int = logging.INFO) -> ReactiveLogHandler:
    """Attach a single ``ReactiveLogHandler`` to the ``spatial_risk`` logger.

    Idempotent: returns the existing handler if one is already attached (guards
    Solara hot-reload / repeated imports). Sets the handler level to ``INFO`` but
    does NOT change the logger's own level (file/console keep DEBUG detail).
    """
    logger = logging.getLogger(_LOGGER_NAME)
    for h in logger.handlers:
        if isinstance(h, ReactiveLogHandler):
            return h
    handler = ReactiveLogHandler()
    handler.setLevel(level)
    logger.addHandler(handler)
    return handler


def clear_log_records() -> None:
    """Empty the buffer (called on project switch)."""
    log_records.set(tuple())
