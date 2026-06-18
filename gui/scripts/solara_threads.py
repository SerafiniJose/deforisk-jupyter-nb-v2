"""Helpers for running background work from Solara event handlers.

Reactive updates (``some_reactive.set(...)``) made from a *bare*
``threading.Thread`` never reach the browser session, so status cards stay
stuck on "running" even after the backend has finished. Registering the render
thread's kernel context onto the worker thread — exactly what
``solara.lab.use_task`` does internally — lets those updates propagate.
"""

import threading


def update_job(jobs_reactive, job_id, *, skip_if_cancelled=True, **changes):
    """Immutably update one job dict by id and publish so the UI re-renders.

    Mutating a job dict in place and then calling ``reactive.set(list(...))``
    does NOT update the browser: the old and new lists share the same dict
    objects, so Solara's ``equals_extra(old, new)`` is True and ``set`` short-
    circuits without firing listeners — the status card stays stuck on a
    spinning "running" icon even after the background job has finished. Building
    a fresh dict for the changed job makes the new list genuinely differ so the
    update propagates.

    Parameters
    ----------
    jobs_reactive : solara.Reactive[list[dict]]
        The reactive holding the list of job dicts (each with an ``"id"``).
    job_id : str
        Id of the job to update.
    skip_if_cancelled : bool
        When True (default) a job the user already cancelled is left untouched,
        so a late-finishing thread can't resurrect it as completed/failed.
    **changes
        Fields to overwrite on the matching job dict.
    """
    new_jobs = []
    for j in jobs_reactive.value:
        if j["id"] == job_id and not (skip_if_cancelled and j["status"] == "cancelled"):
            new_jobs.append({**j, **changes})
        else:
            new_jobs.append(j)
    jobs_reactive.set(new_jobs)


def spawn_in_context(target, args=(), *, daemon=True):
    """Start a daemon thread that inherits the caller's Solara kernel context.

    Falls back to a plain thread when there is no active context (e.g. unit
    tests), so the function is usable outside a running app.

    Parameters
    ----------
    target : callable
        Function to run in the background thread.
    args : tuple
        Positional arguments forwarded to ``target``.
    daemon : bool
        Whether the thread is a daemon (default ``True``).

    Returns
    -------
    threading.Thread
        The started thread.
    """
    from solara.server import kernel_context

    thread = threading.Thread(target=target, args=args, daemon=daemon)
    try:
        ctx = kernel_context.get_current_context()
    except RuntimeError:
        ctx = None
    if ctx is not None:
        kernel_context.set_context_for_thread(ctx, thread)
    thread.start()
    return thread
