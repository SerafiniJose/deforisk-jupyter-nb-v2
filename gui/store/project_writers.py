"""Which project folders currently have a background task writing into them.

Deleting a project's folder while a task is still saving into it is destructive
in a way that is easy to miss: ``Project.save()`` does
``mkdir(parents=True, exist_ok=True)``, so a late auto-save silently **re-creates
the folder that was just deleted**, leaving a half-project on disk. Every
long-running writer therefore marks its project for the duration of its work, and
the delete dialog refuses while the mark is set.

Keyed by project **name** rather than "is this the open project": a job whose
project was switched away keeps writing into the old folder, and that folder is
then one the user can delete. A count, not a flag, because writers overlap
(a download and a training run can be in flight together).
"""

import contextlib
import threading

import solara

# {project_name: number of background tasks currently writing into it}
project_writers = solara.reactive({})

_lock = threading.Lock()


def _add(project_name: str, delta: int) -> None:
    # Writers run on several threads, so read-modify-write needs the lock. A new
    # dict is published each time: mutating in place would leave old and new
    # equal, and Solara would short-circuit the update.
    with _lock:
        counts = dict(project_writers.value)
        count = counts.get(project_name, 0) + delta
        if count > 0:
            counts[project_name] = count
        else:
            counts.pop(project_name, None)
        project_writers.set(counts)


@contextlib.contextmanager
def writing(project_name: str):
    """Mark a project's folder as being written to for the duration of the block.

    The mark is always released, including when the body raises — a crashed job
    must not block deletes for the rest of the session.
    """
    _add(project_name, 1)
    try:
        yield
    finally:
        _add(project_name, -1)


def is_writing(project_name: str) -> bool:
    """True when a background task is writing into this project's folder."""
    return project_writers.value.get(project_name, 0) > 0
