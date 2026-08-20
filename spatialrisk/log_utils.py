"""Progress-logging helpers for multi-item operations.

`log_progress` wraps an iterable and emits one INFO line per item on the shared
``spatial_risk`` logger (so the lines surface as notification-task milestones
in the GUI, on the console, and in the log file), then yields the item for the
caller's loop body.
"""

import logging

logger = logging.getLogger("spatial_risk")


def log_progress(items, verb, *, label=None):
    """Yield each item, logging '<verb> <i>/<n>: <name>' at INFO before yielding.

    ``items`` is materialized to a list so the total is known up front. ``label``
    is a callable ``item -> str`` supplying the per-item name; when ``None`` the
    ``': name'`` suffix is dropped.
    """
    items = list(items)
    total = len(items)
    for i, item in enumerate(items, 1):
        suffix = f": {label(item)}" if label else ""
        logger.info("%s %d/%d%s", verb, i, total, suffix)
        yield item
