"""geedim_tile_progress turns geedim's tile tqdm bar into a progress callback.

geedim's ``map_tiles`` builds its per-image bar via
``geedim.utils.auto_leave_tqdm(..., total=n_tiles, unit='tiles')`` and ticks it
once per completed tile. The hook rebinds ``geedim.utils.tqdm`` so those ticks
also reach a thread-scoped ``callback(done, total)`` — without forking geedim.
"""

import io
import threading

from spatialrisk.gee.progress import geedim_tile_progress


def _run_fake_download(n_tiles):
    """Create and drain a bar exactly the way geedim's map_tiles does."""
    import geedim.utils as gd_utils

    kwargs = gd_utils.get_tqdm_kwargs(desc="img", unit="tiles")
    bar = gd_utils.auto_leave_tqdm(
        range(n_tiles), total=n_tiles, file=io.StringIO(), **kwargs
    )
    for _ in bar:
        pass


def test_callback_receives_each_tile_tick():
    """Every yielded tile reaches the callback as (done, total)."""
    calls = []
    with geedim_tile_progress(lambda done, total: calls.append((done, total))):
        _run_fake_download(3)
    assert calls == [(1, 3), (2, 3), (3, 3)]


def test_no_callback_outside_context():
    """Bars created after the context exits report nothing."""
    calls = []
    with geedim_tile_progress(lambda done, total: calls.append((done, total))):
        pass
    _run_fake_download(2)
    assert calls == []


def test_context_restores_after_exception():
    """A failing download body still deregisters the callback."""
    calls = []
    try:
        with geedim_tile_progress(lambda done, total: calls.append((done, total))):
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    _run_fake_download(2)
    assert calls == []


def test_bars_without_total_are_ignored():
    """Export-monitor bars (no total) must never reach the callback."""
    import geedim.utils as gd_utils

    calls = []
    with geedim_tile_progress(lambda done, total: calls.append((done, total))):
        bar = gd_utils.tqdm(iter(range(2)), file=io.StringIO())
        for _ in bar:
            pass
    assert calls == []


def test_callback_is_thread_scoped():
    """A bar created on another thread must not see this thread's callback."""
    calls = []
    with geedim_tile_progress(lambda done, total: calls.append((done, total))):
        t = threading.Thread(target=_run_fake_download, args=(3,))
        t.start()
        t.join()
    assert calls == []
