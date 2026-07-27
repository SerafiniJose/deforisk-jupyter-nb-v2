"""Regression: edge/dist must not run on the Solara event-handler thread.

``PostProcessTile.on_submit`` sent change detection to a background task but ran
edge/dist inline, straight in the widget callback. Solara executes widget
callbacks inside ``process_kernel_messages`` — called synchronously, while
holding ``context.lock``, from the per-session websocket thread's
``while True: message = await ws.receive()`` loop (solara/server/server.py).
Blocking there means the session reads no further websocket messages for the
whole run: every button goes dead and no re-render or notification milestone
reaches the browser. Over a large AOI ``gdal.ComputeProximity`` takes minutes,
so the app simply appeared to hang; over a small test AOI it returned in under a
second and nobody noticed.

A worker thread is genuinely enough here — GDAL releases the GIL for
``ComputeProximity`` (measured: a ticker thread kept its 5 ms cadence for the
whole call), unlike the forestatrisk MCMC case that needed a subprocess.
"""

import threading

import reacton
import solara

# Long enough that a synchronous handler visibly blocks, short enough that the
# regression case fails fast instead of hanging the suite.
BLOCK_TIMEOUT = 5.0


def _project_with_processed_var():
    from spatialrisk.project import Project
    from spatialrisk.variables.local_raster_var import LocalRasterVar

    p = Project(project_name="demo")
    p.processed_variables["forest_2010"] = LocalRasterVar.model_construct(
        name="forest",
        year=2010,
        data_type="raster",
        raster_type="categorical",
        path=None,
        project=p,
    )
    return p


def _render_capturing_on_submit(monkeypatch, project, process_error):
    """Render PostProcessTile with the dialog stubbed out to hand back on_submit."""
    from gui.tile import postprocess_tile

    captured = {}

    @solara.component
    def _StubDialog(project, open_, on_submit):
        captured["on_submit"] = on_submit
        solara.Text("")

    monkeypatch.setattr(postprocess_tile, "DerivedLayerDialog", _StubDialog)

    box, rc = reacton.render(
        postprocess_tile.PostProcessTile(
            project=project, process_error=process_error, map_=None
        ),
        handle_error=False,
    )
    return captured["on_submit"], rc


def test_edge_dist_does_not_block_the_event_handler(monkeypatch):
    """on_submit must hand edge/dist to a worker and return immediately."""
    from gui.scripts import process_actions

    started = threading.Event()
    release = threading.Event()
    ran_on = {}

    def _blocking_apply(project, key, step):
        ran_on["ident"] = threading.get_ident()
        started.set()
        release.wait(BLOCK_TIMEOUT)

    monkeypatch.setattr(process_actions, "apply_post_processing", _blocking_apply)

    project = solara.reactive(_project_with_processed_var(), equals=lambda a, b: a is b)
    process_error = solara.reactive(None)
    on_submit, rc = _render_capturing_on_submit(monkeypatch, project, process_error)

    handler_ident = threading.get_ident()
    on_submit({"op": "dist", "start_key": "", "end_key": "", "pp_key": "forest_2010"})
    handler_returned_early = not release.is_set() and started.wait(BLOCK_TIMEOUT)

    try:
        assert started.is_set(), "edge/dist work never ran"
        assert ran_on["ident"] != handler_ident, (
            "edge/dist ran on the event-handler thread — that blocks solara's "
            "websocket message loop and freezes the whole session"
        )
        assert handler_returned_early, (
            "on_submit only returned after the work finished — the handler is "
            "still synchronous"
        )
    finally:
        release.set()
        rc.close()


def test_edge_dist_reports_failures_through_process_error(monkeypatch):
    """Moving to a thread must not lose the error path the sync version had."""
    from gui.scripts import process_actions

    def _boom(project, key, step):
        raise RuntimeError("gdal exploded")

    monkeypatch.setattr(process_actions, "apply_post_processing", _boom)

    project = solara.reactive(_project_with_processed_var(), equals=lambda a, b: a is b)
    process_error = solara.reactive(None)
    on_submit, rc = _render_capturing_on_submit(monkeypatch, project, process_error)

    on_submit({"op": "dist", "start_key": "", "end_key": "", "pp_key": "forest_2010"})

    deadline = threading.Event()
    for _ in range(int(BLOCK_TIMEOUT * 100)):
        if process_error.value is not None:
            break
        deadline.wait(0.01)

    assert process_error.value is not None, "the failure never reached the UI"
    assert "gdal exploded" in process_error.value
    rc.close()
