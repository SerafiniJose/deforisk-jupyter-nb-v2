"""The Predictions list exposes a details ("how was this produced?") action.

Rendered, not grepped: a widget can import cleanly and still render no button
(see reacton-ipyvuetify-import-required), so these drive the real component and
click the real handler.
"""

import ipyvuetify as vw
import reacton
import solara

from gui.i18n import t

# Warm the translator before the first render — the first t() *during* a render
# breaks reacton's widget map.
t("common.cancel")

from gui.widget.inference_output_list import InferenceOutputList  # noqa: E402
from gui.widget.product_table import action_icon  # noqa: E402
from spatialrisk.predictions.prediction import Prediction  # noqa: E402
from spatialrisk.project import Project  # noqa: E402

INFO_ICON = action_icon("open")


def _find(widget, cls, out=None):
    out = [] if out is None else out
    if isinstance(widget, cls):
        out.append(widget)
    for child in getattr(widget, "children", []) or []:
        if hasattr(child, "children") or isinstance(child, cls):
            _find(child, cls, out)
    return out


def _icon_names(box):
    """Icon names of every action button rendered by the table."""
    return [
        str(icon.children[0])
        for btn in _find(box, vw.Btn)
        for icon in _find(btn, vw.Icon)
        if icon.children
    ]


def _project_with_prediction():
    p = Project(project_name="p")
    p.predictions["run_a"] = Prediction(
        name="run_a",
        path="/tmp/run_a.tif",
        model_key="glm_glm_v1",
        dataset_name="calibration",
        model_snapshot={"model_type": "glm", "name": "glm_v1"},
    )
    return solara.reactive(p)


def _render(project, jobs, on_open):
    box, _rc = reacton.render(
        InferenceOutputList(
            project=project,
            inference_jobs=solara.reactive(jobs),
            on_open=on_open,
        )
    )
    return box


def test_prediction_row_offers_the_details_action():
    """A registered prediction gets the info button."""
    box = _render(_project_with_prediction(), [], lambda row: None)
    assert INFO_ICON in _icon_names(box)


def test_details_action_reports_the_row_it_belongs_to():
    """The tile needs the row key to know which prediction to explain."""
    seen = []
    box = _render(_project_with_prediction(), [], seen.append)

    for btn in _find(box, vw.Btn):
        if any(
            str(i.children[0]) == INFO_ICON for i in _find(btn, vw.Icon) if i.children
        ):
            btn.fire_event("click", {})

    assert [row["key"] for row in seen] == ["run_a"]


def test_running_job_row_has_no_details_action():
    """A job with no registered output has nothing to explain yet."""
    jobs = [
        {
            "id": "j1",
            "status": "running",
            "pred_name": "pending_run",
            "model_key": "glm_glm_v1",
            "dataset_name": "calibration",
        }
    ]
    box = _render(solara.reactive(Project(project_name="p")), jobs, lambda row: None)
    assert INFO_ICON not in _icon_names(box)


def test_details_action_is_omitted_when_the_tile_supplies_no_handler():
    """No on_open, no button — the list never invents an action."""
    box, _rc = reacton.render(
        InferenceOutputList(
            project=_project_with_prediction(),
            inference_jobs=solara.reactive([]),
        )
    )
    assert INFO_ICON not in _icon_names(box)


def test_tile_mounts_the_details_dialog_and_feeds_the_list():
    """The tile owns the selected row; the list only reports the click.

    Same three-part shape as the Train and Sampling tabs: a state hook, the
    list's on_open bound to its setter, and the dialog mounted beside the list.
    """
    import inspect

    from gui.tile.inference_tile import InferenceTile

    src = inspect.getsource(InferenceTile)
    assert "set_details_key = solara.use_state(None)" in src
    assert 'on_open=lambda row: set_details_key(row["key"])' in src
    assert "row_key=details_key" in src
    assert "on_close=lambda: set_details_key(None)" in src
