"""SampleSetList: the eye action opens details, on registered samples only."""

import ipyvuetify as vw
import reacton
import solara

from gui.i18n import t

# Warm the translator before the first render (see test_model_form_dialog_render).
t("common.cancel")

from gui.widget.sample_set_list import SampleSetList  # noqa: E402
from spatialrisk.project import Project  # noqa: E402
from spatialrisk.sample import Sample  # noqa: E402


def _find(widget, cls, out=None):
    out = [] if out is None else out
    if isinstance(widget, cls):
        out.append(widget)
    for child in getattr(widget, "children", []) or []:
        if hasattr(child, "children") or isinstance(child, cls):
            _find(child, cls, out)
    return out


def _icon_names(box):
    """Every action icon in the table, in render order.

    ProductTable renders its own ``mdi-chevron-up`` panel-header icon before
    any row, so that one is filtered out — verified against the real widget
    tree, which yields ['mdi-chevron-up', 'mdi-table-eye', 'mdi-map-plus',
    'mdi-delete-outline'] for a single three-action row.
    """
    names = []
    for icon in _find(box, vw.Icon):
        for child in icon.children or []:
            if isinstance(child, str) and child.startswith("mdi-"):
                names.append(child)
    return [n for n in names if not n.startswith("mdi-chevron")]


def _captured_rows(project, jobs=None, **kwargs):
    """The row specs SampleSetList hands to ProductTable.

    Asserting on the specs rather than on the rendered icon tree keeps the
    order assertions independent of the table's own chrome icons.
    """
    import gui.widget.sample_set_list as mod

    seen = {}
    original = mod.ProductTable

    def _capture(**kw):
        seen["rows"] = kw["rows"]
        return original(**kw)

    mod.ProductTable = _capture
    try:
        _render(project, jobs=jobs, **kwargs)
    finally:
        mod.ProductTable = original
    return seen["rows"]


def _project_with_one_sample():
    p = Project(project_name="p")
    p.samples["rand_1"] = Sample(
        name="rand_1", raster_var_name="fcc", strategy="random", n_samples=10
    )
    return solara.reactive(p)


def _project_with_two_samples():
    p = Project(project_name="p")
    for key in ("rand_1", "rand_2"):
        p.samples[key] = Sample(
            name=key, raster_var_name="fcc", strategy="random", n_samples=10
        )
    return solara.reactive(p)


def _render(project, jobs=None, **kwargs):
    box, _rc = reacton.render(
        SampleSetList(
            project=project,
            sampling_jobs=solara.reactive(jobs or []),
            on_toggle_map=lambda k: None,
            on_remove=lambda k: None,
            on_dismiss=lambda i: None,
            **kwargs,
        )
    )
    return box


def test_open_action_absent_when_no_callback_is_given():
    """Without on_open, no eye icon is rendered at all."""
    box = _render(_project_with_one_sample())
    assert "mdi-table-eye" not in _icon_names(box)


def test_eye_icon_reaches_the_rendered_tree():
    """With on_open given, the eye icon shows up in the real widget tree."""
    box = _render(_project_with_one_sample(), on_open=lambda k: None)
    assert "mdi-table-eye" in _icon_names(box)


def test_open_action_is_the_first_action_on_a_sample_row():
    """The eye action leads, ahead of the map-toggle and delete actions."""
    rows = _captured_rows(_project_with_one_sample(), on_open=lambda k: None)
    kinds = [a["kind"] for a in rows[0]["actions"]]
    assert kinds == ["open", "map_toggle", "delete"]


def test_job_rows_get_no_open_action():
    """A failed job has no sample to inspect — only dismiss."""
    jobs = [
        {
            "id": "j1",
            "name": "pending_1",
            "strategy": "random",
            "status": "failed",
            "error": "boom",
        }
    ]
    rows = _captured_rows(_project_with_one_sample(), jobs=jobs, on_open=lambda k: None)
    # Job rows come first (product_rows.sample_rows), then registered samples.
    by_kind = {r["key"]: [a["kind"] for a in r["actions"]] for r in rows}
    assert by_kind["job_j1"] == ["dismiss"]
    assert by_kind["rand_1"] == ["open", "map_toggle", "delete"]


def test_open_action_carries_the_sample_key():
    """Each row's eye action calls on_open with that row's OWN sample key.

    Two samples are required to catch a missing ``k=key`` default-argument
    capture: with a single sample, a buggy bare closure over the loop
    variable would still pass by coincidence, since the loop's final value
    happens to equal the lone row's key.
    """
    seen = []
    rows = _captured_rows(_project_with_two_samples(), on_open=seen.append)
    for row in rows:
        row["actions"][0]["on_click"]()
    assert seen == ["rand_1", "rand_2"]
