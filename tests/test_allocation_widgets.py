"""Structural assertions on the allocation widgets (browserless)."""

import inspect

from gui.widget import allocation_form, allocation_list


def test_form_uses_creation_dialog_frame():
    """The form reuses the shared Create flow instead of a bespoke dialog."""
    src = inspect.getsource(allocation_form)
    assert "CreationDialog" in src
    assert "validate_form" in src


def test_form_offers_project_predictions_via_map_items():
    """Risk-map choices come from the same helper the evaluation tab uses."""
    assert "map_items" in inspect.getsource(allocation_form)


def test_widgets_never_use_rv_btn_on_click():
    """rv.Btn(on_click=...) silently drops clicks — solara.Button only."""
    for module in (allocation_form, allocation_list):
        src = inspect.getsource(module)
        assert "rv.Btn(" not in src or "on_click" not in src.split("rv.Btn(")[1][:200]


def test_list_renders_annual_and_total_hectares():
    """Both headline numbers of a run reach the list."""
    src = inspect.getsource(allocation_list)
    assert "annual_ha" in src and "total_ha" in src


def test_form_uses_the_shared_file_picker_for_local_paths():
    """Borders/mask/external files are picked, not hand-typed."""
    assert "FileInputComponent" in inspect.getsource(allocation_form)


def test_list_offers_density_toggle_only_when_a_density_map_exists():
    """The density action is gated on the run having written one."""
    src = inspect.getsource(allocation_list)
    assert "density_map_path" in src


# --- render tests -------------------------------------------------------
# The assertions above are source-substring checks; these mount the widgets so
# a wrong prop name or an unsupported cell spec fails here instead of in the app.

import ipyvuetify as vw  # noqa: E402
import reacton  # noqa: E402
import solara  # noqa: E402

from gui.i18n import t  # noqa: E402

# See test_manage_projects_render: warm the translator before the first render.
t("common.cancel")

from gui.widget.allocation_form import AllocationFormDialog  # noqa: E402
from gui.widget.allocation_list import AllocationList  # noqa: E402
from spatialrisk.project import Project  # noqa: E402


def _find(widget, cls, out=None):
    out = [] if out is None else out
    if isinstance(widget, cls):
        out.append(widget)
    for child in getattr(widget, "children", []) or []:
        if hasattr(child, "children") or isinstance(child, cls):
            _find(child, cls, out)
    return out


def test_form_dialog_mounts_with_its_fields():
    """The dialog renders: name, hectares and years text fields are present."""
    box, _rc = reacton.render(
        AllocationFormDialog(
            open_=solara.reactive(True),
            project=solara.reactive(Project(project_name="p")),
            on_launch=lambda form: None,
            on_close=lambda: None,
        )
    )
    labels = [f.label for f in _find(box, vw.TextField)]
    assert t("toolbox.allocation.field_name") in labels
    assert t("toolbox.allocation.field_juris_ha") in labels
    assert t("toolbox.allocation.field_years") in labels


def test_list_mounts_records_and_jobs():
    """A saved run and a running job both render without raising."""
    rows = [
        {
            "kind": "job",
            "key": "j1",
            "name": "pending",
            "status": "running",
            "error": None,
        },
        {
            "kind": "record",
            "key": "reserve_abc123",
            "name": "reserve",
            "created_at": "2026-07-29T10:00:00",
            "annual_ha": 312.4,
            "total_ha": 1249.6,
            "years_forecast": 4,
            "density_map_path": None,
            "warnings": [],
            "provenance": "computed",
        },
    ]
    box, _rc = reacton.render(AllocationList(rows=rows, on_delete=lambda key: None))
    texts = [w.children[0] for w in _find(box, vw.Html) if w.children]
    flat = " ".join(str(x) for x in texts)
    assert "reserve" in flat


def test_list_density_toggle_appears_only_for_runs_that_wrote_one():
    """A run without a density raster shows no map toggle."""
    from gui.scripts.density_map import density_layer_key

    with_density = {
        "kind": "record",
        "key": "r1",
        "name": "r1",
        "created_at": None,
        "annual_ha": 1.0,
        "total_ha": 4.0,
        "years_forecast": 4,
        "density_map_path": "/tmp/d.tif",
        "warnings": [],
        "provenance": "user",
    }
    without = dict(with_density, key="r2", name="r2", density_map_path=None)

    seen = []
    box, _rc = reacton.render(
        AllocationList(
            rows=[with_density, without],
            on_delete=lambda key: None,
            on_toggle_density=lambda row: seen.append(row),
            density_on_map={density_layer_key("r1")},
        )
    )
    icons = [i.children[0] for i in _find(box, vw.Icon) if i.children]
    # r1 is on the map -> mdi-map-minus; r2 contributes no toggle icon at all.
    assert "mdi-map-minus" in [str(i) for i in icons]
    assert "mdi-map-plus" not in [str(i) for i in icons]
