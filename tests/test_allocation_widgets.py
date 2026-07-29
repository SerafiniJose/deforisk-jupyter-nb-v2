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


def test_tight_field_css_collapses_only_the_empty_messages_row():
    """The hint fix must not suppress real Vuetify error messages.

    `hide_details` would remove the row entirely; collapsing min-height lets an
    error still render at its natural height while an empty row takes none.
    """
    from gui.widget.creation_dialog import _ADVANCED_PANEL_CSS

    assert ".sr-tight-field .v-text-field__details" in _ADVANCED_PANEL_CSS
    assert ".sr-tight-field .v-messages" in _ADVANCED_PANEL_CSS
    assert "hide_details" not in inspect.getsource(allocation_form)


def test_form_hints_use_the_shared_field_hint():
    """Every hint in the form goes through FieldHint, not a bare Text."""
    src = inspect.getsource(allocation_form)
    assert "FieldHint" in src
    assert "TIGHT_FIELD" in src


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


# --- mock-fidelity additions: mask select, eager preview, meta rows, card ----


def _all_text(box):
    """Every text fragment in the tree: plain Html, chip labels, alert bodies."""
    out = []
    for cls in (vw.Html, vw.Chip, vw.Alert):
        for w in _find(box, cls):
            out.extend(str(c) for c in (w.children or []) if isinstance(c, str))
    return " ".join(out)


def test_form_mask_choices_come_from_processed_variables():
    """The mask is picked from the project's processed rasters, not free files."""
    src = inspect.getsource(allocation_form)
    assert "mask_items" in src
    # Only the rate-table override and the borders remain file-picked.
    assert src.count("FileInputComponent(") == 2


def test_form_has_no_external_riskmap_field():
    """External risk maps enter on the inference tab; the form offers none."""
    src = inspect.getsource(allocation_form)
    assert "external" not in src
    assert "field_riskmap_external" not in src


def test_form_previews_the_resolved_rate_table():
    """The form resolves the rate table eagerly so the promise is visible."""
    assert "preview_defrate_source" in inspect.getsource(allocation_form)


def test_defrate_hint_names_a_persisted_table(tmp_path):
    """A resolvable table is previewed by file name with its provenance."""
    from types import SimpleNamespace

    from gui.widget.allocation_form import DefrateResolutionHint

    csv = tmp_path / "defrate_cat_bm_forecast.csv"
    csv.write_text("cat\n1\n")
    project = SimpleNamespace(
        predictions={
            "jnr_run": SimpleNamespace(
                path=tmp_path / "prob.tif",
                model_key="benchmark",
                dataset_name="forecast",
                window=None,
                defrate_path=csv,
            )
        }
    )

    box, _rc = reacton.render(
        DefrateResolutionHint(project_value=project, pred_key="jnr_run", override="")
    )

    flat = _all_text(box)
    assert "defrate_cat_bm_forecast.csv" in flat
    assert t("toolbox.allocation.provenance_persisted") in flat


def test_defrate_hint_announces_a_table_to_be_computed(tmp_path):
    """A FAR prediction previews as 'will be computed', not as an error."""
    from types import SimpleNamespace

    from gui.widget.allocation_form import DefrateResolutionHint

    project = SimpleNamespace(
        predictions={
            "icar_run": SimpleNamespace(
                path=tmp_path / "prob.tif",
                model_key="icar",
                dataset_name="forecast",
                window=None,
                defrate_path=None,
            )
        }
    )

    box, _rc = reacton.render(
        DefrateResolutionHint(project_value=project, pred_key="icar_run", override="")
    )

    flat = _all_text(box)
    assert t("toolbox.allocation.defrate_will_compute") in flat
    # The description says it all — a 'computed from the dataset' chip on top
    # of it is redundant.
    assert _find(box, vw.Chip) == []


def test_defrate_hint_surfaces_an_unresolvable_table(tmp_path):
    """When nothing resolves, the reason is shown instead of a silent gap."""
    from types import SimpleNamespace

    from gui.widget.allocation_form import DefrateResolutionHint

    prob = tmp_path / "prob_mw_11_forecast.tif"
    prob.write_bytes(b"")
    project = SimpleNamespace(
        predictions={
            "mw_run": SimpleNamespace(
                path=prob,
                model_key="mw",
                dataset_name="forecast",
                window=11,
                defrate_path=None,
            )
        }
    )

    box, _rc = reacton.render(
        DefrateResolutionHint(project_value=project, pred_key="mw_run", override="")
    )

    flat = _all_text(box)
    assert "rate table" in flat


def _record_row(**kw):
    base = {
        "kind": "record",
        "key": "reserve_abc123",
        "name": "reserve",
        "source": "ICAR — forecast",
        "created_at": "2026-07-29T10:00:00",
        "annual_ha": 312.4,
        "total_ha": 1249.6,
        "years_forecast": 4,
        "density_map_path": None,
        "warnings": [],
        "provenance": "computed",
    }
    base.update(kw)
    return base


def test_list_rows_carry_the_source_meta_line():
    """A run row says what it was computed from, over how long, and when."""
    box, _rc = reacton.render(
        AllocationList(rows=[_record_row()], on_delete=lambda key: None)
    )
    flat = " ".join(str(w.children[0]) for w in _find(box, vw.Html) if w.children)
    assert "ICAR — forecast" in flat
    assert "2026-07-29" in flat


def test_list_labels_external_map_runs():
    """No prediction snapshot -> the meta line says 'external risk map'."""
    box, _rc = reacton.render(
        AllocationList(rows=[_record_row(source=None)], on_delete=lambda key: None)
    )
    flat = " ".join(str(w.children[0]) for w in _find(box, vw.Html) if w.children)
    assert t("toolbox.allocation.source_external") in flat


def test_result_card_shows_both_headline_numbers():
    """The latest-run card carries annual and entire-period hectares."""
    from gui.widget.allocation_list import AllocationResultCard

    box, _rc = reacton.render(AllocationResultCard(row=_record_row()))
    flat = " ".join(str(w.children[0]) for w in _find(box, vw.Html) if w.children)
    assert "312.4" in flat
    assert "1,249.6" in flat
    assert t("toolbox.allocation.result_annual") in flat
    assert t("toolbox.allocation.result_total") in flat


def test_form_defrate_mode_dropdown_defaults_to_automatic():
    """The rate table is a two-option mode select, defaulting to automatic.

    The custom-table file picker stays hidden until that mode is chosen.
    """
    box, _rc = reacton.render(
        AllocationFormDialog(
            open_=solara.reactive(True),
            project=solara.reactive(Project(project_name="p")),
            on_launch=lambda form: None,
            on_close=lambda: None,
        )
    )
    select_labels = [s.label for s in _find(box, vw.Select)]
    assert t("toolbox.allocation.field_defrate") in select_labels
    # No table-file picker mounted while the mode is 'automatic'.
    picker_hosts = _find(box, vw.TextField) + _find(box, vw.Select)
    assert t("toolbox.allocation.field_defrate_override") not in [
        w.label for w in picker_hosts
    ]


def test_form_guards_a_custom_mode_without_a_file():
    """Picking 'custom table' and no file must not silently fall back to auto."""
    src = inspect.getsource(allocation_form)
    assert "_DEFRATE_CUSTOM" in src
    assert "switch back" in src  # the validate() guard message


def test_field_hint_renders_its_children():
    """FieldHint is a wrapper: whatever goes in must come out visible."""
    from gui.widget.text_style import FieldHint

    box, _rc = reacton.render(FieldHint(children=[solara.Text("resolved table.csv")]))
    assert "resolved table.csv" in _all_text(box)
