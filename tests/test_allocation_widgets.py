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


def test_borders_picker_offers_five_methods():
    """AdminLevelSelector reads its cascade depth from the method string.

    So 'administrative boundary' cannot be one option — it is three.
    """
    from gui.widget import borders_picker

    methods = [m for m, _ in borders_picker._METHODS]
    assert methods == ["ADMIN0", "ADMIN1", "ADMIN2", "FILE", "ASSET"]


def test_borders_picker_uses_the_non_gee_admin_path():
    """gee=False keeps Earth Engine out of the most common borders case."""
    from gui.widget import borders_picker

    assert "gee=False" in inspect.getsource(borders_picker)


def test_borders_picker_restricts_assets_to_tables():
    """An IMAGE asset is not a border."""
    from gui.widget import borders_picker

    assert 'types=["TABLE"]' in inspect.getsource(borders_picker)


def test_form_delegates_borders_to_the_picker():
    """The form no longer picks borders itself."""
    src = inspect.getsource(allocation_form)
    assert "BordersPicker" in src
    # Only the rate-table override remains file-picked in this module.
    assert src.count("FileInputComponent(") == 1


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


def test_form_has_no_external_riskmap_field():
    """External risk maps enter on the inference tab; the form offers none.

    The details dialog may *display* a run's external map, so this pins the
    form section only (everything above the details-dialog marker comment).
    """
    src = inspect.getsource(allocation_form)
    form_src = src.split("read-only run details")[0]
    assert "external" not in form_src


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
                model_key="jnr",
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


def test_borders_picker_hint_names_the_chosen_file():
    """The hint tells the user what the run will actually use."""
    from gui.scripts.allocation_runner import BordersSelection
    from gui.widget.borders_picker import _hint_text

    selection = BordersSelection(method="FILE", file_path="/data/reserve.gpkg")
    assert _hint_text(selection) == "reserve.gpkg"


def test_borders_picker_hint_flags_an_incomplete_selection():
    """A method chosen but nothing picked is not runnable; say so."""
    from gui.scripts.allocation_runner import BordersSelection
    from gui.widget.borders_picker import _hint_text

    assert t("toolbox.allocation.borders_hint_empty") == _hint_text(
        BordersSelection(method="ADMIN1")
    )


def test_borders_picker_hint_flags_an_asset_with_an_unset_filter_value():
    """A column chosen but no filter value is not runnable; the hint must agree.

    ``allocation_runner._validate_borders`` rejects an asset selection whose
    ``column`` is set to anything other than None/"ALL" while ``value`` is
    None. The hint used to confidently name the asset in that state anyway,
    so the user clicked Create only to be told to pick a filter value.
    """
    from gui.scripts.allocation_runner import BordersSelection
    from gui.widget.borders_picker import _hint_text

    selection = BordersSelection(
        method="ASSET",
        asset={
            "asset_id": "users/me/t",
            "type": "TABLE",
            "column": "adm1",
            "value": None,
        },
    )
    assert t("toolbox.allocation.borders_hint_empty") == _hint_text(selection)


def test_borders_picker_hint_names_a_fully_filtered_asset():
    """A column WITH a value is a complete, runnable selection."""
    from gui.scripts.allocation_runner import BordersSelection
    from gui.widget.borders_picker import _hint_text

    selection = BordersSelection(
        method="ASSET",
        asset={
            "asset_id": "users/me/t",
            "type": "TABLE",
            "column": "adm1",
            "value": "Nord",
        },
    )
    assert _hint_text(selection) == "users/me/t"


def _tight_wrapped(box, widget):
    """True if *widget* sits inside a ``div`` carrying the TIGHT_FIELD class.

    ``solara.Div(classes=[TIGHT_FIELD])`` renders as an ``ipyvuetify.Html``
    with ``tag='div'`` and a space-separated ``class_`` string.
    """
    from gui.widget.text_style import TIGHT_FIELD

    def contains(root):
        if root is widget:
            return True
        return any(contains(c) for c in (getattr(root, "children", None) or []))

    def walk(w):
        if getattr(w, "tag", None) == "div":
            classes = (getattr(w, "class_", None) or "").split()
            if TIGHT_FIELD in classes and contains(w):
                return True
        return any(walk(c) for c in (getattr(w, "children", None) or []))

    return walk(box)


def test_defrate_select_tight_class_tracks_whether_its_hint_will_render(tmp_path):
    """The rate-table select only collapses its own spacing when a hint follows.

    ``TIGHT_FIELD`` used to be applied unconditionally, so with no prediction
    chosen (DefrateResolutionHint does not render) the select sat ~22px
    tighter than the form's normal field rhythm.
    """
    from spatialrisk.predictions.prediction import Prediction

    project = Project(project_name="p")
    project.predictions["icar_run"] = Prediction(
        path=tmp_path / "prob.tif", model_key="icar", dataset_name="forecast"
    )

    box, _rc = reacton.render(
        AllocationFormDialog(
            open_=solara.reactive(True),
            project=solara.reactive(project),
            on_launch=lambda form: None,
            on_close=lambda: None,
        )
    )

    def defrate_select():
        return [
            s
            for s in _find(box, vw.Select)
            if s.label == t("toolbox.allocation.field_defrate")
        ][0]

    # No risk map chosen yet -> DefrateResolutionHint does not render.
    assert not _tight_wrapped(box, defrate_select())

    riskmap_select = [
        s
        for s in _find(box, vw.Select)
        if s.label == t("toolbox.allocation.field_riskmap")
    ][0]
    riskmap_select.v_model = "icar_run"

    # A prediction is now chosen -> the hint renders -> tight spacing applies.
    assert _tight_wrapped(box, defrate_select())


def test_mask_select_tight_class_tracks_whether_its_hint_will_render(
    tmp_path, monkeypatch
):
    """The mask select only collapses its own spacing when field_mask_none follows."""
    import gui.widget.allocation_form as allocation_form_module

    monkeypatch.setattr(
        allocation_form_module,
        "mask_items",
        lambda p: [{"text": "forest", "value": str(tmp_path / "forest.tif")}],
    )

    box, _rc = reacton.render(
        AllocationFormDialog(
            open_=solara.reactive(True),
            project=solara.reactive(Project(project_name="p")),
            on_launch=lambda form: None,
            on_close=lambda: None,
        )
    )

    def mask_select():
        return [
            s
            for s in _find(box, vw.Select)
            if s.label == t("toolbox.allocation.field_mask")
        ][0]

    # No mask picked yet -> field_mask_none WILL render.
    assert _tight_wrapped(box, mask_select())

    mask_select().v_model = str(tmp_path / "forest.tif")

    # A mask is now picked -> field_mask_none no longer renders.
    assert not _tight_wrapped(box, mask_select())


def test_borders_picker_renders_the_file_method_by_default():
    """The default method mounts without a map, a GEE session or a network.

    Vuetify's ``Select`` keeps its option labels in the ``items`` prop, not as
    rendered text nodes, so a browserless render can't see them via
    ``_all_text``. What it *can* see: which method is selected, that its
    label came from the picker's own i18n key, and that only the FILE
    method's own widget (``FileInput``) mounted — not the admin/asset ones,
    which would need pygaul or a live GEE session.
    """
    from pysepal.sepalwidgets.file_input import FileInput

    from gui.widget.borders_picker import BordersPicker

    box, _rc = reacton.render(BordersPicker(value=None, on_value=lambda _v: None))

    selects = _find(box, vw.Select)
    assert any(s.v_model == "FILE" for s in selects)
    labels = {item["value"]: item["text"] for s in selects for item in s.items}
    assert labels["FILE"] == t("toolbox.allocation.borders_method_file")
    assert _find(box, FileInput)


# --- run details dialog (2026-07-30 icon-rail + run-details spec) -----------


def _sample_run(**kw):
    from spatialrisk.allocations import AllocationRun

    base = dict(
        name="reserve",
        run_id="abc123",
        created_at="2026-07-29T10:00:00",
        prediction_key="rf_2024",
        defrate_source={"provenance": "persisted", "path": "/tmp/defrate.csv"},
        borders_file="/tmp/borders.gpkg",
        borders_source={"method": "ADMIN1", "admin_code": "MTQ"},
        mask_file="/tmp/forest_gfc_2024.tif",
        defor_juris_ha=20000.0,
        years_forecast=4.0,
        annual_ha=312.4,
        total_ha=1249.6,
        out_dir="/out",
        csv_path="/out/defor_project.csv",
    )
    base.update(kw)
    return AllocationRun(**base)


def _details_fields(box):
    return {f.label: f.v_model for f in _find(box, vw.TextField)}


def test_details_dialog_shows_inputs_and_results():
    """Every stored input and both results reach the read-only fields."""
    from gui.widget.allocation_form import AllocationDetailsDialog

    p = Project(project_name="p")
    run = _sample_run()
    p.allocations[run.storage_key()] = run

    box, _rc = reacton.render(
        AllocationDetailsDialog(
            project=solara.reactive(p),
            run_key=run.storage_key(),
            on_close=lambda: None,
        )
    )

    fields = _details_fields(box)
    assert fields[t("toolbox.allocation.field_riskmap")].startswith("rf_2024")
    assert (
        t("toolbox.allocation.provenance_persisted")
        in fields[t("toolbox.allocation.field_defrate")]
    )
    assert "MTQ" in fields[t("toolbox.allocation.field_borders")]
    assert "forest_gfc_2024" in fields[t("toolbox.allocation.field_mask")]
    assert "312.4" in fields[t("toolbox.allocation.result_annual")]
    assert "1,249.6" in fields[t("toolbox.allocation.result_total")]
    assert fields[t("toolbox.allocation.field_output_table")].endswith(
        "defor_project.csv"
    )
    assert fields[t("toolbox.allocation.field_created")] == "2026-07-29"


def test_details_dialog_labels_external_runs_and_borders_fallback():
    """No prediction -> external label; no borders_source -> file-name fallback."""
    from gui.widget.allocation_form import AllocationDetailsDialog

    p = Project(project_name="p")
    run = _sample_run(
        prediction_key=None,
        external_riskmap="/maps/riskmap_v2.tif",
        borders_source={},
        mask_file=None,
    )
    p.allocations[run.storage_key()] = run

    box, _rc = reacton.render(
        AllocationDetailsDialog(
            project=solara.reactive(p),
            run_key=run.storage_key(),
            on_close=lambda: None,
        )
    )

    fields = _details_fields(box)
    riskmap = fields[t("toolbox.allocation.field_riskmap")]
    assert "riskmap_v2.tif" in riskmap
    assert t("toolbox.allocation.source_external") in riskmap
    assert fields[t("toolbox.allocation.field_borders")] == "borders.gpkg"
    assert fields[t("toolbox.allocation.field_mask")] == t(
        "toolbox.allocation.field_mask_none"
    )


def test_details_dialog_is_closed_for_a_stale_key():
    """A run deleted while selected renders the dialog closed, not crashed."""
    from gui.widget.allocation_form import AllocationDetailsDialog

    box, _rc = reacton.render(
        AllocationDetailsDialog(
            project=solara.reactive(Project(project_name="p")),
            run_key="gone_run",
            on_close=lambda: None,
        )
    )
    dialogs = _find(box, vw.Dialog)
    assert dialogs and not dialogs[0].v_model


def test_details_dialog_renders_warnings():
    """Unallocated-classes warnings must be readable in the details view."""
    from gui.widget.allocation_form import AllocationDetailsDialog

    p = Project(project_name="p")
    run = _sample_run(warnings=["3.1 ha could not be allocated"])
    p.allocations[run.storage_key()] = run

    box, _rc = reacton.render(
        AllocationDetailsDialog(
            project=solara.reactive(p),
            run_key=run.storage_key(),
            on_close=lambda: None,
        )
    )
    assert "3.1 ha could not be allocated" in _all_text(box)


def _capture_product_table(monkeypatch):
    """Swap ProductTable for a stub that records its kwargs.

    The stub still renders an element — a component whose body renders nothing
    would make reacton.render itself fail, which is not what these tests probe.
    """
    captured = {}

    def fake_table(**kwargs):
        captured.update(kwargs)
        solara.Text("product-table-stub")

    monkeypatch.setattr("gui.widget.allocation_list.ProductTable", fake_table)
    return captured


def test_list_record_rows_open_details_jobs_do_not(monkeypatch):
    """Row click -> on_open(run_key); in-flight jobs are not clickable."""
    captured = _capture_product_table(monkeypatch)

    opened = []
    job = {
        "kind": "job",
        "key": "j1",
        "name": "pending",
        "status": "running",
        "error": None,
    }
    reacton.render(
        AllocationList(
            rows=[_record_row(), job],
            on_delete=lambda key: None,
            on_open=opened.append,
        )
    )

    rows = {r["key"]: r for r in captured["rows"]}
    rows["reserve_abc123"]["on_click"]()
    assert opened == ["reserve_abc123"]
    assert rows["j1"].get("on_click") is None


def test_list_without_on_open_has_no_clickable_rows(monkeypatch):
    """Summary-style consumers that pass no on_open get inert rows."""
    captured = _capture_product_table(monkeypatch)
    reacton.render(AllocationList(rows=[_record_row()], on_delete=lambda key: None))
    assert captured["rows"][0].get("on_click") is None
