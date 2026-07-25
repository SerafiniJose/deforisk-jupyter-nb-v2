"""SampleDetailsDialog: read-only mirror of the New-sample form's inputs."""

import inspect

import ipyvuetify as vw
import reacton
import solara

from gui.i18n import t

# See test_model_form_dialog_render: warm the translator before the first
# render — the first t() *during* a render breaks reacton's widget map.
t("common.cancel")

from gui.widget.sample_form_dialog import SampleDetailsDialog  # noqa: E402
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


def _project_with(sample: Sample) -> solara.Reactive:
    p = Project(project_name="p")
    p.samples[sample.name] = sample
    return solara.reactive(p)


def _render(project, key):
    box, _rc = reacton.render(
        SampleDetailsDialog(project=project, sample_key=key, on_close=lambda: None)
    )
    return box


def _labels(box):
    return [f.label for f in _find(box, vw.TextField)]


def _values(box):
    return {f.label: f.v_model for f in _find(box, vw.TextField)}


def _stratified_deforisk():
    return Sample(
        name="strat_1",
        raster_var_name="fcc",
        mask_var_name="forest",
        strategy="stratified",
        allocation="deforisk",
        adapt=True,
        n_samples=500,
        seed=42,
    )


def test_every_field_is_read_only():
    """Every rendered TextField is readonly — this is a details view, not a form."""
    box = _render(_project_with(_stratified_deforisk()), "strat_1")
    fields = _find(box, vw.TextField)
    assert fields, "no fields rendered"
    assert all(f.readonly for f in fields)


def test_stratified_deforisk_shows_allocation_and_adapt():
    """A stratified/deforisk sample shows every field, including adapt."""
    box = _render(_project_with(_stratified_deforisk()), "strat_1")
    values = _values(box)
    assert values[t("tiles.sampling.strategy_label")] == "stratified"
    assert values[t("tiles.sampling.allocation_label")] == "deforisk"
    assert values[t("tiles.sampling.adapt_label")] == t("common.yes")
    assert values[t("tiles.sampling.raster_variable_label_strata")] == "fcc"
    assert values[t("tiles.sampling.mask_variable_label")] == "forest"
    assert values[t("tiles.sampling.n_samples_label")] == "500"
    assert values[t("tiles.sampling.sample_name_label")] == "strat_1"
    assert values[t("tiles.sampling.seed_label")] == "42"


def test_stratified_deforisk_field_order_is_exact():
    """Pins the exact field set and order for the stratified/deforisk scenario.

    Nothing extra (n_total, class_counts, crs, created_at) can sneak in, and
    the allocation/adapt conditionals must render in the form's order.
    """
    box = _render(_project_with(_stratified_deforisk()), "strat_1")
    assert _labels(box) == [
        t("tiles.sampling.strategy_label"),
        t("tiles.sampling.raster_variable_label_strata"),
        t("tiles.sampling.mask_variable_label"),
        t("tiles.sampling.allocation_label"),
        t("tiles.sampling.adapt_label"),
        t("tiles.sampling.n_samples_label"),
        t("tiles.sampling.sample_name_label"),
        t("tiles.sampling.seed_label"),
    ]


def test_random_sample_hides_allocation_and_adapt_and_uses_the_area_label():
    """Random strategy has no strata, so allocation/adapt/strata label are absent."""
    s = Sample(name="rand_1", raster_var_name="fcc", strategy="random", n_samples=1000)
    labels = _labels(_render(_project_with(s), "rand_1"))
    assert t("tiles.sampling.raster_variable_label_area") in labels
    assert t("tiles.sampling.raster_variable_label_strata") not in labels
    assert t("tiles.sampling.allocation_label") not in labels
    assert t("tiles.sampling.adapt_label") not in labels


def test_random_sample_field_order_is_exact():
    """Pins the exact field set and order for a non-stratified scenario.

    Allocation and adapt must be absent entirely (not just unchecked), and no
    generated-result field (n_total, class_counts, crs, created_at) appears.
    """
    s = Sample(name="rand_1", raster_var_name="fcc", strategy="random", n_samples=1000)
    box = _render(_project_with(s), "rand_1")
    assert _labels(box) == [
        t("tiles.sampling.strategy_label"),
        t("tiles.sampling.raster_variable_label_area"),
        t("tiles.sampling.mask_variable_label"),
        t("tiles.sampling.n_samples_label"),
        t("tiles.sampling.sample_name_label"),
        t("tiles.sampling.seed_label"),
    ]


def test_equal_allocation_shows_allocation_but_not_adapt():
    """Adapt only applies to the deforisk allocation, not equal."""
    s = Sample(
        name="strat_2",
        raster_var_name="fcc",
        strategy="stratified",
        allocation="equal",
        n_samples=200,
    )
    labels = _labels(_render(_project_with(s), "strat_2"))
    assert t("tiles.sampling.allocation_label") in labels
    assert t("tiles.sampling.adapt_label") not in labels


def test_spacing_defined_sample_shows_spacing_instead_of_count():
    """A spacing-defined sample shows spacing, not the (unset) point count."""
    s = Sample(
        name="sys_1",
        raster_var_name="fcc",
        strategy="systematic",
        n_samples=None,
        spacing_m=1000,
    )
    box = _render(_project_with(s), "sys_1")
    labels, values = _labels(box), _values(box)
    assert t("tiles.sampling.spacing_label") in labels
    assert t("tiles.sampling.n_samples_label") not in labels
    # Integral spacing must render like the form ("1000"), not "1000.0" — the
    # form casts to int(round(...)), so the mirror must match it exactly.
    assert values[t("tiles.sampling.spacing_label")] == "1000"


def test_non_integral_spacing_keeps_its_fractional_part():
    """A non-integral spacing_m still renders its fraction (no silent rounding)."""
    s = Sample(
        name="sys_2",
        raster_var_name="fcc",
        strategy="systematic",
        n_samples=None,
        spacing_m=1250.5,
    )
    box = _render(_project_with(s), "sys_2")
    values = _values(box)
    assert values[t("tiles.sampling.spacing_label")] == "1250.5"


def test_missing_mask_renders_an_em_dash():
    """An unset mask_var_name shows as "—", not a blank or literal "None"."""
    s = Sample(name="rand_2", raster_var_name="fcc", strategy="random", n_samples=10)
    values = _values(_render(_project_with(s), "rand_2"))
    assert values[t("tiles.sampling.mask_variable_label")] == "—"


def test_unknown_key_renders_a_closed_dialog_with_no_fields():
    """The sample can be deleted while the dialog is open."""
    box = _render(_project_with(_stratified_deforisk()), "gone")
    assert not _find(box, vw.TextField)
    dialogs = _find(box, vw.Dialog)
    assert dialogs and dialogs[0].v_model is False


def test_none_key_renders_a_closed_dialog():
    """sample_key=None (nothing selected) renders a closed dialog."""
    box = _render(_project_with(_stratified_deforisk()), None)
    dialogs = _find(box, vw.Dialog)
    assert dialogs and dialogs[0].v_model is False


def test_dialog_is_read_only_by_construction():
    """No CreationDialog frame, no launch path — Close is the only action."""
    src = inspect.getsource(SampleDetailsDialog)
    # No creation frame, no submit path — Close is the only action.
    assert "CreationDialog" not in src and "launch" not in src
    assert "common.close" in src
    assert "details_title" in src
    # seed stays progressive-disclosed, like the form
    assert "ExpansionPanel" in src and "seed_label" in src
