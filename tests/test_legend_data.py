"""Legend spec model and its conversion to pysepal's LegendData."""

import matplotlib

from gui.scripts.legend_data import (
    Label,
    LegendSpec,
    gradient_colors,
    resolve_label,
    to_legend_data,
)


def _t(key, **fmt):
    """Stand-in translator: echoes the key plus any format args."""
    return f"<{key}>" + ("".join(f"|{k}={v}" for k, v in sorted(fmt.items())))


def test_resolve_label_prefers_key_over_literal():
    """A key present on the Label wins over any literal fallback."""
    assert resolve_label(Label(key="legend.risk.low"), _t) == "<legend.risk.low>"


def test_resolve_label_falls_back_to_literal():
    """With no key, the literal text passes through untranslated."""
    assert resolve_label(Label(literal="30"), _t) == "30"


def test_resolve_label_passes_format_args():
    """Label.args is expanded into t(key, **args)."""
    label = Label(key="legend.unit.m_value", args=(("value", "30"),))
    assert resolve_label(label, _t) == "<legend.unit.m_value>|value=30"


def test_gradient_colors_returns_n_hex_stops_ending_at_colormap_ends():
    """gradient_colors samples n hex stops, anchored at the colormap's ends."""
    cmap = matplotlib.colormaps["viridis"]
    colors = gradient_colors(cmap)
    assert len(colors) == 256
    assert all(c.startswith("#") and len(c) == 7 for c in colors)
    assert colors[0] == matplotlib.colors.to_hex(cmap(0.0))
    assert colors[-1] == matplotlib.colors.to_hex(cmap(1.0))


def test_to_legend_data_gradient_carries_colors_labels_and_title():
    """A gradient spec becomes a single GradientEntry with no discrete items."""
    spec = LegendSpec(
        kind="gradient",
        title=Label(key="legend.prediction.title"),
        colors=("#000000", "#ffffff"),
        labels=(Label(key="legend.risk.low"), Label(key="legend.risk.high")),
    )
    data = to_legend_data(spec, _t)
    assert data.items == []
    assert len(data.gradients) == 1
    gradient = data.gradients[0]
    assert gradient.colors == ["#000000", "#ffffff"]
    assert gradient.labels == ["<legend.risk.low>", "<legend.risk.high>"]
    assert gradient.title == "<legend.prediction.title>"


def test_to_legend_data_chips_prepend_a_chipless_title_row():
    """A chips spec's title becomes a leading DiscreteEntry with an empty color."""
    spec = LegendSpec(
        kind="chips",
        title=Label(literal="forest_gfc"),
        colors=("#d9d9d9", "#e31a1c"),
        labels=(Label(key="legend.class.stable"), Label(key="legend.class.loss")),
    )
    data = to_legend_data(spec, _t)
    assert data.gradients == []
    assert [(i.label, i.color) for i in data.items] == [
        ("forest_gfc", ""),
        ("<legend.class.stable>", "#d9d9d9"),
        ("<legend.class.loss>", "#e31a1c"),
    ]


def test_to_legend_data_note_renders_two_chipless_rows():
    """A note spec renders its title and label as chipless text rows."""
    spec = LegendSpec(
        kind="note",
        title=Label(literal="subj"),
        labels=(Label(key="legend.note.random_classes"),),
    )
    data = to_legend_data(spec, _t)
    assert [(i.label, i.color) for i in data.items] == [
        ("subj", ""),
        ("<legend.note.random_classes>", ""),
    ]


def test_to_legend_data_builds_fresh_objects_each_call():
    """Two calls with the same spec return distinct, unshared LegendData."""
    spec = LegendSpec(
        kind="gradient",
        title=Label(literal="x"),
        colors=("#000000", "#ffffff"),
        labels=(Label(literal="a"), Label(literal="b")),
    )
    first = to_legend_data(spec, _t)
    second = to_legend_data(spec, _t)
    assert first is not second
    assert first.gradients[0] is not second.gradients[0]
    assert first.gradients[0].colors is not second.gradients[0].colors


def test_prediction_spec_is_a_gradient_with_semantic_endpoints():
    """prediction_spec builds a 256-stop gradient labelled Low/High risk."""
    from gui.scripts.legend_data import prediction_spec

    spec = prediction_spec("rf_2020")
    assert spec.kind == "gradient"
    assert spec.title.key == "legend.prediction.title"
    assert len(spec.colors) == 256
    assert [label.key for label in spec.labels] == [
        "legend.risk.low",
        "legend.risk.high",
    ]


def test_prediction_spec_labels_are_identical_across_families():
    """Every model family shares the same Low/High risk label keys."""
    from gui.scripts.legend_data import prediction_spec

    labels = {
        family: tuple(label.key for label in prediction_spec(f"{family}_x").labels)
        for family in ("rf", "jnr", "mw")
    }
    assert len(set(labels.values())) == 1


def test_prediction_spec_colors_differ_between_families():
    """Different model families render distinct colour ramps."""
    from gui.scripts.legend_data import prediction_spec

    assert prediction_spec("rf_x").colors != prediction_spec("mw_x").colors


def test_prediction_spec_honours_a_display_palette_override():
    """An explicit display_palette overrides the model-family palette."""
    from gui.scripts.legend_data import prediction_spec

    imported = prediction_spec("imported_thing", display_palette="jnr")
    assert imported.colors == prediction_spec("jnr_x").colors


def test_density_spec_labels_carry_the_real_range():
    """density_spec labels its endpoints with the raster's own min/max."""
    from gui.scripts.legend_data import density_spec

    spec = density_spec(0.0125, 4.5)
    assert spec.kind == "gradient"
    assert spec.title.key == "legend.density.title"
    assert [label.literal for label in spec.labels] == ["0.0125", "4.5"]


def test_density_spec_collapses_a_degenerate_range_to_low_high():
    """A degenerate (vmin == vmax) range falls back to Low/High labels."""
    from gui.scripts.legend_data import density_spec

    spec = density_spec(2.0, 2.0)
    assert [label.key for label in spec.labels] == [
        "legend.range.low",
        "legend.range.high",
    ]


def test_format_number_keeps_three_significant_digits():
    """format_number keeps 3 significant digits and drops trailing zeros."""
    from gui.scripts.legend_data import format_number

    assert format_number(0.012456) == "0.0125"
    assert format_number(1234.0) == "1234"
    assert format_number(30) == "30"


def test_format_number_returns_empty_string_for_non_finite_values():
    """NaN and +-Inf don't crash format_number; they render as empty text."""
    import math

    from gui.scripts.legend_data import format_number

    assert format_number(float("nan")) == ""
    assert format_number(float("inf")) == ""
    assert format_number(float("-inf")) == ""
    assert math.isnan(float("nan"))  # sanity: the literal itself is non-finite


def test_value_labels_falls_back_to_low_high_for_non_finite_vmin_vmax():
    """A non-finite vmin/vmax degrades to the Low/High fallback, not a crash."""
    from gui.scripts.legend_data import _value_labels

    for vmin, vmax in [
        (float("nan"), 10.0),
        (0.0, float("nan")),
        (float("-inf"), 10.0),
        (0.0, float("inf")),
        (float("-inf"), float("inf")),
    ]:
        labels = _value_labels(vmin, vmax, "legend.unit.plain_value")
        assert [label.key for label in labels] == [
            "legend.range.low",
            "legend.range.high",
        ]


class _Var:
    """Minimal stand-in for a variable object."""

    def __init__(self, name="", raster_type="continuous", tags=None, history=None):
        """Store the variable attributes the legend builders read."""
        self.name = name
        self.raster_type = raster_type
        self.tags = tags or []
        self.processing_history = history or []


def test_variable_spec_from_vis_renders_a_categorical_mask_as_chips():
    """A categorical GEE vis dict becomes chips with default class labels."""
    from gui.scripts.legend_data import Label, variable_spec_from_vis

    vis = {"palette": ["ffffff", "4caf50"], "min": 0, "max": 1}
    var = _Var(name="protected_area", raster_type="categorical")
    spec = variable_spec_from_vis(
        vis, "catalogue_palette", var, title=Label(literal="protected_area")
    )
    assert spec.kind == "chips"
    assert spec.colors == ("#ffffff", "#4caf50")
    assert [label.key for label in spec.labels] == [
        "legend.class.absent",
        "legend.class.present",
    ]


def test_variable_spec_uses_catalogue_class_key_overrides():
    """A catalogue entry's legend_class_keys overrides the default chip labels."""
    from gui.scripts.legend_data import Label, variable_spec_from_vis

    vis = {"palette": ["ffffff", "2e7d32"], "min": 0, "max": 1}
    spec = variable_spec_from_vis(
        vis,
        "catalogue_palette",
        _Var(name="forest_gfc", raster_type="categorical"),
        title=Label(literal="forest_gfc"),
    )
    assert [label.key for label in spec.labels] == [
        "legend.class.non_forest",
        "legend.class.forest",
    ]


def test_variable_spec_pins_numeric_endpoints_with_a_unit():
    """A continuous catalogue palette with a unit_key labels real endpoints."""
    from gui.scripts.legend_data import Label, variable_spec_from_vis

    vis = {"palette": ["1a9850", "ffffbf", "d73027"], "min": 0, "max": 60}
    spec = variable_spec_from_vis(
        vis, "catalogue_palette", _Var(name="slope"), title=Label(literal="slope")
    )
    assert spec.kind == "gradient"
    assert len(spec.colors) == 256
    assert [(label.key, dict(label.args)) for label in spec.labels] == [
        ("legend.unit.deg_value", {"value": "0"}),
        ("legend.unit.deg_value", {"value": "60"}),
    ]


def test_variable_spec_without_a_unit_uses_the_plain_value_key():
    """A layer with no unit_key falls back to the plain value label."""
    from gui.scripts.legend_data import Label, variable_spec

    spec = variable_spec(
        colors=("#000000", "#ffffff"),
        vmin=0,
        vmax=5,
        render_kind="continuous_fallback",
        var=_Var(name="mystery"),
        title=Label(literal="mystery"),
    )
    assert [(label.key, dict(label.args)) for label in spec.labels] == [
        ("legend.unit.plain_value", {"value": "0"}),
        ("legend.unit.plain_value", {"value": "5"}),
    ]


def test_variable_spec_stretched_range_falls_back_to_low_high():
    """An unpinned (None) value range falls back to Low/High labels."""
    from gui.scripts.legend_data import Label, variable_spec

    spec = variable_spec(
        colors=("#000000", "#ffffff"),
        vmin=None,
        vmax=None,
        render_kind="continuous_fallback",
        var=_Var(name="mystery"),
        title=Label(literal="mystery"),
    )
    assert spec.kind == "gradient"
    assert [label.key for label in spec.labels] == [
        "legend.range.low",
        "legend.range.high",
    ]


def test_variable_spec_random_visualizer_is_a_note():
    """A random_visualizer render_kind renders as a colourless note."""
    from gui.scripts.legend_data import Label, variable_spec

    spec = variable_spec(
        colors=(),
        vmin=None,
        vmax=None,
        render_kind="random_visualizer",
        var=_Var(name="subj", raster_type="categorical"),
        title=Label(literal="subj"),
    )
    assert spec.kind == "note"
    assert spec.colors == ()
    assert [label.key for label in spec.labels] == ["legend.note.random_classes"]


def test_variable_spec_from_style_renders_a_change_mask_as_chips():
    """A post-process change-mask style renders as stable/loss chips."""
    from gui.scripts.legend_data import Label, variable_spec_from_style
    from gui.scripts.variable_styles import resolve_variable_style

    var = _Var(name="loss_forest_2015_2020", tags=["loss"], raster_type="categorical")
    spec = variable_spec_from_style(
        resolve_variable_style(var), var, title=Label(literal="loss_forest_2015_2020")
    )
    assert spec.kind == "chips"
    assert spec.colors == ("#d9d9d9", "#e31a1c")
    assert [label.key for label in spec.labels] == [
        "legend.class.stable",
        "legend.class.loss",
    ]


def test_variable_spec_from_style_renders_a_distance_raster_in_metres():
    """A post-process distance style labels its endpoints in metres."""
    from gui.scripts.legend_data import Label, variable_spec_from_style
    from gui.scripts.variable_styles import resolve_variable_style

    var = _Var(name="forest_gfc_edge", history=["edge"])
    spec = variable_spec_from_style(
        resolve_variable_style(var), var, title=Label(literal="forest_gfc_edge")
    )
    assert spec.kind == "gradient"
    assert [(label.key, dict(label.args)) for label in spec.labels] == [
        ("legend.unit.m_value", {"value": "30"}),
        ("legend.unit.m_value", {"value": "1000"}),
    ]
