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
