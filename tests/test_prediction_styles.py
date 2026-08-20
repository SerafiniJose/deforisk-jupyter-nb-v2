"""Prediction palette resolution: each model family maps to the colour ramp and
value range translated verbatim from qgis_layer_style/prob*.qml, with an
unknown-family fallback to the FAR (prob.qml) palette."""

from gui.scripts.prediction_styles import resolve_prediction_style


def test_far_families_use_prob_qml_range_and_colours():
    """glm/rf/icar predictions use prob.qml: 1->green .. 65535->black."""
    for model_key in ("glm_m1", "rf_v2", "icar_a"):
        style = resolve_prediction_style(model_key)
        assert style["vmin"] == 1
        assert style["vmax"] == 65535
        assert style["nodata"] == 0
        cmap = style["colormap"]
        assert tuple(round(x * 255) for x in cmap(0.0)[:3]) == (34, 139, 34)  # #228b22 green at vmin
        assert tuple(round(x * 255) for x in cmap(1.0)[:3]) == (0, 0, 0)     # black at vmax
        assert cmap(0.0)[3] == 1.0                                              # opaque alpha


def test_jnr_uses_prob_bm_qml_range():
    """jnr (benchmark) uses prob_bm.qml: 1001..30999, first node #196e19."""
    style = resolve_prediction_style("jnr")
    assert style["vmin"] == 1001
    assert style["vmax"] == 30999
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (25, 110, 25)  # #196e19
    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (0, 0, 0)


def test_mw_uses_prob_mw_qml_range():
    """mw (moving window) uses prob_mw.qml: 1..65535, first node #196e19."""
    style = resolve_prediction_style("mw_5")
    assert style["vmin"] == 1
    assert style["vmax"] == 65535
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (25, 110, 25)
    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (0, 0, 0)


def test_unknown_family_falls_back_to_far():
    """An unmapped family must not raise — it uses the FAR (prob.qml) palette."""
    style = resolve_prediction_style("weird_key")
    assert style["vmin"] == 1 and style["vmax"] == 65535
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (34, 139, 34)


def test_colormap_is_matplotlib_object_with_256_levels():
    from matplotlib.colors import Colormap

    style = resolve_prediction_style("glm_m1")
    cmap = style["colormap"]
    assert isinstance(cmap, Colormap)   # localtileserver needs the object, not a dict
    assert cmap.N == 256
    assert cmap(0.0)[3] == 1.0           # opaque
