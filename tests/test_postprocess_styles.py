"""Default map styling for post-process outputs (edge/dist/loss/gain).

These variables are renamed by the post-process step (`forest_gfc_edge`,
`loss_forest_2015_2020`), so they never hit PREDEFINED_CATALOGUE and used to fall
through to grayscale / black-white. Colours come from qgis_layer_style/dist_edge.qml
and the 1=event convention of `generate_change_var`.
"""

from matplotlib.colors import Colormap

from gui.scripts.postprocess_styles import (
    classify_postprocess,
    resolve_postprocess_style,
)


def _var(name="v", tags=None, history=None):
    """A stand-in LocalRasterVar carrying only what the classifier reads."""
    return type(
        "LocalRasterVar",
        (),
        {"name": name, "tags": tags or [], "processing_history": history or []},
    )()


def _rgb(colormap, x):
    """The colormap's colour at normalised position x, as a 0-255 RGB tuple."""
    return tuple(round(c * 255) for c in colormap(x)[:3])


def _assert_close(actual, expected, tol=8):
    """Compare RGB tuples with a tolerance.

    A 256-level colormap quantizes a lookup into bins, so sampling at an *interior*
    stop's normalised position lands in the nearest bin rather than exactly on the
    stop — e.g. the 100 m orange node sits at (100-30)/970 = 0.0722, which quantizes
    to 0.0706 and returns ~(254,161,1) instead of exactly (255,165,0). The endpoints
    (0.0 and 1.0) *are* exact and are asserted exactly. This tolerance covers the
    quantization error, which is far smaller than the gap between any two stops.
    """
    assert all(abs(a - e) <= tol for a, e in zip(actual, expected)), (actual, expected)


# --- classification ---------------------------------------------------------

def test_classifies_change_layer_by_tag():
    """generate_change_var tags its output [op, "change", years]."""
    assert classify_postprocess(_var(tags=["loss", "change", "2015_2020"])) == "loss"
    assert classify_postprocess(_var(tags=["gain", "change", "2015_2020"])) == "gain"


def test_classifies_edge_and_dist_by_processing_history():
    """_create_post_var appends the step to processing_history."""
    assert classify_postprocess(_var(history=["edge"])) == "distance"
    assert classify_postprocess(_var(history=["dist"])) == "distance"


def test_classifies_legacy_variables_by_name_alone():
    """Variables saved before tags/processing_history existed still classify."""
    assert classify_postprocess(_var(name="loss_forest_2000_2010")) == "loss"
    assert classify_postprocess(_var(name="gain_forest_2000_2010")) == "gain"
    assert classify_postprocess(_var(name="forest_gfc_edge")) == "distance"
    assert classify_postprocess(_var(name="roads_dist")) == "distance"


def test_non_postprocess_variable_is_unclassified():
    """Anything else is not our business -> None (caller keeps its own default)."""
    assert classify_postprocess(_var(name="slope")) is None
    assert classify_postprocess(_var(name="my_dem")) is None
    assert classify_postprocess(_var(name="forest_gfc", tags=["forest"])) is None


def test_classifier_tolerates_missing_attributes():
    """Legacy/stub variables may carry neither tags nor processing_history."""
    bare = type("Bare", (), {"name": "my_dem"})()
    assert classify_postprocess(bare) is None

    bare_edge = type("Bare", (), {"name": "forest_edge"})()
    assert classify_postprocess(bare_edge) == "distance"


def test_classifier_tolerates_a_variable_with_no_name():
    nameless = type("Nameless", (), {})()
    assert classify_postprocess(nameless) is None


# --- resolution -------------------------------------------------------------

def test_distance_style_pins_the_qgis_range_and_stops():
    """dist_edge.qml: interpolated, 30..1000 m, red at the edge -> green inland."""
    style = resolve_postprocess_style(_var(history=["edge"]))

    assert isinstance(style["colormap"], Colormap)  # localtileserver needs the object
    assert style["vmin"] == 30 and style["vmax"] == 1000
    # distance_to_edge_gdal_no_mask (spatialrisk/processing.py) passes NODATA=0 to
    # gdal.ComputeProximity but then declares SetNoDataValue(4294967295) -- a tag that
    # matches nothing in the file. 0 is the real fill value; the style must say so
    # rather than trust the file's (wrong) tag. Accepted side effect: genuine 0-metre
    # pixels (the feature itself -- rivers in rivers_dist, non-forest in forest_edge)
    # also render transparent.
    assert style["nodata"] == 0

    cmap = style["colormap"]
    span = 1000 - 30
    # Endpoints are exact; interior stops carry the 256-bin quantization error.
    assert _rgb(cmap, 0.0) == (227, 26, 28)  # 30 m   -> #e31a1c
    _assert_close(_rgb(cmap, (100 - 30) / span), (255, 165, 0))  # 100 m -> #ffa500
    _assert_close(_rgb(cmap, (500 - 30) / span), (255, 255, 178))  # 500 m -> #ffffb2
    assert _rgb(cmap, 1.0) == (34, 139, 34)  # 1000 m -> #228b22


def test_loss_style_paints_the_event_red_over_an_opaque_stable_class():
    """1 = event (deforestation) -> red; 0 = stable -> neutral grey."""
    style = resolve_postprocess_style(_var(tags=["loss", "change", "2015_2020"]))

    assert style["vmin"] == 0 and style["vmax"] == 1
    assert _rgb(style["colormap"], 0.0) == (217, 217, 217)  # #d9d9d9
    assert _rgb(style["colormap"], 1.0) == (227, 26, 28)  # #e31a1c
    assert style["nodata"] == 255  # generate_change_var writes 255 = nodata


def test_gain_style_paints_the_event_green():
    style = resolve_postprocess_style(_var(tags=["gain", "change", "2015_2020"]))

    assert style["vmin"] == 0 and style["vmax"] == 1
    assert _rgb(style["colormap"], 0.0) == (217, 217, 217)  # #d9d9d9
    assert _rgb(style["colormap"], 1.0) == (34, 139, 34)  # #228b22
    assert style["nodata"] == 255  # generate_change_var writes 255 = nodata


def test_resolve_returns_none_for_a_non_postprocess_variable():
    """The caller must be able to fall through to its own default."""
    assert resolve_postprocess_style(_var(name="slope")) is None
