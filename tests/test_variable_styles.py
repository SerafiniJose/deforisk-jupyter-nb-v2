"""Local-raster variable styling.

A downloaded variable keeps the palette it had as a GEE layer (sourced from
PREDEFINED_CATALOGUE by name), rather than the old hardcoded grayscale.
Mirrors the GEE-side selection in ``_styled_layer``.
"""

from matplotlib.colors import Colormap

from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE
from gui.scripts.variable_styles import resolve_variable_style


def _local_raster(var_name, raster_type):
    """A stand-in LocalRasterVar carrying just ``name`` + ``raster_type``."""
    rt = type("RT", (), {"value": raster_type})()
    return type("LocalRasterVar", (), {"name": var_name, "raster_type": rt})()


def test_predefined_continuous_pins_catalogue_range():
    """Slope keeps its green->red ramp pinned to the catalogue min/max (0..60)."""
    style = resolve_variable_style(_local_raster("slope", "continuous"))

    assert isinstance(style["colormap"], Colormap)  # localtileserver needs the object
    assert style["vmin"] == 0 and style["vmax"] == 60
    # first palette colour (#1a9850 green) lands at vmin, last (#d73027 red) at vmax
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (26, 152, 80)
    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (215, 48, 39)


def test_predefined_continuous_without_range_auto_stretches():
    """Altitude carries a palette but no min/max -> auto-stretch (vmin/vmax None)."""
    style = resolve_variable_style(_local_raster("altitude", "continuous"))

    assert style["vmin"] is None and style["vmax"] is None
    # terrain ramp, not grayscale: first node #006633
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (0, 102, 51)


def test_predefined_binary_uses_catalogue_palette_pinned_0_1():
    """A predefined binary mask (rivers) -> white background, feature colour, [0,1]."""
    style = resolve_variable_style(_local_raster("rivers", "categorical"))

    assert style["vmin"] == 0 and style["vmax"] == 1
    first_hex = PREDEFINED_CATALOGUE["rivers"]["vis_params"]["palette"][0]  # ffffff
    assert first_hex == "ffffff"
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (255, 255, 255)


def test_non_catalogue_categorical_falls_back_to_black_white():
    """A custom (non-catalogue) categorical raster.

    Falls back to 0=black, 1=white, pinned [0,1] — matching ``_styled_layer``'s
    default, not a colourful catalogue palette.
    """
    style = resolve_variable_style(_local_raster("my_custom_mask", "categorical"))

    assert style["vmin"] == 0 and style["vmax"] == 1
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (0, 0, 0)
    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (255, 255, 255)


def test_non_catalogue_continuous_falls_back_to_grayscale():
    """A custom continuous raster keeps the old grayscale, auto-stretched."""
    style = resolve_variable_style(_local_raster("my_dem", "continuous"))

    assert style["vmin"] is None and style["vmax"] is None
    r, g, b = (round(x * 255) for x in style["colormap"](0.0)[:3])
    assert r == g == b  # grayscale


def test_subj_random_visualizer_uses_qualitative_colormap():
    """Subj (random_visualizer) has no local per-class equivalent.

    So it gets a qualitative colormap, auto-stretched (never grayscale).
    """
    style = resolve_variable_style(_local_raster("subj", "categorical"))

    assert style["vmin"] is None and style["vmax"] is None
    assert isinstance(style["colormap"], Colormap)


def _postprocess_raster(var_name, raster_type, tags=None, history=None):
    """A stand-in post-process LocalRasterVar (renamed, so never in the catalogue)."""
    rt = type("RT", (), {"value": raster_type})()
    return type(
        "LocalRasterVar",
        (),
        {
            "name": var_name,
            "raster_type": rt,
            "tags": tags or [],
            "processing_history": history or [],
        },
    )()


def test_postprocess_distance_wins_over_the_grayscale_fallback():
    """An `edge` output is continuous and not in the catalogue.

    It used to land on grayscale. It now gets the dist_edge.qml ramp, pinned
    to 30..1000 m.
    """
    style = resolve_variable_style(
        _postprocess_raster("forest_gfc_edge", "continuous", history=["edge"])
    )

    assert style["vmin"] == 30 and style["vmax"] == 1000
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (227, 26, 28)
    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (34, 139, 34)


def test_postprocess_dist_gets_the_same_distance_ramp():
    """A `dist` output gets the same distance ramp as `edge`."""
    style = resolve_variable_style(
        _postprocess_raster("roads_dist", "continuous", history=["dist"])
    )

    assert style["vmin"] == 30 and style["vmax"] == 1000
    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (34, 139, 34)


def test_postprocess_loss_wins_over_the_black_white_fallback():
    """A change mask is categorical and not in the catalogue.

    It used to land on black(0)/white(1). The event (1) is now red over an
    opaque grey stable class.
    """
    style = resolve_variable_style(
        _postprocess_raster(
            "loss_forest_2015_2020", "categorical", tags=["loss", "change", "2015_2020"]
        )
    )

    assert style["vmin"] == 0 and style["vmax"] == 1
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (217, 217, 217)
    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (227, 26, 28)


def test_postprocess_gain_paints_the_event_green():
    """A `gain` output paints the event (1) green instead of white."""
    style = resolve_variable_style(
        _postprocess_raster(
            "gain_forest_2015_2020", "categorical", tags=["gain", "change", "2015_2020"]
        )
    )

    assert style["vmin"] == 0 and style["vmax"] == 1
    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (34, 139, 34)


def test_legacy_postprocess_variable_classified_by_name_alone():
    """Variables saved before tags/processing_history existed still get the ramp."""
    style = resolve_variable_style(
        _postprocess_raster("loss_forest_2000_2010", "categorical")
    )

    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (227, 26, 28)


def test_catalogue_variable_is_unaffected_by_the_postprocess_branch():
    """Slope still resolves through PREDEFINED_CATALOGUE, exactly as before."""
    style = resolve_variable_style(_postprocess_raster("slope", "continuous"))

    assert style["vmin"] == 0 and style["vmax"] == 60
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (26, 152, 80)


def test_parameterised_predefined_keeps_catalogue_palette():
    """forest_gfc_tc30 must render with the forest palette.

    Not the grayscale / black-white fallback — its name carries a param
    suffix, so a bare PREDEFINED_CATALOGUE lookup misses it.
    """
    style = resolve_variable_style(_local_raster("forest_gfc_tc30", "categorical"))

    assert style["vmin"] == 0 and style["vmax"] == 1
    # catalogue palette: white background -> #2e7d32 forest green
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (255, 255, 255)
    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (46, 125, 50)


def test_unparameterised_forest_gfc_still_resolves():
    """Variables created before the threshold feature are named plain 'forest_gfc'.

    They must keep their palette.
    """
    style = resolve_variable_style(_local_raster("forest_gfc", "categorical"))

    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (46, 125, 50)


# --- render_kind reporting ----------------------------------------------------


def test_render_kind_for_a_postprocess_distance_raster():
    """A distance-classified raster reports render_kind postprocess_distance."""
    style = resolve_variable_style(
        _postprocess_raster("forest_gfc_edge", "continuous", history=["edge"])
    )
    assert style["render_kind"] == "postprocess_distance"


def test_render_kind_for_a_change_mask():
    """A change-classified raster reports render_kind postprocess_change."""
    style = resolve_variable_style(
        _postprocess_raster("loss_x", "categorical", tags=["loss"])
    )
    assert style["render_kind"] == "postprocess_change"


def test_render_kind_for_a_catalogue_palette_variable():
    """A catalogue variable with a palette reports render_kind catalogue_palette."""
    style = resolve_variable_style(_local_raster("slope", "continuous"))
    assert style["render_kind"] == "catalogue_palette"


def test_render_kind_for_a_random_visualizer_variable():
    """Subj (the random_visualizer catalogue entry) reports random_visualizer."""
    style = resolve_variable_style(_local_raster("subj", "categorical"))
    assert style["render_kind"] == "random_visualizer"


def test_render_kind_for_an_unknown_categorical_raster():
    """A non-catalogue categorical raster reports render_kind categorical_fallback."""
    style = resolve_variable_style(_local_raster("mystery", "categorical"))
    assert style["render_kind"] == "categorical_fallback"


def test_render_kind_for_an_unknown_continuous_raster():
    """A non-catalogue continuous raster reports render_kind continuous_fallback."""
    style = resolve_variable_style(_local_raster("mystery", "continuous"))
    assert style["render_kind"] == "continuous_fallback"


def test_existing_style_keys_are_unchanged():
    """render_kind is additive: colormap/vmin/vmax remain in the returned dict."""
    style = resolve_variable_style(_local_raster("slope", "continuous"))
    assert set(style) >= {"colormap", "vmin", "vmax", "render_kind"}
