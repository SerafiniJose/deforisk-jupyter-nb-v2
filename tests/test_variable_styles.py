"""Local-raster variable styling: a downloaded variable keeps the palette it had
as a GEE layer (sourced from PREDEFINED_CATALOGUE by name), rather than the old
hardcoded grayscale. Mirrors the GEE-side selection in ``_styled_layer``."""

from matplotlib.colors import Colormap

from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE
from gui.scripts.variable_styles import resolve_variable_style


def _local_raster(var_name, raster_type):
    """A stand-in LocalRasterVar carrying just ``name`` + ``raster_type``."""
    rt = type("RT", (), {"value": raster_type})()
    return type("LocalRasterVar", (), {"name": var_name, "raster_type": rt})()


def test_predefined_continuous_pins_catalogue_range():
    """slope keeps its green->red ramp pinned to the catalogue min/max (0..60)."""
    style = resolve_variable_style(_local_raster("slope", "continuous"))

    assert isinstance(style["colormap"], Colormap)  # localtileserver needs the object
    assert style["vmin"] == 0 and style["vmax"] == 60
    # first palette colour (#1a9850 green) lands at vmin, last (#d73027 red) at vmax
    assert tuple(round(x * 255) for x in style["colormap"](0.0)[:3]) == (26, 152, 80)
    assert tuple(round(x * 255) for x in style["colormap"](1.0)[:3]) == (215, 48, 39)


def test_predefined_continuous_without_range_auto_stretches():
    """altitude carries a palette but no min/max -> auto-stretch (vmin/vmax None)."""
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
    """A custom (non-catalogue) categorical raster -> 0=black, 1=white, pinned [0,1]
    — matching ``_styled_layer``'s default, not a colourful catalogue palette."""
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
    """subj (random_visualizer) has no local per-class equivalent -> a qualitative
    colormap, auto-stretched (never grayscale)."""
    style = resolve_variable_style(_local_raster("subj", "categorical"))

    assert style["vmin"] is None and style["vmax"] is None
    assert isinstance(style["colormap"], Colormap)
