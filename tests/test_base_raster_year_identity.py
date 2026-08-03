"""Regression: the base raster is identified by name AND year.

Raw variables are keyed ``{name}_{year}``, so a temporal layer (forest 2000,
forest 2010, …) contributes several entries that share a ``name``. The base
raster is a reprojected copy that preserves both ``name`` and ``year``, but
every consumer used to match it back to a raw variable by ``.name`` alone.
Setting one forest year as the base therefore tagged *every* forest year with
the "ref" chip, restored the wrong year in the Process tile's Select, and let
an edit/remove of any same-named year clear the base.
"""

import re
from pathlib import Path
from types import SimpleNamespace

import reacton
import solara

from gui.i18n import t

# Warm the translator before the first render (see test_model_form_dialog_render).
t("common.cancel")

from gui.scripts.summary_helpers import raw_variable_rows  # noqa: E402
from gui.scripts.variable_identity import is_base_raster  # noqa: E402
from gui.tile.process_tile import base_raster_key  # noqa: E402
from spatialrisk.project import Project  # noqa: E402
from spatialrisk.variables import LocalRasterVar  # noqa: E402
from spatialrisk.variables.models import DataType  # noqa: E402

Project._ensure_model_schemas()


def _raster(name, year=None):
    return LocalRasterVar.model_construct(
        name=name, year=year, data_type=DataType.raster, active=True
    )


def _forest_project(base_year=2010):
    """Two years of the same 'forest' layer, one of them the base raster."""
    p = Project(project_name="t")
    for year in (2000, 2010):
        var = _raster("forest", year=year)
        var.project = p
        p.raw_variables[f"forest_{year}"] = var
    # The base is a reprojected *copy*, not the raw object — it keeps name+year.
    p.base_raster = _raster("forest", year=base_year)
    p.base_raster.project = p
    return p


# --- identity helper --------------------------------------------------------


def test_is_base_raster_matches_same_name_and_year():
    """The year that was set as the base is the base."""
    p = _forest_project(base_year=2010)
    assert is_base_raster(p, p.raw_variables["forest_2010"]) is True


def test_is_base_raster_rejects_same_name_other_year():
    """The reported bug: forest 2000 is not the base just because 2010 is."""
    p = _forest_project(base_year=2010)
    assert is_base_raster(p, p.raw_variables["forest_2000"]) is False


def test_is_base_raster_matches_a_yearless_variable():
    """Layers without a year are keyed by bare name and must still match."""
    p = Project(project_name="t")
    p.raw_variables["altitude"] = _raster("altitude")
    p.base_raster = _raster("altitude")
    assert is_base_raster(p, p.raw_variables["altitude"]) is True


def test_is_base_raster_false_without_a_base():
    """No base set: nothing is tagged."""
    p = Project(project_name="t")
    p.raw_variables["altitude"] = _raster("altitude")
    assert is_base_raster(p, p.raw_variables["altitude"]) is False


# --- the "ref" chip on the Variables tile -----------------------------------


def _source_rows(project):
    """Row specs SourceVariableList hands to ProductTable."""
    import gui.widget.variable_list as mod

    seen = {}
    original = mod.ProductTable

    def _capture(**kw):
        seen["rows"] = kw["rows"]
        return original(**kw)

    mod.ProductTable = _capture
    try:
        reacton.render(
            mod.SourceVariableList(project=project, on_remove=lambda k: None)
        )
    finally:
        mod.ProductTable = original
    return {r["key"]: r for r in seen["rows"]}


def test_source_list_ref_chip_only_on_the_base_year():
    """The reported symptom: the chip must not spread to the layer's other years."""
    rows = _source_rows(solara.reactive(_forest_project(base_year=2010)))
    assert rows["forest_2010"]["cells"][0]["chips"], "base year lost its ref chip"
    assert rows["forest_2000"]["cells"][0]["chips"] == []


# --- the "ref" chip in the Summary tab --------------------------------------


def test_raw_variable_rows_base_badge_only_on_matching_year():
    """Same chip, same rule, in the read-only Summary popup."""
    _stats, rows = raw_variable_rows(_forest_project(base_year=2010))
    by_key = {r["key"]: r for r in rows}
    assert by_key["forest_2010"]["is_base"] is True
    assert by_key["forest_2000"]["is_base"] is False


def test_raw_variable_rows_tolerate_year_free_duck_types():
    """Summary rows are built from plain objects too — no attribute assumptions."""
    p = SimpleNamespace(
        raw_variables={"altitude": SimpleNamespace(name="altitude")},
        base_raster=SimpleNamespace(name="altitude"),
    )
    _stats, rows = raw_variable_rows(p)
    assert rows[0]["is_base"] is True


# --- the Process tile's Base raster Select ----------------------------------


def test_base_raster_key_picks_the_matching_year():
    """The Process tile's Select restores the year that actually backs the base."""
    assert base_raster_key(_forest_project(base_year=2010)) == "forest_2010"
    assert base_raster_key(_forest_project(base_year=2000)) == "forest_2000"


# --- no consumer may fall back to name-only matching ------------------------

_NAME_MATCH = re.compile(r"base_raster\.name\s*==|==\s*[\w.]*base_raster\.name")


def test_no_gui_site_matches_the_base_raster_by_name():
    """Guards the tile closures (edit/replace/remove) that reset the base."""
    offenders = [
        str(path.relative_to(Path(__file__).resolve().parents[1]))
        for path in Path(__file__).resolve().parents[1].joinpath("gui").rglob("*.py")
        if _NAME_MATCH.search(path.read_text())
    ]
    assert offenders == [], f"compare base rasters by name+year: {offenders}"
