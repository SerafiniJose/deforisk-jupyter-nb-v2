"""Picklable spec-builder hooks for the execution follow-on (Phase 7).

Each *_spec(...) builder must return a frozen Pydantic model of plain
strings/numbers/recipes — pickle.dumps must round-trip and the payload must
contain no ee/estimator/live objects.
"""

import pickle

import pytest

from spatialrisk.document import CatalogueRecipe
from spatialrisk.session import MaterializeSpec


def _assert_picklable_pure(spec):
    """Round-trip through pickle and assert no banned live objects appear."""
    blob = pickle.dumps(spec)
    restored = pickle.loads(blob)
    assert restored == spec
    # No module named `ee` may have been imported to (de)serialize the spec.
    import sys

    text = blob  # bytes
    for banned in (b"ee.image", b"ee.Image", b"sklearn", b"RandomForest"):
        assert banned not in text, f"banned token {banned!r} pickled into spec"
    return restored


def test_materialize_spec_is_frozen_and_picklable():
    recipe = CatalogueRecipe(
        source="catalogue",
        catalogue_key="altitude",
        params={"scale": 30},
        export_kind="raster",
    )
    spec = MaterializeSpec(
        var_key="altitude",
        recipe=recipe,
        out_path="/data/proj/raw/altitude.tif",
        scale=30.0,
        crs="EPSG:4326",
        export_kind="raster",
        vector_selectors=None,
    )
    restored = _assert_picklable_pure(spec)
    assert restored.var_key == "altitude"
    assert restored.out_path == "/data/proj/raw/altitude.tif"
    assert restored.recipe.catalogue_key == "altitude"
    # frozen
    with pytest.raises(Exception):
        spec.out_path = "/elsewhere.tif"
