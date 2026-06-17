"""Picklable spec-builder hooks for the execution follow-on (Phase 7).

Each *_spec(...) builder must return a frozen Pydantic model of plain
strings/numbers/recipes — pickle.dumps must round-trip and the payload must
contain no ee/estimator/live objects.
"""

import pickle
from types import SimpleNamespace

import pytest

from spatialrisk.document import CatalogueRecipe, GEESpec
from spatialrisk.session import MaterializeSpec
from spatialrisk.variables.models import DataType


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


def _make_gee_var(var_key="altitude"):
    recipe = CatalogueRecipe(
        source="catalogue",
        catalogue_key="altitude",
        params={},
        scale=30.0,
        crs="EPSG:4326",
        export_kind="raster",
    )
    return GEESpec(
        kind="gee",
        name=var_key,
        data_type=DataType.raster,
        recipe=recipe,
    )


class _FakeSession:
    """Minimal stand-in exposing only what the builders read."""

    def __init__(self, var=None, folder_path="/data/proj/raw"):
        self._var = var
        self.folders = SimpleNamespace(raw=SimpleNamespace(joinpath=lambda *_: None))
        self._folder_path = folder_path

    def get_variable(self, key, source=None):
        return self._var

    def _materialize_out_path(self, var_key):
        return f"{self._folder_path}/{var_key}.tif"


def test_materialize_spec_builds_from_gee_var():
    from spatialrisk.session import ProjectSession

    sess = _FakeSession(var=_make_gee_var("altitude"))
    spec = ProjectSession.materialize_spec(sess, "altitude")

    restored = _assert_picklable_pure(spec)
    assert restored.var_key == "altitude"
    assert restored.recipe.catalogue_key == "altitude"
    assert restored.scale == 30.0
    assert restored.crs == "EPSG:4326"
    assert restored.export_kind == "raster"
    assert restored.out_path == "/data/proj/raw/altitude.tif"


def test_materialize_spec_rejects_non_gee_var():
    from spatialrisk.session import ProjectSession
    from spatialrisk.document import LocalRasterSpec
    from spatialrisk.variables.models import RasterType

    local = LocalRasterSpec(
        kind="local_raster",
        name="altitude",
        path="/data/proj/raw/altitude.tif",
        raster_type=RasterType.continuous,
    )
    sess = _FakeSession(var=local)
    with pytest.raises(TypeError):
        ProjectSession.materialize_spec(sess, "altitude")


def test_supervised_fit_spec_is_self_contained_and_picklable():
    from spatialrisk.session import SupervisedFitSpec, FeatureMeta
    from spatialrisk.sampling import Sampling

    spec = SupervisedFitSpec(
        model_key="glm_calibration",
        model_type="glm",
        target_path="/data/proj/processed/forest_loss_2020.tif",
        feature_paths={
            "altitude": "/data/proj/processed/altitude.tif",
            "pa": "/data/proj/processed/pa.tif",
        },
        feature_meta=(
            FeatureMeta(name="altitude", raster_type="continuous", levels=None),
            FeatureMeta(name="pa", raster_type="categorical", levels=(0, 1)),
        ),
        formula="I(fcc) ~ scale(altitude) + C(pa, levels=[0, 1])",
        sampling=Sampling(strategy="random", n_samples=10000, seed=42),
        output_sample_path="/data/proj/glm/samples_glm_calibration.csv",
        parameters={"solver": "lbfgs", "max_iter": 1000},
        estimator_pickle="/data/proj/glm/glm_calibration_20260617.pickle",
    )

    restored = _assert_picklable_pure(spec)
    # carries raster paths + sampling, NOT just a CSV
    assert restored.target_path.endswith("forest_loss_2020.tif")
    assert restored.feature_paths["altitude"].endswith("altitude.tif")
    assert restored.sampling.n_samples == 10000
    assert restored.sampling.seed == 42
    assert restored.feature_meta[1].levels == (0, 1)
    assert restored.output_sample_path.endswith(".csv")
    # frozen
    import pytest

    with pytest.raises(Exception):
        spec.formula = "x ~ y"
