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


def test_icar_fit_spec_adds_target_raster_csize_rho_path():
    from spatialrisk.session import ICARFitSpec, FeatureMeta
    from spatialrisk.sampling import Sampling

    spec = ICARFitSpec(
        model_key="icar_calibration",
        model_type="icar",
        target_path="/data/proj/processed/forest_loss_2020.tif",
        feature_paths={"altitude": "/data/proj/processed/altitude.tif"},
        feature_meta=(FeatureMeta(name="altitude", raster_type="continuous"),),
        formula="I(fcc) ~ scale(altitude) + cell",
        sampling=Sampling(strategy="random", n_samples=10000),
        output_sample_path="/data/proj/icar/samples.csv",
        target_raster="/data/proj/processed/forest_loss_2020.tif",
        csize=10.0,
        mcmc=4000,
        burnin=4000,
        thin=1,
        prior_vrho=-1.0,
        csize_interpolate=0.1,
        rho_path="/data/proj/icar/rho_calibration.tif",
        estimator_pickle="/data/proj/icar/icar_calibration.pickle",
    )
    restored = _assert_picklable_pure(spec)
    assert restored.target_raster.endswith("forest_loss_2020.tif")
    assert restored.csize == 10.0
    assert restored.rho_path.endswith("rho_calibration.tif")


def test_jnr_fit_spec_carries_dist_params_and_paths():
    from spatialrisk.session import JNRFitSpec

    spec = JNRFitSpec(
        model_key="jnr_calibration",
        model_type="jnr",
        defor_file="/data/proj/processed/defor_2020.tif",
        forest_edge_file="/data/proj/processed/forest_edge.tif",
        period="calibration",
        defor_threshold=99.5,
        max_dist=5000,
        blk_rows=128,
        out_root="/data/proj/rmj_bm",
    )
    restored = _assert_picklable_pure(spec)
    assert restored.defor_file.endswith("defor_2020.tif")
    assert restored.forest_edge_file.endswith("forest_edge.tif")
    assert restored.defor_threshold == 99.5
    assert restored.max_dist == 5000


def test_mw_fit_spec_carries_win_sizes_and_time_interval():
    from spatialrisk.session import MWFitSpec

    spec = MWFitSpec(
        model_key="mw_calibration",
        model_type="mw",
        defor_file="/data/proj/processed/defor_2020.tif",
        forest_edge_file="/data/proj/processed/forest_edge.tif",
        forest_file="/data/proj/processed/forest_2015.tif",
        period="calibration",
        win_sizes=(5, 11, 21),
        time_interval=5,
        defor_threshold=99.5,
        blk_rows=256,
        rescale_max_val=65535,
        out_root="/data/proj/rmj_mw",
    )
    restored = _assert_picklable_pure(spec)
    assert restored.win_sizes == (5, 11, 21)
    assert restored.time_interval == 5
    assert restored.forest_file.endswith("forest_2015.tif")


from spatialrisk.document import GLMSpec, DatasetSpec, VariableId, LocalRasterSpec
from spatialrisk.variables.models import RasterType
from spatialrisk.sampling import Sampling


def _local_raster(name, path, raster_type=RasterType.continuous):
    return LocalRasterSpec(
        kind="local_raster", name=name, path=path, raster_type=raster_type
    )


class _FitFakeSession:
    """Stand-in exposing models/datasets registries + variable resolution."""

    def __init__(self):
        self.sampling = Sampling(strategy="random", n_samples=10000, seed=7)
        self.glm = GLMSpec(
            model_type="glm",
            name="calibration",
            project_name="p",
            dataset_name="calibration_2020",
            target_name="forest_loss_2020",
            feature_names=("altitude", "pa"),
            year=2020,
            formula="I(fcc) ~ scale(altitude) + C(pa, levels=[0, 1])",
            parameters={"solver": "lbfgs", "max_iter": 1000},
            sampling=self.sampling,
            samples_path=None,
            trained=False,
        )
        self.dataset = DatasetSpec(
            name="calibration_2020",
            year=2020,
            target_ref=VariableId(source="processed", name="forest_loss_2020", year=2020),
            feature_refs=(
                VariableId(source="processed", name="altitude"),
                VariableId(source="processed", name="pa"),
            ),
            sampling=self.sampling,
        )
        self._vars = {
            "forest_loss_2020": _local_raster(
                "forest_loss_2020", "/data/proj/processed/forest_loss_2020.tif"
            ),
            "altitude": _local_raster("altitude", "/data/proj/processed/altitude.tif"),
            "pa": _local_raster(
                "pa", "/data/proj/processed/pa.tif", RasterType.categorical
            ),
        }
        self._doc = type("Doc", (), {"models": {"glm_calibration": self.glm},
                                     "datasets": {"calibration_2020": self.dataset}})()

    def get_variable(self, ref, source=None):
        name = ref.name if isinstance(ref, VariableId) else ref
        return self._vars[name]

    def _categorical_levels(self, var):
        # avoid touching real rasters in the test
        return (0, 1) if var.raster_type == RasterType.categorical else None

    def _fit_sample_out_path(self, model_key):
        return f"/data/proj/glm/samples_{model_key}.csv"


def test_fit_spec_glm_is_self_contained():
    from spatialrisk.session import ProjectSession, SupervisedFitSpec

    sess = _FitFakeSession()
    spec = ProjectSession.fit_spec(sess, "glm_calibration")

    restored = _assert_picklable_pure(spec)
    assert isinstance(restored, SupervisedFitSpec)
    assert restored.model_type == "glm"
    assert restored.target_path == "/data/proj/processed/forest_loss_2020.tif"
    assert restored.feature_paths == {
        "altitude": "/data/proj/processed/altitude.tif",
        "pa": "/data/proj/processed/pa.tif",
    }
    # categorical metadata present for patsy formula generation in-worker
    metas = {m.name: m for m in restored.feature_meta}
    assert metas["pa"].raster_type == "categorical"
    assert metas["pa"].levels == (0, 1)
    assert metas["altitude"].raster_type == "continuous"
    # sampling carried, not a pre-built CSV
    assert restored.sampling.seed == 7
    assert restored.output_sample_path.endswith("samples_glm_calibration.csv")
    assert restored.formula.startswith("I(fcc)")
