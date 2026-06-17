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

    def get_variable(self, ref, year=None, source=None):
        name = ref.name if isinstance(ref, VariableId) else ref
        return self._vars[name]

    def _categorical_levels(self, var):
        # avoid touching real rasters in the test
        return (0, 1) if var.raster_type == RasterType.categorical else None

    def _fit_sample_out_path(self, model_key):
        return f"/data/proj/glm/samples_{model_key}.csv"

    def _resolve_target_path(self, dataset):
        from spatialrisk.session import ProjectSession
        return ProjectSession._resolve_target_path(self, dataset)

    def _resolve_feature_paths(self, dataset):
        from spatialrisk.session import ProjectSession
        return ProjectSession._resolve_feature_paths(self, dataset)

    def _resolve_feature_meta(self, dataset):
        from spatialrisk.session import ProjectSession
        return ProjectSession._resolve_feature_meta(self, dataset)


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


from spatialrisk.document import ICARSpec, JNRSpec, MWSpec


class _FitFakeSessionMulti(_FitFakeSession):
    def __init__(self):
        super().__init__()
        self.icar = ICARSpec(
            model_type="icar",
            name="calibration",
            project_name="p",
            dataset_name="calibration_2020",
            target_name="forest_loss_2020",
            feature_names=("altitude",),
            year=2020,
            formula="I(fcc) ~ scale(altitude)",
            parameters={"csize": 10.0, "mcmc": 4000, "burnin": 4000},
            sampling=self.sampling,
            samples_path=None,
            trained=False,
        )
        self.jnr = JNRSpec(
            model_type="jnr",
            name="calibration",
            project_name="p",
            dataset_name="calibration_2020",
            target_name="defor_2020",
            feature_names=("forest_edge",),
            year=2020,
            formula=None,
            parameters={"defor_threshold": 99.5, "max_dist": 5000, "blk_rows": 128},
            sampling=None,
            samples_path=None,
            trained=False,
        )
        self.mw = MWSpec(
            model_type="mw",
            name="calibration",
            project_name="p",
            dataset_name="calibration_2020",
            target_name="defor_2020",
            feature_names=("forest_edge", "forest_2015"),
            year=2020,
            formula=None,
            parameters={
                "win_size_list": [5, 11, 21],
                "time_interval": 5,
                "defor_threshold": 99.5,
                "blk_rows": 256,
            },
            sampling=None,
            samples_path=None,
            trained=False,
        )
        # JNR/MW datasets reference defor/forest_edge/forest features
        self.jnr_dataset = DatasetSpec(
            name="jnr_calibration_2020",
            year=2020,
            target_ref=VariableId(source="processed", name="defor_2020", year=2020),
            feature_refs=(
                VariableId(source="processed", name="forest_edge"),
                VariableId(source="processed", name="forest_2015"),
            ),
        )
        self.icar.__dict__  # touch to keep linter calm
        self._doc.models.update(
            {"icar_calibration": self.icar, "jnr_calibration": self.jnr,
             "mw_calibration": self.mw}
        )
        self._doc.datasets["jnr_calibration_2020"] = self.jnr_dataset
        # extra vars for JNR/MW
        self._vars.update({
            "defor_2020": _local_raster("defor_2020", "/data/proj/processed/defor_2020.tif"),
            "forest_edge": _local_raster("forest_edge", "/data/proj/processed/forest_edge.tif"),
            "forest_2015": _local_raster("forest_2015", "/data/proj/processed/forest_2015.tif"),
        })
        # point jnr/mw models at the jnr dataset
        object.__setattr__(self.jnr, "dataset_name", "jnr_calibration_2020") \
            if hasattr(self.jnr, "__dict__") else None

    def _fit_out_root(self, model_key, model_type):
        roots = {"icar": "/data/proj/icar", "jnr": "/data/proj/rmj_bm",
                 "mw": "/data/proj/rmj_mw"}
        return roots[model_type]

    def _fit_rho_path(self, model_key):
        return f"/data/proj/icar/rho_{model_key}.tif"


def test_fit_spec_icar_carries_spatial_inputs():
    from spatialrisk.session import ProjectSession, ICARFitSpec

    sess = _FitFakeSessionMulti()
    spec = ProjectSession.fit_spec(sess, "icar_calibration")
    restored = _assert_picklable_pure(spec)
    assert isinstance(restored, ICARFitSpec)
    assert restored.target_raster == "/data/proj/processed/forest_loss_2020.tif"
    assert restored.csize == 10.0
    assert restored.rho_path == "/data/proj/icar/rho_icar_calibration.tif"
    assert restored.feature_paths == {"altitude": "/data/proj/processed/altitude.tif"}


def test_fit_spec_jnr_carries_dist_params():
    from spatialrisk.session import ProjectSession, JNRFitSpec

    sess = _FitFakeSessionMulti()
    object.__setattr__(sess.jnr, "dataset_name", "jnr_calibration_2020")
    spec = ProjectSession.fit_spec(sess, "jnr_calibration")
    restored = _assert_picklable_pure(spec)
    assert isinstance(restored, JNRFitSpec)
    assert restored.defor_file == "/data/proj/processed/defor_2020.tif"
    assert restored.forest_edge_file == "/data/proj/processed/forest_edge.tif"
    assert restored.defor_threshold == 99.5
    assert restored.max_dist == 5000
    assert restored.out_root == "/data/proj/rmj_bm"


def test_fit_spec_mw_carries_windows():
    from spatialrisk.session import ProjectSession, MWFitSpec

    sess = _FitFakeSessionMulti()
    object.__setattr__(sess.mw, "dataset_name", "jnr_calibration_2020")
    spec = ProjectSession.fit_spec(sess, "mw_calibration")
    restored = _assert_picklable_pure(spec)
    assert isinstance(restored, MWFitSpec)
    assert restored.win_sizes == (5, 11, 21)
    assert restored.time_interval == 5
    assert restored.forest_file == "/data/proj/processed/forest_2015.tif"
    assert restored.out_root == "/data/proj/rmj_mw"


def test_supervised_apply_spec_carries_estimator_and_design():
    from spatialrisk.session import SupervisedApplySpec

    spec = SupervisedApplySpec(
        model_key="glm_calibration",
        model_type="glm",
        out_path="/data/proj/predictions/glm_2020.tif",
        target_path="/data/proj/processed/forest_loss_2020.tif",
        feature_paths={"altitude": "/data/proj/processed/altitude.tif"},
        formula="I(fcc) ~ scale(altitude)",
        estimator_pickle="/data/proj/glm/glm_calibration.pickle",
        design_sample_path="/data/proj/glm/samples_glm_calibration.csv",
        rho_path=None,
        mask=None,
        mask_value=0,
    )
    restored = _assert_picklable_pure(spec)
    assert restored.estimator_pickle.endswith(".pickle")
    assert restored.design_sample_path.endswith(".csv")
    assert restored.rho_path is None


def test_icar_apply_spec_carries_rho_path():
    from spatialrisk.session import SupervisedApplySpec

    spec = SupervisedApplySpec(
        model_key="icar_calibration",
        model_type="icar",
        out_path="/data/proj/predictions/icar_2020.tif",
        target_path="/data/proj/processed/forest_loss_2020.tif",
        feature_paths={"altitude": "/data/proj/processed/altitude.tif"},
        formula="I(fcc) ~ scale(altitude) + cell",
        estimator_pickle="/data/proj/icar/icar_calibration.pickle",
        design_sample_path="/data/proj/icar/samples.csv",
        rho_path="/data/proj/icar/rho_calibration.tif",
        mask=None,
        mask_value=0,
    )
    restored = _assert_picklable_pure(spec)
    assert restored.model_type == "icar"
    assert restored.rho_path.endswith("rho_calibration.tif")


def test_jnr_and_mw_apply_specs():
    from spatialrisk.session import JNRApplySpec, MWApplySpec

    jnr = JNRApplySpec(
        model_key="jnr_calibration",
        model_type="jnr",
        out_path="/data/proj/rmj_bm/vuln_validation.tif",
        defor_file="/data/proj/processed/defor_2020.tif",
        forest_file="/data/proj/processed/forest_2015.tif",
        forest_edge_file="/data/proj/processed/forest_edge.tif",
        subj_file="/data/proj/processed/subj.tif",
        period="validation",
        dist_bins=(0.0, 100.0, 200.0),
        time_interval=5,
        deforate_model="/data/proj/rmj_bm/defrate_cat_bm_calibration.csv",
        blk_rows=128,
    )
    _assert_picklable_pure(jnr)
    assert jnr.dist_bins == (0.0, 100.0, 200.0)

    mw = MWApplySpec(
        model_key="mw_calibration",
        model_type="mw",
        defor_file="/data/proj/processed/defor_2020.tif",
        forest_file="/data/proj/processed/forest_2015.tif",
        forest_edge_file="/data/proj/processed/forest_edge.tif",
        period="validation",
        ldefrate_files={"5": "/data/proj/rmj_mw/ldefrate_mw_5.tif"},
        win_sizes=(5, 11, 21),
        dist_thresh=300.0,
        time_interval=5,
        blk_rows=256,
        output_folder="/data/proj/rmj_mw",
    )
    restored = _assert_picklable_pure(mw)
    assert restored.ldefrate_files == {"5": "/data/proj/rmj_mw/ldefrate_mw_5.tif"}
    assert restored.win_sizes == (5, 11, 21)


class _ApplyFakeSession(_FitFakeSessionMulti):
    """Adds estimator/design/rho resolution for apply_spec."""

    def __init__(self):
        super().__init__()
        object.__setattr__(self.icar, "rho_path", "/data/proj/icar/rho_calibration.tif")
        object.__setattr__(self.icar, "estimator_pickle", "/data/proj/icar/icar.pickle")
        object.__setattr__(self.glm, "estimator_pickle", "/data/proj/glm/glm.pickle")
        object.__setattr__(self.glm, "samples_path", "/data/proj/glm/samples.csv")
        object.__setattr__(self.icar, "samples_path", "/data/proj/icar/samples.csv")
        object.__setattr__(self.jnr, "dataset_name", "jnr_calibration_2020")
        object.__setattr__(self.mw, "dataset_name", "jnr_calibration_2020")
        object.__setattr__(self.jnr, "dist_bins", (0.0, 100.0, 200.0))
        object.__setattr__(self.jnr, "subj_var", "subj")
        object.__setattr__(self.mw, "dist_thresh", 300.0)
        object.__setattr__(
            self.mw, "ldefrate_files", {"5": "/data/proj/rmj_mw/ldefrate_mw_5.tif"}
        )
        object.__setattr__(self.mw, "win_size_list", (5, 11, 21))
        self._vars["subj"] = _local_raster("subj", "/data/proj/processed/subj.tif")
        # JNR/MW datasets need subj + forest features
        self.jnr_dataset = DatasetSpec(
            name="jnr_calibration_2020",
            year=2020,
            target_ref=VariableId(source="processed", name="defor_2020", year=2020),
            feature_refs=(
                VariableId(source="processed", name="forest_edge"),
                VariableId(source="processed", name="forest_2015"),
                VariableId(source="processed", name="subj"),
            ),
        )
        self._doc.datasets["jnr_calibration_2020"] = self.jnr_dataset


def test_apply_spec_glm_carries_estimator():
    from spatialrisk.session import ProjectSession, SupervisedApplySpec

    sess = _ApplyFakeSession()
    spec = ProjectSession.apply_spec(
        sess, "glm_calibration", out_path="/data/proj/predictions/glm_2020.tif"
    )
    restored = _assert_picklable_pure(spec)
    assert isinstance(restored, SupervisedApplySpec)
    assert restored.estimator_pickle == "/data/proj/glm/glm.pickle"
    assert restored.design_sample_path == "/data/proj/glm/samples.csv"
    assert restored.rho_path is None
    assert restored.target_path == "/data/proj/processed/forest_loss_2020.tif"


def test_apply_spec_icar_carries_rho_path():
    from spatialrisk.session import ProjectSession

    sess = _ApplyFakeSession()
    spec = ProjectSession.apply_spec(
        sess, "icar_calibration", out_path="/data/proj/predictions/icar_2020.tif"
    )
    restored = _assert_picklable_pure(spec)
    assert restored.model_type == "icar"
    assert restored.rho_path == "/data/proj/icar/rho_calibration.tif"
    assert restored.estimator_pickle == "/data/proj/icar/icar.pickle"


def test_apply_spec_jnr_and_mw():
    from spatialrisk.session import ProjectSession, JNRApplySpec, MWApplySpec

    sess = _ApplyFakeSession()
    jnr = ProjectSession.apply_spec(
        sess, "jnr_calibration", out_path="/data/proj/rmj_bm/vuln.tif"
    )
    rj = _assert_picklable_pure(jnr)
    assert isinstance(rj, JNRApplySpec)
    assert rj.subj_file == "/data/proj/processed/subj.tif"
    assert rj.dist_bins == (0.0, 100.0, 200.0)

    mw = ProjectSession.apply_spec(sess, "mw_calibration", out_path=None)
    rm = _assert_picklable_pure(mw)
    assert isinstance(rm, MWApplySpec)
    assert rm.ldefrate_files == {"5": "/data/proj/rmj_mw/ldefrate_mw_5.tif"}
    assert rm.dist_thresh == 300.0


def test_no_spec_holds_a_live_object():
    """Every spec built in this phase must contain only JSON-safe primitives.

    A spec that smuggled an ee.Image / sklearn estimator would either fail to
    pickle or expose a non-(str/num/tuple/dict/Sampling/recipe) attribute.
    """
    import numbers
    from spatialrisk.document import CatalogueRecipe, GEERecipe
    from spatialrisk.sampling import Sampling
    from spatialrisk.session import (
        MaterializeSpec,
        SupervisedFitSpec,
        SupervisedApplySpec,
    )

    allowed = (str, bool, numbers.Number, type(None), Sampling)

    def _walk(value):
        if isinstance(value, allowed):
            return
        if isinstance(value, (tuple, list)):
            for v in value:
                _walk(v)
            return
        if isinstance(value, dict):
            for k, v in value.items():
                _walk(k)
                _walk(v)
            return
        if isinstance(value, (CatalogueRecipe,)) or "Recipe" in type(value).__name__:
            return
        if hasattr(value, "model_fields"):  # nested frozen pydantic spec
            for f in value.model_fields:
                _walk(getattr(value, f))
            return
        raise AssertionError(f"non-JSON-safe value in spec: {value!r} ({type(value)})")

    specs = [
        MaterializeSpec(
            var_key="altitude",
            recipe=CatalogueRecipe(
                source="catalogue", catalogue_key="altitude", export_kind="raster"
            ),
            out_path="/x.tif",
            export_kind="raster",
        ),
        SupervisedFitSpec(
            model_key="m",
            model_type="glm",
            target_path="/t.tif",
            feature_paths={"a": "/a.tif"},
            formula="y ~ a",
            sampling=Sampling(strategy="random", n_samples=100),
            output_sample_path="/s.csv",
        ),
        SupervisedApplySpec(
            model_key="m",
            model_type="glm",
            out_path="/o.tif",
            target_path="/t.tif",
            feature_paths={"a": "/a.tif"},
            formula="y ~ a",
            estimator_pickle="/e.pickle",
        ),
    ]
    for s in specs:
        for f in s.model_fields:
            _walk(getattr(s, f))


def test_categorical_levels_reads_via_far_helpers(monkeypatch):
    """_categorical_levels delegates to far_helpers.get_categorical_levels."""
    from spatialrisk.session import ProjectSession
    from spatialrisk.document import LocalRasterSpec
    from spatialrisk.variables.models import RasterType
    import spatialrisk.far_helpers as fh

    captured = {}

    def fake_levels(var):
        captured["var"] = var
        return [0, 1, 2]

    monkeypatch.setattr(fh, "get_categorical_levels", fake_levels)

    var = LocalRasterSpec(
        kind="local_raster",
        name="pa",
        path="/data/proj/processed/pa.tif",
        raster_type=RasterType.categorical,
    )
    levels = ProjectSession._categorical_levels(object.__new__(ProjectSession), var)
    assert levels == (0, 1, 2)
    assert captured["var"] is var


def test_all_fit_and_apply_specs_round_trip_for_every_model_type():
    """Smoke guard: fit_spec + apply_spec for glm/rf/icar/jnr/mw all pickle."""
    from spatialrisk.session import ProjectSession

    sess = _ApplyFakeSession()
    object.__setattr__(sess.jnr, "dataset_name", "jnr_calibration_2020")
    object.__setattr__(sess.mw, "dataset_name", "jnr_calibration_2020")
    object.__setattr__(
        sess.jnr, "parameters", {"defor_threshold": 99.5, "max_dist": 5000,
                                 "blk_rows": 128, "time_interval": 5}
    )
    object.__setattr__(
        sess.mw, "parameters", {"win_size_list": [5, 11, 21], "time_interval": 5,
                                "defor_threshold": 99.5, "blk_rows": 256}
    )

    for key in ("glm_calibration", "icar_calibration", "jnr_calibration",
                "mw_calibration"):
        fs = ProjectSession.fit_spec(sess, key)
        _assert_picklable_pure(fs)

    for key, outp in (
        ("glm_calibration", "/o/glm.tif"),
        ("icar_calibration", "/o/icar.tif"),
        ("jnr_calibration", "/o/jnr.tif"),
        ("mw_calibration", None),
    ):
        aps = ProjectSession.apply_spec(sess, key, out_path=outp)
        _assert_picklable_pure(aps)
