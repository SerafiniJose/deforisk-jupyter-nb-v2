"""run_inference: the family-aware adapter from (model, dataset) to apply()."""

import types
from pathlib import Path

import pytest

from gui.scripts.inference_runner import run_inference


class _RecordingModel:
    def __init__(self):
        self.apply_calls = []

    def apply(self, *args, **kwargs):
        self.apply_calls.append((args, kwargs))
        return Path("/tmp/out.tif")


class _RecordingMW(_RecordingModel):
    def apply(self, *args, **kwargs):
        self.apply_calls.append((args, kwargs))
        return {5: Path("/tmp/mw_5.tif"), 11: Path("/tmp/mw_11.tif")}


def _feature(name):
    """Dataset feature whose path is derived from its name."""
    return types.SimpleNamespace(name=name, path=Path(f"/tmp/{name}.tif"))


def _project(model, model_key, with_forest=True, feature_names=None):
    """Fake project around one dataset.

    ``feature_names`` sets the dataset's feature names explicitly; because the
    paths are derived from the names, a test can assert *which* feature was
    picked. Defaults to the legacy single bare ``forest_gfc``.
    """
    target = types.SimpleNamespace(
        name="forest_loss_2015_2020", path=Path("/tmp/d.tif")
    )
    if feature_names is None:
        feature_names = ["forest_gfc"] if with_forest else []
    feats = [_feature(n) for n in feature_names]
    dataset = types.SimpleNamespace(name="calibration", target=target, features=feats)
    folders = types.SimpleNamespace(
        glm_model=Path("/tmp/far_glm"),
        rf_model=Path("/tmp/far_rf"),
        icar_model=Path("/tmp/far_icar"),
        rmj_bm=Path("/tmp/rmj_bm"),
        rmj_mw=Path("/tmp/rmj_mw"),
    )
    return types.SimpleNamespace(
        models={model_key: model},
        get_dataset=lambda n: dataset,
        folders=folders,
    )


def test_ml_model_apply_gets_mask_and_output(tmp_path):
    """An ML family gets apply(output_file, dataset, mask, mask_value)."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1")
    run_inference(proj, "glm_glm_v1", "calibration")
    (args, kwargs) = m.apply_calls[0]
    # apply(output_file, dataset, mask, mask_value)
    assert str(args[0]).endswith("calibration.tif")
    assert args[1] is proj.get_dataset("calibration")  # dataset positional arg
    assert args[2] == Path("/tmp/forest_gfc.tif")  # forest_gfc mask
    assert args[3] == 0  # mask_value


def test_ml_model_with_none_name_falls_back_to_model_key(tmp_path):
    """A model whose ``name`` is None falls back to model_key, not a crash.

    Real models (BaseRiskModel) default ``name`` to None. The old code used
    ``getattr(model, "name", model_key)``, whose default only applies when the
    attribute is *missing* — so an existing-but-None name returned None and
    ``Path(...) / None`` raised TypeError.
    """
    m = _RecordingModel()
    m.name = None
    proj = _project(m, "glm")
    run_inference(proj, "glm", "calibration")
    (args, _kwargs) = m.apply_calls[0]
    out_path = Path(args[0])
    assert out_path.parent == Path("/tmp/far_glm/glm")  # model_key is the fallback
    assert out_path.name == "calibration.tif"


def test_jnr_model_apply_gets_time_interval(tmp_path):
    """The JNR family gets its time interval derived from the target name."""
    m = _RecordingModel()
    proj = _project(m, "jnr_calibration_jnr")
    run_inference(proj, "jnr_calibration_jnr", "calibration")
    (args, kwargs) = m.apply_calls[0]
    assert str(args[0]).endswith(
        "prob_bm_calibration.tif"
    )  # output path positional arg
    assert args[1] is proj.get_dataset("calibration")  # dataset positional arg
    assert kwargs["time_interval"] == 5
    assert kwargs["deforate_model"] is None


def test_mw_model_apply_returns_multiple_and_uses_output_folder(tmp_path):
    """The MW family writes one raster per window into an output folder."""
    m = _RecordingMW()
    proj = _project(m, "mw_calibration_mw")
    run_inference(proj, "mw_calibration_mw", "calibration")
    (args, kwargs) = m.apply_calls[0]
    assert kwargs["time_interval"] == 5
    assert kwargs["output_folder"] == Path("/tmp/rmj_mw")


def test_ml_model_missing_forest_feature_raises():
    """An ML run without a forest feature fails its precondition, loudly."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1", with_forest=False)
    with pytest.raises(ValueError, match="forest_gfc"):
        run_inference(proj, "glm_glm_v1", "calibration")


# --- forest-mask resolution -------------------------------------------------
#
# Hansen layers created from the Add Variable modal bake their parameters into
# the variable name ("forest_gfc_tc30"), so the mask can no longer be found by
# an exact-name match. See gui/scripts/predefined_variables.resolve_predefined.


def test_ml_model_accepts_a_parameterised_forest_feature():
    """Regression: the mask lookup must resolve the name, not compare it.

    Every project built after the tree-cover-threshold feature shipped names
    its Hansen layer ``forest_gfc_tc<N>``; the old ``f.name == "forest_gfc"``
    match returned None and raised on every GLM/RF/ICAR run.
    """
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1", feature_names=["forest_gfc_tc30"])
    run_inference(proj, "glm_glm_v1", "calibration")
    (args, _kwargs) = m.apply_calls[0]
    assert args[2] == Path("/tmp/forest_gfc_tc30.tif")


def test_non_forest_features_are_not_mask_candidates():
    """Other layers in the dataset must not be mistaken for the forest mask."""
    m = _RecordingModel()
    proj = _project(
        m, "glm_glm_v1", feature_names=["altitude", "forest_tmf", "my_forest_mask"]
    )
    with pytest.raises(ValueError, match="forest_gfc"):
        run_inference(proj, "glm_glm_v1", "calibration")


def test_two_forest_candidates_raise_listing_both():
    """Ambiguity is the user's call (in the Predict dialog), never a guess."""
    m = _RecordingModel()
    proj = _project(
        m, "glm_glm_v1", feature_names=["forest_gfc_tc30", "forest_gfc_tc75"]
    )
    with pytest.raises(ValueError) as excinfo:
        run_inference(proj, "glm_glm_v1", "calibration")
    message = str(excinfo.value)
    assert "forest_gfc_tc30" in message and "forest_gfc_tc75" in message
    assert not m.apply_calls


def test_explicit_forest_feature_resolves_the_ambiguity():
    """The dialog's choice selects one of two candidates."""
    m = _RecordingModel()
    proj = _project(
        m, "glm_glm_v1", feature_names=["forest_gfc_tc30", "forest_gfc_tc75"]
    )
    run_inference(proj, "glm_glm_v1", "calibration", forest_feature="forest_gfc_tc75")
    (args, _kwargs) = m.apply_calls[0]
    assert args[2] == Path("/tmp/forest_gfc_tc75.tif")


def test_explicit_forest_feature_not_in_dataset_raises():
    """A stale choice names the feature and the dataset it is missing from."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1", feature_names=["forest_gfc_tc30"])
    with pytest.raises(ValueError) as excinfo:
        run_inference(
            proj, "glm_glm_v1", "calibration", forest_feature="forest_gfc_tc75"
        )
    message = str(excinfo.value)
    assert "forest_gfc_tc75" in message and "calibration" in message


def test_empty_forest_feature_falls_back_to_the_sole_candidate():
    """The dialog field is optional: blank means 'resolve it for me'."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1", feature_names=["forest_gfc_tc30"])
    run_inference(proj, "glm_glm_v1", "calibration", forest_feature="")
    (args, _kwargs) = m.apply_calls[0]
    assert args[2] == Path("/tmp/forest_gfc_tc30.tif")


def test_forest_feature_candidates_lists_hansen_layers_only():
    """The one place both the dialog and the runner ask 'which are forests?'."""
    from gui.scripts.inference_runner import forest_feature_candidates

    dataset = types.SimpleNamespace(
        features=[
            _feature("altitude"),
            _feature("forest_gfc_tc30"),
            _feature("forest_tmf"),
            _feature("forest_gfc"),
            _feature("my_forest_mask"),
        ]
    )
    assert forest_feature_candidates(dataset) == ["forest_gfc_tc30", "forest_gfc"]


def test_forest_feature_candidates_tolerates_a_missing_dataset():
    """A half-built dataset yields no candidates instead of raising."""
    from gui.scripts.inference_runner import forest_feature_candidates

    assert forest_feature_candidates(None) == []
    assert forest_feature_candidates(types.SimpleNamespace(features=None)) == []


def test_benchmark_families_ignore_the_forest_feature():
    """JNR/MW resolve their own layers, so no mask is required of them."""
    m = _RecordingModel()
    proj = _project(m, "jnr_calibration_jnr", with_forest=False)
    run_inference(proj, "jnr_calibration_jnr", "calibration")
    assert m.apply_calls


def test_named_run_uses_name_subfolder_and_sets_pending_name():
    """A named ML run gets its own subfolder and hands the name to the model.

    _register_prediction keys the prediction by that name.
    """
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1")
    run_inference(proj, "glm_glm_v1", "calibration", name="run_a")
    (args, _kwargs) = m.apply_calls[0]
    out_path = Path(args[0])
    assert out_path.parent == Path("/tmp/far_glm/run_a")  # name is the subfolder
    assert out_path.name == "calibration.tif"
    assert m._pending_pred_name == "run_a"


def test_named_jnr_run_uses_name_subfolder():
    """A named JNR run writes into a per-name subfolder too."""
    m = _RecordingModel()
    proj = _project(m, "jnr_calibration_jnr")
    run_inference(proj, "jnr_calibration_jnr", "calibration", name="bench1")
    (args, _kwargs) = m.apply_calls[0]
    out_path = Path(args[0])
    assert out_path.parent == Path("/tmp/rmj_bm/bench1")
    assert out_path.name == "prob_bm_calibration.tif"


def test_named_mw_run_uses_name_output_folder():
    """A named MW run nests its output folder under the run name."""
    m = _RecordingMW()
    proj = _project(m, "mw_calibration_mw")
    run_inference(proj, "mw_calibration_mw", "calibration", name="mwrun")
    (_args, kwargs) = m.apply_calls[0]
    assert kwargs["output_folder"] == Path("/tmp/rmj_mw/mwrun")
    assert m._pending_pred_name == "mwrun"


# --- across the naming boundary ---------------------------------------------


class _NamingProject:
    """Project stub good enough for a real ``Dataset`` *and* ``run_inference``.

    Deliberately not a namespace of pre-built features: the point of the test
    below is to let ``Dataset.set_features`` decide what ``feature.name`` ends
    up being, so the runner is exercised against the name the app actually
    produces rather than one the test typed out.
    """

    def __init__(self, model, model_key, forest_name, folders):
        self.models = {model_key: model}
        self.folders = folders
        self._instances = {
            "forest_loss_2015_2020": [
                types.SimpleNamespace(
                    name="forest_loss_2015_2020", year=None, path=Path("/tmp/d.tif")
                )
            ],
            forest_name: [
                types.SimpleNamespace(
                    name=forest_name, year=None, path=Path(f"/tmp/{forest_name}.tif")
                )
            ],
        }
        self._dataset = None

    # -- Dataset collaborators
    def get_all_instances(self, name, source="processed"):
        """Every registered instance of *name*."""
        return self._instances.get(name, [])

    def is_temporal(self, name, source="processed"):
        """Both stubbed variables are static."""
        return False

    def get_variable_years(self, name, source="processed"):
        """No years, since nothing here is temporal."""
        return []

    def get_variable(self, name, year=None):
        """The single instance registered under *name*."""
        return self.get_all_instances(name)[0]

    def list_unique_variable_names(self, source="processed"):
        """Names available to ``Dataset.set_features``."""
        return list(self._instances)

    # -- run_inference collaborator
    def get_dataset(self, name):
        """The dataset the test built."""
        return self._dataset


def test_run_inference_accepts_a_dataset_built_from_a_parameterised_layer(tmp_path):
    """Integration-shaped: modal name -> Dataset.set_features -> run_inference.

    The unit tests above exercise each side of this boundary in isolation. This
    one crosses it: the variable is named by ``build_predefined_name`` exactly
    as the Add Variable modal names it, stored on a real ``Dataset`` via
    ``set_features``, and handed to ``run_inference``. Before the fix this
    raised "ML inference needs a 'forest_gfc' feature" — i.e. every GLM/RF/ICAR
    run on any project created after the threshold feature shipped.
    """
    from gui.scripts.predefined_variables import build_predefined_name
    from spatialrisk.dataset import Dataset

    forest_name = build_predefined_name("forest_gfc", {"tree_cover_threshold": 30})
    assert forest_name == "forest_gfc_tc30"

    m = _RecordingModel()
    folders = types.SimpleNamespace(
        glm_model=tmp_path / "far_glm",
        rf_model=tmp_path / "far_rf",
        icar_model=tmp_path / "far_icar",
        rmj_bm=tmp_path / "rmj_bm",
        rmj_mw=tmp_path / "rmj_mw",
    )
    proj = _NamingProject(m, "glm_glm_v1", forest_name, folders)
    ds = Dataset(project=proj, name="calibration")
    ds.set_target("forest_loss_2015_2020")
    ds.set_features([forest_name])
    proj._dataset = ds

    run_inference(proj, "glm_glm_v1", "calibration")

    (args, _kwargs) = m.apply_calls[0]
    assert args[2] == Path(f"/tmp/{forest_name}.tif")
