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


def _raster(name):
    """Processed raster variable whose path is derived from its name."""
    return types.SimpleNamespace(
        name=name, data_type="raster", path=Path(f"/tmp/{name}.tif")
    )


def _project(model, model_key, layer_names=None):
    """Fake project around one dataset.

    ``layer_names`` sets the project's processed raster layers explicitly;
    because the paths are derived from the names, a test can assert *which*
    layer was picked. Defaults to a single ``forest_gfc_tc30``. The dataset's
    own features stay empty on purpose: the mask is resolved against the
    project's processed rasters, never against dataset membership.
    """
    target = types.SimpleNamespace(
        name="forest_loss_2015_2020", path=Path("/tmp/d.tif")
    )
    if layer_names is None:
        layer_names = ["forest_gfc_tc30"]
    dataset = types.SimpleNamespace(name="calibration", target=target, features=[])
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
        processed_variables={n: _raster(n) for n in layer_names},
        folders=folders,
    )


# --- mask resolution ---------------------------------------------------------
#
# The mask is generic: any processed raster of the project can be assigned as
# the mask layer — dataset membership is irrelevant — and omitting it is a
# valid, explicit "predict everywhere". The runner never guesses a mask on the
# user's behalf — seeding a forest suggestion is the Predict dialog's job
# (suggested_mask_layer).


def test_ml_model_apply_gets_the_chosen_mask_and_output():
    """An ML family gets apply(output_file, dataset, mask, mask_value)."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1", layer_names=["altitude", "forest_gfc_tc30"])
    run_inference(proj, "glm_glm_v1", "calibration", mask_layer="forest_gfc_tc30")
    (args, kwargs) = m.apply_calls[0]
    # apply(output_file, dataset, mask, mask_value)
    assert str(args[0]).endswith("calibration.tif")
    assert args[1] is proj.get_dataset("calibration")  # dataset positional arg
    assert args[2] == Path("/tmp/forest_gfc_tc30.tif")  # chosen mask layer
    assert args[3] == 0  # mask_value


def test_any_project_raster_can_be_the_mask():
    """The mask need not be a dataset feature — any project raster works."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1", layer_names=["altitude", "my_forest_mask"])
    assert not proj.get_dataset("calibration").features  # not in the dataset
    run_inference(proj, "glm_glm_v1", "calibration", mask_layer="my_forest_mask")
    (args, _kwargs) = m.apply_calls[0]
    assert args[2] == Path("/tmp/my_forest_mask.tif")


def test_no_mask_predicts_over_the_full_stack():
    """mask_layer=None is an explicit 'no mask': apply() receives mask=None."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1", layer_names=["altitude"])
    run_inference(proj, "glm_glm_v1", "calibration", mask_layer=None)
    (args, _kwargs) = m.apply_calls[0]
    assert args[2] is None


def test_blank_mask_layer_means_no_mask_too():
    """The dialog's empty-string state degrades to no mask, never to a guess."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1")
    run_inference(proj, "glm_glm_v1", "calibration", mask_layer="")
    (args, _kwargs) = m.apply_calls[0]
    assert args[2] is None


def test_mask_layer_not_in_project_raises():
    """A stale choice names the layer and lists what the project does have."""
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1", layer_names=["forest_gfc_tc30"])
    with pytest.raises(ValueError) as excinfo:
        run_inference(proj, "glm_glm_v1", "calibration", mask_layer="forest_gfc_tc75")
    message = str(excinfo.value)
    assert "forest_gfc_tc75" in message and "forest_gfc_tc30" in message
    assert not m.apply_calls


def test_ml_model_with_none_name_falls_back_to_model_key():
    """A model whose ``name`` is None falls back to model_key, not a crash.

    Real models (BaseRiskModel) default ``name`` to None. The old code used
    ``getattr(model, "name", model_key)``, whose default only applies when the
    attribute is *missing* — so an existing-but-None name returned None and
    ``Path(...) / None`` raised TypeError.
    """
    m = _RecordingModel()
    m.name = None
    proj = _project(m, "glm")
    run_inference(proj, "glm", "calibration", mask_layer="forest_gfc_tc30")
    (args, _kwargs) = m.apply_calls[0]
    out_path = Path(args[0])
    assert out_path.parent == Path("/tmp/far_glm/glm")  # model_key is the fallback
    assert out_path.name == "calibration.tif"


# --- candidate list and dialog seed ------------------------------------------


def _catalog(*names, **extra):
    """Fake project exposing only ``processed_variables``."""
    variables = {n: _raster(n) for n in names}
    variables.update(extra)
    return types.SimpleNamespace(processed_variables=variables)


def test_mask_layer_candidates_lists_every_project_raster():
    """Any processed raster is a legal mask — the dialog offers them all."""
    from gui.scripts.inference_runner import mask_layer_candidates

    proj = _catalog("my_forest_mask", "altitude", "forest_gfc_tc30")
    assert mask_layer_candidates(proj) == [
        "altitude",
        "forest_gfc_tc30",
        "my_forest_mask",
    ]


def test_mask_layer_candidates_skip_vector_variables():
    """Only rasters can mask a raster — vector layers are never offered."""
    from gui.scripts.inference_runner import mask_layer_candidates

    proj = _catalog(
        "altitude",
        roads=types.SimpleNamespace(name="roads", data_type="vector"),
    )
    assert mask_layer_candidates(proj) == ["altitude"]


def test_mask_layer_candidates_tolerate_a_missing_project():
    """A half-built project yields no candidates instead of raising."""
    from gui.scripts.inference_runner import mask_layer_candidates

    assert mask_layer_candidates(None) == []
    assert mask_layer_candidates(types.SimpleNamespace(processed_variables=None)) == []


def test_suggested_mask_layer_is_the_sole_forest_layer():
    """A single Hansen-derived layer is the natural deforisk-style seed."""
    from gui.scripts.inference_runner import suggested_mask_layer

    proj = _catalog("altitude", "forest_gfc_tc30")
    assert suggested_mask_layer(proj) == "forest_gfc_tc30"


def test_suggested_mask_layer_resolves_legacy_bare_names():
    """Older projects name the layer plain ``forest_gfc``; it still seeds."""
    from gui.scripts.inference_runner import suggested_mask_layer

    proj = _catalog("forest_gfc")
    assert suggested_mask_layer(proj) == "forest_gfc"


def test_suggested_mask_layer_never_guesses_between_two_forests():
    """Two forest definitions: the choice is the user's, so no seed."""
    from gui.scripts.inference_runner import suggested_mask_layer

    proj = _catalog("forest_gfc_tc30", "forest_gfc_tc75")
    assert suggested_mask_layer(proj) == ""


def test_suggested_mask_layer_is_empty_without_a_forest_layer():
    """No Hansen layer, no suggestion — other layers are never auto-picked."""
    from gui.scripts.inference_runner import suggested_mask_layer

    proj = _catalog("altitude", "forest_tmf")
    assert suggested_mask_layer(proj) == ""
    assert suggested_mask_layer(None) == ""


# --- benchmark families ------------------------------------------------------


def test_jnr_model_apply_gets_time_interval():
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


def test_mw_model_apply_returns_multiple_and_uses_output_folder():
    """The MW family writes one raster per window into an output folder."""
    m = _RecordingMW()
    proj = _project(m, "mw_calibration_mw")
    run_inference(proj, "mw_calibration_mw", "calibration")
    (args, kwargs) = m.apply_calls[0]
    assert kwargs["time_interval"] == 5
    assert kwargs["output_folder"] == Path("/tmp/rmj_mw")


def test_benchmark_families_ignore_the_mask_layer():
    """JNR/MW resolve their own layers, so no mask is required of them."""
    m = _RecordingModel()
    proj = _project(m, "jnr_calibration_jnr", layer_names=[])
    run_inference(proj, "jnr_calibration_jnr", "calibration", mask_layer="whatever")
    assert m.apply_calls


# --- named runs --------------------------------------------------------------


def test_named_run_uses_name_subfolder_and_sets_pending_name():
    """A named ML run gets its own subfolder and hands the name to the model.

    _register_prediction keys the prediction by that name.
    """
    m = _RecordingModel()
    proj = _project(m, "glm_glm_v1")
    run_inference(
        proj,
        "glm_glm_v1",
        "calibration",
        name="run_a",
        mask_layer="forest_gfc_tc30",
    )
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
                    name="forest_loss_2015_2020",
                    year=None,
                    data_type="raster",
                    path=Path("/tmp/d.tif"),
                )
            ],
            forest_name: [
                types.SimpleNamespace(
                    name=forest_name,
                    year=None,
                    data_type="raster",
                    path=Path(f"/tmp/{forest_name}.tif"),
                )
            ],
        }
        self.processed_variables = {
            name: instances[0] for name, instances in self._instances.items()
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

    The variable is named by ``build_predefined_name`` exactly as the Add
    Variable modal names it, stored on a real ``Dataset`` via ``set_features``,
    suggested by ``suggested_mask_layer`` as the dialog would seed it, and
    handed to ``run_inference`` — the whole path the app actually takes.
    """
    from gui.scripts.inference_runner import suggested_mask_layer
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

    seed = suggested_mask_layer(proj)
    assert seed == forest_name

    run_inference(proj, "glm_glm_v1", "calibration", mask_layer=seed)

    (args, _kwargs) = m.apply_calls[0]
    assert args[2] == Path(f"/tmp/{forest_name}.tif")
