"""Tests for the extracted persistence layer (ProjectRepository + ModelStore).

These guard a behaviour-preserving extraction of project JSON (de)serialization
and model pickle persistence out of the 1191-line Project god object, the iCAR
persistence unification, the _default_folder jnr/mw fix, and the removal of the
import-time mkdir side effect.
"""

import os
import subprocess
import sys
import warnings
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _make_project(name="ut_proj"):
    """Build an in-memory project with one raster + one vector variable."""
    from spatialrisk import Project
    from spatialrisk.variables import LocalRasterVar, LocalVectorVar
    from spatialrisk.variables.models import RasterType

    Project._ensure_model_schemas()
    proj = Project(project_name=name)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)  # missing-path warning is expected
        raster = LocalRasterVar(
            name="forest",
            path="/nope/forest.tif",
            raster_type=list(RasterType)[0],
            project=proj,
        )
        vector = LocalVectorVar(name="roads", path="/nope/roads.shp", project=proj)
    proj.raw_variables["forest"] = raster
    proj.processed_variables["roads"] = vector
    return proj


# --------------------------------------------------------------------------- #
# ProjectRepository
# --------------------------------------------------------------------------- #
def test_project_repository_round_trip(tmp_path):
    from spatialrisk.persistence import ProjectRepository

    proj = _make_project()
    repo = ProjectRepository(data_root=tmp_path)

    saved = repo.save(proj)
    assert saved.exists()

    loaded = repo.load(proj.project_name)
    assert loaded.project_name == proj.project_name
    assert set(loaded.raw_variables) == {"forest"}
    assert set(loaded.processed_variables) == {"roads"}
    assert loaded.raw_variables["forest"].data_type == "raster"
    assert loaded.processed_variables["roads"].data_type == "vector"
    # path round-trips back to a Path (pydantic coerces str -> Path)
    assert isinstance(loaded.raw_variables["forest"].path, Path)
    assert str(loaded.raw_variables["forest"].path) == "/nope/forest.tif"


def test_project_repository_list(tmp_path):
    from spatialrisk.persistence import ProjectRepository

    repo = ProjectRepository(data_root=tmp_path)
    repo.save(_make_project(name="alpha"))
    repo.save(_make_project(name="beta"))
    assert repo.list() == ["alpha", "beta"]


def test_project_save_load_delegates_to_repository(tmp_path, monkeypatch):
    """Project.save()/load() keep working unchanged via thin delegations."""
    import spatialrisk.project as project_mod
    from spatialrisk import Project

    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path)

    _make_project(name="delegated").save()
    loaded = Project.load("delegated")

    assert loaded.project_name == "delegated"
    assert set(loaded.raw_variables) == {"forest"}
    assert set(loaded.processed_variables) == {"roads"}


# --------------------------------------------------------------------------- #
# ModelStore
# --------------------------------------------------------------------------- #
def test_modelstore_round_trip(tmp_path):
    from spatialrisk.mlmodels import GLMModel
    from spatialrisk.persistence import ModelStore

    model = GLMModel(name="cal")
    model._ml_model = {"coef": [1, 2, 3]}
    model.formula = "y ~ x"

    path = ModelStore.save(model, folder=tmp_path)
    assert path.exists()

    reloaded = GLMModel(name="cal", model_path=path)
    ModelStore.load(reloaded)
    assert reloaded._ml_model == {"coef": [1, 2, 3]}
    assert reloaded.formula == "y ~ x"


def test_base_model_save_load_delegates(tmp_path):
    """BaseRiskModel.save()/load_model() keep working via ModelStore."""
    from spatialrisk.mlmodels import GLMModel

    model = GLMModel(name="cal")
    model._ml_model = {"coef": 42}

    path = model.save(folder=tmp_path)
    reloaded = GLMModel(model_path=path)
    reloaded.load_model()
    assert reloaded._ml_model == {"coef": 42}


def test_default_folder_covers_jnr_and_mw(tmp_path, monkeypatch):
    """All model types resolve a model folder (mw/jnr via per-subclass overrides)."""
    import spatialrisk.project as project_mod
    from spatialrisk.mlmodels import JNRBenchmarkModel, MWModel

    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path)
    proj = _make_project(name="folders")

    for model_cls in (JNRBenchmarkModel, MWModel):
        model = model_cls()
        model.project = proj
        assert model._default_folder() is not None, (
            f"{model_cls.__name__} resolved no model folder"
        )


# --------------------------------------------------------------------------- #
# Import-time side effect
# --------------------------------------------------------------------------- #
def test_import_spatialrisk_creates_no_directory(tmp_path):
    """Importing the package must not mkdir anything (no import side effects)."""
    workdir = tmp_path / "wd"
    workdir.mkdir()

    result = subprocess.run(
        [sys.executable, "-c", "import spatialrisk"],
        cwd=workdir,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    # project.py computes downloads_folder as Path.cwd().parent / "data"
    assert not (tmp_path / "data").exists(), "importing spatialrisk created a data/ directory"
