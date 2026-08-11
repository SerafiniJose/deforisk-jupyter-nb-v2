"""Model artifacts must never be placed relative to the process CWD.

Two ways that used to happen, both closed here.

**The ``or Path.cwd()`` fallbacks.** Every fit()/apply() site that picks an
output folder read ``self._default_folder() or Path.cwd()``.
``_default_folder()`` returns None only when no project is attached, so the GUI
(which always sets ``model.project`` and passes a folder) never reached the
fallback -- but a script or a notebook did, and then training CSVs, pickles, rho
rasters and rmj outputs landed in whatever directory the process happened to
start in. On SEPAL that directory is the read-only shared module mount, so the
guess does not merely misplace the file, it fails with ``Read-only file
system``. ``BaseRiskModel.save()`` already refused to guess; these tests pin
every other site to that same rule, including the message it raises with.

**The forestatrisk boundary.** ``far.interpolate_rho`` writes a second file the
caller never asked for, at ``os.path.join(os.path.dirname(output_file),
"rho_orig.tif")``. A bare filename makes ``os.path.dirname`` return ``""`` and
drops that sibling in the CWD. The call site now resolves its paths to absolute
before handing them over, so the property holds structurally instead of by luck
(today every caller happens to pass an absolute project folder).
"""

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.transform import from_origin

from spatialrisk.mlmodels.glm_model import GLMModel
from spatialrisk.mlmodels.icar_model import ICARModel
from spatialrisk.mlmodels.jnr_model import JNRBenchmarkModel
from spatialrisk.mlmodels.mw_model import MWModel
from spatialrisk.mlmodels.rf_model import RFModel

# The refusal BaseRiskModel.save() has always raised; every other write site
# must now raise it too rather than falling back to the CWD.
REFUSAL = "Cannot determine output folder: no project is attached."


# ----------------------------------------------------------------------
# Fixtures and stubs
# ----------------------------------------------------------------------


@pytest.fixture()
def temp_cwd(tmp_path, monkeypatch):
    """Run from an empty scratch directory so stray writes are visible."""
    cwd = tmp_path / "process_cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    return cwd


def _write_binary_raster(path):
    """A tiny 0/1 raster: valid input for the rmj binary forest-loss guard.

    EPSG:32631 at 30 m -- a metric CRS, because ``far.cellneigh`` interprets its
    ``csize`` in kilometres against the raster's own units.
    """
    data = np.zeros((8, 8), dtype="uint8")
    data[2:5, 2:5] = 1
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs="EPSG:32631",
        transform=from_origin(500000.0, 5000000.0, 30.0, 30.0),
        nodata=255,
    ) as dst:
        dst.write(data, 1)
    return path


class _Var:
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.raster_type = "continuous"


class _StubDataset:
    """The dataset surface fit() touches: target, features, name, extraction."""

    def __init__(self, raster, name="calibration"):
        self.name = name
        self.year = 2020
        self.target = _Var("target", raster)
        self.features = [
            _Var("altitude", raster),
            # The layer names MW and JNR look up by default.
            _Var("forest_edge", raster),
            _Var("forest", raster),
            _Var("subj", raster),
        ]

    def extract_at_points(self, points, *, drop_nodata=True):
        return pd.DataFrame(
            {
                "target": [0, 1, 0, 1],
                "trial": [1, 1, 1, 1],
                "altitude": [1.0, 2.0, 3.0, 4.0],
                "cell_id": [0, 1, 2, 3],
            }
        )


class _StubSample:
    name = "s1"

    def load_points(self):
        return object()


def _model(cls, raster):
    """An unregistered model: a dataset to work on, deliberately no project."""
    model = cls(name="m")
    model.dataset = _StubDataset(raster)
    model.sample = _StubSample()
    # Skip formula auto-generation, which would read the stub's rasters.
    model.formula = "target + trial ~ altitude"
    return model


@pytest.fixture()
def raster(tmp_path):
    """The one raster every stub layer points at, well outside the CWD."""
    return _write_binary_raster(tmp_path / "binary.tif")


# ----------------------------------------------------------------------
# Class 1: no project and no folder must raise, not write to the CWD
# ----------------------------------------------------------------------


@pytest.mark.parametrize("cls", [GLMModel, RFModel, ICARModel])
def test_ml_fit_without_a_project_refuses_instead_of_writing_to_cwd(
    cls, raster, temp_cwd
):
    """GLM/RF/iCAR: the training-CSV folder is the first thing fit() resolves."""
    model = _model(cls, raster)

    with pytest.raises(RuntimeError, match=REFUSAL):
        model.fit()

    assert list(temp_cwd.iterdir()) == []


def test_mw_fit_without_a_project_refuses_instead_of_writing_to_cwd(raster, temp_cwd):
    """MW resolves its own folder key (rmj_mw) through its own _default_folder."""
    model = _model(MWModel, raster)

    with pytest.raises(RuntimeError, match=REFUSAL):
        model.fit(time_interval=5)

    assert list(temp_cwd.iterdir()) == []


def test_jnr_fit_without_a_project_refuses_instead_of_writing_to_cwd(raster, temp_cwd):
    """JNR resolves its own folder key (rmj_bm) through its own _default_folder."""
    model = _model(JNRBenchmarkModel, raster)

    with pytest.raises(RuntimeError, match=REFUSAL):
        model.fit()

    assert list(temp_cwd.iterdir()) == []


def test_mw_apply_without_a_project_refuses_and_names_its_own_keyword(
    raster, tmp_path, temp_cwd
):
    """The message must name the keyword this caller actually takes."""
    model = _model(MWModel, raster)
    # State a fitted model would carry; apply() resolves its folder before it
    # ever looks at whether these rasters exist.
    model.dist_thresh = 100.0
    model.ldefrate_files = {"5": tmp_path / "ldefrate_mw_5.tif"}

    with pytest.raises(RuntimeError, match=REFUSAL) as excinfo:
        model.apply(time_interval=5)

    assert "pass output_folder= explicitly" in str(excinfo.value)
    assert list(temp_cwd.iterdir()) == []


def test_fit_refuses_with_exactly_the_message_save_refuses_with(raster, temp_cwd):
    """save() is the precedent; fit() must not invent a second dialect of it."""
    model = _model(GLMModel, raster)

    with pytest.raises(RuntimeError) as from_fit:
        model.fit()

    # save() checks for a trained object before it resolves a folder.
    model._ml_model = object()
    with pytest.raises(RuntimeError) as from_save:
        model.save()

    assert str(from_fit.value) == str(from_save.value)
    assert list(temp_cwd.iterdir()) == []


def test_an_explicit_folder_still_works_without_a_project(raster, tmp_path, temp_cwd):
    """The escape hatch the refusal points at: pass folder= and it trains."""
    out = tmp_path / "explicit"
    model = _model(GLMModel, raster)

    model.fit(folder=out)

    assert (out / "samples_glm_m.csv").exists()
    assert Path(model.model_path).parent == out
    assert list(temp_cwd.iterdir()) == []


# ----------------------------------------------------------------------
# Class 2: absolute paths at the forestatrisk boundary
# ----------------------------------------------------------------------


@pytest.fixture()
def spy_interpolate_rho(monkeypatch):
    """Capture far.interpolate_rho's kwargs instead of resampling for real."""
    import forestatrisk as far

    captured = {}

    def _spy(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(far, "interpolate_rho", _spy)
    return captured


@pytest.fixture()
def stub_mcmc(monkeypatch):
    """Return posteriors of the right shape without running the sampler."""
    import spatialrisk.mlmodels.icar_model as icar_module

    def _stub(*args, **kwargs):
        return {
            "betas": np.array([0.1, 0.2]),
            "rho": np.array([0.0]),
            "Vrho": 1.0,
            "deviance": 1.0,
            "worker_pid": os.getpid(),
        }

    monkeypatch.setattr(icar_module, "run_icar_mcmc", _stub)


def test_interpolate_rho_is_handed_absolute_paths(
    raster, temp_cwd, spy_interpolate_rho, stub_mcmc
):
    """A relative output folder must not leak into the library's path maths."""
    model = _model(ICARModel, raster)

    # A relative folder is the worst realistic case and the one that used to
    # send rho_orig.tif to the CWD.
    model.fit(folder="icar_out")

    output_file = spy_interpolate_rho["output_file"]
    assert os.path.isabs(output_file)
    # The load-bearing consequence: interpolate_rho places its unrequested
    # sibling next to output_file with os.path.dirname(), which returns "" for a
    # bare name and lands the file in the CWD.
    sibling = Path(os.path.dirname(output_file)) / "rho_orig.tif"
    assert sibling.parent == (temp_cwd / "icar_out").resolve()

    # The input raster crosses the same boundary and is reopened by name.
    assert os.path.isabs(spy_interpolate_rho["input_raster"])

    # rho_path is what apply() reopens later, possibly from another CWD.
    assert Path(model.rho_path).is_absolute()
    assert Path(model.rho_path) == Path(output_file)

    # Only the folder that was asked for was created in the CWD.
    assert [p.name for p in temp_cwd.iterdir()] == ["icar_out"]
