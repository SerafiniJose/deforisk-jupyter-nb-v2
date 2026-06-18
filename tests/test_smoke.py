"""Smoke tests: the package imports and core domain objects work in-memory.

This is the minimal safety net for the refactor that follows -- it proves the
heavy stack (ee, gdal, rasterio, forestatrisk, rapids, ...) imports cleanly in
the environment and that two representative domain operations run without any
filesystem or Earth Engine access.
"""

import warnings

import numpy as np


def test_public_api_imports():
    from spatialrisk import (  # noqa: F401
        Dataset,
        GLMModel,
        ICARModel,
        JNRBenchmarkModel,
        MWModel,
        Project,
        RFModel,
        Sampling,
        SamplingStrategy,
        rmj,
    )


def test_build_local_raster_var_in_memory():
    from spatialrisk import Project
    from spatialrisk.variables import LocalRasterVar
    from spatialrisk.variables.models import RasterType

    # In the real workflow a Project is created/loaded first, and Project.save()/
    # load() resolve the Variable<->Project Pydantic forward references via
    # _ensure_model_schemas(). The notebooks always do this before constructing
    # variables. We trigger the same resolution here so a variable can be built
    # standalone. (That this is required -- instead of happening at import -- is
    # the load-order fragility flagged as hotspot #2 in the architecture
    # assessment; it is intentionally left for a later refactor step.)
    Project._ensure_model_schemas()

    with warnings.catch_warnings():
        # The path validator warns (not errors) when the file is absent -- expected here.
        warnings.simplefilter("ignore", UserWarning)
        var = LocalRasterVar(
            name="smoke_forest",
            path="/nonexistent/smoke.tif",
            raster_type=list(RasterType)[0],
        )

    assert var.name == "smoke_forest"
    assert var.active is True  # Variable default
    assert var.project is None  # no live Project attached


def test_sampling_runs_on_in_memory_indices():
    from spatialrisk import Sampling

    valid_indices = (np.arange(100), np.arange(100))
    sampling = Sampling(strategy="random", n_samples=10, seed=42)

    rows, cols = sampling.sample_indices(valid_indices)

    assert len(rows) == 10
    assert len(cols) == 10
    assert set(rows.tolist()).issubset(set(range(100)))


def test_import_spatialrisk_succeeds():
    import importlib

    import spatialrisk

    importlib.reload(spatialrisk)
    assert hasattr(spatialrisk, "Prediction")


def test_prediction_is_same_object_as_module():
    from spatialrisk import Prediction as TopLevelPrediction
    from spatialrisk.predictions.prediction import Prediction as ModulePrediction

    assert TopLevelPrediction is ModulePrediction
