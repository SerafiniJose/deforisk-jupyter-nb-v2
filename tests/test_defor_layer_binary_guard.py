"""MW and JNR reject a non-binary forest-loss layer at every entry point.

The guard runs as soon as the layer is resolved, before any raster processing,
so these tests need no stubbing: if the guard fires, the heavy steps are never
reached. apply() is covered as well as fit() because it runs on validation and
forecast datasets whose layer fit() never saw.
"""

import numpy as np
import pytest

rasterio = pytest.importorskip("rasterio")

from spatialrisk.dataset import Dataset  # noqa: E402
from spatialrisk.mlmodels import JNRBenchmarkModel, MWModel  # noqa: E402
from spatialrisk.project import Project  # noqa: E402, F401  (resolves var refs)
from spatialrisk.variables.local_raster_var import LocalRasterVar  # noqa: E402
from spatialrisk.variables.models import RasterType  # noqa: E402

LocalRasterVar.model_rebuild()


def _write(path, array, dtype="uint8"):
    from rasterio.transform import from_origin

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=array.shape[0],
        width=array.shape[1],
        count=1,
        dtype=dtype,
        crs="EPSG:3857",
        transform=from_origin(0, array.shape[0], 1, 1),
    ) as dst:
        dst.write(array.astype(dtype), 1)
    return path


def _var(name, path, raster_type=RasterType.categorical):
    return LocalRasterVar(name=name, path=path, raster_type=raster_type)


@pytest.fixture
def categorical_defor_dataset(tmp_path):
    """Dataset whose forest-loss target is a 1/2/3 multi-period stack."""
    # Big enough that the raster steps would run happily if the guard were
    # absent, so these tests fail on the missing guard and nothing else.
    stack = np.zeros((16, 16), dtype="uint8")
    stack[2:6, 2:6] = 1
    stack[8:12, 8:12] = 2  # a second period — silently dropped without a guard
    stack[14, 14] = 3
    binary = np.zeros((16, 16), dtype="uint8")
    binary[2:8, 2:8] = 1
    edge = (np.indices((16, 16)).sum(axis=0) * 30).astype("uint16")

    target = _var("defostack", _write(tmp_path / "defostack.tif", stack))
    features = [
        _var(
            "forest_edge",
            _write(tmp_path / "edge.tif", edge, dtype="uint16"),
            RasterType.continuous,
        ),
        _var("forest", _write(tmp_path / "forest.tif", binary)),
        _var("subj", _write(tmp_path / "subj.tif", binary)),
    ]
    return Dataset(project=None, name="calibration", target=target, features=features)


def test_jnr_fit_rejects_a_non_binary_layer(categorical_defor_dataset, tmp_path):
    """Training on a 1/2/3 stack fails instead of dropping period 2 silently."""
    model = JNRBenchmarkModel(name="calibration")

    with pytest.raises(ValueError, match="not binary"):
        model.fit(dataset=categorical_defor_dataset, folder=tmp_path)


def test_jnr_apply_rejects_a_non_binary_layer(categorical_defor_dataset, tmp_path):
    """A fitted model still checks the layer of each period it is applied to."""
    model = JNRBenchmarkModel(name="calibration")
    model.dist_thresh = 120.0
    model.dist_bins = [0.0, 60.0, 120.0]

    with pytest.raises(ValueError, match="not binary"):
        model.apply(
            output_file=tmp_path / "vuln.tif",
            dataset=categorical_defor_dataset,
            time_interval=5,
        )


def test_mw_fit_rejects_a_non_binary_layer(categorical_defor_dataset, tmp_path):
    """Moving-window training rejects the same layer JNR does."""
    model = MWModel(name="calibration", blk_rows=4, win_size_list=[3])

    with pytest.raises(ValueError, match="not binary"):
        model.fit(dataset=categorical_defor_dataset, time_interval=5, folder=tmp_path)


def test_mw_apply_rejects_a_non_binary_layer(categorical_defor_dataset, tmp_path):
    """Every period the fitted windows are applied to is checked too."""
    model = MWModel(name="calibration", blk_rows=4, win_size_list=[3])
    model.dist_thresh = 100_000.0  # keep every pixel, so apply() would succeed
    ldefrate = _write(
        tmp_path / "ldefrate_mw_3.tif",
        np.full((16, 16), 1000, dtype="uint16"),
        "uint16",
    )
    model.ldefrate_files = {"3": ldefrate}

    with pytest.raises(ValueError, match="not binary"):
        model.apply(
            dataset=categorical_defor_dataset,
            time_interval=5,
            output_folder=tmp_path,
        )
