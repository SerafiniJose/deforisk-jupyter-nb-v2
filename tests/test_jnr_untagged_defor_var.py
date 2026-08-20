"""JNR fit() accepts any selected forest-loss layer, tagged or not.

``defor_var`` is a free selector over the dataset's layers, so a valid binary
forest-loss raster that was uploaded or built outside ``processing.py`` carries
no ``'deforestation'`` tag.  MW runs the same computation without a tag check,
and JNR's own apply() never checked — fit() must not reject on provenance.
"""

import numpy as np
import pytest

rasterio = pytest.importorskip("rasterio")

from spatialrisk.dataset import Dataset  # noqa: E402
from spatialrisk.mlmodels import JNRBenchmarkModel  # noqa: E402
from spatialrisk.project import Project  # noqa: E402, F401  (resolves var refs)
from spatialrisk.variables.local_raster_var import LocalRasterVar  # noqa: E402
from spatialrisk.variables.models import RasterType  # noqa: E402

LocalRasterVar.model_rebuild()


class _FakeProject:
    base_raster = None


def _write_binary(path):
    """A valid 0/1 forest-loss raster — valid data, just no tags."""
    from rasterio.transform import from_origin

    arr = np.zeros((8, 8), dtype="uint8")
    arr[2:5, 2:5] = 1
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=8,
        width=8,
        count=1,
        dtype="uint8",
        crs="EPSG:3857",
        transform=from_origin(0, 8, 1, 1),
    ) as dst:
        dst.write(arr, 1)
    return path


@pytest.fixture
def untagged_dataset(tmp_path):
    """Dataset whose forest-loss target carries no tags at all."""
    project = _FakeProject()
    target = LocalRasterVar(
        name="my_forest_loss",
        path=_write_binary(tmp_path / "my_forest_loss.tif"),
        raster_type=RasterType.categorical,
        tags=[],
    )
    edge = LocalRasterVar(
        name="forest_edge",
        path=_write_binary(tmp_path / "forest_edge.tif"),
        raster_type=RasterType.continuous,
    )
    return Dataset(project=project, name="calibration", target=target, features=[edge])


@pytest.fixture
def stub_rmj(monkeypatch):
    """Replace the heavy raster steps so fit() only exercises its own logic."""
    from spatialrisk import rmj

    calls = {}

    def fake_dist_edge_threshold(**kwargs):
        calls["defor_file"] = kwargs["defor_file"]
        return {"dist_thresh": 120.0}

    def fake_compute_dist_bins(**kwargs):
        return [0.0, 60.0, 120.0]

    monkeypatch.setattr(rmj.deforrate, "dist_edge_threshold", fake_dist_edge_threshold)
    monkeypatch.setattr(rmj, "compute_dist_bins", fake_compute_dist_bins)
    return calls


def test_fit_accepts_untagged_forest_loss_layer(untagged_dataset, stub_rmj, tmp_path):
    """An untagged forest-loss layer trains instead of raising on its tags."""
    model = JNRBenchmarkModel(name="calibration")

    model.fit(dataset=untagged_dataset, folder=tmp_path)

    assert model.trained
    assert model.dist_thresh == 120.0
    assert str(stub_rmj["defor_file"]).endswith("my_forest_loss.tif")


def test_fit_accepts_untagged_defor_var_feature(untagged_dataset, stub_rmj, tmp_path):
    """The same holds when the layer is picked by name via defor_var."""
    picked = LocalRasterVar(
        name="loss_from_upload",
        path=_write_binary(tmp_path / "loss_from_upload.tif"),
        raster_type=RasterType.categorical,
        tags=["uploaded"],
    )
    untagged_dataset.features.append(picked)
    model = JNRBenchmarkModel(name="calibration", defor_var="loss_from_upload")

    model.fit(dataset=untagged_dataset, folder=tmp_path)

    assert model.trained
    assert str(stub_rmj["defor_file"]).endswith("loss_from_upload.tif")
