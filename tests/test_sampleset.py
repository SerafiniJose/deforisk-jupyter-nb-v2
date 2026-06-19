"""Domain tests for SampleSet.generate() and load_table()."""

from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin

from spatialrisk.dataset import Dataset
from spatialrisk.sampleset import SampleSet


class _Var:
    """Minimal stand-in for a LocalRasterVar (Dataset fields are typed Any)."""

    def __init__(self, name, path, year=None):
        self.name = name
        self.path = path
        self.year = year


class _Proj:
    """Minimal stand-in for Project as used by Dataset.validate()/to_dataframe()."""

    def __init__(self):
        self.datasets = {}

    def is_temporal(self, name):
        return False


def _write_raster(path, array):
    transform = from_origin(0, 10, 1, 1)  # 1x1 px, origin top-left
    with rasterio.open(
        path, "w", driver="GTiff", height=array.shape[0], width=array.shape[1],
        count=1, dtype="float32", crs="EPSG:3857", transform=transform, nodata=-9999,
    ) as dst:
        dst.write(array.astype("float32"), 1)


def _make_dataset(tmp_path):
    # 10x10 target: top half deforested (1), bottom half forest (0).
    target = np.zeros((10, 10), dtype="float32")
    target[:5, :] = 1.0
    feat = np.arange(100, dtype="float32").reshape(10, 10)
    tpath = tmp_path / "target.tif"
    fpath = tmp_path / "feat.tif"
    _write_raster(tpath, target)
    _write_raster(fpath, feat)
    proj = _Proj()
    ds = Dataset(project=proj, name="ds")
    ds.target = _Var("forest_loss", tpath)
    ds.features = [_Var("altitude", fpath)]
    proj.datasets["ds"] = ds
    return proj


def test_generate_writes_table_and_points(tmp_path):
    proj = _make_dataset(tmp_path)
    ss = SampleSet(
        project=proj, name="s1", dataset_name="ds",
        strategy="random", n_samples=40, seed=1,
        table_path=tmp_path / "s1.csv", points_path=tmp_path / "s1.gpkg",
    )
    ss.generate()

    # Files written
    assert ss.table_path.exists()
    assert ss.points_path.exists()

    # Denormalized metadata populated from the source dataset
    assert ss.target_name == "forest_loss"
    assert ss.feature_names == ["altitude"]

    # Table columns: target + features + bookkeeping
    df = ss.load_table()
    assert "forest_loss" in df.columns
    assert "altitude" in df.columns
    assert len(df) == ss.n_total
    assert ss.n_event + ss.n_forest == ss.n_total
    assert ss.n_event == int((df["forest_loss"] == 1).sum())

    # Points layer has one geometry per sampled row and a canonical target column
    import geopandas as gpd

    gdf = gpd.read_file(ss.points_path)
    assert len(gdf) == ss.n_total
    assert "target" in gdf.columns
    assert int((gdf["target"] == 1).sum()) == ss.n_event
    assert gdf.crs is not None


def test_generate_is_reproducible_with_seed(tmp_path):
    proj = _make_dataset(tmp_path)
    kwargs = dict(
        project=proj, dataset_name="ds", strategy="random", n_samples=30, seed=7,
    )
    a = SampleSet(name="a", table_path=tmp_path / "a.csv",
                  points_path=tmp_path / "a.gpkg", **kwargs).generate()
    b = SampleSet(name="b", table_path=tmp_path / "b.csv",
                  points_path=tmp_path / "b.gpkg", **kwargs).generate()
    pd.testing.assert_frame_equal(a.load_table(), b.load_table())
