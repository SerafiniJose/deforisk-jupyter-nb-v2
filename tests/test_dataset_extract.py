import numpy as np
import pandas as pd
import pytest

rasterio = pytest.importorskip("rasterio")
gpd = pytest.importorskip("geopandas")
from shapely.geometry import Point


def _write_raster(path, array, nodata=-1.0, crs="EPSG:3857"):
    from rasterio.transform import from_origin
    with rasterio.open(
        path, "w", driver="GTiff", height=array.shape[0], width=array.shape[1],
        count=1, dtype="float32", nodata=nodata, crs=crs,
        transform=from_origin(0, array.shape[0], 1, 1),
    ) as dst:
        dst.write(array.astype("float32"), 1)


class _Var:
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.year = None


def _make_dataset(tmp_path):
    from spatialrisk.dataset import Dataset

    target = np.zeros((10, 10), dtype="float32")
    target[5, 5] = 1
    feat = np.arange(100, dtype="float32").reshape(10, 10)
    feat[0, 0] = -1.0  # nodata cell
    tpath, fpath = tmp_path / "t.tif", tmp_path / "f.tif"
    _write_raster(tpath, target)
    _write_raster(fpath, feat)

    ds = Dataset(project=None, name="ds")
    ds.target = _Var("target", tpath)
    ds.features = [_Var("altitude", fpath)]
    return ds


def test_extract_returns_expected_schema_and_values(tmp_path):
    ds = _make_dataset(tmp_path)
    # pixel (row=5,col=5) centre and (row=2,col=3) centre, in EPSG:3857
    pts = gpd.GeoDataFrame(
        geometry=[Point(5.5, 10 - 5 - 0.5), Point(3.5, 10 - 2 - 0.5)],
        crs="EPSG:3857",
    )
    df = ds.extract_at_points(pts)
    assert list(df.columns) == ["target", "altitude", "cell_id", "trial"]
    assert set(df["target"]) <= {0.0, 1.0}
    assert (df["trial"] == 1).all()


def test_extract_drops_nodata_points(tmp_path):
    ds = _make_dataset(tmp_path)
    # (row=0,col=0) is nodata in the feature -> dropped.
    pts = gpd.GeoDataFrame(geometry=[Point(0.5, 9.5)], crs="EPSG:3857")
    df = ds.extract_at_points(pts)
    assert len(df) == 0


def test_extract_uses_each_rasters_own_grid(tmp_path):
    # Target and feature on DIFFERENT grids: the feature value must be read at
    # the point's location on the FEATURE grid, not the target grid.
    from spatialrisk.dataset import Dataset
    from rasterio.transform import from_origin

    target = np.ones((10, 10), dtype="float32")
    tpath = tmp_path / "t.tif"
    _write_raster(tpath, target)  # from_origin(0, 10, 1, 1)

    feat = np.arange(100, dtype="float32").reshape(10, 10)  # value = row*10 + col
    fpath = tmp_path / "f.tif"
    with rasterio.open(
        fpath, "w", driver="GTiff", height=10, width=10, count=1,
        dtype="float32", nodata=-1.0, crs="EPSG:3857",
        transform=from_origin(2, 8, 1, 1),  # shifted grid
    ) as dst:
        dst.write(feat, 1)

    ds = Dataset(project=None, name="ds")
    ds.target = _Var("target", tpath)
    ds.features = [_Var("f", fpath)]

    # World point (3.5, 4.5): feature grid (origin 2,8) -> (row=3,col=1) -> 31.
    # Target grid (origin 0,10) would be (row=5,col=3) -> wrong value 53.
    pts = gpd.GeoDataFrame(geometry=[Point(3.5, 4.5)], crs="EPSG:3857")
    df = ds.extract_at_points(pts)
    assert len(df) == 1
    assert df["f"].iloc[0] == 31.0


def test_extract_drops_out_of_bounds_points(tmp_path):
    # A point far outside the extent is dropped, not an IndexError.
    ds = _make_dataset(tmp_path)
    pts = gpd.GeoDataFrame(geometry=[Point(100.0, 100.0)], crs="EPSG:3857")
    df = ds.extract_at_points(pts)
    assert len(df) == 0


def test_extract_reprojects_points_to_raster_crs(tmp_path):
    # Points given in EPSG:4326 must extract the same values as the equivalent
    # EPSG:3857 points (the dataset rasters are EPSG:3857).
    ds = _make_dataset(tmp_path)
    pts_3857 = gpd.GeoDataFrame(
        geometry=[Point(5.5, 4.5), Point(3.5, 7.5)], crs="EPSG:3857"
    )
    df_native = ds.extract_at_points(pts_3857)
    pts_4326 = pts_3857.to_crs("EPSG:4326")
    df_reproj = ds.extract_at_points(pts_4326)
    pd.testing.assert_frame_equal(df_native, df_reproj)
