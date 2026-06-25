import numpy as np
import pytest

rasterio = pytest.importorskip("rasterio")
gpd = pytest.importorskip("geopandas")


def _write_raster(path, array, *, nodata=255, crs="EPSG:3857"):
    from rasterio.transform import from_origin
    transform = from_origin(0, array.shape[0], 1, 1)  # 1x1 m pixels
    with rasterio.open(
        path, "w", driver="GTiff", height=array.shape[0], width=array.shape[1],
        count=1, dtype="uint8", nodata=nodata, crs=crs, transform=transform,
    ) as dst:
        dst.write(array, 1)


def test_generate_points_random_inside_mask(tmp_path):
    from spatialrisk.sampling.service import generate_points

    strata = np.zeros((20, 20), dtype="uint8")
    strata[:, 10:] = 1
    mask = np.ones((20, 20), dtype="uint8")
    mask[0:5, :] = 0  # exclude the top 5 rows
    rpath, mpath = tmp_path / "strata.tif", tmp_path / "mask.tif"
    _write_raster(rpath, strata)
    _write_raster(mpath, mask)

    gdf = generate_points(rpath, mpath, strategy="random", n_samples=50, seed=1)
    assert len(gdf) == 50
    assert set(gdf.columns) >= {"strata", "row", "col", "geometry"}
    assert (gdf["row"] >= 5).all()                 # masked rows excluded
    assert gdf.crs.to_epsg() == 3857


def test_generate_points_stratified_deforisk(tmp_path):
    from spatialrisk.sampling.service import generate_points

    strata = np.zeros((40, 40), dtype="uint8")
    strata[:, 20:] = 1
    mask = np.ones((40, 40), dtype="uint8")
    rpath, mpath = tmp_path / "s.tif", tmp_path / "m.tif"
    _write_raster(rpath, strata)
    _write_raster(mpath, mask)

    gdf = generate_points(
        rpath, mpath, strategy="stratified", n_samples=100,
        allocation="deforisk", seed=1,
    )
    assert (gdf["strata"] == 1).sum() == 100
    assert (gdf["strata"] == 0).sum() == 100


def test_generate_points_systematic_spacing(tmp_path):
    from spatialrisk.sampling.service import generate_points

    strata = np.zeros((40, 40), dtype="uint8")
    mask = np.ones((40, 40), dtype="uint8")
    rpath, mpath = tmp_path / "s.tif", tmp_path / "m.tif"
    _write_raster(rpath, strata)
    _write_raster(mpath, mask)

    # 1 m pixels, spacing 10 m -> step 10 px -> rows/cols at 0,10,20,30 (4x4).
    gdf = generate_points(
        rpath, mpath, strategy="systematic", n_samples=None, spacing_m=10.0,
    )
    assert sorted(gdf["row"].unique().tolist()) == [0, 10, 20, 30]
    assert sorted(gdf["col"].unique().tolist()) == [0, 10, 20, 30]
    assert len(gdf) == 16
