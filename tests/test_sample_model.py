import numpy as np
import pytest

rasterio = pytest.importorskip("rasterio")
gpd = pytest.importorskip("geopandas")


def _write_raster(path, array, nodata=255, crs="EPSG:3857"):
    from rasterio.transform import from_origin
    with rasterio.open(
        path, "w", driver="GTiff", height=array.shape[0], width=array.shape[1],
        count=1, dtype="uint8", nodata=nodata, crs=crs,
        transform=from_origin(0, array.shape[0], 1, 1),
    ) as dst:
        dst.write(array, 1)


class _Var:
    def __init__(self, path):
        self.path = path


class _StubProject:
    def __init__(self, variables):
        self._vars = variables

    def get_variable(self, name, year=None):
        return self._vars[name]


def test_sample_generate_round_trip(tmp_path):
    from spatialrisk.sample import Sample

    strata = np.zeros((30, 30), dtype="uint8")
    strata[:, 15:] = 1
    mask = np.ones((30, 30), dtype="uint8")
    rpath, mpath = tmp_path / "strata.tif", tmp_path / "mask.tif"
    _write_raster(rpath, strata)
    _write_raster(mpath, mask)

    project = _StubProject({"target": _Var(rpath), "forest_mask": _Var(mpath)})
    sample = Sample(
        project=project, name="calib", raster_var_name="target",
        mask_var_name="forest_mask", strategy="stratified", n_samples=100,
        allocation="equal", seed=1, points_path=tmp_path / "calib.gpkg",
    )
    sample.generate()

    assert sample.n_total == 100
    assert set(sample.class_counts) == {"0", "1"}
    assert sample.crs is not None
    gdf = sample.load_points()
    assert len(gdf) == 100
    assert "strata" in gdf.columns


def test_sample_model_dump_excludes_project(tmp_path):
    from spatialrisk.sample import Sample
    s = Sample(project=object(), name="s", raster_var_name="t", strategy="random")
    assert "project" not in s.model_dump()


def test_sample_spacing_generate(tmp_path):
    from spatialrisk.sample import Sample

    strata = np.zeros((40, 40), dtype="uint8")
    mask = np.ones((40, 40), dtype="uint8")
    rpath, mpath = tmp_path / "s.tif", tmp_path / "m.tif"
    _write_raster(rpath, strata)
    _write_raster(mpath, mask)

    project = _StubProject({"target": _Var(rpath), "forest_mask": _Var(mpath)})
    sample = Sample(
        project=project, name="grid", raster_var_name="target",
        mask_var_name="forest_mask", strategy="systematic", n_samples=None,
        spacing_m=10.0, points_path=tmp_path / "grid.gpkg",
    )
    sample.generate()

    # 1 m pixels, spacing 10 m on a 40x40 grid -> 4x4 = 16 points.
    assert sample.spacing_m == 10.0
    assert sample.n_total == 16
    assert "project" not in sample.model_dump()
