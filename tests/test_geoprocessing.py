"""Phase 6: stateless variable-geoprocessing functions.

Each function takes an EXPLICIT out_path + input spec + base/geobox and RETURNS
a new VariableSpec. No function references a live Project. Numeric/geospatial
behavior is the verbatim current path (geo_utils / processing primitives).
"""
import importlib


def test_geoprocessing_module_imports():
    mod = importlib.import_module("spatialrisk.geoprocessing")
    # The four stateless seams this phase delivers.
    assert callable(mod.reproject_and_match)
    assert callable(mod.rasterize_vector)
    assert callable(mod.apply_post_processing)


def test_geoprocessing_does_not_import_project_or_ee():
    """Leaf module: no live-Project reach-through, no runtime ee import."""
    import sys

    sys.modules.pop("spatialrisk.geoprocessing", None)
    importlib.import_module("spatialrisk.geoprocessing")
    mod = sys.modules["spatialrisk.geoprocessing"]
    src = mod.__file__
    text = open(src).read()
    assert "self.project" not in text
    assert ".project.folders" not in text
    assert "import ee" not in text


import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from odc.geo.geobox import GeoBox

from spatialrisk.document import LocalRasterSpec
from spatialrisk.variables.models import RasterType


def _write_raster(path, arr, *, crs="EPSG:4326", transform=None, nodata=255):
    if transform is None:
        transform = from_origin(0, arr.shape[0], 1, 1)
    profile = {
        "driver": "GTiff", "height": arr.shape[0], "width": arr.shape[1],
        "count": 1, "dtype": arr.dtype.name, "nodata": nodata,
        "crs": crs, "transform": transform,
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr, 1)


def test_reproject_and_match_writes_to_explicit_out_path_with_target_geobox(tmp_path):
    from spatialrisk import geoprocessing

    src = tmp_path / "src.tif"
    _write_raster(src, np.array([[1, 2, 3, 4]] * 4, dtype=np.uint8))

    # Target grid: same CRS, coarser 2x2 grid over the same 4x4 extent.
    geobox = GeoBox.from_bbox((0, 0, 4, 4), crs="EPSG:4326", resolution=2)

    in_spec = LocalRasterSpec(
        name="dem", path=str(src), raster_type=RasterType.continuous,
    )
    out_path = tmp_path / "dem_reprojected_matched.tif"

    out_spec = geoprocessing.reproject_and_match(
        in_spec, geobox=geobox, out_path=str(out_path),
    )

    # Returned a new spec, did not mutate input, wrote to the explicit path.
    assert isinstance(out_spec, LocalRasterSpec)
    assert out_spec.path == str(out_path)
    assert out_path.exists()
    assert in_spec.path == str(src)  # frozen input untouched

    # Golden geobox/CRS: output grid matches the requested geobox exactly.
    with rasterio.open(out_path) as r:
        assert r.crs.to_epsg() == geobox.crs.to_epsg() == 4326
        assert (r.height, r.width) == geobox.shape == (2, 2)
        assert abs(r.transform.a) == abs(geobox.resolution.x) == 2.0

    assert out_spec.default_crs == "EPSG:4326"
    assert out_spec.default_resolution == 2.0
    assert "reprojected_matched" in out_spec.processing_history


def test_reproject_and_match_auto_resampling_by_raster_type(tmp_path, monkeypatch):
    from spatialrisk import geoprocessing

    src = tmp_path / "cat.tif"
    _write_raster(src, np.array([[1, 1], [0, 0]], dtype=np.uint8))
    geobox = GeoBox.from_bbox((0, 0, 2, 2), crs="EPSG:4326", resolution=1)

    captured = {}

    def fake_xr_reproject(raster_path, geobox, resampling_method, output_path, **kw):
        captured["resampling"] = resampling_method
        _write_raster(output_path, np.array([[1, 1], [0, 0]], dtype=np.uint8))

    monkeypatch.setattr(geoprocessing, "xr_reproject", fake_xr_reproject)

    spec = LocalRasterSpec(name="c", path=str(src), raster_type=RasterType.categorical)
    geoprocessing.reproject_and_match(
        spec, geobox=geobox, out_path=str(tmp_path / "o.tif"),
    )
    assert captured["resampling"] == "nearest"
