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


from spatialrisk.document import LocalVectorSpec
from spatialrisk.variables.models import RasterizationMethod


def test_rasterize_vector_to_base_grid(tmp_path):
    import geopandas as gpd
    from shapely.geometry import box

    from spatialrisk import geoprocessing

    # A small polygon covering the lower-left quadrant of a 2x2 grid.
    shp = tmp_path / "poly.shp"
    gpd.GeoDataFrame(
        {"id": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326"
    ).to_file(shp)

    base_geobox = GeoBox.from_bbox((0, 0, 2, 2), crs="EPSG:4326", resolution=1)

    in_spec = LocalVectorSpec(
        name="towns", year=2020, path=str(shp),
        rasterization_method=RasterizationMethod.binary,
    )
    out_path = tmp_path / "towns.tif"

    out_spec = geoprocessing.rasterize_vector(
        in_spec, base_geobox=base_geobox, out_path=str(out_path),
    )

    assert isinstance(out_spec, LocalRasterSpec)
    assert out_spec.path == str(out_path)
    assert out_spec.raster_type == RasterType.continuous  # binary -> continuous
    assert "rasterized" in out_spec.processing_history
    assert out_spec.year == 2020

    with rasterio.open(out_path) as r:
        assert (r.height, r.width) == base_geobox.shape == (2, 2)
        data = r.read(1)
    # exactly one cell (the covered quadrant) is burned to 1
    assert data.sum() == 1


def test_rasterize_vector_unique_yields_categorical(tmp_path):
    import geopandas as gpd
    from shapely.geometry import box

    from spatialrisk import geoprocessing

    shp = tmp_path / "poly2.shp"
    gpd.GeoDataFrame(
        {"id": [1, 2]}, geometry=[box(0, 0, 1, 1), box(1, 1, 2, 2)], crs="EPSG:4326"
    ).to_file(shp)
    base_geobox = GeoBox.from_bbox((0, 0, 2, 2), crs="EPSG:4326", resolution=1)

    # The spec carries a valid (required) method; the `unique` OVERRIDE arg wins.
    spec = LocalVectorSpec(name="subj", path=str(shp), rasterization_method=RasterizationMethod.binary)
    out_spec = geoprocessing.rasterize_vector(
        spec, base_geobox=base_geobox, out_path=str(tmp_path / "subj.tif"),
        rasterization_method=RasterizationMethod.unique,
    )
    assert out_spec.raster_type == RasterType.categorical


def test_rasterize_vector_requires_a_method(tmp_path):
    import geopandas as gpd
    from shapely.geometry import box

    from spatialrisk import geoprocessing

    shp = tmp_path / "poly3.shp"
    gpd.GeoDataFrame(
        {"id": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326"
    ).to_file(shp)
    base_geobox = GeoBox.from_bbox((0, 0, 2, 2), crs="EPSG:4326", resolution=1)

    # rasterization_method is REQUIRED on a valid LocalVectorSpec, so the only way
    # _method is None is a validation-bypassing model_construct. The guard must
    # still raise rather than silently producing a bad raster.
    spec = LocalVectorSpec.model_construct(
        kind="local_vector", name="x", path=str(shp), rasterization_method=None
    )
    with pytest.raises(ValueError, match="rasterization_method"):
        geoprocessing.rasterize_vector(
            spec, base_geobox=base_geobox, out_path=str(tmp_path / "x.tif"),
        )


from spatialrisk.variables.models import PostProcessing


def test_apply_post_processing_dist_writes_distance_raster(tmp_path):
    from spatialrisk import geoprocessing

    # A 4x4 binary mask: a single feature pixel (==1) at (0,0), rest background.
    arr = np.zeros((4, 4), dtype=np.uint8)
    arr[0, 0] = 1
    src = tmp_path / "feat.tif"
    _write_raster(src, arr, nodata=0)

    in_spec = LocalRasterSpec(
        name="rivers", path=str(src), raster_type=RasterType.categorical,
    )
    out_path = tmp_path / "rivers_dist.tif"

    out_spec = geoprocessing.apply_post_processing(
        in_spec, PostProcessing.dist, out_path=str(out_path),
    )

    assert isinstance(out_spec, LocalRasterSpec)
    assert out_spec.path == str(out_path)
    assert out_spec.name == "rivers_dist"
    assert out_spec.raster_type == RasterType.continuous
    assert "dist" in out_spec.processing_history
    assert PostProcessing.dist in out_spec.post_processing
    assert out_path.exists()

    with rasterio.open(out_path) as r:
        d = r.read(1)
    assert d.shape == (4, 4)
    # Under USE_INPUT_NODATA=YES (input_nodata=True, nodata=0) the value-0
    # background is masked to nodata in the output; the feature pixel is the only
    # valid computed cell and its distance to the nearest feature (itself) is 0.
    assert d[0, 0] == 0


def test_apply_post_processing_edge_passes_values_zero(tmp_path, monkeypatch):
    from spatialrisk import geoprocessing

    arr = np.zeros((2, 2), dtype=np.uint8)
    arr[0, 0] = 1
    src = tmp_path / "e.tif"
    _write_raster(src, arr, nodata=0)

    captured = {}

    def fake_dist(input_file, dist_file, values, nodata, max_distance_value,
                  input_nodata, verbose):
        captured["values"] = values
        _write_raster(dist_file, np.zeros((2, 2), dtype="uint32"), nodata=0)

    monkeypatch.setattr(
        geoprocessing, "distance_to_edge_gdal_no_mask", fake_dist
    )

    spec = LocalRasterSpec(name="roads", path=str(src), raster_type=RasterType.categorical)
    geoprocessing.apply_post_processing(
        spec, PostProcessing.edge, out_path=str(tmp_path / "roads_edge.tif"),
    )
    assert captured["values"] == 0  # edge -> distance to feature pixels (values=0)


def test_apply_post_processing_rejects_unknown_step(tmp_path):
    from spatialrisk import geoprocessing

    arr = np.zeros((2, 2), dtype=np.uint8)
    src = tmp_path / "u.tif"
    _write_raster(src, arr, nodata=0)
    spec = LocalRasterSpec(name="u", path=str(src), raster_type=RasterType.categorical)

    with pytest.raises(ValueError, match="post-processing"):
        geoprocessing.apply_post_processing(spec, "bogus", out_path=str(tmp_path / "o.tif"))


import inspect


def test_no_stateless_function_takes_a_project_param():
    from spatialrisk import geoprocessing

    for fn in (
        geoprocessing.reproject_and_match,
        geoprocessing.rasterize_vector,
        geoprocessing.apply_post_processing,
    ):
        params = inspect.signature(fn).parameters
        assert "out_path" in params, f"{fn.__name__} must take an explicit out_path"
        assert "project" not in params, f"{fn.__name__} must not take a project"
        assert "self" not in params, f"{fn.__name__} must be a free function"


def test_module_source_has_no_live_project_reachthrough():
    from spatialrisk import geoprocessing

    text = open(geoprocessing.__file__).read()
    for forbidden in ("self.project", ".project.folders", ".project.save", "project="):
        assert forbidden not in text, f"forbidden live-Project reach-through: {forbidden!r}"
