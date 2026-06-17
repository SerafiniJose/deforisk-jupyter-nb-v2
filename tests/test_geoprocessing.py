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
from odc.geo.geobox import GeoBox
from rasterio.transform import from_origin

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


# --------------------------------------------------------------------------- #
# Phase F6: VariableHandle -> stateless-geoprocessing bridge (session-side)
# --------------------------------------------------------------------------- #
from spatialrisk.document import VariableId
from spatialrisk.persistence import LocalFSProjectStore
from spatialrisk.session import ProjectSession, VariableHandle


def test_session_folders_works_without_a_store(monkeypatch, tmp_path):
    """`folders()` must resolve a data_root even when store is None."""
    from spatialrisk import project as project_mod

    # Redirect the legacy default so the fallback writes under tmp_path.
    monkeypatch.setattr(project_mod, "downloads_folder", tmp_path / "data")

    session = ProjectSession.create("p", store=None, gee=None)
    box = session.folders()
    assert box.processed_data_folder == tmp_path / "data" / "p" / "data"
    assert box.processed_data_folder.exists()


def test_variable_handle_to_ref_carries_source():
    session = ProjectSession.create("p", store=None, gee=None)
    session.add_local_raster(LocalRasterSpec(
        name="dem", path="/raw/dem.tif", raster_type=RasterType.continuous,
    ))
    handle = session.get_variable_handle("dem", source="raw")
    ref = handle.to_ref()
    assert isinstance(ref, VariableId)
    assert ref == VariableId(source="raw", name="dem", year=None)


def test_handle_reproject_and_match_delegates_registers_and_returns_handle(
    tmp_path, monkeypatch
):
    from spatialrisk import geoprocessing

    store = LocalFSProjectStore(data_root=tmp_path)
    session = ProjectSession.create("proj", store=store, gee=None)

    # A real base raster on disk so _base_geobox() can open it (EPSG:4326).
    base_path = tmp_path / "base.tif"
    _write_raster(base_path, np.array([[1, 2], [3, 4]], dtype=np.uint8))
    session.add_local_raster(LocalRasterSpec(
        name="base", path=str(base_path), raster_type=RasterType.continuous,
    ), key="base")
    session.set_base_raster(VariableId(source="raw", name="base"))

    # The raw input to reproject.
    dem_path = tmp_path / "dem.tif"
    _write_raster(dem_path, np.array([[5, 6], [7, 8]], dtype=np.uint8))
    session.add_local_raster(LocalRasterSpec(
        name="dem", path=str(dem_path), raster_type=RasterType.continuous,
    ))

    captured = {}

    def fake_reproject_and_match(in_spec, geobox, out_path, resampling=None):
        captured["in_spec"] = in_spec
        captured["geobox"] = geobox
        captured["out_path"] = out_path
        captured["resampling"] = resampling
        return LocalRasterSpec(
            name=in_spec.name,
            path=str(out_path),
            raster_type=in_spec.raster_type,
            processing_history=("reprojected_matched",),
            derived_from=in_spec.name,
        )

    monkeypatch.setattr(geoprocessing, "reproject_and_match", fake_reproject_and_match)

    new_handle = session.get_variable_handle("dem", source="raw").reproject_and_match()

    # The stateless seam was called with the session's processed out_path ...
    processed_dir = session.folders().processed_data_folder
    expected_out = processed_dir / "dem_reprojected_matched.tif"
    assert captured["out_path"] == str(expected_out)
    # ... and the base geobox (EPSG:4326 from the base fixture).
    assert captured["geobox"].crs.to_epsg() == 4326
    assert captured["in_spec"].path == str(dem_path)

    # The returned handle points at the freshly registered PROCESSED spec.
    assert isinstance(new_handle, VariableHandle)
    assert new_handle.source == "processed"
    assert isinstance(new_handle.spec, LocalRasterSpec)
    assert new_handle.spec.path == str(expected_out)
    # registered into processed_variables, not raw.
    assert session.get_variable("dem", source="processed").path == str(expected_out)


def test_handle_apply_post_processing_delegates_and_registers(tmp_path, monkeypatch):
    from spatialrisk import geoprocessing

    store = LocalFSProjectStore(data_root=tmp_path)
    session = ProjectSession.create("proj", store=store, gee=None)

    feat_path = tmp_path / "rivers.tif"
    _write_raster(feat_path, np.array([[1, 0], [0, 0]], dtype=np.uint8), nodata=0)
    session.add_local_raster(LocalRasterSpec(
        name="rivers", path=str(feat_path), raster_type=RasterType.categorical,
    ))

    captured = {}

    def fake_app(in_spec, post_process, out_path):
        captured["out_path"] = out_path
        return LocalRasterSpec(
            name=f"{in_spec.name}_dist",
            path=str(out_path),
            raster_type=RasterType.continuous,
            processing_history=("dist",),
            post_processing=(PostProcessing.dist,),
            derived_from=in_spec.name,
        )

    monkeypatch.setattr(geoprocessing, "apply_post_processing", fake_app)

    new_handle = (
        session.get_variable_handle("rivers", source="raw")
        .apply_post_processing(PostProcessing.dist)
    )

    processed_dir = session.folders().processed_data_folder
    expected_out = processed_dir / "rivers_dist.tif"
    assert captured["out_path"] == str(expected_out)
    assert new_handle.source == "processed"
    assert new_handle.spec.name == "rivers_dist"
    assert session.get_variable("rivers_dist", source="processed") is not None


def test_phase6_public_surface_is_complete():
    """Codifies the Phase 6 public surface so a later refactor can't silently
    drop a stateless seam.
    """
    from spatialrisk import geoprocessing

    expected = {"reproject_and_match", "rasterize_vector", "apply_post_processing"}
    present = {
        n for n in expected if callable(getattr(geoprocessing, n, None))
    }
    assert present == expected
