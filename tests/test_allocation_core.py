"""Numeric core of deforestation allocation (spatialrisk/allocation.py)."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from spatialrisk.allocation import (
    AllocationInputError,
    allocate_deforestation,
    validate_defrate_table,
)


def _write_raster(path, array, *, pixel_size=100.0, nodata=0, dtype=None):
    """Write a small UTM-like GeoTIFF (metre CRS, so pixel_area is well defined)."""
    from osgeo import gdal, osr

    gdal_dtype = dtype if dtype is not None else gdal.GDT_UInt16
    driver = gdal.GetDriverByName("GTiff")
    nrow, ncol = array.shape
    ds = driver.Create(str(path), ncol, nrow, 1, gdal_dtype)
    ds.SetGeoTransform((500000.0, pixel_size, 0.0, 5000000.0, 0.0, -pixel_size))
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(32631)  # WGS84 / UTM 31N — metres
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(float(nodata))
    band.WriteArray(array)
    band.FlushCache()
    ds = None
    return Path(path)


def _write_borders(path, *, minx=500000.0, maxy=5000000.0, size=400.0):
    """Square polygon covering the top-left 4x4 block of a 100 m raster."""
    import geopandas as gpd
    from shapely.geometry import box

    gdf = gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[box(minx, maxy - size, minx + size, maxy)],
        crs="EPSG:32631",
    )
    gdf.to_file(path)
    return Path(path)


def _dense_table(pixel_area=1.0):
    """Dense table over cat 1..3; class 1 is the no-risk class (rate_mod 0)."""
    return pd.DataFrame(
        {
            "cat": [1, 2, 3],
            "nfor": [100, 200, 300],
            "rate_mod": [0.0, 0.001, 0.003],
            "pixel_area": [pixel_area] * 3,
        }
    )


def test_allocates_expected_hectares_for_dense_table(tmp_path):
    """Annual and total hectares follow the correction-factor formula."""
    # 4x4 project area: 8 pixels of class 2, 8 of class 3.
    risk = np.full((8, 8), 1, dtype=np.uint16)
    risk[0:4, 0:2] = 2
    risk[0:4, 2:4] = 3
    riskmap = _write_raster(tmp_path / "risk.tif", risk)
    borders = _write_borders(tmp_path / "borders.shp")

    res = allocate_deforestation(
        riskmap_file=riskmap,
        defrate_table=_dense_table(),
        defor_juris_ha=1000.0,
        years_forecast=4,
        project_borders=borders,
        out_dir=tmp_path / "run",
    )

    # correction = 1000 / (1 * (100*0 + 200*0.001 + 300*0.003)) = 1000 / 1.1
    # defor_dens(cat2) = 0.001 * 1000/1.1 * 1 / 4 ; defor_dens(cat3) = 3x that
    corr = 1000.0 / 1.1
    dens2 = 0.001 * corr * 1.0 / 4
    dens3 = 0.003 * corr * 1.0 / 4
    expected_annual = 8 * dens2 + 8 * dens3

    assert res.annual_ha == pytest.approx(expected_annual, rel=1e-6)
    assert res.total_ha == pytest.approx(expected_annual * 4, rel=1e-6)
    assert res.csv_path.exists()
    assert res.defrate_path.exists()
    assert res.cropped_riskmap_path.exists()
    assert res.density_map_path is None


def test_sparse_jnr_style_table_maps_by_category_not_row(tmp_path):
    """Upstream forestatrisk indexes rows positionally; gappy cats must still map."""
    risk = np.full((8, 8), 1001, dtype=np.uint16)
    risk[0:4, 0:2] = 20000
    risk[0:4, 2:4] = 30999
    riskmap = _write_raster(tmp_path / "risk.tif", risk)
    borders = _write_borders(tmp_path / "borders.shp")
    table = pd.DataFrame(
        {
            "cat": [1001, 20000, 30999],
            "nfor": [100, 200, 300],
            "rate_mod": [0.0, 0.001, 0.003],
            "pixel_area": [1.0, 1.0, 1.0],
        }
    )

    res = allocate_deforestation(
        riskmap_file=riskmap,
        defrate_table=table,
        defor_juris_ha=1000.0,
        years_forecast=4,
        project_borders=borders,
        out_dir=tmp_path / "run",
    )

    corr = 1000.0 / 1.1
    expected_annual = 8 * (0.001 * corr / 4) + 8 * (0.003 * corr / 4)
    assert res.annual_ha == pytest.approx(expected_annual, rel=1e-6)
    assert res.warnings == []


def test_classes_missing_from_table_warn_and_allocate_zero(tmp_path):
    """Risk classes absent from the table warn and contribute nothing."""
    risk = np.full((8, 8), 2, dtype=np.uint16)
    risk[0:4, 2:4] = 7  # class 7 is absent from the table
    riskmap = _write_raster(tmp_path / "risk.tif", risk)
    borders = _write_borders(tmp_path / "borders.shp")

    res = allocate_deforestation(
        riskmap_file=riskmap,
        defrate_table=_dense_table(),
        defor_juris_ha=1000.0,
        years_forecast=4,
        project_borders=borders,
        out_dir=tmp_path / "run",
    )

    assert len(res.warnings) == 1
    assert "absent from the rate table" in res.warnings[0]
    corr = 1000.0 / 1.1
    assert res.annual_ha == pytest.approx(8 * (0.001 * corr / 4), rel=1e-6)


def test_forest_mask_excludes_ineligible_pixels(tmp_path):
    """Masking out half the project block halves the allocated hectares."""
    risk = np.full((8, 8), 2, dtype=np.uint16)
    riskmap = _write_raster(tmp_path / "risk.tif", risk)
    borders = _write_borders(tmp_path / "borders.shp")
    mask = np.zeros((8, 8), dtype=np.uint16)
    mask[0:4, 0:2] = 1  # only half of the 4x4 project block is forest
    mask_file = _write_raster(tmp_path / "mask.tif", mask)

    unmasked = allocate_deforestation(
        riskmap_file=riskmap,
        defrate_table=_dense_table(),
        defor_juris_ha=1000.0,
        years_forecast=4,
        project_borders=borders,
        out_dir=tmp_path / "a",
    )
    masked = allocate_deforestation(
        riskmap_file=riskmap,
        defrate_table=_dense_table(),
        defor_juris_ha=1000.0,
        years_forecast=4,
        project_borders=borders,
        out_dir=tmp_path / "b",
        forest_mask_file=mask_file,
    )

    assert masked.annual_ha == pytest.approx(unmasked.annual_ha / 2, rel=1e-6)


def test_density_map_holds_per_class_density_and_nodata(tmp_path):
    """The density raster carries per-class ha/px/yr and nodata elsewhere."""
    from osgeo import gdal

    from spatialrisk.allocation import DENSITY_NODATA

    risk = np.full((8, 8), 0, dtype=np.uint16)
    risk[0:4, 0:4] = 3
    riskmap = _write_raster(tmp_path / "risk.tif", risk)
    borders = _write_borders(tmp_path / "borders.shp")

    res = allocate_deforestation(
        riskmap_file=riskmap,
        defrate_table=_dense_table(),
        defor_juris_ha=1000.0,
        years_forecast=4,
        project_borders=borders,
        out_dir=tmp_path / "run",
        defor_density_map=True,
        blk_rows=4,
    )

    assert res.density_map_path is not None and res.density_map_path.exists()
    ds = gdal.Open(str(res.density_map_path))
    arr = ds.GetRasterBand(1).ReadAsArray()
    ds = None
    corr = 1000.0 / 1.1
    assert arr[0, 0] == pytest.approx(0.003 * corr / 4, rel=1e-6)
    assert arr[7, 7] == pytest.approx(DENSITY_NODATA)


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda df: df.drop(columns=["nfor"]), "missing required column"),
        (lambda df: df.assign(cat=[1, 1, 2]), "duplicate 'cat'"),
        (lambda df: df.assign(rate_mod=[0.0, 0.0, 0.0]), "denominator"),
        (lambda df: df.assign(pixel_area=[1.0, 2.0, 1.0]), "must be constant"),
        (lambda df: df.assign(nfor=[100, -5, 300]), "negative values"),
    ],
)
def test_validate_defrate_table_rejects_bad_tables(mutate, message):
    """Each malformed rate table raises with a message naming the problem."""
    with pytest.raises(AllocationInputError, match=message):
        validate_defrate_table(mutate(_dense_table()))


def test_rejects_non_positive_forecast_length(tmp_path):
    """A zero-or-negative forecast period is rejected up front."""
    riskmap = _write_raster(tmp_path / "risk.tif", np.full((8, 8), 2, dtype=np.uint16))
    borders = _write_borders(tmp_path / "borders.shp")
    with pytest.raises(AllocationInputError, match="greater than zero years"):
        allocate_deforestation(
            riskmap_file=riskmap,
            defrate_table=_dense_table(),
            defor_juris_ha=10.0,
            years_forecast=0,
            project_borders=borders,
            out_dir=tmp_path / "run",
        )


def test_rejects_geographic_crs(tmp_path):
    """A degree-based risk map is rejected: pixel area would be meaningless."""
    from osgeo import gdal, osr

    path = tmp_path / "geo.tif"
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), 8, 8, 1, gdal.GDT_UInt16)
    ds.SetGeoTransform((3.0, 0.001, 0.0, 45.0, 0.0, -0.001))
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    ds.SetProjection(srs.ExportToWkt())
    ds.GetRasterBand(1).WriteArray(np.full((8, 8), 2, dtype=np.uint16))
    ds = None
    borders = _write_borders(tmp_path / "borders.shp")

    with pytest.raises(AllocationInputError, match="projected, metre-based CRS"):
        allocate_deforestation(
            riskmap_file=path,
            defrate_table=_dense_table(),
            defor_juris_ha=10.0,
            years_forecast=4,
            project_borders=borders,
            out_dir=tmp_path / "run",
        )


def test_rejects_table_whose_pixel_area_disagrees_with_raster(tmp_path):
    """A table whose pixel_area does not match the raster belongs elsewhere."""
    riskmap = _write_raster(tmp_path / "risk.tif", np.full((8, 8), 2, dtype=np.uint16))
    borders = _write_borders(tmp_path / "borders.shp")
    with pytest.raises(AllocationInputError, match="different raster"):
        allocate_deforestation(
            riskmap_file=riskmap,
            defrate_table=_dense_table(pixel_area=0.09),
            defor_juris_ha=10.0,
            years_forecast=4,
            project_borders=borders,
            out_dir=tmp_path / "run",
        )


def test_borders_outside_riskmap_raise(tmp_path):
    """Borders that miss the risk map entirely raise instead of returning zero."""
    riskmap = _write_raster(tmp_path / "risk.tif", np.full((8, 8), 2, dtype=np.uint16))
    far_away = _write_borders(tmp_path / "far.shp", minx=900000.0, maxy=4000000.0)
    with pytest.raises(AllocationInputError, match="do not intersect"):
        allocate_deforestation(
            riskmap_file=riskmap,
            defrate_table=_dense_table(),
            defor_juris_ha=10.0,
            years_forecast=4,
            project_borders=far_away,
            out_dir=tmp_path / "run",
        )


def test_project_extent_density_map_covers_only_the_cropped_grid(tmp_path):
    """extent='project' writes the density raster on the cropped 4x4 grid."""
    from osgeo import gdal

    risk = np.full((8, 8), 0, dtype=np.uint16)
    risk[0:4, 0:4] = 3
    riskmap = _write_raster(tmp_path / "risk.tif", risk)
    borders = _write_borders(tmp_path / "borders.shp")

    res = allocate_deforestation(
        riskmap_file=riskmap,
        defrate_table=_dense_table(),
        defor_juris_ha=1000.0,
        years_forecast=4,
        project_borders=borders,
        out_dir=tmp_path / "run",
        defor_density_map="project",
        blk_rows=2,
    )

    assert res.density_map_path is not None and res.density_map_path.exists()
    ds = gdal.Open(str(res.density_map_path))
    assert (ds.RasterXSize, ds.RasterYSize) == (4, 4)
    arr = ds.GetRasterBand(1).ReadAsArray()
    ds = None
    corr = 1000.0 / 1.1
    assert arr[0, 0] == pytest.approx(0.003 * corr / 4, rel=1e-6)


def test_project_extent_density_map_respects_the_forest_mask(tmp_path):
    """Masked-out pixels of the cropped grid carry nodata, not a density."""
    from osgeo import gdal

    from spatialrisk.allocation import DENSITY_NODATA

    risk = np.full((8, 8), 2, dtype=np.uint16)
    riskmap = _write_raster(tmp_path / "risk.tif", risk)
    borders = _write_borders(tmp_path / "borders.shp")
    mask = np.zeros((8, 8), dtype=np.uint16)
    mask[0:4, 0:2] = 1  # only the left half of the 4x4 project block is forest
    mask_file = _write_raster(tmp_path / "mask.tif", mask)

    res = allocate_deforestation(
        riskmap_file=riskmap,
        defrate_table=_dense_table(),
        defor_juris_ha=1000.0,
        years_forecast=4,
        project_borders=borders,
        out_dir=tmp_path / "run",
        forest_mask_file=mask_file,
        defor_density_map="project",
    )

    ds = gdal.Open(str(res.density_map_path))
    arr = ds.GetRasterBand(1).ReadAsArray()
    ds = None
    corr = 1000.0 / 1.1
    assert arr[0, 0] == pytest.approx(0.001 * corr / 4, rel=1e-6)
    assert arr[0, 3] == pytest.approx(DENSITY_NODATA)


def test_legacy_bool_true_still_writes_the_whole_aoi_raster(tmp_path):
    """defor_density_map=True keeps its historical whole-AOI meaning."""
    from osgeo import gdal

    risk = np.full((8, 8), 2, dtype=np.uint16)
    riskmap = _write_raster(tmp_path / "risk.tif", risk)
    borders = _write_borders(tmp_path / "borders.shp")

    res = allocate_deforestation(
        riskmap_file=riskmap,
        defrate_table=_dense_table(),
        defor_juris_ha=1000.0,
        years_forecast=4,
        project_borders=borders,
        out_dir=tmp_path / "run",
        defor_density_map=True,
    )

    ds = gdal.Open(str(res.density_map_path))
    assert (ds.RasterXSize, ds.RasterYSize) == (8, 8)
    ds = None


def test_invalid_density_extent_is_rejected(tmp_path):
    """A typo'd extent fails loudly instead of silently skipping the raster."""
    riskmap = _write_raster(tmp_path / "risk.tif", np.full((8, 8), 2, dtype=np.uint16))
    borders = _write_borders(tmp_path / "borders.shp")
    with pytest.raises(AllocationInputError, match="defor_density_map"):
        allocate_deforestation(
            riskmap_file=riskmap,
            defrate_table=_dense_table(),
            defor_juris_ha=1000.0,
            years_forecast=4,
            project_borders=borders,
            out_dir=tmp_path / "run",
            defor_density_map="everything",
        )
