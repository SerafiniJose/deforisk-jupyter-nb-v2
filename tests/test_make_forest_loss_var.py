"""Tests for make_forest_loss_var (2-layer forest-loss helper)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import spatialrisk.processing as processing
from spatialrisk.variables.models import RasterType


class _Folders:
    def __init__(self, root: Path):
        self.data_raw_folder = root


class _FakeProject:
    def __init__(self, root: Path):
        self._root = root

    @property
    def folders(self):
        return _Folders(self._root)


class _Layer:
    def __init__(self, year, path, crs="EPSG:4326", res=30.0):
        self.year = year
        self.path = Path(path)
        self.default_crs = crs
        self.default_resolution = res


def test_make_forest_loss_var_naming_and_tags(tmp_path):
    project = _FakeProject(tmp_path)
    start = _Layer(2015, tmp_path / "forest_gfc_2015.tif")
    end = _Layer(2020, tmp_path / "forest_gfc_2020.tif")

    with patch.object(processing, "process_forest_loss_xarray") as m_proc, patch(
        "spatialrisk.variables.LocalRasterVar"
    ) as m_lrv:
        m_lrv.return_value = MagicMock(name="forest_loss_var")
        var = processing.make_forest_loss_var(project, start, end)

    out_path = tmp_path / "forest_loss_2015_2020.tif"
    m_proc.assert_called_once_with(str(start.path), str(end.path), str(out_path))
    m_lrv.assert_called_once()
    kwargs = m_lrv.call_args.kwargs
    assert kwargs["name"] == "forest_loss_2015_2020"
    assert kwargs["raster_type"] == RasterType.categorical
    assert kwargs["tags"] == ["deforestation", "forest_loss", "2015_2020"]
    assert kwargs["path"] == out_path
    assert kwargs["project"] is project  # 3-layer behavior preserved: real project passed
    assert var is m_lrv.return_value


def test_make_forest_loss_var_idempotent(tmp_path):
    project = _FakeProject(tmp_path)
    start = _Layer(2015, tmp_path / "forest_gfc_2015.tif")
    end = _Layer(2020, tmp_path / "forest_gfc_2020.tif")
    (tmp_path / "forest_loss_2015_2020.tif").write_bytes(b"x")  # output already exists

    with patch.object(processing, "process_forest_loss_xarray") as m_proc, patch(
        "spatialrisk.variables.LocalRasterVar"
    ) as m_lrv:
        m_lrv.return_value = MagicMock(name="forest_loss_var")
        processing.make_forest_loss_var(project, start, end)

    m_proc.assert_not_called()  # existing output -> heavy step skipped
