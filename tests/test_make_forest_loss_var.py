from pathlib import Path
from unittest.mock import patch

from spatialrisk.processing import make_forest_loss_var
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

    with patch("spatialrisk.processing.process_forest_loss_xarray") as m:
        var = make_forest_loss_var(project, start, end)

    m.assert_called_once()
    assert var.name == "forest_loss_2015_2020"
    assert var.raster_type == RasterType.categorical
    assert var.tags == ["deforestation", "forest_loss", "2015_2020"]
    assert var.path == tmp_path / "forest_loss_2015_2020.tif"


def test_make_forest_loss_var_idempotent(tmp_path):
    project = _FakeProject(tmp_path)
    start = _Layer(2015, tmp_path / "forest_gfc_2015.tif")
    end = _Layer(2020, tmp_path / "forest_gfc_2020.tif")
    (tmp_path / "forest_loss_2015_2020.tif").write_bytes(b"x")  # pretend it exists

    with patch("spatialrisk.processing.process_forest_loss_xarray") as m:
        make_forest_loss_var(project, start, end)

    m.assert_not_called()  # output already exists -> skip the heavy write
