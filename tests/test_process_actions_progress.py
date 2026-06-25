import logging

from gui.scripts import process_actions


class _Local:
    def add_as_raw(self, auto_save=False):
        pass


class GEEVar:  # name matters: _is_geevar checks type(var).__name__ == "GEEVar"
    data_type = "raster"  # not DataType.vector -> takes the to_local_raster path

    def to_local_raster(self):
        return _Local()


class _Project:
    def __init__(self, raw_variables):
        self.raw_variables = raw_variables


def test_download_emits_count_lines(caplog):
    project = _Project({"forest_gfc": GEEVar(), "dem": GEEVar()})
    with caplog.at_level(logging.INFO, logger="spatial_risk"):
        out = process_actions.materialize_raw_layers(project)
    assert out == ["forest_gfc", "dem"]
    messages = [r.getMessage() for r in caplog.records]
    assert "Downloading layer 1/2: forest_gfc" in messages
    assert "Downloading layer 2/2: dem" in messages
    assert "Downloaded 2 layer(s)." in messages
