import logging
from unittest.mock import MagicMock, patch

from gui.scripts import process_actions


class _Proj:
    def __init__(self):
        self.raw_variables = {}
        self.processed_variables = {}
        self.base_raster = None
        self.saved = False

    def reproject_and_match_all(self, source="raw"):
        self.reprojected = source

    def rasterize_all(self, source="raw"):
        self.rasterized = source

    def save(self):
        self.saved = True


def test_set_base_raster_reprojects_and_sets():
    p = _Proj()
    raw = MagicMock(name="rawbase")
    reprojected = MagicMock(name="reprojected")
    raw.reproject.return_value = reprojected
    p.raw_variables["subj"] = raw

    out = process_actions.set_base_raster(p, "subj", "EPSG:5490", 30.0)

    raw.reproject.assert_called_once_with(target_epsg="EPSG:5490", resolution=30.0)
    reprojected.use_as_base_raster.assert_called_once()
    assert out is reprojected


def test_run_processing_sequences_steps():
    p = _Proj()
    p.base_raster = MagicMock(name="base")
    with patch("gui.scripts.process_actions.materialize_raw_layers") as mat:
        process_actions.run_processing(p)

    mat.assert_called_once_with(p)
    assert p.reprojected == "raw"
    assert p.rasterized == "raw"
    assert p.saved is True


def test_apply_post_processing_adds_processed():
    p = _Proj()
    var = MagicMock(name="processed")
    derived = MagicMock(name="derived")
    var.apply_post_processing.return_value = derived
    p.processed_variables["rivers"] = var

    out = process_actions.apply_post_processing(p, "rivers", "dist")

    var.apply_post_processing.assert_called_once_with("dist")
    derived.add_as_processed.assert_called_once()
    assert out is derived


def test_run_processing_raises_without_base_raster():
    import pytest
    p = _Proj()  # base_raster is None by default
    with pytest.raises(ValueError, match="base raster"):
        process_actions.run_processing(p)


def test_run_processing_logs_reproject_and_rasterize(caplog):
    p = _Proj()
    p.base_raster = MagicMock(name="base")
    with caplog.at_level(logging.INFO, logger="spatial_risk"):
        with patch("gui.scripts.process_actions.materialize_raw_layers"):
            process_actions.run_processing(p)
    text = caplog.text.lower()
    assert "reproject" in text
    assert "rasteriz" in text
    assert "complete" in text


def test_apply_post_processing_logs_step(caplog):
    p = _Proj()
    var = MagicMock(name="processed")
    derived = MagicMock(name="derived")
    var.apply_post_processing.return_value = derived
    p.processed_variables["rivers"] = var
    with caplog.at_level(logging.INFO, logger="spatial_risk"):
        process_actions.apply_post_processing(p, "rivers", "dist")
    assert "dist" in caplog.text.lower()
    assert "rivers" in caplog.text.lower()
