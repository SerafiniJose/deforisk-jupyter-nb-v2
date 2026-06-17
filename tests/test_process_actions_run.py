from unittest.mock import MagicMock, patch

from gui.scripts import process_actions


class _Proj:
    def __init__(self):
        self.raw_variables = {}
        self.processed_variables = {}
        self.forest_loss_specs = []
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


def test_generate_forest_loss_targets_adds_raw():
    from spatialrisk.variables.models import ForestLossSpec

    p = _Proj()
    start = MagicMock(name="start")
    end = MagicMock(name="end")
    p.raw_variables = {"forest_gfc_2015": start, "forest_gfc_2020": end}
    p.forest_loss_specs = [
        ForestLossSpec(
            name="forest_loss_2015_2020",
            start_key="forest_gfc_2015",
            end_key="forest_gfc_2020",
            start_year=2015,
            end_year=2020,
        )
    ]
    new_var = MagicMock(name="forest_loss_var")

    with patch("gui.scripts.process_actions.make_forest_loss_var", return_value=new_var) as m:
        out = process_actions.generate_forest_loss_targets(p)

    m.assert_called_once_with(p, start, end)
    new_var.add_as_raw.assert_called_once_with(auto_save=False)
    assert out == [new_var]


def test_run_processing_sequences_steps():
    p = _Proj()
    p.base_raster = MagicMock(name="base")
    with patch("gui.scripts.process_actions.materialize_raw_layers") as mat, patch(
        "gui.scripts.process_actions.generate_forest_loss_targets"
    ) as gen:
        process_actions.run_processing(p)

    mat.assert_called_once_with(p)
    gen.assert_called_once_with(p)
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


def test_generate_forest_loss_targets_skips_missing_key():
    from spatialrisk.variables.models import ForestLossSpec
    p = _Proj()
    p.raw_variables = {}  # neither key present
    p.forest_loss_specs = [
        ForestLossSpec(
            name="forest_loss_2015_2020",
            start_key="forest_gfc_2015",
            end_key="forest_gfc_2020",
            start_year=2015,
            end_year=2020,
        )
    ]
    with patch("gui.scripts.process_actions.make_forest_loss_var") as m:
        out = process_actions.generate_forest_loss_targets(p)
    m.assert_not_called()
    assert out == []
