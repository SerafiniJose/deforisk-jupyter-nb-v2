import types
from spatialrisk.evaluation import interval_from_target, label_for


def test_interval_from_target_parses_two_years():
    assert interval_from_target("forest_loss_2015_2020") == 5
    assert interval_from_target("forest_loss_2020_2024") == 4


def test_interval_from_target_handles_missing_years():
    assert interval_from_target("no_years_here") is None


def _pred(model_key, window=None):
    return types.SimpleNamespace(model_key=model_key, window=window)


def test_label_for_maps_family_and_window():
    assert label_for(_pred("glm_glm_v1")) == "GLM"
    assert label_for(_pred("rf_rf_v1")) == "RF"
    assert label_for(_pred("icar_icar_v1")) == "ICAR"
    assert label_for(_pred("jnr_calibration_jnr")) == "JNR"
    assert label_for(_pred("mw_calibration_mw", window=11)) == "MW_w11"
