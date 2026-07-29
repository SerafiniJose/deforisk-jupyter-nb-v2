"""MW/JNR apply() must record the defrate table on the Prediction it registers."""

import inspect

from spatialrisk.mlmodels import base, jnr_model, mw_model


def test_register_prediction_accepts_defrate_path():
    """The shared registration helper takes the rate-table path."""
    sig = inspect.signature(base.BaseRiskModel._register_prediction)
    assert "defrate_path" in sig.parameters


def test_mw_apply_passes_its_defrate_table():
    """Moving-window apply() hands its per-window table to the Prediction."""
    src = inspect.getsource(mw_model.MWModel.apply)
    assert "defrate_path=defrate_tab" in src


def test_jnr_apply_passes_its_defrate_table():
    """JNR benchmark apply() hands its table to the Prediction."""
    src = inspect.getsource(jnr_model.JNRBenchmarkModel.apply)
    assert "defrate_path=defrate_tab" in src
