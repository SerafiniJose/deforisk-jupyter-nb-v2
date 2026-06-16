# tests/test_train_tile_fit.py
import types
from pathlib import Path

from gui.tile.train_tile import build_fit_kwargs


def _dataset():
    target = types.SimpleNamespace(name="forest_loss_2015_2020")
    return types.SimpleNamespace(name="calibration", target=target)


def _project():
    return types.SimpleNamespace(folders=types.SimpleNamespace(
        rmj_mw=Path("/tmp/rmj_mw"), rmj_bm=Path("/tmp/rmj_bm")))


def test_ml_models_get_empty_fit_kwargs():
    assert build_fit_kwargs("glm", _dataset(), _project()) == {}
    assert build_fit_kwargs("rf", _dataset(), _project()) == {}
    assert build_fit_kwargs("icar", _dataset(), _project()) == {}


def test_mw_gets_time_interval_and_folder():
    kw = build_fit_kwargs("mw", _dataset(), _project())
    assert kw["time_interval"] == 5
    assert kw["folder"] == Path("/tmp/rmj_mw")


def test_jnr_gets_folder_only():
    kw = build_fit_kwargs("benchmark", _dataset(), _project())
    assert kw["folder"] == Path("/tmp/rmj_bm")
    assert "time_interval" not in kw     # JNR.fit() has no time_interval arg
