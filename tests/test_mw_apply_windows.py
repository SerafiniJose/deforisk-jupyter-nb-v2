"""MWModel.apply(windows=...) runs only the requested trained windows."""

import types

import pytest

from spatialrisk.mlmodels import MWModel


def _var(tmp_path, name):
    p = tmp_path / f"{name}.tif"
    p.touch()
    return types.SimpleNamespace(name=name, path=p)


def _dataset(tmp_path):
    return types.SimpleNamespace(
        name="validation",
        target=_var(tmp_path, "forest_loss"),
        features=[_var(tmp_path, "forest_edge"), _var(tmp_path, "forest")],
    )


def _fitted_model(tmp_path):
    files = {}
    for w in (5, 11, 21):
        p = tmp_path / f"ldefrate_mw_{w}.tif"
        p.touch()
        files[str(w)] = p
    return MWModel(name="calib", dist_thresh=120.0, ldefrate_files=files)


@pytest.fixture
def stubbed_rmj(monkeypatch):
    """Stub the raster steps; record which windows' files reach them."""
    calls = []
    import spatialrisk.rmj as rmj

    monkeypatch.setattr(
        rmj, "set_defor_cat_zero", lambda **kw: calls.append(("cat_zero", kw))
    )
    monkeypatch.setattr(rmj.deforrate, "validate_binary_defor", lambda p: None)
    monkeypatch.setattr(
        rmj.deforrate, "defrate_per_cat", lambda **kw: calls.append(("defrate", kw))
    )
    return calls


def test_apply_without_windows_runs_every_trained_window(tmp_path, stubbed_rmj):
    """windows=None (the default) still runs every trained window."""
    model = _fitted_model(tmp_path)
    out = model.apply(_dataset(tmp_path), time_interval=5, output_folder=tmp_path)
    assert sorted(out) == ["11", "21", "5"]


def test_apply_windows_subset_runs_only_those(tmp_path, stubbed_rmj):
    """windows=[11] runs only that window's ldefrate file."""
    model = _fitted_model(tmp_path)
    out = model.apply(
        _dataset(tmp_path), time_interval=5, output_folder=tmp_path, windows=[11]
    )
    assert list(out) == ["11"]
    ran = {c[1]["ldefrate_file"].name for c in stubbed_rmj if c[0] == "cat_zero"}
    assert ran == {"ldefrate_mw_11.tif"}


def test_apply_rejects_an_untrained_window(tmp_path, stubbed_rmj):
    """A window not in ldefrate_files raises ValueError naming it."""
    model = _fitted_model(tmp_path)
    with pytest.raises(ValueError, match=r"7"):
        model.apply(
            _dataset(tmp_path), time_interval=5, output_folder=tmp_path, windows=[7]
        )


def test_apply_rejects_an_empty_window_list(tmp_path, stubbed_rmj):
    """An empty windows= list raises ValueError."""
    model = _fitted_model(tmp_path)
    with pytest.raises(ValueError, match="at least one"):
        model.apply(
            _dataset(tmp_path), time_interval=5, output_folder=tmp_path, windows=[]
        )
