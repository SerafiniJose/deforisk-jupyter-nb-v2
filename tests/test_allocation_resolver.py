"""Rate-table resolution per model family."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from gui.scripts.allocation_runner import (
    AllocationResolveError,
    resolve_defrate_table,
)


def _pred(**kw):
    """Stand-in for a registered Prediction, with the fields the resolver reads."""
    base = dict(
        path=Path("/data/p/inference/forecast/prob.tif"),
        model_key="icar",
        dataset_name="forecast",
        window=None,
        defrate_path=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _project(predictions):
    """Minimal project stand-in holding a predictions registry."""
    return SimpleNamespace(predictions=predictions, models={}, datasets={})


def test_persisted_defrate_path_wins(tmp_path):
    """A table recorded on the Prediction is used as-is."""
    csv = tmp_path / "defrate_cat_bm_forecast.csv"
    csv.write_text("cat,nfor,rate_mod,pixel_area\n1,10,0.0,0.09\n")
    project = _project({"jnr_run": _pred(model_key="benchmark", defrate_path=csv)})

    src = resolve_defrate_table(project, "jnr_run")

    assert src.path == csv
    assert src.provenance == "persisted"


def test_jnr_without_model_table_carries_a_caveat(tmp_path):
    """JNR tables hold observed-period rates, so the user is warned."""
    csv = tmp_path / "defrate_cat_bm_forecast.csv"
    csv.write_text("cat,nfor,rate_mod,pixel_area\n1,10,0.0,0.09\n")
    project = _project({"jnr_run": _pred(model_key="benchmark", defrate_path=csv)})

    src = resolve_defrate_table(project, "jnr_run")

    assert src.caveat is not None
    assert "observed" in src.caveat.lower()


def test_mw_falls_back_to_sibling_path(tmp_path):
    """Moving-window runs find their table beside the raster."""
    prob = tmp_path / "prob_mw_11_forecast.tif"
    prob.write_bytes(b"")
    sibling = tmp_path / "defrate_cat_mw_11_forecast.csv"
    sibling.write_text("cat,nfor,rate_mod,pixel_area\n1,10,0.0,0.09\n")
    project = _project({"mw_run_w11": _pred(model_key="mw", window=11, path=prob)})

    src = resolve_defrate_table(project, "mw_run_w11")

    assert src.path == sibling
    assert src.provenance == "mw-sibling"


def test_mw_without_sibling_reports_missing(tmp_path):
    """A moving-window run with no table fails with an actionable message."""
    prob = tmp_path / "prob_mw_11_forecast.tif"
    prob.write_bytes(b"")
    project = _project({"mw_run_w11": _pred(model_key="mw", window=11, path=prob)})

    with pytest.raises(AllocationResolveError, match="rate table"):
        resolve_defrate_table(project, "mw_run_w11", compute=False)


def test_far_prediction_computes_via_resolve_layers(tmp_path, monkeypatch):
    """FAR-family runs compute the table from the prediction's own dataset."""
    import gui.scripts.allocation_runner as runner

    out = tmp_path / "defrate_cat_icar_forecast.csv"
    calls = {}

    def fake_resolve_layers(project, pred):
        return {
            "defor_file": "/d.tif",
            "forest_file": "/f.tif",
            "riskmap_file": str(pred.path),
            "time_interval": 5,
            "period": "forecast",
        }

    def fake_defrate_per_cat(**kwargs):
        calls.update(kwargs)
        Path(kwargs["tab_file_defrate"]).write_text(
            "cat,nfor,rate_mod,pixel_area\n1,10,0.0,0.09\n"
        )

    monkeypatch.setattr(runner, "_resolve_layers", fake_resolve_layers)
    monkeypatch.setattr(runner, "_defrate_per_cat", fake_defrate_per_cat)
    pred = _pred(path=tmp_path / "prob.tif")
    project = _project({"icar_run": pred})

    src = resolve_defrate_table(project, "icar_run")

    assert src.provenance == "computed"
    assert calls["time_interval"] == 5
    assert calls["defor_file"] == "/d.tif"
    assert Path(src.path).exists()
    assert out.exists()


def test_missing_time_interval_is_an_explicit_error(tmp_path, monkeypatch):
    """Without a period length the rate cannot be computed — say so."""
    import gui.scripts.allocation_runner as runner

    monkeypatch.setattr(
        runner,
        "_resolve_layers",
        lambda project, pred: {
            "defor_file": "/d.tif",
            "forest_file": "/f.tif",
            "riskmap_file": "/r.tif",
            "time_interval": None,
            "period": "forecast",
        },
    )
    project = _project({"icar_run": _pred(path=tmp_path / "prob.tif")})

    with pytest.raises(AllocationResolveError, match="period length"):
        resolve_defrate_table(project, "icar_run")


def test_user_path_short_circuits_everything(tmp_path):
    """An explicit override skips every other resolution route."""
    csv = tmp_path / "mine.csv"
    csv.write_text("cat,nfor,rate_mod,pixel_area\n1,10,0.0,0.09\n")
    project = _project({"icar_run": _pred()})

    src = resolve_defrate_table(project, "icar_run", user_path=csv)

    assert src.path == csv
    assert src.provenance == "user"


def test_unknown_prediction_key_raises():
    """Resolving against a missing registry key fails loudly."""
    with pytest.raises(AllocationResolveError, match="not found"):
        resolve_defrate_table(_project({}), "nope")
