import types
import types as _types
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin

from spatialrisk.evaluation import interval_from_target, label_for, make_square, validate_two_layer
import spatialrisk.evaluation as ev


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


def _write_raster(path, array, pixel=30.0):
    """Write a single-band GeoTIFF (EPSG:3857, square pixels)."""
    array = np.asarray(array)
    transform = from_origin(0, array.shape[0] * pixel, pixel, pixel)
    with rasterio.open(
        path, "w", driver="GTiff", height=array.shape[0], width=array.shape[1],
        count=1, dtype="int32", crs="EPSG:3857", transform=transform, nodata=0,
    ) as dst:
        dst.write(array.astype("int32"), 1)
    return str(path)


def test_make_square_partitions_600x300_into_two_cells(tmp_path):
    r = _write_raster(tmp_path / "r.tif", np.ones((300, 600)))
    nsquare, nsquare_x, nsquare_y, x, y, nx, ny = make_square(r, 300)
    assert nsquare == 2 and nsquare_x == 2 and nsquare_y == 1
    assert x == [0, 300] and y == [0]
    assert nx == [300, 300] and ny == [300]


def test_make_square_handles_remainder(tmp_path):
    r = _write_raster(tmp_path / "r2.tif", np.ones((100, 250)))
    nsquare, nsquare_x, nsquare_y, x, y, nx, ny = make_square(r, 100)
    assert nsquare_x == 3 and nx == [100, 100, 50]   # 250 = 100+100+50
    assert nsquare_y == 1 and ny == [100]


def test_validate_two_layer_perfect_prediction(tmp_path):
    # 700 px wide -> make_square gives 3 cells [300,300,100]; the smaller cell makes
    # predicted/observed vary across cells so corrcoef (R2) is well-defined (=1.0).
    nrow, ncol, pixel = 300, 700, 30.0
    pix_area_ha = (pixel * pixel) / 10000.0          # 0.09 ha
    forest = np.ones((nrow, ncol), dtype="int32")     # all forest

    # 30% deforested per coarse cell (top 90 rows of each 300x300 block).
    defor = np.zeros((nrow, ncol), dtype="int32")
    defor[:90, :] = 1     # top 30% of rows deforested across all 700 cols

    risk = np.ones((nrow, ncol), dtype="int32")       # all category 1

    f = _write_raster(tmp_path / "forest.tif", forest, pixel)
    d = _write_raster(tmp_path / "defor.tif", defor, pixel)
    rk = _write_raster(tmp_path / "risk.tif", risk, pixel)

    # Per-cell: ndefor = 90*300 px; nfor = 300*300 px; cat-1 count = nfor.
    # predicted_ha = count * defor_dens * ti ; want == ndefor*pix_area_ha.
    time_interval = 5
    ndefor_px, nfor_px = 90 * 300, 300 * 300
    defor_dens = (ndefor_px * pix_area_ha) / (nfor_px * time_interval)
    tab = tmp_path / "defrate.csv"
    pd.DataFrame({"cat": [1], "defor_dens": [defor_dens]}).to_csv(tab, index=False)

    idx = validate_two_layer(
        defor_file=d, forest_file=f, riskmap_file=rk, tab_file_defor=str(tab),
        time_interval=time_interval, csize_coarse_grid=300,
        indices_file_pred=tmp_path / "indices.csv",
        tab_file_pred=tmp_path / "pred_obs.csv",
        fig_file_pred=tmp_path / "pred_obs.png",
        model_name="TEST", period="calibration",
    )
    assert idx["ncell"] == 3
    assert idx["RMSE"] == 0.0
    assert idx["wRMSE"] == 0.0
    assert idx["MedAE"] == 0.0
    assert idx["R2"] == 1.0
    assert (tmp_path / "indices.csv").exists()
    assert (tmp_path / "pred_obs.png").exists()


def _fake_project_with_prediction(tmp_path):
    target = _types.SimpleNamespace(name="forest_loss_2015_2020",
                                    path=tmp_path / "defor.tif")
    forest = _types.SimpleNamespace(name="forest_gfc", path=tmp_path / "forest.tif")
    dataset = _types.SimpleNamespace(name="calibration", target=target,
                                     features=[forest])
    pred = _types.SimpleNamespace(model_key="glm_glm_v1", window=None,
                                  dataset_name="calibration",
                                  path=tmp_path / "risk.tif", metrics={})
    project = _types.SimpleNamespace(
        folders=_types.SimpleNamespace(project_folder=tmp_path),
        get_dataset=lambda n: dataset if n == "calibration" else None,
        predictions={"glm_glm_v1__calibration_y2015": pred},
        save=lambda: None,
    )
    return project, pred, dataset


def test_resolve_layers_recovers_from_dataset(tmp_path):
    project, pred, dataset = _fake_project_with_prediction(tmp_path)
    lay = ev.resolve_layers(project, pred)
    assert lay["defor_file"] == dataset.target.path
    assert lay["forest_file"] == dataset.features[0].path
    assert lay["riskmap_file"] == pred.path
    assert lay["time_interval"] == 5
    assert lay["period"] == "calibration"


def test_evaluate_prediction_runs_defrate_then_validate(tmp_path, monkeypatch):
    project, pred, dataset = _fake_project_with_prediction(tmp_path)
    calls = {}

    def fake_defrate_per_cat(**kw):
        calls["defrate"] = kw
        _Path(kw["tab_file_defrate"]).write_text("cat,defor_dens\n1,0.01\n")

    def fake_validate(**kw):
        calls["validate"] = kw
        return {"RMSE": 1.0, "wRMSE": 2.0, "MedAE": 0.5, "R2": 0.9,
                "ncell": 26, "csize_coarse_grid": kw["csize_coarse_grid"],
                "csize_coarse_grid_ha": 8100.0}

    monkeypatch.setattr(ev, "_defrate_per_cat", fake_defrate_per_cat)
    monkeypatch.setattr(ev, "validate_two_layer", fake_validate)

    rows = ev.evaluate_prediction(project, pred, csizes=(300,))
    assert len(rows) == 1
    assert rows[0]["model"] == "GLM" and rows[0]["period"] == "calibration"
    assert rows[0]["R2"] == 0.9
    assert calls["defrate"]["time_interval"] == 5
    assert calls["validate"]["csize_coarse_grid"] == 300
    # prediction key: fake pred has no storage_key() -> fallback "{model_key}__{period}"
    assert rows[0]["prediction"] == "glm_glm_v1__calibration"
    # fig_path is added by evaluate_prediction (the fake validate does not return it)
    assert rows[0]["fig_path"].endswith("pred_obs_GLM_calibration_300.png")
    # pred.metrics receives the per-(period,csize) index subset
    assert pred.metrics == {
        "calibration_300": {"RMSE": 1.0, "wRMSE": 2.0, "MedAE": 0.5, "R2": 0.9, "ncell": 26}
    }


def test_evaluate_predictions_filters_and_aggregates(tmp_path, monkeypatch):
    project, pred, dataset = _fake_project_with_prediction(tmp_path)
    # second prediction in a different period that we will filter out
    pred2 = _types.SimpleNamespace(model_key="rf_rf_v1", window=None,
                                   dataset_name="validation",
                                   path=tmp_path / "risk2.tif", metrics={})
    project.predictions["rf_rf_v1__validation_y2015"] = pred2

    monkeypatch.setattr(ev, "evaluate_prediction",
                        lambda proj, p, csizes=(300,), recompute_defrate=True: [
                            {"prediction": p.model_key, "model": (label_from := ev.label_for(p)),
                             "period": p.dataset_name, "csize_coarse_grid": 300,
                             "csize_coarse_grid_ha": 8100.0, "ncell": 26,
                             "MedAE": 1.0, "R2": 0.5, "RMSE": 2.0, "wRMSE": 3.0,
                             "fig_path": "x.png"}])

    df = ev.evaluate_predictions(project, dataset_filter=["calibration"])
    assert list(df["period"].unique()) == ["calibration"]   # validation filtered out
    assert set(["MedAE", "R2", "RMSE", "wRMSE"]).issubset(df.columns)
    assert (_Path(tmp_path) / "evaluation" / "indices_all.csv").exists()


def test_evaluate_one_against_truth_uses_explicit_truth(tmp_path, monkeypatch):
    pred = _types.SimpleNamespace(
        model_key="glm_glm_v1", window=None, dataset_name="validation",
        path=tmp_path / "risk.tif", metrics={},
        storage_key=lambda: "glm_glm_v1__validation")
    project = _types.SimpleNamespace(
        folders=_types.SimpleNamespace(project_folder=tmp_path))
    calls = {}

    def fake_defrate(**kw):
        calls["defrate"] = kw
        _Path(kw["tab_file_defrate"]).write_text("cat,defor_dens\n1,0.01\n")

    def fake_validate(**kw):
        calls["validate"] = kw
        return {"RMSE": 1.0, "wRMSE": 2.0, "MedAE": 0.5, "R2": 0.9, "ncell": 26,
                "csize_coarse_grid": kw["csize_coarse_grid"],
                "csize_coarse_grid_ha": 8100.0}

    monkeypatch.setattr(ev, "_defrate_per_cat", fake_defrate)
    monkeypatch.setattr(ev, "validate_two_layer", fake_validate)

    truth_defor = tmp_path / "truth_defor.tif"
    truth_forest = tmp_path / "truth_forest.tif"
    rows = ev._evaluate_one_against_truth(
        project, pred, defor_file=truth_defor, forest_file=truth_forest,
        time_interval=7, truth_tag="forest_loss_2015_2020", csizes=(300,))

    # the SHARED truth is used, not the map's own dataset
    assert calls["defrate"]["defor_file"] == truth_defor
    assert calls["defrate"]["forest_file"] == truth_forest
    assert calls["defrate"]["time_interval"] == 7
    assert calls["validate"]["riskmap_file"] == pred.path
    assert calls["validate"]["time_interval"] == 7
    # row annotations
    assert rows[0]["truth"] == "forest_loss_2015_2020"
    assert rows[0]["period"] == "validation"
    assert rows[0]["model"] == "GLM"
    assert rows[0]["prediction"] == "glm_glm_v1__validation"
    # output namespaced under evaluation/<truth_tag>/
    assert (tmp_path / "evaluation" / "forest_loss_2015_2020").is_dir()
    assert rows[0]["fig_path"].endswith(
        "evaluation/forest_loss_2015_2020/pred_obs_GLM_validation_300.png")
    # metrics keyed by "<tag>__<period>_<csize>"
    assert pred.metrics == {
        "forest_loss_2015_2020__validation_300":
            {"RMSE": 1.0, "wRMSE": 2.0, "MedAE": 0.5, "R2": 0.9, "ncell": 26}}


def test_evaluate_against_truth_selects_keys_and_namespaces(tmp_path, monkeypatch):
    p1 = _types.SimpleNamespace(model_key="glm_glm_v1", window=None,
                                dataset_name="calibration", path=tmp_path / "r1.tif",
                                metrics={}, storage_key=lambda: "glm_glm_v1__calibration")
    p2 = _types.SimpleNamespace(model_key="rf_rf_v1", window=None,
                                dataset_name="validation", path=tmp_path / "r2.tif",
                                metrics={}, storage_key=lambda: "rf_rf_v1__validation")
    saved = {"n": 0}
    project = _types.SimpleNamespace(
        folders=_types.SimpleNamespace(project_folder=tmp_path),
        predictions={"k1": p1, "k2": p2},
        save=lambda: saved.__setitem__("n", saved["n"] + 1))

    monkeypatch.setattr(
        ev, "_evaluate_one_against_truth",
        lambda proj, pred, **kw: [{
            "prediction": pred.storage_key(), "model": ev.label_for(pred),
            "period": pred.dataset_name, "truth": kw["truth_tag"],
            "csize_coarse_grid": 300, "csize_coarse_grid_ha": 8100.0, "ncell": 26,
            "MedAE": 1.0, "R2": 0.5, "RMSE": 2.0, "wRMSE": 3.0, "fig_path": "x.png"}])

    df = ev.evaluate_against_truth(
        project, prediction_keys=["k1"], defor_file=tmp_path / "d.tif",
        forest_file=tmp_path / "f.tif", time_interval=5,
        truth_tag="forest_loss_2015_2020")

    assert list(df["period"]) == ["calibration"]          # only k1 was selected
    assert list(df["truth"]) == ["forest_loss_2015_2020"]
    assert "truth" in df.columns
    assert (tmp_path / "evaluation" / "forest_loss_2015_2020"
            / "indices_all.csv").exists()
    assert saved["n"] == 1                                 # auto_save ran


def test_evaluate_against_truth_skips_unknown_key(tmp_path, monkeypatch, capsys):
    project = _types.SimpleNamespace(
        folders=_types.SimpleNamespace(project_folder=tmp_path),
        predictions={}, save=lambda: None)

    df = ev.evaluate_against_truth(
        project, prediction_keys=["nope"], defor_file=tmp_path / "d.tif",
        forest_file=tmp_path / "f.tif", time_interval=5, truth_tag="t")

    assert len(df) == 0
    assert "skipped nope" in capsys.readouterr().out
    assert (tmp_path / "evaluation" / "t" / "indices_all.csv").exists()
