import types
import types as _types
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin

from spatialrisk.evaluation import (
    PRED_OBS_X_LABEL,
    PRED_OBS_Y_LABEL,
    PredObsPlotData,
    ValidationResult,
    compute_validation,
    interval_from_target,
    label_for,
    make_square,
    pred_obs_axis_bounds,
    save_pred_obs_png,
    validate_two_layer,
    write_indices_csv,
    write_pred_obs_csv,
)
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


def _varied_validation_fixture(tmp_path):
    """Three coarse cells with DIFFERENT observed/predicted values and two risk
    categories, so metrics, axis bounds and the scatter are all non-degenerate,
    PLUS a zero-forest bottom half (cells 3, 4, 5) that must be dropped by the
    ``nfor_obs > 0`` filter.

    Cell 2 is the 100px-wide remainder column, which makes nfor_obs vary too.
    The bottom half (rows 300-599) has no forest and no deforestation at all,
    so cells 3/4/5 get nfor_obs == 0 and are excluded from the result entirely;
    cells 0/1/2 read the exact same pixel region as before, so the previously
    pinned golden values are unaffected by this addition.
    """
    nrow, ncol, pixel = 600, 700, 30.0
    forest = np.ones((nrow, ncol), dtype="int32")
    forest[300:600, :] = 0     # bottom half: no forest recorded at all

    defor = np.zeros((nrow, ncol), dtype="int32")
    defor[:90, 0:300] = 1      # cell 0
    defor[:150, 300:600] = 1   # cell 1
    defor[:40, 600:700] = 1    # cell 2
    # bottom half (cells 3, 4, 5) stays all-zero deforestation too.

    risk = np.ones((nrow, ncol), dtype="int32")
    risk[:, 350:] = 2          # two categories with different densities

    tab = tmp_path / "defrate.csv"
    pd.DataFrame({"cat": [1, 2], "defor_dens": [0.0004, 0.00025]}).to_csv(tab, index=False)

    return dict(
        defor_file=_write_raster(tmp_path / "defor.tif", defor, pixel),
        forest_file=_write_raster(tmp_path / "forest.tif", forest, pixel),
        riskmap_file=_write_raster(tmp_path / "risk.tif", risk, pixel),
        tab_file_defor=str(tab),
        time_interval=5,
    )


# Golden values captured from validate_two_layer BEFORE the plot-data refactor
# (git show 643a441:spatialrisk/evaluation.py), run against the fixture ABOVE
# (including its zero-forest bottom half). They pin the frozen numerics: metric
# formulas, round(..., 2), CSV columns/order, and the nfor_obs > 0 drop filter.
# Re-verified after the fixture gained the zero-forest cells (3, 4, 5): the
# legacy implementation drops them too, so the surviving-cell values below are
# byte-identical to what was pinned before the fixture change.
_GOLDEN_INDICES = {
    "RMSE": 2619.28, "wRMSE": 2964.98, "MedAE": 2250.0, "R2": 0.43,
    "ncell": 3, "csize_coarse_grid": 300, "csize_coarse_grid_ha": 8100.0,
}
_GOLDEN_POINT_CSV = (
    "cell,nfor_obs,ndefor_obs,nfor_obs_ha,ndefor_obs_ha,ndefor_pred_ha\n"
    "0,90000,27000,8100.0,2430.0,180.0\n"
    "1,90000,45000,8100.0,4050.0,123.75\n"
    "2,30000,4000,2700.0,360.0,37.5\n"
)
_GOLDEN_INDICES_CSV = (
    "RMSE,wRMSE,MedAE,R2,ncell,csize_coarse_grid,csize_coarse_grid_ha\n"
    "2619.28,2964.98,2250.0,0.43,3,300,8100.0\n"
)


def test_validate_two_layer_matches_golden_metrics_and_csvs(tmp_path):
    """Characterization test: numbers and CSV bytes must never move."""
    lay = _varied_validation_fixture(tmp_path)
    idx = validate_two_layer(
        **lay, csize_coarse_grid=300,
        indices_file_pred=tmp_path / "indices.csv",
        tab_file_pred=tmp_path / "pred_obs.csv",
        fig_file_pred=tmp_path / "pred_obs.png",
        model_name="TEST", period="calibration",
    )
    assert idx == _GOLDEN_INDICES
    assert (tmp_path / "pred_obs.csv").read_text() == _GOLDEN_POINT_CSV
    assert (tmp_path / "indices.csv").read_text() == _GOLDEN_INDICES_CSV
    assert (tmp_path / "pred_obs.png").stat().st_size > 1000


def test_compute_validation_drops_cells_with_zero_forest(tmp_path):
    """Cells with nfor_obs == 0 (no forest recorded at the start of the period)
    must be excluded from the result entirely. The fixture's bottom half (cells
    3, 4, 5) has zero forest and zero deforestation everywhere; only the top
    row's cells 0/1/2 may survive the filter."""
    lay = _varied_validation_fixture(tmp_path)
    result = compute_validation(**lay, csize_coarse_grid=300,
                                model_name="TEST", period="calibration")
    assert set(result.plot_data.points["cell"]) == {0, 1, 2}
    assert result.indices["ncell"] == 3


def test_validate_two_layer_forwards_figsize_and_dpi_to_png(tmp_path):
    """figsize/dpi forwarding is essentially the wrapper's remaining job: a
    non-default value must actually reach the rendered PNG's pixel dimensions,
    not just be accepted and silently dropped."""
    import matplotlib.image as mpimg

    lay = _varied_validation_fixture(tmp_path)
    fig_path = tmp_path / "pred_obs.png"
    validate_two_layer(
        **lay, csize_coarse_grid=300,
        indices_file_pred=tmp_path / "indices.csv",
        tab_file_pred=tmp_path / "pred_obs.csv",
        fig_file_pred=fig_path,
        model_name="TEST", period="calibration",
        figsize=(3.0, 3.0), dpi=50,
    )
    img = mpimg.imread(fig_path)
    height, width = img.shape[0], img.shape[1]
    assert (width, height) == (150, 150)  # figsize(3.0, 3.0) * dpi(50)


def _points(obs, pred):
    """Minimal points frame with just the two columns the plot layer reads."""
    return pd.DataFrame({"ndefor_obs_ha": obs, "ndefor_pred_ha": pred})


def test_pred_obs_axis_bounds_spans_both_series(tmp_path):
    lo, hi = pred_obs_axis_bounds(_points([2430.0, 4050.0, 360.0], [180.0, 123.75, 37.5]))
    assert (lo, hi) == (37.5, 4050.0)


def test_pred_obs_axis_bounds_empty_falls_back_to_unit_range():
    assert pred_obs_axis_bounds(_points([], [])) == (0.0, 1.0)


def test_pred_obs_axis_bounds_all_nan_falls_back_to_unit_range():
    assert pred_obs_axis_bounds(_points([np.nan, np.nan], [np.nan, np.nan])) == (0.0, 1.0)


def test_pred_obs_axis_bounds_ignores_infinities():
    lo, hi = pred_obs_axis_bounds(_points([1.0, np.inf], [-np.inf, 4.0]))
    assert (lo, hi) == (1.0, 4.0)


def test_pred_obs_axis_bounds_all_infinite_falls_back_to_unit_range():
    assert pred_obs_axis_bounds(_points([np.inf, -np.inf], [np.inf, np.inf])) == (0.0, 1.0)


def test_pred_obs_axis_bounds_constant_series_is_padded():
    # A zero-width domain would collapse an ECharts axis; pad it symmetrically.
    assert pred_obs_axis_bounds(_points([5.0, 5.0], [5.0, 5.0])) == (0.0, 10.0)


def test_pred_obs_axis_bounds_all_zero_falls_back_to_unit_range():
    assert pred_obs_axis_bounds(_points([0.0, 0.0], [0.0, 0.0])) == (0.0, 1.0)


def test_finite_points_drops_non_finite_rows_without_touching_points():
    points = _points([1.0, np.nan, 3.0, np.inf], [1.0, 2.0, np.inf, 4.0])
    data = PredObsPlotData(
        model="M", period="p", csize_px=300, csize_ha=8100.0, points=points,
        axis_min=1.0, axis_max=3.0, medae=0.0, r2=1.0, ncell=4,
    )
    assert len(data.points) == 4                      # CSV payload untouched
    assert list(data.finite_points["ndefor_obs_ha"]) == [1.0]
    assert list(data.finite_points["ndefor_pred_ha"]) == [1.0]


def test_compute_validation_returns_indices_and_plot_data(tmp_path):
    lay = _varied_validation_fixture(tmp_path)
    result = compute_validation(**lay, csize_coarse_grid=300,
                                model_name="TEST", period="calibration")

    assert isinstance(result, ValidationResult)
    assert result.indices == _GOLDEN_INDICES

    pd_ = result.plot_data
    assert isinstance(pd_, PredObsPlotData)
    assert (pd_.model, pd_.period) == ("TEST", "calibration")
    assert (pd_.csize_px, pd_.csize_ha) == (300, 8100.0)
    assert (pd_.axis_min, pd_.axis_max) == (37.5, 4050.0)
    assert (pd_.medae, pd_.r2, pd_.ncell) == (2250.0, 0.43, 3)
    assert list(pd_.points.columns) == [
        "cell", "nfor_obs", "ndefor_obs", "nfor_obs_ha", "ndefor_obs_ha", "ndefor_pred_ha",
    ]


def test_compute_validation_writes_nothing(tmp_path):
    lay = _varied_validation_fixture(tmp_path)
    before = sorted(p.name for p in tmp_path.iterdir())
    compute_validation(**lay, csize_coarse_grid=300)
    assert sorted(p.name for p in tmp_path.iterdir()) == before


def test_plot_data_carries_chart_labels():
    data = PredObsPlotData(
        model="GLM", period="calibration", csize_px=300, csize_ha=8100.0,
        points=_points([1.0], [2.0]), axis_min=1.0, axis_max=2.0,
        medae=1.5, r2=0.42, ncell=1,
    )
    assert data.title == (
        "GLM model, calibration period\n"
        "Predicted vs. observed deforestation in 8100.0 ha grid cells."
    )
    assert data.annotation == "MedAE = 1.50 ha\nR2 = 0.42\nn = 1"
    assert (data.x_label, data.y_label) == (PRED_OBS_X_LABEL, PRED_OBS_Y_LABEL)


def _base_plot_data_kwargs():
    """Valid PredObsPlotData kwargs (2 finite points, sane axis bounds), so
    each __post_init__ test only overrides the one field under test."""
    return dict(
        model="M", period="p", csize_px=300, csize_ha=8100.0,
        points=_points([1.0, 3.0], [2.0, 4.0]),
        axis_min=1.0, axis_max=4.0, medae=0.0, r2=1.0, ncell=2,
    )


def test_plot_data_rejects_nan_axis_bounds():
    kwargs = _base_plot_data_kwargs()
    kwargs["axis_min"] = float("nan")
    with pytest.raises(ValueError):
        PredObsPlotData(**kwargs)


def test_plot_data_rejects_infinite_axis_bounds():
    kwargs = _base_plot_data_kwargs()
    kwargs["axis_max"] = float("inf")
    with pytest.raises(ValueError):
        PredObsPlotData(**kwargs)


def test_plot_data_rejects_zero_width_axis_domain():
    kwargs = _base_plot_data_kwargs()
    kwargs["axis_min"] = kwargs["axis_max"] = 5.0
    with pytest.raises(ValueError):
        PredObsPlotData(**kwargs)


def test_plot_data_rejects_inverted_axis_domain():
    kwargs = _base_plot_data_kwargs()
    kwargs["axis_min"], kwargs["axis_max"] = 4.0, 1.0
    with pytest.raises(ValueError):
        PredObsPlotData(**kwargs)


def test_plot_data_rejects_ncell_mismatched_with_points():
    kwargs = _base_plot_data_kwargs()
    kwargs["ncell"] = 3  # points only has 2 rows
    with pytest.raises(ValueError):
        PredObsPlotData(**kwargs)


def test_plot_data_accepts_well_formed_bounds_and_ncell():
    # Sanity: the valid baseline itself must construct without raising.
    PredObsPlotData(**_base_plot_data_kwargs())


def test_write_pred_obs_csv_matches_golden_bytes(tmp_path):
    lay = _varied_validation_fixture(tmp_path)
    result = compute_validation(**lay, csize_coarse_grid=300)
    out = write_pred_obs_csv(result.plot_data, tmp_path / "points.csv")
    assert _Path(out).read_text() == _GOLDEN_POINT_CSV


def test_write_pred_obs_csv_persists_non_finite_rows(tmp_path):
    """The point CSV is the frozen artifact: ``points`` (not the renderer-safe
    ``finite_points`` subset) must be what gets written, so non-finite rows
    survive to disk exactly as computed."""
    points = _points([1.0, np.nan, 3.0], [1.0, 2.0, np.inf])
    data = PredObsPlotData(
        model="M", period="p", csize_px=300, csize_ha=8100.0, points=points,
        axis_min=1.0, axis_max=3.0, medae=0.0, r2=1.0, ncell=3,
    )
    assert len(data.finite_points) == 1  # sanity: the renderer subset drops 2 rows

    out = write_pred_obs_csv(data, tmp_path / "points.csv")
    lines = _Path(out).read_text().strip().splitlines()
    assert len(lines) == 1 + 3  # header + all 3 rows, non-finite ones included


def test_write_indices_csv_matches_golden_bytes(tmp_path):
    lay = _varied_validation_fixture(tmp_path)
    result = compute_validation(**lay, csize_coarse_grid=300)
    out = write_indices_csv(result.indices, tmp_path / "idx.csv")
    assert _Path(out).read_text() == _GOLDEN_INDICES_CSV


def _legacy_pred_obs_png(df, *, model_name, period, csize_ha, MedAE, r_square,
                         ncell, path, figsize=(6.4, 6.4), dpi=100):
    """The pre-refactor matplotlib block, verbatim, for byte-equivalence proof."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    title = (
        f"{model_name} model, {period} period\n"
        f"Predicted vs. observed deforestation in {csize_ha} ha grid cells."
    )
    p = [df[["ndefor_obs_ha", "ndefor_pred_ha"]].min(axis=None),
         df[["ndefor_obs_ha", "ndefor_pred_ha"]].max(axis=None)]
    fig = plt.figure(figsize=figsize, dpi=dpi)
    ax = plt.subplot(111)
    ax.set_box_aspect(1)
    plt.scatter(df["ndefor_obs_ha"], df["ndefor_pred_ha"],
                color=None, marker="o", edgecolor="k")
    plt.plot(p, p, "r--")
    plt.title(title)
    plt.xlabel("Observed deforestation (ha)")
    plt.ylabel("Predicted deforestation (ha)")
    t = f"MedAE = {MedAE:.2f} ha\nR2 = {r_square:.2f}\nn = {ncell:d}"
    y_text = df[["ndefor_obs_ha", "ndefor_pred_ha"]].max(axis=None)
    plt.text(0, y_text, t, ha="left", va="top")
    fig.savefig(path)
    plt.close(fig)
    return path


def test_save_pred_obs_png_is_byte_identical_to_legacy_matplotlib(tmp_path):
    lay = _varied_validation_fixture(tmp_path)
    result = compute_validation(**lay, csize_coarse_grid=300,
                                model_name="TEST", period="calibration")

    legacy = _legacy_pred_obs_png(
        result.plot_data.points, model_name="TEST", period="calibration",
        csize_ha=8100.0, MedAE=2250.0, r_square=0.43, ncell=3,
        path=tmp_path / "legacy.png",
    )
    new = save_pred_obs_png(result.plot_data, tmp_path / "new.png")

    assert _Path(new).read_bytes() == _Path(legacy).read_bytes()


def test_save_pred_obs_png_handles_degenerate_plot_data(tmp_path):
    """Empty / NaN / constant input must still render, with a finite axis."""
    cols = ["cell", "nfor_obs", "ndefor_obs", "nfor_obs_ha",
            "ndefor_obs_ha", "ndefor_pred_ha"]
    cases = {
        "empty": pd.DataFrame(columns=cols),
        "nan": pd.DataFrame({**{c: [0] for c in cols[:4]},
                             "ndefor_obs_ha": [np.nan], "ndefor_pred_ha": [np.nan]}),
        "constant": pd.DataFrame({**{c: [0, 0] for c in cols[:4]},
                                  "ndefor_obs_ha": [7.0, 7.0],
                                  "ndefor_pred_ha": [7.0, 7.0]}),
    }
    for name, points in cases.items():
        lo, hi = pred_obs_axis_bounds(points)
        assert np.isfinite(lo) and np.isfinite(hi) and lo < hi, name
        data = PredObsPlotData(
            model="M", period="p", csize_px=300, csize_ha=8100.0, points=points,
            axis_min=lo, axis_max=hi, medae=float("nan"), r2=float("nan"),
            ncell=len(points),
        )
        out = save_pred_obs_png(data, tmp_path / f"{name}.png")
        assert _Path(out).stat().st_size > 1000, name


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


def test_evaluation_results_widget_exports_list_and_dialog():
    import inspect
    import gui.widget.evaluation_results as er

    assert hasattr(er, "EvaluationResults")
    assert hasattr(er, "EvaluationTableDialog")
    src = inspect.getsource(er)
    # list reads the persisted registry; dialog renders the saved table
    assert "evaluations" in src
    assert "on_open" in src and "on_delete" in src
    assert "solara.DataFrame" in src
    # Regression: the open action must be an explicit clickable action — on_click
    # on ListItem/ListItemContent does NOT fire in this reacton.ipyvuetify setup,
    # so the row uses ProductTable's "open" ActionSpec (a real Button, icon
    # mdi-table-eye) to open the popup, rather than a clickable row/list item.
    assert '"kind": "open"' in src


def test_evaluation_tile_wires_record_and_dialog():
    import inspect
    import gui.tile.evaluation_tile as et

    src = inspect.getsource(et)
    assert "build_evaluation_record" in src
    assert "add_evaluation" in src
    assert "EvaluationTableDialog" in src
    assert "delete_evaluation" in src
    # background job does the mutate-then-replace re-render
    assert "model_copy()" in src
    # eval_indices transient table is gone
    assert "eval_indices" not in src


# ---------------------------------------------------------------------------
# Chart builders (gui/scripts/evaluation_charts.py)
# ---------------------------------------------------------------------------


def _chart_rows():
    rows = []
    for model, base in [("glm", 1.0), ("rf", 0.7)]:
        for csize in (100, 300):
            rows.append({
                "prediction": f"{model}__d1", "model": model, "period": "d1",
                "csize_coarse_grid": csize, "ncell": 40,
                "MedAE": base * csize / 100, "R2": 0.5, "RMSE": base, "wRMSE": base,
                "fig_path": f"/tmp/pred_obs_{model}_d1_{csize}.png",
            })
    return rows


def test_metric_bars_figure_layout():
    from gui.scripts.evaluation_charts import metric_bars_figure

    fig = metric_bars_figure(_chart_rows(), ["MedAE", "R2"])
    # one bar trace per (metric, csize): 2 metrics x 2 csizes
    assert len(fig.data) == 4
    assert all(tr.type == "bar" for tr in fig.data)
    # x axis carries the map labels
    assert list(fig.data[0].x) == ["glm — d1", "rf — d1"]
    # legend shows one entry per csize (first-metric traces only)
    assert sum(1 for tr in fig.data if tr.showlegend) == 2
    # empty metric selection means "all four"
    fig_all = metric_bars_figure(_chart_rows(), [])
    assert len(fig_all.data) == 8
    # nothing chartable -> None
    assert metric_bars_figure([], ["RMSE"]) is None


def test_figure_entries_and_csizes():
    from gui.scripts.evaluation_charts import figure_entries, record_csizes

    rows = _chart_rows()
    assert record_csizes(rows) == [100, 300]
    entries = figure_entries(rows, 300)
    assert [label for label, _ in entries] == ["glm — d1", "rf — d1"]
    assert str(entries[0][1]).endswith("pred_obs_glm_d1_300.png")


def test_figure_entries_derives_paths_without_fig_path_column():
    """Real records store indices WITHOUT fig_path (evaluate_against_truth's
    explicit column list drops it) — entries must be derived from the record's
    evaluation folder instead of coming up empty."""
    from pathlib import Path

    from gui.scripts.evaluation_charts import figure_entries

    rows = [{k: v for k, v in r.items() if k != "fig_path"} for r in _chart_rows()]
    fig_dir = Path("/data/proj/evaluation/loss_2010")
    entries = figure_entries(rows, 300, fig_dir=fig_dir)
    assert [label for label, _ in entries] == ["glm — d1", "rf — d1"]
    assert entries[0][1] == fig_dir / "pred_obs_glm_d1_300.png"
    # without a fig_dir there is nothing to derive from
    assert figure_entries(rows, 300) == []


def test_evaluation_dialog_has_tabs_and_csize_select():
    import inspect
    import gui.widget.evaluation_results as er

    src = inspect.getsource(er)
    assert "metric_bars_figure" in src and "FigurePlotly" in src
    assert "figure_entries" in src and "csize_select_label" in src
    assert "rv.Tabs" in src and "rv.TabsItems" in src
