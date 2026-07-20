import types
import types as _types
from pathlib import Path as _Path

import numpy as np
import pandas as pd
import pytest

rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin

from spatialrisk.evaluation import (
    PLOT_COLUMNS,
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
    """Minimal points frame: the four columns every renderer path requires.

    ``pred_obs_axis_bounds`` only reads the two coordinate columns, but
    ``PredObsPlotData`` requires all of ``PLOT_COLUMNS``, so this builds the
    narrowest frame the dataclass accepts.
    """
    n = len(list(obs))
    return pd.DataFrame({
        "cell": list(range(n)),
        "nfor_obs_ha": [100.0] * n,
        "ndefor_obs_ha": obs,
        "ndefor_pred_ha": pred,
    })


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


@pytest.mark.parametrize("missing", PLOT_COLUMNS)
def test_plot_data_rejects_points_missing_a_required_plot_column(missing):
    """``points`` has two construction paths of different widths.

    ``compute_validation`` builds the full 6-column CSV table; the GUI loader
    reads back only the plotted columns. Both must carry every column a renderer
    indexes, so the guard is on the columns rather than on the width — which
    also turns a truncated or renamed CSV into an error at construction instead
    of a KeyError inside a render.
    """
    kwargs = _base_plot_data_kwargs()
    kwargs["points"] = kwargs["points"].drop(columns=[missing])
    with pytest.raises(ValueError, match="missing required plot column"):
        PredObsPlotData(**kwargs)


def test_plot_data_accepts_the_wider_compute_validation_frame(tmp_path):
    """The other path: 6 columns is MORE than required, never an error."""
    lay = _varied_validation_fixture(tmp_path)
    points = compute_validation(**lay, csize_coarse_grid=300).plot_data.points

    assert len(points.columns) == 6
    data = PredObsPlotData(
        model="M", period="p", csize_px=300, csize_ha=8100.0, points=points,
        axis_min=1.0, axis_max=4.0, medae=0.0, r2=1.0, ncell=len(points),
    )
    assert list(data.points.columns) == list(points.columns)


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


# ---------------------------------------------------------------------------
# Run-scoped, history-safe artifacts (Task 4)
# ---------------------------------------------------------------------------


def _truth_project_and_pred(tmp_path):
    """Fake project holding ONE prediction, scored against an explicit truth."""
    pred = _types.SimpleNamespace(
        model_key="glm_glm_v1", window=None, dataset_name="validation",
        path=tmp_path / "risk.tif", metrics={},
        storage_key=lambda: "glm_glm_v1__validation")
    project = _types.SimpleNamespace(
        folders=_types.SimpleNamespace(project_folder=tmp_path),
        predictions={"k1": pred}, save=lambda: None)
    return project, pred


def _fake_defrate(**kw):
    _Path(kw["tab_file_defrate"]).write_text("cat,defor_dens\n1,0.01\n")


def _fake_validate_writing(value):
    """validate_two_layer stand-in whose three artifacts encode ``value``.

    Lets a test tell one run's files apart from another's byte-for-byte, with
    the same truth/model/period/cell size — i.e. exactly the collision the
    run-scoped layout must survive.
    """
    def fake_validate(**kw):
        _Path(kw["tab_file_pred"]).write_text(f"cell,ndefor_obs_ha\n0,{value}\n")
        _Path(kw["fig_file_pred"]).write_bytes(f"PNG-{value}".encode())
        _Path(kw["indices_file_pred"]).write_text(f"MedAE\n{value}\n")
        return {"RMSE": value, "wRMSE": value, "MedAE": value, "R2": 0.9,
                "ncell": 26, "csize_coarse_grid": kw["csize_coarse_grid"],
                "csize_coarse_grid_ha": 8100.0}
    return fake_validate


_TRUTH_TAG = "forest_loss_2015_2020"


def _run_against_truth(project, tmp_path, run_id, value, monkeypatch, csizes=(300,)):
    monkeypatch.setattr(ev, "_defrate_per_cat", _fake_defrate)
    monkeypatch.setattr(ev, "validate_two_layer", _fake_validate_writing(value))
    return ev.evaluate_against_truth(
        project, prediction_keys=["k1"], defor_file=tmp_path / "d.tif",
        forest_file=tmp_path / "f.tif", time_interval=5, truth_tag=_TRUTH_TAG,
        csizes=csizes, run_id=run_id)


def _spec(tmp_path):
    return {"defor_file": str(tmp_path / "d.tif"),
            "forest_file": str(tmp_path / "f.tif"),
            "time_interval": 5, "truth_tag": _TRUTH_TAG}


def test_two_runs_same_truth_retain_distinct_artifacts(tmp_path, monkeypatch):
    """HEADLINE REGRESSION: a later run must not overwrite an older saved run.

    Two runs share truth, model, period AND cell size — under the old shared
    ``evaluation/<truth_tag>/`` layout the second silently clobbered the first's
    point CSV and PNG, so reopening the older record showed the newer numbers.
    """
    from gui.scripts.evaluation_charts import figure_entries
    from gui.tile.evaluation_helpers import build_evaluation_record

    project, _pred = _truth_project_and_pred(tmp_path)
    runs = [("run00001", 11.0), ("run00002", 22.0)]
    records = []
    for i, (run_id, value) in enumerate(runs):
        df = _run_against_truth(project, tmp_path, run_id, value, monkeypatch)
        records.append(build_evaluation_record(
            project, df, _spec(tmp_path), resolved_keys=["k1"], run_id=run_id,
            created_at=f"2026-06-22T14:0{i}:00", csizes=(300,)))

    for record, (run_id, value) in zip(records, runs):
        assert len(record.artifacts) == 1, "one artifact per map per cell size"
        art = record.artifacts[0]
        assert art.prediction_key == "glm_glm_v1__validation"
        assert art.model == "GLM" and art.period == "validation"
        assert art.csize_px == 300
        # each record's own files survived the other run untouched
        assert _Path(art.points_csv).read_text() == f"cell,ndefor_obs_ha\n0,{value}\n"
        assert _Path(art.png_path).read_bytes() == f"PNG-{value}".encode()
        assert run_id in art.points_csv and run_id in art.png_path
        # ...and the path the Figures tab derives resolves to that same file
        fig_dir = _Path(record.csv_path).parent
        entries = figure_entries(record.indices, 300, fig_dir=fig_dir)
        assert [str(p) for _, p in entries] == [art.png_path]
        assert entries[0][1].read_bytes() == f"PNG-{value}".encode()

    assert records[0].artifacts[0].png_path != records[1].artifacts[0].png_path
    assert records[0].csv_path != records[1].csv_path


def test_run_scoped_artifacts_live_under_run_directory(tmp_path, monkeypatch):
    project, _pred = _truth_project_and_pred(tmp_path)
    _run_against_truth(project, tmp_path, "run00001", 11.0, monkeypatch)

    run_dir = tmp_path / "evaluation" / _TRUTH_TAG / "run00001"
    assert run_dir.is_dir()
    for name in ("defrate_cat_GLM_validation.csv",
                 "pred_obs_GLM_validation_300.csv",
                 "indices_GLM_validation_300.csv",
                 "pred_obs_GLM_validation_300.png",
                 "indices_all.csv"):
        assert (run_dir / name).exists(), name


def test_run_scoped_evaluation_also_publishes_legacy_shared_paths(tmp_path, monkeypatch):
    """Dual-publish shim: notebooks reading the old shared paths keep working."""
    project, _pred = _truth_project_and_pred(tmp_path)
    _run_against_truth(project, tmp_path, "run00001", 11.0, monkeypatch)
    _run_against_truth(project, tmp_path, "run00002", 22.0, monkeypatch)

    shared = tmp_path / "evaluation" / _TRUTH_TAG
    for name in ("defrate_cat_GLM_validation.csv",
                 "pred_obs_GLM_validation_300.csv",
                 "indices_GLM_validation_300.csv",
                 "pred_obs_GLM_validation_300.png",
                 "indices_all.csv"):
        assert (shared / name).exists(), name
    # the shared copy tracks the LATEST run
    assert (shared / "pred_obs_GLM_validation_300.png").read_bytes() == b"PNG-22.0"
    # while the older run's own copy is untouched
    assert (shared.parent / _TRUTH_TAG / "run00001"
            / "pred_obs_GLM_validation_300.png").read_bytes() == b"PNG-11.0"


def test_evaluate_against_truth_without_run_id_keeps_legacy_layout(tmp_path, monkeypatch):
    """The notebook path (no run_id) writes exactly where it always did."""
    project, _pred = _truth_project_and_pred(tmp_path)
    df = _run_against_truth(project, tmp_path, None, 11.0, monkeypatch)

    shared = tmp_path / "evaluation" / _TRUTH_TAG
    assert (shared / "pred_obs_GLM_validation_300.png").read_bytes() == b"PNG-11.0"
    assert (shared / "indices_all.csv").exists()
    # no run sub-directory was created, and no artifacts are claimed
    assert [p for p in shared.iterdir() if p.is_dir()] == []
    assert df.attrs.get("artifacts", []) == []


def test_evaluate_against_truth_threads_run_id_into_each_prediction(tmp_path, monkeypatch):
    seen = {}

    def fake_one(proj, pred, **kw):
        seen.update(kw)
        return []

    project, _pred = _truth_project_and_pred(tmp_path)
    monkeypatch.setattr(ev, "_evaluate_one_against_truth", fake_one)
    ev.evaluate_against_truth(
        project, prediction_keys=["k1"], defor_file=tmp_path / "d.tif",
        forest_file=tmp_path / "f.tif", time_interval=5, truth_tag=_TRUTH_TAG,
        run_id="run00001")
    assert seen["run_id"] == "run00001"


def test_one_artifact_per_prediction_per_cell_size(tmp_path, monkeypatch):
    project, _pred = _truth_project_and_pred(tmp_path)
    df = _run_against_truth(project, tmp_path, "run00001", 11.0, monkeypatch,
                            csizes=(100, 300))
    arts = df.attrs["artifacts"]
    assert sorted(a.csize_px for a in arts) == [100, 300]
    assert {a.model for a in arts} == {"GLM"}
    assert len({a.points_csv for a in arts}) == 2


def test_legacy_record_without_artifacts_still_shows_its_pngs(tmp_path):
    """Acceptance: pre-Task-4 records keep loading AND keep displaying figures.

    Such a record has ``artifacts == []`` and a ``csv_path`` pointing at the
    SHARED evaluation/<truth_tag>/indices_all.csv, so the figure directory is
    the shared folder and the derived-filename branch must still find the PNG.
    """
    from gui.scripts.evaluation_charts import figure_entries
    from spatialrisk.evaluations import EvaluationRecord

    shared = tmp_path / "evaluation" / _TRUTH_TAG
    shared.mkdir(parents=True)
    png = shared / "pred_obs_GLM_validation_300.png"
    png.write_bytes(b"PNG-legacy")

    record = EvaluationRecord(
        truth_tag=_TRUTH_TAG, truth_defor="d", truth_forest="f", time_interval=5,
        prediction_keys=["k1"], csizes=[300], created_at="2026-06-01T10:00:00",
        indices=[{"model": "GLM", "period": "validation",
                  "csize_coarse_grid": 300, "MedAE": 1.0}],
        csv_path=str(shared / "indices_all.csv"), run_id="legacy00")

    assert record.artifacts == []
    entries = figure_entries(record.indices, 300,
                             fig_dir=_Path(record.csv_path).parent)
    assert [p for _, p in entries] == [png]
    assert entries[0][1].read_bytes() == b"PNG-legacy"


def test_evaluation_tile_threads_run_id_and_orders_delete():
    import inspect
    import gui.tile.evaluation_tile as et

    src = inspect.getsource(et)
    # the run id reaches the computation, not just the record builder
    assert "run_id=job_id" in src
    # deletion goes through the helper that commits BEFORE removing files
    assert "delete_evaluation_run" in src


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


# --- metric_bar_option: one serializable ECharts option per metric ---------
#
# These replace the old Plotly object-structure assertions
# (test_metric_bars_figure_layout). The single multi-subplot figure is gone:
# each metric now gets its own option dict, laid out two-per-row by the widget.


def test_metric_bar_option_categories_are_the_map_labels():
    """x axis = one category per model/period label, sorted (as Plotly did)."""
    from gui.scripts.evaluation_charts import metric_bar_option

    option = metric_bar_option(_chart_rows(), "MedAE")
    assert option["xAxis"]["type"] == "category"
    assert option["xAxis"]["data"] == ["glm — d1", "rf — d1"]


def test_metric_bar_option_has_one_bar_series_per_cell_size():
    from gui.scripts.evaluation_charts import metric_bar_option

    option = metric_bar_option(_chart_rows(), "MedAE")
    assert [s["type"] for s in option["series"]] == ["bar", "bar"]
    assert [s["name"] for s in option["series"]] == ["csize 100 px", "csize 300 px"]


def test_metric_bar_option_series_data_is_the_metric_values_per_label():
    """Values follow the category order, one list per cell size."""
    from gui.scripts.evaluation_charts import metric_bar_option

    option = metric_bar_option(_chart_rows(), "MedAE")
    # _chart_rows: MedAE = base * csize / 100, base 1.0 (glm) / 0.7 (rf)
    assert option["series"][0]["data"] == [1.0, 0.7]
    assert option["series"][1]["data"] == [3.0, pytest.approx(2.1)]


def test_metric_bar_option_leaves_a_missing_label_csize_pair_empty():
    """A map evaluated at only one cell size must not shift the other bars."""
    from gui.scripts.evaluation_charts import metric_bar_option

    rows = [r for r in _chart_rows()
            if not (r["model"] == "rf" and r["csize_coarse_grid"] == 300)]
    option = metric_bar_option(rows, "MedAE")
    assert option["xAxis"]["data"] == ["glm — d1", "rf — d1"]
    assert option["series"][1]["data"] == [3.0, None]


def test_metric_bar_option_title_carries_the_direction_hint():
    from gui.scripts.evaluation_charts import metric_bar_option

    titles = {m: metric_bar_option(_chart_rows(), m)["title"]["text"]
              for m in ("MedAE", "R2", "RMSE", "wRMSE")}
    assert titles == {
        "MedAE": "MedAE (ha) ↓",
        "R2": "R² ↑",
        "RMSE": "RMSE (ha) ↓",
        "wRMSE": "wRMSE (ha) ↓",
    }


def test_metric_bar_option_shows_the_legend_only_for_several_cell_sizes():
    """Matches the old showlegend=len(csizes) > 1."""
    from gui.scripts.evaluation_charts import metric_bar_option

    many = metric_bar_option(_chart_rows(), "MedAE")
    assert many["legend"]["show"] is True
    assert many["legend"]["data"] == ["csize 100 px", "csize 300 px"]

    one = metric_bar_option(
        [r for r in _chart_rows() if r["csize_coarse_grid"] == 100], "MedAE")
    assert one["legend"]["show"] is False


def test_metric_bar_option_reserves_the_legend_row_only_when_it_shows():
    """A hidden legend must not leave a blank band above the plot.

    grid.top has to clear the title always, and the legend row (placed at
    top=24) only when there is more than one cell size to name.
    """
    from gui.scripts.evaluation_charts import metric_bar_option

    with_legend = metric_bar_option(_chart_rows(), "MedAE")
    assert with_legend["legend"]["show"] is True
    assert with_legend["grid"]["top"] == 52

    one_csize = metric_bar_option(
        [r for r in _chart_rows() if r["csize_coarse_grid"] == 100], "MedAE")
    assert one_csize["legend"]["show"] is False
    assert one_csize["grid"]["top"] == with_legend["legend"]["top"] == 24


def test_metric_bar_option_colors_bars_from_the_app_palette():
    """The application-owned Blues ramp, not plotly.colors.sample_colorscale."""
    from gui.scripts.echarts_options import csize_colors
    from gui.scripts.evaluation_charts import metric_bar_option

    option = metric_bar_option(_chart_rows(), "MedAE")
    assert [s["itemStyle"]["color"] for s in option["series"]] == csize_colors(2)

    single = metric_bar_option(
        [r for r in _chart_rows() if r["csize_coarse_grid"] == 100], "MedAE")
    assert single["series"][0]["itemStyle"]["color"] == "#2a78d6"


def test_metric_bar_option_tooltip_shows_label_value_and_cell_size():
    """ECharts template tokens: {b} category, {c} value, {a} series name."""
    from gui.scripts.evaluation_charts import metric_bar_option

    tooltip = metric_bar_option(_chart_rows(), "MedAE")["tooltip"]
    assert tooltip["trigger"] == "item"
    assert tooltip["formatter"] == "{b}<br/>MedAE = {c}<br/>{a}"
    # the series name is what carries "csize N px" into {a}
    assert metric_bar_option(_chart_rows(), "R2")["series"][1]["name"] == "csize 300 px"


def test_metric_bar_option_uses_the_theme_ink_and_grid_colors():
    """themed_option only sets ink; the grid colour must be wired in here."""
    from gui.scripts.echarts_options import theme_colors
    from gui.scripts.evaluation_charts import metric_bar_option

    for dark in (False, True):
        colors = theme_colors(dark)
        option = metric_bar_option(_chart_rows(), "MedAE", dark=dark)
        assert option["title"]["textStyle"]["color"] == colors["ink"]
        assert option["legend"]["textStyle"]["color"] == colors["ink"]
        assert option["xAxis"]["axisLabel"]["color"] == colors["ink"]
        assert option["yAxis"]["axisLabel"]["color"] == colors["ink"]
        assert option["xAxis"]["axisLine"]["lineStyle"]["color"] == colors["grid"]
        assert option["yAxis"]["splitLine"]["lineStyle"]["color"] == colors["grid"]


def test_metric_bar_option_keeps_the_x_axis_gridlines_off():
    """Plotly parity: update_xaxes(showgrid=False)."""
    from gui.scripts.evaluation_charts import metric_bar_option

    option = metric_bar_option(_chart_rows(), "MedAE")
    assert option["xAxis"]["splitLine"]["show"] is False


def test_metric_bar_option_is_json_serializable():
    """EChartsRawWidget hands the dict straight to the frontend."""
    import json

    from gui.scripts.evaluation_charts import metric_bar_option

    json.dumps(metric_bar_option(_chart_rows(), "MedAE"))


def test_metric_bar_option_returns_none_when_nothing_is_chartable():
    from gui.scripts.evaluation_charts import metric_bar_option

    assert metric_bar_option([], "RMSE") is None
    # a metric no row carries has nothing to draw
    rows = [{k: v for k, v in r.items() if k != "wRMSE"} for r in _chart_rows()]
    assert metric_bar_option(rows, "wRMSE") is None
    # an unknown metric key has no title and no data
    assert metric_bar_option(_chart_rows(), "nope") is None
    # rows without a cell size cannot be split into series
    assert metric_bar_option(
        [{"model": "glm", "period": "d1", "MedAE": 1.0}], "MedAE") is None


def test_record_metrics_keeps_the_canonical_order_and_drops_empties():
    """Unchanged behaviour — the widget builds one option per returned metric."""
    from gui.scripts.evaluation_charts import record_metrics

    rows = _chart_rows()
    assert record_metrics(rows, []) == ["MedAE", "R2", "RMSE", "wRMSE"]
    assert record_metrics(rows, ["R2", "MedAE"]) == ["R2", "MedAE"]
    assert record_metrics(rows, ["R2", "bogus"]) == ["R2"]
    stripped = [{k: v for k, v in r.items() if k != "R2"} for r in rows]
    assert record_metrics(stripped, ["R2", "MedAE"]) == ["MedAE"]


# --- widget identity --------------------------------------------------------
#
# There is no chart_identity helper any more: EChartsChart digests the option
# it is handed, so nothing the chart DRAWS has to be re-listed by a caller (see
# tests/test_echarts_adapter.py). What the option cannot show — the active tab,
# which run is on screen — is asserted on the rendered tab further down.


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
    assert "metric_bar_option" in src and "EChartsChart" in src
    assert "figure_entries" in src and "csize_select_label" in src
    assert "rv.Tabs" in src and "rv.TabsItems" in src


def test_evaluation_dialog_drops_the_plotly_modebar_workaround():
    """The modebar was a FigurePlotly artefact; the table width rule stays."""
    import inspect
    import gui.widget.evaluation_results as er

    src = inspect.getsource(er)
    assert "modebar" not in src
    assert "width: 100%" in src


def _run_blocked(blocked, body):
    """Run ``body`` in a subprocess where importing ``blocked`` raises."""
    import subprocess
    import sys
    from pathlib import Path

    code = (
        "import sys\n"
        f"BLOCKED = {tuple(blocked)!r}\n"
        "class Block:\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name.split('.')[0] in BLOCKED:\n"
        "            raise ImportError('blocked: ' + name)\n"
        "        return None\n"
        "sys.meta_path.insert(0, Block())\n"
        + body
    )
    root = Path(__file__).resolve().parents[1]
    return subprocess.run(
        [sys.executable, "-c", code], cwd=root, capture_output=True, text=True)


_CHART_SMOKE = (
    "rows = [{'model': m, 'period': 'd1', 'csize_coarse_grid': c,\n"
    "         'MedAE': 1.0, 'R2': 0.5, 'RMSE': 1.0, 'wRMSE': 1.0}\n"
    "        for m in ('glm', 'rf') for c in (100, 300)]\n"
    "opt = ec.metric_bar_option(rows, 'MedAE')\n"
    "assert opt['series'][0]['itemStyle']['color'].startswith('#')\n"
    "assert opt['title']['text'] and opt['legend']['show'] is True\n"
)

# The scatter half of the same layering rule. Builds from an in-memory
# PredObsPlotData rather than a file so the smoke needs no fixture on disk —
# the point is that the option BUILDS, which is where a lazy import would bite.
_SCATTER_SMOKE = (
    "import pandas as pd\n"
    "import gui.scripts.evaluation_echarts as ee\n"
    "from spatialrisk.evaluation import PredObsPlotData\n"
    "pts = pd.DataFrame({'cell': [0, 1], 'nfor_obs_ha': [9.0, 8.0],\n"
    "                    'ndefor_obs_ha': [1.0, 2.0],\n"
    "                    'ndefor_pred_ha': [1.5, 2.5]})\n"
    "pd_ = PredObsPlotData(model='glm', period='d1', csize_px=300,\n"
    "                      csize_ha=90.0, points=pts, axis_min=1.0,\n"
    "                      axis_max=2.5, medae=0.5, r2=0.9, ncell=2)\n"
    "sopt = ee.pred_obs_scatter_option(pd_)\n"
    "assert [v[:2] for v in sopt['series'][0]['data']] == [[1.0, 1.5], [2.0, 2.5]]\n"
    "assert sopt['xAxis']['min'] == sopt['yAxis']['min'] == 1.0\n"
    "assert ee.pred_obs_renderer(pd_) == 'svg'\n"
)


def test_evaluation_gui_path_imports_no_plotly():
    """No module the Evaluation tile pulls in may still reach for plotly.

    Blocks 'plotly' at import time in a subprocess and then actually BUILDS a
    chart option: the old code imported plotly lazily inside the figure
    builder, so importing the modules alone would not have caught it. The
    palette call is the one that used to land in plotly.colors.
    """
    proc = _run_blocked(
        ["plotly"],
        "import gui.scripts.evaluation_charts as ec\n"
        "import gui.widget.evaluation_results  # noqa: F401\n"
        "import gui.tile.evaluation_tile  # noqa: F401\n"
        + _CHART_SMOKE + _SCATTER_SMOKE +
        "assert 'plotly' not in sys.modules\n"
        "print('OK')\n",
    )
    assert proc.returncode == 0, proc.stderr
    assert "OK" in proc.stdout


def test_evaluation_charts_builds_options_without_solara():
    """gui/scripts/* is solara-free by the app's layering rule.

    The chart builders moved from plotly to ECharts but must still import (and
    run) with solara, ipyvuetify and ipecharts all blocked — the widget half of
    the adapter is the only module allowed to know ipecharts exists. Covers
    BOTH builders: the metric bars and the predicted-vs-observed scatter.
    """
    proc = _run_blocked(
        ["solara", "reacton", "ipyvuetify", "ipecharts", "plotly"],
        "import gui.scripts.evaluation_charts as ec\n"
        + _CHART_SMOKE + _SCATTER_SMOKE +
        "print('OK')\n",
    )
    assert proc.returncode == 0, proc.stderr
    assert "OK" in proc.stdout


# ---------------------------------------------------------------------------
# Charts tab — headless render (the evaluation dialog is not covered by any
# other render test, and EChartsChart's identity contract can only fail here)
# ---------------------------------------------------------------------------

def _chart_record(metrics=("MedAE", "R2"), rows=None):
    import types as _t

    return _t.SimpleNamespace(
        indices=list(_chart_rows() if rows is None else rows),
        metrics=list(metrics),
        csv_path="/data/proj/evaluation/loss_2010/indices_all.csv",
        truth_tag="loss_2010",
    )


def _render_charts_tab(**kwargs):
    import ipecharts
    import reacton

    from gui.i18n import t as _t

    _t("common.close")  # warm the translator before the first render
    from gui.widget.evaluation_results import _ChartsTab

    kwargs.setdefault("record", _chart_record())
    kwargs.setdefault("eval_key", "run-a")
    kwargs.setdefault("active_tab", 1)
    _, rc = reacton.render(_ChartsTab(**kwargs), handle_error=False)
    return rc, ipecharts.EChartsRawWidget


def test_charts_tab_renders_one_chart_per_selected_metric():
    rc, cls = _render_charts_tab()
    assert len(rc.find(cls).widgets) == 2


def test_charts_tab_renders_all_four_metrics_when_none_were_selected():
    rc, cls = _render_charts_tab(record=_chart_record(metrics=()))
    assert len(rc.find(cls).widgets) == 4


def test_charts_tab_charts_carry_the_per_metric_options():
    rc, cls = _render_charts_tab()
    titles = [w.option["title"]["text"] for w in rc.find(cls).widgets]
    assert titles == ["MedAE (ha) ↓", "R² ↑"]


def test_charts_tab_builds_its_options_from_the_live_theme():
    """The dark theme must reach metric_bar_option, not just the widget.

    themed_option (applied inside the adapter) only sets the top-level ink, so
    a `dark=` that never reaches the builder leaves the title, the legend, both
    axis labels and both grid colours on their light values over a dark
    surface. Asserted on title.textStyle.color specifically: the option's
    top-level textStyle would still be dark via the adapter and would hide the
    bug.
    """
    from solara.lab.components.theming import theme

    from gui.scripts.echarts_options import theme_colors

    before = theme.dark
    try:
        theme.dark = True
        rc, cls = _render_charts_tab()
        option = rc.find(cls).widgets[0].option
        assert option["title"]["textStyle"]["color"] == theme_colors(True)["ink"]
        assert option["xAxis"]["axisLabel"]["color"] == theme_colors(True)["ink"]
        assert (option["yAxis"]["splitLine"]["lineStyle"]["color"]
                == theme_colors(True)["grid"])
    finally:
        theme.dark = before

    rc, cls = _render_charts_tab()
    assert (rc.find(cls).widgets[0].option["title"]["textStyle"]["color"]
            == theme_colors(False)["ink"])


def test_charts_tab_draws_the_bar_charts_with_the_svg_renderer():
    """Renderer is a deliberate per-call-site choice, not a default to drift.

    Small bar charts: SVG (crisp text, tiny DOM). Canvas here would be a silent
    performance/quality change, which is what resolve_renderer exists to stop.
    """
    rc, cls = _render_charts_tab()
    assert {w.renderer for w in rc.find(cls).widgets} == {"svg"}


def test_charts_tab_lays_metrics_out_in_two_columns():
    import ipyvuetify as vw

    rc, _ = _render_charts_tab()
    grids = [w for w in rc.find(vw.Html).widgets
             if "grid-template-columns" in (w.style_ or "")]
    assert grids and "repeat(2," in grids[0].style_


def test_charts_tab_uses_a_single_column_for_a_single_metric():
    import ipyvuetify as vw

    rc, _ = _render_charts_tab(record=_chart_record(metrics=("R2",)))
    grids = [w for w in rc.find(vw.Html).widgets
             if "grid-template-columns" in (w.style_ or "")]
    assert grids and "repeat(1," in grids[0].style_


def test_charts_tab_swaps_the_chart_when_the_metric_selection_changes():
    """The identity must carry the metric: same run, different selection.

    Position 0 held MedAE; after the re-render it must hold R2. If the metric
    were missing from the identity, use_memo would hand back the stale MedAE
    widget with no error at all.
    """
    import reacton

    from gui.widget.evaluation_results import _ChartsTab

    rc, cls = _render_charts_tab()
    assert rc.find(cls).widgets[0].option["title"]["text"] == "MedAE (ha) ↓"
    rc.render(_ChartsTab(record=_chart_record(metrics=("R2",)),
                         eval_key="run-a", active_tab=1))
    assert rc.find(cls).widgets[0].option["title"]["text"] == "R² ↑"


def test_charts_tab_rebuilds_the_chart_when_the_charted_values_change():
    """Same run key, different index rows — the option must reach the widget."""
    from gui.widget.evaluation_results import _ChartsTab

    rc, cls = _render_charts_tab()
    first = rc.find(cls).widgets[0]
    bumped = [{**r, "MedAE": r["MedAE"] + 5} for r in _chart_rows()]
    rc.render(_ChartsTab(record=_chart_record(rows=bumped),
                         eval_key="run-a", active_tab=1))
    second = rc.find(cls).widgets[0]
    assert second is not first
    assert second.option["series"][0]["data"] == [6.0, 5.7]


def test_charts_tab_rebuilds_its_charts_when_the_tab_becomes_active():
    """ipecharts sizes on attach only, so re-entering the tab rebuilds."""
    from gui.widget.evaluation_results import _ChartsTab

    rc, cls = _render_charts_tab(active_tab=1)
    first = rc.find(cls).widgets[0]
    rc.render(_ChartsTab(record=_chart_record(), eval_key="run-a", active_tab=2))
    assert rc.find(cls).widgets[0] is not first


def test_charts_tab_says_so_when_there_is_nothing_to_chart():
    import ipecharts

    rc, _ = _render_charts_tab(record=_chart_record(rows=[]))
    assert rc.find(ipecharts.EChartsRawWidget).widgets == []


def test_evaluation_table_dialog_mounts_with_its_charts():
    """Mount smoke for the whole dialog — nothing else renders it.

    Catches a prop mismatch between the dialog and _ChartsTab (the tab index is
    threaded through now), which no source-substring test can see. `eager=True`
    on the dialog means the tab bodies are built even though nothing is
    visible headlessly.
    """
    import ipecharts
    import reacton
    import solara

    from gui.i18n import t as _t

    _t("common.close")  # warm the translator before the first render
    from gui.widget.evaluation_results import EvaluationTableDialog

    p = types.SimpleNamespace(evaluations={"run-a": _chart_record()})
    project = solara.reactive(p, equals=lambda a, b: a is b)
    _, rc = reacton.render(
        EvaluationTableDialog(
            project=project, eval_key="run-a", on_close=lambda *_: None),
        handle_error=False,
    )
    assert len(rc.find(ipecharts.EChartsRawWidget).widgets) == 2
    rc.close()
