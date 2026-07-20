"""Quantitative model evaluation (udef-arp accuracy indices).

Promoted verbatim from notebooks/6.models_evaluation.ipynb so the GUI and the
notebook share one implementation. Native two-explicit-layer port of
forestatrisk.validation_udef_arp — no forestatrisk dependency.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from osgeo import gdal

FAMILY = {"glm": "GLM", "rf": "RF", "icar": "ICAR", "mw": "MW", "jnr": "JNR"}
FOREST_VAR = "forest_gfc"  # dataset feature used as 'forest at period start'

# Axis titles of the predicted-vs-observed scatter. Deliberately NOT i18n'd:
# spatialrisk/ must never import gui/, and these strings are baked into the
# archived PNG. A localized chart supplies its own labels at render time.
PRED_OBS_X_LABEL = "Observed deforestation (ha)"
PRED_OBS_Y_LABEL = "Predicted deforestation (ha)"

_OBS_COL = "ndefor_obs_ha"
_PRED_COL = "ndefor_pred_ha"

# Axis domain used when the data carries no finite value at all (empty result,
# all-NaN or all-infinite series). A unit range keeps both renderers valid.
_FALLBACK_AXIS = (0.0, 1.0)


def interval_from_target(name):
    """'forest_loss_2015_2020' -> 5; None if fewer than two 4-digit years."""
    yrs = [int(y) for y in re.findall(r"\d{4}", name or "")]
    return (yrs[1] - yrs[0]) if len(yrs) >= 2 else None


def label_for(pred):
    """Short display label for a prediction (e.g. 'GLM', 'MW_w11')."""
    fam = FAMILY.get(pred.model_key.split("_")[0], pred.model_key)
    return f"{fam}_w{pred.window}" if pred.window is not None else fam


def make_square(raster_file, square_size):
    """Coarse-grid partition (replicates forestatrisk.make_square, no far dep)."""
    ds = gdal.Open(str(raster_file))
    ncol, nrow = ds.RasterXSize, ds.RasterYSize
    del ds
    nsquare_x = int(np.ceil(ncol / square_size))
    nsquare_y = int(np.ceil(nrow / square_size))
    nsquare = nsquare_x * nsquare_y
    x = list(range(0, ncol, square_size))
    y = list(range(0, nrow, square_size))
    nx = [square_size] * nsquare_x
    ny = [square_size] * nsquare_y
    if ncol % square_size > 0:
        nx[-1] = ncol % square_size
    if nrow % square_size > 0:
        ny[-1] = nrow % square_size
    return nsquare, nsquare_x, nsquare_y, x, y, nx, ny


def _finite_mask(points):
    """Boolean mask of rows whose observed AND predicted values are finite."""
    obs = pd.to_numeric(points[_OBS_COL], errors="coerce").to_numpy(dtype="float64")
    pred = pd.to_numeric(points[_PRED_COL], errors="coerce").to_numpy(dtype="float64")
    return np.isfinite(obs) & np.isfinite(pred)


def pred_obs_axis_bounds(points):
    """One common finite axis domain spanning BOTH observed and predicted values.

    Matches the legacy ``p = [min over both columns, max over both columns]``
    whenever the data is well-behaved, but never returns NaN/inf or a zero-width
    domain (which collapses an ECharts axis and breaks a 1:1 reference line):

    * no finite value at all -> ``(0.0, 1.0)``
    * constant series at 0   -> ``(0.0, 1.0)``
    * constant series at v   -> ``(v - |v|, v + |v|)``, i.e. ``(0, 2v)`` for v > 0
    """
    values = np.concatenate([
        pd.to_numeric(points[c], errors="coerce").to_numpy(dtype="float64")
        for c in (_OBS_COL, _PRED_COL)
    ])
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return _FALLBACK_AXIS

    lo, hi = float(finite.min()), float(finite.max())
    if lo == hi:
        pad = abs(lo)
        if pad == 0.0:
            return _FALLBACK_AXIS
        return (lo - pad, hi + pad)
    return (lo, hi)


@dataclass(frozen=True, eq=False)
class PredObsPlotData:
    """Everything a predicted-vs-observed chart needs, with no raster access.

    Shared by the archived matplotlib PNG and the interactive ECharts scatter so
    the two can never disagree. ``eq=False`` on purpose: the default dataclass
    ``__eq__`` would compare ``points`` element-wise and raise "truth value of a
    DataFrame is ambiguous" during reacton's prop diffing. Identity equality also
    makes every freshly computed result re-render, which is the safe default.

    ``points`` is the exact frame persisted to the point CSV, non-finite rows
    included; renderers must use ``finite_points`` instead.
    """

    model: str
    period: str
    csize_px: int
    csize_ha: float
    points: pd.DataFrame
    axis_min: float
    axis_max: float
    medae: float
    r2: float
    ncell: int

    @property
    def title(self):
        return (
            f"{self.model} model, {self.period} period\n"
            f"Predicted vs. observed deforestation in {self.csize_ha} ha grid cells."
        )

    @property
    def annotation(self):
        """The 'MedAE / R2 / n' summary block, formatted as on the PNG."""
        return (f"MedAE = {self.medae:.2f} ha\n"
                f"R2 = {self.r2:.2f}\n"
                f"n = {self.ncell:d}")

    @property
    def x_label(self):
        return PRED_OBS_X_LABEL

    @property
    def y_label(self):
        return PRED_OBS_Y_LABEL

    @property
    def finite_points(self):
        """Plottable subset: rows where both values are finite (never NaN/inf)."""
        return self.points[_finite_mask(self.points)]


@dataclass(frozen=True, eq=False)
class ValidationResult:
    """Source of truth for one validation run: metrics + shared chart input."""

    indices: dict[str, Any]
    plot_data: PredObsPlotData


def compute_validation(
    defor_file,
    forest_file,
    riskmap_file,
    tab_file_defor,
    time_interval,
    csize_coarse_grid=300,
    model_name="model",
    period="calibration",
):
    """Per-cell tally + accuracy indices. Pure computation — writes no files.

    Numerics are frozen: same formulas, same ``round(..., 2)``, same dropped
    cells (``nfor_obs == 0``) and same column order as before the split.

    defor_file   : binary deforestation in the period (1 = deforested)
    forest_file  : binary forest at the START of the period (1 = forest)
    riskmap_file : UInt16 categorical risk (categories 1..65535, nodata 0)
    tab_file_defor: per-category defrate CSV (cols 'cat', 'defor_dens')
    """
    defor_ds = gdal.Open(str(defor_file))
    defor_band = defor_ds.GetRasterBand(1)
    forest_ds = gdal.Open(str(forest_file))
    forest_band = forest_ds.GetRasterBand(1)
    risk_ds = gdal.Open(str(riskmap_file))
    risk_band = risk_ds.GetRasterBand(1)

    defor_dens_per_cat = pd.read_csv(tab_file_defor)
    cat = defor_dens_per_cat["cat"].values
    defor_dens_period = defor_dens_per_cat["defor_dens"].values * time_interval

    gt = defor_ds.GetGeoTransform()
    pix_area = gt[1] * (-gt[5])
    csize_ha = round(csize_coarse_grid * csize_coarse_grid * pix_area / 10000, 2)

    nsquare, nsquare_x, _, x, y, nx, ny = make_square(defor_file, csize_coarse_grid)

    df = pd.DataFrame({
        "cell": list(range(nsquare)),
        "nfor_obs": 0, "ndefor_obs": 0,
        "nfor_obs_ha": 0.0, "ndefor_obs_ha": 0.0, "ndefor_pred_ha": 0.0,
    })

    for s in range(nsquare):
        px, py = s % nsquare_x, s // nsquare_x
        defor_data = defor_band.ReadAsArray(x[px], y[py], nx[px], ny[py])
        forest_data = forest_band.ReadAsArray(x[px], y[py], nx[px], ny[py])
        defor_mask = defor_data == 1
        forest_start = (forest_data == 1) | defor_mask
        df.loc[s, "nfor_obs"] = int(forest_start.sum())
        df.loc[s, "ndefor_obs"] = int(defor_mask.sum())

        risk_data = risk_band.ReadAsArray(x[px], y[py], nx[px], ny[py])
        risk_cat = pd.Categorical(risk_data.flatten(), categories=cat)
        risk_count = risk_cat.value_counts().values
        df.loc[s, "ndefor_pred_ha"] = np.nansum(risk_count * defor_dens_period)

    del defor_ds, forest_ds, risk_ds

    df = df[df["nfor_obs"] > 0]
    ncell = df.shape[0]
    df["nfor_obs_ha"] = df["nfor_obs"] * pix_area / 10000
    df["ndefor_obs_ha"] = df["ndefor_obs"] * pix_area / 10000

    error_pred = df["ndefor_pred_ha"] - df["ndefor_obs_ha"]
    squared_error = error_pred ** 2
    RMSE = round(float(np.sqrt(np.mean(squared_error))), 2)
    w = df["nfor_obs_ha"] / df["nfor_obs_ha"].sum()
    wRMSE = round(float(np.sqrt(np.sum(squared_error * w))), 2)
    MedAE = round(float(np.median(np.absolute(error_pred))), 2)
    r = np.corrcoef(df["ndefor_pred_ha"], df["ndefor_obs_ha"])[0, 1]
    r_square = round(float(r ** 2), 2)

    indices = {
        "RMSE": RMSE, "wRMSE": wRMSE, "MedAE": MedAE, "R2": r_square,
        "ncell": ncell, "csize_coarse_grid": csize_coarse_grid,
        "csize_coarse_grid_ha": csize_ha,
    }
    axis_min, axis_max = pred_obs_axis_bounds(df)
    plot_data = PredObsPlotData(
        model=model_name, period=period,
        csize_px=csize_coarse_grid, csize_ha=csize_ha,
        points=df, axis_min=axis_min, axis_max=axis_max,
        medae=MedAE, r2=r_square, ncell=ncell,
    )
    return ValidationResult(indices=indices, plot_data=plot_data)


def write_pred_obs_csv(plot_data, output_path):
    """Persist the per-cell point table (the frozen 6-column CSV)."""
    plot_data.points.to_csv(output_path, index=False)
    return output_path


def write_indices_csv(indices, output_path):
    """Persist the one-row accuracy-indices table."""
    pd.DataFrame([indices]).to_csv(output_path, index=False)
    return output_path


def save_pred_obs_png(plot_data, output_path, *, figsize=(6.4, 6.4), dpi=100):
    """Render the archived predicted-vs-observed scatter to a PNG.

    Byte-identical to the pre-split inline matplotlib block for well-behaved
    data. Non-finite rows are dropped and the reference line uses the guaranteed
    finite ``axis_min``/``axis_max``, so degenerate input renders instead of
    producing a blank or NaN-scaled figure.
    """
    import matplotlib
    matplotlib.use("Agg")   # worker-safe: must precede the pyplot import
    import matplotlib.pyplot as plt

    points = plot_data.finite_points
    p = [plot_data.axis_min, plot_data.axis_max]

    fig = plt.figure(figsize=figsize, dpi=dpi)
    ax = plt.subplot(111)
    ax.set_box_aspect(1)
    plt.scatter(points[_OBS_COL], points[_PRED_COL],
                color=None, marker="o", edgecolor="k")
    plt.plot(p, p, "r--")
    plt.title(plot_data.title)
    plt.xlabel(plot_data.x_label)
    plt.ylabel(plot_data.y_label)
    plt.text(0, plot_data.axis_max, plot_data.annotation, ha="left", va="top")
    fig.savefig(output_path)
    plt.close(fig)
    return output_path


def validate_two_layer(
    defor_file,
    forest_file,
    riskmap_file,
    tab_file_defor,
    time_interval,
    csize_coarse_grid=300,
    indices_file_pred="indices.csv",
    tab_file_pred="pred_obs.csv",
    fig_file_pred="pred_obs.png",
    model_name="model",
    period="calibration",
    figsize=(6.4, 6.4),
    dpi=100,
):
    """Compute + persist one validation run (compatibility wrapper).

    Two-explicit-layer port of forestatrisk.validation_udef_arp. Kept with its
    original signature and return value (the indices dict) so existing callers
    and the notebook keep working; the computation now lives in
    ``compute_validation`` and the artifacts in ``write_pred_obs_csv`` /
    ``write_indices_csv`` / ``save_pred_obs_png``. Prefer those directly when
    you also need the chart input.
    """
    result = compute_validation(
        defor_file=defor_file,
        forest_file=forest_file,
        riskmap_file=riskmap_file,
        tab_file_defor=tab_file_defor,
        time_interval=time_interval,
        csize_coarse_grid=csize_coarse_grid,
        model_name=model_name,
        period=period,
    )
    write_pred_obs_csv(result.plot_data, tab_file_pred)
    save_pred_obs_png(result.plot_data, fig_file_pred, figsize=figsize, dpi=dpi)
    write_indices_csv(result.indices, indices_file_pred)
    return result.indices


def _defrate_per_cat(**kwargs):
    """Indirection over rmj.deforrate.defrate_per_cat (monkeypatchable seam)."""
    from spatialrisk import rmj
    return rmj.deforrate.defrate_per_cat(**kwargs)


def resolve_layers(project, pred):
    """Recover the two binary layers + time interval from the prediction's dataset."""
    ds = project.get_dataset(pred.dataset_name)
    if ds is None:
        raise ValueError(
            f"Dataset '{pred.dataset_name}' not found in project."
        )
    forest = next((f for f in ds.features if f.name == FOREST_VAR), None)
    if forest is None:
        raise ValueError(
            f"Feature '{FOREST_VAR}' not in dataset '{ds.name}'. "
            f"Available: {[f.name for f in ds.features]}"
        )
    return {
        "defor_file": ds.target.path,
        "forest_file": forest.path,
        "riskmap_file": pred.path,
        "time_interval": interval_from_target(ds.target.name),
        "period": pred.dataset_name,
    }


def evaluate_prediction(project, pred, csizes=(300,), recompute_defrate=True):
    """Defrate + validate one prediction across coarse-grid sizes.

    Returns a list of index dicts, each annotated with prediction/model/period/fig_path.
    Also writes results into ``pred.metrics``.
    """
    lay = resolve_layers(project, pred)
    label, period, ti = label_for(pred), lay["period"], lay["time_interval"]
    evaluation_folder = Path(project.folders.project_folder) / "evaluation"
    period_dir = evaluation_folder / period
    period_dir.mkdir(parents=True, exist_ok=True)

    defrate_csv = period_dir / f"defrate_cat_{label}_{period}.csv"
    if recompute_defrate or not defrate_csv.exists():
        _defrate_per_cat(
            defor_file=lay["defor_file"],
            forest_file=lay["forest_file"],
            riskmap_file=lay["riskmap_file"],
            time_interval=ti,
            tab_file_defrate=defrate_csv,
            verbose=False,
        )

    rows = []
    for csize in csizes:
        fig_path = period_dir / f"pred_obs_{label}_{period}_{csize}.png"
        idx = validate_two_layer(
            defor_file=lay["defor_file"],
            forest_file=lay["forest_file"],
            riskmap_file=lay["riskmap_file"],
            tab_file_defor=defrate_csv,
            time_interval=ti,
            csize_coarse_grid=csize,
            indices_file_pred=period_dir / f"indices_{label}_{period}_{csize}.csv",
            tab_file_pred=period_dir / f"pred_obs_{label}_{period}_{csize}.csv",
            fig_file_pred=fig_path,
            model_name=label,
            period=period,
        )
        idx.update({"prediction": pred.storage_key() if hasattr(pred, "storage_key")
                    else f"{pred.model_key}__{period}",
                    "model": label, "period": period, "fig_path": str(fig_path)})
        pred.metrics[f"{period}_{csize}"] = {k: idx[k] for k in
                                             ("RMSE", "wRMSE", "MedAE", "R2", "ncell")}
        rows.append(idx)
    return rows


def _evaluate_one_against_truth(project, pred, *, defor_file, forest_file,
                                time_interval, truth_tag, csizes=(300,),
                                recompute_defrate=True):
    """Defrate + validate ONE prediction against an explicit shared truth.

    Mirrors evaluate_prediction but takes the truth (defor + forest + interval)
    explicitly instead of deriving it from the prediction's own dataset, and
    namespaces all output under evaluation/<truth_tag>/.
    """
    label, period = label_for(pred), pred.dataset_name
    riskmap_file = pred.path
    truth_dir = Path(project.folders.project_folder) / "evaluation" / truth_tag
    truth_dir.mkdir(parents=True, exist_ok=True)

    defrate_csv = truth_dir / f"defrate_cat_{label}_{period}.csv"
    if recompute_defrate or not defrate_csv.exists():
        _defrate_per_cat(
            defor_file=defor_file,
            forest_file=forest_file,
            riskmap_file=riskmap_file,
            time_interval=time_interval,
            tab_file_defrate=defrate_csv,
            verbose=False,
        )

    rows = []
    for csize in csizes:
        fig_path = truth_dir / f"pred_obs_{label}_{period}_{csize}.png"
        idx = validate_two_layer(
            defor_file=defor_file,
            forest_file=forest_file,
            riskmap_file=riskmap_file,
            tab_file_defor=defrate_csv,
            time_interval=time_interval,
            csize_coarse_grid=csize,
            indices_file_pred=truth_dir / f"indices_{label}_{period}_{csize}.csv",
            tab_file_pred=truth_dir / f"pred_obs_{label}_{period}_{csize}.csv",
            fig_file_pred=fig_path,
            model_name=label,
            period=period,
        )
        idx.update({"prediction": pred.storage_key() if hasattr(pred, "storage_key")
                    else f"{pred.model_key}__{period}",
                    "model": label, "period": period, "truth": truth_tag,
                    "fig_path": str(fig_path)})
        pred.metrics[f"{truth_tag}__{period}_{csize}"] = {k: idx[k] for k in
                                                          ("RMSE", "wRMSE", "MedAE", "R2", "ncell")}
        rows.append(idx)
    return rows


def evaluate_predictions(project, dataset_filter=None, model_filter=None,
                         windows=None, csizes=(300,), recompute_defrate=True,
                         auto_save=True):
    """Select predictions from the project, evaluate each, return aggregated indices.

    Skips (with a printed warning) any prediction whose layers cannot be resolved,
    rather than aborting the whole batch. Writes <project>/evaluation/indices_all.csv.
    """
    selected = {}
    for key, pred in project.predictions.items():
        if dataset_filter and pred.dataset_name not in dataset_filter:
            continue
        if model_filter and pred.model_key not in model_filter:
            continue
        if windows is not None and pred.window is not None and pred.window not in windows:
            continue
        selected[key] = pred

    rows = []
    for key, pred in selected.items():
        try:
            rows.extend(evaluate_prediction(project, pred, csizes=csizes,
                                            recompute_defrate=recompute_defrate))
        except Exception as exc:  # noqa: BLE001 - skip-and-warn is intentional
            print(f"⚠ skipped {key}: {exc}")

    cols = ["prediction", "model", "period", "csize_coarse_grid",
            "csize_coarse_grid_ha", "ncell", "MedAE", "R2", "RMSE", "wRMSE"]
    df = (pd.DataFrame(rows, columns=cols).sort_values(
        ["csize_coarse_grid", "period", "model"]).reset_index(drop=True)
        if rows else pd.DataFrame(columns=cols))

    evaluation_folder = Path(project.folders.project_folder) / "evaluation"
    evaluation_folder.mkdir(parents=True, exist_ok=True)
    df.to_csv(evaluation_folder / "indices_all.csv", index=False)

    if auto_save and rows:
        try:
            project.save()
        except Exception as exc:  # noqa: BLE001
            print(f"⚠ project.save() after evaluation failed: {exc}")
    return df


def evaluate_against_truth(project, prediction_keys=None, *, defor_file,
                           forest_file, time_interval, truth_tag,
                           csizes=(300,), recompute_defrate=True, auto_save=True):
    """Score selected maps against ONE common truth.

    Unlike evaluate_predictions (which derives each map's truth from its own
    dataset), this applies a single user-chosen truth (defor + forest + interval)
    to every selected map, enabling comparison of maps from different datasets.

    prediction_keys : list[str] or None
        Registry keys of the maps to score. None = all registered predictions.
        Unknown keys are skipped with a printed warning.
    """
    if prediction_keys is None:
        selected = dict(project.predictions)
    else:
        selected = {}
        for key in prediction_keys:
            pred = project.predictions.get(key)
            if pred is None:
                print(f"⚠ skipped {key}: not registered")
                continue
            selected[key] = pred

    rows = []
    for key, pred in selected.items():
        try:
            rows.extend(_evaluate_one_against_truth(
                project, pred, defor_file=defor_file, forest_file=forest_file,
                time_interval=time_interval, truth_tag=truth_tag,
                csizes=csizes, recompute_defrate=recompute_defrate))
        except Exception as exc:  # noqa: BLE001 - skip-and-warn is intentional
            print(f"⚠ skipped {key}: {exc}")

    cols = ["prediction", "model", "period", "truth", "csize_coarse_grid",
            "csize_coarse_grid_ha", "ncell", "MedAE", "R2", "RMSE", "wRMSE"]
    df = (pd.DataFrame(rows, columns=cols).sort_values(
        ["csize_coarse_grid", "period", "model"]).reset_index(drop=True)
        if rows else pd.DataFrame(columns=cols))

    truth_dir = Path(project.folders.project_folder) / "evaluation" / truth_tag
    truth_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(truth_dir / "indices_all.csv", index=False)

    if auto_save and rows:
        try:
            project.save()
        except Exception as exc:  # noqa: BLE001
            print(f"⚠ project.save() after evaluation failed: {exc}")
    return df
