"""Quantitative model evaluation (udef-arp accuracy indices).

Promoted verbatim from notebooks/6.models_evaluation.ipynb so the GUI and the
notebook share one implementation. Native two-explicit-layer port of
forestatrisk.validation_udef_arp — no forestatrisk dependency.
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd
from osgeo import gdal

FAMILY = {"glm": "GLM", "rf": "RF", "icar": "ICAR", "mw": "MW", "jnr": "JNR"}
FOREST_VAR = "forest_gfc"  # dataset feature used as 'forest at period start'


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
    """Two-explicit-layer port of forestatrisk.validation_udef_arp.

    defor_file   : binary deforestation in the period (1 = deforested)
    forest_file  : binary forest at the START of the period (1 = forest)
    riskmap_file : UInt16 categorical risk (categories 1..65535, nodata 0)
    tab_file_defor: per-category defrate CSV (cols 'cat', 'defor_dens')
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

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
    df.to_csv(tab_file_pred, index=False)

    error_pred = df["ndefor_pred_ha"] - df["ndefor_obs_ha"]
    squared_error = error_pred ** 2
    RMSE = round(float(np.sqrt(np.mean(squared_error))), 2)
    w = df["nfor_obs_ha"] / df["nfor_obs_ha"].sum()
    wRMSE = round(float(np.sqrt(np.sum(squared_error * w))), 2)
    MedAE = round(float(np.median(np.absolute(error_pred))), 2)
    r = np.corrcoef(df["ndefor_pred_ha"], df["ndefor_obs_ha"])[0, 1]
    r_square = round(float(r ** 2), 2)

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
    fig.savefig(fig_file_pred)
    plt.close(fig)

    indices = {
        "RMSE": RMSE, "wRMSE": wRMSE, "MedAE": MedAE, "R2": r_square,
        "ncell": ncell, "csize_coarse_grid": csize_coarse_grid,
        "csize_coarse_grid_ha": csize_ha,
    }
    pd.DataFrame([indices]).to_csv(indices_file_pred, index=False)
    return indices
