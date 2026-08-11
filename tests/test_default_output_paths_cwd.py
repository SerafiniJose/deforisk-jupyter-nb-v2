"""Public functions must never default an output path to a bare filename.

A default like ``tab_file_defrate="defrate_per_cat.csv"`` resolves against the
*process* CWD. On SEPAL the app runs with its CWD on the read-only shared module
mount, so any direct caller that did not override the default died with::

    [Errno 30] Read-only file system: 'defrate_per_cat.csv'

and on a writable machine it silently littered the CWD instead. The fix is that
every such parameter defaults to ``None``, meaning "do not write this file".

These tests pin the *behaviour*, not the signature: each function is called from
an empty temporary CWD **without** its output-path argument, and that directory
must still be empty afterwards. A future default that is a bare filename again
fails here whether or not it is spelled as a default value.

Sibling of ``test_gdal_readonly_cwd.py``, which covers the other half of the
same SEPAL bug class: GDAL's own scratch files.
"""

import inspect

import numpy as np
import pandas as pd
import pytest

rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin  # noqa: E402

import spatialrisk.rmj.deforrate as deforrate  # noqa: E402
from spatialrisk.evaluation import validate_two_layer  # noqa: E402

PIXEL = 30.0
SIZE = 60


def _write_raster(path, array, pixel=PIXEL):
    """Write a single-band int32 GeoTIFF (EPSG:3857, square metric pixels)."""
    array = np.asarray(array)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=array.shape[0],
        width=array.shape[1],
        count=1,
        dtype="int32",
        crs="EPSG:3857",
        transform=from_origin(0, array.shape[0] * pixel, pixel, pixel),
    ) as dst:
        dst.write(array.astype("int32"), 1)
    return str(path)


@pytest.fixture()
def empty_cwd(tmp_path, monkeypatch):
    """Run the test from a directory that starts — and must stay — empty.

    Deliberately NOT the read-only CWD of ``test_gdal_readonly_cwd.py``: an
    empty writable directory catches a bare-filename default that *succeeds*
    just as well as one that raises, and it reports which file was written.
    """
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    return cwd


@pytest.fixture()
def layers(tmp_path):
    """The rasters + defrate CSV the four functions need, all outside the CWD.

    Kept in their own directory so that GDAL side files (``.aux.xml``) and any
    output written next to an *input* can never be confused with a stray write
    into the CWD under test.
    """
    data = tmp_path / "data"
    data.mkdir()

    # Rows 1..20 deforested: every deforested pixel has a distance > 0, so
    # dist_edge_threshold's ``dist_def > 0`` filter keeps all of them and the
    # cumulative percentage reaches 100 (below 99.5 it raises instead).
    defor = np.zeros((SIZE, SIZE), dtype="int32")
    defor[1:21, :] = 1
    forest = np.ones((SIZE, SIZE), dtype="int32")
    dist = np.tile(np.arange(SIZE, dtype="int32")[:, None] * PIXEL, (1, SIZE))
    risk = np.ones((SIZE, SIZE), dtype="int32")
    risk[:, 30:] = 2
    # Vulnerability codes are ``class * 1000 + subj_id`` (see vulnerability_map).
    vuln = np.ones((SIZE, SIZE), dtype="int32")
    vuln[:, 30:] = 1001

    defrate_csv = data / "defrate.csv"
    pd.DataFrame({"cat": [1, 2], "defor_dens": [0.0004, 0.00025]}).to_csv(
        defrate_csv, index=False
    )

    return {
        "defor_file": _write_raster(data / "defor.tif", defor),
        "forest_file": _write_raster(data / "forest.tif", forest),
        "dist_file": _write_raster(data / "dist.tif", dist),
        "riskmap_file": _write_raster(data / "risk.tif", risk),
        "vulnerability_file": _write_raster(data / "vuln.tif", vuln),
        "tab_file_defor": str(defrate_csv),
    }


def _assert_cwd_untouched(cwd):
    stray = sorted(p.name for p in cwd.iterdir())
    assert stray == [], f"wrote {stray} into the CWD instead of skipping the output"


# --------------------------------------------------------------------------
# End-to-end: real call, real rasters, output-path argument omitted.
# --------------------------------------------------------------------------
def test_dist_edge_threshold_writes_nothing_into_the_cwd(layers, empty_cwd):
    """``tab_file_dist`` omitted -> no perc_dist.csv in the CWD."""
    result = deforrate.dist_edge_threshold(
        defor_file=layers["defor_file"],
        dist_file=layers["dist_file"],
        dist_bins=np.arange(0, 2000, 30),
    )
    # The computation still happens; only the artifact is skipped.
    assert result["dist_thresh"] == 600
    assert result["perc_thresh"] == 100.0
    _assert_cwd_untouched(empty_cwd)


def test_defrate_per_cat_writes_nothing_into_the_cwd(layers, empty_cwd):
    """``tab_file_defrate`` omitted -> no defrate_per_cat.csv in the CWD."""
    df = deforrate.defrate_per_cat(
        defor_file=layers["defor_file"],
        forest_file=layers["forest_file"],
        riskmap_file=layers["riskmap_file"],
        time_interval=5,
    )
    # The table is still returned in full — skipping the file is not skipping
    # the work.
    assert {"cat", "nfor", "ndefor", "rate_obs", "defor_dens"} <= set(df.columns)
    assert df["ndefor"].sum() == 20 * SIZE
    _assert_cwd_untouched(empty_cwd)


def test_defrate_per_class_writes_nothing_into_the_cwd(layers, empty_cwd):
    """``tab_file_defrate`` omitted -> no defrate_per_class.csv in the CWD."""
    df = deforrate.defrate_per_class(
        defor_file=layers["defor_file"],
        forest_file=layers["forest_file"],
        vulnerability_file=layers["vulnerability_file"],
        time_interval=5,
    )
    assert {"cat", "nfor", "ndefor", "rate_obs", "defor_dens"} <= set(df.columns)
    assert df["ndefor"].sum() == 20 * SIZE
    _assert_cwd_untouched(empty_cwd)


def test_validate_two_layer_writes_nothing_into_the_cwd(layers, empty_cwd):
    """All three artifact paths omitted -> no indices/pred_obs files in the CWD.

    ``validate_two_layer`` is the site that needed a real guard: it forwarded
    its three path arguments straight into ``write_pred_obs_csv`` /
    ``save_pred_obs_png`` / ``write_indices_csv``, so ``None`` would have made
    ``fig.savefig(None)`` raise rather than mean "skip".
    """
    indices = validate_two_layer(
        defor_file=layers["defor_file"],
        forest_file=layers["forest_file"],
        riskmap_file=layers["riskmap_file"],
        tab_file_defor=layers["tab_file_defor"],
        time_interval=5,
        csize_coarse_grid=30,
    )
    # The indices dict is the return value, and it is unaffected by the skip.
    assert set(indices) == {
        "RMSE",
        "wRMSE",
        "MedAE",
        "R2",
        "ncell",
        "csize_coarse_grid",
        "csize_coarse_grid_ha",
    }
    assert indices["ncell"] == 4
    _assert_cwd_untouched(empty_cwd)


def test_validate_two_layer_guards_each_artifact_independently(
    layers, empty_cwd, tmp_path
):
    """One path given, two omitted: exactly one artifact appears, CWD stays empty.

    Pins that the three guards are separate ``if`` statements rather than one
    all-or-nothing branch, so a caller can ask for the points CSV without also
    paying for the matplotlib render.
    """
    out = tmp_path / "out"
    out.mkdir()
    points_csv = out / "pred_obs.csv"

    validate_two_layer(
        defor_file=layers["defor_file"],
        forest_file=layers["forest_file"],
        riskmap_file=layers["riskmap_file"],
        tab_file_defor=layers["tab_file_defor"],
        time_interval=5,
        csize_coarse_grid=30,
        tab_file_pred=points_csv,
    )

    assert [p.name for p in out.iterdir()] == ["pred_obs.csv"]
    assert points_csv.read_text().startswith("cell,nfor_obs,ndefor_obs,")
    _assert_cwd_untouched(empty_cwd)


# --------------------------------------------------------------------------
# Signature guard: names every one of the six parameters, so a new bare-filename
# default fails with the parameter's own name rather than a generic stray file.
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("func", "param"),
    [
        (deforrate.dist_edge_threshold, "tab_file_dist"),
        (deforrate.dist_edge_threshold, "fig_file_dist"),
        (deforrate.defrate_per_cat, "tab_file_defrate"),
        (deforrate.defrate_per_class, "tab_file_defrate"),
        (validate_two_layer, "indices_file_pred"),
        (validate_two_layer, "tab_file_pred"),
        (validate_two_layer, "fig_file_pred"),
    ],
)
def test_output_path_parameters_default_to_none(func, param):
    """No output-path parameter may default to a CWD-relative filename."""
    default = inspect.signature(func).parameters[param].default
    assert default is None, (
        f"{func.__name__}({param}=...) defaults to {default!r}; a relative "
        f"default lands in the process CWD, which is read-only on SEPAL"
    )
