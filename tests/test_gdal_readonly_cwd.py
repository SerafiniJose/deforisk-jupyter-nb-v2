"""GDAL must never fall back to the process CWD for its scratch files.

``gdal.ComputeProximity()`` computes in Float32, so when the destination band is
some other type -- ``distance_to_edge_gdal_no_mask`` writes ``GDT_UInt32`` -- GDAL
allocates a full-size Float32 working GeoTIFF whose path comes from
``CPLGenerateTempFilename("proximity")``. That resolves ``CPL_TMPDIR`` ->
``TMPDIR`` -> ``TEMP`` and otherwise falls back to ``"."``. On SEPAL the app's CWD
is the read-only shared module mount, so post-processing died with::

    Attempt to create new tiff file `./proximity_997_3' failed:
    ./proximity_997_3: Read-only file system

``spatialrisk/gdal_env.py`` points GDAL at a scratch directory we know is
writable. These tests reproduce the SEPAL conditions -- unwritable CWD, no
temp-dir hint anywhere -- and pin the output contract, because the fix must not
be achieved by quietly writing a different dtype or nodata than
``gui/scripts/postprocess_styles.py`` styles the layer around.
"""

import os
import tempfile
from pathlib import Path

import numpy as np
import pytest
import rasterio
from osgeo import gdal
from rasterio.transform import from_origin

from spatialrisk.gdal_env import configure_gdal_tmpdir, scratch_dir
from spatialrisk.processing import distance_to_edge_gdal_no_mask

# UInt32 max: what distance_to_edge_gdal_no_mask declares as the output nodata.
NODATA = 4294967295

# The temp-dir hints CPLGenerateTempFilename() consults, in order, before it
# falls back to "." -- the fallback that breaks on SEPAL.
GDAL_TMP_VARS = ("CPL_TMPDIR", "TMPDIR", "TEMP")


@pytest.fixture()
def unconfigured_gdal_tmpdir(monkeypatch):
    """Strip every temp-dir hint GDAL has, through both of its channels.

    Clearing the environment alone is not enough to recreate the SEPAL
    conditions: importing ``spatialrisk`` already called
    ``configure_gdal_tmpdir()``, and ``gdal.SetConfigOption()`` values outlive
    ``os.environ`` edits. Without wiping the config option too, the regression
    test would pass on the import-time setup alone and never exercise the call
    inside ``distance_to_edge_gdal_no_mask``. pytest does not know about GDAL's
    globals, so the previous value is put back by hand.
    """
    for var in GDAL_TMP_VARS:
        monkeypatch.delenv(var, raising=False)
    previous = gdal.GetConfigOption("CPL_TMPDIR")
    gdal.SetConfigOption("CPL_TMPDIR", None)
    yield
    gdal.SetConfigOption("CPL_TMPDIR", previous)


@pytest.fixture()
def readonly_cwd(tmp_path, monkeypatch):
    """Run from a directory nothing can be written to, like SEPAL's module mount."""
    cwd = tmp_path / "readonly_module_mount"
    cwd.mkdir()
    cwd.chmod(0o555)
    monkeypatch.chdir(cwd)
    yield cwd
    # pytest restores the CWD but not the mode, and an unwritable directory
    # survives tmp_path cleanup to poison later runs.
    cwd.chmod(0o755)


def _write_forest_mask(path, features):
    """A 128x128 forest mask (1 = forest) with ``features`` (row, col) set to 0.

    EPSG:32631 at 30 m: the metric CRS makes the DISTUNITS=GEO distances metres.
    """
    data = np.ones((128, 128), dtype="uint8")
    for row, col in features:
        data[row, col] = 0
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs="EPSG:32631",
        transform=from_origin(500000.0, 5000000.0, 30.0, 30.0),
        nodata=255,
    ) as dst:
        dst.write(data, 1)


@pytest.mark.skipif(
    os.geteuid() == 0,
    reason="root ignores permission bits, so a read-only CWD proves nothing",
)
def test_distance_to_edge_runs_with_a_readonly_cwd(
    tmp_path, readonly_cwd, unconfigured_gdal_tmpdir
):
    """The SEPAL regression: unwritable CWD, no temp-dir hint, must still work."""
    src = tmp_path / "forest.tif"
    dst = tmp_path / "forest_edge.tif"
    features = [(40, 40), (100, 90)]
    _write_forest_mask(src, features)

    distance_to_edge_gdal_no_mask(
        input_file=str(src),
        dist_file=str(dst),
        values=0,
        nodata=0,
        max_distance_value=NODATA,
        input_nodata=True,
        verbose=False,
    )

    with rasterio.open(dst) as out:
        data = out.read(1)
        # The contract postprocess_styles.py and the map legends read.
        assert out.dtypes[0] == "uint32"
        assert out.nodata == NODATA

    # A failed ComputeProximity leaves the destination band unwritten, which
    # GTiff reads back as a solid nodata fill -- so "the file exists" proves
    # nothing. Every pixel but the two features must carry a real distance.
    assert ((data > 0) & (data < NODATA)).sum() == data.size - len(features)
    for row, col in features:
        assert data[row, col] == 0


def test_scratch_dir_honours_an_explicit_writable_cpl_tmpdir(tmp_path, monkeypatch):
    """An operator-set CPL_TMPDIR is returned unchanged when it is writable."""
    explicit = tmp_path / "gdal_scratch"
    explicit.mkdir()
    monkeypatch.setenv("CPL_TMPDIR", str(explicit))
    assert scratch_dir() == explicit


def test_scratch_dir_falls_back_under_the_system_tempdir(unconfigured_gdal_tmpdir):
    """With nothing configured, fall back to a writable dir under the system temp."""
    fallback = scratch_dir()
    assert fallback.parent == Path(tempfile.gettempdir())
    assert fallback.is_dir()
    # A real write, not os.access(): the whole bug was a directory that looked
    # fine until GDAL tried to create a file in it.
    probe = fallback / "write_probe.tif"
    probe.write_text("ok")
    probe.unlink()


def test_configure_gdal_tmpdir_sets_env_and_gdal_config(unconfigured_gdal_tmpdir):
    """Both channels: the env var for child processes, the config for this one."""
    configured = configure_gdal_tmpdir()
    assert configured is not None
    assert os.environ["CPL_TMPDIR"] == str(configured)
    assert gdal.GetConfigOption("CPL_TMPDIR") == str(configured)
