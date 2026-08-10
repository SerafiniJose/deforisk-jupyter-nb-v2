"""Point GDAL's temp-file allocator at a directory we know is writable.

``gdal.ComputeProximity()`` computes in Float32. When the destination band is a
different type (ours is ``GDT_UInt32``) GDAL allocates a full-size Float32
working band in a temporary GeoTIFF named by ``CPLGenerateTempFilename()``,
which resolves its directory from the ``CPL_TMPDIR`` / ``TMPDIR`` / ``TEMP``
config options and falls back to ``"."`` -- the process CWD -- when none is
set. On SEPAL the app runs with its CWD on the read-only shared module mount,
so the call dies with::

    Attempt to create new tiff file `./proximity_997_3' failed:
    ./proximity_997_3: Read-only file system

The same defect lives in third-party code we cannot patch (notably
``riskmapjnr.dist_edge_threshold``), so the fix has to be a process-wide GDAL
config rather than a change to any one destination dtype.
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

from osgeo import gdal

logger = logging.getLogger("spatial_risk")


def scratch_dir() -> Path:
    """Return the app's writable scratch directory, creating it if needed.

    An explicit ``CPL_TMPDIR`` wins when it is actually writable; otherwise we
    use ``<system temp>/spatial_risk_gdal_<uid>``. ``tempfile.gettempdir()``
    honours ``TMPDIR``/``TEMP``/``TMP`` and falls back to ``/tmp``, which on
    SEPAL is writable and wiped when the instance closes -- GDAL unlinks its
    own scratch file as soon as it is done, so nothing accumulates there.
    """
    env = os.environ.get("CPL_TMPDIR")
    if env and os.access(env, os.W_OK):
        return Path(env)

    # The uid suffix keeps us off a directory of the same name created by
    # another user on a shared /tmp: that one would not be writable by us and
    # would reproduce the very bug this module exists to prevent.
    d = Path(tempfile.gettempdir()) / f"spatial_risk_gdal_{os.getuid()}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def configure_gdal_tmpdir() -> Optional[Path]:
    """Point GDAL's temp-file allocator at :func:`scratch_dir`. Idempotent.

    Sets both the environment variable -- inherited by spawned children, such
    as the iCAR MCMC worker started through ``multiprocessing`` in "spawn"
    mode -- and the GDAL config option, which covers the library already
    initialised in this process.

    Returns the configured directory, or ``None`` if configuration failed.
    Never raises: this runs at import time, so an exception here would break
    every import of the package.
    """
    try:
        d = scratch_dir()
        os.environ["CPL_TMPDIR"] = str(d)
        gdal.SetConfigOption("CPL_TMPDIR", str(d))
        return d
    except Exception as e:  # pragma: no cover - defensive, import-time safety
        logger.warning("Could not configure a GDAL temp directory: %s", e)
        return None
