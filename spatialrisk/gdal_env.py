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


def _is_usable(d: Path) -> bool:
    """Create ``d`` and prove we can write a file in it.

    A real write probe rather than ``os.access()``: the whole bug this module
    exists for was a directory that looked fine until GDAL created a file in it.
    """
    try:
        d.mkdir(parents=True, exist_ok=True)
        probe = d / f".probe_{os.getpid()}"
        probe.write_text("")
        probe.unlink()
        return True
    except OSError:
        return False


def _candidates() -> list:
    """Scratch directories to try, best first."""
    out = []

    env = os.environ.get("CPL_TMPDIR")
    if env:
        out.append(Path(env))

    # ``tempfile.gettempdir()`` honours TMPDIR/TEMP/TMP and falls back to /tmp,
    # which on SEPAL is writable and wiped when the instance closes -- GDAL
    # unlinks its own scratch file as soon as it is done, so nothing
    # accumulates there. Its candidate list ends with the CWD, though, so a
    # machine where every system temp dir is unusable would hand us back the
    # read-only module mount: skip it in that case rather than reintroduce the
    # bug (or litter the checkout in dev, where the CWD *is* writable).
    system_tmp = Path(tempfile.gettempdir())
    if system_tmp.resolve() != Path.cwd().resolve():
        # The uid suffix keeps us off a directory of the same name created by
        # another user on a shared /tmp: that one would not be writable by us
        # and would reproduce the very bug this module exists to prevent.
        out.append(system_tmp / f"spatial_risk_gdal_{os.getuid()}")

    # Last resort: the module's own output root. SEPAL guarantees it is
    # writable, because every app output goes there. Resolved independently of
    # ``project.DATA_DIR`` to avoid an import cycle -- project.py imports this
    # module -- so keep it in step with ``project._resolve_data_dir``.
    data_dir = os.environ.get("SPATIAL_RISK_DATA_DIR")
    base = (
        Path(data_dir)
        if data_dir
        else Path.home() / "module_results" / "spatial_risk_module"
    )
    out.append(base / ".gdal_tmp")

    return out


def scratch_dir() -> Path:
    """Return the app's writable scratch directory, creating it if needed.

    An explicit ``CPL_TMPDIR`` wins when it is genuinely writable, then the
    system temp dir, then the module's output root. Raises ``OSError`` if none
    of them can hold a file, because every caller's alternative is writing into
    the CWD.
    """
    tried = _candidates()
    for candidate in tried:
        if _is_usable(candidate):
            return candidate
    raise OSError(
        "No writable scratch directory found; tried " + ", ".join(str(c) for c in tried)
    )


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
        logger.warning(
            "Could not configure a GDAL temp directory (%s). GDAL will fall back "
            "to the working directory, so raster steps will fail wherever that is "
            "read-only -- set CPL_TMPDIR to somewhere writable.",
            e,
        )
        return None
