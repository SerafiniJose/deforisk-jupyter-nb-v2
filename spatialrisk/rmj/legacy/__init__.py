"""LEGACY single-layer / riskmapjnr-delegating routines — SUPERSEDED.

⚠️  These functions are **NOT used by any model**. They are kept only for
reference and for reproducing pre-migration results. The current, supported
implementation is the native two-explicit-layer module
``spatialrisk.rmj.deforrate`` (verified numerically identical on equivalent
inputs). Do not call these from new code.

Why they were superseded
-------------------------
* ``dist_edge_threshold`` / ``local_defor_rate`` here are thin wrappers that
  feed a binary raster to ``riskmapjnr`` as an ``fcc_file`` (``check_fcc=False``).
  ``riskmapjnr`` then derives the forest denominator as ``(in_data > 0)``, which
  for a genuinely binary deforestation raster collapses to the deforested pixels
  themselves — i.e. it only works if the input secretly carries forest as a
  nonzero value. ``deforrate`` removes this ambiguity with an explicit
  ``forest_file``.
* ``defrate_per_cat`` / ``defrate_per_class`` here are rasterio re-implementations
  whose forest denominator is ``forest == forest_value`` (no union with the
  deforested pixels) and which dropped the ``rate_mod``/``rate_abs`` columns for
  ``defrate_per_cat``. ``deforrate`` restores those and uses the period-robust
  ``(forest == 1) | (defor == 1)`` denominator.

Current equivalent
-------------------
    legacy.dist_edge_threshold   ->  rmj.deforrate.dist_edge_threshold
    legacy.local_defor_rate      ->  rmj.deforrate.local_defor_rate
    legacy.defrate_per_cat       ->  rmj.deforrate.defrate_per_cat
    legacy.defrate_per_class     ->  rmj.deforrate.defrate_per_class
"""

from spatialrisk.rmj.legacy.dist_edge_threshold import dist_edge_threshold
from spatialrisk.rmj.legacy.local_defor_rate import local_defor_rate
from spatialrisk.rmj.legacy.defrate_per_cat import defrate_per_cat
from spatialrisk.rmj.legacy.defrate_per_class import defrate_per_class

__all__ = [
    "dist_edge_threshold",
    "local_defor_rate",
    "defrate_per_cat",
    "defrate_per_class",
]
