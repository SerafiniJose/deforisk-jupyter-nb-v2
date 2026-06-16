"""Deforestation-rate routines for the MW and JNR risk models.

Current implementation
----------------------
The supported, model-facing computations are the **native two-explicit-layer**
functions in the ``deforrate`` submodule. They take separate binary rasters —
a deforestation layer (``1`` = deforested) and a forest-at-start layer
(``1`` = forest) — with no ``fcc123`` packing, no ``defor_values`` and no
``period`` branching:

    deforrate.dist_edge_threshold   forest-edge distance threshold
    deforrate.local_defor_rate      moving-window local deforestation rate
    deforrate.defrate_per_cat       rate per moving-window risk category (MW)
    deforrate.defrate_per_class     rate per vulnerability class (JNR)

Shared helpers (already fcc-free, kept as-is and re-exported at top level):

    compute_dist_bins       geometric distance-bin edges (thin wrapper)
    vulnerability_map       build the vulnerability/risk raster (thin wrapper)
    set_defor_cat_zero      zero risk categories beyond the distance threshold

Legacy
------
The previous single-layer / ``riskmapjnr``-delegating implementations of
``dist_edge_threshold`` / ``local_defor_rate`` / ``defrate_per_cat`` /
``defrate_per_class`` have been quarantined in the ``legacy`` submodule. They are
**not used by any model** and are kept only for reference / reproducing
pre-migration results. ``deforrate`` is verified numerically identical to them on
equivalent inputs. Reach them explicitly via ``rmj.legacy.<name>`` if needed.
"""

from spatialrisk.rmj.compute_dist_bins import compute_dist_bins
from spatialrisk.rmj.vulnerability_map import vulnerability_map
from spatialrisk.rmj.set_defor_cat_zero import set_defor_cat_zero
from spatialrisk.rmj import deforrate
from spatialrisk.rmj import legacy

__all__ = [
    # current native core
    "deforrate",
    # current shared helpers
    "compute_dist_bins",
    "vulnerability_map",
    "set_defor_cat_zero",
    # superseded, reference only
    "legacy",
]
