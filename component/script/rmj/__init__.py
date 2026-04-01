"""Generic wrappers and re-implementations of riskmapjnr functions.

All functions here accept **binary (0/1) rasters** instead of the
multi-period FCC stack that the original riskmapjnr library requires.

Functions
---------
dist_edge_threshold     Compute the forest-edge distance threshold from a
                        binary deforestation raster.
compute_dist_bins       Compute geometric distance-bin edges (thin wrapper).
vulnerability_map       Build the vulnerability/risk raster (thin wrapper).
defrate_per_class       Compute deforestation rates per vulnerability class
                        from separate binary forest + deforestation rasters.
"""

from component.script.rmj.dist_edge_threshold import dist_edge_threshold
from component.script.rmj.compute_dist_bins import compute_dist_bins
from component.script.rmj.vulnerability_map import vulnerability_map
from component.script.rmj.defrate_per_class import defrate_per_class

__all__ = [
    "dist_edge_threshold",
    "compute_dist_bins",
    "vulnerability_map",
    "defrate_per_class",
]
