"""Step 1 — AOI selection tile."""

import solara
from pysepal.solara.components.aoi import AoiView

from gui.i18n import t


@solara.component
def AoiTile(map_, gee_interface, aoi_result, restore_signal, loading):
    """AOI selection step using pysepal's AoiView (restore-on-mount).

    Args:
        map_: SepalMap instance (shared with left panel).
        gee_interface: Current GEEInterface from session.
        aoi_result: Reactive holding the current AoiResult (or None). ASSET
            selections carry their picker inputs on ``AoiResult.asset``.
        restore_signal: project_loaded_signal value; remounts the picker per load
            so its mount-time restore re-runs against the freshly loaded AOI.
        loading: Reactive bool for loading state.
    """
    # Key the subtree on the load signal: each project switch remounts AoiView so
    # its (and its children's) mount-time restore effects re-read the current AOI.
    with solara.Column(style="gap: 16px;").key(f"aoi-{restore_signal}"):
        solara.Text(t("tiles.aoi.description"))

        AoiView(
            value=aoi_result,
            loading=loading,
            methods="ALL",
            map_=map_,
            gee=True,
        )
