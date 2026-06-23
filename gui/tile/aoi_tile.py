"""Step 1 — AOI selection tile."""

import solara

from gui.widget.aoi_view import AoiView  # vendored fork (restore-on-load)


@solara.component
def AoiTile(map_, gee_interface, aoi_result, aoi_asset, on_selection, restore_signal, loading):
    """AOI selection step using the vendored AoiView.

    Args:
        map_: SepalMap instance (shared with left panel).
        gee_interface: Current GEEInterface from session.
        aoi_result: Reactive holding the current AoiResult (or None).
        aoi_asset: Reactive holding the captured ASSET descriptor (or None).
        on_selection: Callback fired with the asset descriptor (or None) on select.
        restore_signal: project_loaded_signal value; remounts the picker per load
            so its mount-time restore re-runs against the freshly loaded AOI.
        loading: Reactive bool for loading state.
    """
    # Key the subtree on the load signal: each project switch remounts AoiView so
    # its (and its children's) mount-time restore effects re-read the current AOI.
    with solara.Column(style="gap: 16px;").key(f"aoi-{restore_signal}"):
        solara.Markdown("### Step 1 — Area of Interest")
        solara.Text("Select the area you want to analyse. All subsequent steps will be clipped to this boundary.")

        AoiView(
            value=aoi_result,
            loading=loading,
            methods="ALL",
            map_=map_,
            gee=True,
            restore_asset=aoi_asset.value,
            on_selection=on_selection,
        )
