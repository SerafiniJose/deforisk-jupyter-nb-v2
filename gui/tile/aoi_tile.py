"""Step 1 — AOI selection tile."""

import solara
from pysepal.solara.components.aoi import AoiView


@solara.component
def AoiTile(map_, gee_interface, aoi_result, loading):
    """AOI selection step using pysepal AoiView.

    Args:
        map_: SepalMap instance (shared with left panel).
        gee_interface: Current GEEInterface from session.
        aoi_result: Reactive holding the current AoiResult (or None).
        loading: Reactive bool for loading state.
    """
    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 1 — Area of Interest")
        solara.Text("Select the area you want to analyse. All subsequent steps will be clipped to this boundary.")

        AoiView(
            value=aoi_result,
            loading=loading,
            methods="ALL",
            map_=map_,
            gee=True,
        )

        if aoi_result.value is not None:
            solara.Success(f"AOI selected: **{aoi_result.value.name}**")
