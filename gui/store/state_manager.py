import solara


class AppState:
    """Reactive application state for Spatial Risk."""

    def __init__(self):
        # Navigation
        self.current_step = solara.reactive(0)  # 0=AOI, 1=Variables, 2=Dataset

        # Project (spatialrisk.project.Project | None)
        self.project = solara.reactive(None)

        # AOI (pysepal AoiResult | None)
        self.aoi_result = solara.reactive(None)

        # Variable processing
        self.processing = solara.reactive(False)
        self.process_error = solara.reactive(None)

        # Global UI state
        self.loading = solara.reactive(False)
        self.error_message = solara.reactive(None)
        self.status_message = solara.reactive(None)

    @property
    def aoi_complete(self) -> bool:
        return self.aoi_result.value is not None

    @property
    def variables_complete(self) -> bool:
        p = self.project.value
        if p is None:
            return False
        has_vars = bool(p.raw_variables)
        has_base = p.base_raster is not None
        return has_vars and has_base


app_state = AppState()
