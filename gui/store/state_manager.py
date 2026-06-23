from datetime import datetime
from typing import Optional

import solara


class AppState:
    """Reactive application state for Spatial Risk."""

    def __init__(self):
        # Navigation
        self.current_step = solara.reactive(0)  # 0=AOI, 1=Variables, 2=Dataset

        # Project (spatialrisk.project.Project | None)
        # Use identity equality: Project is a mutable pydantic model — field-value
        # comparison (pydantic __eq__) would suppress re-renders after in-place
        # mutations. Any new reference must fire, so we compare by identity only.
        self.project = solara.reactive(None, equals=lambda a, b: a is b)

        # Unsaved-changes tracking (best-effort: "changed since last save/load").
        self.project_dirty = solara.reactive(False)
        self.last_saved = solara.reactive(None)  # datetime | None

        # AOI (pysepal AoiResult | None)
        self.aoi_result = solara.reactive(None)

        # Captured/loaded ASSET descriptor ({asset_id,type,column,value}). The only
        # AOI selection input not recoverable from the AoiResult, so it is tracked
        # and persisted separately. See gui/scripts/aoi_io.py.
        self.aoi_asset = solara.reactive(None)

        # Bumped each time a project is loaded from disk. The map view subscribes
        # to this so it can zoom to the AOI on load (a plain project-reference
        # watch would also fire on every in-place mutation).
        self.project_loaded_signal = solara.reactive(0)

        # Variable processing
        self.processing = solara.reactive(False)
        self.process_error = solara.reactive(None)

        # Global UI state
        self.loading = solara.reactive(False)
        self.error_message = solara.reactive(None)
        self.status_message = solara.reactive(None)

        # Mark the project dirty whenever its reference changes (tiles call
        # project.set(p.model_copy()) on every mutation). Suppressed during load
        # so reloading from disk does not mark the project dirty.
        self._suppress_dirty = False
        self.project.subscribe(self._on_project_changed)

    def _on_project_changed(self, new_value) -> None:
        if self._suppress_dirty:
            return
        self.project_dirty.set(new_value is not None)

    def mark_saved(self, when: datetime) -> None:
        """Record a successful save: clear dirty, stamp last-saved time."""
        self.project_dirty.set(False)
        self.last_saved.set(when)

    def load_project_state(self, project, when: Optional[datetime]) -> None:
        """Install a loaded project without marking it dirty."""
        self._suppress_dirty = True
        try:
            self.project.set(project)
        finally:
            self._suppress_dirty = False
        self.project_dirty.set(False)
        self.last_saved.set(when)
        self.project_loaded_signal.set(self.project_loaded_signal.value + 1)

    def new_project_state(self, project) -> None:
        """Install a freshly created project (dirty, never saved) and reset the
        workflow context so the user starts clean at the AOI step."""
        self.project.set(project)  # subscription marks dirty=True
        self.last_saved.set(None)
        self.aoi_result.set(None)
        self.aoi_asset.set(None)
        self.process_error.set(None)
        self.status_message.set(None)
        self.error_message.set(None)
        # Bump the same signal a load does so the shell's on-switch effects run
        # (clear the previous project's map overlays + tracking, rebuild the
        # empty Train/Inference job lists). The signal means "a project was
        # installed" — loaded from disk OR newly created.
        self.project_loaded_signal.set(self.project_loaded_signal.value + 1)

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
