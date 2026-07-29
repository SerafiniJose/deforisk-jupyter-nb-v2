"""One saved deforestation-allocation run.

Provenance-first: the record keeps the exact ``project.predictions`` registry key
the user picked (registry keys are user-chosen names, NOT reconstructable from
``Prediction.storage_key()``) plus an immutable snapshot of the prediction, so a
reloaded run still says what it was computed from even if the prediction is gone.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


def _sanitize(name: Optional[str]) -> str:
    """Path/key-safe token. Mirrors gui/scripts/artifact_names.py::sanitize_key."""
    return re.sub(r"[^A-Za-z0-9_-]+", "_", (name or "").strip()).strip("_")


class AllocationRun(BaseModel):
    """Inputs, provenance and results of one allocation run."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    run_id: str
    created_at: Optional[str] = None

    # Inputs / provenance
    prediction_key: Optional[str] = None
    prediction_snapshot: Dict[str, Any] = Field(default_factory=dict)
    external_riskmap: Optional[str] = None
    defrate_source: Dict[str, Any] = Field(default_factory=dict)
    borders_file: str
    #: How the user picked those borders: ``{method, file_path, admin_code,
    #: asset}``. Mirrors ``defrate_source``. Defaulted, so runs saved before
    #: the borders picker existed still load.
    borders_source: Dict[str, Any] = Field(default_factory=dict)
    mask_file: Optional[str] = None
    defor_juris_ha: float
    years_forecast: float

    # Results
    annual_ha: float
    total_ha: float
    out_dir: str
    csv_path: str
    density_map_path: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)

    def storage_key(self) -> str:
        """History-safe registry key and folder name: ``<sanitized name>_<run_id>``."""
        return f"{_sanitize(self.name)}_{self.run_id}"
