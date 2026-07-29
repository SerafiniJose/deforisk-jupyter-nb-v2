"""Bridge between the allocation tool's UI and the numeric core.

Solara-free by contract (this module is imported by tests without a render
harness); heavy geo dependencies are imported lazily inside functions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("spatial_risk")

#: Families whose apply() writes a per-category rate table next to the raster.
_MW_FAMILY = "mw"
_JNR_FAMILY = "benchmark"

_JNR_CAVEAT = (
    "This table's rates are the deforestation observed in the prediction's own "
    "period (the app applies the JNR benchmark without a model-period rate table). "
    "Override it if you have a calibration/historical table."
)


class AllocationResolveError(ValueError):
    """Raised when the rate table for a prediction cannot be resolved."""


@dataclass
class DefrateSource:
    """Where an allocation run's rate table came from."""

    path: Optional[Path]
    provenance: str  # "persisted" | "mw-sibling" | "computed" | "user"
    caveat: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        """Serializable form, stored on the AllocationRun record."""
        return {
            "path": str(self.path) if self.path else None,
            "provenance": self.provenance,
            "caveat": self.caveat,
        }


def _resolve_layers(project, pred):
    """Indirection so tests can stub the evaluation resolver."""
    from spatialrisk.evaluation import resolve_layers

    return resolve_layers(project, pred)


def _defrate_per_cat(**kwargs):
    """Indirection so tests can stub the (slow, GDAL-bound) rate computation."""
    from spatialrisk.rmj.deforrate import defrate_per_cat

    return defrate_per_cat(**kwargs)


def resolve_defrate_table(
    project,
    pred_key: str,
    *,
    user_path: Optional[Path] = None,
    compute: bool = True,
) -> DefrateSource:
    """Find (or compute) the per-category rate table for a registered prediction.

    Order: explicit user override → the table persisted on the Prediction →
    the MW sibling-path convention → computed from the prediction's dataset.
    """
    if user_path:
        return DefrateSource(path=Path(user_path), provenance="user")

    pred = (project.predictions or {}).get(pred_key)
    if pred is None:
        raise AllocationResolveError(
            f"Prediction '{pred_key}' not found in this project."
        )

    persisted = getattr(pred, "defrate_path", None)
    if persisted and Path(persisted).exists():
        caveat = _JNR_CAVEAT if pred.model_key == _JNR_FAMILY else None
        return DefrateSource(
            path=Path(persisted), provenance="persisted", caveat=caveat
        )

    if pred.model_key == _MW_FAMILY:
        sibling = Path(pred.path).parent / (
            f"defrate_cat_mw_{pred.window}_{pred.dataset_name}.csv"
        )
        if sibling.exists():
            return DefrateSource(path=sibling, provenance="mw-sibling")
        raise AllocationResolveError(
            f"No rate table for moving-window prediction '{pred_key}': expected "
            f"{sibling.name} beside the raster. Re-run inference or select a table "
            "manually."
        )

    if pred.model_key == _JNR_FAMILY:
        raise AllocationResolveError(
            f"No rate table recorded for JNR prediction '{pred_key}'. Re-run "
            "inference or select a table manually."
        )

    if not compute:
        raise AllocationResolveError(
            f"No rate table available for '{pred_key}' and computing is disabled."
        )

    layers = _resolve_layers(project, pred)
    if not layers.get("time_interval"):
        raise AllocationResolveError(
            "Cannot determine the period length from the dataset's target name "
            "(expected two 4-digit years, e.g. 'forest_loss_2015_2020'). Select a "
            "rate table manually."
        )
    out = (
        Path(pred.path).parent / f"defrate_cat_{pred.model_key}_{pred.dataset_name}.csv"
    )
    if not out.exists():
        logger.info("Computing rate table for '%s' → %s", pred_key, out.name)
        _defrate_per_cat(
            defor_file=layers["defor_file"],
            forest_file=layers["forest_file"],
            riskmap_file=layers["riskmap_file"],
            time_interval=layers["time_interval"],
            tab_file_defrate=out,
            verbose=False,
        )
    return DefrateSource(path=out, provenance="computed")
