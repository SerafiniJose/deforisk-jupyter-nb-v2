"""Bridge between the allocation tool's UI and the numeric core.

Solara-free by contract (this module is imported by tests without a render
harness); heavy geo dependencies are imported lazily inside functions.
"""

from __future__ import annotations

import logging
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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

    out = (
        Path(pred.path).parent / f"defrate_cat_{pred.model_key}_{pred.dataset_name}.csv"
    )
    if out.exists():
        return DefrateSource(path=out, provenance="computed")
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


def preview_defrate_source(
    project, pred_key: Optional[str], *, user_path: Optional[Path] = None
) -> DefrateSource:
    """Form-time preview of ``resolve_defrate_table``: never computes, never raises.

    Same resolution order as the run itself, so what the form shows is what the
    run will use. Provenance ``"unavailable"`` (path None, reason in ``caveat``)
    stands in for the resolver's errors; provenance ``"computed"`` with a
    not-yet-existing path means "will be computed from the dataset".
    """
    if user_path:
        return DefrateSource(path=Path(user_path), provenance="user")
    pred = (getattr(project, "predictions", None) or {}).get(pred_key)
    if pred is None:
        return DefrateSource(
            path=None,
            provenance="unavailable",
            caveat=f"Prediction '{pred_key}' not found in this project.",
        )
    try:
        return resolve_defrate_table(project, pred_key, compute=False)
    except AllocationResolveError as exc:
        if pred.model_key in (_MW_FAMILY, _JNR_FAMILY):
            return DefrateSource(path=None, provenance="unavailable", caveat=str(exc))
        out = (
            Path(pred.path).parent
            / f"defrate_cat_{pred.model_key}_{pred.dataset_name}.csv"
        )
        return DefrateSource(path=out, provenance="computed")


#: Raster suffixes a processed variable must have to be offered as a mask.
_MASK_SUFFIXES = {".tif", ".tiff", ".vrt"}


def mask_items(project) -> List[dict]:
    """Mask choices for the form: the project's processed raster variables.

    ``[{"text": <variable name>, "value": <raster path>}]``, name-sorted.
    Vector variables are skipped — the allocation core wants a raster aligned
    with the risk map (it warps it to the cropped grid itself).
    """
    items = []
    for key, var in (getattr(project, "processed_variables", None) or {}).items():
        path = getattr(var, "path", None)
        if path and Path(path).suffix.lower() in _MASK_SUFFIXES:
            items.append({"text": key, "value": str(path)})
    return sorted(items, key=lambda d: d["text"])


@dataclass
class AllocationForm:
    """User inputs of one allocation run (already parsed from the form widgets).

    The risk map is always one of the project's predictions: external risk
    maps are imported on the inference tab (where they become predictions),
    so the allocation tool never takes a raster file directly.
    """

    name: str
    prediction_key: Optional[str]
    user_defrate_path: Optional[str]
    borders_file: Optional[str]
    mask_file: Optional[str]
    defor_juris_ha: float
    years_forecast: float
    density_map: bool = False


def _allocate(**kwargs):
    """Indirection so tests can stub the (slow, GDAL-bound) core."""
    from spatialrisk.allocation import allocate_deforestation

    return allocate_deforestation(**kwargs)


def validate_form(project, form: AllocationForm) -> Optional[str]:
    """Return a user-facing error message, or None when the form is runnable."""
    if not (form.name or "").strip():
        return "Give the allocation run a name."
    if not form.prediction_key:
        return "Choose a risk map from the project's predictions."
    if not form.borders_file:
        return "Choose the project borders vector file."
    if form.years_forecast is None or form.years_forecast <= 0:
        return "The forecast period must be at least one year."
    if form.defor_juris_ha is None or form.defor_juris_ha < 0:
        return "Expected jurisdictional deforestation cannot be negative (hectares)."
    for label, value in (
        ("rate table", form.user_defrate_path),
        ("project borders", form.borders_file),
        ("mask", form.mask_file),
    ):
        if value and not Path(value).exists():
            return f"The selected {label} file does not exist: {value}"
    return None


def run_allocation(
    project,
    form: AllocationForm,
    project_reactive=None,
    notifier=None,
    jobs_reactive=None,
    job_id: Optional[str] = None,
):
    """Execute one allocation and register it on the project.

    Runs on a worker thread (the GDAL calls release the GIL). Wrapped by the
    caller in ``tracked_job`` + ``writing`` when a notifier is available.
    """
    from spatialrisk.allocations import AllocationRun

    run_id = uuid.uuid4().hex[:8]
    record_key = AllocationRun(
        name=form.name,
        run_id=run_id,
        borders_file=form.borders_file or "",
        defor_juris_ha=form.defor_juris_ha,
        years_forecast=form.years_forecast,
        annual_ha=0.0,
        total_ha=0.0,
        out_dir="",
        csv_path="",
    ).storage_key()
    out_dir = Path(project.folders.project_folder) / "allocation" / record_key

    pred = (project.predictions or {}).get(form.prediction_key)
    if pred is None:
        raise AllocationResolveError(
            f"Prediction '{form.prediction_key}' is no longer in this project."
        )
    riskmap_file = pred.path
    source = resolve_defrate_table(
        project, form.prediction_key, user_path=form.user_defrate_path
    )

    logger.info("Allocating deforestation for '%s'…", form.name)
    result = _allocate(
        riskmap_file=riskmap_file,
        defrate_table=source.path,
        defor_juris_ha=form.defor_juris_ha,
        years_forecast=form.years_forecast,
        project_borders=form.borders_file,
        out_dir=out_dir,
        forest_mask_file=form.mask_file,
        defor_density_map=form.density_map,
    )

    record = AllocationRun(
        name=form.name,
        run_id=run_id,
        created_at=datetime.now().isoformat(timespec="seconds"),
        prediction_key=form.prediction_key,
        prediction_snapshot={
            "path": str(pred.path),
            "model_key": pred.model_key,
            "dataset_name": pred.dataset_name,
            "year": getattr(pred, "year", None),
            "window": getattr(pred, "window", None),
        },
        defrate_source=source.as_dict(),
        borders_file=form.borders_file or "",
        mask_file=form.mask_file,
        defor_juris_ha=form.defor_juris_ha,
        years_forecast=form.years_forecast,
        annual_ha=result.annual_ha,
        total_ha=result.total_ha,
        out_dir=str(result.out_dir),
        csv_path=str(result.csv_path),
        density_map_path=(
            str(result.density_map_path) if result.density_map_path else None
        ),
        warnings=list(result.warnings),
    )
    project.add_allocation(record, auto_save=True)
    if project_reactive is not None:
        from gui.scripts.solara_threads import publish_if_current

        publish_if_current(project_reactive, project)
    return record


def _run_source(record) -> Optional[str]:
    """'<MODEL> — <dataset>' of the prediction a run came from, None if external."""
    snapshot = getattr(record, "prediction_snapshot", None) or {}
    if not snapshot.get("model_key"):
        return None
    from types import SimpleNamespace

    from spatialrisk.evaluation import label_for

    pred = SimpleNamespace(
        model_key=snapshot["model_key"], window=snapshot.get("window")
    )
    return f"{label_for(pred)} — {snapshot.get('dataset_name', '')}"


def allocation_rows(project, jobs=None) -> List[dict]:
    """Rows for the allocation list: in-flight jobs first, then saved runs."""
    rows: List[dict] = []
    for job in jobs or []:
        rows.append(
            {
                "kind": "job",
                "key": job.get("id"),
                "name": job.get("name"),
                "status": job.get("status"),
                "error": job.get("error"),
            }
        )
    for key, record in (getattr(project, "allocations", None) or {}).items():
        rows.append(
            {
                "kind": "record",
                "key": key,
                "name": record.name,
                "source": _run_source(record),
                "created_at": record.created_at,
                "annual_ha": record.annual_ha,
                "total_ha": record.total_ha,
                "years_forecast": record.years_forecast,
                "density_map_path": record.density_map_path,
                "warnings": record.warnings,
                "provenance": (record.defrate_source or {}).get("provenance"),
            }
        )
    return rows


def delete_allocation_run(project, key: str) -> bool:
    """Delete a saved allocation: registry entry, commit, THEN artifacts.

    Deletion is a transaction, mirroring evaluation_helpers.delete_evaluation_run:
    a failed manifest save must never leave a record pointing at files that are
    already gone, so the record is restored and the files kept.
    """
    record = (getattr(project, "allocations", None) or {}).get(key)
    if record is None:
        return False
    project.delete_allocation(key)
    try:
        project.save()
    except Exception:
        project.allocations[key] = record
        raise

    run_dir = Path(record.out_dir).resolve()
    allocation_root = (Path(project.folders.project_folder) / "allocation").resolve()
    if run_dir.is_dir() and allocation_root in run_dir.parents and run_dir.name == key:
        shutil.rmtree(run_dir, ignore_errors=True)
    else:
        logger.warning(
            "Refusing to delete '%s': not a run directory under %s",
            run_dir,
            allocation_root,
        )
    return True
