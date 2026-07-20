# gui/tile/evaluation_helpers.py
"""Pure (solara-free) helpers for the Step 7 evaluation tile.

Build dropdown items, locate the default forest variable, parse the interval,
and resolve user selections into kwargs for
``spatialrisk.evaluation.evaluate_against_truth``. Kept import-light so the
logic is unit-testable without a running GUI.
"""

import json
import re
import shutil
from pathlib import Path

from spatialrisk.evaluation import interval_from_target, label_for

# Accuracy indices produced by validate_two_layer; value = column key in the
# stored ``indices`` table, text = display label. All four are always computed.
ALL_METRICS = ["MedAE", "R2", "RMSE", "wRMSE"]
_METRIC_LABELS = {"R2": "R²"}


def metric_items():
    """[{text, value}] for the metric selector — one per accuracy index."""
    return [{"text": _METRIC_LABELS.get(m, m), "value": m} for m in ALL_METRICS]


def parse_csizes(text):
    """Parse a cell-size field into a list of positive ints.

    Accepts comma- and/or space-separated values (e.g. "100, 300 1000").
    Duplicates are dropped, preserving first-seen order. Returns
    (sizes, error): on success error is None; on failure sizes is None and
    error is a user-facing message.
    """
    tokens = [t for t in re.split(r"[,\s]+", (text or "").strip()) if t]
    if not tokens:
        return None, "Enter at least one cell size (pixels)."
    sizes = []
    for tok in tokens:
        try:
            val = int(tok)
        except (TypeError, ValueError):
            return None, "Cell size(s) must be whole numbers."
        if val <= 0:
            return None, "Cell size(s) must be positive."
        if val not in sizes:
            sizes.append(val)
    return sizes, None


def rows_for_record(record):
    """Display rows for an EvaluationRecord, tolerant of legacy/stale instances.

    Reads ``indices``/``metrics`` defensively so records created before the
    ``metrics`` field existed (or stale in-memory instances after a hot-reload)
    fall back to showing all columns instead of raising AttributeError.
    """
    indices = getattr(record, "indices", None) or []
    return displayed_indices(indices, getattr(record, "metrics", None))


def displayed_indices(indices, metrics):
    """Drop unselected metric columns from stored index rows for display.

    ``metrics`` is the list of metric keys to keep; an empty/falsy list keeps
    every column (legacy runs predate metric selection). Non-metric context
    columns (model, period, ncell, cell-size, …) are always kept.
    """
    if not metrics:
        return indices
    drop = {m for m in ALL_METRICS if m not in metrics}
    return [{k: v for k, v in row.items() if k not in drop} for row in indices]


def variable_items(project):
    """[{text, value}] for each processed-variable instance (value = storage key)."""
    if project is None or not getattr(project, "processed_variables", None):
        return []
    items = []
    for key, var in project.processed_variables.items():
        year = getattr(var, "year", None)
        text = f"{var.name} ({year})" if year is not None else var.name
        items.append({"text": text, "value": key})
    return sorted(items, key=lambda d: d["text"])


def map_items(project):
    """[{text: '<MODEL> — <period>', value: key}] for each registered prediction."""
    if project is None or not getattr(project, "predictions", None):
        return []
    items = [{"text": f"{label_for(pred)} — {pred.dataset_name}", "value": key}
             for key, pred in project.predictions.items()]
    return sorted(items, key=lambda d: d["text"])


def default_forest_key(project):
    """Storage key of the 'forest_gfc' instance, or None."""
    if project is None or not getattr(project, "processed_variables", None):
        return None
    for key, var in project.processed_variables.items():
        if var.name == "forest_gfc":
            return key
    return None


def parse_interval(project, truth_key):
    """Interval (int) parsed from the truth variable's name, or None."""
    if project is None or not truth_key:
        return None
    pv = getattr(project, "processed_variables", None)
    if not pv:
        return None
    var = pv.get(truth_key)
    if var is None:
        return None
    return interval_from_target(var.name)


def build_truth_spec(project, truth_key, forest_key, interval):
    """Resolve selections into evaluate_against_truth kwargs.

    Returns (spec, error). On success spec is a dict with keys
    defor_file, forest_file, time_interval, truth_tag and error is None.
    On failure spec is None and error is a user-facing message.
    """
    if not truth_key:
        return None, "Select a truth (deforestation) variable."
    if not forest_key:
        return None, "Select a forest-at-start variable."
    try:
        ti = int(interval)
    except (TypeError, ValueError):
        return None, "Interval (years) must be a whole number."
    if ti <= 0:
        return None, "Interval (years) must be a positive number."
    truth_var = project.processed_variables.get(truth_key)
    forest_var = project.processed_variables.get(forest_key)
    if truth_var is None or forest_var is None:
        return None, "Selected variable no longer exists in the project."
    year = getattr(truth_var, "year", None)
    tag = f"{truth_var.name}_{year}" if year is not None else truth_var.name
    return {
        "defor_file": truth_var.path,
        "forest_file": forest_var.path,
        "time_interval": ti,
        "truth_tag": tag,
    }, None


def build_evaluation_record(project, df, spec, resolved_keys, run_id,
                            created_at, csizes=(300,), metrics=None):
    """Build an EvaluationRecord from a result DataFrame and the run's truth spec.

    ``indices`` is materialized via ``df.to_json`` so values are JSON-native
    (numpy scalars/NaN become float/None), keeping ``Project.save()``'s
    ``json.dumps`` happy. ``metrics`` records which accuracy-index columns the
    user chose to show (empty = all).

    ``csv_path`` points at THIS run's aggregate CSV inside
    ``evaluation/<truth_tag>/<run_id>/``, so the existing
    ``Path(csv_path).parent`` figure-directory derivation keeps resolving to the
    run's own PNGs. ``artifacts`` comes from ``df.attrs`` (populated by
    ``evaluate_against_truth`` when it is given a run id) and stays empty when
    the run was not run-scoped.
    """
    from spatialrisk.evaluations import EvaluationRecord

    truth_tag = spec["truth_tag"]
    indices = json.loads(df.to_json(orient="records"))
    csv_path = str(
        Path(project.folders.project_folder) / "evaluation" / truth_tag
        / str(run_id) / "indices_all.csv"
    )
    artifacts = list((getattr(df, "attrs", None) or {}).get("artifacts") or [])
    return EvaluationRecord(
        name=truth_tag,
        truth_tag=truth_tag,
        truth_defor=str(spec["defor_file"]),
        truth_forest=str(spec["forest_file"]),
        time_interval=int(spec["time_interval"]),
        prediction_keys=list(resolved_keys),
        csizes=list(csizes),
        metrics=list(metrics) if metrics else [],
        created_at=created_at,
        indices=indices,
        csv_path=csv_path,
        run_id=run_id,
        artifacts=artifacts,
    )


def run_artifact_dir(project, record):
    """This run's own artifact directory, or None if it has none on disk.

    Returns ``evaluation/<truth_tag>/<run_id>`` when that directory exists.
    Legacy records (saved before run-scoping) wrote straight into the shared
    ``evaluation/<truth_tag>/`` folder and therefore have no run directory —
    they get None, which is what keeps their files out of any cleanup.
    """
    if project is None or record is None:
        return None
    truth_tag = getattr(record, "truth_tag", None)
    run_id = getattr(record, "run_id", None)
    if not truth_tag or not run_id:
        return None
    candidate = (Path(project.folders.project_folder) / "evaluation"
                 / str(truth_tag) / str(run_id))
    return candidate if candidate.is_dir() else None


def delete_run_artifacts(project, record):
    """Remove ONE run's artifact directory. Returns True if it was removed.

    Guarded twice over: the resolved target must sit strictly inside the
    project's ``evaluation/`` folder, and its final path component must be the
    record's run id. That makes it impossible to remove the shared
    ``evaluation/<truth_tag>/`` folder (whose legacy files must stay
    recoverable), a sibling run, or anything outside the project.

    Call ONLY after the project manifest commit has succeeded — see
    ``delete_evaluation_run``.
    """
    target_dir = run_artifact_dir(project, record)
    if target_dir is None:
        return False
    try:
        target = target_dir.resolve()
        eval_root = (Path(project.folders.project_folder) / "evaluation").resolve()
    except (OSError, RuntimeError):
        return False
    if eval_root not in target.parents or target.name != str(record.run_id):
        return False
    shutil.rmtree(target, ignore_errors=True)
    return not target.exists()


def delete_evaluation_run(project, key):
    """Delete a saved evaluation: registry entry, then commit, THEN artifacts.

    The ordering is load-bearing. Removing the run directory before the manifest
    is persisted would destroy the data a still-registered record points at if
    the save then failed, so the files are only unlinked once ``project.save()``
    has returned successfully. A failed save leaves every artifact on disk.

    Returns ``(deleted, error)``: ``deleted`` is False only when *key* is not
    registered; ``error`` is the save failure message, or None.
    """
    record = project.get_evaluation(key)
    if record is None:
        return False, None
    project.delete_evaluation(key, auto_save=False)
    try:
        project.save()
    except Exception as exc:  # noqa: BLE001 - artifacts must survive a bad save
        return True, str(exc)
    delete_run_artifacts(project, record)
    return True, None
