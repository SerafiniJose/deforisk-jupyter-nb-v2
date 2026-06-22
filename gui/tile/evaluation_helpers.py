# gui/tile/evaluation_helpers.py
"""Pure (solara-free) helpers for the Step 7 evaluation tile.

Build dropdown items, locate the default forest variable, parse the interval,
and resolve user selections into kwargs for
``spatialrisk.evaluation.evaluate_against_truth``. Kept import-light so the
logic is unit-testable without a running GUI.
"""

import json
from pathlib import Path

from spatialrisk.evaluation import interval_from_target, label_for


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
                            created_at, csizes=(300,)):
    """Build an EvaluationRecord from a result DataFrame and the run's truth spec.

    ``indices`` is materialized via ``df.to_json`` so values are JSON-native
    (numpy scalars/NaN become float/None), keeping ``Project.save()``'s
    ``json.dumps`` happy.
    """
    from spatialrisk.evaluations import EvaluationRecord

    truth_tag = spec["truth_tag"]
    indices = json.loads(df.to_json(orient="records"))
    csv_path = str(
        Path(project.folders.project_folder) / "evaluation" / truth_tag
        / "indices_all.csv"
    )
    return EvaluationRecord(
        name=truth_tag,
        truth_tag=truth_tag,
        truth_defor=str(spec["defor_file"]),
        truth_forest=str(spec["forest_file"]),
        time_interval=int(spec["time_interval"]),
        prediction_keys=list(resolved_keys),
        csizes=list(csizes),
        created_at=created_at,
        indices=indices,
        csv_path=csv_path,
        run_id=run_id,
    )
