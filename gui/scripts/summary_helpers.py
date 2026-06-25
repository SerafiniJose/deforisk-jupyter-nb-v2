"""Solara-free data shaping for the read-only Project Summary popup.

Each extractor takes a Project and returns ``(stats, rows)`` where every row is a
dict of pre-formatted display values. The registries are typed ``Any`` on the
Project, so all attribute access is via ``getattr(..., default)`` — extractors
must never raise on a partially-populated or legacy component object.
"""

from typing import Any, Dict, List, Tuple


def _enum_str(v: Any) -> Any:
    """Return an enum's ``.value`` (variables use ``use_enum_values``) or the value as-is."""
    return getattr(v, "value", v)


def _fmt(v: Any, dash: str = "—") -> Any:
    """Replace empty values with an em-dash; otherwise return the value unchanged."""
    if v is None or v == "":
        return dash
    return v


def project_overview(p: Any, last_saved: Any = None, dirty: bool = False) -> Dict[str, Any]:
    """Header facts for the popup: name, AOI, discovered years, per-registry counts."""
    aoi = getattr(p, "aoi", None) or {}
    return {
        "project_name": getattr(p, "project_name", "—"),
        "aoi_name": aoi.get("name") if isinstance(aoi, dict) else None,
        "years": p.get_available_years() if hasattr(p, "get_available_years") else [],
        "counts": {
            "raw": len(getattr(p, "raw_variables", {})),
            "processed": len(getattr(p, "processed_variables", {})),
            "datasets": len(getattr(p, "datasets", {})),
            "samples": len(getattr(p, "samples", {})),
            "models": len(getattr(p, "models", {})),
            "predictions": len(getattr(p, "predictions", {})),
            "evaluations": len(getattr(p, "evaluations", {})),
        },
        "last_saved": last_saved,
        "dirty": dirty,
    }


def _count_kinds(items) -> Tuple[int, int]:
    vec = ras = 0
    for v in items:
        dt = _enum_str(getattr(v, "data_type", None))
        if dt == "vector":
            vec += 1
        elif dt == "raster":
            ras += 1
    return vec, ras


def raw_variable_rows(p: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    variables = getattr(p, "raw_variables", {})
    base = getattr(p, "base_raster", None)
    base_name = getattr(base, "name", None)
    rows = []
    for key, v in variables.items():
        name = getattr(v, "name", key)
        rows.append({
            "key": key,
            "name": name,
            "data_type": _fmt(_enum_str(getattr(v, "data_type", None))),
            "raster_type": _fmt(_enum_str(getattr(v, "raster_type", None))),
            "year": _fmt(getattr(v, "year", None)),
            "is_base": base_name is not None and name == base_name,
        })
    vec, ras = _count_kinds(variables.values())
    return {"total": len(rows), "vector": vec, "raster": ras}, rows


def processed_variable_rows(p: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    raw = getattr(p, "raw_variables", {})
    processed = getattr(p, "processed_variables", {})
    rows = []
    for key, v in processed.items():
        name = getattr(v, "name", key)
        source = next(
            (rk for rk, rv in raw.items() if name.startswith(getattr(rv, "name", "\0"))),
            "—",
        )
        rows.append({
            "key": key,
            "name": name,
            "source": source,
            "raster_type": _fmt(_enum_str(getattr(v, "raster_type", None))),
            "year": _fmt(getattr(v, "year", None)),
        })
    vec, ras = _count_kinds(processed.values())
    return {"total": len(rows), "vector": vec, "raster": ras}, rows


def dataset_rows(p: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows = []
    for key, ds in getattr(p, "datasets", {}).items():
        target = getattr(ds, "target", None)
        rows.append({
            "key": key,
            "name": getattr(ds, "name", None) or key,
            "target_name": getattr(target, "name", None) or "—",
            "feature_count": len(getattr(ds, "features", []) or []),
            "year": _fmt(getattr(ds, "year", None)),
        })
    return {"total": len(rows)}, rows


def sample_rows(p: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows = []
    points = 0
    for key, s in getattr(p, "samples", {}).items():
        n_total = getattr(s, "n_total", 0) or 0
        points += n_total
        allocation = getattr(s, "allocation", None)
        class_counts = getattr(s, "class_counts", {}) or {}
        rows.append({
            "key": key,
            "name": getattr(s, "name", None) or key,
            "strategy": _fmt(getattr(s, "strategy", None)),
            "allocation": _fmt(allocation),
            "n_total": n_total,
            "class_counts": class_counts,
            "seed": _fmt(getattr(s, "seed", None)),
        })
    return {"total": len(rows), "points": points}, rows


def model_rows(p: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows = []
    n_trained = 0
    for key, m in getattr(p, "models", {}).items():
        trained = bool(getattr(m, "trained", False))
        if trained:
            n_trained += 1
        params = getattr(m, "parameters", {}) or {}
        params_str = " · ".join(f"{k}={v}" for k, v in params.items()) or "—"
        dev = getattr(m, "deviance", None)
        rows.append({
            "key": key,
            "name": getattr(m, "name", None) or key,
            "model_type": _fmt(getattr(m, "model_type", None)),
            "year": _fmt(getattr(m, "year", None)),
            "trained": trained,
            "trained_at": _fmt(getattr(m, "trained_at", None)),
            "n_samples": _fmt(getattr(m, "n_samples", None)),
            "deviance": round(dev, 3) if isinstance(dev, (int, float)) else "—",
            "params": params_str,
        })
    return {"total": len(rows), "trained": n_trained}, rows


def prediction_rows(p: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows = []
    n_active = 0
    for key, pr in getattr(p, "predictions", {}).items():
        active = bool(getattr(pr, "active", False))
        if active:
            n_active += 1
        rows.append({
            "key": key,
            "model_key": _fmt(getattr(pr, "model_key", None)),
            "dataset_name": _fmt(getattr(pr, "dataset_name", None)),
            "year": _fmt(getattr(pr, "year", None)),
            "window": _fmt(getattr(pr, "window", None)),
            "active": active,
        })
    return {"total": len(rows), "active": n_active}, rows


def evaluation_rows(p: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows = []
    for key, ev in getattr(p, "evaluations", {}).items():
        pkeys = list(getattr(ev, "prediction_keys", []) or [])
        csizes = list(getattr(ev, "csizes", []) or [])
        metrics = list(getattr(ev, "metrics", []) or [])
        rows.append({
            "key": key,
            "name": getattr(ev, "name", None) or getattr(ev, "truth_tag", None) or key,
            "truth_tag": _fmt(getattr(ev, "truth_tag", None)),
            "n_predictions": len(pkeys),
            "csizes": ", ".join(str(c) for c in csizes) or "—",
            "metrics": ", ".join(metrics) if metrics else "all",
            "created_at": _fmt(getattr(ev, "created_at", None)),
        })
    return {"total": len(rows)}, rows
