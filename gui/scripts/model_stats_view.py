"""Solara-free view-model for the model Statistics tab (Spec A §4).

Row/card shaping only — no widgets, no i18n resolution (the widget resolves
``key`` by calling ``t()`` on ``"tiles.train.stats." + key``). Values arrive
display-ready so the widget never formats numbers.
"""

import math
from typing import List, Optional

DASH = "—"


def _fmt(v, digits=4):
    if v is None:
        return DASH
    if isinstance(v, float):
        if not math.isfinite(v):
            return DASH
        return f"{v:,.{digits}g}" if abs(v) >= 1000 else f"{v:.{digits}g}"
    return f"{v:,}" if isinstance(v, int) else str(v)


def stat_cards(model) -> List[dict]:
    """Header cards.

    None-valued cards are omitted (spec §4.2); a stored non-finite deviance
    renders as a flagged em-dash, never 'nan'.
    """
    stats = getattr(model, "stats", None)
    cards = []

    def add(key, value, warn=False):
        if value is None:
            return
        cards.append({"key": key, "value": value, **({"warn": True} if warn else {})})

    def add_num(key, raw, digits=4):
        # Guard on the RAW value before formatting: _fmt(None) returns "—",
        # not None, so `add` alone can never skip a None-valued numeric card
        # — every GLM/RF model would otherwise render the MW-only threshold
        # cards reading "—". Checking `raw` here (before formatting) is what
        # actually omits them, matching this function's own docstring.
        if raw is None:
            return
        add(key, _fmt(raw, digits))

    if stats is not None and stats.n_rows is not None:
        add("card_rows", _fmt(stats.n_rows))
    if stats is not None and stats.n_events is not None:
        events = _fmt(stats.n_events)
        if stats.sample_design:
            events = f"{events} ({stats.sample_design})"
        add("card_events", events)
    deviance = getattr(model, "deviance", None)
    if deviance is not None:
        if isinstance(deviance, float) and not math.isfinite(deviance):
            add("card_deviance", DASH, warn=True)
        else:
            add("card_deviance", _fmt(float(deviance), digits=6))
    if stats is not None:
        add_num("card_dist_thresh", getattr(stats, "dist_thresh", None))
        add_num("card_perc_thresh", getattr(stats, "perc_thresh", None))
        add_num("card_tot_defor", getattr(stats, "tot_defor_ha", None))
        add_num("card_n_classes", getattr(stats, "n_classes", None))
        vrho = getattr(stats, "vrho", None)
        if vrho is not None:
            add_num("card_vrho", vrho.estimate)
    add("card_trained_at", getattr(model, "trained_at", None))
    return cards


def coefficient_rows(stats) -> List[dict]:
    """Display rows for GLM/iCAR coefficient tables.

    odds_ratio is computed here, at display time (spec §2.1) — it is not a
    stored field.
    """
    rows = []
    for c in getattr(stats, "coefficients", None) or []:
        est = c.estimate
        rows.append(
            {
                "name": c.name,
                "estimate": _fmt(est),
                "estimate_raw": est,
                "odds_ratio": _fmt(math.exp(est)) if est is not None else DASH,
                "std": _fmt(c.std),
                "ci_low": _fmt(c.ci_low),
                "ci_high": _fmt(c.ci_high),
                # raw floats for the effect bar — never parse the display strings
                "ci_low_raw": c.ci_low,
                "ci_high_raw": c.ci_high,
            }
        )
    return rows


def glm_convergence_line(stats) -> Optional[str]:
    """Format as 'n_iter / max_iter', or None.

    n_iter < max_iter is a HEURISTIC for convergence (spec §2.1) — the widget
    words it as such.
    """
    n, m = getattr(stats, "n_iter", None), getattr(stats, "max_iter", None)
    if n is None or m is None:
        return None
    return f"{n} / {m}"


def importance_entries(stats, top: int = 15):
    """[(name, value)] descending, capped at ``top`` for the bar chart."""
    entries = [(i.name, i.value) for i in getattr(stats, "importances", None) or []]
    return entries[:top]


def load_tab_dist(stats):
    """Rows (distance, perc) of the stored tab_dist.csv, or None if missing."""
    import pandas as pd

    path = getattr(stats, "tab_dist_path", None)
    if not path:
        return None
    try:
        t = pd.read_csv(path)
        return t[["distance", "perc"]].to_dict("records")
    except Exception:
        return None
