"""Solara-free view-model for the model Statistics tab (Spec A §4).

Row/card shaping only — no widgets, no i18n resolution: each row carries a
``key`` that the widget turns into a catalog lookup under the stats namespace.
The widget builds that lookup with an f-string rather than by concatenating a
literal prefix, because tests/test_i18n.py scans for literal translator calls
with a regex and would read the bare prefix as a key of its own. Values arrive
display-ready so the widget never formats numbers.
"""

import math
from typing import List, Optional

DASH = "—"


def _fmt_float(v, digits):
    """Comma-grouped fixed-point rendering of ``v``, never scientific notation.

    Python's ``g`` format switches to scientific once a value's exponent
    reaches ``digits`` (e.g. ``f"{316892.88:.4g}"`` -> ``"3.169e+05"``), which
    is unreadable for the hectare/deviance magnitudes this module actually
    renders. Instead, ``digits`` is spent as significant figures on the
    fractional part: a value whose integer part already has >= ``digits``
    digits gets 0 decimals (rounded to the nearest integer, comma-grouped);
    a smaller value gets enough decimals to keep ``digits`` significant
    figures (so a coefficient like -0.4746 keeps all four). Trailing zeros
    introduced by the fixed decimal count are stripped so "100.000" reads as
    "100", matching the old ``g`` format's habit of dropping them.
    """
    if v == 0:
        return "0"
    magnitude = math.floor(math.log10(abs(v))) + 1
    decimals = max(digits - magnitude, 0)
    s = f"{v:,.{decimals}f}"
    if decimals > 0 and "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _fmt(v, digits=4):
    if v is None:
        return DASH
    if isinstance(v, float):
        if not math.isfinite(v):
            return DASH
        return _fmt_float(v, digits)
    return f"{v:,}" if isinstance(v, int) else str(v)


def stat_cards(model, stats=None) -> List[dict]:
    """Header cards.

    None-valued cards are omitted (spec §4.2); a stored non-finite deviance
    renders as a flagged em-dash, never 'nan'.

    ``stats`` overrides ``model.stats``. Recovered statistics (spec §3) are
    deliberately never written back onto the model — opening a dialog must not
    mutate the project — so without this override the whole recovery path would
    render a populated family panel under a card strip showing only the model's
    own attributes. ``model`` is still needed for deviance/trained_at, which
    live on the model rather than in the stats record.
    """
    if stats is None:
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
        if est is None:
            odds_ratio = DASH
        else:
            try:
                odds_ratio = _fmt(math.exp(est))
            except OverflowError:
                # A finite-but-extreme estimate (quasi/perfect separation is
                # exactly where this happens on rare-event data) still passes
                # the schema's non-finite check, so it must degrade to a dash
                # here rather than crash the whole coefficients table.
                odds_ratio = DASH
        rows.append(
            {
                "name": c.name,
                "estimate": _fmt(est),
                "estimate_raw": est,
                "odds_ratio": odds_ratio,
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
