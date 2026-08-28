"""Solara-free view-model for the model Statistics tab (Spec A §4).

Row/card shaping only — no widgets, no i18n resolution: each row carries a
``key`` that the widget turns into a catalog lookup under the stats namespace.
The widget builds that lookup with an f-string rather than by concatenating a
literal prefix, because tests/test_i18n.py scans for literal translator calls
with a regex and would read the bare prefix as a key of its own. Values arrive
display-ready so the widget never formats numbers.
"""

import math
import re
from datetime import datetime
from typing import List, Optional

DASH = "—"

# patsy's categorical column label, e.g. 'C(subj, levels=[1, 2, 3])[T.9]':
# group 1 is the variable, group 2 the levels list's contents (absent on a
# bare C(x)), group 3 the treatment-coded level (absent on a bare term name).
#
# Only [T.k] treatment columns exist in practice: generate_patsy_formula never
# drops patsy's implicit intercept, so the no-intercept full-dummy [k] columns
# cannot be produced. Should a hand-edited formula ever remove the intercept,
# those names simply fail to match and every column stays its own raw-named
# row — degraded display, never a wrong number.
_CATEGORICAL_WRAPPER = re.compile(
    r"^C\(([^,)]+)(?:,\s*levels=\[([^\]]*)\])?[^)]*\)(?:\[T\.([^]]+)\])?$"
)


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


def _fmt_timestamp(v):
    """``"2026-07-31T17:09:19.272928"`` -> ``"2026-07-31 17:09"``.

    Seconds and microseconds carry no meaning on a "trained at" card and made
    it the widest card in the strip. The pattern stays ISO-ordered rather than
    locale-formatted: the one value is rendered as-is by all four locales, and
    a day/month swap between them would be unreadable (is 07-08 August 7th or
    July 8th?). A value that is not an ISO timestamp passes through untouched,
    so nothing is ever lost to the formatter.
    """
    if v is None:
        return None
    try:
        return datetime.fromisoformat(str(v)).strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return str(v)


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
        add("card_samples", _fmt(stats.n_rows))
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
            add(
                "card_deviance",
                _with_interval(
                    _fmt(float(deviance), digits=6),
                    getattr(stats, "deviance_summary", None),
                    digits=6,
                ),
            )
            null_dev = getattr(stats, "null_deviance", None)
            if null_dev:
                # deforisk's model_deviances.csv figure: how much better than
                # the intercept-only model, as a share of its deviance.
                pct = 100.0 * (1.0 - float(deviance) / null_dev)
                add("card_dev_explained", f"{_fmt(pct, 3)}%")
    if stats is not None:
        add_num("card_dist_thresh", getattr(stats, "dist_thresh", None))
        add_num("card_perc_thresh", getattr(stats, "perc_thresh", None))
        add_num("card_tot_defor", getattr(stats, "tot_defor_ha", None))
        add_num("card_n_classes", getattr(stats, "n_classes", None))
        vrho = getattr(stats, "vrho", None)
        if vrho is not None and vrho.estimate is not None:
            add("card_vrho", _with_interval(_fmt(vrho.estimate), vrho))
        _add_rho_cards(add_num, stats)
    add("card_trained_at", _fmt_timestamp(getattr(model, "trained_at", None)))
    return cards


def _with_interval(value: str, summary, digits=4) -> str:
    """``"31.78 (28 — 36)"`` — a point value plus its 95% credible interval.

    ``summary`` is a Coefficient-shaped record (or None); a missing bound
    keeps the bare point value, so recovered models degrade to the old card.
    The separator is the em dash for the reason ``_add_rho_cards`` records —
    the one glyph that reads as a range and survives lint (an en dash trips
    RUF001, a hyphen collides with a negative bound's minus sign). Between
    two numbers inside parentheses it cannot be misread as DASH's "missing".
    """
    lo = getattr(summary, "ci_low", None)
    hi = getattr(summary, "ci_high", None)
    if lo is None or hi is None:
        return value
    return f"{value} ({_fmt(lo, digits)} {DASH} {_fmt(hi, digits)})"


def _add_rho_cards(add_num, stats):
    """Summary of the iCAR cell-level spatial random effect (spec §4.3).

    The spatial random effect is the entire reason to run iCAR over GLM, so its
    spread is the one number that says whether the spatial term did anything.

    Three cards, not four: ``rho_mean`` is ~0 by construction — an iCAR prior
    is identified only up to a constant, which the intercept absorbs — so a
    mean card would read as information while carrying none.

    Min and max stay two cards rather than one "lo — hi" range string. Every
    other card in this strip is a single value formatted by ``add_num``; a
    composite would need its own separator glyph, and the only glyph that both
    reads as a range and survives lint is the em dash this module already
    spends on "missing" (DASH). Bracketed interval notation was the other
    option and is worse: it would sit two rows above a table whose columns are
    literally CI 2.5%/97.5% and be read as a credible interval, which it is
    not.

    The labels say "cell-level" because that is the provenance: A5 summarises
    ``posteriors["rho"]``, one value per native-``csize`` spatial cell, NOT the
    interpolated rho GeoTIFF. Calling these raster statistics would be a false
    claim — that raster is a bilinear resampling at ``csize_interpolate``.
    """
    add_num("card_rho_min", getattr(stats, "rho_min", None))
    add_num("card_rho_max", getattr(stats, "rho_max", None))
    add_num("card_rho_sd", getattr(stats, "rho_std", None))


def _split_term(name: str):
    """``(variable, level)`` for a stored design-column name.

    ``level`` is None for anything that is not a treatment-coded categorical
    dummy — a continuous predictor, patsy's ``Intercept``, or a ``C(...)``
    wrapper with no ``[T.k]`` suffix. One definition, because the coefficient
    tables and the importance chart have to agree on what a "variable" is.
    """
    m = _CATEGORICAL_WRAPPER.match(name)
    return (name, None) if m is None else (m.group(1), m.group(3))


def coefficient_rows(stats, aggregate: bool = True) -> List[dict]:
    """Display rows for GLM/iCAR coefficient tables.

    odds_ratio is computed here, at display time (spec §2.1) — it is not a
    stored field.

    Stored stats keep the raw patsy column names, one row per treatment-coded
    dummy column, so a categorical arrives as several rows named
    ``C(subj, levels=[...])[T.k]``. Both views rename those; they differ in how
    many rows survive:

    * ``aggregate`` (the default) keeps ONE row per variable. A categorical is
      represented by its STRONGEST CONTRAST — the level whose estimate is
      largest in magnitude — named ``subj (= k)`` and carrying that level's own
      estimate, SD and interval.
    * ``aggregate=False`` keeps every level as its own row, named ``subj = k``.

    Deliberately NOT the sum that ``importance_entries`` takes. An importance is
    a non-negative contribution that decomposes additively over the design
    columns, so summing a categorical's levels answers a real question. These
    numbers are log-odds contrasts against a reference level: their sum is not a
    quantity the model estimates, ``exp()`` of it is not an odds ratio, and the
    SD/interval of a sum is not the sum of the SDs/intervals. Picking one real
    row is the aggregation that stays true.

    Variables keep first-appearance order in both views, so the two are read as
    the same table at two depths rather than as two different tables.
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
        variable, level = _split_term(c.name)
        rows.append(
            {
                "name": variable if level is None else f"{variable} = {level}",
                "variable": variable,
                "level": level,
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
    return rows if not aggregate else _strongest_per_variable(rows)


def _sign_resolved(row: dict) -> bool:
    """True when the interval takes a side of zero.

    Deliberately the same test the effect bar uses for its grey colour, so the
    collapsed slot can never lead with a bar the display itself refuses to
    give a direction.
    """
    lo, hi = row["ci_low_raw"], row["ci_high_raw"]
    return (
        row["estimate_raw"] is not None
        and lo is not None
        and hi is not None
        and not lo < 0 < hi
    )


def _strongest_per_variable(rows: List[dict]) -> List[dict]:
    """One row per variable: the largest |estimate| among resolved contrasts.

    "Resolved" means the interval excludes zero (see ``_sign_resolved``).
    Preferring those keeps the noisiest level — on rare-event data a sparse
    category gets a huge estimate AND a huge interval — from headlining the
    collapsed view on magnitude alone. When nothing is resolved (intervals all
    cross zero, or the model stored none), largest |estimate| decides, so a
    model without intervals behaves as before.

    A variable that contributes a single row is returned untouched, name and
    all — there is no contrast to choose between, so labelling it as one would
    only be noise. A row whose estimate is None loses to any real number and
    wins only against other Nones, so a variable with nothing estimated still
    appears (as a dash) rather than vanishing from the table.
    """
    groups: dict = {}
    for row in rows:
        groups.setdefault(row["variable"], []).append(row)
    out = []
    for candidates in groups.values():
        if len(candidates) == 1:
            out.append(candidates[0])
            continue
        pool = [r for r in candidates if _sign_resolved(r)] or candidates
        best = max(pool, key=lambda r: abs(r["estimate_raw"] or 0.0))
        # The level is named, because "subj" alone would claim to describe the
        # whole variable when it is one contrast out of several.
        out.append({**best, "name": f"{best['variable']} (= {best['level']})"})
    return out


def _mixing_worst_case(params) -> Optional[dict]:
    """Largest split R-hat / smallest ESS over ``params``, or None if none.

    ``bad`` flips at R-hat > 1.1 (the classic Gelman-Rubin threshold; the
    modern 1.01 recommendation would flag nearly every affordable run of this
    sampler) or ESS < 100 (below ~100 the 95% CI bounds shown in the table
    are themselves too noisy to trust).
    """
    rhats = [c.rhat for c in params if c.rhat is not None]
    esss = [c.ess for c in params if c.ess is not None]
    if not rhats or not esss:
        return None
    worst_rhat, worst_ess = max(rhats), min(esss)
    return {
        "rhat": _fmt(worst_rhat, 3),
        "ess": _fmt(worst_ess, 3),
        "bad": worst_rhat > 1.1 or worst_ess < 100,
    }


def icar_convergence_summary(stats) -> Optional[dict]:
    """Per-group MCMC mixing (betas vs Vrho), display-ready, or None.

    ``{"coef": {"rhat", "ess", "warn"} | None, "vrho": {"rhat", "ess",
    "slow"} | None}`` — each group's worst case, never an average: one
    unmixed coefficient invalidates the table.

    The groups are judged SEPARATELY on purpose: Vrho mixes slowly in
    virtually every affordable iCAR run (deforisk's chains behaved the same,
    it just never printed a number), so folding it into one worst-case would
    warn on every fit and train users to ignore the warning. Only the
    coefficient group carries the alarm the panel acts on; a sluggish Vrho is
    reported as its own neutral note (``slow``).

    None when nothing carries diagnostics — a recovered model or a summary
    from before they existed — so the panel stays silent rather than claiming
    convergence it cannot see.
    """
    coef = _mixing_worst_case(getattr(stats, "coefficients", None) or [])
    vrho_param = getattr(stats, "vrho", None)
    vrho = _mixing_worst_case([vrho_param] if vrho_param is not None else [])
    if coef is None and vrho is None:
        return None
    return {
        "coef": {"rhat": coef["rhat"], "ess": coef["ess"], "warn": coef["bad"]}
        if coef
        else None,
        "vrho": {"rhat": vrho["rhat"], "ess": vrho["ess"], "slow": vrho["bad"]}
        if vrho
        else None,
    }


def categorical_references(stats) -> List[tuple]:
    """``(variable, reference_level)`` per categorical, first-appearance order.

    Every categorical estimate in these tables is a contrast AGAINST its
    reference level, and no row names that level — without this the odds-ratio
    column is unreadable. Under patsy's default treatment coding the reference
    is the first entry of the ``levels=[...]`` list the formula stored in the
    column name; a bare ``C(x)`` term carries no domain, so no reference can
    be claimed for it and it is skipped rather than guessed at.
    """
    refs = []
    seen = set()
    for c in getattr(stats, "coefficients", None) or []:
        m = _CATEGORICAL_WRAPPER.match(c.name)
        if m is None or m.group(2) is None or m.group(3) is None:
            continue
        variable = m.group(1)
        if variable in seen:
            continue
        seen.add(variable)
        reference = m.group(2).split(",")[0].strip().strip("'\"")
        refs.append((variable, reference))
    return refs


def glm_convergence_line(stats) -> Optional[str]:
    """Format as 'n_iter / max_iter', or None.

    n_iter < max_iter is a HEURISTIC for convergence (spec §2.1) — the widget
    words it as such.
    """
    n, m = getattr(stats, "n_iter", None), getattr(stats, "max_iter", None)
    if n is None or m is None:
        return None
    return f"{n} / {m}"


def importance_entries(stats, top: int = 15, aggregate: bool = True):
    """[(name, value)] descending, capped at ``top`` for the bar chart.

    Stored stats keep the raw patsy column names — one row per
    treatment-coded dummy column for a categorical. With ``aggregate``
    (the default) those rows are summed into one per variable, named
    plainly (`C(subj, levels=[...])[T.k]` -> `subj`) — a categorical's
    importance is only comparable to a continuous variable's as that sum.
    With ``aggregate=False`` each level keeps its own row (`subj = k`),
    the drill-down that shows WHICH category carries the importance.

    The sum is what makes this different from ``coefficient_rows``' aggregation,
    which picks one level instead — see there for why a sum is right here and
    wrong for a coefficient.
    """
    summed: dict = {}
    for i in getattr(stats, "importances", None) or []:
        variable, level = _split_term(i.name)
        if level is None or aggregate:
            name = variable
        else:
            name = f"{variable} = {level}"
        summed[name] = summed.get(name, 0.0) + i.value
    entries = sorted(summed.items(), key=lambda e: e[1], reverse=True)
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
