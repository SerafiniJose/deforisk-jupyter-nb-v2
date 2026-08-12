"""Statistics tab content for the model details dialog (Spec A §4).

Every number here is in-sample — the caveat strip states that first. Stats
come from model.stats when present; otherwise a background recovery attempt
runs via solara.use_task (an RF pickle is tens of MB and rebuilding design
info re-reads the training CSV — never on the render thread, see
solara-event-handlers-block-websocket-loop). Recovery is read-only: this
panel NEVER assigns model.stats or saves the project.

Card labels are resolved by catalog convention under ``tiles.train.stats``.
The lookup is built with an f-string, exactly like the per-parameter hints in
model_form_dialog: tests/test_i18n.py scans for literal translator calls with a
regex, and a literal prefix concatenated with a variable is picked up as if the
prefix itself were a key.
"""

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.scripts.model_stats_view import stat_cards
from gui.widget.text_style import MUTED

_CARD_STYLE = (
    "border:1px solid rgba(128,128,128,0.25);border-radius:4px;"
    "padding:8px 12px;min-width:110px;"
)


@solara.component
def _StatCards(model):
    """Header cards — one per non-None statistic (the view-model omits the rest)."""
    cards = stat_cards(model)
    with solara.Row(gap="8px", style="flex-wrap:wrap;"):
        for card in cards:
            with solara.Column(style=_CARD_STYLE, gap="0px"):
                solara.Text(
                    t(f"tiles.train.stats.{card['key']}"),
                    style=MUTED + "font-size:0.68rem;text-transform:uppercase;"
                    "letter-spacing:0.06em;",
                )
                solara.Text(
                    str(card["value"]),
                    style=(
                        "font-size:1.0rem;font-weight:600;"
                        + ("color:var(--v-error-base);" if card.get("warn") else "")
                    ),
                )


@solara.component
def ModelStatsPanel(model):
    """Caveat + stat cards + family panel (A11) with pending/empty states."""
    # Hooks run unconditionally — the task itself decides whether to work.
    # use_task lives in solara.lab; .pending/.finished/.value per the
    # solara-task-pending-and-error-gotchas memory (.error is a bool, the
    # exception is on .exception).
    # raise_error=False keeps stats_recovery's own contract intact end to end:
    # it promises the caller an empty state rather than an error, and the
    # default (True) would re-raise inside the render instead, taking the whole
    # details dialog down. An errored task is neither pending nor finished, so
    # it falls through to the empty state below.
    recovered = solara.lab.use_task(
        lambda: _recover(model),
        dependencies=[
            getattr(model, "name", None),
            getattr(model, "trained_at", None),
            getattr(model, "stats", None) is not None,
        ],
        raise_error=False,
    )
    stats = getattr(model, "stats", None)
    if stats is None and recovered.finished and recovered.value is not None:
        stats = recovered.value

    with solara.Column(gap="12px"):
        with solara.Row(gap="8px", style="align-items:flex-start;"):
            solara.Info(t("tiles.train.stats.caveat_training_fit"), dense=True)
        _StatCards(model=model)
        if stats is not None:
            _FamilyPanel(model=model, stats=stats)
        elif getattr(model, "stats", None) is None and recovered.pending:
            with solara.Row(gap="8px"):
                rv.ProgressCircular(indeterminate=True, size=18, width=2)
                solara.Text(t("tiles.train.stats.recovering"), style=MUTED)
        else:
            solara.Info(t("tiles.train.stats.empty_state"))
            if getattr(model, "model_type", "") == "icar":
                solara.Text(t("tiles.train.stats.icar_retrain_hint"), style=MUTED)


def _recover(model):
    """Best-effort disk recovery, off the render thread. Never writes."""
    if getattr(model, "stats", None) is not None:
        return None  # nothing to do — fit-time stats win
    from spatialrisk.mlmodels.stats_recovery import recover_stats

    return recover_stats(model)


@solara.component
def _FamilyPanel(model, stats):
    """Family-specific body — filled in by Task A11.

    Keeping the seam here lets A10 ship with cards + states only.
    """
    solara.Text("")
