"""Read-only, tabbed Project Summary popup (left-drawer dialog content)."""

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.scripts.summary_helpers import project_overview
from gui.widget.summary_lists import (
    RawVariablesSummary,
    ProcessedVariablesSummary,
    DatasetsSummary,
    SamplesSummary,
    ModelsSummary,
    PredictionsSummary,
    EvaluationsSummary,
)

# (i18n key for tab label, renderer) — order is the displayed tab order.
_TABS = [
    ("tiles.summary.tab_raw_variables", RawVariablesSummary),
    ("tiles.summary.tab_processed_variables", ProcessedVariablesSummary),
    ("tiles.summary.tab_datasets", DatasetsSummary),
    ("tiles.summary.tab_samples", SamplesSummary),
    ("tiles.summary.tab_trained_models", ModelsSummary),
    ("tiles.summary.tab_predictions", PredictionsSummary),
    ("tiles.summary.tab_evaluations", EvaluationsSummary),
]


@solara.component
def ProjectSummaryTile(project, project_dirty=None, last_saved=None):
    """Read-only overview of every component registered on the current project."""
    p = project.value
    active_tab, set_active_tab = solara.use_state(0)

    if p is None:
        solara.Info(t("tiles.summary.error_no_project"))
        return

    dirty = project_dirty.value if project_dirty is not None else False
    ls = last_saved.value if last_saved is not None else None
    ov = project_overview(p, last_saved=ls, dirty=dirty)
    c = ov["counts"]

    with solara.Column(style="gap:12px;"):
        # Project header (read-only context)
        with solara.Row(style="gap:8px;align-items:center;"):
            solara.Text(str(ov["project_name"]), style="font-weight:600;font-size:1.05rem;")
            rv.Chip(
                children=[t("project.chip_unsaved") if dirty else t("project.chip_saved")],
                color="amber" if dirty else "green",
                text_color="white",
                x_small=True,
            )
        meta = []
        if ov["aoi_name"]:
            meta.append(f"AOI: {ov['aoi_name']}")
        if ov["years"]:
            meta.append("years: " + ", ".join(str(y) for y in ov["years"]))
        meta.append(
            f"{c['raw']} raw · {c['processed']} processed · {c['datasets']} datasets · "
            f"{c['samples']} samples · {c['models']} models · "
            f"{c['predictions']} predictions · {c['evaluations']} evaluations"
        )
        solara.Text(" — ".join(meta), style="font-size:0.8rem;color:grey;")

        # One tab per component type
        with rv.Tabs(v_model=active_tab, on_v_model=set_active_tab, grow=False, show_arrows=True):
            for label_key, _ in _TABS:
                rv.Tab(children=[t(label_key)])
        with rv.TabsItems(v_model=active_tab):
            for _, renderer in _TABS:
                with rv.TabItem():
                    renderer(p)
