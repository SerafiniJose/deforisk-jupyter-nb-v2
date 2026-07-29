"""Form dialog for a deforestation-allocation run.

Follows the form idioms of ``model_form_dialog.py``: a ``CreationDialog`` frame
owns the Create flow (validate → launch → close), the fields are ``rv`` inputs
bound through ``v_model``/``on_v_model``, and local paths are picked with
pysepal's ``FileInputComponent`` rather than typed into a bare text field.
"""

import logging

import reacton.ipyvuetify as rv
import solara
from pysepal.solara.components.inputs import FileInputComponent

from gui.i18n import t
from gui.scripts.allocation_runner import (
    AllocationForm,
    mask_items,
    preview_defrate_source,
    validate_form,
)
from gui.tile.evaluation_helpers import map_items
from gui.widget.borders_picker import BordersPicker
from gui.widget.creation_dialog import CreationDialog
from gui.widget.text_style import MUTED, TIGHT_FIELD, FieldHint

logger = logging.getLogger("spatial_risk")

_TABLE_EXTENSIONS = [".csv"]

_HINT = MUTED + "font-size:0.75rem;"

# Rate-table modes: resolve automatically from the prediction (default), or
# let the user pick a table file.
_DEFRATE_AUTO = "auto"
_DEFRATE_CUSTOM = "custom"


def _as_float(text):
    """Parse a numeric field, or None when blank or not a number."""
    try:
        return float(str(text).strip())
    except (TypeError, ValueError):
        return None


@solara.component
def DefrateResolutionHint(project_value, pred_key, override):
    """Eager preview of the rate table the run will use.

    The visible half of "auto-resolved": provenance chip + file name for a
    table that exists, "will be computed" for FAR families, and the resolver's
    reason when nothing resolves — shown at form time, not after the run.
    """
    src = preview_defrate_source(project_value, pred_key, user_path=override or None)

    if src.provenance == "unavailable":
        solara.Warning(src.caveat or "", dense=True)
        return

    will_compute = src.path is not None and not src.path.exists()
    with FieldHint():
        with solara.Row(style="gap:8px;align-items:center;flex-wrap:wrap;"):
            # 'Computed' needs no provenance chip: its description already says
            # where the table comes from. The other provenances name a file, so
            # the chip is what tells persisted/sibling/user tables apart.
            if src.provenance != "computed":
                provenance_key = (
                    f"toolbox.allocation.provenance_{src.provenance}".replace("-", "_")
                )
                rv.Chip(
                    small=True,
                    outlined=True,
                    color="primary",
                    children=[t(provenance_key)],
                )
            solara.Text(
                t("toolbox.allocation.defrate_will_compute")
                if will_compute
                else src.path.name,
                style=_HINT,
            )
    if src.caveat:  # the JNR observed-rates caveat
        solara.Warning(src.caveat, dense=True)


@solara.component
def AllocationFormDialog(open_, project, on_launch, on_close, sepal_client=None):
    """Collect the allocation inputs; hand a validated AllocationForm to on_launch.

    Args:
        open_: solara.Reactive[bool] — dialog visibility (owned by the tile).
        project: solara.Reactive[Project] — source of the prediction list.
        on_launch: callback(AllocationForm) — start the run.
        on_close: callback() — the tile closes the dialog.
        sepal_client: passed through to the file pickers.
    """
    p = project.value

    name, set_name = solara.use_state("")
    pred_key, set_pred_key = solara.use_state(None)
    defrate_mode, set_defrate_mode = solara.use_state(_DEFRATE_AUTO)
    defrate_override, set_defrate_override = solara.use_state("")
    borders, set_borders = solara.use_state(None)
    mask, set_mask = solara.use_state("")
    juris_ha, set_juris_ha = solara.use_state("")
    years, set_years = solara.use_state("4")
    density, set_density = solara.use_state(False)

    custom_table = defrate_mode == _DEFRATE_CUSTOM

    def build_form():
        return AllocationForm(
            name=name,
            prediction_key=pred_key,
            user_defrate_path=(
                str(defrate_override) if custom_table and defrate_override else None
            ),
            borders=borders,
            mask_file=str(mask) if mask else None,
            defor_juris_ha=_as_float(juris_ha),
            years_forecast=_as_float(years),
            density_map=density,
        )

    def validate():
        if custom_table and not defrate_override:
            # Silent fallback to auto-resolution would betray the visible mode.
            return "Choose the rate-table file, or switch back to automatic resolution."
        return validate_form(p, build_form())

    def launch():
        on_launch(build_form())

    def reset():
        set_name("")
        set_pred_key(None)
        set_defrate_mode(_DEFRATE_AUTO)
        set_defrate_override("")
        set_borders(None)
        set_mask("")
        set_juris_ha("")
        set_years("4")
        set_density(False)
        on_close()

    items = map_items(p) if p is not None else []

    with CreationDialog(
        open_=open_,
        title=t("toolbox.allocation.title"),
        create_label=t("toolbox.allocation.run"),
        validate=validate,
        # History-safe storage keys (name + run id): runs never replace each
        # other, so the overwrite branch of the Create flow never triggers.
        will_replace=lambda: None,
        launch=launch,
        on_close=reset,
        max_width="620px",
    ):
        rv.TextField(
            label=t("toolbox.allocation.field_name"),
            v_model=name,
            on_v_model=set_name,
            dense=True,
            outlined=True,
        )

        if items:
            rv.Select(
                label=t("toolbox.allocation.field_riskmap"),
                items=items,
                item_text="text",
                item_value="value",
                v_model=pred_key,
                on_v_model=set_pred_key,
                dense=True,
                outlined=True,
                clearable=True,
            )
        else:
            solara.Info(t("toolbox.allocation.no_predictions"))

        with solara.Div(classes=[TIGHT_FIELD]):
            rv.Select(
                label=t("toolbox.allocation.field_defrate"),
                items=[
                    {
                        "text": t("toolbox.allocation.defrate_option_auto"),
                        "value": _DEFRATE_AUTO,
                    },
                    {
                        "text": t("toolbox.allocation.defrate_option_custom"),
                        "value": _DEFRATE_CUSTOM,
                    },
                ],
                item_text="text",
                item_value="value",
                v_model=defrate_mode,
                on_v_model=lambda v: set_defrate_mode(v or _DEFRATE_AUTO),
                dense=True,
                outlined=True,
            )
        if custom_table:
            with solara.Div(classes=[TIGHT_FIELD]):
                FileInputComponent(
                    label=t("toolbox.allocation.field_defrate_override"),
                    value=defrate_override,
                    on_value=set_defrate_override,
                    sepal_client=sepal_client,
                    root="",
                    extensions=_TABLE_EXTENSIONS,
                    clearable=True,
                )
        if pred_key or (custom_table and defrate_override):
            DefrateResolutionHint(
                project_value=p,
                pred_key=pred_key,
                override=defrate_override if custom_table else "",
            )

        BordersPicker(
            value=borders,
            on_value=set_borders,
            sepal_client=sepal_client,
        )

        # The mask is one of the project's processed rasters (Hansen forest &
        # co.), not a free file: everything the risk map was built from is
        # already registered, so offer exactly that.
        mask_choices = mask_items(p)
        if mask_choices:
            with solara.Div(classes=[TIGHT_FIELD]):
                rv.Select(
                    label=t("toolbox.allocation.field_mask"),
                    items=mask_choices,
                    item_text="text",
                    item_value="value",
                    v_model=mask or None,
                    on_v_model=lambda v: set_mask(v or ""),
                    dense=True,
                    outlined=True,
                    clearable=True,
                )
        else:
            FieldHint(
                children=[
                    solara.Text(t("toolbox.allocation.field_mask_empty"), style=_HINT)
                ]
            )
        if not mask:
            FieldHint(
                children=[
                    solara.Text(t("toolbox.allocation.field_mask_none"), style=_HINT)
                ]
            )

        with solara.Row(style="gap:12px;"):
            rv.TextField(
                label=t("toolbox.allocation.field_juris_ha"),
                v_model=juris_ha,
                on_v_model=set_juris_ha,
                type="number",
                dense=True,
                outlined=True,
                style_="flex:2;",
            )
            rv.TextField(
                label=t("toolbox.allocation.field_years"),
                v_model=years,
                on_v_model=set_years,
                type="number",
                dense=True,
                outlined=True,
                style_="flex:1;",
            )

        rv.Checkbox(
            label=t("toolbox.allocation.field_density"),
            v_model=density,
            on_v_model=set_density,
            dense=True,
        )
        if density:
            solara.Warning(t("toolbox.allocation.density_warning"), dense=True)
