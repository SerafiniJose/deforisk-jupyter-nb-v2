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
from gui.scripts.allocation_runner import AllocationForm, validate_form
from gui.tile.evaluation_helpers import map_items
from gui.widget.creation_dialog import CreationDialog

logger = logging.getLogger("spatial_risk")

_RASTER_EXTENSIONS = [".tif", ".tiff", ".vrt"]
_TABLE_EXTENSIONS = [".csv"]
_VECTOR_EXTENSIONS = [".gpkg", ".shp", ".geojson", ".json"]


def _as_float(text):
    """Parse a numeric field, or None when blank or not a number."""
    try:
        return float(str(text).strip())
    except (TypeError, ValueError):
        return None


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
    external_map, set_external_map = solara.use_state("")
    defrate_override, set_defrate_override = solara.use_state("")
    borders, set_borders = solara.use_state("")
    mask, set_mask = solara.use_state("")
    juris_ha, set_juris_ha = solara.use_state("")
    years, set_years = solara.use_state("4")
    density, set_density = solara.use_state(False)

    def build_form():
        return AllocationForm(
            name=name,
            prediction_key=pred_key,
            external_riskmap=str(external_map) if external_map else None,
            user_defrate_path=str(defrate_override) if defrate_override else None,
            borders_file=str(borders) if borders else None,
            mask_file=str(mask) if mask else None,
            defor_juris_ha=_as_float(juris_ha),
            years_forecast=_as_float(years),
            density_map=density,
        )

    def validate():
        return validate_form(p, build_form())

    def launch():
        on_launch(build_form())

    def reset():
        set_name("")
        set_pred_key(None)
        set_external_map("")
        set_defrate_override("")
        set_borders("")
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

        # An external map is the alternative to a project prediction; it needs
        # its own rate table, since nothing in the project describes it.
        if not pred_key:
            FileInputComponent(
                label=t("toolbox.allocation.field_riskmap_external"),
                value=external_map,
                on_value=set_external_map,
                sepal_client=sepal_client,
                root="",
                extensions=_RASTER_EXTENSIONS,
                clearable=True,
            )

        FileInputComponent(
            label=(
                t("toolbox.allocation.field_defrate")
                if not pred_key
                else t("toolbox.allocation.field_defrate_override")
            ),
            value=defrate_override,
            on_value=set_defrate_override,
            sepal_client=sepal_client,
            root="",
            extensions=_TABLE_EXTENSIONS,
            clearable=True,
        )
        if pred_key and not defrate_override:
            solara.Text(
                t("toolbox.allocation.field_defrate_auto"),
                style="font-size:0.75rem;opacity:0.7;",
            )

        FileInputComponent(
            label=t("toolbox.allocation.field_borders"),
            value=borders,
            on_value=set_borders,
            sepal_client=sepal_client,
            root="",
            extensions=_VECTOR_EXTENSIONS,
            clearable=True,
        )

        FileInputComponent(
            label=t("toolbox.allocation.field_mask"),
            value=mask,
            on_value=set_mask,
            sepal_client=sepal_client,
            root="",
            extensions=_RASTER_EXTENSIONS,
            clearable=True,
        )
        if not mask:
            solara.Text(
                t("toolbox.allocation.field_mask_none"),
                style="font-size:0.75rem;opacity:0.7;",
            )

        rv.TextField(
            label=t("toolbox.allocation.field_juris_ha"),
            v_model=juris_ha,
            on_v_model=set_juris_ha,
            type="number",
            dense=True,
            outlined=True,
        )
        rv.TextField(
            label=t("toolbox.allocation.field_years"),
            v_model=years,
            on_v_model=set_years,
            type="number",
            dense=True,
            outlined=True,
        )

        rv.Checkbox(
            label=t("toolbox.allocation.field_density"),
            v_model=density,
            on_v_model=set_density,
            dense=True,
        )
        if density:
            solara.Warning(t("toolbox.allocation.density_warning"), dense=True)
