"""Step 4 — Dataset tile."""

import logging

import reacton.ipyvuetify as rv
import solara

from gui.store.state_manager import app_state
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.dataset_list import DatasetList
from spatialrisk.dataset import Dataset

logger = logging.getLogger("spatial_risk")


def _get_available_vars(project):
    """Return list of unique variable names from processed_variables."""
    if project is None:
        return []
    return project.list_unique_variable_names(source="processed")


def _get_available_years(project, var_names):
    """Return sorted intersection of years available across given temporal variable names."""
    if project is None or not var_names:
        return []
    year_sets = []
    for name in var_names:
        if project.is_temporal(name, source="processed"):
            years = project.get_variable_years(name, source="processed")
            if years:
                year_sets.append(set(years))
    if not year_sets:
        return []
    return sorted(set.intersection(*year_sets))


@solara.component
def DatasetTile(project):
    """Dataset configuration step: create, validate, and register datasets."""
    p = project.value

    # Form state
    ds_name, set_ds_name = solara.use_state("")
    target_name, set_target_name = solara.use_state("")
    feature_names, set_feature_names = solara.use_state([])
    year, set_year = solara.use_state(None)
    editing_key, set_editing_key = solara.use_state(None)
    form_error, set_form_error = solara.use_state(None)
    form_success, set_form_success = solara.use_state(None)

    available_vars = _get_available_vars(p) if p else []
    # Features exclude the selected target
    feature_options = [v for v in available_vars if v != target_name]
    # Available years for selected target + features
    selected_temporal = [target_name] + feature_names if target_name else feature_names
    available_years = _get_available_years(p, selected_temporal) if p else []

    def reset_form():
        set_ds_name("")
        set_target_name("")
        set_feature_names([])
        set_year(None)
        set_editing_key(None)
        set_form_error(None)
        set_form_success(None)

    def on_edit(key):
        if p is None or key not in p.datasets:
            return
        ds = p.datasets[key]
        set_editing_key(key)
        set_ds_name(ds.name or key)
        set_target_name(ds.target.name if ds.target else "")
        set_feature_names([f.name for f in ds.features])
        set_year(ds.year)
        set_form_error(None)
        set_form_success(None)

    pending_remove, set_pending_remove = solara.use_state(None)

    def _do_remove(key):
        if p is None or key not in p.datasets:
            return
        del p.datasets[key]
        project.set(p.model_copy())

    def on_validate():
        set_form_error(None)
        set_form_success(None)
        if p is None:
            set_form_error("No active project.")
            return
        if not ds_name.strip():
            set_form_error("Dataset name is required.")
            return
        if not target_name:
            set_form_error("Select a target variable.")
            return
        if not feature_names:
            set_form_error("Select at least one feature variable.")
            return
        try:
            # year is stored on the dataset for temporal feature alignment.
            ds = Dataset(project=p, name=ds_name.strip(), year=year)
            # Only pass the year to set_target when the target itself is temporal,
            # since set_target rejects a year argument for static targets.
            target_is_temporal = p.is_temporal(target_name)
            ds.set_target(target_name, year=year if target_is_temporal else None)
            ds.set_features(feature_names)
            ds.validate()
            set_form_success("Dataset is valid.")
        except Exception as exc:
            set_form_error(str(exc))

    def on_register():
        set_form_error(None)
        set_form_success(None)
        if p is None:
            set_form_error("No active project.")
            return
        if not ds_name.strip():
            set_form_error("Dataset name is required.")
            return
        if not target_name:
            set_form_error("Select a target variable.")
            return
        if not feature_names:
            set_form_error("Select at least one feature variable.")
            return
        try:
            # See on_validate: store year for feature alignment, but only pass it
            # to set_target for temporal targets.
            ds = Dataset(project=p, name=ds_name.strip(), year=year)
            target_is_temporal = p.is_temporal(target_name)
            ds.set_target(target_name, year=year if target_is_temporal else None)
            ds.set_features(feature_names)

            key = editing_key if editing_key else ds_name.strip()
            # Remove old key if renaming during edit
            if editing_key and editing_key != key and editing_key in p.datasets:
                del p.datasets[editing_key]
            p.datasets[key] = ds

            logger.debug("Registered dataset '%s' with %d features", key, len(feature_names))
            reset_form()
            set_form_success(f"Dataset '{key}' registered.")
            project.set(p.model_copy())
        except Exception as exc:
            logger.exception("on_register failed")
            set_form_error(str(exc))

    has_processed = p is not None and bool(p.processed_variables)

    with solara.Column(style="gap:16px;"):
        solara.Markdown("### Step 4 — Dataset")
        solara.Text(
            "Bundle a target and feature variables into a dataset for model training."
        )

        if not has_processed:
            solara.Info("Run Step 3 — Process first.")
            return

        # Existing datasets
        if p and p.datasets:
            solara.Markdown(f"**DATASETS** ({len(p.datasets)})")
            DatasetList(project=project, on_edit=on_edit, on_remove=set_pending_remove)

        # Create / Edit form
        solara.Markdown("**NEW DATASET**" if not editing_key else f"**EDIT — {editing_key}**")

        rv.TextField(
            label="Dataset name",
            v_model=ds_name,
            on_v_model=set_ds_name,
            dense=True,
            outlined=True,
            placeholder="e.g. calibration_2020",
        )

        # Target
        rv.Select(
            label="Target variable",
            items=available_vars,
            v_model=target_name,
            on_v_model=set_target_name,
            dense=True,
            outlined=True,
        )

        # Features (multi-select)
        rv.Select(
            label="Feature variables",
            items=feature_options,
            v_model=feature_names,
            on_v_model=set_feature_names,
            multiple=True,
            dense=True,
            outlined=True,
            chips=True,
            small_chips=True,
            deletable_chips=True,
        )

        # Year (shown if temporal vars detected)
        if available_years:
            rv.Select(
                label="Year (temporal alignment)",
                items=available_years,
                v_model=year,
                on_v_model=set_year,
                dense=True,
                outlined=True,
            )

        # Action buttons
        with solara.Row(style="gap:8px;align-items:center;"):
            solara.Button(
                "Validate",
                icon_name="mdi-check-circle-outline",
                color="primary",
                outlined=True,
                small=True,
                on_click=on_validate,
            )
            solara.Button(
                "Register",
                icon_name="mdi-database-plus",
                color="primary",
                small=True,
                on_click=on_register,
            )
            if editing_key:
                solara.Button(
                    "Cancel",
                    on_click=reset_form,
                    text=True,
                    small=True,
                )
            # Push Clear to the right; resets all form inputs.
            rv.Spacer()
            solara.Button(
                "Clear",
                icon_name="mdi-eraser",
                text=True,
                small=True,
                on_click=reset_form,
            )

        if form_error:
            rv.Alert(type_="error", dense=True, children=[form_error])
        if form_success:
            rv.Alert(type_="success", dense=True, children=[form_success])

        ConfirmDialog(
            open=pending_remove is not None,
            on_cancel=lambda: set_pending_remove(None),
            on_confirm=lambda: (_do_remove(pending_remove), set_pending_remove(None)),
            title="Remove dataset?",
            message=f"Remove dataset '{pending_remove}' from this project? This cannot be undone.",
            confirm_label="Remove",
        )
