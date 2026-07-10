"""Step 4 — Dataset tile."""

import logging

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.store.state_manager import app_state
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.dataset_list import DatasetList
from gui.widget.help import InfoButton
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
        # Persist the removal to disk (matches delete_sample/delete_prediction
        # auto_save behaviour) so it isn't silently resurrected on reload.
        p.save()
        project.set(p.model_copy())

    def on_register():
        set_form_error(None)
        set_form_success(None)
        if p is None:
            set_form_error(t("tiles.dataset.error_no_project"))
            return
        if not ds_name.strip():
            set_form_error(t("tiles.dataset.error_dataset_name_required"))
            return
        if not target_name:
            set_form_error(t("tiles.dataset.error_target_required"))
            return
        if not feature_names:
            set_form_error(t("tiles.dataset.error_features_required"))
            return
        try:
            # Store year for feature alignment, but only pass it to set_target for
            # temporal targets (set_target rejects a year for static targets).
            ds = Dataset(project=p, name=ds_name.strip(), year=year)
            target_is_temporal = p.is_temporal(target_name)
            ds.set_target(target_name, year=year if target_is_temporal else None)
            ds.set_features(feature_names)
            # Validate before registering — confirms variable rasters exist on disk
            # and temporal years align. (Replaces the standalone Validate button.)
            ds.validate()

            key = editing_key if editing_key else ds_name.strip()
            # Remove old key if renaming during edit
            if editing_key and editing_key != key and editing_key in p.datasets:
                del p.datasets[editing_key]
            # Persist to the project JSON immediately so the dataset survives a
            # reload without a manual Save — matches the auto_save behaviour of
            # add_sample/add_model/add_prediction in the other workflow tiles.
            p.add_dataset(ds, key=key, auto_save=True)

            logger.debug("Registered dataset '%s' with %d features", key, len(feature_names))
            reset_form()
            set_form_success(t("tiles.dataset.success_registered", key=key))
            project.set(p.model_copy())
        except Exception as exc:
            logger.exception("on_register failed")
            set_form_error(t("tiles.dataset.error_registration_failed", exc=exc))

    has_processed = p is not None and bool(p.processed_variables)

    with solara.Column(style="gap:16px;"):
        solara.Markdown(t("tiles.dataset.header"))
        with solara.Row(style="gap:4px;align-items:center;"):
            solara.Text(t("tiles.dataset.description"))
            InfoButton(t("tiles.dataset.info_header"), t("tiles.dataset.info_md"))

        if not has_processed:
            solara.Info(t("tiles.dataset.error_no_processed"))
            return

        # Create / Edit form
        solara.Markdown(
            t("tiles.dataset.form_header_new") if not editing_key
            else t("tiles.dataset.form_header_edit", key=editing_key)
        )

        rv.TextField(
            label=t("tiles.dataset.dataset_name_label"),
            v_model=ds_name,
            on_v_model=set_ds_name,
            dense=True,
            outlined=True,
            placeholder=t("tiles.dataset.dataset_name_placeholder"),
        )

        # Target
        rv.Select(
            label=t("tiles.dataset.target_variable_label"),
            items=available_vars,
            v_model=target_name,
            on_v_model=set_target_name,
            dense=True,
            outlined=True,
            hint=t("tiles.dataset.target_hint"),
            persistent_hint=True,
        )

        # Features (multi-select)
        rv.Select(
            label=t("tiles.dataset.feature_variables_label"),
            items=feature_options,
            v_model=feature_names,
            on_v_model=set_feature_names,
            multiple=True,
            dense=True,
            outlined=True,
            chips=True,
            small_chips=True,
            deletable_chips=True,
            hint=t("tiles.dataset.features_hint"),
            persistent_hint=True,
        )

        # Year (shown if temporal vars detected)
        if available_years:
            rv.Select(
                label=t("tiles.dataset.year_label"),
                items=available_years,
                v_model=year,
                on_v_model=set_year,
                dense=True,
                outlined=True,
                hint=t("tiles.dataset.year_hint"),
                persistent_hint=True,
            )

        # Action buttons
        with solara.Row(style="gap:8px;align-items:center;"):
            solara.Button(
                t("tiles.dataset.register_button"),
                icon_name="mdi-database-plus",
                color="primary",
                small=True,
                on_click=on_register,
            )
            if editing_key:
                solara.Button(
                    t("common.cancel"),
                    on_click=reset_form,
                    text=True,
                    small=True,
                )
            # Push Clear to the right; resets all form inputs.
            rv.Spacer()
            solara.Button(
                t("tiles.dataset.clear_button"),
                icon_name="mdi-eraser",
                text=True,
                small=True,
                on_click=reset_form,
            )

        if form_error:
            rv.Alert(type_="error", dense=True, children=[form_error])
        if form_success:
            rv.Alert(type_="success", dense=True, children=[form_success])

        # Existing datasets — shown below the form, matching the other tabs
        # (Train/Sampling/Inference all render their results list at the bottom).
        if p and p.datasets:
            # DatasetList renders its own collapsible ProductTable header.
            DatasetList(project=project, on_edit=on_edit, on_remove=set_pending_remove)

        ConfirmDialog(
            open=pending_remove is not None,
            on_cancel=lambda: set_pending_remove(None),
            on_confirm=lambda: (_do_remove(pending_remove), set_pending_remove(None)),
            title=t("tiles.dataset.confirm_remove_title"),
            message=t("tiles.dataset.confirm_remove_message", name=pending_remove or ""),
            confirm_label=t("common.remove"),
        )
