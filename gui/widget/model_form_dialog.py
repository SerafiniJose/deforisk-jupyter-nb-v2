"""New Model dialog for the Train step."""

from typing import Callable

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.scripts.artifact_names import sanitize_key, suggest_version
from gui.scripts.model_registry import MODEL_KEYS, MODEL_REGISTRY
from gui.widget.artifact_name_field import ArtifactNameField, use_artifact_name
from gui.widget.creation_dialog import CreationDialog
from gui.widget.help import InfoButton


def model_label(key: str) -> str:
    """Resolve a model's display label at render time."""
    return t(MODEL_REGISTRY[key]["label_key"])


def model_labels() -> list:
    """Resolve all model display labels at render time."""
    return [model_label(k) for k in MODEL_KEYS]


def _default_params(model_key: str) -> dict:
    """Return default parameter values for a model."""
    return {p["key"]: p["default"] for p in MODEL_REGISTRY[model_key]["params"]}


def _make_param_component(model_key: str, group: str):
    """Factory: dedicated component for one model's params (see train_tile
    history: one component *type* per (model, group) so switching models does a
    clean unmount/mount instead of child-tree reconciliation)."""
    param_defs = [
        p for p in MODEL_REGISTRY[model_key]["params"]
        if p.get("group", "params") == group
    ]
    is_variables = group == "variables"

    @solara.component
    def _Params(params: dict, set_params, feature_options=None):
        def _update(param_key, value):
            set_params({**params, param_key: value})

        for param_def in param_defs:
            pkey = param_def["key"]
            current = params.get(pkey, param_def["default"])
            ptype = param_def["type"]
            # Per-parameter help resolved by catalog convention (enforced by
            # tests/test_i18n.py).
            hint = t(f"models.{model_key}.params.{pkey}.hint")

            if is_variables:
                rv.Select(
                    label=t(param_def["label_key"]),
                    items=feature_options or [],
                    v_model=current or None,
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True, outlined=True, clearable=True,
                    no_data_text=t("tiles.train.variables_select_no_data"),
                    hint=hint, persistent_hint=True,
                )
            elif ptype == "select":
                rv.Select(
                    label=t(param_def["label_key"]),
                    items=param_def.get("items", []),
                    v_model=current,
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True, outlined=True,
                    hint=hint, persistent_hint=True,
                )
            else:
                rv.TextField(
                    label=t(param_def["label_key"]),
                    v_model=str(current) if current is not None else "",
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True, outlined=True,
                    type_="number" if ptype in ("int", "float") else None,
                    hint=hint, persistent_hint=True,
                )

    _Params.__name__ = f"Params_{model_key}_{group}"
    _Params.__qualname__ = f"Params_{model_key}_{group}"
    return _Params


PARAM_COMPONENTS = {k: _make_param_component(k, "params") for k in MODEL_KEYS}
VARIABLE_COMPONENTS = {k: _make_param_component(k, "variables") for k in MODEL_KEYS}
MODEL_HAS_VARIABLES = {
    k: any(p.get("group") == "variables" for p in MODEL_REGISTRY[k]["params"])
    for k in MODEL_KEYS
}
MODEL_HAS_PARAMS = {
    k: any(p.get("group", "params") == "params" for p in MODEL_REGISTRY[k]["params"])
    for k in MODEL_KEYS
}

# Restyle the Advanced-parameters panel to sit in the form's flow: same
# border/height/label colour as the outlined dense fields, no 24px inset.
_ADVANCED_PANEL_CSS = """
.advanced-params .v-expansion-panel { border: 1px solid rgba(0, 0, 0, .38); border-radius: 4px; }
.theme--dark .advanced-params .v-expansion-panel { border-color: rgba(255, 255, 255, .24); }
.advanced-params .v-expansion-panel::before { box-shadow: none; }
.advanced-params .v-expansion-panel-header { min-height: 40px; padding: 0 12px; font-size: 14px; color: rgba(0, 0, 0, .6); }
.theme--dark .advanced-params .v-expansion-panel-header { color: rgba(255, 255, 255, .7); }
.advanced-params .v-expansion-panel-content__wrap { padding: 16px 12px 4px; }
"""


@solara.component
def ModelFormDialog(project, open_, on_submit: Callable[[dict], None]):
    """Model form in the shared CreationDialog frame.

    on_submit(entry) receives {"model_key","name","params","dataset_key",
    "sample_key"}; the tile owns job creation and training.
    """
    p = project.value

    selected_key, set_selected_key = solara.use_state(MODEL_KEYS[0])
    all_params, set_all_params = solara.use_state(
        {k: _default_params(k) for k in MODEL_KEYS}
    )

    dataset_keys = sorted(p.datasets.keys()) if p and p.datasets else []
    selected_dataset, set_selected_dataset = solara.use_state(
        dataset_keys[0] if dataset_keys else ""
    )
    sample_keys = sorted(p.samples.keys()) if p and p.samples else []
    selected_sample, set_selected_sample = solara.use_state(
        sample_keys[0] if sample_keys else ""
    )

    models = p.models if p and p.models else {}
    name_value, on_name_input, reset_name = use_artifact_name(
        suggest_version(selected_key, models)
    )
    clean = sanitize_key(name_value)
    storage_key = f"{selected_key}_{clean}" if clean else selected_key

    registry = MODEL_REGISTRY[selected_key]
    needs_sample = registry.get("has_sampling", False)

    selected_ds_obj = (
        p.datasets.get(selected_dataset)
        if p and p.datasets and selected_dataset
        else None
    )
    feature_options = []
    if selected_ds_obj:
        feature_options = [v.name for v in selected_ds_obj.features]
        if selected_ds_obj.target is not None and selected_ds_obj.target.name not in feature_options:
            feature_options.append(selected_ds_obj.target.name)

    def reset():
        reset_name()

    def validate():
        if p is None:
            return t("tiles.train.error_no_project")
        if not clean:
            return t("tiles.train.error_name_required")
        if not selected_dataset or selected_dataset not in p.datasets:
            return t("tiles.train.error_invalid_dataset")
        if needs_sample and (not selected_sample or selected_sample not in p.samples):
            return t("tiles.train.error_invalid_sample")
        if MODEL_HAS_VARIABLES[selected_key]:
            model_params = all_params.get(selected_key, {})
            for pdef in registry["params"]:
                if pdef.get("group") != "variables":
                    continue
                val = model_params.get(pdef["key"])
                if not val:
                    return t("tiles.train.error_select_layer", label=t(pdef["label_key"]))
                if val not in feature_options:
                    return t(
                        "tiles.train.error_layer_not_in_dataset",
                        label=t(pdef["label_key"]), val=val, dataset=selected_dataset,
                    )
        return None

    def will_replace():
        return storage_key if storage_key in models else None

    def launch():
        on_submit(
            {
                "model_key": selected_key,
                "name": clean,
                "params": all_params.get(selected_key, {}),
                "dataset_key": selected_dataset,
                "sample_key": selected_sample if needs_sample else "",
            }
        )

    def _set_model_params(new_params, mk=selected_key):
        set_all_params({**all_params, mk: new_params})

    ParamComponent = PARAM_COMPONENTS[selected_key]

    with CreationDialog(
        open_=open_,
        title=t("tiles.train.dialog_title"),
        create_label=t("tiles.train.train_button"),
        validate=validate,
        will_replace=will_replace,
        launch=launch,
        on_close=reset,
        replace_message=lambda k: t("tiles.train.confirm_overwrite_message", key=k),
    ):
        solara.Style(_ADVANCED_PANEL_CSS)
        with solara.Row(style="gap:4px;align-items:center;margin-bottom:12px;"):
            # hide_details drops the (empty) message strip under the input so
            # the row centres the info button on the input box itself.
            rv.Select(
                label=t("tiles.train.model_select_label"),
                items=[{"text": model_label(k), "value": k} for k in MODEL_KEYS],
                item_text="text", item_value="value",
                v_model=selected_key, on_v_model=set_selected_key,
                dense=True, outlined=True, hide_details=True,
                style_="flex:1 1 auto;",
            )
            InfoButton(
                t("tiles.train.model_description_header_for", label=model_label(selected_key)),
                t(f"models.{selected_key}.summary_md") + "\n\n" + t(registry["description_key"]),
            )

        rv.Select(
            label=t("tiles.train.dataset_select_label"), items=dataset_keys,
            v_model=selected_dataset, on_v_model=set_selected_dataset,
            dense=True, outlined=True,
            no_data_text=t("tiles.train.dataset_select_no_data"),
            hint=t("tiles.train.dataset_select_hint"), persistent_hint=True,
        )
        if needs_sample:
            rv.Select(
                label=t("tiles.train.sample_select_label"), items=sample_keys,
                v_model=selected_sample, on_v_model=set_selected_sample,
                dense=True, outlined=True,
                no_data_text=t("tiles.train.sample_select_no_data"),
                hint=t("tiles.train.sample_select_hint"), persistent_hint=True,
            )

        if MODEL_HAS_VARIABLES[selected_key]:
            solara.Markdown(t("tiles.train.variables_header"))
            if not selected_dataset:
                solara.Info(t("tiles.train.variables_info_no_dataset"))
            elif not feature_options:
                solara.Info(t("tiles.train.variables_info_no_features", dataset=selected_dataset))
            VarComponent = VARIABLE_COMPONENTS[selected_key]
            VarComponent(
                params=all_params.get(selected_key, {}),
                set_params=_set_model_params,
                feature_options=feature_options,
            )

        ArtifactNameField(
            value=name_value,
            on_input=on_name_input,
            storage_key=storage_key,
            exists=storage_key in models,
            label=t("tiles.train.model_name_label"),
        )

        # Every parameter has a working default, so tuning is progressive-
        # disclosed at the end of the form, collapsed by default.
        if MODEL_HAS_PARAMS[selected_key]:
            with rv.ExpansionPanels(flat=True, class_="advanced-params"):
                with rv.ExpansionPanel():
                    with rv.ExpansionPanelHeader():
                        solara.Text(t("tiles.train.advanced_parameters_header"))
                    with rv.ExpansionPanelContent():
                        ParamComponent(
                            params=all_params.get(selected_key, {}),
                            set_params=_set_model_params,
                        )
