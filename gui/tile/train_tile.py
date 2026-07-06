"""Step 6 — Train tile."""

import logging
import re
import uuid

import reacton.ipyvuetify as rv
import solara

from spatialrisk.mlmodels import (
    GLMModel,
    ICARModel,
    JNRBenchmarkModel,
    MWModel,
    RFModel,
)
from spatialrisk.evaluation import interval_from_target

from gui.i18n import t
from gui.scripts.solara_threads import spawn_in_context, update_job
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.help import InfoButton
from gui.widget.train_model_list import TrainModelList

logger = logging.getLogger("spatial_risk")

# ---------------------------------------------------------------------------
# Model registry
# ---------------------------------------------------------------------------

MODEL_REGISTRY = {
    "benchmark": {
        "label_key": "models.benchmark.label",
        "class": JNRBenchmarkModel,
        "description_key": "models.benchmark.description",
        "params": [
            {"key": "blk_rows", "label_key": "models.benchmark.params.blk_rows.label", "type": "int", "default": 128},
            {
                "key": "defor_threshold",
                "label_key": "models.benchmark.params.defor_threshold.label",
                "type": "float",
                "default": 99.5,
            },
            {"key": "defor_var", "label_key": "models.benchmark.params.defor_var.label",
             "type": "select", "default": "", "group": "variables"},
            {"key": "forest_edge_var", "label_key": "models.benchmark.params.forest_edge_var.label",
             "type": "select", "default": "", "group": "variables"},
            {"key": "forest_var", "label_key": "models.benchmark.params.forest_var.label",
             "type": "select", "default": "", "group": "variables"},
            {"key": "subj_var", "label_key": "models.benchmark.params.subj_var.label",
             "type": "select", "default": "", "group": "variables"},
        ],
        "has_sampling": False,
    },
    "mw": {
        "label_key": "models.mw.label",
        "class": MWModel,
        "description_key": "models.mw.description",
        "params": [
            {
                "key": "win_size_list",
                "label_key": "models.mw.params.win_size_list.label",
                "type": "text",
                "default": "5, 11, 21",
            },
            {"key": "blk_rows", "label_key": "models.mw.params.blk_rows.label", "type": "int", "default": 256},
            {
                "key": "defor_threshold",
                "label_key": "models.mw.params.defor_threshold.label",
                "type": "float",
                "default": 99.5,
            },
            {"key": "defor_var", "label_key": "models.mw.params.defor_var.label",
             "type": "select", "default": "", "group": "variables"},
            {"key": "forest_edge_var", "label_key": "models.mw.params.forest_edge_var.label",
             "type": "select", "default": "", "group": "variables"},
            {"key": "forest_var", "label_key": "models.mw.params.forest_var.label",
             "type": "select", "default": "", "group": "variables"},
        ],
        "has_sampling": False,
    },
    "glm": {
        "label_key": "models.glm.label",
        "class": GLMModel,
        "description_key": "models.glm.description",
        "params": [
            {
                "key": "solver",
                "label_key": "models.glm.params.solver.label",
                "type": "select",
                "default": "lbfgs",
                "items": ["lbfgs", "liblinear", "newton-cg", "sag", "saga"],
            },
            {
                "key": "max_iter",
                "label_key": "models.glm.params.max_iter.label",
                "type": "int",
                "default": 1000,
            },
            {
                "key": "random_seed",
                "label_key": "models.glm.params.random_seed.label",
                "type": "int",
                "default": 1234,
            },
        ],
        "has_sampling": True,
    },
    "rf": {
        "label_key": "models.rf.label",
        "class": RFModel,
        "description_key": "models.rf.description",
        "params": [
            {
                "key": "n_trees",
                "label_key": "models.rf.params.n_trees.label",
                "type": "int",
                "default": 100,
            },
            {
                "key": "max_depth",
                "label_key": "models.rf.params.max_depth.label",
                "type": "int",
                "default": 15,
            },
            {
                "key": "min_samples_leaf",
                "label_key": "models.rf.params.min_samples_leaf.label",
                "type": "int",
                "default": 2,
            },
            {
                "key": "random_seed",
                "label_key": "models.rf.params.random_seed.label",
                "type": "int",
                "default": 1234,
            },
        ],
        "has_sampling": True,
    },
    "icar": {
        "label_key": "models.icar.label",
        "class": ICARModel,
        "description_key": "models.icar.description",
        "params": [
            {
                "key": "csize",
                "label_key": "models.icar.params.csize.label",
                "type": "float",
                "default": 10.0,
            },
            {
                "key": "mcmc",
                "label_key": "models.icar.params.mcmc.label",
                "type": "int",
                "default": 4000,
            },
            {
                "key": "burnin",
                "label_key": "models.icar.params.burnin.label",
                "type": "int",
                "default": 4000,
            },
            {"key": "thin", "label_key": "models.icar.params.thin.label", "type": "int", "default": 1},
            {
                "key": "prior_vrho",
                "label_key": "models.icar.params.prior_vrho.label",
                "type": "float",
                "default": -1.0,
            },
            {
                "key": "beta_start",
                "label_key": "models.icar.params.beta_start.label",
                "type": "float",
                "default": -99.0,
            },
            {
                "key": "random_seed",
                "label_key": "models.icar.params.random_seed.label",
                "type": "int",
                "default": 1234,
            },
            {
                "key": "csize_interpolate",
                "label_key": "models.icar.params.csize_interpolate.label",
                "type": "float",
                "default": 0.1,
            },
        ],
        "has_sampling": True,
    },
}

MODEL_KEYS = list(MODEL_REGISTRY.keys())


def model_label(key: str) -> str:
    """Resolve a model's display label at render time."""
    return t(MODEL_REGISTRY[key]["label_key"])


def model_labels() -> list:
    """Resolve all model display labels at render time."""
    return [model_label(k) for k in MODEL_KEYS]


# Module-level reactive shared across re-renders
train_jobs = solara.reactive([])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_params(model_key: str) -> dict:
    """Return default parameter values for a model."""
    return {
        p["key"]: p["default"] for p in MODEL_REGISTRY[model_key]["params"]
    }


def _sanitize_name(name: str) -> str:
    """Normalise a user-typed model name for use as a storage key / filename.

    Keeps alphanumerics, dash and underscore; collapses any other run of
    characters into a single underscore and trims leading/trailing ones. The
    result feeds both the ``project.models`` key (``{model_type}_{name}``) and
    the on-disk pickle filename, so it must be path-safe.
    """
    return re.sub(r"[^A-Za-z0-9_-]+", "_", (name or "").strip()).strip("_")


def _storage_key(model_key: str, name: str) -> str:
    """Project.models key for a model — mirrors BaseRiskModel's key formula."""
    return f"{model_key}_{name}" if name else model_key


def _update_job(job_id, *, skip_if_cancelled=True, **changes):
    """Immutably update a train job by id so the UI re-renders (see update_job)."""
    update_job(train_jobs, job_id, skip_if_cancelled=skip_if_cancelled, **changes)


def _parse_param(value: str, ptype: str):
    """Parse a string parameter value to the correct type."""
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    if ptype == "int":
        return int(value)
    if ptype == "float":
        return float(value)
    return value


def build_fit_kwargs(model_key, dataset, project):
    """Family-specific fit() kwargs. ML models fit on the attached dataset (no args)."""
    if model_key == "mw":
        return {
            "time_interval": interval_from_target(dataset.target.name),
            "folder": project.folders.rmj_mw,
        }
    if model_key == "benchmark":
        return {"folder": project.folders.rmj_bm}
    return {}


def _run_training(job_id, model_key, param_values, dataset, sample, project,
                  project_reactive=None, model_name=None):
    """Run model training in a background thread."""
    registry = MODEL_REGISTRY[model_key]
    model_cls = registry["class"]

    try:
        # Build model kwargs from param_values
        kwargs = {}
        for param_def in registry["params"]:
            key = param_def["key"]
            raw = param_values.get(key, param_def["default"])
            if key == "win_size_list" and isinstance(raw, str):
                kwargs[key] = [int(x.strip()) for x in raw.split(",") if x.strip()]
            else:
                kwargs[key] = _parse_param(str(raw) if raw is not None else None, param_def["type"])

        model = model_cls(**kwargs)
        # The user-chosen name drives both the project.models key and the pickle
        # filename, so it MUST be set before fit() (fit() calls save()).
        model.name = model_name or None
        model.dataset = dataset
        model.project = project
        if sample is not None:
            model.sample = sample
            model.sample_name = sample.name

        model.fit(**build_fit_kwargs(model_key, dataset, project))

        # Update job on success (immutably, so the UI actually re-renders).
        _update_job(
            job_id,
            status="completed",
            deviance=model.deviance,
            n_samples=model.n_samples,
        )

        # Register in the project under the name-derived key. The user already
        # confirmed any overwrite in the UI, so if the key is taken we delete the
        # superseded model (and its files) first to avoid orphaned pickles.
        storage_key = _storage_key(model_key, model_name)
        if storage_key in project.models:
            project.delete_model(storage_key, auto_save=False)
        model.register(project, key=storage_key, auto_save=True)
        _update_job(job_id, model_storage_key=model._model_key())

        # register() mutates project.models in place; publish a fresh copy on the
        # reactive so dependent tiles (Step 7 — Inference) re-render and list the
        # newly trained model. Without this set() the identity-equality reactive
        # never fires and the Inference model dropdown stays empty.
        if project_reactive is not None:
            project_reactive.set(project.model_copy())
        logger.info("Model %s trained and registered.", model_key)

    except Exception as exc:
        logger.exception("Training failed for %s", model_key)
        _update_job(job_id, status="failed", error=str(exc))


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------


def _make_param_component(model_key: str, group: str):
    """Factory: create a dedicated Solara component for one model's params.

    ``group`` selects which subset of the model's params this component renders
    ("params" for hyperparameters, "variables" for dataset-layer references).
    Each (model, group) pair gets its own component *type*, so when the user
    switches models reacton unmounts the old component and mounts the new one
    cleanly — no child-tree reconciliation, no callback-cleanup crashes.
    """
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
            # Per-parameter help resolved by catalog convention (same path as
            # the label). Every registry param has a matching .hint entry —
            # enforced by tests/test_i18n.py.
            hint = t(f"models.{model_key}.params.{pkey}.hint")

            if is_variables:
                # Dataset-layer reference: the options are the feature names of
                # the dataset selected above, not a hardcoded default. The field
                # starts empty so the user must pick a layer that actually
                # exists in their data (the model looks it up by exact name).
                rv.Select(
                    label=t(param_def["label_key"]),
                    items=feature_options or [],
                    v_model=current or None,
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True,
                    outlined=True,
                    clearable=True,
                    no_data_text=t("tiles.train.variables_select_no_data"),
                    hint=hint,
                    persistent_hint=True,
                )
            elif ptype == "select":
                rv.Select(
                    label=t(param_def["label_key"]),
                    items=param_def.get("items", []),
                    v_model=current,
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True,
                    outlined=True,
                    hint=hint,
                    persistent_hint=True,
                )
            elif ptype in ("int", "float"):
                rv.TextField(
                    label=t(param_def["label_key"]),
                    v_model=str(current) if current is not None else "",
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True,
                    outlined=True,
                    type_="number",
                    hint=hint,
                    persistent_hint=True,
                )
            else:
                rv.TextField(
                    label=t(param_def["label_key"]),
                    v_model=str(current) if current is not None else "",
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True,
                    outlined=True,
                    hint=hint,
                    persistent_hint=True,
                )

    _Params.__name__ = f"Params_{model_key}_{group}"
    _Params.__qualname__ = f"Params_{model_key}_{group}"
    return _Params


# One component per (model, group) — distinct types avoid reconciliation issues.
# "params" render inside the collapsed Parameters panel; "variables" (the
# dataset-layer references used by Benchmark/MW) render with the dataset.
PARAM_COMPONENTS = {k: _make_param_component(k, "params") for k in MODEL_KEYS}
VARIABLE_COMPONENTS = {k: _make_param_component(k, "variables") for k in MODEL_KEYS}
MODEL_HAS_VARIABLES = {
    k: any(p.get("group") == "variables" for p in MODEL_REGISTRY[k]["params"])
    for k in MODEL_KEYS
}



@solara.component
def TrainTile(project):
    """Train tab: select model, configure parameters, select dataset, train."""
    p = project.value

    # Model selection — store the key, not the label, for locale-independence.
    selected_key, set_selected_key = solara.use_state(MODEL_KEYS[0])

    # Model name — user-chosen, drives the project.models key and pickle filename
    # so models no longer overwrite each other. Prefilled with an editable default
    # the user can keep or replace; reusing a name prompts an overwrite confirm.
    model_name, set_model_name = solara.use_state("v1")
    clean_name = _sanitize_name(model_name)
    storage_key = _storage_key(selected_key, clean_name)

    # Parameters — one dict per model, all initialised up-front so widget
    # tree is always the same shape (avoids reacton reconciliation crashes).
    all_params, set_all_params = solara.use_state(
        {k: _default_params(k) for k in MODEL_KEYS}
    )

    # Dataset selection — always required
    dataset_keys = sorted(p.datasets.keys()) if p and p.datasets else []
    selected_dataset, set_selected_dataset = solara.use_state(
        dataset_keys[0] if dataset_keys else ""
    )

    # Sample selection — only used by sampling-based models (GLM/RF/iCAR)
    sample_keys = sorted(p.samples.keys()) if p and p.samples else []
    selected_sample, set_selected_sample = solara.use_state(
        sample_keys[0] if sample_keys else ""
    )

    # Form messages
    form_error, set_form_error = solara.use_state(None)

    registry = MODEL_REGISTRY[selected_key]
    needs_sample = registry.get("has_sampling", False)

    # Feature names available in the selected dataset — these are the options
    # for the Benchmark/MW "Variables" selects (each names a dataset layer).
    selected_ds_obj = (
        p.datasets.get(selected_dataset)
        if p and p.datasets and selected_dataset
        else None
    )
    # Options for the Benchmark/MW "Variables" selects: every layer in the
    # dataset. The forest-loss layer is typically the dataset target (not a
    # feature), so include the target name alongside the features.
    feature_options = []
    if selected_ds_obj:
        feature_options = [v.name for v in selected_ds_obj.features]
        if selected_ds_obj.target is not None and selected_ds_obj.target.name not in feature_options:
            feature_options.append(selected_ds_obj.target.name)

    pending_overwrite, set_pending_overwrite = solara.use_state(None)

    def _launch_training(name, dataset, sample):
        """Create the job row and spawn the worker. Assumes inputs validated."""
        job_id = str(uuid.uuid4())[:8]
        job = {
            "id": job_id,
            "model_name": name,
            "model_type": selected_key,
            "model_label": model_label(selected_key),
            "dataset_name": selected_dataset,
            "sample_name": selected_sample if needs_sample else None,
            "status": "running",
            "error": None,
            "deviance": None,
            "n_samples": None,
        }
        train_jobs.set(list(train_jobs.value) + [job])

        spawn_in_context(
            _run_training,
            (job_id, selected_key, all_params.get(selected_key, {}),
             dataset, sample, p, project, name),
        )
        logger.info("Training started: %s '%s' on dataset %s (job=%s)",
                    selected_key, name, selected_dataset, job_id)

    def on_train():
        set_form_error(None)
        if p is None:
            set_form_error(t("tiles.train.error_no_project"))
            return
        name = _sanitize_name(model_name)
        if not name:
            set_form_error(t("tiles.train.error_name_required"))
            return
        if not selected_dataset or selected_dataset not in p.datasets:
            set_form_error(t("tiles.train.error_invalid_dataset"))
            return
        sample = None
        if needs_sample:
            if not selected_sample or selected_sample not in p.samples:
                set_form_error(t("tiles.train.error_invalid_sample"))
                return
            sample = p.samples[selected_sample]
        dataset = p.datasets[selected_dataset]

        # Benchmark/MW reference dataset layers by name. The user must pick each
        # one from the dataset's features (no hardcoded default), and the chosen
        # name must exist — otherwise fit() fails deep in the worker thread.
        if MODEL_HAS_VARIABLES[selected_key]:
            model_params = all_params.get(selected_key, {})
            available = [v.name for v in dataset.features]
            if dataset.target is not None and dataset.target.name not in available:
                available.append(dataset.target.name)
            for pdef in registry["params"]:
                if pdef.get("group") != "variables":
                    continue
                val = model_params.get(pdef["key"])
                if not val:
                    set_form_error(t("tiles.train.error_select_layer", label=t(pdef["label_key"])))
                    return
                if val not in available:
                    set_form_error(
                        t("tiles.train.error_layer_not_in_dataset",
                          label=t(pdef["label_key"]), val=val, dataset=selected_dataset)
                    )
                    return

        # A model with this name+type already exists — confirm before replacing
        # it (training would otherwise silently overwrite the registry entry).
        if _storage_key(selected_key, name) in p.models:
            set_pending_overwrite({
                "name": name,
                "dataset": dataset,
                "sample": sample,
                "storage_key": _storage_key(selected_key, name),
            })
            return

        _launch_training(name, dataset, sample)

    def on_cancel(job_id):
        _update_job(job_id, skip_if_cancelled=False, status="cancelled")

    pending_remove, set_pending_remove = solara.use_state(None)

    def _do_remove(job_id):
        # Completed jobs carry the registered model's key; delete it (registry +
        # on-disk artifacts) from the current project. Failed/cancelled jobs have
        # no model, so this is just a list dismissal.
        job = next((j for j in train_jobs.value if j["id"] == job_id), None)
        key = job.get("model_storage_key") if job else None
        if key:
            cur = project.value
            if cur is not None and key in cur.models:
                cur.delete_model(key, auto_save=True)
                project.set(cur.model_copy())
        train_jobs.set([j for j in train_jobs.value if j["id"] != job_id])

    with solara.Column(style="gap: 16px;"):
        solara.Markdown(t("tiles.train.header"))
        solara.Text(t("tiles.train.description"))

        # Model selector, with an info button opening the model description
        # popup for the selected model (structured summary — approach /
        # training data / output — followed by the prose description).
        with solara.Row(style="gap:4px;align-items:center;"):
            rv.Select(
                label=t("tiles.train.model_select_label"),
                items=[{"text": model_label(k), "value": k} for k in MODEL_KEYS],
                item_text="text",
                item_value="value",
                v_model=selected_key,
                on_v_model=set_selected_key,
                dense=True,
                outlined=True,
                style_="flex:1 1 auto;",
            )
            InfoButton(
                t("tiles.train.model_description_header_for",
                  label=model_label(selected_key)),
                t(f"models.{selected_key}.summary_md")
                + "\n\n"
                + t(registry["description_key"]),
            )

        # Model name — required; gives each trained model a distinct key so it no
        # longer overwrites the previous one. The hint shows the resulting storage
        # key and flags when that key is already taken (training will overwrite).
        name_exists = bool(p and clean_name and storage_key in p.models)
        rv.TextField(
            label=t("tiles.train.model_name_label"),
            v_model=model_name,
            on_v_model=set_model_name,
            dense=True,
            outlined=True,
            messages=(
                t("tiles.train.model_name_exists_warning", key=storage_key)
                if name_exists
                else (t("tiles.train.model_name_saved_as", key=storage_key) if clean_name else t("tiles.train.model_name_required"))
            ),
            error=not clean_name,
        )

        # Parameters — each model has its own component type, so reacton
        # does clean unmount/mount instead of reconciling children. Collapsed
        # by default (self-managed ExpansionPanels) to keep the form compact;
        # the dataset and sampling options below stay visible.
        def _set_model_params(new_params, mk=selected_key):
            set_all_params({**all_params, mk: new_params})

        ParamComponent = PARAM_COMPONENTS[selected_key]
        with rv.ExpansionPanels(flat=True):
            with rv.ExpansionPanel():
                with rv.ExpansionPanelHeader():
                    solara.Text(t("tiles.train.parameters_header"))
                with rv.ExpansionPanelContent():
                    ParamComponent(
                        params=all_params.get(selected_key, {}),
                        set_params=_set_model_params,
                    )

        # Training data — dataset always required; sample only for sampling-based models.
        solara.Markdown(t("tiles.train.training_data_header"))
        rv.Select(
            label=t("tiles.train.dataset_select_label"),
            items=dataset_keys,
            v_model=selected_dataset,
            on_v_model=set_selected_dataset,
            dense=True,
            outlined=True,
            no_data_text=t("tiles.train.dataset_select_no_data"),
            hint=t("tiles.train.dataset_select_hint"),
            persistent_hint=True,
        )
        if needs_sample:
            rv.Select(
                label=t("tiles.train.sample_select_label"),
                items=sample_keys,
                v_model=selected_sample,
                on_v_model=set_selected_sample,
                dense=True,
                outlined=True,
                no_data_text=t("tiles.train.sample_select_no_data"),
                hint=t("tiles.train.sample_select_hint"),
                persistent_hint=True,
            )

        # Variables — for Benchmark/MW these name layers within the dataset.
        # The selects are populated from the chosen dataset's features and start
        # empty; the user picks which layer plays each role.
        if MODEL_HAS_VARIABLES[selected_key]:
            solara.Markdown(t("tiles.train.variables_header"))
            if not selected_dataset:
                solara.Info(t("tiles.train.variables_info_no_dataset"))
            elif not feature_options:
                solara.Info(
                    t("tiles.train.variables_info_no_features", dataset=selected_dataset)
                )
            VarComponent = VARIABLE_COMPONENTS[selected_key]
            VarComponent(
                params=all_params.get(selected_key, {}),
                set_params=_set_model_params,
                feature_options=feature_options,
            )

        # Train button — disabled when no name, no dataset, or (needs_sample and no sample).
        train_disabled = (
            not clean_name
            or not selected_dataset
            or (needs_sample and not selected_sample)
        )
        solara.Button(
            t("tiles.train.train_button"),
            icon_name="mdi-play",
            color="primary",
            small=True,
            on_click=on_train,
            disabled=train_disabled,
        )

        if form_error:
            rv.Alert(type_="error", dense=True, children=[form_error])

        # Trained models list
        TrainModelList(
            train_jobs=train_jobs,
            on_cancel=on_cancel,
            on_remove=set_pending_remove,
        )

        _pending_job = (
            next((j for j in train_jobs.value if j["id"] == pending_remove), None)
            if pending_remove
            else None
        )
        _pending_model_key = _pending_job.get("model_storage_key") if _pending_job else None
        ConfirmDialog(
            open=pending_remove is not None,
            on_cancel=lambda: set_pending_remove(None),
            on_confirm=lambda: (_do_remove(pending_remove), set_pending_remove(None)),
            title=t("tiles.train.confirm_delete_model_title") if _pending_model_key else t("tiles.train.confirm_remove_job_title"),
            message=(
                t("tiles.train.confirm_delete_model_message", key=_pending_model_key)
                if _pending_model_key
                else t("tiles.train.confirm_remove_job_message")
            ),
            confirm_label=t("common.delete") if _pending_model_key else t("common.remove"),
        )

        # Overwrite confirmation — shown when the chosen name+type already exists.
        def _confirm_overwrite():
            ov = pending_overwrite
            set_pending_overwrite(None)
            if ov:
                _launch_training(ov["name"], ov["dataset"], ov["sample"])

        ConfirmDialog(
            open=pending_overwrite is not None,
            on_cancel=lambda: set_pending_overwrite(None),
            on_confirm=_confirm_overwrite,
            title=t("tiles.train.confirm_overwrite_title"),
            message=(
                t("tiles.train.confirm_overwrite_message", key=pending_overwrite["storage_key"])
                if pending_overwrite
                else ""
            ),
            confirm_label=t("tiles.train.confirm_overwrite_label"),
        )
