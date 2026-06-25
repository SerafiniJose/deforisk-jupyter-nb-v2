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

from gui.scripts.solara_threads import spawn_in_context, update_job
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.train_model_list import TrainModelList

logger = logging.getLogger("spatial_risk")

# ---------------------------------------------------------------------------
# Model registry
# ---------------------------------------------------------------------------

MODEL_REGISTRY = {
    "benchmark": {
        "label": "Benchmark (JNR)",
        "class": JNRBenchmarkModel,
        "description": (
            "The JNR Benchmark model implements the Jurisdictional and Nested "
            "REDD+ approach for deforestation risk mapping. It stratifies the "
            "landscape by distance-to-forest-edge bins and subjurisdictions, "
            "assigning historical deforestation rates as vulnerability scores. "
            "This non-parametric method requires no feature variables — it relies "
            "solely on spatial proximity to the forest edge. It serves as the "
            "baseline against which more complex models are compared."
        ),
        "params": [
            {"key": "blk_rows", "label": "Block rows", "type": "int", "default": 128},
            {
                "key": "defor_threshold",
                "label": "Deforestation threshold (%)",
                "type": "float",
                "default": 99.5,
            },
            {"key": "forest_edge_var", "label": "Forest-edge variable",
             "type": "select", "default": "", "group": "variables"},
            {"key": "forest_var", "label": "Forest variable",
             "type": "select", "default": "", "group": "variables"},
            {"key": "subj_var", "label": "Subjurisdiction variable",
             "type": "select", "default": "", "group": "variables"},
        ],
        "has_sampling": False,
    },
    "mw": {
        "label": "Moving Window",
        "class": MWModel,
        "description": (
            "The Moving Window model computes local deforestation rates within "
            "spatial windows of specified sizes to produce risk rasters. For each "
            "window size, it calculates the proportion of deforested pixels in the "
            "neighbourhood, creating a spatial heuristic of event density. No machine "
            "learning is involved — the model captures local spatial patterns of "
            "forest loss. Multiple window sizes (e.g. 5, 11, 21 pixels) can be "
            "evaluated simultaneously."
        ),
        "params": [
            {
                "key": "win_size_list",
                "label": "Window sizes (px, comma-separated)",
                "type": "text",
                "default": "5, 11, 21",
            },
            {"key": "blk_rows", "label": "Block rows", "type": "int", "default": 256},
            {
                "key": "defor_threshold",
                "label": "Deforestation threshold (%)",
                "type": "float",
                "default": 99.5,
            },
            {"key": "forest_edge_var", "label": "Forest-edge variable",
             "type": "select", "default": "", "group": "variables"},
            {"key": "forest_var", "label": "Forest variable",
             "type": "select", "default": "", "group": "variables"},
        ],
        "has_sampling": False,
    },
    "glm": {
        "label": "GLM (Logistic Regression)",
        "class": GLMModel,
        "description": (
            "The Generalized Linear Model uses logistic regression to estimate "
            "deforestation probability as a function of spatial predictor variables. "
            "It fits a linear combination of features (altitude, slope, distance to "
            "roads, etc.) through a logit link function. The model is fast to train, "
            "highly interpretable, and produces coefficient estimates that quantify "
            "each variable's contribution to deforestation risk. Uses sklearn's "
            "LogisticRegression with Patsy formula support."
        ),
        "params": [
            {
                "key": "solver",
                "label": "Solver",
                "type": "select",
                "default": "lbfgs",
                "items": ["lbfgs", "liblinear", "newton-cg", "sag", "saga"],
            },
            {
                "key": "max_iter",
                "label": "Max iterations",
                "type": "int",
                "default": 1000,
            },
            {
                "key": "random_seed",
                "label": "Random seed",
                "type": "int",
                "default": 1234,
            },
        ],
        "has_sampling": True,
    },
    "rf": {
        "label": "Random Forest",
        "class": RFModel,
        "description": (
            "The Random Forest model is an ensemble method that builds multiple "
            "decision trees on random subsets of training data and features. Each "
            "tree votes on whether a pixel is at risk of deforestation, and the "
            "ensemble averages these votes into a probability. It captures non-linear "
            "relationships and variable interactions without requiring explicit feature "
            "engineering. Feature importance scores help identify the most influential "
            "predictors."
        ),
        "params": [
            {
                "key": "n_trees",
                "label": "Number of trees",
                "type": "int",
                "default": 100,
            },
            {
                "key": "max_depth",
                "label": "Max depth",
                "type": "int",
                "default": 15,
            },
            {
                "key": "min_samples_leaf",
                "label": "Min samples per leaf",
                "type": "int",
                "default": 2,
            },
            {
                "key": "random_seed",
                "label": "Random seed",
                "type": "int",
                "default": 1234,
            },
        ],
        "has_sampling": True,
    },
    "icar": {
        "label": "iCAR (Bayesian Spatial)",
        "class": ICARModel,
        "description": (
            "The intrinsic Conditional Auto-Regressive (iCAR) model is a Bayesian "
            "spatial model that accounts for spatial autocorrelation through a latent "
            "random effect (rho). It combines the explanatory power of predictor "
            "variables with a spatial smoothing component that captures unobserved "
            "neighbourhood effects. Training uses Markov Chain Monte Carlo (MCMC) "
            "sampling via the forestatrisk library. This is the most computationally "
            "intensive model but often yields the best spatial predictions."
        ),
        "params": [
            {
                "key": "csize",
                "label": "Cell size (km)",
                "type": "float",
                "default": 10.0,
            },
            {
                "key": "mcmc",
                "label": "MCMC iterations",
                "type": "int",
                "default": 4000,
            },
            {
                "key": "burnin",
                "label": "Burn-in iterations",
                "type": "int",
                "default": 4000,
            },
            {"key": "thin", "label": "Thinning factor", "type": "int", "default": 1},
            {
                "key": "prior_vrho",
                "label": "Prior variance (rho)",
                "type": "float",
                "default": -1.0,
            },
            {
                "key": "beta_start",
                "label": "Beta start",
                "type": "float",
                "default": -99.0,
            },
            {
                "key": "random_seed",
                "label": "Random seed",
                "type": "int",
                "default": 1234,
            },
            {
                "key": "csize_interpolate",
                "label": "Interpolation cell size",
                "type": "float",
                "default": 0.1,
            },
        ],
        "has_sampling": True,
    },
}

MODEL_KEYS = list(MODEL_REGISTRY.keys())
MODEL_LABELS = [MODEL_REGISTRY[k]["label"] for k in MODEL_KEYS]

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

            if is_variables:
                # Dataset-layer reference: the options are the feature names of
                # the dataset selected above, not a hardcoded default. The field
                # starts empty so the user must pick a layer that actually
                # exists in their data (the model looks it up by exact name).
                rv.Select(
                    label=param_def["label"],
                    items=feature_options or [],
                    v_model=current or None,
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True,
                    outlined=True,
                    clearable=True,
                    no_data_text="No features in the selected dataset.",
                )
            elif ptype == "select":
                rv.Select(
                    label=param_def["label"],
                    items=param_def.get("items", []),
                    v_model=current,
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True,
                    outlined=True,
                )
            elif ptype in ("int", "float"):
                rv.TextField(
                    label=param_def["label"],
                    v_model=str(current) if current is not None else "",
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True,
                    outlined=True,
                    type_="number",
                )
            else:
                rv.TextField(
                    label=param_def["label"],
                    v_model=str(current) if current is not None else "",
                    on_v_model=lambda v, k=pkey: _update(k, v),
                    dense=True,
                    outlined=True,
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

    # Model selection
    selected_label, set_selected_label = solara.use_state(MODEL_LABELS[0])
    selected_key = MODEL_KEYS[MODEL_LABELS.index(selected_label)] if selected_label in MODEL_LABELS else MODEL_KEYS[0]

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
    feature_options = (
        [v.name for v in selected_ds_obj.features] if selected_ds_obj else []
    )

    pending_overwrite, set_pending_overwrite = solara.use_state(None)

    def _launch_training(name, dataset, sample):
        """Create the job row and spawn the worker. Assumes inputs validated."""
        job_id = str(uuid.uuid4())[:8]
        job = {
            "id": job_id,
            "model_name": name,
            "model_type": selected_key,
            "model_label": registry["label"],
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
            set_form_error("No active project.")
            return
        name = _sanitize_name(model_name)
        if not name:
            set_form_error("Enter a model name (letters, numbers, - or _).")
            return
        if not selected_dataset or selected_dataset not in p.datasets:
            set_form_error("Select a valid dataset.")
            return
        sample = None
        if needs_sample:
            if not selected_sample or selected_sample not in p.samples:
                set_form_error("Select a valid sample.")
                return
            sample = p.samples[selected_sample]
        dataset = p.datasets[selected_dataset]

        # Benchmark/MW reference dataset layers by name. The user must pick each
        # one from the dataset's features (no hardcoded default), and the chosen
        # name must exist — otherwise fit() fails deep in the worker thread.
        if MODEL_HAS_VARIABLES[selected_key]:
            model_params = all_params.get(selected_key, {})
            available = [v.name for v in dataset.features]
            for pdef in registry["params"]:
                if pdef.get("group") != "variables":
                    continue
                val = model_params.get(pdef["key"])
                if not val:
                    set_form_error(f"Select a layer for '{pdef['label']}'.")
                    return
                if val not in available:
                    set_form_error(
                        f"'{pdef['label']}' = '{val}' is not a feature in "
                        f"dataset '{selected_dataset}'."
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
        solara.Markdown("### Step 6 — Train")
        solara.Text("Select a model, configure its parameters, and launch training.")

        # Model selector
        rv.Select(
            label="Model",
            items=MODEL_LABELS,
            v_model=selected_label,
            on_v_model=set_selected_label,
            dense=True,
            outlined=True,
        )

        # Model name — required; gives each trained model a distinct key so it no
        # longer overwrites the previous one. The hint shows the resulting storage
        # key and flags when that key is already taken (training will overwrite).
        name_exists = bool(p and clean_name and storage_key in p.models)
        rv.TextField(
            label="Model name",
            v_model=model_name,
            on_v_model=set_model_name,
            dense=True,
            outlined=True,
            messages=(
                f"⚠ A model named '{storage_key}' already exists — training overwrites it."
                if name_exists
                else (f"Saved as '{storage_key}'." if clean_name else "Required.")
            ),
            error=not clean_name,
        )

        # Collapsible model description — collapsed by default to save space.
        # The expansion panel handles expand/collapse entirely in the browser,
        # so it needs no Python state or click round-trip.
        with rv.ExpansionPanels(flat=True):
            with rv.ExpansionPanel():
                with rv.ExpansionPanelHeader():
                    solara.Text("Model description")
                with rv.ExpansionPanelContent():
                    solara.Markdown(registry["description"])

        # Parameters — each model has its own component type, so reacton
        # does clean unmount/mount instead of reconciling children. Collapsed
        # by default (same self-managed ExpansionPanels pattern as the model
        # description) to keep the form compact; the dataset and sampling
        # options below stay visible.
        def _set_model_params(new_params, mk=selected_key):
            set_all_params({**all_params, mk: new_params})

        ParamComponent = PARAM_COMPONENTS[selected_key]
        with rv.ExpansionPanels(flat=True):
            with rv.ExpansionPanel():
                with rv.ExpansionPanelHeader():
                    solara.Text("Parameters")
                with rv.ExpansionPanelContent():
                    ParamComponent(
                        params=all_params.get(selected_key, {}),
                        set_params=_set_model_params,
                    )

        # Training data — dataset always required; sample only for sampling-based models.
        solara.Markdown("**Training data**")
        rv.Select(
            label="Dataset",
            items=dataset_keys,
            v_model=selected_dataset,
            on_v_model=set_selected_dataset,
            dense=True,
            outlined=True,
            no_data_text="No datasets. Process one in Step 2 — Process.",
        )
        if needs_sample:
            rv.Select(
                label="Sample",
                items=sample_keys,
                v_model=selected_sample,
                on_v_model=set_selected_sample,
                dense=True,
                outlined=True,
                no_data_text="No samples. Generate one in Step 5 — Sampling.",
            )

        # Variables — for Benchmark/MW these name layers within the dataset.
        # The selects are populated from the chosen dataset's features and start
        # empty; the user picks which layer plays each role.
        if MODEL_HAS_VARIABLES[selected_key]:
            solara.Markdown("**Variables**")
            if not selected_dataset:
                solara.Info("Select a dataset above to list its features.")
            elif not feature_options:
                solara.Info(
                    f"Dataset '{selected_dataset}' has no features to choose from."
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
            "Train",
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
            title="Delete model?" if _pending_model_key else "Remove job?",
            message=(
                f"Delete model '{_pending_model_key}'? This removes it from the project "
                "and deletes its files. This cannot be undone."
                if _pending_model_key
                else "Remove this training job from the list?"
            ),
            confirm_label="Delete" if _pending_model_key else "Remove",
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
            title="Overwrite existing model?",
            message=(
                f"A model named '{pending_overwrite['storage_key']}' already exists. "
                "Training will replace it and delete its files. This cannot be undone."
                if pending_overwrite
                else ""
            ),
            confirm_label="Overwrite",
        )
