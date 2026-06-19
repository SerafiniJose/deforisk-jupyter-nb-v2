"""Step 5 — Train tile."""

import logging
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
from spatialrisk.sampling import Sampling

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
             "type": "text", "default": "forest_gfc_edge", "group": "variables"},
            {"key": "forest_var", "label": "Forest variable",
             "type": "text", "default": "forest_gfc", "group": "variables"},
            {"key": "subj_var", "label": "Subjurisdiction variable",
             "type": "text", "default": "subj", "group": "variables"},
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
             "type": "text", "default": "forest_gfc_edge", "group": "variables"},
            {"key": "forest_var", "label": "Forest variable",
             "type": "text", "default": "forest_gfc", "group": "variables"},
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

SAMPLING_STRATEGIES = ["random", "stratified", "systematic", "legacy"]

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


def _run_training(job_id, model_key, param_values, dataset, sampling_cfg, project):
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
        model.dataset = dataset
        model.project = project

        if sampling_cfg and registry["has_sampling"]:
            model.sampling = Sampling(**sampling_cfg)

        model.fit(**build_fit_kwargs(model_key, dataset, project))

        # Update job on success (immutably, so the UI actually re-renders).
        _update_job(
            job_id,
            status="completed",
            deviance=model.deviance,
            n_samples=model.n_samples,
        )

        # Auto-register in project; record its registry key on the job so the
        # list "remove" action can delete the right model.
        model.register(project, auto_save=True)
        _update_job(job_id, model_storage_key=model._model_key())
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

    @solara.component
    def _Params(params: dict, set_params):
        def _update(param_key, value):
            set_params({**params, param_key: value})

        for param_def in param_defs:
            pkey = param_def["key"]
            current = params.get(pkey, param_def["default"])
            ptype = param_def["type"]

            if ptype == "select":
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
def SamplingConfig(strategy, set_strategy, n_samples, set_n_samples, seed, set_seed):
    """Sampling configuration section for supervised models."""
    solara.Markdown("**Sampling**")
    rv.Select(
        label="Strategy",
        items=SAMPLING_STRATEGIES,
        v_model=strategy,
        on_v_model=set_strategy,
        dense=True,
        outlined=True,
    )
    rv.TextField(
        label="Number of samples",
        v_model=str(n_samples) if n_samples is not None else "",
        on_v_model=lambda v: set_n_samples(int(v) if v and v.strip() else None),
        dense=True,
        outlined=True,
        type_="number",
    )
    rv.TextField(
        label="Random seed",
        v_model=str(seed) if seed is not None else "",
        on_v_model=lambda v: set_seed(int(v) if v and v.strip() else None),
        dense=True,
        outlined=True,
        type_="number",
    )


@solara.component
def TrainTile(project):
    """Train tab: select model, configure parameters, select dataset, train."""
    p = project.value

    # Model selection
    selected_label, set_selected_label = solara.use_state(MODEL_LABELS[0])
    selected_key = MODEL_KEYS[MODEL_LABELS.index(selected_label)] if selected_label in MODEL_LABELS else MODEL_KEYS[0]

    # Parameters — one dict per model, all initialised up-front so widget
    # tree is always the same shape (avoids reacton reconciliation crashes).
    all_params, set_all_params = solara.use_state(
        {k: _default_params(k) for k in MODEL_KEYS}
    )

    # Dataset selection
    dataset_keys = sorted(p.datasets.keys()) if p and p.datasets else []
    selected_dataset, set_selected_dataset = solara.use_state("")

    # Sampling (supervised models only)
    sampling_strategy, set_sampling_strategy = solara.use_state("random")
    sampling_n_samples, set_sampling_n_samples = solara.use_state(10000)
    sampling_seed, set_sampling_seed = solara.use_state(1234)

    # Form messages
    form_error, set_form_error = solara.use_state(None)

    registry = MODEL_REGISTRY[selected_key]

    def on_train():
        set_form_error(None)
        if p is None:
            set_form_error("No active project.")
            return
        if not selected_dataset or selected_dataset not in p.datasets:
            set_form_error("Select a valid dataset.")
            return

        dataset = p.datasets[selected_dataset]

        sampling_cfg = None
        if registry["has_sampling"]:
            sampling_cfg = {
                "strategy": sampling_strategy,
                "n_samples": sampling_n_samples,
                "seed": sampling_seed,
            }

        job_id = str(uuid.uuid4())[:8]
        job = {
            "id": job_id,
            "model_type": selected_key,
            "model_label": registry["label"],
            "dataset_name": selected_dataset,
            "status": "running",
            "error": None,
            "deviance": None,
            "n_samples": None,
        }
        train_jobs.set(list(train_jobs.value) + [job])

        spawn_in_context(
            _run_training,
            (job_id, selected_key, all_params.get(selected_key, {}), dataset, sampling_cfg, p),
        )
        logger.info("Training started: %s on %s (job=%s)", selected_key, selected_dataset, job_id)

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
        solara.Markdown("### Step 5 — Train")
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

        # Training data — the dataset and the variable (layer) references that
        # name fields within it live together under one heading.
        solara.Markdown("**Training data**")

        # Dataset selector
        rv.Select(
            label="Dataset",
            items=dataset_keys,
            v_model=selected_dataset,
            on_v_model=set_selected_dataset,
            dense=True,
            outlined=True,
            no_data_text="No datasets registered. Create one in Step 4.",
        )

        # Variables — for Benchmark/MW these name layers within the dataset, so
        # they belong with the dataset selection rather than the collapsed
        # parameters. Models without variable params render nothing here.
        if MODEL_HAS_VARIABLES[selected_key]:
            solara.Markdown("**Variables**")
            VarComponent = VARIABLE_COMPONENTS[selected_key]
            VarComponent(
                params=all_params.get(selected_key, {}),
                set_params=_set_model_params,
            )

        # Sampling (conditional)
        if registry["has_sampling"]:
            SamplingConfig(
                strategy=sampling_strategy,
                set_strategy=set_sampling_strategy,
                n_samples=sampling_n_samples,
                set_n_samples=set_sampling_n_samples,
                seed=sampling_seed,
                set_seed=set_sampling_seed,
            )

        # Train button
        has_dataset = bool(selected_dataset and dataset_keys)
        solara.Button(
            "Train",
            icon_name="mdi-play",
            color="primary",
            small=True,
            on_click=on_train,
            disabled=not has_dataset,
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
