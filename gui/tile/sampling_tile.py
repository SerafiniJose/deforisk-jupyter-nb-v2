"""Step 5 — Sampling tile.

Generates persistent, selectable samples from a processed raster
variable (defines the grid and, for stratified sampling, the strata) and an
optional mask variable (restricts valid pixels). Step 6 (Train) selects a
sample and a dataset, then extracts features at the sample points.
"""

import logging
import uuid

import reacton.ipyvuetify as rv
import solara

from gui.scripts.map_helpers import (
    add_sample_points_on_map,
    remove_sample_points_from_map,
)
from gui.scripts.solara_threads import spawn_in_context, update_job
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.sample_set_list import SampleSetList

logger = logging.getLogger("spatial_risk")

SAMPLING_STRATEGIES = ["random", "stratified", "systematic"]
ALLOCATION_METHODS = ["equal", "proportional", "deforisk"]
SYSTEMATIC_MODES = [
    {"text": "Number of samples", "value": "n_samples"},
    {"text": "Distance between points (m)", "value": "spacing"},
]

# Module-level reactives shared across re-renders.
sampling_jobs = solara.reactive([])
samples_on_map = solara.reactive(set())


def _sample_layer_key(name: str) -> str:
    return f"sample_{name}"


def _update_job(job_id, *, skip_if_cancelled=True, **changes):
    update_job(sampling_jobs, job_id, skip_if_cancelled=skip_if_cancelled, **changes)


def _run_sampling(job_id, name, raster_var, mask_var, strategy, allocation,
                  adapt, n_samples, spacing_m, seed, project_reactive):
    """Generate a Sample in the background and register it on the project."""
    try:
        from spatialrisk.sample import Sample

        p = project_reactive.value
        folder = p.folders.samples_folder
        sample = Sample(
            project=p, name=name, raster_var_name=raster_var,
            mask_var_name=mask_var if mask_var else None,
            strategy=strategy,
            allocation=allocation if strategy == "stratified" else None,
            adapt=adapt, n_samples=n_samples, spacing_m=spacing_m, seed=seed,
            points_path=folder / f"{name}.gpkg",
        )
        sample.generate()
        p.add_sample(sample, auto_save=True)
        project_reactive.set(p.model_copy())
        _update_job(job_id, status="completed", n_total=sample.n_total,
                    class_counts=sample.class_counts)
        logger.info("Sample generated: %s (%d points)", name, sample.n_total)
    except Exception as exc:
        logger.exception("Sampling failed for %s", name)
        _update_job(job_id, status="failed", error=str(exc))


@solara.component
def SamplingTile(project, map_=None):
    """Sampling tab: generate persistent samples and add them to the map."""
    p = project.value
    raster_keys = sorted(
        k for k, v in p.processed_variables.items()
        if getattr(v, "data_type", None) == "raster"
    ) if p else []

    # All hooks are called unconditionally before any early return so the hook
    # count is stable across renders (this tile is gated, so it renders before
    # raster variables exist and then again once they do).
    raster_var, set_raster_var = solara.use_state(raster_keys[0] if raster_keys else "")
    mask_var, set_mask_var = solara.use_state("")
    name, set_name = solara.use_state("")
    strategy, set_strategy = solara.use_state("random")
    allocation, set_allocation = solara.use_state("equal")
    adapt, set_adapt = solara.use_state(False)
    n_samples, set_n_samples = solara.use_state(10000)
    seed, set_seed = solara.use_state(1234)
    form_error, set_form_error = solara.use_state(None)
    sys_mode, set_sys_mode = solara.use_state("n_samples")
    spacing_m, set_spacing_m = solara.use_state(1000.0)
    pending_remove, set_pending_remove = solara.use_state(None)

    if p is None:
        return
    if not raster_keys:
        solara.Info("Create raster variables first (Step 3 — Process).")
        return

    def on_generate():
        set_form_error(None)
        nm = (name or "").strip()
        if not nm:
            set_form_error("Give the sample a name.")
            return
        if nm in p.samples:
            set_form_error(f"A sample named '{nm}' already exists.")
            return
        if not raster_var or raster_var not in p.processed_variables:
            set_form_error("Select a valid raster variable.")
            return
        if mask_var and mask_var not in p.processed_variables:
            set_form_error("Select a valid mask variable.")
            return

        use_spacing = strategy == "systematic" and sys_mode == "spacing"
        if use_spacing and (spacing_m is None or spacing_m <= 0):
            set_form_error("Enter a positive distance between points (m).")
            return
        spacing_arg = spacing_m if use_spacing else None
        n_samples_arg = None if use_spacing else n_samples

        job_id = str(uuid.uuid4())[:8]
        sampling_jobs.set(list(sampling_jobs.value) + [{
            "id": job_id, "name": nm, "raster_var_name": raster_var,
            "mask_var_name": mask_var,
            "status": "running", "error": None,
            "n_total": None, "class_counts": None,
        }])
        spawn_in_context(
            _run_sampling,
            (job_id, nm, raster_var, mask_var, strategy, allocation,
             adapt, n_samples_arg, spacing_arg, seed, project),
        )
        set_name("")
        logger.info("Sampling started: %s (raster=%s, job=%s)", nm, raster_var, job_id)

    def on_toggle_map(key):
        if map_ is None:
            return
        cur = project.value
        if cur is None:
            return
        ss = cur.samples.get(key)
        if ss is None or ss.points_path is None:
            return
        try:
            if key in samples_on_map.value:
                remove_sample_points_from_map(map_, _sample_layer_key(key))
                samples_on_map.set(samples_on_map.value - {key})
            else:
                add_sample_points_on_map(
                    map_, ss.points_path, key, _sample_layer_key(key)
                )
                samples_on_map.set(samples_on_map.value | {key})
        except Exception as exc:
            logger.exception("sample map toggle failed for %s", key)
            set_form_error(f"Could not toggle sample on map: {exc}")

    def _do_remove(key):
        if map_ is not None and key in samples_on_map.value:
            remove_sample_points_from_map(map_, _sample_layer_key(key))
            samples_on_map.set(samples_on_map.value - {key})
        cur = project.value
        if cur is not None and key in cur.samples:
            cur.delete_sample(key, auto_save=True)
            project.set(cur.model_copy())

    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 5 — Sampling")
        solara.Text(
            "Draw a persistent, named sample from a raster variable. Samples are "
            "selectable in Train (Step 6) and can be added to the map."
        )

        rv.Select(
            label="Raster variable", items=raster_keys, v_model=raster_var,
            on_v_model=set_raster_var, dense=True, outlined=True,
        )
        rv.Select(
            label="Mask variable (optional)", items=[""] + raster_keys,
            v_model=mask_var, on_v_model=set_mask_var, dense=True, outlined=True,
        )
        rv.TextField(
            label="Sample name", v_model=name,
            on_v_model=set_name, dense=True, outlined=True,
        )
        rv.Select(
            label="Strategy", items=SAMPLING_STRATEGIES, v_model=strategy,
            on_v_model=set_strategy, dense=True, outlined=True,
        )

        if strategy == "stratified":
            rv.Select(
                label="Allocation", items=ALLOCATION_METHODS, v_model=allocation,
                on_v_model=set_allocation, dense=True, outlined=True,
            )
            if allocation == "deforisk":
                rv.Switch(
                    label="Adapt allocation to observed deforestation rate",
                    v_model=adapt, on_v_model=set_adapt,
                )

        if strategy == "systematic":
            rv.Select(
                label="Define grid by", items=SYSTEMATIC_MODES,
                item_text="text", item_value="value",
                v_model=sys_mode, on_v_model=set_sys_mode,
                dense=True, outlined=True,
            )

        if strategy == "systematic" and sys_mode == "spacing":
            rv.TextField(
                label="Distance between points (m)", type_="number",
                v_model=str(spacing_m) if spacing_m is not None else "",
                on_v_model=lambda v: set_spacing_m(float(v) if v and v.strip() else None),
                dense=True, outlined=True,
            )
        else:
            rv.TextField(
                label="Number of samples", type_="number",
                v_model=str(n_samples) if n_samples is not None else "",
                on_v_model=lambda v: set_n_samples(int(v) if v and v.strip() else None),
                dense=True, outlined=True,
            )
        rv.TextField(
            label="Random seed", type_="number",
            v_model=str(seed) if seed is not None else "",
            on_v_model=lambda v: set_seed(int(v) if v and v.strip() else None),
            dense=True, outlined=True,
        )

        solara.Button(
            "Generate", icon_name="mdi-play", color="primary", small=True,
            on_click=on_generate,
        )
        if form_error:
            rv.Alert(type_="error", dense=True, children=[form_error])

        # In-flight job rows.
        for job in sampling_jobs.value:
            if job["status"] == "running":
                rv.Alert(type_="info", dense=True,
                         children=[f"Generating '{job['name']}'…"])
            elif job["status"] == "failed":
                rv.Alert(type_="error", dense=True,
                         children=[f"'{job['name']}' failed: {job['error']}"])

        SampleSetList(
            project=project,
            on_map=frozenset(samples_on_map.value),
            on_toggle_map=on_toggle_map,
            on_remove=set_pending_remove,
        )

        ConfirmDialog(
            open=pending_remove is not None,
            on_cancel=lambda: set_pending_remove(None),
            on_confirm=lambda: (_do_remove(pending_remove), set_pending_remove(None)),
            title="Delete sample?",
            message=(
                f"Delete sample '{pending_remove}'? This removes it from the "
                "project and deletes its files. This cannot be undone."
            ),
            confirm_label="Delete",
        )
