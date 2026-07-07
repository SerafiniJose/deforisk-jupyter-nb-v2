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

from gui.i18n import t
from gui.scripts.solara_threads import spawn_in_context, update_job
from gui.widget.confirm_dialog import ConfirmDialog
from gui.widget.help import InfoButton
from gui.widget.sample_set_list import SampleSetList

logger = logging.getLogger("spatial_risk")

SAMPLING_STRATEGIES = ["random", "stratified", "systematic"]
ALLOCATION_METHODS = ["equal", "proportional", "deforisk"]


def _systematic_modes():
    return [
        {"text": t("tiles.sampling.systematic_mode_n_samples"), "value": "n_samples"},
        {"text": t("tiles.sampling.systematic_mode_spacing"), "value": "spacing"},
    ]


# Module-level reactives shared across re-renders.
sampling_jobs = solara.reactive([])
samples_on_map = solara.reactive(set())
samples_pending = solara.reactive(frozenset())


def _sample_layer_key(name: str) -> str:
    return f"sample_{name}"


def _remove_sample_layers(map_, base_key):
    """Clear both rendering paths for a sample: PMTiles (base key) and the
    GeoJSON fallback (base_key__event / base_key__forest)."""
    from gui.scripts.map_helpers import remove_sample_points_from_map
    from gui.scripts.pmtiles_map import remove_sample_pmtiles_from_map
    remove_sample_pmtiles_from_map(map_, base_key)
    remove_sample_points_from_map(map_, base_key)


def _toggle_sample_on_map(key, project_reactive, map_, turn_on):
    """Background worker: add/remove a sample's map layer off the kernel thread.

    Prefers PMTiles vector tiles; falls back to GeoJSON when the sample has no
    .pmtiles (old project / tippecanoe missing) or PMTiles add fails.
    """
    base_key = _sample_layer_key(key)
    try:
        if turn_on:
            cur = project_reactive.value
            ss = cur.samples.get(key) if cur else None
            if ss is None:
                return
            drew = False
            if getattr(ss, "pmtiles_path", None):
                try:
                    from gui.scripts.pmtiles_map import add_sample_pmtiles_on_map
                    add_sample_pmtiles_on_map(map_, ss.pmtiles_path, key, base_key)
                    drew = True
                except Exception:
                    logger.exception(
                        "PMTiles add failed for %s; GeoJSON fallback", key)
            if not drew:
                if ss.points_path is None:
                    return
                from gui.scripts.map_helpers import add_sample_points_on_map
                add_sample_points_on_map(map_, ss.points_path, key, base_key)
            samples_on_map.set(samples_on_map.value | {key})
        else:
            _remove_sample_layers(map_, base_key)
            samples_on_map.set(samples_on_map.value - {key})
    except Exception:
        logger.exception("sample map toggle failed for %s", key)
    finally:
        samples_pending.set(samples_pending.value - {key})


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
    spacing_m, set_spacing_m = solara.use_state(1000)
    pending_remove, set_pending_remove = solara.use_state(None)

    if p is None:
        return
    if not raster_keys:
        solara.Info(t("tiles.sampling.error_no_raster"))
        return

    def on_generate():
        set_form_error(None)
        nm = (name or "").strip()
        if not nm:
            set_form_error(t("tiles.sampling.error_name_required"))
            return
        if nm in p.samples:
            set_form_error(t("tiles.sampling.error_name_exists", name=nm))
            return
        if not raster_var or raster_var not in p.processed_variables:
            set_form_error(t("tiles.sampling.error_invalid_raster"))
            return
        if mask_var and mask_var not in p.processed_variables:
            set_form_error(t("tiles.sampling.error_invalid_mask"))
            return

        use_spacing = strategy == "systematic" and sys_mode == "spacing"
        if use_spacing and (spacing_m is None or spacing_m <= 0):
            set_form_error(t("tiles.sampling.error_invalid_spacing"))
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
        if key in samples_pending.value:        # idempotent: ignore re-clicks
            return
        cur = project.value
        if cur is None:
            return
        ss = cur.samples.get(key)
        if ss is None:
            return
        turn_on = key not in samples_on_map.value
        if turn_on and ss.points_path is None and getattr(ss, "pmtiles_path", None) is None:
            return
        samples_pending.set(samples_pending.value | {key})
        spawn_in_context(_toggle_sample_on_map, (key, project, map_, turn_on))

    def _do_remove(key):
        if map_ is not None and key in samples_on_map.value:
            _remove_sample_layers(map_, _sample_layer_key(key))
            samples_on_map.set(samples_on_map.value - {key})
        cur = project.value
        if cur is not None and key in cur.samples:
            cur.delete_sample(key, auto_save=True)
            project.set(cur.model_copy())

    with solara.Column(style="gap: 16px;"):
        solara.Markdown(t("tiles.sampling.header"))
        with solara.Row(style="gap:4px;align-items:center;"):
            solara.Text(t("tiles.sampling.description"))
            InfoButton(t("tiles.sampling.info_header"), t("tiles.sampling.info_md"))

        rv.Select(
            label=t("tiles.sampling.raster_variable_label"), items=raster_keys, v_model=raster_var,
            on_v_model=set_raster_var, dense=True, outlined=True,
            hint=t("tiles.sampling.raster_variable_hint"), persistent_hint=True,
        )
        rv.Select(
            label=t("tiles.sampling.mask_variable_label"), items=[""] + raster_keys,
            v_model=mask_var, on_v_model=set_mask_var, dense=True, outlined=True,
            hint=t("tiles.sampling.mask_variable_hint"), persistent_hint=True,
        )
        rv.TextField(
            label=t("tiles.sampling.sample_name_label"), v_model=name,
            on_v_model=set_name, dense=True, outlined=True,
        )
        rv.Select(
            label=t("tiles.sampling.strategy_label"), items=SAMPLING_STRATEGIES, v_model=strategy,
            on_v_model=set_strategy, dense=True, outlined=True,
            # Dynamic help: describes the currently selected sample type.
            hint=t(f"tiles.sampling.strategy_hint_{strategy}"), persistent_hint=True,
        )

        if strategy == "stratified":
            rv.Select(
                label=t("tiles.sampling.allocation_label"), items=ALLOCATION_METHODS, v_model=allocation,
                on_v_model=set_allocation, dense=True, outlined=True,
                # Dynamic help: how the selected rule splits points across classes.
                hint=t(f"tiles.sampling.allocation_hint_{allocation}"), persistent_hint=True,
            )
            if allocation == "deforisk":
                rv.Switch(
                    label=t("tiles.sampling.adapt_label"),
                    v_model=adapt, on_v_model=set_adapt,
                    hint=t("tiles.sampling.adapt_hint"), persistent_hint=True,
                )

        if strategy == "systematic":
            rv.Select(
                label=t("tiles.sampling.define_grid_label"), items=_systematic_modes(),
                item_text="text", item_value="value",
                v_model=sys_mode, on_v_model=set_sys_mode,
                dense=True, outlined=True,
            )

        if strategy == "systematic" and sys_mode == "spacing":
            rv.TextField(
                label=t("tiles.sampling.spacing_label"), type_="number",
                v_model=str(spacing_m) if spacing_m is not None else "",
                on_v_model=lambda v: set_spacing_m(int(round(float(v))) if v and v.strip() else None),
                dense=True, outlined=True, step="1",
                hint=t("tiles.sampling.spacing_hint"), persistent_hint=True,
            )
        else:
            # deforisk allocation draws N from EACH class, not a split total.
            _n_hint = (
                t("tiles.sampling.n_samples_hint_deforisk")
                if strategy == "stratified" and allocation == "deforisk"
                else t("tiles.sampling.n_samples_hint")
            )
            rv.TextField(
                label=t("tiles.sampling.n_samples_label"), type_="number",
                v_model=str(n_samples) if n_samples is not None else "",
                on_v_model=lambda v: set_n_samples(int(v) if v and v.strip() else None),
                dense=True, outlined=True,
                hint=_n_hint, persistent_hint=True,
            )
        rv.TextField(
            label=t("tiles.sampling.seed_label"), type_="number",
            v_model=str(seed) if seed is not None else "",
            on_v_model=lambda v: set_seed(int(v) if v and v.strip() else None),
            dense=True, outlined=True,
            hint=t("tiles.sampling.seed_hint"), persistent_hint=True,
        )

        solara.Button(
            t("tiles.sampling.generate_button"), icon_name="mdi-play", color="primary", small=True,
            on_click=on_generate,
        )
        if form_error:
            rv.Alert(type_="error", dense=True, children=[form_error])

        # In-flight job rows.
        for job in sampling_jobs.value:
            if job["status"] == "running":
                rv.Alert(type_="info", dense=True,
                         children=[t("tiles.sampling.job_running", name=job["name"])])
            elif job["status"] == "failed":
                rv.Alert(type_="error", dense=True,
                         children=[t("tiles.sampling.job_failed", name=job["name"], error=job["error"])])

        SampleSetList(
            project=project,
            on_map=frozenset(samples_on_map.value),
            on_toggle_map=on_toggle_map,
            on_remove=set_pending_remove,
            pending=frozenset(samples_pending.value),
        )

        ConfirmDialog(
            open=pending_remove is not None,
            on_cancel=lambda: set_pending_remove(None),
            on_confirm=lambda: (_do_remove(pending_remove), set_pending_remove(None)),
            title=t("tiles.sampling.confirm_remove_title"),
            message=t("tiles.sampling.confirm_remove_message", name=pending_remove or ""),
            confirm_label=t("tiles.sampling.confirm_remove_label"),
        )
