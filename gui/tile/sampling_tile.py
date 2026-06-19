"""Step 5 — Sampling tile.

Generates persistent, selectable sample sets from a registered Dataset and
lets the user draw them on the map. Each sample set materializes a training
table (CSV) + a points GeoPackage; Step 6 (Train) selects one to fit on.
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

SAMPLING_STRATEGIES = ["random", "stratified", "systematic", "legacy"]

# Module-level reactives shared across re-renders.
sampling_jobs = solara.reactive([])
samples_on_map = solara.reactive(set())


def _sample_layer_key(name: str) -> str:
    return f"sample_{name}"


def _update_job(job_id, *, skip_if_cancelled=True, **changes):
    update_job(sampling_jobs, job_id, skip_if_cancelled=skip_if_cancelled, **changes)


def _run_sampling(job_id, name, dataset_name, strategy, n_samples, seed, project_reactive):
    """Generate a SampleSet in the background and register it on the project."""
    try:
        from spatialrisk.sampleset import SampleSet

        p = project_reactive.value
        folder = p.folders.samples_folder
        sample_set = SampleSet(
            project=p, name=name, dataset_name=dataset_name, strategy=strategy,
            n_samples=n_samples, seed=seed,
            table_path=folder / f"{name}.csv",
            points_path=folder / f"{name}.gpkg",
        )
        sample_set.generate()

        # Mutate-then-replace so the registry change re-renders the UI.
        p.add_sample_set(sample_set, auto_save=True)
        project_reactive.set(p.model_copy())

        _update_job(
            job_id, status="completed",
            n_total=sample_set.n_total, n_event=sample_set.n_event,
            n_forest=sample_set.n_forest,
        )
        logger.info("Sample set generated: %s (%d rows)", name, sample_set.n_total)
    except Exception as exc:
        logger.exception("Sampling failed for %s", name)
        _update_job(job_id, status="failed", error=str(exc))


@solara.component
def SamplingTile(project, map_=None):
    """Sampling tab: generate persistent sample sets and add them to the map."""
    p = project.value
    if p is None:
        return
    if not p.datasets:
        solara.Info("Create a dataset first (Step 4 — Dataset).")
        return

    dataset_keys = sorted(p.datasets.keys())
    selected_dataset, set_selected_dataset = solara.use_state(dataset_keys[0])
    name, set_name = solara.use_state("")
    strategy, set_strategy = solara.use_state("random")
    n_samples, set_n_samples = solara.use_state(10000)
    seed, set_seed = solara.use_state(1234)
    form_error, set_form_error = solara.use_state(None)

    def on_generate():
        set_form_error(None)
        nm = (name or "").strip()
        if not nm:
            set_form_error("Give the sample set a name.")
            return
        if nm in p.samples:
            set_form_error(f"A sample set named '{nm}' already exists.")
            return
        if selected_dataset not in p.datasets:
            set_form_error("Select a valid dataset.")
            return

        job_id = str(uuid.uuid4())[:8]
        sampling_jobs.set(list(sampling_jobs.value) + [{
            "id": job_id, "name": nm, "dataset_name": selected_dataset,
            "status": "running", "error": None,
            "n_total": None, "n_event": None, "n_forest": None,
        }])
        spawn_in_context(
            _run_sampling,
            (job_id, nm, selected_dataset, strategy, n_samples, seed, project),
        )
        set_name("")
        logger.info("Sampling started: %s on %s (job=%s)", nm, selected_dataset, job_id)

    def on_toggle_map(key):
        if map_ is None:
            return
        ss = p.samples.get(key)
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
            set_form_error(f"Could not toggle sample set on map: {exc}")

    pending_remove, set_pending_remove = solara.use_state(None)

    def _do_remove(key):
        if map_ is not None and key in samples_on_map.value:
            remove_sample_points_from_map(map_, _sample_layer_key(key))
            samples_on_map.set(samples_on_map.value - {key})
        cur = project.value
        if cur is not None and key in cur.samples:
            cur.delete_sample_set(key, auto_save=True)
            project.set(cur.model_copy())

    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 5 — Sampling")
        solara.Text(
            "Draw a persistent, named sample set from a dataset. Sample sets are "
            "selectable in Train and can be added to the map."
        )

        rv.Select(
            label="Dataset", items=dataset_keys, v_model=selected_dataset,
            on_v_model=set_selected_dataset, dense=True, outlined=True,
        )
        rv.TextField(
            label="Sample set name", v_model=name,
            on_v_model=set_name, dense=True, outlined=True,
        )
        rv.Select(
            label="Strategy", items=SAMPLING_STRATEGIES, v_model=strategy,
            on_v_model=set_strategy, dense=True, outlined=True,
        )
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
            title="Delete sample set?",
            message=(
                f"Delete sample set '{pending_remove}'? This removes it from the "
                "project and deletes its files. This cannot be undone."
            ),
            confirm_label="Delete",
        )
