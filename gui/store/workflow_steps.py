"""Single source of truth for the workflow pipeline steps.

Order, labels, gates, lock reasons and per-step output counts all
derive from ``STEPS`` — the shell (PipelineHeader), the notification area and
tests must never hand-maintain step numbers again.

Semantics are outputs, not completion: steps are revisitable workspaces. A
step is LOCKED (upstream requirement missing), EMPTY (unlocked, nothing
created yet) or HAS_OUTPUTS (holds at least one artifact). "Current" is view
state owned by the shell, not a status.

Solara-free: gate/count callables take plain values (``project``,
``aoi_result``), never reactives, so everything here is unit-testable without
a render harness.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, List, Optional, Tuple, Union


class StepStatus(Enum):
    LOCKED = "locked"
    EMPTY = "empty"
    HAS_OUTPUTS = "has_outputs"


@dataclass(frozen=True)
class StepSpec:
    key: str
    label_key: str
    unlocked: Callable[[Any, Any], bool]  # (project, aoi_result) -> bool
    lock_reason_key: Optional[str]
    count: Callable[[Any, Any], Union[int, str, None]]
    count_key_one: Optional[str] = None
    count_key_other: Optional[str] = None


def _has_processed_raster(p, aoi) -> bool:
    # Mirrors the legacy check: tolerate DataType enums and plain strings.
    return p is not None and any(
        getattr(v, "data_type", None) == "raster"
        or str(getattr(v, "data_type", "")) == "raster"
        for v in p.processed_variables.values()
    )


def _postprocess_count(p, aoi) -> int:
    if p is None:
        return 0
    from gui.scripts.process_actions import postprocess_output_keys

    return len(postprocess_output_keys(p))


STEPS: List[StepSpec] = [
    StepSpec(
        key="aoi",
        label_key="workflow.tab_aoi",
        unlocked=lambda p, aoi: True,
        lock_reason_key=None,
        count=lambda p, aoi: getattr(aoi, "name", None),
    ),
    StepSpec(
        key="variables",
        label_key="workflow.tab_variables",
        unlocked=lambda p, aoi: aoi is not None,
        lock_reason_key="workflow.lock_variables",
        count=lambda p, aoi: 0 if p is None else len(p.raw_variables),
        count_key_one="workflow.count_variables_one",
        count_key_other="workflow.count_variables_other",
    ),
    StepSpec(
        key="process",
        label_key="workflow.tab_process",
        unlocked=lambda p, aoi: p is not None and bool(p.raw_variables),
        lock_reason_key="workflow.lock_process",
        count=lambda p, aoi: 0 if p is None else len(p.processed_variables),
        count_key_one="workflow.count_rasters_one",
        count_key_other="workflow.count_rasters_other",
    ),
    StepSpec(
        key="postprocess",
        label_key="workflow.tab_postprocess",
        unlocked=lambda p, aoi: p is not None and bool(p.processed_variables),
        lock_reason_key="workflow.lock_postprocess",
        count=_postprocess_count,
        count_key_one="workflow.count_outputs_one",
        count_key_other="workflow.count_outputs_other",
    ),
    StepSpec(
        key="dataset",
        label_key="workflow.tab_dataset",
        unlocked=lambda p, aoi: p is not None and bool(p.processed_variables),
        lock_reason_key="workflow.lock_dataset",
        count=lambda p, aoi: 0 if p is None else len(p.datasets),
        count_key_one="workflow.count_datasets_one",
        count_key_other="workflow.count_datasets_other",
    ),
    StepSpec(
        key="sampling",
        label_key="workflow.tab_sampling",
        unlocked=_has_processed_raster,
        lock_reason_key="workflow.lock_sampling",
        count=lambda p, aoi: 0 if p is None else len(p.samples),
        count_key_one="workflow.count_samplesets_one",
        count_key_other="workflow.count_samplesets_other",
    ),
    StepSpec(
        key="train",
        label_key="workflow.tab_train",
        unlocked=lambda p, aoi: p is not None and bool(p.datasets),
        lock_reason_key="workflow.lock_train",
        count=lambda p, aoi: 0 if p is None else len(p.models),
        count_key_one="workflow.count_models_one",
        count_key_other="workflow.count_models_other",
    ),
    StepSpec(
        key="inference",
        label_key="workflow.tab_inference",
        unlocked=lambda p, aoi: p is not None and bool(p.models),
        lock_reason_key="workflow.lock_inference",
        count=lambda p, aoi: 0 if p is None else len(p.predictions),
        count_key_one="workflow.count_predictions_one",
        count_key_other="workflow.count_predictions_other",
    ),
    StepSpec(
        key="evaluation",
        label_key="workflow.tab_evaluation",
        unlocked=lambda p, aoi: p is not None and bool(p.predictions),
        lock_reason_key="workflow.lock_evaluation",
        count=lambda p, aoi: 0 if p is None else len(p.evaluations),
        count_key_one="workflow.count_evaluations_one",
        count_key_other="workflow.count_evaluations_other",
    ),
]


def step_status(spec: StepSpec, project, aoi_result) -> StepStatus:
    if not spec.unlocked(project, aoi_result):
        return StepStatus.LOCKED
    c = spec.count(project, aoi_result)
    has = bool(c) if isinstance(c, str) else bool(c or 0)
    return StepStatus.HAS_OUTPUTS if has else StepStatus.EMPTY


def step_states(project, aoi_result) -> List[StepStatus]:
    return [step_status(s, project, aoi_result) for s in STEPS]


def nav_targets(
    active: int, states: List[StepStatus]
) -> Tuple[Optional[int], Optional[int]]:
    """Nearest non-locked step on each side of ``active`` (Back/Next)."""
    prev_t = next(
        (i for i in range(active - 1, -1, -1) if states[i] is not StepStatus.LOCKED),
        None,
    )
    next_t = next(
        (
            i
            for i in range(active + 1, len(states))
            if states[i] is not StepStatus.LOCKED
        ),
        None,
    )
    return prev_t, next_t
