"""Registry integrity + gate parity for the workflow pipeline header.

The registry is the single source of truth for step order/labels/gates/counts
(spec: docs/superpowers/specs/2026-07-09-workflow-pipeline-header-design.md).
Gates must reproduce the legacy disabled_flags truth table exactly.
"""

import json
from types import SimpleNamespace

from gui import i18n
from gui.store.workflow_steps import (
    STEPS,
    StepStatus,
    nav_targets,
    step_states,
    step_status,
)


def _aoi():
    return SimpleNamespace(name="Acre")


def _project():
    from spatialrisk.project import Project

    return Project(project_name="t")


def test_registry_shape():
    assert [s.key for s in STEPS] == [
        "aoi", "variables", "process", "postprocess", "dataset",
        "sampling", "train", "inference", "evaluation",
    ]
    assert len({s.key for s in STEPS}) == 9
    # AOI is always reachable, so it carries no lock reason.
    assert STEPS[0].lock_reason_key is None
    for spec in STEPS[1:]:
        assert spec.lock_reason_key, spec.key


def test_gate_matrix_matches_legacy_disabled_flags():
    """Walk the pipeline forward, asserting each artifact unlocks exactly the
    steps the old inline disabled_flags did (gui/solara_app.py)."""
    p = _project()
    aoi = _aoi()

    def unlocked():
        return [s.unlocked(p, aoi) for s in STEPS]

    # Project + AOI, nothing else: only AOI and Variables reachable.
    assert unlocked() == [True, True, False, False, False, False, False, False, False]

    p.raw_variables["v"] = SimpleNamespace(data_type="raster")
    assert unlocked() == [True, True, True, False, False, False, False, False, False]

    # A processed VECTOR unlocks Post-process + Dataset but NOT Sampling.
    p.processed_variables["v"] = SimpleNamespace(data_type="vector")
    assert unlocked() == [True, True, True, True, True, False, False, False, False]

    # A processed RASTER unlocks Sampling.
    p.processed_variables["r"] = SimpleNamespace(data_type="raster")
    assert unlocked() == [True, True, True, True, True, True, False, False, False]

    p.datasets["d"] = object()
    assert unlocked()[6] is True and unlocked()[7] is False

    p.models["m"] = object()
    assert unlocked()[7] is True and unlocked()[8] is False

    p.predictions["pr"] = object()
    assert unlocked()[8] is True


def test_gates_with_no_project_and_no_aoi():
    states = step_states(None, None)
    assert states[0] is StepStatus.EMPTY  # AOI reachable, nothing selected
    assert all(s is StepStatus.LOCKED for s in states[1:])


def test_step_status_empty_vs_has_outputs():
    p = _project()
    aoi = _aoi()
    variables = STEPS[1]
    assert step_status(variables, p, aoi) is StepStatus.EMPTY
    p.raw_variables["v"] = SimpleNamespace(data_type="raster")
    assert step_status(variables, p, aoi) is StepStatus.HAS_OUTPUTS
    # Train stays LOCKED regardless of its own (empty) count.
    assert step_status(STEPS[6], p, aoi) is StepStatus.LOCKED


def test_aoi_count_is_the_aoi_name():
    assert STEPS[0].count(None, _aoi()) == "Acre"
    assert STEPS[0].count(None, None) is None


def test_postprocess_count_zero_without_outputs():
    p = _project()
    p.processed_variables["v"] = SimpleNamespace(
        data_type="raster", tags=[], processing_history=[], name="v"
    )
    assert STEPS[3].count(p, _aoi()) == 0
    assert STEPS[3].count(None, None) == 0


def test_nav_targets_skip_locked():
    states = [
        StepStatus.HAS_OUTPUTS, StepStatus.HAS_OUTPUTS, StepStatus.LOCKED,
        StepStatus.EMPTY, StepStatus.LOCKED, StepStatus.LOCKED,
        StepStatus.LOCKED, StepStatus.LOCKED, StepStatus.LOCKED,
    ]
    assert nav_targets(3, states) == (1, None)
    assert nav_targets(0, states) == (None, 1)
    assert nav_targets(1, states) == (0, 3)


def _flat(d, prefix=""):
    out = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flat(v, key))
        else:
            out[key] = v
    return out


def _lang_keys(lang):
    merged = {}
    for f in sorted((i18n.MESSAGES_DIR / lang).glob("*.json")):
        merged.update(_flat(json.loads(f.read_text())))
    return set(merged)


def test_registry_keys_resolve_in_both_locales():
    for lang in ("en", "es-ES"):
        keys = _lang_keys(lang)
        for spec in STEPS:
            assert spec.label_key in keys, (lang, spec.key)
            if spec.lock_reason_key:
                assert spec.lock_reason_key in keys, (lang, spec.key)
            if spec.count_key_one:
                assert spec.count_key_one in keys, (lang, spec.key)
                assert spec.count_key_other in keys, (lang, spec.key)
        for extra in ("workflow.step_position", "workflow.all_steps",
                      "workflow.count_empty"):
            assert extra in keys, (lang, extra)
