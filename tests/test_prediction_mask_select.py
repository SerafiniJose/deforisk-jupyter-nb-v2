"""The Predict dialog lets the user assign which layer masks an ML run.

The mask is generic — any dataset feature can be assigned, and "no mask" is a
valid explicit choice — so the algorithms carry no Hansen-specific assumption.
The deforisk convention (mask to forest at period start) survives only as a
*seed*: a sole ``forest_gfc``-derived layer is preselected as a suggestion the
user can override. The field is only meaningful for the ML families
(GLM/RF/ICAR); JNR and MW resolve their own layers and must never see it.
"""

import types

import ipyvuetify as vw
import reacton
import solara

from gui.i18n import t

# See test_manage_projects_render: warm the translator before the first render.
t("common.cancel")

from gui.widget.prediction_form_dialog import (  # noqa: E402
    NO_MASK,
    PredictionFormDialog,
)


def _find(widget, cls, out=None):
    out = [] if out is None else out
    if isinstance(widget, cls):
        out.append(widget)
    for child in getattr(widget, "children", []) or []:
        if hasattr(child, "children") or isinstance(child, cls):
            _find(child, cls, out)
    return out


def _project(tmp_path, feature_names):
    features = [
        types.SimpleNamespace(name=n, path=f"/tmp/{n}.tif") for n in feature_names
    ]
    dataset = types.SimpleNamespace(name="calibration", features=features)
    return types.SimpleNamespace(
        models={"glm_glm_v1": object(), "mw_calibration_mw": object()},
        datasets={"calibration": dataset},
        predictions={},
        filter_predictions=lambda **kw: [],
        folders=types.SimpleNamespace(project_folder=str(tmp_path)),
    )


def _render(tmp_path, feature_names):
    submitted = []
    box, _rc = reacton.render(
        PredictionFormDialog(
            project=solara.reactive(_project(tmp_path, feature_names)),
            open_=solara.reactive(True),
            on_submit=submitted.append,
        )
    )
    return box, submitted


def _select(box, label):
    return next((s for s in _find(box, vw.Select) if s.label == label), None)


def _mask_select(box):
    return _select(box, t("tiles.inference.mask_layer_label"))


def _pick(box, model_key, dataset_key="calibration"):
    """Drive the two selects the mask field depends on."""
    _select(box, t("tiles.inference.model_select_label")).v_model = model_key
    if dataset_key is not None:
        _select(box, t("tiles.inference.dataset_select_label")).v_model = dataset_key


def _create(box):
    label = t("tiles.inference.run_button")
    btn = next(b for b in _find(box, vw.Btn) if label in str(b.children))
    btn.fire_event("click", {})


def _item_values(select):
    return [i["value"] for i in select.items]


def test_mask_select_is_hidden_until_a_model_and_dataset_are_chosen(tmp_path):
    """No model or dataset yet — there is nothing to choose between."""
    box, _ = _render(tmp_path, ["forest_gfc_tc30"])
    assert _mask_select(box) is None


def test_mask_select_offers_every_feature_plus_no_mask(tmp_path):
    """Any dataset layer can be the mask, and 'no mask' is always on offer."""
    box, _ = _render(tmp_path, ["altitude", "forest_gfc_tc30", "my_forest_mask"])
    _pick(box, "glm_glm_v1")
    sel = _mask_select(box)
    assert sel is not None
    assert _item_values(sel) == [
        NO_MASK,
        "altitude",
        "forest_gfc_tc30",
        "my_forest_mask",
    ]


def test_sole_forest_layer_is_seeded_as_the_suggestion(tmp_path):
    """One Hansen-derived layer: seed it, the deforisk-style default."""
    box, _ = _render(tmp_path, ["altitude", "forest_gfc_tc30"])
    _pick(box, "glm_glm_v1")
    assert _mask_select(box).v_model == "forest_gfc_tc30"


def test_two_forest_layers_start_unset(tmp_path):
    """Two forest definitions: the app must not pick one on the user's behalf."""
    box, _ = _render(tmp_path, ["forest_gfc_tc30", "forest_gfc_tc75"])
    _pick(box, "glm_glm_v1")
    assert _mask_select(box).v_model in ("", None)


def test_no_forest_layer_starts_unset(tmp_path):
    """Without a forest layer nothing is seeded — not even 'no mask'."""
    box, _ = _render(tmp_path, ["altitude", "forest_tmf"])
    _pick(box, "glm_glm_v1")
    assert _mask_select(box).v_model in ("", None)


def test_benchmark_family_never_shows_the_field(tmp_path):
    """JNR/MW resolve their own layers — the field would be meaningless."""
    box, _ = _render(tmp_path, ["forest_gfc_tc30"])
    _pick(box, "mw_calibration_mw")
    assert _mask_select(box) is None


def test_submitted_entry_carries_the_chosen_mask_feature(tmp_path):
    """The pick reaches the tile as entry["mask_feature"]."""
    box, submitted = _render(tmp_path, ["forest_gfc_tc30", "forest_gfc_tc75"])
    _pick(box, "glm_glm_v1")
    _mask_select(box).v_model = "forest_gfc_tc75"
    _create(box)
    assert submitted and submitted[0]["mask_feature"] == "forest_gfc_tc75"
    assert submitted[0]["kind"] == "model"


def test_no_mask_choice_submits_none(tmp_path):
    """The 'No mask' choice reaches the runner as None: predict everywhere."""
    box, submitted = _render(tmp_path, ["altitude", "forest_tmf"])
    _pick(box, "glm_glm_v1")
    _mask_select(box).v_model = NO_MASK
    _create(box)
    assert submitted and submitted[0]["mask_feature"] is None


def test_unset_mask_blocks_the_run(tmp_path):
    """An ML run cannot launch until the user assigns a mask (or 'no mask')."""
    box, submitted = _render(tmp_path, ["forest_gfc_tc30", "forest_gfc_tc75"])
    _pick(box, "glm_glm_v1")
    _create(box)
    assert not submitted


def test_benchmark_entry_does_not_carry_the_key(tmp_path):
    """A benchmark entry stays exactly as it was before this feature."""
    box, submitted = _render(tmp_path, ["forest_gfc_tc30"])
    _pick(box, "mw_calibration_mw")
    _create(box)
    assert submitted and "mask_feature" not in submitted[0]


def test_dialog_and_runner_share_one_candidate_source(tmp_path):
    """Candidates and the forest seed live next to run_inference, not here."""
    import inspect

    import gui.widget.prediction_form_dialog as mod

    src = inspect.getsource(mod)
    assert "mask_layer_candidates" in src
    assert "suggested_mask_layer" in src
    # the resolve_predefined(...) == "forest_gfc" test must not be duplicated here
    assert "resolve_predefined" not in src
