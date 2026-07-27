"""The Predict dialog lets the user pick which forest layer masks an ML run.

A dataset can hold more than one Hansen forest layer now that the tree-cover
threshold is baked into the variable name (``forest_gfc_tc30`` /
``forest_gfc_tc75``). ``run_inference`` refuses to guess between them, so the
choice has to be made here — the same shape as the Evaluation dialog's forest
select. The field is only meaningful for the ML families (GLM/RF/ICAR); JNR and
MW resolve their own layers and must never see it.
"""

import types

import ipyvuetify as vw
import reacton
import solara

from gui.i18n import t

# See test_manage_projects_render: warm the translator before the first render.
t("common.cancel")

from gui.widget.prediction_form_dialog import PredictionFormDialog  # noqa: E402


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


def _forest_select(box):
    return _select(box, t("tiles.inference.forest_feature_label"))


def _pick(box, model_key, dataset_key="calibration"):
    """Drive the two selects the forest field depends on."""
    _select(box, t("tiles.inference.model_select_label")).v_model = model_key
    if dataset_key is not None:
        _select(box, t("tiles.inference.dataset_select_label")).v_model = dataset_key


def _create(box):
    label = t("tiles.inference.run_button")
    btn = next(b for b in _find(box, vw.Btn) if label in str(b.children))
    btn.fire_event("click", {})


def test_forest_select_is_hidden_until_a_model_and_dataset_are_chosen(tmp_path):
    """No model or dataset yet — there is nothing to choose between."""
    box, _ = _render(tmp_path, ["forest_gfc_tc30"])
    assert _forest_select(box) is None


def test_forest_select_lists_only_the_datasets_forest_layers(tmp_path):
    """Only features resolving to the forest_gfc catalogue key are offered."""
    box, _ = _render(
        tmp_path, ["altitude", "forest_gfc_tc30", "forest_tmf", "forest_gfc_tc75"]
    )
    _pick(box, "glm_glm_v1")
    sel = _forest_select(box)
    assert sel is not None
    assert sel.items == ["forest_gfc_tc30", "forest_gfc_tc75"]


def test_sole_candidate_is_preselected(tmp_path):
    """One forest layer needs no decision — seed it, like default_forest_key does."""
    box, _ = _render(tmp_path, ["altitude", "forest_gfc_tc30"])
    _pick(box, "glm_glm_v1")
    assert _forest_select(box).v_model == "forest_gfc_tc30"


def test_ambiguous_dataset_starts_empty(tmp_path):
    """Two candidates: the app must not pick one on the user's behalf."""
    box, _ = _render(tmp_path, ["forest_gfc_tc30", "forest_gfc_tc75"])
    _pick(box, "glm_glm_v1")
    assert _forest_select(box).v_model in ("", None)


def test_benchmark_family_never_shows_the_field(tmp_path):
    """JNR/MW resolve their own layers — the field would be meaningless."""
    box, _ = _render(tmp_path, ["forest_gfc_tc30"])
    _pick(box, "mw_calibration_mw")
    assert _forest_select(box) is None


def test_submitted_entry_carries_the_chosen_forest_feature(tmp_path):
    """The pick reaches the tile as entry["forest_feature"]."""
    box, submitted = _render(tmp_path, ["forest_gfc_tc30", "forest_gfc_tc75"])
    _pick(box, "glm_glm_v1")
    _forest_select(box).v_model = "forest_gfc_tc75"
    _create(box)
    assert submitted and submitted[0]["forest_feature"] == "forest_gfc_tc75"
    assert submitted[0]["kind"] == "model"


def test_benchmark_entry_does_not_carry_the_key(tmp_path):
    """A benchmark entry stays exactly as it was before this feature."""
    box, submitted = _render(tmp_path, ["forest_gfc_tc30"])
    _pick(box, "mw_calibration_mw")
    _create(box)
    assert submitted and "forest_feature" not in submitted[0]


def test_dialog_and_runner_share_one_candidate_predicate(tmp_path):
    """The predicate lives next to run_inference, not copied into the dialog."""
    import inspect

    import gui.widget.prediction_form_dialog as mod

    src = inspect.getsource(mod)
    assert "forest_feature_candidates" in src
    # the resolve_predefined(...) == "forest_gfc" test must not be duplicated here
    assert "resolve_predefined" not in src
