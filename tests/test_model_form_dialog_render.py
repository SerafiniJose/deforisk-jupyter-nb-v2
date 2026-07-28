"""The New Model dialog mounts, and its help icon lives on the select itself.

The model help moved from a sibling ``InfoButton`` into the select's own
``prepend-inner`` icon. That only works if two things hold at once, and neither
is visible to a source-substring check: the icon prop must reach the widget,
and a ``click:prepend-inner`` listener must be registered on it. Without the
listener Vuetify's ``genIcon`` neither emits the event nor stops the click
propagating, so the icon would be dead *and* would drop the model menu open.
"""

import inspect

import ipyvuetify as vw
import reacton
import solara

from gui.i18n import t

# See test_manage_projects_render: warm the translator before the first render.
t("common.cancel")

import gui.widget.model_form_dialog as mfd  # noqa: E402
from gui.widget.model_form_dialog import ModelFormDialog  # noqa: E402
from spatialrisk.project import Project  # noqa: E402


def _find(widget, cls, out=None):
    out = [] if out is None else out
    if isinstance(widget, cls):
        out.append(widget)
    for child in getattr(widget, "children", []) or []:
        if hasattr(child, "children") or isinstance(child, cls):
            _find(child, cls, out)
    return out


def _render_dialog(tmp_path):
    project = solara.reactive(Project(project_name="p"))
    box, _rc = reacton.render(
        ModelFormDialog(
            project=project, open_=solara.reactive(True), on_submit=lambda entry: None
        )
    )
    return box


def _model_select(box):
    selects = _find(box, vw.Select)
    assert selects, "no v-select rendered"
    return selects[0]


def test_model_select_carries_the_help_icon(tmp_path):
    """The model select carries its own help icon (prepend-inner)."""
    sel = _model_select(_render_dialog(tmp_path))
    assert sel.prepend_inner_icon == "mdi-information-outline"
    # The CSS that parks the icon on the right hangs off this class.
    assert "field-info-icon" in (sel.class_ or "")


def test_help_icon_click_is_wired(tmp_path):
    """The help icon has a click:prepend-inner listener registered."""
    sel = _model_select(_render_dialog(tmp_path))
    assert "click:prepend-inner" in sel._event_handlers_map


def test_model_select_owns_a_message_strip(tmp_path):
    """The model select owns its own hint strip.

    The hint is what spaces the model select off the dataset select now —
    no hand-tuned margin under a flex row.
    """
    sel = _model_select(_render_dialog(tmp_path))
    assert sel.persistent_hint is True
    assert sel.hint
    assert not sel.hide_details  # unset — the strip must not be suppressed


def test_no_formula_textarea_for_default_benchmark(tmp_path):
    """The default (non-formula) model must not render a formula textarea."""
    box = _render_dialog(tmp_path)
    assert not _find(box, vw.Textarea), "benchmark must not render a formula field"


def test_formula_textarea_renders_for_glm(tmp_path):
    """Switching to glm renders the prefilled formula textarea."""
    box = _render_dialog(tmp_path)
    sel = _model_select(box)
    sel.v_model = "glm"  # reacton observes the trait -> on_v_model fires
    areas = _find(box, vw.Textarea)
    assert areas, "glm must render the formula textarea"
    assert areas[0].label == t("tiles.train.formula_label")


def test_formula_contract():
    """Source-contract check for the formula prefill/validate/submit wiring."""
    src = inspect.getsource(mfd)
    # single shared formula state, prefilled off the render path
    assert "MODEL_HAS_FORMULA" in src
    assert "use_task" in src and "generate_patsy_formula" in src
    assert "asyncio.to_thread" in src
    # validated via the pure rule checker, submitted with the entry
    assert "validate_formula" in src
    assert '"formula"' in src
    # save blocked while the prefill is still running
    assert "error_formula_generating" in src
    # reopen regression: reset() must bump a nonce that is threaded into both
    # the prefill task's dependencies and the apply-effect's dependencies, or
    # a reopen of the eagerly-mounted dialog never regenerates the formula.
    reset_src = src[src.index("def reset()") : src.index("def validate()")]
    assert "prefill_nonce" in reset_src
    task_deps_src = src[src.index("use_task(") : src.index("async def prefill_formula")]
    assert "prefill_nonce" in task_deps_src
    effect_src = src[src.index("solara.use_effect(") : src.index("def reset()")]
    assert "prefill_nonce" in effect_src
    # users never see categorical levels: the prefill is generated without
    # them (fit re-arms via inject_categorical_levels) and the details dialog
    # strips them from the stored formula.
    assert "include_levels=False" in src
    assert "strip_categorical_levels" in src
    # failed re-generation for a newly-selected dataset must not leave the
    # previous dataset's stale formula sitting in the textarea
    apply_src = src[src.index("def _apply_prefill()") : src.index("solara.use_effect(")]
    assert "prefill_formula.error" in apply_src
