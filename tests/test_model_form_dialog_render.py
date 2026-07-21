"""The New Model dialog mounts, and its help icon lives on the select itself.

The model help moved from a sibling ``InfoButton`` into the select's own
``prepend-inner`` icon. That only works if two things hold at once, and neither
is visible to a source-substring check: the icon prop must reach the widget,
and a ``click:prepend-inner`` listener must be registered on it. Without the
listener Vuetify's ``genIcon`` neither emits the event nor stops the click
propagating, so the icon would be dead *and* would drop the model menu open.
"""

import ipyvuetify as vw
import reacton
import solara

from gui.i18n import t

# See test_manage_projects_render: warm the translator before the first render.
t("common.cancel")

from spatialrisk.project import Project  # noqa: E402
from gui.widget.model_form_dialog import ModelFormDialog  # noqa: E402


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
    sel = _model_select(_render_dialog(tmp_path))
    assert sel.prepend_inner_icon == "mdi-information-outline"
    # The CSS that parks the icon on the right hangs off this class.
    assert "field-info-icon" in (sel.class_ or "")


def test_help_icon_click_is_wired(tmp_path):
    sel = _model_select(_render_dialog(tmp_path))
    assert "click:prepend-inner" in sel._event_handlers_map


def test_model_select_owns_a_message_strip(tmp_path):
    """The hint is what spaces the model select off the dataset select now —
    no hand-tuned margin under a flex row."""
    sel = _model_select(_render_dialog(tmp_path))
    assert sel.persistent_hint is True
    assert sel.hint
    assert not sel.hide_details  # unset — the strip must not be suppressed
