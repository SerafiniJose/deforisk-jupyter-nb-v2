"""PipelineHeader: badge text logic, render smoke, and codebase-gotcha
contracts (no rv.Btn(on_click), anchored dropdown, reactives-as-props)."""

import inspect
from types import SimpleNamespace

import reacton
import solara


def _project_with(**counts):
    from spatialrisk.project import Project

    p = Project(project_name="t")
    for _ in range(counts.get("raw", 0)):
        p.raw_variables[f"v{_}"] = SimpleNamespace(data_type="raster")
    return p


def test_count_text_pluralizes_and_falls_back():
    from gui.store.workflow_steps import STEPS
    from gui.widget.pipeline_header import count_text

    aoi = SimpleNamespace(name="Acre")
    p = _project_with(raw=2)
    variables = STEPS[1]
    assert count_text(variables, p, aoi) == "2 variables"
    assert count_text(variables, _project_with(raw=1), aoi) == "1 variable"
    assert count_text(variables, _project_with(raw=0), aoi) == "nothing yet"
    # AOI step: the badge is the AOI name (a string count).
    assert count_text(STEPS[0], None, aoi) == "Acre"
    assert count_text(STEPS[0], None, None) == "nothing yet"


def test_render_smoke_empty_session():
    from gui.widget.pipeline_header import PipelineHeader

    project = solara.reactive(None, equals=lambda a, b: a is b)
    aoi = solara.reactive(None)
    box, rc = reacton.render(
        PipelineHeader(
            active_step=0, on_navigate=lambda i: None,
            project=project, aoi_result=aoi,
        ),
        handle_error=False,
    )
    rc.close()


def test_render_smoke_populated_project():
    from gui.widget.pipeline_header import PipelineHeader

    project = solara.reactive(_project_with(raw=3), equals=lambda a, b: a is b)
    aoi = solara.reactive(SimpleNamespace(name="Acre"))
    box, rc = reacton.render(
        PipelineHeader(
            active_step=1, on_navigate=lambda i: None,
            project=project, aoi_result=aoi,
        ),
        handle_error=False,
    )
    rc.close()


def test_jump_menu_is_a_dropdown_not_a_modal():
    """The unified title activator opens a dropdown anchored under it, not a
    centred modal — and its rows keep the working click primitives."""
    from gui.widget import pipeline_header

    src = inspect.getsource(pipeline_header)
    assert "solara.lab.Menu(" in src     # anchored dropdown
    assert "rv.Dialog(" not in src       # no modal popup
    # use_activator_width=False sends min-width="auto" to v-menu; Vuetify's
    # off-screen guard then computes NaN and stops clamping, so the dropdown
    # runs off the right edge of the viewport.
    assert "use_activator_width=False" not in src


def test_no_dead_click_patterns():
    from gui.widget import pipeline_header

    src = inspect.getsource(pipeline_header)
    assert "rv.Btn(" not in src          # dead-click gotcha
    assert "rv.use_event(" in src        # segment + dropdown-row clicks
    # Reads reactives inside the component (prop-equality bailout).
    assert "project.value" in src and "aoi_result.value" in src


def test_unified_activator_replaces_all_steps_button():
    """Title + badge + count + caret are ONE menu activator; the separate
    "ALL STEPS" button and the "Step 7 of 9" subtitle are gone."""
    from gui.widget import pipeline_header

    src = inspect.getsource(pipeline_header)
    assert "workflow.step_badge" in src       # n/total badge in the activator
    assert "workflow.all_steps" not in src    # old button label gone
    assert "workflow.step_position" not in src  # old subtitle gone
    assert "mdi-menu-down" in src             # caret affordance kept
    assert "sr-step-jump:hover" in src        # hover tint on the activator
    assert 'class_="sr-step-jump"' in src   # ...and the class is on the activator
