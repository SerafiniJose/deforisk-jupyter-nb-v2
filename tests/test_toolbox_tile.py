"""ToolboxTile wiring (browserless, structural)."""

import inspect

import solara

from gui.tile import toolbox_tile


def test_module_exposes_job_and_map_reactives():
    """Both survive re-renders, so the shell can reset them on project switch."""
    assert isinstance(toolbox_tile.allocation_jobs, solara.Reactive)
    assert isinstance(toolbox_tile.density_on_map, solara.Reactive)
    assert toolbox_tile.allocation_jobs.value == []


def test_worker_runs_in_context_with_tracked_job_and_writing():
    """The worker follows the app's background-job contract."""
    src = inspect.getsource(toolbox_tile)
    assert "spawn_in_context" in src
    assert "tracked_job" in src
    assert "writing(" in src
    assert "update_job" in src


def test_tile_takes_project_reactive_not_app_state():
    """Tiles receive the project reactive directly (see the tile contract)."""
    sig = inspect.signature(toolbox_tile.ToolboxTile.f)
    assert list(sig.parameters)[0] == "project"
    assert "app_state" not in inspect.getsource(toolbox_tile)


def test_tile_mounts_with_a_project():
    """The tile renders end to end without a map or a client."""
    import reacton

    from gui.i18n import t
    from spatialrisk.project import Project

    t("common.cancel")  # warm the translator before the first render
    box, _rc = reacton.render(
        toolbox_tile.ToolboxTile(project=solara.reactive(Project(project_name="p")))
    )
    assert box is not None


# --- two-pane shell + latest-run card (mock fidelity) -------------------


def _find(widget, cls, out=None):
    out = [] if out is None else out
    if isinstance(widget, cls):
        out.append(widget)
    for child in getattr(widget, "children", []) or []:
        if hasattr(child, "children") or isinstance(child, cls):
            _find(child, cls, out)
    return out


def _all_text(box):
    import ipyvuetify as vw

    out = []
    for cls in (vw.Html, vw.Btn, vw.Chip):
        for w in _find(box, cls):
            out.extend(str(c) for c in (w.children or []) if isinstance(c, str))
    return " ".join(out)


def test_tile_has_a_tool_registry():
    """Future tools are one registry entry away — the mock's tool list."""
    tools = toolbox_tile._TOOLS
    assert [tool["key"] for tool in tools] == ["allocation"]
    assert all("label_key" in tool and "icon" in tool for tool in tools)


def test_tile_renders_the_tool_list_pane():
    """The dialog shows the tool list beside the selected tool's panel."""
    import reacton

    from gui.i18n import t
    from spatialrisk.project import Project

    t("common.cancel")
    box, _rc = reacton.render(
        toolbox_tile.ToolboxTile(project=solara.reactive(Project(project_name="p")))
    )
    assert t("toolbox.tool_allocation") in _all_text(box)


def test_tile_has_no_latest_run_card():
    """Runs live in the table only; the headline card is gone (2026-07-30 spec)."""
    import reacton

    from gui.i18n import t
    from spatialrisk.allocations import AllocationRun
    from spatialrisk.project import Project

    t("common.cancel")
    project = Project(project_name="p")
    project.allocations["reserve_bbb22222"] = AllocationRun(
        name="reserve",
        run_id="bbb22222",
        created_at="2026-07-29T10:00:00",
        borders_file="/b.gpkg",
        defor_juris_ha=20000.0,
        years_forecast=4,
        annual_ha=312.4,
        total_ha=1249.6,
        out_dir="/out",
        csv_path="/out/defor_project.csv",
    )

    box, _rc = reacton.render(
        toolbox_tile.ToolboxTile(project=solara.reactive(project))
    )

    assert "312.4" in _all_text(box)  # the run still renders, in the table
    src = inspect.getsource(toolbox_tile)
    assert "latest_result" not in src
    assert "AllocationResultCard" not in src


def test_body_has_no_heading_and_defers_description_to_an_info_button():
    """The body starts at the panes; the description is an InfoButton popup.

    The dialog frame already says 'Tools', so the body repeats neither the
    heading nor the subtitle.
    """
    src = inspect.getsource(toolbox_tile)
    assert "InfoButton" in src
    assert "toolbox.allocation.description" in src  # still the popup's content
    assert "toolbox.title" not in src  # no duplicated heading
    assert "toolbox.subtitle" not in src
    assert 'solara.Text(t("toolbox.allocation.description")' not in src


def _rail_button(box):
    """The rail's tool button: the Btn whose icon is the tool's mdi icon."""
    import ipyvuetify as vw

    for btn in _find(box, vw.Btn):
        icons = [str(i.children[0]) for i in _find(btn, vw.Icon) if i.children]
        if "mdi-earth-remove" in icons:
            return btn
    return None


def test_rail_is_icon_only_with_primary_selection():
    """The tool rail mirrors the app drawer: icon button, primary when active."""
    import reacton

    from gui.i18n import t
    from spatialrisk.project import Project

    t("common.cancel")
    box, _rc = reacton.render(
        toolbox_tile.ToolboxTile(project=solara.reactive(Project(project_name="p")))
    )

    btn = _rail_button(box)
    assert btn is not None
    assert btn.icon  # icon-only, no text label on the button
    assert btn.color == "primary"  # the (only) tool is selected
    assert not [c for c in (btn.children or []) if isinstance(c, str) and c.strip()]


def test_panel_header_carries_title_and_info_button():
    """The pane header owns the tool title; the description stays an InfoButton."""
    src = inspect.getsource(toolbox_tile)
    assert "InfoButton" in src
    assert "solara.Tooltip" in src  # rail buttons announce their tool name
