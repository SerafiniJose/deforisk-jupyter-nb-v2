"""Compact pipeline header for the workflow panel.

Replaces the overflowing ``rv.Tabs`` strip: a 9-segment strip mapping the
pipeline (has-outputs / empty / locked, with a ring on the current step), a
title row with the active step's name, output count and position, an
"All steps" jump dialog, and Back / Next buttons.

Semantics are outputs, not completion — steps are revisitable workspaces,
never "done". Segment/badge data all derives from the STEPS registry.
"""

import reacton.ipyvuetify as rv
import solara
import solara.lab

from gui.i18n import plural, t
from gui.store.workflow_steps import STEPS, StepStatus, nav_targets, step_states

# Empty / locked tones stay theme-neutral grey; the HAS_OUTPUTS fill and the
# current-step ring derive from the live Vuetify "primary" colour at render
# time (see PipelineHeader) so the strip matches the app accent in both modes.
_SEG_EMPTY = "rgba(128, 128, 128, 0.28)"
_SEG_LOCKED = (
    "repeating-linear-gradient(90deg, rgba(128,128,128,0.30) 0 3px,"
    " rgba(128,128,128,0.10) 3px 6px)"
)


def _rgba(hex_color: str, alpha: float) -> str:
    """`#rrggbb` -> `rgba(r, g, b, alpha)` (for the translucent current-step ring)."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r}, {g}, {b}, {alpha})"


def count_text(spec, project, aoi_result) -> str:
    """Human badge for a step: outputs count, AOI name, or 'nothing yet'."""
    c = spec.count(project, aoi_result)
    if isinstance(c, str) and c:
        return c
    n = int(c or 0)
    if n > 0 and spec.count_key_one:
        return plural(n, spec.count_key_one, spec.count_key_other)
    return t("workflow.count_empty")


@solara.component
def PipelineHeader(active_step: int, on_navigate, project, aoi_result):
    """Pipeline map + step navigation. Takes the app_state *reactives* (not
    their values — reacton's prop-equality bailout would eat project-only
    changes) plus the active index and a navigate callback."""
    p = project.value
    aoi = aoi_result.value
    states = step_states(p, aoi)
    prev_t, next_t = nav_targets(active_step, states)
    menu_open, set_menu_open = solara.use_state(False)

    # Filled segments + the current-step ring use the app's theme "primary"
    # accent (green in light mode, gold in dark) — the same colour as the Next
    # button and every color="primary" control, so the strip matches the app.
    themes = solara.lab.theme.themes
    primary = themes.dark.primary if solara.lab.use_dark_effective() else themes.light.primary
    ring_css = f"box-shadow: 0 0 0 2px {_rgba(primary, 0.55)};"

    def _seg_style(i: int) -> str:
        """Style of the inner visual bar (the padded wrapper owns the click)."""
        if states[i] is StepStatus.LOCKED:
            bg = f"background: {_SEG_LOCKED};"
        else:
            fill = primary if states[i] is StepStatus.HAS_OUTPUTS else _SEG_EMPTY
            bg = f"background: {fill};"
        ring = ring_css if i == active_step else ""
        return f"width: 100%; height: 7px; border-radius: 3.5px; {bg}{ring}"

    with solara.Column(
        style="gap: 0px; padding: 10px 12px 6px;"
        " border-bottom: 1px solid rgba(128, 128, 128, 0.25);"
    ):
        # Segment strip. Each cell is a padded transparent WRAPPER (~21 px
        # tall) carrying the tooltip and click handler — the 7 px bar inside
        # is purely visual, so the hit area meets the spec's >=20 px.
        with solara.Row(style="gap: 4px; align-items: center;"):
            for i, spec in enumerate(STEPS):
                if states[i] is StepStatus.LOCKED:
                    tip = t(spec.lock_reason_key)
                    cursor = ""
                else:
                    tip = f"{t(spec.label_key)} · {count_text(spec, p, aoi)}"
                    cursor = " cursor: pointer;"
                with rv.Html(
                    tag="div",
                    style_=f"flex: 1; padding: 7px 0;{cursor}",
                    attributes={"title": tip},
                ) as cell:
                    rv.Html(tag="div", style_=_seg_style(i))
                # Attach the click handler UNCONDITIONALLY so the number of
                # use_event hook calls stays constant (one per step) across
                # renders as segments unlock — a conditional call here is a
                # rules-of-hooks violation on the core progressive-unlock
                # interaction. Locked steps simply no-op inside the handler.
                rv.use_event(
                    cell,
                    "click",
                    lambda *_, i=i: states[i] is not StepStatus.LOCKED
                    and on_navigate(i),
                )

        active = STEPS[active_step]
        with solara.Row(style="gap: 8px; align-items: center; padding: 4px 0;"):
            solara.Button(
                icon_name="mdi-chevron-left", icon=True, small=True,
                disabled=prev_t is None,
                on_click=lambda: prev_t is not None and on_navigate(prev_t),
            )
            with solara.Column(style="gap: 0px;"):
                with solara.Row(style="gap: 6px; align-items: center;"):
                    solara.Text(t(active.label_key), style="font-weight: 700;")
                    rv.Chip(children=[count_text(active, p, aoi)], x_small=True)
                solara.Text(
                    t("workflow.step_position", n=active_step + 1, total=len(STEPS)),
                    style="font-size: 11px;",
                    classes=["text--secondary"],
                )
            solara.Button(
                t("workflow.all_steps"), icon_name="mdi-menu-down",
                small=True, outlined=True, text=True,
                on_click=lambda: set_menu_open(True),
                style="margin-left: auto;",
            )
            solara.Button(
                icon_name="mdi-chevron-right", icon=True, small=True,
                color="primary", disabled=next_t is None,
                on_click=lambda: next_t is not None and on_navigate(next_t),
            )

    def _jump(i):
        # ListItemGroup reports None when the active row is toggled off.
        if i is None:
            return
        set_menu_open(False)
        on_navigate(i)

    # Proven popup pattern: top-level use_state + eager rv.Dialog (InfoButton).
    with rv.Dialog(
        v_model=menu_open, on_v_model=set_menu_open, max_width="340px", eager=True
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text(t("workflow.all_steps"))
            with rv.List(dense=True):
                with rv.ListItemGroup(v_model=None, on_v_model=_jump):
                    for i, spec in enumerate(STEPS):
                        locked = states[i] is StepStatus.LOCKED
                        with rv.ListItem(
                            value=i, disabled=locked,
                            input_value=i == active_step,
                        ):
                            # Number for reachable steps, lock glyph otherwise
                            # (matches the approved mockup — no status icons).
                            if locked:
                                rv.Icon(
                                    children=["mdi-lock-outline"],
                                    small=True,
                                    style_="margin-right: 10px;",
                                )
                            else:
                                solara.Text(
                                    str(i + 1),
                                    style="width: 18px; text-align: center;"
                                    " font-weight: 600; font-size: 12px;"
                                    " margin-right: 10px;",
                                )
                            with rv.ListItemContent():
                                rv.ListItemTitle(children=[t(spec.label_key)])
                            solara.Text(
                                t(spec.lock_reason_key)
                                if locked
                                else count_text(spec, p, aoi),
                                style="font-size: 11px; margin-left: 12px;"
                                " text-align: right;",
                                classes=["text--secondary"],
                            )
