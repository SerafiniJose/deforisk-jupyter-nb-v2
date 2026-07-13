"""Floating, collapsible log console pinned to the map's lower-right corner.

Render-only: reads ``log_records`` (fed by ``gui.scripts.log_bridge``) and shows
the latest milestones. Collapsed by default via a self-managed ``rv.ExpansionPanels``
(no Python toggle state — that pattern is unreliable here). Newest line first, so the
latest is always visible without fragile auto-scroll-to-bottom JS.
"""

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.scripts.log_bridge import install_log_console_handler, log_records

# Level -> Vuetify chip colour (matches the colorlog scheme in logging_config.toml).
# Vuetify theme tokens so the level chips follow the app palette in both modes.
# DEBUG keeps a literal grey: it is deliberately neutral, not a theme tone.
_LEVEL_COLOR = {
    "DEBUG": "grey",
    "INFO": "success",
    "WARNING": "warning",
    "ERROR": "error",
    "CRITICAL": "error",
}


@solara.component
def LogConsole():
    records = log_records.value

    # Bind this session's kernel context to the handler so background-thread emits
    # (asyncio.to_thread) can attach it and reach the browser. No-op without a
    # context (e.g. tests) — file/console handlers still record everything.
    def _bind_context():
        try:
            from solara.server import kernel_context

            install_log_console_handler().bind_context(
                kernel_context.get_current_context()
            )
        except Exception:
            pass

    solara.use_effect(_bind_context, [])

    # Pin to the MAP's bottom-right, not the viewport's. pysepal syncs the right
    # panel's live width and the narrow-mode bottom-panel height to CSS vars on
    # the document root precisely for floating components that track the map
    # edges, so offsetting by them keeps the console clear of the right panel
    # (and the bottom sheet on narrow screens) as it opens/closes.
    # Width is driven by Vuetify's own active-panel class via :has() — narrow
    # when collapsed, wider when expanded — so there is still no Python state.
    solara.Style(
        ".log-console-wrap { width: 210px; transition: width 0.25s ease; }"
        ".log-console-wrap:has(.v-expansion-panel--active) { width: 360px; }"
    )
    with solara.Column(
        classes=["log-console-wrap"],
        style=(
            "position: fixed; z-index: 1000; max-width: 90vw; "
            "right: calc(var(--sepal-notification-right-offset, 0px) + 16px); "
            "bottom: calc(var(--sepal-bottom-reserved, 0px) + 16px);"
        ),
    ):
        # No v_model -> Vuetify owns open/close; panels start collapsed.
        with rv.ExpansionPanels(flat=True, hover=True):
            with rv.ExpansionPanel():
                with rv.ExpansionPanelHeader():
                    with solara.Row(style="align-items: center; gap: 8px;"):
                        rv.Icon(children=["mdi-text-box-outline"], small=True)
                        solara.Text(
                            t("widgets.log_console.header"),
                            style="font-weight: 600; white-space: nowrap;",
                        )
                        rv.Chip(children=[str(len(records))], x_small=True)
                with rv.ExpansionPanelContent():
                    with solara.Column(
                        style=(
                            "height: 260px; overflow-y: auto; "
                            "font-family: monospace; font-size: 12px; gap: 2px;"
                        )
                    ):
                        if not records:
                            solara.Text(
                                t("widgets.log_console.empty"),
                                classes=["text--secondary"],
                                style="font-style: italic;",
                            )
                        # Newest first — latest line is always visible at the top.
                        for rec in reversed(records):
                            with solara.Row(style="gap: 6px; align-items: baseline;"):
                                solara.Text(
                                    rec["time"],
                                    classes=["text--secondary"],
                                    style="flex: 0 0 auto;",
                                )
                                rv.Chip(
                                    children=[rec["level"]],
                                    color=_LEVEL_COLOR.get(rec["level"], "grey"),
                                    text_color="white",
                                    x_small=True,
                                )
                                solara.Text(rec["msg"], style="flex: 1 1 auto;")
