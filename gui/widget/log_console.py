"""Floating, collapsible log console pinned to the map's lower-right corner.

Render-only: reads ``log_records`` (fed by ``gui.scripts.log_bridge``) and shows
the latest milestones. Collapsed by default via a self-managed ``rv.ExpansionPanels``
(no Python toggle state — that pattern is unreliable here). Newest line first, so the
latest is always visible without fragile auto-scroll-to-bottom JS.
"""

import reacton.ipyvuetify as rv
import solara

from gui.scripts.log_bridge import install_log_console_handler, log_records

# Level -> Vuetify chip colour (matches the colorlog scheme in logging_config.toml).
_LEVEL_COLOR = {
    "DEBUG": "grey",
    "INFO": "green",
    "WARNING": "amber",
    "ERROR": "red",
    "CRITICAL": "red darken-2",
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

    with solara.Column(
        style=(
            "position: fixed; right: 16px; bottom: 16px; z-index: 1000; "
            "width: 380px; max-width: 90vw;"
        )
    ):
        # No v_model -> Vuetify owns open/close; panels start collapsed.
        with rv.ExpansionPanels(flat=True, hover=True):
            with rv.ExpansionPanel():
                with rv.ExpansionPanelHeader():
                    with solara.Row(style="align-items: center; gap: 8px;"):
                        rv.Icon(children=["mdi-text-box-outline"], small=True)
                        solara.Text("Process log", style="font-weight: 600;")
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
                                "No activity yet.",
                                style="color: var(--md-grey-500); font-style: italic;",
                            )
                        # Newest first — latest line is always visible at the top.
                        for rec in reversed(records):
                            with solara.Row(style="gap: 6px; align-items: baseline;"):
                                solara.Text(
                                    rec["time"],
                                    style="color: var(--md-grey-500); flex: 0 0 auto;",
                                )
                                rv.Chip(
                                    children=[rec["level"]],
                                    color=_LEVEL_COLOR.get(rec["level"], "grey"),
                                    text_color="white",
                                    x_small=True,
                                )
                                solara.Text(rec["msg"], style="flex: 1 1 auto;")
