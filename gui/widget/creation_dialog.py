"""Shared creation dialog: form frame + Create action + replace confirmation.

Every list-first tab opens its "New <thing>" form through this frame so the
layout, button order, validation flow and duplicate policy stay identical:
Create validates on click (button never disabled), an existing key asks for
confirmation (never a silent overwrite, never a hard refusal), and the
caller's `launch` performs the actual mutation / job spawn.
"""

from typing import Callable, List, Optional

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.widget.confirm_dialog import ConfirmDialog

# Restyle the Advanced-parameters panel to sit in the form's flow: same
# border/height/label colour as the outlined dense fields, no 24px inset.
# Injected here so every creation form's `class_="advanced-params"` panel
# looks identical.
_ADVANCED_PANEL_CSS = """
.advanced-params .v-expansion-panel { border: 1px solid rgba(0, 0, 0, .38); border-radius: 4px; }
.theme--dark .advanced-params .v-expansion-panel { border-color: rgba(255, 255, 255, .24); }
.advanced-params .v-expansion-panel::before { box-shadow: none; }
.advanced-params .v-expansion-panel-header { min-height: 40px; padding: 0 12px; font-size: 14px; color: rgba(0, 0, 0, .6); }
.theme--dark .advanced-params .v-expansion-panel-header { color: rgba(255, 255, 255, .7); }
.advanced-params .v-expansion-panel-content__wrap { padding: 16px 12px 4px; }
"""


@solara.component
def CreationDialog(
    open_,
    title: str,
    create_label: str,
    validate: Callable[[], Optional[str]],
    will_replace: Callable[[], Optional[str]],
    launch: Callable[[], None],
    on_close: Optional[Callable[[], None]] = None,
    replace_title: Optional[str] = None,
    replace_message: Optional[Callable[[str], str]] = None,
    max_width: str = "560px",
    children: List = [],
):
    """Creation-form frame with the unified Create flow.

    Args:
        open_: solara.Reactive[bool] — dialog visibility (owned by the tile).
        validate: () -> error message | None; runs on Create click.
        will_replace: () -> existing storage key | None; runs after validate.
            A returned key opens the confirm-replace dialog instead of
            launching directly.
        launch: () -> None; performs the creation. The dialog closes and the
            caller's form resets (on_close) after it returns.
        on_close: reset callback fired on Cancel/ESC and after a launch.
        replace_message: key -> confirmation body; defaults to the generic
            widgets.creation_dialog.replace_message copy.
    """
    error, set_error = solara.use_state(None)
    pending_replace, set_pending_replace = solara.use_state(None)

    def close():
        set_error(None)
        set_pending_replace(None)
        open_.set(False)
        if on_close is not None:
            on_close()

    def do_launch():
        launch()
        close()

    def on_create():
        set_error(None)
        err = validate()
        if err:
            set_error(err)
            return
        key = will_replace()
        if key:
            set_pending_replace(key)
            return
        do_launch()

    with rv.Dialog(
        v_model=open_.value,
        on_v_model=lambda v: None if v else close(),
        max_width=max_width,
        eager=True,
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text(title)
            with rv.CardText():
                solara.Style(_ADVANCED_PANEL_CSS)
                solara.Column(style="gap:4px;", children=children)
                if error:
                    rv.Alert(type_="error", dense=True, children=[error])
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button(t("common.cancel"), on_click=close, text=True, small=True)
                solara.Button(
                    create_label,
                    icon_name="mdi-plus",
                    color="primary",
                    small=True,
                    on_click=on_create,
                )

    _msg = (
        replace_message(pending_replace)
        if replace_message is not None and pending_replace
        else t("widgets.creation_dialog.replace_message", key=pending_replace or "")
    )
    ConfirmDialog(
        open=pending_replace is not None,
        on_cancel=lambda: set_pending_replace(None),
        on_confirm=do_launch,
        title=replace_title or t("widgets.creation_dialog.replace_title"),
        message=_msg,
        confirm_label=t("common.replace"),
        confirm_color="warning",
    )
