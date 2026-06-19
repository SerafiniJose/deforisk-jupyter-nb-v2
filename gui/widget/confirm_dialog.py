"""Reusable confirmation dialog for destructive actions.

Follows the proven ProjectPanel discard/overwrite pattern: a single ``rv.Dialog``
rendered at the component's top level and toggled by ``use_state`` (a row button
merely sets the pending target). Nested button -> Dialog toggles have proved
unreliable, so callers should render this once per tile, not inside a list row.
"""

import reacton.ipyvuetify as rv
import solara


@solara.component
def ConfirmDialog(
    open,
    on_cancel,
    on_confirm,
    title="Delete?",
    message="This action cannot be undone.",
    confirm_label="Delete",
    confirm_color="error",
):
    """Modal confirm dialog for a destructive action.

    Args:
        open: bool — whether the dialog is shown.
        on_cancel: callback() — dismiss without acting (also fired on ESC / outside click).
        on_confirm: callback() — perform the action; the caller is responsible for closing.
        title / message: dialog copy.
        confirm_label / confirm_color: confirm-button label and color.
    """
    with rv.Dialog(
        v_model=open,
        on_v_model=lambda v: None if v else on_cancel(),
        max_width="380px",
        eager=True,
    ):
        with rv.Card():
            with rv.CardTitle():
                solara.Text(title)
            with rv.CardText():
                solara.Text(message)
            with rv.CardActions(style_="justify-content: flex-end; gap: 8px;"):
                solara.Button("Cancel", on_click=on_cancel, text=True, small=True)
                solara.Button(
                    confirm_label, on_click=on_confirm, color=confirm_color, small=True
                )
