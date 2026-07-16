"""Shared artifact-name field: suggested-until-edited value + key preview.

Every creation form names its output through this widget so the behaviour is
identical everywhere: the field shows a live suggestion until the user edits
it, previews the storage key ("Will be saved as ..."), and warns (amber-style
message, not an error) when the key is already taken.
"""

from typing import Callable, Optional

import reacton.ipyvuetify as rv
import solara

from gui.i18n import t
from gui.scripts.artifact_names import name_field_messages


def use_artifact_name(suggestion: str):
    """Name state that tracks `suggestion` until the user edits the field.

    Returns (value, on_input, reset). Typing the exact suggestion keeps the
    field non-dirty (it continues tracking); reset() re-arms the suggestion —
    call it after a successful submit so the next open re-suggests.
    """
    name, set_name = solara.use_state("")
    dirty, set_dirty = solara.use_state(False)
    value = name if dirty else suggestion

    def on_input(v):
        set_name(v)
        set_dirty(v != suggestion)

    def reset():
        set_name("")
        set_dirty(False)

    return value, on_input, reset


@solara.component
def ArtifactNameField(
    value: str,
    on_input: Callable[[str], None],
    storage_key: str,
    exists: bool,
    attempted: bool = False,
    label: Optional[str] = None,
    disabled: bool = False,
):
    """Name field with a 'Will be saved as' preview and overwrite warning.

    Args:
        value / on_input: controlled field state (use use_artifact_name).
        storage_key: the key the artifact will be stored under (may differ
            from value, e.g. Train's "{model}_{name}").
        exists: True when storage_key is already taken — shows the replace
            warning (creation stays possible; the dialog confirms).
        attempted: True once a submit was attempted — flags an empty name red.
        disabled: render read-only (e.g. dataset edit keeps its key).
    """
    msg_key, is_error = name_field_messages(storage_key, exists, attempted)
    rv.TextField(
        label=label or t("widgets.artifact_name.label"),
        v_model=value,
        on_v_model=on_input,
        dense=True,
        outlined=True,
        disabled=disabled,
        messages=t(msg_key, key=storage_key),
        error=is_error,
    )
