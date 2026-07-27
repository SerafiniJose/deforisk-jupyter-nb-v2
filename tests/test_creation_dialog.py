"""Wiring tests for the shared CreationDialog frame."""
import inspect


def test_creation_dialog_importable_and_contract():
    """The frame keeps the unified Create flow: validate, confirm, launch."""
    from gui.widget.creation_dialog import CreationDialog

    assert callable(CreationDialog)
    src = inspect.getsource(CreationDialog.f)  # solara component wraps the fn
    # validate runs before will_replace, which gates the confirm step
    assert src.index("validate()") < src.index("will_replace()")
    # duplicate policy: confirm dialog, launch on confirm
    assert "ConfirmDialog" in src
    # proven dialog pattern: eager top-level rv.Dialog
    assert "eager=True" in src
    # unified verb/icon on the submit button
    assert "mdi-plus" in src


def test_creation_dialog_survives_outside_click_but_closes_on_esc():
    """A click outside must dismiss an open dropdown, not the whole form.

    Vuetify stacks a v-select's menu above the dialog only when the overlay
    stack lines up; in this embedding it does not, so the same outside click
    that closes the dropdown also closed the form. `persistent` takes the
    dialog out of the click-outside path entirely (and `no_click_animation`
    stops it shaking at a click meant for the dropdown). VDialog still emits
    `keydown` while persistent, so ESC stays wired from Python — and VSelect
    stops ESC propagating while its menu is open, so ESC closes the dropdown
    first and the form only once no dropdown is open.
    """
    from gui.widget.creation_dialog import CreationDialog

    src = inspect.getsource(CreationDialog.f)
    assert "persistent=True" in src
    assert "no_click_animation=True" in src
    # ESC replaces what `persistent` took away, via the component event, and
    # must run the same reset/close path as Cancel.
    esc = next(line for line in src.splitlines() if "keydown.esc" in line)
    assert 'rv.use_event(dialog, "keydown.esc"' in esc
    assert "close()" in esc
