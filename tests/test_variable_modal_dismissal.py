"""The Add/Edit Variable modal must not close on a click aimed at a dropdown."""
import inspect

import gui.widget.variable_modal as vm


def test_variable_modal_survives_outside_click_but_closes_on_esc():
    """Same dismissal contract as the shared CreationDialog frame.

    This modal predates that frame and hand-rolls its own rv.Dialog, so the
    fix has to be repeated here: `persistent` keeps a click outside an open
    v-select menu from taking the whole form with it, and the ESC handler
    restores the keyboard dismissal `persistent` disables.
    """
    src = inspect.getsource(vm.VariableModal.f)
    assert "persistent=True" in src
    assert "no_click_animation=True" in src
    assert 'rv.use_event(dialog, "keydown.esc"' in src


def test_variable_modal_esc_resets_the_form():
    """ESC must go through on_cancel, not a bare open_.set(False).

    A close that skips reset() leaves the previous entry's source, name and
    params behind for the next open.
    """
    src = inspect.getsource(vm.VariableModal.f)
    esc = next(line for line in src.splitlines() if "keydown.esc" in line)
    assert "on_cancel()" in esc
