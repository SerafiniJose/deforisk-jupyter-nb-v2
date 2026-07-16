"""Wiring tests for the shared CreationDialog frame."""
import inspect


def test_creation_dialog_importable_and_contract():
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
