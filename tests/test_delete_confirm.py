"""Destructive deletes route through a confirm dialog instead of deleting on click.

Variables and datasets are removed from the project (data loss), so their list
"remove" buttons must open a ConfirmDialog rather than delete immediately. These
guard the wiring so it cannot be silently dropped.
"""

import inspect


def test_confirm_dialog_importable():
    from gui.widget.confirm_dialog import ConfirmDialog
    assert callable(ConfirmDialog)


def test_variables_tile_confirms_remove():
    from gui.tile.variables_tile import VariablesTile
    src = inspect.getsource(VariablesTile)
    assert "ConfirmDialog" in src
    # The list's remove button opens the dialog rather than deleting directly.
    assert "on_remove=set_pending_remove" in src


def test_dataset_tile_confirms_remove():
    from gui.tile.dataset_tile import DatasetTile
    src = inspect.getsource(DatasetTile)
    assert "ConfirmDialog" in src
    assert "on_remove=set_pending_remove" in src


def test_train_tile_confirms_and_really_deletes_model():
    from gui.tile.train_tile import TrainTile
    src = inspect.getsource(TrainTile)
    assert "ConfirmDialog" in src
    assert "delete_model" in src  # removal actually unregisters the model
    # Model deletion (not job dismissal) drives the confirm dialog — see
    # docs/spec: completed runs render as model rows and "delete" on a model
    # row deletes the model.
    assert "on_delete=set_pending_delete" in src


def test_inference_tile_confirms_and_really_deletes_predictions():
    from gui.tile.inference_tile import InferenceTile
    src = inspect.getsource(InferenceTile)
    assert "ConfirmDialog" in src
    assert "delete_prediction" in src  # removal actually unregisters the predictions
    assert "on_delete=set_pending_delete" in src


def test_manage_projects_widget_confirms_delete():
    from gui.widget.manage_projects import ConfirmDeleteProjectDialog, ManageProjectsDialog
    assert callable(ManageProjectsDialog) and callable(ConfirmDeleteProjectDialog)

    src = inspect.getsource(ManageProjectsDialog)
    assert "on_delete(" in src          # the row hands the target up; it never deletes
    assert "delete_project" not in src  # the widget must not touch the disk
    assert "rv.Btn(" not in src         # rv.Btn silently drops clicks in this codebase
    # A disabled rv.ListItem suppresses its children, which would make the trash
    # button dead on exactly the corrupt projects we most need to remove.
    assert "disabled=not info.readable" not in src

    confirm = inspect.getsource(ConfirmDeleteProjectDialog)
    assert "delete_confirm_valid" in confirm   # type-the-name gating
    assert "writer_active" in confirm          # refused while a task is writing


def test_project_panel_confirms_and_really_deletes_projects():
    import gui.solara_app as app
    src = inspect.getsource(app.ProjectPanel)
    assert "ConfirmDeleteProjectDialog" in src
    assert "delete_project" in src        # removal actually hits the disk
    assert "on_delete=open_delete" in src # the trash button opens the confirm dialog
    assert "close_project_state" in src   # deleting the open project closes it
    assert "is_writing" in src            # refused while a task is writing to it


def test_project_panel_delete_is_not_re_entrant_and_owns_its_error():
    """The two ways the confirm button can go wrong on an uncancellable rmtree.

    The button's `disabled` only reaches the browser a round-trip later, so a real
    double-click invokes the task twice — and TaskAsyncio.__call__ cancels the
    in-flight one, which does not stop the rmtree but does skip its continuation.
    The confirm button must therefore go through the `pending` guard, never straight
    to the task. And a Task keeps `.error`/`.exception` until the next invoke, so the
    failure text must be owned by the panel or it leaks into the next target's dialog.
    """
    import gui.solara_app as app
    src = inspect.getsource(app.ProjectPanel)
    assert "on_confirm=confirm_delete" in src   # not on_confirm=delete_task
    assert "if delete_task.pending:" in src     # the synchronous re-entrancy guard
    assert "error=delete_error.value" in src    # the panel owns the message …
    assert "delete_task.exception" not in src   # … and never the task's sticky one
