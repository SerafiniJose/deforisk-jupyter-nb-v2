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
    assert "on_remove=set_pending_remove" in src
