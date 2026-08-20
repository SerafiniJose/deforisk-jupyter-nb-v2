"""Wiring: Datasets tab is list-first with a CreationDialog-based form."""
import inspect


def test_dataset_tile_is_list_first_with_dialog():
    from gui.tile.dataset_tile import DatasetTile
    src = inspect.getsource(DatasetTile)
    assert "DatasetFormDialog" in src
    assert "tiles.dataset.new_button" in src
    # destructive delete still confirmed (test_delete_confirm.py contract)
    assert "ConfirmDialog" in src
    assert "on_remove=set_pending_remove" in src
    # the old inline form is gone from the tile
    assert "feature_variables_label" not in src
    # no success alert: the list row is the feedback
    assert "success_registered" not in src


def test_dataset_form_dialog_contract():
    from gui.widget.dataset_form_dialog import DatasetFormDialog
    import gui.widget.dataset_form_dialog as mod
    src = inspect.getsource(mod)
    assert "CreationDialog" in src
    assert "ArtifactNameField" in src
    assert "use_artifact_name" in src
    # duplicate policy: replace is confirmed, not silent
    assert "will_replace" in src
    # editing keeps the storage key: name field disabled in edit mode
    assert "disabled=" in src
