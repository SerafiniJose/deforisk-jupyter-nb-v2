"""Wiring: Train tab is list-first; the form lives in ModelFormDialog."""
import inspect


def test_registry_moved_and_reexported():
    from gui.scripts.model_registry import MODEL_REGISTRY, MODEL_KEYS
    # compat re-export (tests/test_i18n.py imports from the tile)
    from gui.tile.train_tile import MODEL_REGISTRY as reexp
    assert reexp is MODEL_REGISTRY
    assert set(MODEL_KEYS) == set(MODEL_REGISTRY)


def test_train_tile_is_list_first_with_dialog():
    from gui.tile.train_tile import TrainTile
    src = inspect.getsource(TrainTile)
    assert "ModelFormDialog" in src
    assert "tiles.train.new_button" in src
    # delete still confirmed (test_delete_confirm.py contract)
    assert "ConfirmDialog" in src and "delete_model" in src
    assert "on_delete=set_pending_delete" in src
    # form moved out; the tile no longer renders parameter fields
    assert "parameters_header" not in src
    # old overwrite dialog gone (CreationDialog owns the confirm now)
    assert "confirm_overwrite_title" not in src
    # row click opens the read-only details dialog
    assert "ModelDetailsDialog" in src
    assert "on_open=set_details_key" in src


def test_model_details_dialog_is_read_only():
    import inspect

    from gui.widget.model_form_dialog import ModelDetailsDialog

    src = inspect.getsource(ModelDetailsDialog)
    # Mirrors the creation form's fields, but read-only: no Create action.
    assert "details_title" in src
    assert "advanced_parameters_header" in src
    assert "common.close" in src
    assert "CreationDialog" not in src and "launch" not in src


def test_model_form_dialog_contract():
    import gui.widget.model_form_dialog as mod
    src = inspect.getsource(mod)
    assert "CreationDialog" in src and "ArtifactNameField" in src
    assert "use_artifact_name" in src and "suggest_version" in src
    # parameters stay progressive-disclosed
    assert "ExpansionPanel" in src
    # Benchmark/MW layer references validated against the dataset
    assert "error_layer_not_in_dataset" in src
