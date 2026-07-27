"""Wiring: Inference tab is list-first; the run form lives in a dialog."""

import inspect


def test_naming_helpers_are_aliases():
    """The tile's private naming helpers stay bound to artifact_names."""
    from gui.scripts import artifact_names as an
    from gui.tile import inference_tile as it

    assert it._sanitize_pred_name is an.sanitize_key
    assert it._default_pred_name is an.default_pred_name
    assert it._prediction_name_exists is an.prediction_name_exists


def test_inference_tile_is_list_first_with_dialog():
    """The tile renders a list plus one dialog — no inline form left."""
    from gui.tile.inference_tile import InferenceTile

    src = inspect.getsource(InferenceTile)
    assert "PredictionFormDialog" in src
    assert "tiles.inference.new_button" in src
    # single entry point: the import modal and its button are gone
    assert "PredictionImportModal" not in src
    assert "import_modal_open" not in src
    # old inline form + overwrite dialog stay gone
    assert "pred_name_label" not in src
    assert "confirm_overwrite_title" not in src


def test_prediction_form_dialog_contract():
    """The dialog keeps the shared frame, the name field and both sources."""
    import gui.widget.prediction_form_dialog as mod

    src = inspect.getsource(mod)
    assert "CreationDialog" in src and "ArtifactNameField" in src
    assert "use_artifact_name" in src and "default_pred_name" in src
    assert "prediction_name_exists" in src
    # unified dialog: source kind slot + import mode
    assert "tiles.inference.source_label" in src
    assert "FileInputComponent" in src
    assert "sepal_client" in src
    # import previews the resolved (suffixed) key, it never replaces
    assert "resolve_import_key" in src
