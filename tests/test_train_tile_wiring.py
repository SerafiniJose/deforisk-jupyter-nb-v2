"""Wiring: Train tab is list-first; the form lives in ModelFormDialog."""
import inspect


def test_registry_moved_and_reexported():
    """Test MODEL_REGISTRY is moved and re-exported from train_tile."""
    # compat re-export (tests/test_i18n.py imports from the tile)
    from gui.scripts.model_registry import MODEL_KEYS, MODEL_REGISTRY
    from gui.tile.train_tile import MODEL_REGISTRY as reexp

    assert reexp is MODEL_REGISTRY
    assert set(MODEL_KEYS) == set(MODEL_REGISTRY)


def test_train_tile_is_list_first_with_dialog():
    """Test TrainTile is list-first with form in ModelFormDialog."""
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
    """Test ModelDetailsDialog mirrors creation form but is read-only."""
    import inspect

    from gui.widget.model_form_dialog import ModelDetailsDialog

    src = inspect.getsource(ModelDetailsDialog)
    # Mirrors the creation form's fields, but read-only: no Create action.
    assert "details_title" in src
    assert "advanced_parameters_header" in src
    assert "common.close" in src
    assert "CreationDialog" not in src and "launch" not in src
    # Stored formula shown read-only; row hidden for legacy models without one.
    assert "formula_label" in src
    assert 'getattr(model, "formula", None)' in src


def test_model_form_dialog_contract():
    """Test ModelFormDialog has correct dependencies and structure."""
    import gui.widget.model_form_dialog as mod

    src = inspect.getsource(mod)
    assert "CreationDialog" in src and "ArtifactNameField" in src
    assert "use_artifact_name" in src and "suggest_version" in src
    # parameters stay progressive-disclosed
    assert "ExpansionPanel" in src
    # Benchmark/MW layer references validated against the dataset
    assert "error_layer_not_in_dataset" in src


def test_formula_flag_on_patsy_models_only():
    """Test has_formula flag set only on patsy-capable models."""
    from gui.scripts.model_registry import MODEL_REGISTRY

    flags = {k: v.get("has_formula", False) for k, v in MODEL_REGISTRY.items()}
    assert flags == {
        "benchmark": False,
        "mw": False,
        "glm": True,
        "rf": True,
        "icar": True,
    }


def test_formula_i18n_keys_exist_in_both_locales():
    """Test formula i18n keys exist in both English and Spanish."""
    import json

    for locale in ("en", "es-ES"):
        with open(f"gui/messages/{locale}/tiles.json") as f:
            train = json.load(f)["tiles"]["train"]
        for key in (
            "formula_label",
            "formula_hint",
            "formula_generating",
            "error_formula_generating",
            "error_formula_shape",
            "error_formula_parse",
            "error_formula_missing_target",
            "error_formula_lhs",
            "error_formula_rhs_reserved",
            "error_formula_rhs_unknown",
        ):
            assert key in train, f"{locale} missing tiles.train.{key}"


def test_run_training_accepts_and_forwards_formula():
    """Test _run_training signature accepts formula and forwards it to model_cls."""
    from gui.tile.train_tile import TrainTile, _run_training

    sig = inspect.signature(_run_training)
    assert "formula" in sig.parameters
    assert sig.parameters["formula"].default is None

    src = inspect.getsource(_run_training)
    # The formula must reach the constructor kwargs, not be silently dropped.
    assert 'kwargs["formula"] = formula' in src

    # on_submit forwards entry["formula"] into the spawn tuple.
    tile_src = inspect.getsource(TrainTile)
    assert 'entry["formula"]' in tile_src
