"""VariableModal shows the shared saved-as preview / replace warning."""
import inspect


def test_variable_modal_uses_artifact_name_field():
    import gui.widget.variable_modal as mod
    src = inspect.getsource(mod)
    assert "ArtifactNameField" in src
    assert "existing_keys" in src
    # predefined branch previews the storage key too
    assert "widgets.artifact_name.saved_as" in src


def test_variables_tile_passes_existing_keys():
    from gui.tile.variables_tile import VariablesTile
    src = inspect.getsource(VariablesTile)
    assert "existing_keys=" in src
