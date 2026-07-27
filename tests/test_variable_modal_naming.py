"""VariableModal shows the shared saved-as preview / replace warning."""
import inspect


def test_variable_modal_uses_artifact_name_field():
    """The modal renders ArtifactNameField and previews the storage key."""
    import gui.widget.variable_modal as mod

    src = inspect.getsource(mod)
    assert "ArtifactNameField" in src
    assert "existing_keys" in src
    # predefined branch previews the storage key too
    assert "widgets.artifact_name.saved_as" in src


def test_variables_tile_passes_existing_keys():
    """VariablesTile passes existing_keys down to the modal."""
    from gui.tile.variables_tile import VariablesTile

    src = inspect.getsource(VariablesTile)
    assert "existing_keys=" in src


def test_predefined_branch_renders_and_validates_catalogue_params():
    """The modal renders catalogue params generically and submits the suffixed name.

    No per-layer branching. (Source inspection: the modal is a Solara
    component with no headless render harness in this suite; the logic it
    calls is unit-tested in tests/test_predefined_params.py.)
    """
    import inspect

    import gui.widget.variable_modal as mod

    src = inspect.getsource(mod)

    assert "param_specs" in src
    assert "coerce_param_values" in src
    assert "build_predefined_name" in src
    assert "vars.modal.error_param_range" in src
    # Generic: driven by the catalogue, never by a branch on a specific layer.
    # (Layer names may appear in comments; a hardcoded comparison may not.)
    assert 'predefined_key == "forest_gfc"' not in src
    assert "predefined_key == 'forest_gfc'" not in src
