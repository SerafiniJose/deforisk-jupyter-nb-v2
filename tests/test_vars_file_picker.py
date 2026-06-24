"""The custom-variable file field is an interactive picker, and the SEPAL
client is threaded modal -> tile -> shell."""

import inspect

import gui.solara_app as app
import gui.tile.variables_tile as vt
import gui.widget.variable_modal as vm


def test_modal_uses_file_input_component():
    src = inspect.getsource(vm._render_custom_fields)
    assert "FileInputComponent" in src, "the picker must be rendered in the custom fields"
    assert 'root=""' in src, "the picker must be rooted at the SEPAL/user home"
    assert 'label="File path"' not in inspect.getsource(vm), "plain text file-path field must be gone"


def test_modal_accepts_sepal_client():
    # getsource on a solara component returns the decorated def, incl. signature.
    assert "sepal_client" in inspect.getsource(vm.VariableModal)


def test_variables_tile_threads_sepal_client():
    src = inspect.getsource(vt.VariablesTile)
    assert "sepal_client" in src
    assert "sepal_client=sepal_client" in src


def test_shell_resolves_and_threads_sepal_client():
    assert "get_current_sepal_client()" in inspect.getsource(app.Page)
    assert "sepal_client=sepal_client" in inspect.getsource(app.WorkflowTabs)
