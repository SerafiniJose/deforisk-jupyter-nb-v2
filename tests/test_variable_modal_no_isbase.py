import inspect

import gui.widget.variable_modal as vm


def test_modal_has_no_is_base_state():
    src = inspect.getsource(vm)
    assert "is_base" not in src, "is_base must be fully removed from the modal"
    assert "Set as base raster" not in src
