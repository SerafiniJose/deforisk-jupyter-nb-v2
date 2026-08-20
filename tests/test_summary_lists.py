import inspect

HELPERS = [
    "raw_variable_rows", "processed_variable_rows", "dataset_rows",
    "sample_rows", "model_rows", "prediction_rows", "evaluation_rows",
]


def test_renderers_wired_to_helpers():
    import gui.widget.summary_lists as m
    src = inspect.getsource(m)
    for fn in HELPERS:
        assert fn in src, f"renderer module never calls helper {fn}"
    # Read-only: the widgets must not import or wire the editable action callbacks.
    assert "on_remove" not in src
    assert "on_edit" not in src
