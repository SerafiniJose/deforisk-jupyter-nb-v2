"""Map layer names carry a two-character origin marker.

A raw variable and its harmonized counterpart share a registry key by design
(``add_as_processed`` computes the key exactly as ``add_as_raw`` does), so
without a marker both render in the layer control under the same visible name.
"""

from gui.scripts.layer_labels import (
    MARKER_DERIVED,
    MARKER_HARMONIZED,
    MARKER_RAW,
    processed_layer_label,
    raw_layer_label,
)


def _var(**attrs):
    """A duck-typed variable; postprocess_output_keys reads attrs via getattr."""
    return type("FakeVar", (), attrs)()


class _Project:
    def __init__(self, **processed):
        self.processed_variables = dict(processed)


def test_markers_are_the_agreed_strings():
    """Hard-coded ASCII — never translated, never reformatted."""
    assert MARKER_RAW == "[R]"
    assert MARKER_HARMONIZED == "[H]"
    assert MARKER_DERIVED == "[D]"


def test_raw_label_is_prefixed():
    """A raw variable's layer name is prefixed with the raw marker."""
    assert raw_layer_label("forest_gfc_tc30_2020") == "[R] forest_gfc_tc30_2020"


def test_harmonized_label_is_prefixed():
    """A plain reprojected/matched raster is a Process-step output."""
    project = _Project(
        forest_gfc_tc30_2020=_var(
            name="forest_gfc_tc30",
            tags=[],
            processing_history=["reprojected", "reprojected_matched"],
        )
    )

    label = processed_layer_label(project, "forest_gfc_tc30_2020")

    assert label == "[H] forest_gfc_tc30_2020"


def test_derived_label_for_a_change_tagged_variable():
    """Branch 1 of postprocess_output_keys: the "change" tag."""
    project = _Project(
        loss_forest_2010_2020=_var(
            name="loss_forest_2010_2020", tags=["change"], processing_history=[]
        )
    )

    label = processed_layer_label(project, "loss_forest_2010_2020")

    assert label == "[D] loss_forest_2010_2020"


def test_derived_label_from_processing_history():
    """Branch 2: an edge/dist step recorded in processing_history."""
    project = _Project(
        road_distance_2020=_var(
            name="road_distance_2020",
            tags=[],
            processing_history=["reprojected_matched", "dist"],
        )
    )

    label = processed_layer_label(project, "road_distance_2020")

    assert label == "[D] road_distance_2020"


def test_derived_label_from_legacy_name_suffix():
    """Branch 3: the fallback for variables saved before processing_history."""
    project = _Project(road_dist=_var(name="road_dist", tags=[], processing_history=[]))

    label = processed_layer_label(project, "road_dist")

    assert label == "[D] road_dist"


def test_processed_label_defaults_to_harmonized_for_an_unknown_key():
    """A key absent from the registry cannot be classified as derived."""
    assert processed_layer_label(_Project(), "ghost") == "[H] ghost"


def test_processed_label_tolerates_a_missing_project():
    """The toggle resolves project.value, which can be None mid-teardown."""
    assert processed_layer_label(None, "forest_2020") == "[H] forest_2020"


def test_module_does_not_import_gui_tile_at_module_level():
    """Ensure no module-level gui.tile import.

    process_actions imports gui.tile.derived_map lazily to avoid a cycle;
    a module-level gui.tile import here would close it.

    Parsed with ast rather than grepped: the module docstring names gui.tile
    when it explains this very rule, so a substring check would false-positive.
    """
    import ast
    import inspect

    from gui.scripts import layer_labels

    tree = ast.parse(inspect.getsource(layer_labels))
    imported = set()
    for node in tree.body:  # module level only — nested imports are fine
        if isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")

    assert not any(m.startswith("gui.tile") for m in imported), imported
