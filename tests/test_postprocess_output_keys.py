"""Tests for postprocess_output_keys (derived-list filter on the post-process tab)."""

from types import SimpleNamespace

from gui.scripts import process_actions


def _var(name, tags=None, history=None):
    return SimpleNamespace(name=name, tags=tags or [], processing_history=history or [])


def test_filters_to_change_and_edge_dist_outputs():
    p = SimpleNamespace(
        processed_variables={
            # Process-step outputs (aligned/rasterized) — must be excluded
            "forest_gfc_2015": _var("forest_gfc", history=["reprojected"]),
            "altitude": _var("altitude"),
            # Post-process outputs — must be included
            "loss_forest_gfc_2015_2020": _var(
                "loss_forest_gfc_2015_2020", tags=["loss", "change", "2015_2020"]
            ),
            "roads_dist": _var("roads_dist", history=["dist"]),
            "forest_gfc_edge_2020": _var("forest_gfc_edge", history=["reprojected", "edge"]),
        }
    )
    assert process_actions.postprocess_output_keys(p) == [
        "loss_forest_gfc_2015_2020",
        "roads_dist",
        "forest_gfc_edge_2020",
    ]


def test_legacy_vars_without_history_fall_back_to_name_suffix():
    legacy = SimpleNamespace(name="rivers_dist")  # no tags / processing_history attrs
    p = SimpleNamespace(processed_variables={"rivers_dist": legacy})
    assert process_actions.postprocess_output_keys(p) == ["rivers_dist"]


def test_empty_when_no_postprocess_outputs():
    p = SimpleNamespace(processed_variables={"altitude": _var("altitude")})
    assert process_actions.postprocess_output_keys(p) == []
