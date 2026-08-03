"""Pure registry operations plus their AppState wiring."""

from gui.scripts.legend_data import Label, LegendSpec
from gui.scripts.legend_registry import (
    LayerLegend,
    next_selection,
    remove,
    upsert,
)


def _legend(layer_id, kind="gradient"):
    return LayerLegend(
        layer_id=layer_id,
        label=Label(literal=layer_id),
        spec=LegendSpec(kind=kind, title=Label(literal=layer_id)),
    )


def test_upsert_appends_new_entries_in_order():
    """upsert() with no existing entries appends new ones in call order."""
    result = upsert((), _legend("a"), _legend("b"))
    assert [e.layer_id for e in result] == ["a", "b"]


def test_upsert_replaces_same_id_in_place():
    """upsert() with a matching id replaces the entry without reordering."""
    current = upsert((), _legend("a"), _legend("b"))
    replacement = _legend("a", kind="chips")
    result = upsert(current, replacement)
    assert [e.layer_id for e in result] == ["a", "b"]
    assert result[0].spec.kind == "chips"


def test_remove_drops_only_the_named_ids():
    """remove() drops only the named ids and ignores unknown ones."""
    current = upsert((), _legend("a"), _legend("b"), _legend("c"))
    assert [e.layer_id for e in remove(current, "b", "missing")] == ["a", "c"]


def test_next_selection_keeps_a_still_present_selection():
    """next_selection() keeps the previous selection if it still exists."""
    remaining = upsert((), _legend("a"), _legend("b"))
    assert next_selection(remaining, "a") == "a"


def test_next_selection_falls_back_to_the_last_entry():
    """next_selection() falls back to the last entry when previous is gone."""
    remaining = upsert((), _legend("a"), _legend("b"))
    assert next_selection(remaining, "gone") == "b"


def test_next_selection_is_empty_when_nothing_remains():
    """next_selection() returns empty string when nothing remains."""
    assert next_selection((), "a") == ""


def test_app_state_register_selects_the_newest_layer():
    """AppState.register_legends() selects the most recently registered layer."""
    from gui.store.state_manager import AppState

    state = AppState()
    state.register_legends(_legend("a"))
    state.register_legends(_legend("b"))
    assert [e.layer_id for e in state.layer_legends.value] == ["a", "b"]
    assert state.selected_legend.value == "b"


def test_app_state_unregister_falls_back_to_remaining_selection():
    """AppState.unregister_legends() falls back to a remaining selection."""
    from gui.store.state_manager import AppState

    state = AppState()
    state.register_legends(_legend("a"), _legend("b"))
    state.unregister_legends("b")
    assert [e.layer_id for e in state.layer_legends.value] == ["a"]
    assert state.selected_legend.value == "a"


def test_app_state_clear_empties_registry_and_selection():
    """AppState.clear_legends() empties both the registry and selection."""
    from gui.store.state_manager import AppState

    state = AppState()
    state.register_legends(_legend("a"))
    state.clear_legends()
    assert state.layer_legends.value == ()
    assert state.selected_legend.value == ""


def test_app_state_unregister_of_unknown_id_is_a_noop():
    """AppState.unregister_legends() of an unknown id changes nothing."""
    from gui.store.state_manager import AppState

    state = AppState()
    state.register_legends(_legend("a"))
    state.unregister_legends("never-added")
    assert [e.layer_id for e in state.layer_legends.value] == ["a"]
    assert state.selected_legend.value == "a"
