def test_postprocess_tile_importable():
    from gui.tile.postprocess_tile import PostProcessTile
    assert callable(PostProcessTile)


def test_postprocess_tile_uses_change_and_edge_dist():
    import inspect
    import gui.tile.postprocess_tile as mod

    src = inspect.getsource(mod)
    assert "generate_change_var" in src
    assert "apply_post_processing" in src
    assert "DerivedVariableList" in src
