import inspect

TAB_LABELS = [
    "tiles.summary.tab_raw_variables", "tiles.summary.tab_processed_variables",
    "tiles.summary.tab_datasets", "tiles.summary.tab_samples",
    "tiles.summary.tab_trained_models", "tiles.summary.tab_predictions",
    "tiles.summary.tab_evaluations",
]
RENDERERS = [
    "RawVariablesSummary", "ProcessedVariablesSummary", "DatasetsSummary",
    "SamplesSummary", "ModelsSummary", "PredictionsSummary", "EvaluationsSummary",
]


def test_tile_guards_and_uses_tabs():
    from gui.tile.summary_tile import ProjectSummaryTile
    fsrc = inspect.getsource(ProjectSummaryTile)
    assert "project.value" in fsrc
    assert "solara.Info" in fsrc          # None-project guard
    assert "rv.Tabs" in fsrc


def test_tile_has_all_seven_tabs_and_renderers():
    import gui.tile.summary_tile as m
    src = inspect.getsource(m)
    for label in TAB_LABELS:
        assert label in src, f"missing tab {label}"
    for renderer in RENDERERS:
        assert renderer in src, f"missing renderer wiring {renderer}"
