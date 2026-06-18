def test_predictors_public_exports():
    import spatialrisk.predictors as p

    assert hasattr(p, "SupervisedPredictor")
    assert hasattr(p, "MWPredictor")
    assert hasattr(p, "JNRPredictor")
    assert hasattr(p, "supervised_block_fn")
    assert hasattr(p, "icar_block_fn")
    assert hasattr(p, "register_supervised")
    assert hasattr(p, "build_dataset_snapshot")
    assert set(
        ["SupervisedPredictor", "MWPredictor", "JNRPredictor",
         "supervised_block_fn", "icar_block_fn",
         "register_supervised", "build_dataset_snapshot"]
    ).issubset(set(p.__all__))
