# tests/test_predictors_mw.py
from pathlib import Path


def test_mw_predictor_one_output_per_window_and_registers(tmp_path):
    from spatialrisk.predictors.mw import MWPredictor

    calls = {"set_zero": [], "defrate": []}

    def fake_set_defor_cat_zero(ldefrate_file, forest_edge_file, dist_thresh,
                                output_file, blk_rows, verbose):
        Path(output_file).write_bytes(b"")  # touch
        calls["set_zero"].append(Path(output_file).name)

    def fake_defrate_per_cat(defor_file, forest_file, riskmap_file,
                             time_interval, tab_file_defrate, blk_rows):
        calls["defrate"].append(Path(riskmap_file).name)

    # ldefrate rasters must exist on disk (apply checks .exists())
    ldefrate = {}
    for w in ("5", "11"):
        p = tmp_path / f"ldefrate_{w}.tif"
        p.write_bytes(b"")
        ldefrate[w] = p

    registered = []

    def register(path, dataset, window):
        registered.append((Path(path).name, getattr(dataset, "name", None), window))

    out = MWPredictor(
        set_defor_cat_zero_fn=fake_set_defor_cat_zero,
        defrate_per_cat_fn=fake_defrate_per_cat,
    ).apply(
        ldefrate_files=ldefrate,
        defor_file=str(tmp_path / "defor.tif"),
        forest_file=str(tmp_path / "forest.tif"),
        forest_edge_file=str(tmp_path / "edge.tif"),
        dist_thresh=1234.0,
        time_interval=5,
        period="validation",
        output_folder=tmp_path / "out",
        blk_rows=256,
        register_prediction=register,
        dataset=type("D", (), {"name": "ds_2020"})(),
    )

    assert set(out.keys()) == {"5", "11"}
    assert all(isinstance(v, Path) and v.exists() for v in out.values())
    assert out["5"].name == "prob_mw_5_validation.tif"
    # one registration per window, with the right window discriminator
    assert sorted(w for _, _, w in registered) == [5, 11]
    assert all(name.startswith("prob_mw_") for name, _, _ in registered)
    assert all(ds == "ds_2020" for _, ds, _ in registered)
    assert len(calls["set_zero"]) == 2 and len(calls["defrate"]) == 2
