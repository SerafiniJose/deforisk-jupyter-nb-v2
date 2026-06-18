# tests/test_predictors_jnr.py
from pathlib import Path


def test_jnr_predictor_writes_output_and_registers_once(tmp_path):
    from spatialrisk.predictors.jnr import JNRPredictor

    calls = {"vuln": 0, "defrate": 0}

    def fake_vulnerability_map(forest_file, forest_edge_file, dist_bins,
                               subj_file, output_file, blk_rows, verbose):
        Path(output_file).write_bytes(b"")  # touch
        calls["vuln"] += 1

    def fake_defrate_per_class(defor_file, forest_file, vulnerability_file,
                               time_interval, tab_file_defrate,
                               deforate_model, blk_rows):
        Path(tab_file_defrate).write_text("class,rate\n")
        calls["defrate"] += 1

    registered = []

    def register(path, dataset, window=None):
        registered.append((Path(path).name, getattr(dataset, "name", None), window))

    out_file = tmp_path / "vuln.tif"
    result, defrate_tab = JNRPredictor(
        vulnerability_map_fn=fake_vulnerability_map,
        defrate_per_class_fn=fake_defrate_per_class,
    ).apply(
        output_file=out_file,
        defor_file=str(tmp_path / "defor.tif"),
        forest_file=str(tmp_path / "forest.tif"),
        forest_edge_file=str(tmp_path / "edge.tif"),
        subj_file=str(tmp_path / "subj.tif"),
        dist_bins=[0.0, 100.0, 200.0],
        time_interval=5,
        period="validation",
        blk_rows=128,
        deforate_model=None,
        register_prediction=register,
        dataset=type("D", (), {"name": "ds_2020"})(),
    )

    assert result == out_file and out_file.exists()
    assert defrate_tab.name == "defrate_cat_bm_validation.csv" and defrate_tab.exists()
    assert calls == {"vuln": 1, "defrate": 1}
    # exactly one registration, no window
    assert len(registered) == 1
    name, ds, window = registered[0]
    assert name == "vuln.tif" and ds == "ds_2020" and window is None
