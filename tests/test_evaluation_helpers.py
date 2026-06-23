import types
import types as _types
from pathlib import Path

import pandas as pd

from gui.tile import evaluation_helpers as h
from gui.tile.evaluation_helpers import build_evaluation_record


def _var(name, year=None, path=None):
    return _types.SimpleNamespace(name=name, year=year,
                                  path=path or f"/data/{name}.tif")


def _project():
    pv = {
        "forest_loss_2015_2020": _var("forest_loss_2015_2020"),
        "forest_gfc": _var("forest_gfc"),
    }
    preds = {
        "glm__cal": _types.SimpleNamespace(model_key="glm_glm_v1", window=None,
                                           dataset_name="calibration"),
        "mw__val": _types.SimpleNamespace(model_key="mw_calibration_mw", window=11,
                                          dataset_name="validation"),
    }
    return _types.SimpleNamespace(processed_variables=pv, predictions=preds)


def test_variable_items_lists_instances():
    items = h.variable_items(_project())
    values = {i["value"] for i in items}
    assert values == {"forest_loss_2015_2020", "forest_gfc"}


def test_variable_items_labels_year_when_present():
    proj = _types.SimpleNamespace(
        processed_variables={"forest_loss_2015": _var("forest_loss", year=2015)},
        predictions={})
    items = h.variable_items(proj)
    assert items[0]["text"] == "forest_loss (2015)"
    assert items[0]["value"] == "forest_loss_2015"


def test_map_items_labels_model_and_period():
    items = h.map_items(_project())
    texts = {i["text"] for i in items}
    assert texts == {"GLM — calibration", "MW_w11 — validation"}


def test_default_forest_key_finds_forest_gfc():
    assert h.default_forest_key(_project()) == "forest_gfc"


def test_default_forest_key_none_when_absent():
    proj = _types.SimpleNamespace(
        processed_variables={"altitude": _var("altitude")}, predictions={})
    assert h.default_forest_key(proj) is None


def test_parse_interval_from_truth_name():
    assert h.parse_interval(_project(), "forest_loss_2015_2020") == 5


def test_parse_interval_none_for_unparseable():
    proj = _types.SimpleNamespace(
        processed_variables={"forest_gfc": _var("forest_gfc")}, predictions={})
    assert h.parse_interval(proj, "forest_gfc") is None


def test_parse_interval_none_when_project_has_no_processed_variables():
    assert h.parse_interval(_types.SimpleNamespace(), "anything") is None


def test_build_truth_spec_resolves_paths():
    spec, err = h.build_truth_spec(
        _project(), "forest_loss_2015_2020", "forest_gfc", "5")
    assert err is None
    assert spec["defor_file"] == "/data/forest_loss_2015_2020.tif"
    assert spec["forest_file"] == "/data/forest_gfc.tif"
    assert spec["time_interval"] == 5
    assert spec["truth_tag"] == "forest_loss_2015_2020"


def test_build_truth_spec_tags_year_when_present():
    proj = _types.SimpleNamespace(
        processed_variables={"floss_2015": _var("forest_loss", year=2015),
                             "forest_gfc": _var("forest_gfc")},
        predictions={})
    spec, err = h.build_truth_spec(proj, "floss_2015", "forest_gfc", "1")
    assert err is None and spec["truth_tag"] == "forest_loss_2015"


def test_build_truth_spec_requires_truth():
    spec, err = h.build_truth_spec(_project(), "", "forest_gfc", "5")
    assert spec is None and "truth" in err.lower()


def test_build_truth_spec_requires_forest():
    spec, err = h.build_truth_spec(_project(), "forest_loss_2015_2020", "", "5")
    assert spec is None and "forest" in err.lower()


def test_build_truth_spec_rejects_nonpositive_interval():
    spec, err = h.build_truth_spec(
        _project(), "forest_loss_2015_2020", "forest_gfc", "0")
    assert spec is None and "positive" in err.lower()


def test_build_truth_spec_rejects_nonnumeric_interval():
    spec, err = h.build_truth_spec(
        _project(), "forest_loss_2015_2020", "forest_gfc", "abc")
    assert spec is None and "whole number" in err.lower()


def _fake_project(tmp):
    return types.SimpleNamespace(
        folders=types.SimpleNamespace(project_folder=Path(tmp))
    )


def test_build_evaluation_record_maps_df_and_paths(tmp_path):
    df = pd.DataFrame([
        {"model": "GLM", "period": "ds_A", "MedAE": 12.3, "R2": 0.81},
        {"model": "RF", "period": "ds_A", "MedAE": 9.1, "R2": 0.88},
    ])
    spec = {
        "defor_file": "/x/forest_loss_2015_2020.tif",
        "forest_file": "/x/forest_gfc.tif",
        "time_interval": 5,
        "truth_tag": "forest_loss_2015_2020",
    }
    rec = build_evaluation_record(
        _fake_project(tmp_path), df, spec,
        resolved_keys=["glm__ds_A", "rf__ds_A"],
        run_id="abcd1234", created_at="2026-06-22T14:05:33",
    )
    assert rec.truth_tag == "forest_loss_2015_2020"
    assert rec.name == "forest_loss_2015_2020"
    assert rec.time_interval == 5
    assert rec.prediction_keys == ["glm__ds_A", "rf__ds_A"]
    assert rec.indices[0]["MedAE"] == 12.3       # JSON-native float, not str
    assert rec.indices[1]["model"] == "RF"
    assert rec.csv_path.endswith(
        "evaluation/forest_loss_2015_2020/indices_all.csv")
    assert rec.run_id == "abcd1234"


# --- cell-size parsing -------------------------------------------------------

def test_parse_csizes_single_value():
    sizes, err = h.parse_csizes("300")
    assert err is None and sizes == [300]


def test_parse_csizes_comma_separated():
    sizes, err = h.parse_csizes("100, 300, 1000")
    assert err is None and sizes == [100, 300, 1000]


def test_parse_csizes_space_separated():
    sizes, err = h.parse_csizes("100 300")
    assert err is None and sizes == [100, 300]


def test_parse_csizes_dedupes_preserving_order():
    sizes, err = h.parse_csizes("300, 100, 300")
    assert err is None and sizes == [300, 100]


def test_parse_csizes_rejects_empty():
    sizes, err = h.parse_csizes("   ")
    assert sizes is None and "cell size" in err.lower()


def test_parse_csizes_rejects_noninteger():
    sizes, err = h.parse_csizes("100, abc")
    assert sizes is None and "whole number" in err.lower()


def test_parse_csizes_rejects_nonpositive():
    sizes, err = h.parse_csizes("100, 0")
    assert sizes is None and "positive" in err.lower()


# --- metric options + display filtering --------------------------------------

def test_metric_items_lists_four_indices():
    items = h.metric_items()
    assert [i["value"] for i in items] == ["MedAE", "R2", "RMSE", "wRMSE"]
    # display label uses the squared glyph for R2
    assert next(i["text"] for i in items if i["value"] == "R2") == "R²"


def test_displayed_indices_drops_unselected_metric_columns():
    indices = [
        {"model": "GLM", "period": "ds_A", "MedAE": 12.3, "R2": 0.81,
         "RMSE": 5.0, "wRMSE": 4.0},
    ]
    out = h.displayed_indices(indices, ["MedAE", "R2"])
    assert out == [{"model": "GLM", "period": "ds_A", "MedAE": 12.3, "R2": 0.81}]


def test_displayed_indices_empty_metrics_keeps_all_columns():
    indices = [{"model": "GLM", "MedAE": 12.3, "RMSE": 5.0}]
    assert h.displayed_indices(indices, []) == indices


def test_displayed_indices_keeps_context_columns():
    indices = [{"model": "GLM", "period": "ds_A", "ncell": 42,
                "csize_coarse_grid_ha": 81.0, "MedAE": 12.3, "RMSE": 5.0}]
    out = h.displayed_indices(indices, ["MedAE"])
    assert out == [{"model": "GLM", "period": "ds_A", "ncell": 42,
                    "csize_coarse_grid_ha": 81.0, "MedAE": 12.3}]


def test_rows_for_record_handles_record_without_metrics_attr():
    # Records created before the 'metrics' field existed lack the attribute
    # entirely (stale in-memory instance after hot-reload). Must not crash.
    legacy = _types.SimpleNamespace(
        indices=[{"model": "GLM", "MedAE": 12.3, "R2": 0.81, "RMSE": 5.0}])
    assert not hasattr(legacy, "metrics")
    assert h.rows_for_record(legacy) == legacy.indices  # all columns kept


def test_rows_for_record_applies_selected_metrics():
    rec = _types.SimpleNamespace(
        indices=[{"model": "GLM", "MedAE": 12.3, "R2": 0.81, "RMSE": 5.0}],
        metrics=["MedAE"])
    assert h.rows_for_record(rec) == [{"model": "GLM", "MedAE": 12.3}]


def test_build_evaluation_record_stores_metrics_and_csizes(tmp_path):
    df = pd.DataFrame([{"model": "GLM", "period": "ds_A", "MedAE": 12.3}])
    spec = {
        "defor_file": "/x/d.tif", "forest_file": "/x/f.tif",
        "time_interval": 5, "truth_tag": "forest_loss_2015_2020",
    }
    rec = build_evaluation_record(
        _fake_project(tmp_path), df, spec, resolved_keys=["glm__ds_A"],
        run_id="abcd1234", created_at="2026-06-22T14:05:33",
        csizes=(100, 300), metrics=["MedAE", "R2"],
    )
    assert rec.csizes == [100, 300]
    assert rec.metrics == ["MedAE", "R2"]
