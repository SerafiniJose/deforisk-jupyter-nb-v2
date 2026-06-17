def test_make_prediction_payload_supervised_fields():
    from spatialrisk.predictors.registration import make_prediction_payload

    class _Var:
        def __init__(self, name, year=None):
            self.name, self.year = name, year

    class _DS:
        name = "ds_2020"
        year = 2020
        target = _Var("forest_loss", 2020)
        features = [_Var("slope"), _Var("dist_road")]

    payload = make_prediction_payload(
        path="/tmp/glm.tif",
        model_key="glm_m1",
        dataset=_DS(),
        year=None,            # falls back to dataset year via model_year arg
        model_year=2020,
        window=None,
        model_snapshot={"model_type": "glm", "name": "m1"},
    )

    assert payload["path"] == "/tmp/glm.tif"
    assert payload["model_key"] == "glm_m1"
    assert payload["dataset_name"] == "ds_2020"
    assert payload["year"] == 2020          # model_year fallback
    assert payload["window"] is None
    assert payload["model_snapshot"]["model_type"] == "glm"
    assert payload["dataset_snapshot"]["feature_names"] == ["slope", "dist_road"]
    assert payload["dataset_snapshot"]["target_name"] == "forest_loss"


def test_register_supervised_calls_callback_once():
    from spatialrisk.predictors.registration import register_supervised

    class _DS:
        name = "ds"
        year = None
        target = None
        features = []

    seen = []
    register_supervised(
        register_prediction=lambda **kw: seen.append(kw),
        path="/tmp/out.tif",
        model_key="rf_x",
        dataset=_DS(),
        year=2019,
        model_year=None,
        window=None,
        model_snapshot={"model_type": "rf"},
    )
    assert len(seen) == 1
    assert seen[0]["model_key"] == "rf_x"
    assert seen[0]["year"] == 2019
    assert seen[0]["window"] is None
