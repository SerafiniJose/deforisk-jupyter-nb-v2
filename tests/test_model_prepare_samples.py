import pandas as pd


class _StubDataset:
    def __init__(self):
        class _V:
            def __init__(self, n):
                self.name = n
        self.target = _V("target")
        self.features = [_V("altitude")]
        self.year = None

    def extract_at_points(self, points, *, drop_nodata=True):
        return pd.DataFrame(
            {"target": [0, 1, 0], "altitude": [1.0, 2.0, 3.0],
             "cell_id": [1, 2, 3], "trial": [1, 1, 1]}
        )


class _StubSample:
    name = "s1"

    def load_points(self):
        return object()


def test_prepare_samples_uses_dataset_extraction(tmp_path):
    from spatialrisk.mlmodels.glm_model import GLMModel

    m = GLMModel(name="m")
    m.dataset = _StubDataset()
    m.sample = _StubSample()
    df, formula = m._prepare_samples(output_csv=tmp_path / "s.csv")

    assert list(df.columns) == ["target", "altitude", "cell_id", "trial"]
    assert m.target_name == "target"
    assert m.feature_names == ["altitude"]
    assert "target" in formula and "altitude" in formula


def test_model_has_no_sample_set_field():
    from spatialrisk.mlmodels.glm_model import GLMModel
    fields = GLMModel.model_fields          # excluded-from-dump fields are still listed here
    assert "sample" in fields
    assert "sample_name" in fields
    assert "sample_set" not in fields
    assert "sampling" not in fields
