"""Recovery of stats from disk for models trained before Spec A (§3)."""

import pickle
from types import SimpleNamespace

import numpy as np
import pandas as pd

from spatialrisk.mlmodels import (
    GLMModel,
    ICARModel,
    JNRBenchmarkModel,
    MWModel,
    RFModel,
)
from spatialrisk.mlmodels.stats_recovery import recover_stats

FORMULA = "I(loss) + trial ~ scale(a) + C(cat, levels=[0, 1, 2])"


def _samples_csv(path, n=300):
    """Write a training-sample CSV shaped like extract_at_points() output."""
    rng = np.random.default_rng(2)
    df = pd.DataFrame(
        {
            "loss": rng.integers(0, 2, size=n),
            "a": rng.normal(size=n),
            "cat": rng.integers(0, 3, size=n),
            "cell_id": np.arange(n),
            "trial": 1,
        }
    )
    df.to_csv(path, index=False)
    return df


def _fit_pickle(tmp_path, cls_name):
    """Fit a real estimator and save it in the pre-change pickle payload."""
    from patsy import dmatrices

    csv = tmp_path / "samples.csv"
    df = _samples_csv(csv)
    y, x = dmatrices(FORMULA, df, NA_action="drop")
    if cls_name == "glm":
        from sklearn.linear_model import LogisticRegression

        clf = LogisticRegression(max_iter=500).fit(np.asarray(x), np.asarray(y)[:, 0])
    else:
        from sklearn.ensemble import RandomForestClassifier

        clf = RandomForestClassifier(n_estimators=10, random_state=0).fit(
            np.asarray(x), np.asarray(y)[:, 0]
        )
    pkl = tmp_path / f"{cls_name}.pickle"
    with open(pkl, "wb") as fh:
        pickle.dump(
            {
                "ml_model": clf,
                "design_sample": None,
                "formula": FORMULA,
                "samples_path": str(csv),
            },
            fh,
        )
    return pkl, csv


def test_recover_glm_names_come_from_the_design(tmp_path):
    """GLM coefficient labels are rebuilt from the patsy design, not features."""
    pkl, csv = _fit_pickle(tmp_path, "glm")
    m = GLMModel(name="old", formula=FORMULA, model_path=pkl, samples_path=csv)
    s = recover_stats(m)
    assert s is not None
    names = [c.name for c in s.coefficients]
    assert "scale(a)" in names and "Intercept" not in names
    assert any(n.startswith("C(cat") for n in names)
    assert s.intercept_fitted is not None
    # scale() is a stateful transform: one NaN in a scaled column makes patsy
    # return zero rows instead of dropping a few, so the count is asserted.
    assert s.n_rows == 300 and s.n_events == int(pd.read_csv(csv)["loss"].sum())


def test_recover_rf(tmp_path):
    """RF recovery yields importances and never invents an OOB score."""
    pkl, csv = _fit_pickle(tmp_path, "rf")
    m = RFModel(name="old", formula=FORMULA, model_path=pkl, samples_path=csv)
    s = recover_stats(m)
    assert s is not None and len(s.importances) > 0
    # oob was not enabled on old fits — recovery must not invent it
    assert s.oob_accuracy is None


def test_recover_returns_none_when_samples_csv_is_gone(tmp_path):
    """Without the training CSV there are no honest labels, so nothing is returned."""
    pkl, csv = _fit_pickle(tmp_path, "glm")
    csv.unlink()
    m = GLMModel(name="old", formula=FORMULA, model_path=pkl, samples_path=csv)
    assert recover_stats(m) is None  # never guess names from feature_names


def test_recover_icar_point_estimates_only(tmp_path):
    """Recovery of an iCAR fit gives point estimates; the posterior is gone."""
    csv = tmp_path / "samples.csv"
    _samples_csv(csv)
    icar_formula = FORMULA + " + cell"
    # 1 Intercept + 2 C(cat) levels + 1 scale(a) = 4 betas for this design
    payload = {
        "ml_model": {
            "betas": np.array([-3.0, 0.1, 0.2, 0.4]),
            "rho": np.array([0.5, -0.5]),
            "Vrho": 31.8,
            "deviance": float("nan"),
            "formula": icar_formula,
        },
        "formula": FORMULA,
        "samples_path": str(csv),
    }
    pkl = tmp_path / "icar.pickle"
    with open(pkl, "wb") as fh:
        pickle.dump(payload, fh)
    m = ICARModel(name="old", formula=FORMULA, model_path=pkl, samples_path=csv)
    s = recover_stats(m)
    assert s is not None
    assert all(c.std is None for c in s.coefficients)  # posterior unrecoverable
    assert s.vrho.estimate == 31.8
    assert s.rho_min == -0.5 and s.rho_max == 0.5


def test_recover_mw_from_tab_dist(tmp_path):
    """MW recovery reads the period table sitting beside the ldefrate rasters."""
    period = tmp_path / "p1"
    period.mkdir()
    pd.DataFrame(
        {
            "distance": [30, 270, 2010, 4830],
            "npix": [10, 5, 2, 0],
            "area": [0.9, 0.45, 0.18, 0.0],
            "cum": [0.9, 1.35, 1.53, 1.53],
            "perc": [58.8, 88.2, 99.5, 100.0],
        }
    ).to_csv(period / "tab_dist.csv", index=False)
    m = MWModel(
        name="old",
        dist_thresh=2010.0,
        ldefrate_files={"5": period / "ldefrate_mw_5.tif"},
    )
    s = recover_stats(m)
    assert s is not None
    assert s.tot_defor_ha == 1.53  # last cum row
    assert s.perc_thresh == 99.5  # perc at distance == dist_thresh
    assert s.tab_dist_path == period / "tab_dist.csv"


def test_recover_jnr_reads_its_own_training_period(tmp_path):
    """JNR recovery picks the model's period folder, not a sibling model's."""
    root = tmp_path / "rmj_bm"
    for period, cum in (("calibration", 10.0), ("validation", 99.0)):
        period_dir = root / period
        period_dir.mkdir(parents=True)
        pd.DataFrame(
            {
                "distance": [30, 270],
                "npix": [10, 5],
                "area": [cum / 2, cum / 2],
                "cum": [cum / 2, cum],
                "perc": [50.0, 100.0],
            }
        ).to_csv(period_dir / "tab_dist.csv", index=False)
    m = JNRBenchmarkModel(
        name="bm",
        dataset_name="calibration",
        dist_thresh=270.0,
        dist_bins=[0.0, 100.0, 270.0],
        project=SimpleNamespace(folders=SimpleNamespace(rmj_bm=root)),
    )
    s = recover_stats(m)
    assert s is not None and s.n_classes == 2
    assert s.tot_defor_ha == 10.0 and s.perc_thresh == 100.0
    assert s.tab_dist_path == root / "calibration" / "tab_dist.csv"
    # The figure was never written here; recovery leaves the field empty rather
    # than recording a path that cannot exist.
    assert s.perc_dist_png is None


def test_recover_is_read_only(tmp_path):
    """Recovery touches neither the model's fields nor the files on disk."""
    pkl, csv = _fit_pickle(tmp_path, "glm")
    m = GLMModel(name="old", formula=FORMULA, model_path=pkl, samples_path=csv)
    before_fields = m.model_dump(mode="json")
    before_disk = {p: p.stat().st_mtime_ns for p in sorted(tmp_path.rglob("*"))}
    assert recover_stats(m) is not None
    assert m.stats is None
    assert m.model_dump(mode="json") == before_fields
    assert {p: p.stat().st_mtime_ns for p in sorted(tmp_path.rglob("*"))} == before_disk


def test_recover_never_raises_on_garbage(tmp_path):
    """Untrained models, unknown types and None all return None instead of raising."""
    assert recover_stats(GLMModel(name="x")) is None
    assert recover_stats(JNRBenchmarkModel(name="x")) is None
    assert recover_stats(None) is None
    # A file that is not a pickle at all exercises the outer guard.
    junk = tmp_path / "junk.pickle"
    junk.write_bytes(b"not a pickle")
    assert recover_stats(GLMModel(name="x", model_path=junk)) is None
