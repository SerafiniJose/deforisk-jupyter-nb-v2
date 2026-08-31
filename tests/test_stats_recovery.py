"""Recovery of stats from disk for models trained before Spec A (§3)."""

import pickle

import numpy as np
import pandas as pd
import pytest

from spatialrisk.mlmodels import (
    GLMModel,
    ICARModel,
    JNRBenchmarkModel,
    MWModel,
    RFModel,
)
from spatialrisk.mlmodels.stats import JNRStats
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


def _write_tab_dist(folder, *, distance, cum, perc):
    """Write a dist_edge_threshold-shaped tab_dist.csv into *folder*."""
    pd.DataFrame(
        {
            "distance": distance,
            "npix": [1] * len(distance),
            "area": np.diff(np.asarray(cum, dtype=float), prepend=0.0),
            "cum": cum,
            "perc": perc,
        }
    ).to_csv(folder / "tab_dist.csv", index=False)


def _sandboxed_project(monkeypatch, tmp_path, name="recovery"):
    """A real Project rooted in tmp_path (no folder tree created)."""
    import spatialrisk.project as project_module
    from spatialrisk import Project

    monkeypatch.setattr(project_module, "downloads_folder", tmp_path)
    return Project(project_name=name)


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


def test_recover_mw_total_deforestation_covers_unbinned_pixels(tmp_path):
    """tot_defor_ha is the full deforested area, not only the binned part.

    ``cum`` accumulates the pixels that fell inside dist_bins, while fit()
    stores dist_edge_threshold's ``tot_def`` — every deforested pixel. The two
    coincide only when perc reaches 100, so the table's own
    ``perc = 100 * cum / tot_def`` is divided back out.
    """
    period = tmp_path / "p1"
    period.mkdir()
    _write_tab_dist(period, distance=[30, 270], cum=[0.9, 1.8], perc=[45.0, 90.0])
    m = MWModel(
        name="old",
        dist_thresh=270.0,
        ldefrate_files={"5": period / "ldefrate_mw_5.tif"},
    )
    s = recover_stats(m)
    assert s.tot_defor_ha == pytest.approx(2.0)  # 1.8 ha is only 90% of it
    assert s.perc_thresh == 90.0

    # A table with no deforestation at all must not divide by zero.
    empty = tmp_path / "p2"
    empty.mkdir()
    _write_tab_dist(empty, distance=[30], cum=[0.0], perc=[0.0])
    m2 = MWModel(
        name="old", dist_thresh=30.0, ldefrate_files={"5": empty / "ldefrate.tif"}
    )
    assert recover_stats(m2).tot_defor_ha is None


def test_recover_jnr_reads_its_own_training_period(monkeypatch, tmp_path):
    """JNR recovery picks the model's period folder, not a sibling model's."""
    project = _sandboxed_project(monkeypatch, tmp_path, name="proj")
    root = tmp_path / "proj" / "rmj_bm"
    for period, cum in (("calibration", 10.0), ("validation", 99.0)):
        period_dir = root / period
        period_dir.mkdir(parents=True)
        _write_tab_dist(
            period_dir, distance=[30, 270], cum=[cum / 2, cum], perc=[50.0, 100.0]
        )
    m = JNRBenchmarkModel(
        name="bm",
        dataset_name="calibration",
        dist_thresh=270.0,
        dist_bins=[0.0, 100.0, 270.0],
        project=project,
    )
    s = recover_stats(m)
    assert s is not None and s.n_classes == 2
    assert s.tot_defor_ha == 10.0 and s.perc_thresh == 100.0
    assert s.tab_dist_path == root / "calibration" / "tab_dist.csv"
    # The figure was never written here; recovery leaves the field empty rather
    # than recording a path that cannot exist.
    assert s.perc_dist_png is None

    # The stats class follows the family, not the data: JNRBenchmarkModel.stats
    # is typed Optional[JNRStats], so unpopulated bins must not yield MWStats.
    unbinned = m.model_copy(update={"dist_bins": []})
    recovered = recover_stats(unbinned)
    assert isinstance(recovered, JNRStats) and recovered.n_classes == 0


def test_recover_with_a_real_project_creates_no_folders(monkeypatch, tmp_path):
    """Opening stats for an MW/JNR model must not build a project folder tree.

    ``project.folders`` is a property that calls ``initialize_folders()``, which
    mkdirs the project folder and all ten of its sub-folders. Reaching the
    output folder through the models' ``_default_folder()`` therefore created
    nine directories per call — resurrecting a deleted tree, and raising
    outright on a read-only mount. Fakes cannot catch this: their ``folders``
    is a plain attribute, so this test uses a real Project.
    """
    project = _sandboxed_project(monkeypatch, tmp_path)
    for subfolder in ("rmj_bm", "rmj_mw"):
        period_dir = tmp_path / "recovery" / subfolder / "calibration"
        period_dir.mkdir(parents=True)
        _write_tab_dist(
            period_dir, distance=[30, 270], cum=[1.0, 2.0], perc=[50.0, 100.0]
        )
    before = sorted(tmp_path.rglob("*"))

    jnr = JNRBenchmarkModel(
        name="bm",
        dataset_name="calibration",
        dist_thresh=270.0,
        dist_bins=[0.0, 100.0, 270.0],
        project=project,
    )
    # No ldefrate_files, so MW resolves through the project folder too.
    mw = MWModel(
        name="mw", dataset_name="calibration", dist_thresh=270.0, project=project
    )
    assert recover_stats(jnr).tot_defor_ha == 2.0
    assert recover_stats(mw).tot_defor_ha == 2.0
    assert sorted(tmp_path.rglob("*")) == before


def test_recover_returns_none_when_the_formula_drifted_from_the_fit(tmp_path):
    """A design that no longer matches the stored coefficients yields None."""
    pkl, csv = _fit_pickle(tmp_path, "glm")
    payload = pickle.loads(pkl.read_bytes())
    payload["formula"] = "I(loss) + trial ~ scale(a)"  # 2 columns vs 4 coef_
    pkl.write_bytes(pickle.dumps(payload))
    m = GLMModel(name="old", formula=FORMULA, model_path=pkl, samples_path=csv)
    # stats._named_values refuses to zip mismatched labels, so recovery reports
    # nothing rather than mislabelling every coefficient.
    assert recover_stats(m) is None


def test_recover_icar_returns_none_when_the_design_guard_trips(tmp_path):
    """A wrong beta count or a design not ending in 'cell' recovers nothing."""
    csv = tmp_path / "samples.csv"
    _samples_csv(csv)

    def _icar_pickle(filename, formula, betas):
        path = tmp_path / filename
        path.write_bytes(
            pickle.dumps(
                {
                    "ml_model": {
                        "betas": betas,
                        "rho": np.array([0.1, 0.2]),
                        "Vrho": 1.0,
                        "deviance": 1.0,
                        "formula": formula,
                    },
                    "formula": FORMULA,
                    "samples_path": str(csv),
                }
            )
        )
        return path

    too_few = _icar_pickle("short.pickle", FORMULA + " + cell", np.array([-3.0, 0.1]))
    # No '+ cell': the last design column is a real covariate, and consuming it
    # as the spatial term would shift every label by one.
    no_cell = _icar_pickle("nocell.pickle", FORMULA, np.array([-3.0, 0.1, 0.2, 0.4]))
    for pkl in (too_few, no_cell):
        m = ICARModel(name="old", formula=FORMULA, model_path=pkl, samples_path=csv)
        assert recover_stats(m) is None


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
