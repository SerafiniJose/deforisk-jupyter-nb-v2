"""fit()-side stats wiring for GLM and RF (Spec A §2.1, §2.3).

Drives _collect_stats_from_design directly with a synthetic patsy design so
no Dataset/Sample fixture is needed; fit() calls the same method.
"""

import numpy as np
import pandas as pd
from patsy import dmatrices

from spatialrisk.mlmodels import GLMModel, RFModel


def _frame(n=300, with_na=True):
    rng = np.random.default_rng(1)
    df = pd.DataFrame(
        {
            "loss": rng.integers(0, 2, size=n),
            "a": rng.normal(size=n),
            "cat": rng.integers(0, 3, size=n),
            "trial": 1,
        }
    )
    if with_na:
        # 5 rows dropped by NA_action="drop". NaNs go in "cat", not "a":
        # scale(a) is a patsy stateful transform whose memorize_chunk pass
        # computes a running mean/M2 over the WHOLE column before NA-drop
        # filtering runs, so any NaN in "a" poisons the mean for every row
        # and all 300 rows get dropped instead of 5 (patsy 1.0.2). "cat"
        # only feeds the stateless C() factor coding, so it drops cleanly.
        df.loc[:4, "cat"] = np.nan
    return df


FORMULA = "I(loss) + trial ~ scale(a) + C(cat, levels=[0, 1, 2])"


def test_glm_fit_stats_from_design(tmp_path):
    """_collect_stats_from_design populates GLMStats from a real patsy design."""
    df = _frame()
    m = GLMModel(name="m", max_iter=500)
    y, x = dmatrices(FORMULA, df, NA_action="drop")
    from sklearn.linear_model import LogisticRegression

    clf = LogisticRegression(max_iter=500).fit(np.asarray(x), np.asarray(y)[:, 0])
    m._ml_model = clf
    m._collect_stats_from_design(y, x)
    s = m.stats
    assert s is not None
    # post-NA row count, not len(df)
    assert s.n_rows == 295 and s.n_rows == x.shape[0]
    assert s.n_events == int(np.asarray(y)[:, 0].sum())
    # names come from the design, factor levels expanded
    names = [c.name for c in s.coefficients]
    assert "scale(a)" in names
    assert any(n.startswith("C(cat") for n in names)
    assert "Intercept" not in names
    assert s.intercept_design is not None and s.intercept_fitted is not None
    assert s.max_iter == 500 and s.n_iter is not None


def test_rf_fit_stats_from_design(tmp_path):
    """_collect_stats_from_design populates RFStats from a real patsy design."""
    df = _frame(with_na=False)
    m = RFModel(name="m", n_trees=20)
    y, x = dmatrices(FORMULA, df, NA_action="drop")
    from sklearn.ensemble import RandomForestClassifier

    clf = RandomForestClassifier(n_estimators=20, oob_score=True, random_state=0).fit(
        np.asarray(x), np.asarray(y)[:, 0]
    )
    m._ml_model = clf
    m._collect_stats_from_design(y, x)
    assert m.stats is not None
    assert m.stats.oob_accuracy is not None
    assert [i.name for i in m.stats.importances][0] != "Intercept"
