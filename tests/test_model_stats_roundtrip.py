"""model.stats must survive the exact save/load path Project uses."""

import json
from pathlib import Path

from spatialrisk.mlmodels import (
    GLMModel,
    ICARModel,
    JNRBenchmarkModel,
    MWModel,
    RFModel,
)
from spatialrisk.mlmodels.stats import (
    Coefficient,
    GLMStats,
    ICARStats,
    Importance,
    JNRStats,
    MWStats,
    RFStats,
)


def _round_trip(model, cls):
    """Helper to serialize and deserialize a model via JSON."""
    data = json.loads(json.dumps(model.model_dump(mode="json")))
    return cls(**data)


def test_glm_stats_round_trip():
    """GLM stats survive the project's save/load round-trip."""
    m = GLMModel(
        name="m",
        stats=GLMStats(
            n_rows=19997,
            n_events=10000,
            sample_design="random (random_1)",
            coefficients=[Coefficient(name="scale(towns_dist)", estimate=-0.4746)],
            intercept_design=-0.0565,
            intercept_fitted=-0.0572,
            n_iter=22,
            max_iter=1000,
        ),
    )
    m2 = _round_trip(m, GLMModel)
    assert m2.stats.coefficients[0].estimate == -0.4746
    assert m2.stats.intercept_fitted == -0.0572


def test_rf_stats_round_trip():
    """RF stats survive the project's save/load round-trip."""
    m = RFModel(
        name="m",
        stats=RFStats(
            importances=[Importance(name="towns_dist", value=0.2807)], oob_accuracy=0.81
        ),
    )
    assert _round_trip(m, RFModel).stats.importances[0].value == 0.2807


def test_icar_stats_round_trip():
    """ICAR stats survive the project's save/load round-trip."""
    m = ICARModel(
        name="m",
        stats=ICARStats(
            coefficients=[
                Coefficient(
                    name="scale(rivers_dist)",
                    estimate=0.4263,
                    std=0.07,
                    ci_low=0.29,
                    ci_high=0.56,
                )
            ],
            vrho=Coefficient(name="Vrho", estimate=31.78),
            rho_mean=0.02,
        ),
    )
    m2 = _round_trip(m, ICARModel)
    assert m2.stats.coefficients[0].ci_high == 0.56
    assert m2.stats.vrho.estimate == 31.78


def test_mw_and_jnr_stats_round_trip_with_nested_path():
    """MW and JNR stats with nested Path fields survive round-trip."""
    mw = MWModel(
        name="m",
        stats=MWStats(
            dist_thresh=270.0,
            perc_thresh=99.5,
            tot_defor_ha=316892.88,
            tab_dist_path=Path("/tmp/x/tab_dist.csv"),
        ),
    )
    mw2 = _round_trip(mw, MWModel)
    # Nested Path fields are coerced back by pydantic — no manual
    # rehydration in project.py's load loop (spec §1).
    assert isinstance(mw2.stats.tab_dist_path, Path)
    assert mw2.stats.tab_dist_path == Path("/tmp/x/tab_dist.csv")

    jnr = JNRBenchmarkModel(name="j", stats=JNRStats(n_classes=29))
    assert _round_trip(jnr, JNRBenchmarkModel).stats.n_classes == 29


def test_stats_none_round_trips_as_none():
    """Stats defaults to None and round-trips as None."""
    assert _round_trip(GLMModel(name="m"), GLMModel).stats is None
