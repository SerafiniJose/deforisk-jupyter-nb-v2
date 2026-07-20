"""Unit tests for the interactive predicted-vs-observed scatter (Task 6).

Module under test: ``gui.scripts.evaluation_echarts`` — the solara-free half of
the scatter. It resolves a saved run's point CSV, loads it, and turns it into a
plain ECharts option dict. The widget half (Task 7) only renders what this
builds, so everything worth asserting about the chart is asserted here.

The headline assertion is ``test_scatter_coordinates_are_exactly_the_saved_csv
_values``: the coordinates the chart plots must be the *exact* floats sitting in
``pred_obs_*.csv``. That is the whole point of the migration — the interactive
chart and the archived PNG are two renderings of one frozen table, and any
rounding, resampling or downsampling between them would make the two disagree.

That assertion reads the expected values with ``float()`` on the raw file TEXT,
never with a second ``pd.read_csv``. pandas' default C float parser is fast but
not correctly rounded: it returns ``0.92`` for the text ``0.9199999999999999``.
Comparing one pandas parse against another is self-consistent with that error
and so cannot see it — the file's bytes are the only neutral referee.
"""

import json
from pathlib import Path

import pytest

_POINT_COLUMNS = ["cell", "nfor_obs", "ndefor_obs",
                  "nfor_obs_ha", "ndefor_obs_ha", "ndefor_pred_ha"]

# Deliberately awkward floats. Each does NOT survive a round(x, 2) or a float32
# trip, and the first, second and fourth are also mis-parsed by pandas' DEFAULT
# CSV float parser (-> 0.92, 0.3, 123456789.1234568), so the headline equality
# assertion fails unless the loader reads with float_precision="round_trip".
_OBS = [0.9199999999999999, 0.30000000000000004,
        3.3333333333333335, 123456789.12345679]
_PRED = [1.0700000000000003, 0.7000000000000001,
         4.000000000000001, 123456789.12345678]


def _write_points(path, obs=None, pred=None, forest=None):
    """Write the frozen 6-column point CSV and return the frame it holds."""
    import pandas as pd

    obs = list(_OBS if obs is None else obs)
    pred = list(_PRED if pred is None else pred)
    forest = list(range(10, 10 + len(obs))) if forest is None else list(forest)
    frame = pd.DataFrame({
        "cell": list(range(len(obs))),
        "nfor_obs": [100] * len(obs),
        "ndefor_obs": [7] * len(obs),
        "nfor_obs_ha": [float(f) for f in forest],
        "ndefor_obs_ha": obs,
        "ndefor_pred_ha": pred,
    })[_POINT_COLUMNS]
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return frame


def _csv_floats(csv_path, *columns):
    """Columns of a point CSV parsed with ``float()`` straight from the TEXT.

    The neutral referee for "the chart plots the saved values". A second
    ``pd.read_csv`` is NOT one: pandas' default float parser is not correctly
    rounded (text ``0.9199999999999999`` parses to ``0.92``), so a pandas-vs-
    pandas comparison is self-consistent with its own error. ``float()`` is
    correctly rounded and is the exact inverse of the round-trippable repr
    ``to_csv`` writes.
    """
    lines = csv_path.read_text().strip().splitlines()
    header = lines[0].split(",")
    idx = [header.index(c) for c in columns]
    return [tuple(float(row[i]) for i in idx)
            for row in (line.split(",") for line in lines[1:])]


def _index_row(model="GLM", period="d1", csize=300, **over):
    row = {"model": model, "period": period, "prediction": f"{model}__{period}",
           "csize_coarse_grid": csize, "csize_coarse_grid_ha": 90.0,
           "MedAE": 1.5, "R2": 0.9, "RMSE": 2.0, "wRMSE": 1.8, "ncell": 4}
    row.update(over)
    return row


def _record(run_dir, *, artifacts=None, indices=None, run_id="run1"):
    """A real EvaluationRecord pointing at ``run_dir``."""
    from spatialrisk.evaluations import EvaluationRecord

    return EvaluationRecord(
        truth_tag="loss_2010", truth_defor="/d.tif", truth_forest="/f.tif",
        time_interval=5, created_at="2026-07-20T10:00:00", run_id=run_id,
        csizes=[300], indices=list(indices if indices is not None
                                   else [_index_row()]),
        csv_path=str(run_dir / "indices_all.csv"),
        artifacts=list(artifacts or []),
    )


def _typed_record(run_dir, *, csize=300, model="GLM", period="d1", **kw):
    from spatialrisk.evaluations import EvaluationPlotArtifact

    artifact = EvaluationPlotArtifact(
        prediction_key=f"{model}__{period}", model=model, period=period,
        csize_px=csize,
        points_csv=str(run_dir / f"pred_obs_{model}_{period}_{csize}.csv"),
        png_path=str(run_dir / f"pred_obs_{model}_{period}_{csize}.png"),
    )
    return _record(run_dir, artifacts=[artifact], **kw)


def _saved(tmp_path, **kw):
    """Write a run's point CSV and return ``(csv_path, record)``."""
    run_dir = tmp_path / "evaluation" / "loss_2010" / "run1"
    csv_path = run_dir / "pred_obs_GLM_d1_300.csv"
    _write_points(csv_path, **kw)
    return csv_path, _typed_record(run_dir)


def _loaded(tmp_path, **kw):
    """Write a run's CSV, then load it back through the public loader.

    Returns ``(csv_frame, plot_data)`` where ``csv_frame`` is an INDEPENDENT
    read of the file on disk — never the pre-save frame. The CSV is the frozen
    artifact both this chart and the archived PNG render, so it, not whatever
    was in memory before ``to_csv`` wrote it, is what the chart must match.

    ``float_precision="round_trip"`` for the same reason the loader uses it: the
    default parser is not correctly rounded, so a default read here would make
    this helper's frame disagree with the file's own text by up to 1 ulp and
    every test built on it would inherit that error. The headline test skips
    this helper entirely and reads the text with ``float()`` (``_csv_floats``),
    which is the parser-independent check.
    """
    import pandas as pd

    from gui.scripts.evaluation_echarts import load_pred_obs_plot_data

    csv_path, record = _saved(tmp_path, **kw)
    return (pd.read_csv(csv_path, float_precision="round_trip"),
            load_pred_obs_plot_data(record, "GLM", "d1", 300))


def _scatter(option):
    return next(s for s in option["series"] if s["type"] == "scatter")


def _refline(option):
    return next(s for s in option["series"] if s["type"] == "line")


# ---------------------------------------------------------------------------
# Headline: the plotted coordinates ARE the saved CSV values
# ---------------------------------------------------------------------------

def test_scatter_coordinates_are_exactly_the_saved_csv_values(tmp_path):
    """No rounding, no resampling: chart floats == CSV floats, bit for bit.

    The interactive scatter and the archived PNG are two renderings of the same
    frozen table. Anything that reshapes the numbers on the way to the option —
    a ``round``, a float32 cast, a downsample, or a lossy CSV *parse* — makes
    the two disagree about what the model predicted, which is the one thing this
    chart exists to show.

    Expected values come from ``float()`` on the file's own text (see
    ``_csv_floats``). Comparing against a second ``pd.read_csv`` would compare
    one lossy parse with an identical lossy parse and pass either way; this
    assertion fails if the loader drops ``float_precision="round_trip"``.
    """
    from gui.scripts.evaluation_echarts import (
        load_pred_obs_plot_data, pred_obs_scatter_option)

    csv_path, record = _saved(tmp_path)
    plot_data = load_pred_obs_plot_data(record, "GLM", "d1", 300)
    option = pred_obs_scatter_option(plot_data)

    plotted = [(v[0], v[1]) for v in _scatter(option)["data"]]
    assert plotted == _csv_floats(csv_path, "ndefor_obs_ha", "ndefor_pred_ha")


def test_every_saved_point_is_plotted(tmp_path):
    """No downsampling, ever — not even above the large-mode threshold.

    Large mode changes HOW the points are drawn (one batched canvas call), never
    how many there are. Dropping points would quietly redraw the model's error
    distribution, so the count is pinned on both sides of the threshold.

    Point count is deliberately several multiples past the threshold, not
    ``threshold + 500``: a cap-style downsample ("keep the first N") only shows
    up when the data is bigger than a cap someone would plausibly pick. The
    first and last rows are asserted too, so truncation from either end is
    caught by value and not only by count.
    """
    from gui.scripts.evaluation_echarts import (
        PRED_OBS_LARGE_POINT_COUNT, load_pred_obs_plot_data,
        pred_obs_scatter_option)

    n = 5 * PRED_OBS_LARGE_POINT_COUNT + 1        # 10001 — past 5000 and 10000
    obs = [float(i) for i in range(n)]
    pred = [float(i) * 1.1 for i in range(n)]
    csv_path, record = _saved(tmp_path, obs=obs, pred=pred)
    plot_data = load_pred_obs_plot_data(record, "GLM", "d1", 300)
    values = _scatter(pred_obs_scatter_option(plot_data))["data"]

    assert len(values) == n
    assert (values[0][0], values[0][1]) == (obs[0], pred[0])
    assert (values[-1][0], values[-1][1]) == (obs[-1], pred[-1])
    assert csv_path.exists()


def test_scatter_values_carry_cell_forest_and_residual(tmp_path):
    """Dimensions 2..4 exist only to reach the tooltip template.

    ECharts tooltips here are template strings (the option is serialized, so no
    callback can cross the wire), which means anything the tooltip shows must
    already be in the value array — including the residual, which is therefore
    precomputed rather than derived at hover time.
    """
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    frame, plot_data = _loaded(tmp_path)
    values = _scatter(pred_obs_scatter_option(plot_data))["data"]

    assert [v[2] for v in values] == frame["cell"].tolist()
    assert [v[3] for v in values] == frame["nfor_obs_ha"].tolist()
    assert [v[4] for v in values] == pytest.approx(
        (frame["ndefor_pred_ha"] - frame["ndefor_obs_ha"]).tolist())


def test_non_finite_rows_are_dropped_but_n_still_counts_them(tmp_path):
    """NaN is not valid JSON — it must never reach the option.

    ``save_pred_obs_png`` drops the same rows from the scatter while its
    annotation keeps quoting the full ``ncell``. Matching that exactly is what
    keeps the interactive chart and the archived PNG telling the same story.
    """
    from gui.scripts.evaluation_echarts import (
        pred_obs_annotation, pred_obs_scatter_option)

    _, plot_data = _loaded(tmp_path, obs=[1.0, float("nan"), 3.0],
                           pred=[1.5, 2.0, float("inf")])
    option = pred_obs_scatter_option(plot_data)

    assert [(v[0], v[1]) for v in _scatter(option)["data"]] == [(1.0, 1.5)]
    assert "n = 3" in pred_obs_annotation(plot_data)


def test_option_is_json_clean(tmp_path):
    """A numpy scalar would make the adapter's digest fall back to str()."""
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path)
    option = pred_obs_scatter_option(plot_data, title="GLM — d1")
    assert json.loads(json.dumps(option)) == option


# ---------------------------------------------------------------------------
# Shared axis domain and the 1:1 reference line
# ---------------------------------------------------------------------------

def test_both_axes_share_one_numeric_domain(tmp_path):
    """An independently scaled y-axis would invalidate the 1:1 comparison.

    Points would sit above or below the reference line for reasons that have
    nothing to do with the model — the single most misleading thing this chart
    could do — so the domain is pinned on both axes, not left to autoscale.
    """
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path)
    option = pred_obs_scatter_option(plot_data)

    assert option["xAxis"]["min"] == option["yAxis"]["min"] == plot_data.axis_min
    assert option["xAxis"]["max"] == option["yAxis"]["max"] == plot_data.axis_max


def test_axis_domain_spans_both_columns(tmp_path):
    """The bounds come from observed AND predicted, as the PNG's do."""
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path, obs=[2.0, 40.0], pred=[1.0, 30.0])
    option = pred_obs_scatter_option(plot_data)

    assert option["xAxis"]["min"] == 1.0     # min lives in the predicted column
    assert option["xAxis"]["max"] == 40.0    # max lives in the observed column


def test_reference_line_spans_the_shared_domain(tmp_path):
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path)
    line = _refline(pred_obs_scatter_option(plot_data))

    assert line["data"] == [[plot_data.axis_min, plot_data.axis_min],
                            [plot_data.axis_max, plot_data.axis_max]]


def test_reference_line_is_dashed_red_like_the_png(tmp_path):
    """The PNG draws it with ``plt.plot(p, p, "r--")``."""
    from gui.scripts.evaluation_echarts import (
        REFERENCE_LINE_COLOR, pred_obs_scatter_option)

    _, plot_data = _loaded(tmp_path)
    line = _refline(pred_obs_scatter_option(plot_data))

    assert line["lineStyle"]["color"] == REFERENCE_LINE_COLOR
    assert line["lineStyle"]["type"] == "dashed"
    assert line["showSymbol"] is False


def test_reference_line_is_silent_and_stays_out_of_legend_and_tooltip(tmp_path):
    """It is an annotation, not data.

    A named series lands in the legend, where a user can toggle away the chart's
    own frame of reference; a hoverable one puts a meaningless two-point series
    in the tooltip. Both are ruled out at the series level, not by hiding the
    legend, so the guarantee survives someone turning the legend back on.
    """
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path)
    option = pred_obs_scatter_option(plot_data)
    line = _refline(option)

    assert "name" not in line
    assert line["silent"] is True
    assert line["tooltip"]["show"] is False
    assert line["legendHoverLink"] is False
    assert option["legend"]["show"] is False


# ---------------------------------------------------------------------------
# Tooltip, annotation, labels, theme
# ---------------------------------------------------------------------------

def test_tooltip_names_every_dimension(tmp_path):
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path)
    formatter = _scatter(pred_obs_scatter_option(plot_data))["tooltip"]["formatter"]

    for label in ("Cell", "Observed", "Predicted", "Forest", "Residual"):
        assert label in formatter
    # each value-array slot the tooltip promises is actually addressed
    for slot in ("{c0}", "{c1}", "{c2}", "{c3}", "{c4}"):
        assert slot in formatter


def test_annotation_uses_the_pngs_number_formats(tmp_path):
    """Same values, same rounding, same wording as the archived figure."""
    from gui.scripts.evaluation_echarts import pred_obs_annotation

    _, plot_data = _loaded(tmp_path)

    assert pred_obs_annotation(plot_data) == "MedAE = 1.50 ha\nR2 = 0.90\nn = 4"


def test_annotation_reaches_the_option(tmp_path):
    from gui.scripts.evaluation_echarts import (
        pred_obs_annotation, pred_obs_scatter_option)

    _, plot_data = _loaded(tmp_path)
    option = pred_obs_scatter_option(plot_data)
    texts = [g["style"]["text"] for g in option["graphic"]]

    assert pred_obs_annotation(plot_data) in texts


def test_annotation_omits_a_metric_the_record_never_stored(tmp_path):
    """Better a shorter block than 'MedAE = nan ha'."""
    from gui.scripts.evaluation_echarts import (
        load_pred_obs_plot_data, pred_obs_annotation)

    run_dir = tmp_path / "evaluation" / "loss_2010" / "run1"
    _write_points(run_dir / "pred_obs_GLM_d1_300.csv")
    record = _typed_record(run_dir, indices=[_index_row(MedAE=None, R2=None)])
    plot_data = load_pred_obs_plot_data(record, "GLM", "d1", 300)

    assert pred_obs_annotation(plot_data) == "n = 4"


def test_labels_are_translatable_but_number_formats_are_not(tmp_path):
    """The widget layer owns wording; the formats stay frozen to the PNG's."""
    from gui.scripts.evaluation_echarts import (
        pred_obs_annotation, pred_obs_scatter_option)

    _, plot_data = _loaded(tmp_path)
    labels = {"x_axis": "Observado (ha)", "y_axis": "Predicho (ha)",
              "cell": "Celda", "medae": "EMA", "n": "n"}
    option = pred_obs_scatter_option(plot_data, labels=labels)

    assert option["xAxis"]["name"] == "Observado (ha)"
    assert option["yAxis"]["name"] == "Predicho (ha)"
    assert "Celda {c2}" in _scatter(option)["tooltip"]["formatter"]
    assert pred_obs_annotation(plot_data, labels).startswith("EMA = 1.50 ha")


def test_the_title_is_the_callers_not_the_pngs_english_one(tmp_path):
    """PredObsPlotData.title is PNG text; the dialog shows a translated title."""
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path)

    assert "title" not in pred_obs_scatter_option(plot_data)
    titled = pred_obs_scatter_option(plot_data, title="Mi gráfico")
    assert titled["title"]["text"] == "Mi gráfico"
    # asserted on a substring: json.dumps escapes the newline inside
    # plot_data.title, so comparing the whole string would never match anyway
    assert "Predicted vs. observed deforestation" not in json.dumps(titled)


def test_axes_and_annotation_follow_the_theme(tmp_path):
    """themed_option only sets the top-level ink, so axes are wired here."""
    from gui.scripts.echarts_options import theme_colors
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path)
    for dark in (True, False):
        option = pred_obs_scatter_option(plot_data, dark=dark)
        ink, grid = theme_colors(dark)["ink"], theme_colors(dark)["grid"]
        assert option["xAxis"]["axisLabel"]["color"] == ink
        assert option["yAxis"]["nameTextStyle"]["color"] == ink
        assert option["xAxis"]["splitLine"]["lineStyle"]["color"] == grid
        assert option["yAxis"]["axisLine"]["lineStyle"]["color"] == grid
        assert option["graphic"][0]["style"]["fill"] == ink


# ---------------------------------------------------------------------------
# Interaction, layout, and the large-point-count switch
# ---------------------------------------------------------------------------

def test_toolbox_offers_zoom_reset_and_save(tmp_path):
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path)
    features = pred_obs_scatter_option(plot_data)["toolbox"]["feature"]

    assert set(features) == {"dataZoom", "restore", "saveAsImage"}


def test_inside_zoom_drives_both_axes_together(tmp_path):
    """Zooming one axis alone would tilt the 1:1 line off 45 degrees."""
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path)
    zooms = pred_obs_scatter_option(plot_data)["dataZoom"]

    assert [z["type"] for z in zooms] == ["inside"]
    assert zooms[0]["xAxisIndex"] == 0 and zooms[0]["yAxisIndex"] == 0


def test_plot_box_is_horizontally_symmetric(tmp_path):
    """Equal insets keep the plot square in a square container.

    ECharts has no aspect lock for a cartesian grid, so squareness comes from
    the container (PRED_OBS_SQUARE_HEIGHT); the option's job is to not skew it.
    """
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    _, plot_data = _loaded(tmp_path)
    grid = pred_obs_scatter_option(plot_data)["grid"]

    assert grid["left"] == grid["right"]
    assert grid["containLabel"] is True


def test_a_small_scatter_stays_on_svg_with_no_large_mode(tmp_path):
    from gui.scripts.echarts_options import RENDERER_SVG
    from gui.scripts.evaluation_echarts import (
        pred_obs_renderer, pred_obs_scatter_option)

    _, plot_data = _loaded(tmp_path)
    scatter = _scatter(pred_obs_scatter_option(plot_data))

    assert pred_obs_renderer(plot_data) == RENDERER_SVG
    assert "large" not in scatter
    assert "progressive" not in scatter


def test_a_dense_scatter_switches_to_canvas_and_large_mode(tmp_path):
    """One batched draw call instead of one DOM node per point."""
    from gui.scripts.echarts_options import RENDERER_CANVAS
    from gui.scripts.evaluation_echarts import (
        PRED_OBS_LARGE_POINT_COUNT, PRED_OBS_PROGRESSIVE_CHUNK,
        load_pred_obs_plot_data, pred_obs_renderer, pred_obs_scatter_option)

    n = PRED_OBS_LARGE_POINT_COUNT
    _, record = _saved(tmp_path, obs=[float(i) for i in range(n)],
                       pred=[float(i) for i in range(n)])
    plot_data = load_pred_obs_plot_data(record, "GLM", "d1", 300)
    option = pred_obs_scatter_option(plot_data)
    scatter = _scatter(option)

    assert pred_obs_renderer(plot_data) == RENDERER_CANVAS
    assert scatter["large"] is True
    assert scatter["largeThreshold"] == PRED_OBS_LARGE_POINT_COUNT
    assert scatter["progressive"] == PRED_OBS_PROGRESSIVE_CHUNK
    assert option["animation"] is False   # animating 200k points helps nobody


# ---------------------------------------------------------------------------
# Artifact resolution — typed record vs legacy derivation
# ---------------------------------------------------------------------------

def test_typed_artifact_wins_over_the_derived_path(tmp_path):
    """The run's own file, even when a same-named one sits in the shared folder.

    This is the whole reason Task 4 introduced typed artifacts: a later run
    against the same truth overwrites ``evaluation/<truth_tag>/``, so a derived
    path can point at another run's numbers.
    """
    from spatialrisk.evaluations import EvaluationPlotArtifact

    from gui.scripts.evaluation_echarts import resolve_points_csv

    run_dir = tmp_path / "evaluation" / "loss_2010" / "run1"
    elsewhere = tmp_path / "somewhere_else" / "points.csv"
    record = _record(run_dir, artifacts=[EvaluationPlotArtifact(
        prediction_key="GLM__d1", model="GLM", period="d1", csize_px=300,
        points_csv=str(elsewhere), png_path=str(elsewhere.with_suffix(".png")))])

    assert resolve_points_csv(record, "GLM", "d1", 300) == elsewhere


def test_a_legacy_record_derives_the_path_beside_its_indices_csv(tmp_path):
    """No artifacts: same derivation figure_entries uses for the PNG."""
    from gui.scripts.evaluation_charts import pred_obs_artifact_name
    from gui.scripts.evaluation_echarts import resolve_points_csv

    run_dir = tmp_path / "evaluation" / "loss_2010"
    resolved = resolve_points_csv(_record(run_dir), "GLM", "d1", 300)

    assert resolved == run_dir / pred_obs_artifact_name("GLM", "d1", 300, "csv")


def test_the_derived_csv_is_the_pngs_twin(tmp_path):
    """The two artifacts share one stem, so the derivations cannot drift."""
    from gui.scripts.evaluation_charts import figure_entries
    from gui.scripts.evaluation_echarts import resolve_points_csv

    run_dir = tmp_path / "evaluation" / "loss_2010"
    record = _record(run_dir)
    png = figure_entries(record.indices, 300, fig_dir=run_dir)[0][1]
    csv = resolve_points_csv(record, "GLM", "d1", 300)

    assert csv.with_suffix(".png") == png


def test_resolution_matches_on_cell_size(tmp_path):
    from spatialrisk.evaluations import EvaluationPlotArtifact

    from gui.scripts.evaluation_echarts import resolve_points_csv

    run_dir = tmp_path / "evaluation" / "loss_2010" / "run1"
    artifacts = [
        EvaluationPlotArtifact(prediction_key="GLM__d1", model="GLM",
                               period="d1", csize_px=c,
                               points_csv=str(run_dir / f"p{c}.csv"),
                               png_path=str(run_dir / f"p{c}.png"))
        for c in (100, 300)
    ]
    record = _record(run_dir, artifacts=artifacts)

    assert resolve_points_csv(record, "GLM", "d1", 100).name == "p100.csv"
    assert resolve_points_csv(record, "GLM", "d1", 300).name == "p300.csv"


def _two_predictions_one_label(run_dir):
    """Two artifacts identical in (model, period, csize), different maps."""
    from spatialrisk.evaluations import EvaluationPlotArtifact

    return [
        EvaluationPlotArtifact(prediction_key=key, model="GLM", period="d1",
                               csize_px=300,
                               points_csv=str(run_dir / f"{key}.csv"),
                               png_path=str(run_dir / f"{key}.png"))
        for key in ("GLM__d1__a", "GLM__d1__b")
    ]


def test_prediction_key_picks_between_artifacts_sharing_a_label(tmp_path):
    """``(model, period, csize)`` is the FILE identity, not the map identity.

    Two predictions can carry the same model+period label — the same model rerun
    against another dataset revision, say — and then only ``prediction_key``
    tells their artifacts apart. Without it the first match wins, which is the
    right default for the single-prediction case but the wrong file here.
    """
    from gui.scripts.evaluation_echarts import resolve_points_csv

    run_dir = tmp_path / "evaluation" / "loss_2010" / "run1"
    record = _record(run_dir, artifacts=_two_predictions_one_label(run_dir))

    assert resolve_points_csv(record, "GLM", "d1", 300).name == "GLM__d1__a.csv"
    assert resolve_points_csv(record, "GLM", "d1", 300,
                              prediction_key="GLM__d1__b").name == "GLM__d1__b.csv"
    assert resolve_points_csv(record, "GLM", "d1", 300,
                              prediction_key="GLM__d1__a").name == "GLM__d1__a.csv"


def test_an_unmatched_prediction_key_falls_back_to_the_derived_path(tmp_path):
    """The fallback fires on ANY miss, not only on a record with no artifacts.

    Documented behavior (see ``resolve_points_csv``): a typed record's
    ``csv_path`` already sits inside its own run folder, so the derived path
    stays run-scoped, and a caller narrowing on a key the record never stored
    still gets the run's own deterministic file instead of nothing.
    """
    from gui.scripts.evaluation_charts import pred_obs_artifact_name
    from gui.scripts.evaluation_echarts import resolve_points_csv

    run_dir = tmp_path / "evaluation" / "loss_2010" / "run1"
    record = _record(run_dir, artifacts=_two_predictions_one_label(run_dir))
    resolved = resolve_points_csv(record, "GLM", "d1", 300,
                                  prediction_key="GLM__d1__nope")

    assert resolved == (Path(record.csv_path).parent
                        / pred_obs_artifact_name("GLM", "d1", 300, "csv"))


def test_a_record_with_nothing_to_derive_from_resolves_to_nothing():
    import types

    from gui.scripts.evaluation_echarts import resolve_points_csv

    record = types.SimpleNamespace(artifacts=[], indices=[], csv_path=None)
    assert resolve_points_csv(record, "GLM", "d1", 300) is None


def test_an_incomplete_selection_resolves_to_nothing_instead_of_raising(tmp_path):
    """The Figures tab has no cell size until a run has one; don't crash on it."""
    from gui.scripts.evaluation_echarts import (
        load_pred_obs_plot_data, pred_obs_chart_identity, resolve_points_csv)

    record = _record(tmp_path / "evaluation" / "loss_2010")
    for args in (("GLM", "d1", None), (None, "d1", 300), ("GLM", None, 300)):
        assert resolve_points_csv(record, *args) is None
        assert load_pred_obs_plot_data(record, *args) is None
        assert pred_obs_chart_identity(record, *args) is None


def test_a_missing_file_loads_as_none_instead_of_raising(tmp_path):
    """A saved run whose files were deleted must degrade, not crash the dialog."""
    from gui.scripts.evaluation_echarts import load_pred_obs_plot_data

    run_dir = tmp_path / "evaluation" / "loss_2010" / "run1"
    assert load_pred_obs_plot_data(_typed_record(run_dir), "GLM", "d1", 300) is None


def test_an_unreadable_file_loads_as_none_instead_of_raising(tmp_path):
    from gui.scripts.evaluation_echarts import load_pred_obs_plot_data

    run_dir = tmp_path / "evaluation" / "loss_2010" / "run1"
    run_dir.mkdir(parents=True)
    (run_dir / "pred_obs_GLM_d1_300.csv").write_text("not,a,point,table\n1,2,3,4\n")

    assert load_pred_obs_plot_data(_typed_record(run_dir), "GLM", "d1", 300) is None


def test_the_option_of_nothing_is_nothing(tmp_path):
    """No plot data, or a table with no finite row, draws no empty frame."""
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    assert pred_obs_scatter_option(None) is None
    _, plot_data = _loaded(tmp_path, obs=[float("nan")], pred=[1.0])
    assert pred_obs_scatter_option(plot_data) is None


# ---------------------------------------------------------------------------
# Memoization keyed on file modification identity
# ---------------------------------------------------------------------------

def test_reloading_an_unchanged_file_does_not_re_read_it(tmp_path):
    """Reopening the dialog must not re-parse a 200k-row CSV."""
    from gui.scripts.evaluation_echarts import load_pred_obs_plot_data

    _, record = _saved(tmp_path)
    first = load_pred_obs_plot_data(record, "GLM", "d1", 300)
    second = load_pred_obs_plot_data(record, "GLM", "d1", 300)

    assert first is second


def test_a_same_size_rewrite_is_never_served_from_the_cache(tmp_path):
    """The exact bug class Task 4 exists to prevent, at the cache layer.

    Two runs against one truth write identically NAMED files. Keying the cache
    on the path alone would hand a record the previous run's points — a wrong
    chart with no error anywhere.

    The rewrite here is byte-for-byte the same LENGTH (same column widths), so
    only the mtime differs: this fails if the key drops mtime and keeps size.
    """
    import os

    from gui.scripts.evaluation_echarts import load_pred_obs_plot_data

    csv_path, record = _saved(tmp_path, obs=[1.0, 2.0], pred=[3.0, 4.0])
    first = load_pred_obs_plot_data(record, "GLM", "d1", 300)
    size_before = csv_path.stat().st_size

    _write_points(csv_path, obs=[5.0, 6.0], pred=[7.0, 8.0])
    os.utime(csv_path, (1, 1))   # a coarse mtime clock must not hide a rewrite
    assert csv_path.stat().st_size == size_before, "rewrite must be same-size"

    second = load_pred_obs_plot_data(record, "GLM", "d1", 300)
    assert second is not first
    assert second.points["ndefor_obs_ha"].tolist() == [5.0, 6.0]


def test_a_same_mtime_rewrite_is_never_served_from_the_cache(tmp_path):
    """The mirror case: mtime pinned identical, only the size moves.

    Fails if the key drops size and keeps mtime. Together with the same-size
    test above, both halves of the modification identity are load-bearing.
    """
    import os

    from gui.scripts.evaluation_echarts import load_pred_obs_plot_data

    csv_path, record = _saved(tmp_path, obs=[1.0, 2.0], pred=[3.0, 4.0])
    os.utime(csv_path, (1, 1))
    first = load_pred_obs_plot_data(record, "GLM", "d1", 300)

    _write_points(csv_path, obs=[1.0, 2.0, 3.0], pred=[3.0, 4.0, 5.0])
    os.utime(csv_path, (1, 1))   # same mtime, more rows
    assert csv_path.stat().st_mtime_ns == 1_000_000_000

    second = load_pred_obs_plot_data(record, "GLM", "d1", 300)
    assert second is not first
    assert second.ncell == 3


def test_the_chart_identity_tracks_everything_the_chart_shows(tmp_path):
    """The widget's memo key and the adapter's digest escape hatch.

    Must move whenever the drawn chart would: a different run, a rewritten file,
    another cell size, the other theme.
    """
    import os

    from gui.scripts.evaluation_echarts import pred_obs_chart_identity

    csv_path, record = _saved(tmp_path)
    base = pred_obs_chart_identity(record, "GLM", "d1", 300)

    assert base == pred_obs_chart_identity(record, "GLM", "d1", 300)
    assert base != pred_obs_chart_identity(record, "GLM", "d1", 300, dark=True)
    assert base != pred_obs_chart_identity(record, "GLM", "d1", 100)

    other = _typed_record(csv_path.parent, run_id="run2")
    assert base != pred_obs_chart_identity(other, "GLM", "d1", 300)

    _write_points(csv_path, obs=[9.0], pred=[9.5])
    os.utime(csv_path, (1, 1))
    assert base != pred_obs_chart_identity(record, "GLM", "d1", 300)


def test_the_chart_identity_moves_when_the_dialog_switches_map(tmp_path):
    """Same record, same cell size, a different model/period.

    The likeliest interaction in the dialog: the map selector changes only
    ``model``/``period`` while the record and the cell size stay put. The
    identity separates the two through the RESOLVED PATH — the file name is
    built from exactly model, period and csize — so a version that dropped the
    path would hand one map's digest to the other map's option, which as an
    ``option_digest`` means the previous map's chart stays on screen.

    The three point CSVs are written with IDENTICAL content (default
    obs/pred/forest values every time), so they are byte-identical and thus
    already the same size. Left alone, ``mtime_ns`` from three sequential
    ``to_csv()`` calls would *usually* differ by OS write-timing jitter alone
    — which would let this assertion pass even with ``path`` dropped from the
    identity, since (size, mtime_ns) would then be the accidental
    discriminator instead. Pinning size (via the identical-content fixture)
    AND mtime_ns (explicitly, via ``os.utime``) to the SAME values on all
    three files removes that accident: ``path`` is left as the only thing
    that can still tell the three identities apart.
    """
    import os

    from spatialrisk.evaluations import EvaluationPlotArtifact

    from gui.scripts.evaluation_echarts import pred_obs_chart_identity

    run_dir = tmp_path / "evaluation" / "loss_2010" / "run1"
    maps = [("GLM", "d1"), ("MW_w11", "d1"), ("GLM", "d2")]
    artifacts = []
    csv_paths = []
    for model, period in maps:
        csv_path = run_dir / f"pred_obs_{model}_{period}_300.csv"
        _write_points(csv_path)
        csv_paths.append(csv_path)
        artifacts.append(EvaluationPlotArtifact(
            prediction_key=f"{model}__{period}", model=model, period=period,
            csize_px=300, points_csv=str(csv_path),
            png_path=str(csv_path.with_suffix(".png"))))
    record = _record(run_dir, artifacts=artifacts)

    sizes = {p.stat().st_size for p in csv_paths}
    assert len(sizes) == 1, "fixture CSVs must be byte-identical in size"
    pinned_ns = 1_700_000_000_000_000_000
    for p in csv_paths:
        os.utime(p, ns=(pinned_ns, pinned_ns))
        assert p.stat().st_mtime_ns == pinned_ns

    identities = [pred_obs_chart_identity(record, model, period, 300)
                  for model, period in maps]
    assert len(set(identities)) == len(maps)


def test_the_chart_identity_covers_the_labels_and_title_too(tmp_path):
    """It is passed as ``option_digest``, so it must cover every option input.

    ``labels`` and ``title`` are option inputs like any other — the widget layer
    (Task 7) fills them with ``t(...)`` output. Used as ``option_digest`` this
    string REPLACES the adapter's content hash, so text it did not cover would
    leave the old language's axis titles on screen after a locale switch, with
    no error anywhere.
    """
    from gui.scripts.evaluation_echarts import (
        pred_obs_chart_identity, pred_obs_scatter_option)

    _, record = _saved(tmp_path)
    base = pred_obs_chart_identity(record, "GLM", "d1", 300)
    spanish = {"x_axis": "Observado (ha)", "y_axis": "Predicho (ha)"}

    assert base == pred_obs_chart_identity(record, "GLM", "d1", 300, labels={})
    assert base != pred_obs_chart_identity(record, "GLM", "d1", 300,
                                           labels=spanish)
    assert base != pred_obs_chart_identity(record, "GLM", "d1", 300,
                                           title="Mi gráfico")
    assert (pred_obs_chart_identity(record, "GLM", "d1", 300, labels=spanish)
            != pred_obs_chart_identity(record, "GLM", "d1", 300,
                                       title="Mi gráfico"))
    # and the options those identities stand for really do differ
    _, plot_data = _loaded(tmp_path)
    assert (pred_obs_scatter_option(plot_data, labels=spanish)
            != pred_obs_scatter_option(plot_data))


def test_the_chart_identity_never_parses_the_file(tmp_path):
    """One stat() — no read, no parse, no option hash.

    That is the whole point: it exists so a per-render sha1 of an option holding
    hundreds of thousands of points never has to happen. Proven by pointing it
    at a file whose CONTENT is unparseable — a loader would raise or return
    None here, while a stat-only identity is unbothered.
    """
    from gui.scripts.evaluation_echarts import (
        load_pred_obs_plot_data, pred_obs_chart_identity)

    run_dir = tmp_path / "evaluation" / "loss_2010" / "run1"
    run_dir.mkdir(parents=True)
    (run_dir / "pred_obs_GLM_d1_300.csv").write_text("garbage,not,a,table\n")
    record = _typed_record(run_dir)

    identity = pred_obs_chart_identity(record, "GLM", "d1", 300)
    assert isinstance(identity, str) and len(identity) < 500
    assert load_pred_obs_plot_data(record, "GLM", "d1", 300) is None


def test_the_chart_identity_of_an_unresolvable_artifact_is_none():
    import types

    from gui.scripts.evaluation_echarts import pred_obs_chart_identity

    record = types.SimpleNamespace(artifacts=[], indices=[], csv_path=None)
    assert pred_obs_chart_identity(record, "GLM", "d1", 300) is None
