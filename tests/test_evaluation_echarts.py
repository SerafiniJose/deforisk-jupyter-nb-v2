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
"""

import json

import pytest

_POINT_COLUMNS = ["cell", "nfor_obs", "ndefor_obs",
                  "nfor_obs_ha", "ndefor_obs_ha", "ndefor_pred_ha"]

# Deliberately awkward floats: values that do NOT survive a round(x, 2) or a
# float32 trip, so the headline equality assertion can actually fail.
_OBS = [0.9199999999999999, 12.34567890123, 3.3333333333333335, 250.125]
_PRED = [1.0700000000000003, 11.98765432109, 4.0000000000000009, 249.875]


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
    was in memory before ``to_csv`` rounded it, is what the chart must match.
    """
    import pandas as pd

    from gui.scripts.evaluation_echarts import load_pred_obs_plot_data

    csv_path, record = _saved(tmp_path, **kw)
    return (pd.read_csv(csv_path),
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
    a ``round``, a float32 cast, a downsample — makes the two disagree about
    what the model predicted, which is the one thing this chart exists to show.
    """
    from gui.scripts.evaluation_echarts import pred_obs_scatter_option

    frame, plot_data = _loaded(tmp_path)
    option = pred_obs_scatter_option(plot_data)

    plotted = [(v[0], v[1]) for v in _scatter(option)["data"]]
    expected = list(zip(frame["ndefor_obs_ha"].tolist(),
                        frame["ndefor_pred_ha"].tolist()))
    assert plotted == expected


def test_every_saved_point_is_plotted(tmp_path):
    """No downsampling, ever — not even above the large-mode threshold.

    Large mode changes HOW the points are drawn (one batched canvas call), never
    how many there are. Dropping points would quietly redraw the model's error
    distribution, so the count is pinned on both sides of the threshold.
    """
    from gui.scripts.evaluation_echarts import (
        PRED_OBS_LARGE_POINT_COUNT, load_pred_obs_plot_data,
        pred_obs_scatter_option)

    n = PRED_OBS_LARGE_POINT_COUNT + 500
    csv_path, record = _saved(tmp_path, obs=[float(i) for i in range(n)],
                              pred=[float(i) * 1.1 for i in range(n)])
    plot_data = load_pred_obs_plot_data(record, "GLM", "d1", 300)
    option = pred_obs_scatter_option(plot_data)

    assert len(_scatter(option)["data"]) == n
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
