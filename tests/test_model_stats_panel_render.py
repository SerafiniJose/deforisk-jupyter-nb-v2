"""Statistics panel renders through reacton, with real content assertions.

These catch the plain-ipyvuetify failure mode a substring test cannot see.
``reacton.render`` hands back a container for essentially any element, so an
``is not None`` assertion proves nothing: a component that silently renders
*nothing* — the plain ``import ipyvuetify as rv`` failure mode these tests
exist to catch — satisfies it just as well as a correct one. So every test
here walks the rendered widget tree and asserts on the strings that actually
reached it.
"""

import threading
import time

import reacton
import solara

from gui.i18n import t

# See test_manage_projects_render: warm the translator before the first render.
t("common.cancel")

import spatialrisk.mlmodels.stats_recovery as stats_recovery  # noqa: E402
from gui.widget.model_form_dialog import ModelDetailsDialog  # noqa: E402
from gui.widget.model_stats_panel import ModelStatsPanel  # noqa: E402
from spatialrisk.mlmodels import GLMModel, ICARModel, MWModel  # noqa: E402
from spatialrisk.mlmodels.stats import Coefficient, GLMStats  # noqa: E402
from spatialrisk.project import Project  # noqa: E402


def _render(el):
    box, rc = reacton.render(el)
    return box


def _texts(widget, out=None):
    """Every string that reached the rendered widget tree.

    Strings are the leaves of a reacton tree (``solara.Text`` renders a
    ``v.Html`` whose only child is the text, ``solara.Info`` puts its label
    straight into the alert's children), so collecting them is what makes an
    empty render distinguishable from a populated one.
    """
    out = [] if out is None else out
    for child in getattr(widget, "children", []) or []:
        if isinstance(child, str):
            out.append(child)
        else:
            _texts(child, out)
    return out


def _wait_for_text(widget, needle, timeout=5.0):
    """Collect the tree's strings, polling until ``needle`` shows up.

    Recovery deliberately runs in a background thread, so the first render
    completes while the task is still pending — the panel shows its spinner and
    only swaps in the outcome once the thread finishes. Without this wait the
    test raced the thread and failed roughly half the time. On a panel that
    renders nothing the needle never arrives and the caller's assertion still
    fails, just ``timeout`` seconds later.
    """
    deadline = time.monotonic() + timeout
    while True:
        texts = _texts(widget)
        if needle in texts or time.monotonic() > deadline:
            return texts
        time.sleep(0.02)


def _labels(widget, out=None):
    """Every ``label`` trait in the tree (the read-only fields carry theirs)."""
    out = [] if out is None else out
    label = getattr(widget, "label", None)
    if isinstance(label, str) and label:
        out.append(label)
    for child in getattr(widget, "children", []) or []:
        if not isinstance(child, str):
            _labels(child, out)
    return out


def _glm_model():
    return GLMModel(
        name="m",
        deviance=100.0,
        trained_at="2026-08-04T13:40:05",
        stats=GLMStats(
            n_rows=10,
            n_events=5,
            coefficients=[Coefficient(name="scale(a)", estimate=0.5)],
            n_iter=22,
            max_iter=1000,
        ),
    )


def test_panel_renders_with_stats():
    """Stored stats render the caveat strip and the stat cards."""
    texts = _texts(_render(ModelStatsPanel(model=_glm_model())))
    assert t("tiles.train.stats.caveat_training_fit") in texts
    # label + value of the first card, so a card that renders only its
    # chrome fails too
    assert t("tiles.train.stats.card_rows") in texts
    assert "10" in texts
    assert t("tiles.train.stats.card_trained_at") in texts
    assert "2026-08-04T13:40:05" in texts


def test_panel_renders_empty_state_without_stats_or_paths():
    """No stats, no model_path/samples_path -> recovery returns None.

    The empty state must render (and the caveat strip stays), not raise.
    """
    model = MWModel(name="w")
    box = _render(ModelStatsPanel(model=model))
    texts = _wait_for_text(box, t("tiles.train.stats.empty_state"))
    assert t("tiles.train.stats.caveat_training_fit") in texts
    assert t("tiles.train.stats.empty_state") in texts
    # Non-negotiable: opening the dialog is read-only. Recovery has run to
    # completion by now and must not have written its outcome back.
    assert model.stats is None


def test_panel_renders_recovered_stats_in_the_cards(monkeypatch):
    """A recovered model's numbers reach the card strip, not just the body.

    ``model.stats`` stays None by the read-only contract, so the cards can only
    show anything if the panel forwards the recovered stats explicitly.
    """
    monkeypatch.setattr(
        stats_recovery,
        "recover_stats",
        lambda model: GLMStats(n_rows=42, n_events=7),
    )
    model = GLMModel(name="legacy", trained_at="2026-08-04T13:40:05")
    box = _render(ModelStatsPanel(model=model))
    texts = _wait_for_text(box, "42")
    assert t("tiles.train.stats.card_rows") in texts
    assert "42" in texts
    assert t("tiles.train.stats.card_events") in texts
    assert "7" in texts
    assert model.stats is None  # recovery stayed read-only


def test_panel_shows_the_recovering_state_while_the_task_runs(monkeypatch):
    """The pending body renders while the background recovery is in flight."""
    release = threading.Event()

    def _blocking_recovery(model):
        release.wait(5.0)
        return None

    monkeypatch.setattr(stats_recovery, "recover_stats", _blocking_recovery)
    box = _render(ModelStatsPanel(model=MWModel(name="w")))
    try:
        texts = _wait_for_text(box, t("tiles.train.stats.recovering"))
        assert t("tiles.train.stats.recovering") in texts
        assert t("tiles.train.stats.empty_state") not in texts
    finally:
        release.set()


def test_panel_hints_at_retraining_for_icar_without_stats(monkeypatch):
    """An unrecoverable iCAR model explains why its intervals are missing."""
    monkeypatch.setattr(stats_recovery, "recover_stats", lambda model: None)
    box = _render(ModelStatsPanel(model=ICARModel(name="i")))
    texts = _wait_for_text(box, t("tiles.train.stats.icar_retrain_hint"))
    assert t("tiles.train.stats.empty_state") in texts
    assert t("tiles.train.stats.icar_retrain_hint") in texts


def test_details_dialog_keeps_configuration_content_under_tabs():
    """Both tab headers render and tab 1 hosts the statistics panel."""
    project = solara.reactive(Project(project_name="p"))
    project.value.models["m"] = _glm_model()
    texts = _texts(
        _render(
            ModelDetailsDialog(project=project, model_key="m", on_close=lambda: None)
        )
    )
    assert t("tiles.train.stats.tab_config") in texts
    assert t("tiles.train.stats.tab_statistics") in texts
    assert t("tiles.train.stats.caveat_training_fit") in texts


def test_details_dialog_configuration_fields_survive_the_restructure():
    """The model/dataset/name read-only fields are still rendered."""
    project = solara.reactive(Project(project_name="p"))
    project.value.models["m"] = _glm_model()
    labels = _labels(
        _render(
            ModelDetailsDialog(project=project, model_key="m", on_close=lambda: None)
        )
    )
    for key in (
        "tiles.train.model_select_label",
        "tiles.train.dataset_select_label",
        "tiles.train.model_name_label",
    ):
        assert t(key) in labels, key
